// parity-cc-routing-trainer (PLAN §3.8, §5.9; phases 5b+5c): the learned forecaster's training,
// never inside RE.
//
//   trainer base     --scenarios=DIR --out=DIR [--minutes=20] [--threads=6] [--samples=N] [--holdout=N]
//                    [--tiny] [--seed=S] [--d-model=48 --layers=2 --heads=4 --ffn=128]
//       The BASE model on synthetic data only (parity-contracts' training catalogue expanded by
//       scripts/forecast_scenarios.py into DIR, with its manifest): the series DES turns each sample
//       into 10 s / 1 min / 15 min series; AdamW on the pinball loss for the time budget; the holdout
//       WAPE (H1: 15 x 1 min, H2: 8 x 15 min) against the baseline (opt::Forecaster with two weeks of
//       profile). Writes DIR/base.pt and DIR/base.json (config, sha256, seeds, the evidence).
//   trainer finetune --tenant=T --conninfo-file=F --base=DIR [--minutes=5] [--days=28]
//       The nightly per-tenant fine-tune (one tenant per run, its own role): reads ONLY the tenant's
//       routing_bucket_actuals (row-level security on parity.tenant_id and WHERE tenant_id = $1),
//       recomputes each queue's range, fine-tunes from the base, scores a holdout (the last day)
//       against the baseline and inserts a `candidate` row into routing_forecast_model.
//   trainer rollback --tenant=T --conninfo-file=F
//       Re-activates the previous model (the active one retires).
// Logs are log-record.v1 (service parity-cc-routing, events trainer.* and model.*) on stderr.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "sim/series.h"
#include "src/forecast/sha256.h"
#include "src/forecast/torch_net.h"
#include "src/ops/log.h"
#include "src/trainer/data.h"
#include "src/trainer/tenant_db.h"

using json = nlohmann::json;
using namespace pcr;
using Clock = std::chrono::steady_clock;

namespace {

std::map<std::string, std::string> parse(int argc, char** argv) {
  std::map<std::string, std::string> f;
  for (int i = 2; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--", 0) != 0) continue;
    const auto eq = a.find('=');
    if (eq == std::string::npos) {
      f[a.substr(2)] = "1";
    } else {
      f[a.substr(2, eq - 2)] = a.substr(eq + 1);
    }
  }
  return f;
}

double secs(Clock::time_point a) { return std::chrono::duration<double>(Clock::now() - a).count(); }

struct Data {
  std::vector<trainer::QSeries> train, eval, hist;
};

// The base model's data: each training sample through the series DES (in parallel); each holdout
// sample too, plus two weeks of its normal traffic before it (the baseline's profile, the ranges).
Data load_samples(const std::string& dir, int max_train, int max_hold, int threads) {
  const json man = json::parse(std::ifstream(dir + "/manifest.json"));
  std::vector<std::pair<std::string, uint64_t>> tr, ho;
  for (const auto& f : man.at("files")) {
    auto& v = f.at("split") == "holdout" ? ho : tr;
    v.emplace_back(dir + "/" + f.at("name").get<std::string>() + ".json", f.at("seed").get<uint64_t>());
  }
  if (max_train > 0 && static_cast<int>(tr.size()) > max_train) tr.resize(static_cast<size_t>(max_train));
  if (max_hold >= 0 && static_cast<int>(ho.size()) > max_hold) ho.resize(static_cast<size_t>(max_hold));
  Data d;
  std::mutex mu;
  std::atomic<size_t> next{0};
  const size_t total = tr.size() + ho.size();
  auto work = [&] {
    for (size_t i = next++; i < total; i = next++) {
      const bool hold = i >= tr.size();
      const auto& [path, seed] = hold ? ho[i - tr.size()] : tr[i];
      const json sc = json::parse(std::ifstream(path));
      std::mt19937_64 rng(seed * 7919 + 1);
      if (!hold) {
        auto q = trainer::from_sim(sim::simulate_series(sc, seed), nullptr, rng);
        std::lock_guard lk(mu);
        for (auto& x : q) d.train.push_back(std::move(x));
        continue;
      }
      const int64_t start = sc.at("forecast").value("start_ms", int64_t{0});
      std::vector<trainer::QSeries> hist;
      for (int w = 2; w >= 1; --w) {
        sim::SeriesOptions o;
        o.spikes = false;
        o.apply_faults = false;
        o.start_ms = start - w * 7LL * 86'400'000;
        auto h = trainer::from_sim(sim::simulate_series(sc, seed + 100'000 * static_cast<uint64_t>(w), o), nullptr, rng);
        for (auto& x : h) {
          x.tenant = sc.at("name").get<std::string>();
          hist.push_back(std::move(x));
        }
      }
      // The ranges come from the week before (history), never from the evaluated week.
      std::vector<trainer::QSeries> last(hist.end() - static_cast<long>(hist.size() / 2), hist.end());
      auto ev = trainer::from_sim(sim::simulate_series(sc, seed), &last, rng);
      std::lock_guard lk(mu);
      for (auto& x : ev) d.eval.push_back(std::move(x));
      for (auto& x : hist) d.hist.push_back(std::move(x));
    }
  };
  std::vector<std::thread> th;
  for (int t = 0; t < std::max(1, threads); ++t) th.emplace_back(work);
  for (auto& t : th) t.join();
  return d;
}

// The trained envelope: the 99.9th percentile of normalized 1-min inputs over the training queues.
double envelope_of(const std::vector<trainer::QSeries>& qs, std::vector<double>* psi_ref) {
  std::vector<double> z;
  std::mt19937_64 rng(5);
  for (const auto& q : qs) {
    for (size_t m = 0; m + 6 <= q.c10.size(); m += 6 * 7) {  // every 7th minute: plenty
      double s = 0;
      bool ok = true;
      for (int k = 0; k < 6; ++k) {
        if (!std::isfinite(q.c10[m + static_cast<size_t>(k)])) ok = false;
        s += q.c10[m + static_cast<size_t>(k)];
      }
      if (ok) z.push_back(q.norm.r.norm(s));
    }
  }
  if (z.empty()) return 2.0;
  *psi_ref = fc::psi_hist(z);
  std::sort(z.begin(), z.end());
  return std::max(1.2, z[static_cast<size_t>(0.999 * static_cast<double>(z.size() - 1))]);
}

struct Sampler {
  const std::vector<trainer::QSeries>* qs;
  std::vector<std::pair<int64_t, int64_t>> range;
  std::vector<double> cum;
  explicit Sampler(const std::vector<trainer::QSeries>& q) : qs(&q) {
    double c = 0;
    for (const auto& x : q) {
      const int64_t a = trainer::first_anchor(x), b = trainer::last_anchor(x);
      range.emplace_back(a, b);
      c += b > a ? static_cast<double>(b - a) : 0;
      cum.push_back(c);
    }
  }
  bool empty() const { return cum.empty() || cum.back() <= 0; }
  fc::Batch batch(int n, std::mt19937_64& rng) const {
    fc::Batch b;
    std::uniform_real_distribution<double> u(0, cum.back());
    while (static_cast<int>(b.in.size()) < n) {
      const size_t i = static_cast<size_t>(std::lower_bound(cum.begin(), cum.end(), u(rng)) - cum.begin());
      if (i >= range.size() || range[i].second <= range[i].first) continue;
      const int64_t s = std::uniform_int_distribution<int64_t>(range[i].first, range[i].second)(rng);
      trainer::Example e;
      if (!trainer::make_example((*qs)[i], s, &e)) continue;
      b.in.push_back(std::move(e.in));
      b.t1.push_back(std::move(e.t1));
      b.t2a.push_back(std::move(e.t2a));
      b.t2h.push_back(std::move(e.t2h));
    }
    return b;
  }
};

fc::NetConfig config_from(std::map<std::string, std::string>& f) {
  fc::NetConfig c;
  if (f.count("tiny")) {
    c.d_model = 32;
    c.layers = 1;
    c.ffn = 85;
  }
  if (f.count("d-model")) c.d_model = std::stoi(f["d-model"]);
  if (f.count("layers")) c.layers = std::stoi(f["layers"]);
  if (f.count("heads")) c.heads = std::stoi(f["heads"]);
  if (f.count("ffn")) c.ffn = std::stoi(f["ffn"]);
  return c;
}

// Trains `tr` until the budget; returns {steps, last train loss, validation loss}.
struct TrainOut {
  uint64_t steps = 0;
  double loss = 0, val = 0, first_loss = 0;
  std::vector<std::pair<uint64_t, double>> curve;
};
TrainOut train(fc::Trainer* tr, const std::vector<trainer::QSeries>& train_q, const std::vector<trainer::QSeries>& val_q,
               double minutes, uint64_t max_steps, int batch, double lr_max, uint64_t seed, const char* what) {
  TrainOut o;
  Sampler s(train_q), sv(val_q.empty() ? train_q : val_q);
  if (s.empty()) return o;
  std::mt19937_64 rng(seed);
  std::mt19937_64 vr(seed + 1);
  std::vector<fc::Batch> val;
  for (int i = 0; i < 4; ++i) val.push_back(sv.batch(batch, vr));
  const auto t0 = Clock::now();
  const double budget = minutes * 60;
  double ema = 0;
  for (;;) {
    const double prog = budget > 0 ? std::min(1.0, secs(t0) / budget) : 0;
    if ((budget > 0 && prog >= 1) || (max_steps > 0 && o.steps >= max_steps)) break;
    const double p = max_steps > 0 && budget <= 0 ? static_cast<double>(o.steps) / static_cast<double>(max_steps) : prog;
    const double warm = std::min(1.0, static_cast<double>(o.steps + 1) / 200.0);
    const double lr = lr_max * warm * (0.1 + 0.9 * 0.5 * (1 + std::cos(M_PI * p)));
    const double l = tr->step(s.batch(batch, rng), lr);
    if (!std::isfinite(l)) continue;
    ema = o.steps == 0 ? l : 0.98 * ema + 0.02 * l;
    if (o.steps == 0) o.first_loss = l;
    ++o.steps;
    if (o.steps % 500 == 0) {
      double v = 0;
      for (const auto& b : val) v += tr->loss(b);
      v /= static_cast<double>(val.size());
      o.curve.emplace_back(o.steps, v);
      LogRec("info", "trainer.progress", "training progress").attr("phase", what).attr("steps", o.steps).attr("loss", ema)
          .attr("val_loss", v).attr("elapsed_s", secs(t0));
    }
  }
  o.loss = ema;
  double v = 0;
  for (const auto& b : val) v += tr->loss(b);
  o.val = v / static_cast<double>(val.size());
  return o;
}

int cmd_base(std::map<std::string, std::string>& f) {
  const std::string dir = f["scenarios"], out = f["out"];
  if (dir.empty() || out.empty()) {
    std::fprintf(stderr, "usage: trainer base --scenarios=DIR --out=DIR [--minutes=20]\n");
    return 2;
  }
  const int threads = f.count("threads") ? std::stoi(f["threads"]) : 6;  // the shared host: <= 6 torch threads (coordinator, 2026-11-02)
  const double minutes = f.count("minutes") ? std::stod(f["minutes"]) : 20;
  const uint64_t max_steps = f.count("steps") ? std::stoull(f["steps"]) : 0;
  const int max_train = f.count("samples") ? std::stoi(f["samples"]) : 0;
  const int max_hold = f.count("holdout") ? std::stoi(f["holdout"]) : -1;
  const uint64_t seed = f.count("seed") ? std::stoull(f["seed"]) : 1;
  const int batch = f.count("batch") ? std::stoi(f["batch"]) : 256;
  const fc::NetConfig cfg = config_from(f);
  fc::torch_threads(threads, 1);
  const auto t0 = Clock::now();
  LogRec("info", "trainer.started", "base training started").attr("mode", "base").attr("threads", threads)
      .attr("minutes", minutes);
  Data d = load_samples(dir, max_train, max_hold, threads);
  const double sim_s = secs(t0);
  std::vector<double> psi_ref;
  const double env = envelope_of(d.train, &psi_ref);
  for (auto& q : d.train) q.norm.envelope = env;
  for (auto& q : d.eval) q.norm.envelope = env;
  auto tr = fc::make_trainer(fc::make_net(cfg, seed), 1e-4);
  // Validation: the holdout's own examples (never trained on).
  TrainOut to = train(tr.get(), d.train, d.eval, minutes, max_steps, batch, 2e-3, seed, "base");
  const double train_s = secs(t0) - sim_s;
  const trainer::Wape w = trainer::evaluate(&tr->net(), d.eval, d.hist, 30);
  std::string bytes, sha;
  if (!fc::save_net(tr->net(), &bytes, &sha)) {
    LogRec("error", "trainer.refused", "could not serialize the model").error("internal", "save_net");
    return 1;
  }
  std::filesystem::create_directories(out);
  std::ofstream(out + "/base.pt", std::ios::binary) << bytes;
  const json man = json::parse(std::ifstream(dir + "/manifest.json"));
  json curve = json::array();
  for (const auto& [st, v] : to.curve) curve.push_back({{"steps", st}, {"val_loss", v}});
  const json meta = {
      {"kind", "parity-cc-routing forecaster base model"},
      {"config", cfg.to_json()},
      {"sha256", sha},
      {"bytes", bytes.size()},
      {"parameters", tr->net().parameters()},
      {"created_at_ms", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()},
      {"normalization", {{"envelope", env}, {"psi_ref", psi_ref}, {"rule", "log1p per-minute, fixed p1/p99 per (tenant, queue), no clipping"}}},
      {"data", {{"catalogue", man.value("catalogue", "")}, {"base_seed", man.value("base_seed", 0)},
                {"train_queues", d.train.size()}, {"holdout_queues", d.eval.size()},
                {"train_samples", max_train > 0 ? max_train : man.value("samples", 0) - man.value("holdout", 0)},
                {"holdout_samples", max_hold >= 0 ? max_hold : man.value("holdout", 0)}}},
      {"training", {{"steps", to.steps}, {"batch", batch}, {"first_loss", to.first_loss}, {"final_loss", to.loss},
                    {"val_loss", to.val}, {"curve", curve}, {"series_s", sim_s}, {"train_s", train_s},
                    {"total_s", secs(t0)}, {"threads", threads}, {"seed", seed}}},
      {"holdout", {{"anchors", w.anchors}, {"h1_wape_model", w.h1()}, {"h1_wape_baseline", w.b_h1()},
                   {"h2_wape_model", w.h2()}, {"h2_wape_baseline", w.b_h2()},
                   {"h1", "the first 15 one-minute steps after each anchor (every 5 min)"},
                   {"h2", "the 8 fifteen-minute buckets from the current one"}}},
  };
  std::ofstream(out + "/base.json") << meta.dump(2) << "\n";
  LogRec("info", "trainer.finished", "base model written").attr("mode", "base").attr("sha256", sha)
      .attr("steps", to.steps).attr("val_loss", to.val).attr("h1_wape_model", w.h1()).attr("h1_wape_baseline", w.b_h1())
      .attr("h2_wape_model", w.h2()).attr("h2_wape_baseline", w.b_h2()).duration_ms(secs(t0) * 1000);
  std::printf("%s\n", meta.dump(2).c_str());
  return 0;
}

int cmd_finetune(std::map<std::string, std::string>& f) {
  const std::string tenant = f["tenant"], conn_file = f["conninfo-file"], base_dir = f["base"];
  if (tenant.empty() || conn_file.empty() || base_dir.empty()) {
    std::fprintf(stderr, "usage: trainer finetune --tenant=T --conninfo-file=F --base=DIR\n");
    return 2;
  }
  const double minutes = f.count("minutes") ? std::stod(f["minutes"]) : 5;
  const uint64_t max_steps = f.count("steps") ? std::stoull(f["steps"]) : 0;
  const int days = f.count("days") ? std::stoi(f["days"]) : 28;
  const int threads = f.count("threads") ? std::stoi(f["threads"]) : 2;
  fc::torch_threads(threads, 1);
  const auto t0 = Clock::now();
  LogRec("info", "trainer.started", "fine-tune started").tenant(tenant).attr("mode", "finetune");
  const json base = json::parse(std::ifstream(base_dir + "/base.json"));
  std::string bytes;
  {
    std::ifstream in(base_dir + "/base.pt", std::ios::binary);
    bytes.assign(std::istreambuf_iterator<char>(in), {});
  }
  const fc::NetConfig cfg = fc::NetConfig::from_json(base.at("config"));
  std::string why;
  auto net = fc::load_net(cfg, bytes, base.at("sha256").get<std::string>(), &why);
  if (!net) {
    LogRec("error", "trainer.refused", "the base model was refused").tenant(tenant).error("base_model", why);
    return 1;
  }
  trainer::TenantDb db;
  if (!db.connect_file(conn_file)) {
    LogRec("error", "trainer.refused", "no database connection").tenant(tenant).error("unavailable", "connect");
    return 1;
  }
  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  const int64_t to_ms = f.count("to-ms") ? std::stoll(f["to-ms"]) : now_ms / fc::kBucketMs * fc::kBucketMs;
  const int64_t from_ms = to_ms - static_cast<int64_t>(days) * 86'400'000;
  std::vector<trainer::QSeries> all;
  if (!db.load_series(tenant, from_ms, to_ms, &all)) {
    LogRec("error", "trainer.refused", "could not read the tenant's history").tenant(tenant).error("unavailable", "load_series");
    return 1;
  }
  const double env = base.at("normalization").value("envelope", 2.0);
  // Holdout: the last day; the ranges come from the days before it.
  std::vector<trainer::QSeries> train_q, eval_q, hist_q;
  const int64_t cut = std::max<int64_t>(0, (to_ms - 86'400'000 - from_ms) / fc::kStepMs);
  for (auto& q : all) {
    trainer::QSeries a = q, b = q;
    a.c10.resize(std::min(a.c10.size(), static_cast<size_t>(cut)));
    a.b_off.resize(static_cast<size_t>(cut / fc::kStepsPerBucket));
    a.b_ab.resize(a.b_off.size());
    a.b_aht.resize(a.b_off.size());
    a.norm = trainer::norm_from(a, env);
    b.norm = a.norm;
    // The evaluated day keeps the whole window (its anchors start a day before the end).
    hist_q.push_back(a);
    train_q.push_back(std::move(a));
    eval_q.push_back(std::move(b));
  }
  auto tr = fc::make_trainer(std::move(net), 1e-4);
  const auto before = trainer::evaluate(&tr->net(), eval_q, hist_q, 30, cut);
  TrainOut to = train(tr.get(), train_q, {}, minutes, max_steps, 128, 3e-4, 7, "finetune");
  const auto w = trainer::evaluate(&tr->net(), eval_q, hist_q, 30, cut);
  std::string out_bytes, sha;
  if (!fc::save_net(tr->net(), &out_bytes, &sha)) return 1;
  json norm = json::object();
  for (const auto& q : train_q) norm[q.queue] = q.norm.to_json();
  trainer::ModelRow row;
  row.tenant = tenant;
  row.config = cfg.to_json();
  row.normalization = norm;
  row.sha256 = sha;
  row.bytes = out_bytes;
  row.base_sha256 = base.at("sha256").get<std::string>();
  row.train_from_ms = from_ms;
  row.train_to_ms = to_ms;
  row.created_at_ms = now_ms;
  row.holdout = {{"anchors", w.anchors}, {"h1_wape_model", w.h1()}, {"h1_wape_baseline", w.b_h1()},
                 {"h2_wape_model", w.h2()}, {"h2_wape_baseline", w.b_h2()}, {"h1_wape_base_model", before.h1()},
                 {"steps", to.steps}, {"val_loss", to.val}};
  uint64_t version = 0;
  if (!db.insert_candidate(row, &version)) {
    LogRec("error", "trainer.refused", "could not write the model row").tenant(tenant).error("unavailable", "insert");
    return 1;
  }
  LogRec("info", "model.written", "candidate model written").tenant(tenant).attr("version", version).attr("sha256", sha)
      .attr("h1_wape_model", w.h1()).attr("h1_wape_baseline", w.b_h1()).attr("queues", static_cast<int64_t>(train_q.size()))
      .duration_ms(secs(t0) * 1000);
  std::printf("%s\n", json({{"tenant", tenant}, {"version", version}, {"sha256", sha}, {"holdout", row.holdout}}).dump().c_str());
  return 0;
}

int cmd_rollback(std::map<std::string, std::string>& f) {
  const std::string tenant = f["tenant"];
  trainer::TenantDb db;
  if (tenant.empty() || !db.connect_file(f["conninfo-file"])) {
    std::fprintf(stderr, "usage: trainer rollback --tenant=T --conninfo-file=F\n");
    return 2;
  }
  uint64_t from = 0, to = 0;
  if (!db.rollback(tenant, &from, &to)) {
    LogRec("warn", "trainer.refused", "nothing to roll back to").tenant(tenant).error("failed_precondition", "rollback");
    return 1;
  }
  LogRec("info", "model.rolled_back", "previous model re-activated").tenant(tenant).attr("from_version", from)
      .attr("to_version", to);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: trainer base|finetune|rollback --...\n");
    return 2;
  }
  auto f = parse(argc, argv);
  LogInit li;
  li.instance = "trainer";
  li.level = f.count("log-level") ? f["log-level"] : "info";
  log_init(li);
  const std::string cmd = argv[1];
  int rc = 2;
  if (cmd == "base") rc = cmd_base(f);
  else if (cmd == "finetune") rc = cmd_finetune(f);
  else if (cmd == "rollback") rc = cmd_rollback(f);
  else std::fprintf(stderr, "unknown command %s\n", cmd.c_str());
  log_flush();
  return rc;
}
