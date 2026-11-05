// The learned forecaster's OOD suite (PLAN §3.8, §5.9; the user, 2026-11-01): every held-out
// scenario of parity-contracts' scenarios/forecast-ood/ through the routing DES (the real dispatch,
// redials and bots) with the real optimizer acting every 10 s (auto mode: levers a/b), in three arms
// on the same seeds:
//   forecaster     the learned source (the shipped base model) behind ForecastSource, with the
//                  scenario's data faults applied to ITS inputs only, the arrival guard and the
//                  event-driven early snapshot (the writer's 1 s check);
//   baseline_only  the baseline forecast alone;
//   oracle         a perfect forecast (the scenario's true rates, bots included, redials not).
// History: `history_days` of the scenario's normal traffic (spikes, bots, level steps and redials
// removed) through the series DES at the same hours of the days before: the baseline's profile and
// the learned source's ranges. Measured per scenario: the first fallback reason after the onset and
// its time, fallback entries after the onset (flapping), recovery after the end, the WAPE of arrivals
// per source (1 min and 15 min, over the measurement window), SL, abandonment and J per arm.
//   bazel run -c opt //sim:forecast_ood -- --contracts=$PWD/../parity-contracts --model=$PWD/model/forecast
//       --out=<dir> [--reps=2] [--threads=4] [--only=<name>]
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "sim/arrivals.h"
#include "sim/des.h"
#include "sim/series.h"
#include "src/core/tenant.h"
#include "src/forecast/model_file.h"
#include "src/forecast/source.h"
#include "src/forecast/torch_net.h"
#include "src/opt/optimizer.h"

using json = nlohmann::json;
using namespace pcr;
using namespace pcr::sim;

namespace {

constexpr int64_t kDayMs = 86'400'000;
constexpr double kWarmS = 7200, kDrainS = 600;

// The scenario's data faults, applied to the learned source's inputs only (never the traffic).
class FaultFeed final : public opt::ForecastSource {
 public:
  FaultFeed(opt::ForecastSource* inner, json faults, int64_t start_ms) : in_(inner), f_(std::move(faults)), start_(start_ms) {}
  std::optional<opt::QueueForecast> forecast(const std::string& t, const std::string& q, int64_t now,
                                             const opt::QueueForecast& b) override {
    return in_->forecast(t, q, now, b);
  }
  std::string why_not(const std::string& t, const std::string& q) const override { return in_->why_not(t, q); }
  void history(const std::string& t, const std::vector<opt::HistoryRow>& rows) override { in_->history(t, rows); }
  void observe(const std::string& tenant, const std::vector<opt::ObservedWindow>& ws) override {
    std::vector<opt::ObservedWindow> out;
    std::map<std::string, opt::ObservedWindow> merged;
    for (opt::ObservedWindow w : ws) {
      const double t = static_cast<double>(w.at_ms - start_) / 1000.0;
      bool drop = false;
      for (const auto& d : f_) {
        const std::string kind = d.value("kind", std::string());
        const double a = d.value("at_s", 0.0), e = a + d.value("duration_s", 1e12);
        const bool on = t >= a && t < e;
        const bool mine = !d.contains("queue") || d["queue"] == w.queue;
        if (kind == "gap" && on && mine) drop = true;
        if (kind == "duplicate" && on && mine) w.arrivals *= 2;
        if (kind == "clock_jump" && on) w.at_ms += static_cast<int64_t>(d.value("jump_s", 0.0) * 1000);
        if (kind == "rename" && t >= a && mine) w.queue = d.value("to_queue", w.queue);
        if (kind == "merge" && t >= a) {
          const auto qs = d.value("queues", json::array());
          if (std::find(qs.begin(), qs.end(), w.queue) != qs.end()) {
            const std::string to = d.value("to_queue", w.queue);
            auto [it, fresh] = merged.try_emplace(to, w);
            if (!fresh) {
              it->second.arrivals += w.arrivals;
              it->second.abandons += w.abandons;
              it->second.handled += w.handled;
              it->second.handle_s += w.handle_s;
            }
            it->second.queue = to;
            drop = true;
          }
        }
      }
      if (!drop) out.push_back(w);
    }
    for (auto& [q, w] : merged) out.push_back(w);
    in_->observe(tenant, out);
  }

 private:
  opt::ForecastSource* in_;
  json f_;
  int64_t start_;
};

// A perfect forecast: the scenario's true rates (bots included; redials are a consequence of service).
class Oracle final : public opt::ForecastSource {
 public:
  Oracle(std::map<std::string, RateModel> rm, int64_t start_ms) : rm_(std::move(rm)), start_(start_ms) {}
  std::optional<opt::QueueForecast> forecast(const std::string&, const std::string& q, int64_t now,
                                             const opt::QueueForecast& b) override {
    auto it = rm_.find(q);
    if (it == rm_.end()) return std::nullopt;
    const RateModel& m = it->second;
    auto r = [&](double t) { return m.rate(t) + m.bot_rate(t); };
    const double t = static_cast<double>(now - start_) / 1000.0;
    opt::QueueForecast f = b;
    f.now_per_s = r(t);
    const double bucket_end = (std::floor(t / 900) + 1) * 900;
    for (size_t k = 0; k < f.per_s.size(); ++k) {
      const double a = k == 0 ? t : bucket_end + static_cast<double>(k - 1) * 900, e = k == 0 ? bucket_end : a + 900;
      double s = 0;
      int n = 0;
      for (double x = a; x < e; x += 10, ++n) s += r(x);
      f.per_s[k] = n ? s / n : r(a);
    }
    return f;
  }

 private:
  std::map<std::string, RateModel> rm_;
  int64_t start_;
};

Dist dist_of(const json& j) {
  const std::string k = j.value("kind", std::string("exponential"));
  if (k == "lognormal") return Dist::lognormal(j.value("mean_s", 180.0), j.value("cv", 1.0));
  if (k == "fixed") return Dist::fixed(j.value("s", 0.0));
  return Dist::exp(j.value("mean_s", 120.0));
}

json normal_traffic(json sc) {
  for (auto& q : sc["tenants"][0]["queues"]) {
    q["arrivals"].erase("spikes");
    q["arrivals"].erase("bots");
    q["arrivals"].erase("level_steps");
    q.erase("redial");
  }
  sc["forecast"].erase("data_faults");
  return sc;
}

struct Point {
  int64_t t_ms;
  std::string queue;
  bool driving;
  fc::Reason reason;
};

struct ArmResult {
  std::string arm;
  double sl = 0, abandon = 0, j = 0;
  uint64_t offered = 0, answered = 0, abandoned = 0, redials = 0, bots = 0, violations = 0, errors = 0;
  // forecaster arm only
  fc::PredError err;
  std::vector<Point> timeline;
  uint64_t early = 0, inferences = 0;
  uint64_t plans = 0, changes = 0, exact_plans = 0;  // lever plans, planned weight/delay changes
  double wall_s = 0;
};

struct Case {
  std::string name;
  json doc;
  Scenario sc;
  std::map<std::string, RateModel> rm;
  std::vector<std::pair<std::string, std::vector<BucketRow>>> history;  // per queue rows
  int64_t start_ms = 0;
  double onset_s = 0, end_s = 0;
};

Case build(const std::string& path) {
  Case c;
  c.doc = json::parse(std::ifstream(path));
  c.name = c.doc.at("name").get<std::string>();
  const json& f = c.doc.at("forecast");
  c.start_ms = f.at("start_ms").get<int64_t>();
  c.onset_s = f.at("ood").at("onset_s").get<double>();
  c.end_s = f["ood"].value("end_s", -1.0);
  const double dur = c.doc.at("duration_s").get<double>();
  Scenario& s = c.sc;
  s.name = c.name;
  s.warmup_s = kWarmS;
  s.measure_s = dur - kWarmS;
  s.drain_s = kDrainS;
  s.mode = 2;
  s.start_ms = c.start_ms;
  s.hook_every_s = 1;
  const json& t = c.doc.at("tenants").at(0);
  for (const auto& q : t.at("queues")) {
    RateModel rm = RateModel::from_json(q.at("arrivals"));
    QueueSpec x;
    x.id = q.at("id").get<std::string>();
    x.bucket_s = 10;
    bool bots = false;
    for (double ts = 0; ts < dur + kDrainS; ts += 10) {
      x.per_s.push_back(rm.rate(ts + 5));
      const double b = rm.bot_rate(ts + 5);
      x.bot_per_s.push_back(b);
      bots = bots || b > 0;
    }
    if (!bots) x.bot_per_s.clear();
    for (const auto& b : q.at("arrivals").value("bots", json::array())) {
      x.bot_patience_s = b.value("patience_s", 3.0);
      x.bot_talk_s = b.value("talk_s", 5.0);
    }
    x.talk = dist_of(q.at("handle_time"));
    x.patience = dist_of(q.at("patience"));
    x.max_wait_s = q.value("max_wait_s", 600.0);
    x.sla_s = 20;
    if (q.contains("redial")) {
      x.redial_p = q["redial"].value("p", 0.0);
      x.redial_delay_s = q["redial"].value("mean_delay_s", 120.0);
    }
    s.queues.push_back(x);
    c.rm.emplace(x.id, rm);
  }
  for (const auto& a : t.at("agents")) {
    std::vector<std::string> qs;
    for (const auto& q : a.at("queues")) qs.push_back(q.get<std::string>());
    s.groups.emplace_back(a.at("count").get<int>(), qs);
  }
  // History: the normal traffic at the same hours of each of the days before.
  const json normal = normal_traffic(c.doc);
  const int days = f.value("history_days", 14);
  std::map<std::string, std::vector<BucketRow>> rows;
  for (int d = days; d >= 1; --d) {
    SeriesOptions o;
    o.spikes = false;
    o.apply_faults = false;
    o.start_ms = c.start_ms - d * kDayMs;
    const SeriesTenant st = simulate_series(normal, 7000 + static_cast<uint64_t>(d), o);
    for (const auto& q : st.q) {
      for (size_t b = 0; b < q.b_off.size(); ++b) {
        BucketRow r;
        r.tenant = "acme";
        r.queue_id = q.id;
        r.bucket_start_ms = o.start_ms + static_cast<int64_t>(b) * 900'000;
        r.offered = static_cast<uint64_t>(q.b_off[b]);
        r.abandoned = static_cast<uint64_t>(q.b_ab[b]);
        r.handled = static_cast<uint64_t>(q.b_hand[b]);
        r.handle_ms = q.b_handle_s[b] * 1000.0;
        r.offered_10s.assign(q.c10.begin() + static_cast<long>(b * 90),
                             q.c10.begin() + static_cast<long>(std::min(q.c10.size(), (b + 1) * 90)));
        if (r.offered_10s.size() != 90) continue;
        rows[q.id].push_back(std::move(r));
      }
    }
  }
  for (auto& [q, r] : rows) c.history.emplace_back(q, std::move(r));
  return c;
}

ArmResult run_arm(const Case& c, const std::string& arm, uint64_t seed, std::shared_ptr<const fc::ModelSpec> base) {
  ArmResult out;
  out.arm = arm;
  const auto w0 = std::chrono::steady_clock::now();
  Scenario sc = c.sc;
  auto state = std::make_shared<Optimizer::TenantState>();
  auto stats = std::make_shared<model::Stats>();
  std::vector<BucketRow> all;
  for (const auto& [q, rows] : c.history) {
    for (const auto& r : rows) {
      state->fc.add_actual(r.queue_id, r.bucket_start_ms, static_cast<double>(r.offered));
      all.push_back(r);
    }
  }
  state->history_asked = true;
  std::shared_ptr<fc::LearnedSource> src;
  std::shared_ptr<opt::ForecastSource> feed;
  if (arm == "forecaster") {
    src = std::make_shared<fc::LearnedSource>(fc::SourceOptions{}, base);
    feed = std::make_shared<FaultFeed>(src.get(), c.doc["forecast"].value("data_faults", json::array()), c.start_ms);
    feed->history("acme", Optimizer::history_rows(all));
  } else if (arm == "oracle") {
    feed = std::make_shared<Oracle>(c.rm, c.start_ms);
  }
  Optimizer::Options o;
  o.levers = true;
  o.learned = feed.get();
  auto last_tick = std::make_shared<int64_t>(INT64_MIN);
  auto timeline = std::make_shared<std::vector<Point>>();
  auto early = std::make_shared<uint64_t>(0);
  auto plans = std::make_shared<std::array<uint64_t, 3>>();
  auto err_at_warm = std::make_shared<fc::PredError>();
  auto warm_done = std::make_shared<bool>(false);
  const int64_t warm_ms = c.start_ms + static_cast<int64_t>(kWarmS * 1000);
  sc.hook = [=](Tenant* t, int64_t now) {
    const bool regular = *last_tick == INT64_MIN || now - *last_tick >= 10'000;
    if (!regular) {
      if (!src || !t->guard_breached(now)) return;
      ++*early;
    } else {
      *last_tick = now;
    }
    auto snap = t->model_snapshot(now);
    Optimizer::LeverOut lo = Optimizer::levers_for(*snap, state.get(), o, stats.get());
    if (lo.guard) t->install_guard(lo.guard, now);
    if (lo.plan) {
      ++(*plans)[0];
      if (lo.plan->quality == 0) ++(*plans)[2];
      for (size_t i = 0; i < lo.queues.size() && i < lo.result.w_planned.size(); ++i) {
        if (lo.result.w_planned[i] != lo.queues[i].weight || lo.result.d_planned[i] != lo.queues[i].delay_s) ++(*plans)[1];
      }
      t->install_levers(lo.plan, now);
    }
    if (src) {
      if (!*warm_done && now >= warm_ms) {
        *warm_done = true;
        *err_at_warm = src->error("acme");
      }
      for (const auto& q : snap->queues) {
        const auto h = src->health("acme", q.id);
        timeline->push_back(Point{now, q.id, h.driving, h.reason});
      }
    }
  };
  const RepResult r = run_replication(sc, seed);
  double lam_terms = 0;
  uint64_t in_sla = 0, short_ab = 0;
  for (size_t i = 0; i < r.q.size(); ++i) {
    const QueueStats& q = r.q[i];
    out.offered += q.offered;
    out.answered += q.answered;
    out.abandoned += q.abandoned;
    in_sla += q.answered_in_sla;
    short_ab += q.short_abandoned;
    lam_terms += static_cast<double>(q.offered) / sc.measure_s * ((1 - q.sl()) + q.abandon());
  }
  out.sl = out.offered > short_ab ? static_cast<double>(in_sla) / static_cast<double>(out.offered - short_ab) : 0;
  out.abandon = out.offered ? static_cast<double>(out.abandoned) / static_cast<double>(out.offered) : 0;
  out.j = lam_terms;
  out.plans = (*plans)[0];
  out.changes = (*plans)[1];
  out.exact_plans = (*plans)[2];
  out.redials = r.redials;
  out.bots = r.bots;
  out.violations = r.invariant_violations;
  out.errors = r.engine_errors;
  if (src) {
    fc::PredError e = src->error("acme");
    for (int s = 0; s < fc::kSrcs; ++s) {
      e.m1[static_cast<size_t>(s)].abs -= err_at_warm->m1[static_cast<size_t>(s)].abs;
      e.m1[static_cast<size_t>(s)].act -= err_at_warm->m1[static_cast<size_t>(s)].act;
      e.m15[static_cast<size_t>(s)].abs -= err_at_warm->m15[static_cast<size_t>(s)].abs;
      e.m15[static_cast<size_t>(s)].act -= err_at_warm->m15[static_cast<size_t>(s)].act;
    }
    out.err = e;
    out.timeline = std::move(*timeline);
    out.early = *early;
    out.inferences = src->totals().inferences;
  }
  out.wall_s = std::chrono::duration<double>(std::chrono::steady_clock::now() - w0).count();
  return out;
}

// From the forecaster's timeline: the first reason after the onset (and when), entries into fallback
// after the onset (per queue, the largest), the time the source drove every queue again after the end,
// and the share of the time it drove before the onset (normal traffic).
struct Detect {
  std::string first_reason = "none";
  double first_s = -1;          // seconds after the onset
  double expected_s = -1;       // the first EXPECTED trigger, seconds after the onset
  uint64_t max_entries = 0;
  double recover_s = -1;        // seconds after end_s when every queue drives again (-1: never)
  double drove_before = 0;      // share of evaluations driving in [warm-up end, onset)
};

Detect detect(const Case& c, const ArmResult& a) {
  Detect d;
  const int64_t onset = c.start_ms + static_cast<int64_t>(c.onset_s * 1000);
  const int64_t warm = c.start_ms + static_cast<int64_t>(kWarmS * 1000);
  const int64_t end = c.end_s >= 0 ? c.start_ms + static_cast<int64_t>(c.end_s * 1000) : INT64_MAX;
  std::set<std::string> expected;
  for (const auto& t : c.doc["forecast"]["ood"]["expect"].value("triggers", json::array())) expected.insert(t.get<std::string>());
  std::map<std::string, bool> driving;
  std::map<std::string, uint64_t> entries;
  uint64_t before_n = 0, before_drive = 0;
  int64_t last_t = 0;
  std::map<std::string, bool> at_t;
  for (const auto& p : a.timeline) {
    const bool was = driving.count(p.queue) ? driving[p.queue] : false;
    if (p.t_ms >= warm && p.t_ms < onset) {
      ++before_n;
      before_drive += p.driving;
    }
    if (p.t_ms >= onset && p.reason != fc::Reason::kNone) {
      const std::string rn = fc::reason_name(p.reason);
      if (d.first_s < 0) {
        d.first_s = static_cast<double>(p.t_ms - onset) / 1000.0;
        d.first_reason = rn;
      }
      if (d.expected_s < 0 && expected.count(rn)) d.expected_s = static_cast<double>(p.t_ms - onset) / 1000.0;
    }
    if (p.t_ms >= onset && was && !p.driving) ++entries[p.queue];
    driving[p.queue] = p.driving;
    if (p.t_ms != last_t) {
      at_t.clear();
      last_t = p.t_ms;
    }
    at_t[p.queue] = p.driving;
    if (d.recover_s < 0 && end != INT64_MAX && p.t_ms >= end && at_t.size() == c.sc.queues.size()) {
      bool all = true;
      for (const auto& [q, v] : at_t) all = all && v;
      if (all) d.recover_s = static_cast<double>(p.t_ms - end) / 1000.0;
    }
  }
  for (const auto& [q, n] : entries) d.max_entries = std::max(d.max_entries, n);
  d.drove_before = before_n ? static_cast<double>(before_drive) / static_cast<double>(before_n) : 0;
  return d;
}

}  // namespace

int main(int argc, char** argv) {
  std::string contracts = "../parity-contracts", model_dir = "model/forecast", out_dir = ".", only;
  int reps = 2, threads = 4;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a.rfind("--contracts=", 0) == 0) contracts = a.substr(12);
    else if (a.rfind("--model=", 0) == 0) model_dir = a.substr(8);
    else if (a.rfind("--out=", 0) == 0) out_dir = a.substr(6);
    else if (a.rfind("--reps=", 0) == 0) reps = std::stoi(a.substr(7));
    else if (a.rfind("--threads=", 0) == 0) threads = std::stoi(a.substr(10));
    else if (a.rfind("--only=", 0) == 0) only = a.substr(7);
  }
  fc::torch_threads(1, 1);
  std::string why;
  auto base = fc::load_base_spec(model_dir, "", &why);
  if (!base) {
    std::fprintf(stderr, "base model refused: %s\n", why.c_str());
    return 2;
  }
  std::vector<std::string> files;
  for (const auto& e : std::filesystem::directory_iterator(contracts + "/scenarios/forecast-ood")) {
    if (e.path().extension() == ".json" && (only.empty() || e.path().stem() == only)) files.push_back(e.path().string());
  }
  std::sort(files.begin(), files.end());
  const auto t0 = std::chrono::steady_clock::now();
  std::vector<Case> cases;
  for (const auto& f : files) cases.push_back(build(f));
  const std::vector<std::string> arms{"forecaster", "baseline_only", "oracle"};
  struct Job {
    size_t c, a;
    int rep;
  };
  std::vector<Job> jobs;
  for (size_t c = 0; c < cases.size(); ++c) {
    for (size_t a = 0; a < arms.size(); ++a) {
      for (int r = 0; r < reps; ++r) jobs.push_back({c, a, r});
    }
  }
  std::vector<std::vector<std::vector<ArmResult>>> res(cases.size(), std::vector<std::vector<ArmResult>>(arms.size(), std::vector<ArmResult>(static_cast<size_t>(reps))));
  std::atomic<size_t> next{0};
  std::vector<std::thread> pool;
  for (int t = 0; t < std::max(1, threads); ++t) {
    pool.emplace_back([&] {
      for (size_t k = next++; k < jobs.size(); k = next++) {
        const Job& j = jobs[k];
        res[j.c][j.a][static_cast<size_t>(j.rep)] = run_arm(cases[j.c], arms[j.a], 9000 + static_cast<uint64_t>(j.rep), base);
      }
    });
  }
  for (auto& t : pool) t.join();
  json all = json::object();
  std::string md =
      "| scenario | expected triggers | first reason (s after onset) | expected within (s) | detect | max fallback entries | "
      "recovery after end (s) | drove before onset | WAPE 1 min model / in use / baseline | WAPE 15 min model / in use / baseline | "
      "SL fc / base / oracle | abandon fc / base / oracle | J fc / base / oracle | early snapshots | pass |\n"
      "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n";
  int passed = 0;
  for (size_t c = 0; c < cases.size(); ++c) {
    const Case& cs = cases[c];
    const json ex = cs.doc["forecast"]["ood"]["expect"];
    auto mean = [&](size_t a, auto pick) {
      double s = 0;
      for (const auto& r : res[c][a]) s += pick(r);
      return s / static_cast<double>(res[c][a].size());
    };
    const double j_fc = mean(0, [](const ArmResult& r) { return r.j; }), j_b = mean(1, [](const ArmResult& r) { return r.j; }),
                 j_o = mean(2, [](const ArmResult& r) { return r.j; });
    const double sl_fc = mean(0, [](const ArmResult& r) { return r.sl; }), sl_b = mean(1, [](const ArmResult& r) { return r.sl; }),
                 sl_o = mean(2, [](const ArmResult& r) { return r.sl; });
    const double ab_fc = mean(0, [](const ArmResult& r) { return r.abandon; }), ab_b = mean(1, [](const ArmResult& r) { return r.abandon; }),
                 ab_o = mean(2, [](const ArmResult& r) { return r.abandon; });
    fc::PredError e;
    uint64_t early = 0, viol = 0, errors = 0;
    for (const auto& r : res[c][0]) {
      e.add(r.err);
      early += r.early;
    }
    for (size_t a = 0; a < arms.size(); ++a) {
      for (const auto& r : res[c][a]) {
        viol += r.violations;
        errors += r.errors;
      }
    }
    // Detection over the reps: the worst (latest) and the largest flapping count.
    Detect d;
    bool any_detect = true;
    double worst_expected = -1, worst_recover = 0;
    bool recovered = true;
    std::string first_reason = "none";
    double first_s = -1, drove = 0;
    for (const auto& r : res[c][0]) {
      const Detect x = detect(cs, r);
      if (first_s < 0 || (x.first_s >= 0 && x.first_s < first_s)) {
        first_s = x.first_s;
        first_reason = x.first_reason;
      }
      if (x.expected_s < 0) any_detect = false;
      worst_expected = std::max(worst_expected, x.expected_s);
      d.max_entries = std::max(d.max_entries, x.max_entries);
      if (x.recover_s < 0) recovered = false;
      worst_recover = std::max(worst_recover, x.recover_s);
      drove += x.drove_before / static_cast<double>(reps);
    }
    const bool need_detect = ex.contains("detect_within_s");
    const bool detect_ok = !need_detect || (any_detect && worst_expected <= ex["detect_within_s"].get<double>());
    const bool flap_ok = d.max_entries <= ex.value("max_fallback_entries", uint64_t{3});
    const double tol = ex.value("kpi_tolerance", 0.05);
    const bool kpi_ok = j_fc <= j_b * (1 + tol) + 0.002;
    const bool need_recover = ex.value("recover", false);
    const bool recover_ok = !need_recover || (recovered && worst_recover <= ex.value("recover_within_s", 1800.0));
    const bool pass = detect_ok && flap_ok && kpi_ok && recover_ok && viol == 0;
    passed += pass;
    auto w = [&](const fc::WapeSum& x) { return x.act > 0 ? x.wape() : -1.0; };
    const double w1m = w(e.m1[static_cast<size_t>(fc::Src::kModel)]), w1u = w(e.m1[static_cast<size_t>(fc::Src::kInUse)]),
                 w1b = w(e.m1[static_cast<size_t>(fc::Src::kBaseline)]);
    const double w15m = w(e.m15[static_cast<size_t>(fc::Src::kModel)]), w15u = w(e.m15[static_cast<size_t>(fc::Src::kInUse)]),
                 w15b = w(e.m15[static_cast<size_t>(fc::Src::kBaseline)]);
    std::string trig;
    for (const auto& t : ex.value("triggers", json::array())) trig += (trig.empty() ? "" : ", ") + t.get<std::string>();
    char row[1400];
    std::snprintf(row, sizeof row,
                  "| %s | %s | %s (%s) | %s | %s | %llu | %s | %.0f %% | %.3f / %.3f / %.3f | %.3f / %.3f / %.3f | %.3f / %.3f / %.3f | "
                  "%.3f / %.3f / %.3f | %.4f / %.4f / %.4f | %llu | %s |\n",
                  cs.name.c_str(), trig.empty() ? "-" : trig.c_str(), first_reason.c_str(),
                  first_s >= 0 ? std::to_string(static_cast<int>(first_s)).c_str() : "-",
                  need_detect ? std::to_string(static_cast<int>(ex["detect_within_s"].get<double>())).c_str() : "-",
                  need_detect ? (detect_ok ? (std::to_string(static_cast<int>(worst_expected)) + " s ok").c_str() : "MISSED") : "n/a",
                  static_cast<unsigned long long>(d.max_entries),
                  need_recover ? (recover_ok ? (std::to_string(static_cast<int>(worst_recover)) + " ok").c_str() : "NO") : "n/a",
                  drove * 100, w1m, w1u, w1b, w15m, w15u, w15b, sl_fc, sl_b, sl_o, ab_fc, ab_b, ab_o, j_fc, j_b, j_o,
                  static_cast<unsigned long long>(early), pass ? "PASS" : "FAIL");
    md += row;
    std::printf("%s", row);
    std::fflush(stdout);
    json arms_j = json::object();
    for (size_t a = 0; a < arms.size(); ++a) {
      json reps_j = json::array();
      for (const auto& r : res[c][a]) {
        reps_j.push_back({{"sl", r.sl}, {"abandon", r.abandon}, {"j", r.j}, {"offered", r.offered}, {"redials", r.redials},
                          {"bots", r.bots}, {"violations", r.violations}, {"engine_errors", r.errors}, {"wall_s", r.wall_s},
                          {"early_snapshots", r.early}, {"inferences", r.inferences}, {"lever_plans", r.plans},
                          {"lever_plans_exact", r.exact_plans}, {"lever_changes", r.changes}});
      }
      arms_j[arms[a]] = reps_j;
    }
    all[cs.name] = {{"arms", arms_j}, {"first_reason", first_reason}, {"first_s", first_s}, {"expected_s", worst_expected},
                    {"detect_ok", detect_ok}, {"max_fallback_entries", d.max_entries}, {"recover_s", worst_recover},
                    {"recover_ok", recover_ok}, {"drove_before_onset", drove}, {"kpi_ok", kpi_ok}, {"pass", pass},
                    {"wape_1min", {{"model", w1m}, {"in_use", w1u}, {"baseline", w1b}}},
                    {"wape_15min", {{"model", w15m}, {"in_use", w15u}, {"baseline", w15b}}},
                    {"invariant_violations", viol}, {"engine_errors", errors}};
  }
  const double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  all["wall_s"] = wall;
  all["reps"] = reps;
  all["passed"] = passed;
  all["scenarios"] = cases.size();
  std::ofstream(out_dir + "/forecast-ood.json") << all.dump(2) << "\n";
  std::ofstream(out_dir + "/forecast-ood.md") << "# Forecaster OOD suite\n\n" << md << "\n" << passed << "/" << cases.size()
                                              << " pass; wall " << static_cast<int>(wall) << " s\n";
  std::printf("%d/%zu pass; wall %.0f s\n", passed, cases.size(), wall);
  return 0;
}
