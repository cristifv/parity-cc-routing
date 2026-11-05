// parity-cc-routing: the routing engine (the routing-engine role).
// --ha=off (phase 1's mode): single instance, state in memory; a restart loses every queue entry,
// offer, reservation and agent state: restart interaction-engine and parity-gateway with it.
// --ha=on (phase 6, PLAN §5.5): a leader and standbys per bin through etcd; every change is in
// re.state (Redpanda) before its effect; a standby or a restarted instance resumes from the exact
// state. Stepping down exits with code 3 (the supervisor restarts it as a standby).
#include <cmath>
#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "src/core/engine.h"
#include "src/events/kafka_events.h"
#ifdef PCR_WITH_FORECASTER  // phases 5b+5c: only with --define=forecaster=on (BUILD.bazel)
#include "src/forecast/model_file.h"
#include "src/forecast/source.h"
#include "src/forecast/torch_net.h"
#endif
#include "src/ha/node.h"
#include "src/opt/optimizer.h"
#include "src/ops/http.h"
#include "src/ops/log.h"
#include "src/ops/metrics.h"
#include "src/ops/seed.h"
#include "src/ops/state.h"
#include "src/server/hub.h"
#include "src/server/services.h"
#include "src/server/test_control.h"
#include "src/store/pg_store.h"

namespace {

void usage() {
  std::fprintf(stderr,
               "usage: parity-cc-routing --env=<local-dev|local-prodlike|...> --bin=<bin> "
               "--state-dir=<dir> --seed=<seed.json>\n"
               "  [--listen=127.0.0.1:9800] [--ops-listen=127.0.0.1:9810] [--shards=N]\n"
               "  [--model-ewt=off|on (phase 2: the CTMC's EWT in QueueEntry/PositionUpdate, with its quality; default off)]\n"
               "  [--optimizer=off|on (phase 5: baseline forecast, per-agent CTMC, levers: (a) weights and delays and\n"
               "   (b) min/max agents in config.routing mode auto (exact results only), (c) staffing advice;\n"
               "   parity.staffing/queue.events/parity.forecast; no automatic queue moves; default off)]\n"
               "  [--forecaster=off|on (phases 5b+5c: the learned short-term forecaster feeding the CTMC; needs\n"
               "   --optimizer=on and --forecast-model-dir=<dir with base.pt + base.json>; [--forecast-model-sha256=<hex>\n"
               "   pins the base model], [--forecast-aht=off|on (its handle time, opt-in)], [--forecast-shadow-days=3]; tenant\n"
               "   models come from routing_forecast_model with --pg-conninfo-file; default off)]\n"
               "  [--pg-conninfo-file=<file> (phase 3: libpq conninfo for callbacks, voicemail tasks, bucket actuals;\n"
               "   without it callbacks and voicemail tasks are refused)] [--kafka-brokers=<host:port> (agent.events)]\n"
               "  (loopback by default: phase 1 has no mTLS; widen only with an explicit address)\n"
               "  [--health-dir=/tmp] [--test-control=127.0.0.1:9811 (local-* only; manual clock)]\n"
               "  [--first-start=yes (only to initialise an empty state dir, or an empty re.state for the bin: a new deployment)]\n"
               "  [--ha=off|on (phase 6: leader + standby through etcd, state in re.state; needs --etcd and --kafka-brokers;\n"
               "   --state-dir is then unused)] [--etcd=<host:port>] [--state-topic=re.state] [--advertise=<host:port of\n"
               "   this instance's gRPC, the leader key's value; default --listen>] [--lease-ttl-s=5]\n"
               "  [--instance=<this process's name in every log record; default the host name>]\n"
               "  [--log-level=trace|debug|info|warn|error (default info)] [--log-file=<path: log-record.v1 there, not stderr>]\n");
}

}  // namespace

int main(int argc, char** argv) {
  using namespace pcr;
  // Signals go to sigwait below only (no handler runs on another thread).
  sigset_t sigs;
  sigemptyset(&sigs);
  sigaddset(&sigs, SIGTERM);
  sigaddset(&sigs, SIGINT);
  pthread_sigmask(SIG_BLOCK, &sigs, nullptr);
  signal(SIGPIPE, SIG_IGN);

  // CR-058: loopback unless the operator names another address (phase 1: no authentication).
  std::map<std::string, std::string> f = {{"listen", "127.0.0.1:9800"}, {"ops-listen", "127.0.0.1:9810"},
                                          {"health-dir", "/tmp"}, {"shards", ""}, {"model-ewt", "off"}, {"optimizer", "off"},
                                          {"forecaster", "off"}, {"forecast-aht", "off"}, {"forecast-shadow-days", "3"},
                                          {"ha", "off"}, {"state-topic", "re.state"}, {"lease-ttl-s", "5"}};
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--", 0) != 0 || a.find('=') == std::string::npos) {
      usage();
      return 2;
    }
    f[a.substr(2, a.find('=') - 2)] = a.substr(a.find('=') + 1);
  }
  // Phase 8: log-record.v1 (parity-contracts 0.13.0). --instance names this process in every record
  // (default: the host name); --log-level (default info); --log-file writes the records there.
  log_init(LogInit{f["instance"], "", "", f["log-level"].empty() ? "info" : f["log-level"], f["log-file"]});
  LogRec("info", "service.starting", "parity-cc-routing starting").str("bin", f["bin"]).attr("env", f["env"]);
  if (f["ha"] != "on" && f["ha"] != "off") {
    usage();
    return 2;
  }
  const bool ha = f["ha"] == "on";
  for (const char* k : {"env", "bin", "state-dir"}) {
    if (f[k].empty() && !(ha && std::string(k) == "state-dir")) {
      usage();
      return 2;
    }
  }
  if (ha && (f["etcd"].empty() || f["kafka-brokers"].empty())) {
    LogRec("error", "startup.refused", "--ha=on needs --etcd and --kafka-brokers").error("invalid_request");
    return 2;
  }
  const std::string env = f["env"];
  const bool local = env.rfind("local-", 0) == 0;
  const bool test = !f["test-control"].empty();
  if (test && !local) {
    LogRec("error", "startup.refused", "--test-control is refused outside local-* environments").error("invalid_request").attr("env", env);
    return 2;
  }
  if (test && ha) {
    LogRec("error", "startup.refused", "--test-control runs only with --ha=off (one instance, a manual clock)").error("invalid_request");
    return 2;
  }
  // CR-096 is closed by parity-contracts 0.11.0: QueueEntry and PositionUpdate carry the EWT's
  // quality (MODEL, APPROX, LONG), so IE can tell a rough figure from a validated one.
  if (!local) {
    // local-dev and local-prodlike only until the BEFORE DEPLOY rows are closed (mTLS to IE and the
    // gateway CR-131, Kafka TLS CR-110, etcd authentication CR-133, callback numbers CR-107).
    LogRec("error", "startup.refused", "parity-cc-routing runs only in local-* environments").error("invalid_request").attr("env", env);
    return 2;
  }
  if (f["seed"].empty() && !test) {
    usage();
    return 2;
  }
  const std::string healthy = f["health-dir"] + "/parity-cc-routing.healthy";
  const std::string drained = f["health-dir"] + "/parity-cc-routing.drained";
  remove_file(healthy);
  remove_file(drained);

  std::vector<TenantConfig> tenants;
  std::vector<std::pair<std::string, TenantConfig>> all_tenants;  // --ha=on: every tenant, with its static bin
  if (!f["seed"].empty()) {
    std::ifstream in(f["seed"]);
    std::stringstream ss;
    ss << in.rdbuf();
    std::string error;
    if (!in || !parse_seed(ss.str(), f["bin"], &tenants, &error) || (ha && !parse_seed_all(ss.str(), &all_tenants, &error))) {
      LogRec("error", "config.refused", "bad seed").error("invalid_request", in ? error : std::string("unreadable")).attr("file", f["seed"]);
      return 2;
    }
  }
  std::string error;
  if (f["first-start"] != "" && f["first-start"] != "yes" && f["first-start"] != "no") {
    usage();
    return 2;
  }
  uint64_t incarnation = 0;
  if (!ha) {
    if (!lock_state_dir(f["state-dir"], &error)) {
      LogRec("error", "startup.refused", "the state dir is refused").error("invalid_request", error).attr("path", f["state-dir"]);
      return 2;
    }
    incarnation = bump_incarnation(f["state-dir"], f["first-start"] == "yes", &error);
    if (incarnation == 0) {
      LogRec("error", "startup.refused", "the state dir is refused").error("invalid_request", error).attr("path", f["state-dir"]);
      return 2;
    }
  }
  // --ha=on: the epochs come with the takeover (leader_epoch from etcd, the incarnation from re.state).
  const Epochs epochs = ha ? Epochs{0, 0} : Epochs{incarnation, (incarnation - 1) << 32};

  int shards = static_cast<int>(std::max(2u, std::thread::hardware_concurrency() / 2));
  if (!f["shards"].empty()) shards = std::max(1, std::stoi(f["shards"]));

  SystemClock system_clock;
  std::unique_ptr<ManualClock> manual;
  const Clock* clock = &system_clock;
  if (test) {
    manual = std::make_unique<ManualClock>(system_clock.now_ms());
    clock = manual.get();
  }

  Metrics metrics;
  // The hub needs the engine's epochs; the engine needs the hub as its sink.
  struct Late final : Sink {
    Hub* hub = nullptr;
    void to_ie(const std::string& r, uint64_t g, const rv1::WatchResponse& m) override { hub->to_ie(r, g, m); }
    void to_gw(const std::string& r, uint64_t g, const av1::WatchResponse& m) override { hub->to_gw(r, g, m); }
    bool ie_live(const std::string& r) const override { return hub->ie_live(r); }
    std::vector<std::string> ie_replicas() const override { return hub->ie_replicas(); }
  } late;
  // Phase 2 (PLAN §3.4): the model's EWT in QueueEntry/PositionUpdate, off by default. When on, the
  // optimization layer's 2 threads take each tenant's snapshot every 10 s and post plans back.
  if (f["model-ewt"] != "on" && f["model-ewt"] != "off") {
    usage();
    return 2;
  }
  const bool model_ewt = f["model-ewt"] == "on";
  if (f["optimizer"] != "on" && f["optimizer"] != "off") {
    usage();
    return 2;
  }
  const bool staffing = f["optimizer"] == "on";
  // Phases 5b+5c (PLAN §5.9): the learned forecaster, a ForecastSource of the optimizer (the CTMC
  // stays the only decision maker; the baseline is always computed and is the fallback).
  if ((f["forecaster"] != "on" && f["forecaster"] != "off") || (f["forecast-aht"] != "on" && f["forecast-aht"] != "off")) {
    usage();
    return 2;
  }
#ifdef PCR_WITH_FORECASTER
  std::unique_ptr<fc::LearnedSource> learned;
  struct PgModelStore final : fc::ModelStore {
    PgStore* s = nullptr;
    void set_status(const std::string& tenant, uint64_t version, const std::string& status, const nlohmann::json& shadow,
                    int64_t at_ms) override {
      if (s) s->set_model_status(tenant, version, status, shadow.empty() ? std::string() : shadow.dump(), at_ms);
    }
  } model_store;
  if (f["forecaster"] == "on") {
    if (!staffing || f["forecast-model-dir"].empty()) {
      LogRec("error", "startup.refused", "--forecaster=on needs --optimizer=on and --forecast-model-dir").error("invalid_request");
      return 2;
    }
    fc::torch_threads(1, 1);  // one intra-op thread per inference (PLAN §5.9)
    std::string why;
    auto base = fc::load_base_spec(f["forecast-model-dir"], f["forecast-model-sha256"], &why);
    if (!base) {
      LogRec("error", "model.refused", "the base forecaster model was refused").error("failed_precondition", why)
          .attr("version", int64_t{0}).attr("reason", why);
      return 2;
    }
    fc::SourceOptions so;
    so.owns_aht = f["forecast-aht"] == "on";
    try {
      so.shadow_ms = static_cast<int64_t>(std::stod(f["forecast-shadow-days"]) * 86'400'000);
    } catch (const std::exception&) {
      so.shadow_ms = -1;
    }
    if (so.shadow_ms <= 0) {
      LogRec("error", "startup.refused", "--forecast-shadow-days must be > 0").error("invalid_request");
      return 2;
    }
    LogRec("info", "forecast.model_loaded", "base forecaster model loaded").attr("version", int64_t{0})
        .attr("sha256", base->sha256).attr("parameters", static_cast<int64_t>(base->net->parameters()));
    learned = std::make_unique<fc::LearnedSource>(so, std::move(base), &model_store);
  }
#else
  if (f["forecaster"] == "on") {
    LogRec("error", "startup.refused", "--forecaster=on: this binary was built without the forecaster (--define=forecaster=on)")
        .error("invalid_request");
    return 2;
  }
#endif
  std::unique_ptr<Optimizer> optimizer;
  Engine::Limits limits;
  limits.model_ewt = model_ewt;
  limits.optimizer = staffing;
  Engine::ModelFeed feed;
  if (model_ewt || staffing) {
    Optimizer::Options oo;
    oo.levers = staffing;
#ifdef PCR_WITH_FORECASTER
    oo.learned = learned.get();
#endif
    optimizer = std::make_unique<Optimizer>(oo);
    feed = [opt = optimizer.get()](std::shared_ptr<const ModelSnapshot> s) { opt->submit(std::move(s)); };
  }
  Engine engine(clock, &late, shards, limits, feed);
  if (optimizer) optimizer->attach(&engine);
  Hub hub(&engine.epochs());
  late.hub = &hub;
  // Phase 3: Postgres (callbacks, voicemail tasks, routing_bucket_actuals) and agent.events. The
  // conninfo is a secret: read from its file, never logged.
  std::unique_ptr<PgStore> store;
  if (!f["pg-conninfo-file"].empty()) {
    std::ifstream in(f["pg-conninfo-file"]);
    std::stringstream ss;
    ss << in.rdbuf();
    std::string conninfo = ss.str();
    while (!conninfo.empty() && (conninfo.back() == '\n' || conninfo.back() == '\r')) conninfo.pop_back();
    if (!in || conninfo.empty()) {
      LogRec("error", "startup.refused", "--pg-conninfo-file is unreadable or empty").error("invalid_request");
      return 2;
    }
    PgStore::Options po;
    po.conninfo = std::move(conninfo);
    for (const auto& t : tenants) po.tenants.push_back(t.id);
    po.fenced = ha;  // phase 6: fenced writes, callbacks loaded per tenant as it is acquired
    po.offered_10s = f["forecaster"] == "on";  // phases 5b+5c: db/V3's column, only with the forecaster
    store = std::make_unique<PgStore>(std::move(po), &engine, clock);
  }
  std::unique_ptr<HaNode> node;
  if (ha) {
    const std::string advertise = f["advertise"].empty() ? f["listen"] : f["advertise"];
    int ttl = 5;
    try {
      ttl = std::stoi(f["lease-ttl-s"]);
    } catch (const std::exception&) {
      ttl = 0;
    }
    if (ttl < 3 || ttl > 30) {
      LogRec("error", "startup.refused", "--lease-ttl-s must be 3-30").error("invalid_request");
      return 2;
    }
    node = std::make_unique<HaNode>(HaNode::Options{f["bin"], advertise, f["etcd"], f["kafka-brokers"], f["state-topic"], ttl,
                                                    f["first-start"] == "yes"},
                                    &engine, store.get(), all_tenants, &metrics);
    engine.set_state_log(node->log());
  }
  std::unique_ptr<KafkaEvents> events;
  if (!f["kafka-brokers"].empty()) {
    events = std::make_unique<KafkaEvents>(KafkaEvents::Options{f["kafka-brokers"]});
    std::string kerr;
    if (!events->start(&kerr)) {
      LogRec("error", "dependency.unavailable", "the agent.events producer can't start").error("unavailable", kerr).attr("dependency", "kafka");
      return 2;
    }
  }
  engine.set_durable(store.get(), events.get());
  if (optimizer && staffing) {
    // Phase 5: staffing events straight from the optimization threads (not state: no commit gate);
    // the forecaster's profile from routing_bucket_actuals, read once per tenant (best effort).
    optimizer->attach_events(events.get());
    if (store) {
      optimizer->set_history_loader([s = store.get(), o = optimizer.get()](const std::string& tenant, int64_t since) {
        return s->load_actuals(tenant, since, [o, tenant](std::vector<BucketRow> rows) { o->add_actuals(tenant, std::move(rows)); });
      });
    }
  }
#ifdef PCR_WITH_FORECASTER
  // Phases 5b+5c: the tenants' fine-tuned models, polled every 5 minutes (only rows the trainer's
  // role wrote; sha256 and config checked before the bytes are parsed; refusals counted and logged).
  model_store.s = store.get();
  std::atomic<uint64_t> models_refused{0};
  std::atomic<bool> stop_models{false};
  std::thread model_poll;
  if (learned && store) {
    std::vector<std::string> ids;
    for (const auto& t : tenants) ids.push_back(t.id);
    for (const auto& [bin, t] : all_tenants) {
      if (bin == f["bin"] && std::find(ids.begin(), ids.end(), t.id) == ids.end()) ids.push_back(t.id);
    }
    model_poll = std::thread([&, ids] {
      while (!stop_models.load()) {
        for (const auto& tenant : ids) {
          store->load_models(tenant, [&, tenant](std::vector<PgStore::ModelRow> rows) {
            std::shared_ptr<const fc::ModelSpec> active, candidate;
            for (const auto& r : rows) {
              auto& slot = r.status == "active" ? active : candidate;
              if (slot) continue;
              std::string why;
              if (r.written_by != "parity_cc_routing_trainer") why = "written_by";
              std::shared_ptr<const fc::ModelSpec> spec;
              if (why.empty()) {
                spec = fc::spec_from_row(r.version, r.sha256, r.bytes, nlohmann::json::parse(r.config, nullptr, false),
                                         nlohmann::json::parse(r.normalization, nullptr, false), r.created_at_ms, &why);
              }
              if (!spec) {
                ++models_refused;
                LogRec("warn", "model.refused", "a forecaster model row was refused").tenant(tenant)
                    .error("failed_precondition", why).attr("version", r.version).attr("reason", why);
                continue;
              }
              slot = std::move(spec);
            }
            const auto have = learned->versions(tenant);
            const uint64_t av = active ? active->version : 0, cv = candidate ? candidate->version : 0;
            if (have.first == av && have.second == cv) return;
            if (active) {
              LogRec("info", "forecast.model_loaded", "tenant forecaster model loaded").tenant(tenant)
                  .attr("version", av).attr("sha256", active->sha256);
            }
            learned->set_models(tenant, std::move(active), std::move(candidate));
          });
        }
        for (int i = 0; i < 300 && !stop_models.load(); ++i) std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    });
  }
#endif
  engine.load(ha ? std::vector<TenantConfig>{} : tenants, epochs);
  if (store) store->start();  // --ha=off: loads the open callbacks into their tenants once connected

  metrics.computed("parity_cc_routing_incarnation", "gauge", "The incarnation (--ha=off: = leader_epoch; --ha=on: the bin's, from re.state)",
                   [&] { return static_cast<double>(node ? node->incarnation() : engine.epochs().leader_epoch); });
  auto total = [&](const char* name, const char* help, auto pick) {
    metrics.computed(name, "counter", help, [&engine, pick] { return static_cast<double>(pick(engine.totals())); });
  };
  total("parity_cc_routing_offers_total", "Offers made", [](const Engine::Totals& t) { return t.offers; });
  total("parity_cc_routing_offers_accepted_total", "Offers accepted", [](const Engine::Totals& t) { return t.accepted; });
  total("parity_cc_routing_offers_rejected_total", "Offers rejected", [](const Engine::Totals& t) { return t.rejected; });
  total("parity_cc_routing_offers_expired_total", "Offers expired", [](const Engine::Totals& t) { return t.expired; });
  total("parity_cc_routing_overflows_total", "Max-wait actions", [](const Engine::Totals& t) { return t.overflows; });
  total("parity_cc_routing_sweep_matches_total", "Dispatches the 1 s sweep had to make (a bug)", [](const Engine::Totals& t) { return t.sweep_matches; });
  total("parity_cc_routing_assignments_expired_total", "L7 expiries", [](const Engine::Totals& t) { return t.assign_expired; });
  total("parity_cc_routing_redeliveries_total", "Watch redeliveries", [](const Engine::Totals& t) { return t.redeliveries; });
  total("parity_cc_routing_owner_lost_total", "Interactions cleaned up because their IE owner was gone (CR-083)", [](const Engine::Totals& t) { return t.owner_lost; });
  total("parity_cc_routing_inbox_refused_total", "Requests refused UNAVAILABLE by the inbox limits", [](const Engine::Totals& t) { return t.inbox_refused; });
  total("parity_cc_routing_ewt_model_total", "expected_wait_s answers from the model's plan (--model-ewt)", [](const Engine::Totals& t) { return t.ewt_model; });
  total("parity_cc_routing_ewt_heuristic_total", "expected_wait_s answers from phase 1's estimate", [](const Engine::Totals& t) { return t.ewt_heuristic; });
  total("parity_cc_routing_model_plans_installed_total", "EWT plans the writers installed", [](const Engine::Totals& t) { return t.plans_installed; });
  total("parity_cc_routing_model_plans_refused_total", "EWT plans refused (stale or another tenant's)", [](const Engine::Totals& t) { return t.plans_refused; });
  metrics.computed("parity_cc_routing_offer_decision_max_seconds", "gauge", "Longest offer decision since start (I9)",
                   [&] { return engine.totals().decision_max_us / 1e6; });
  // Phase 7: what holds RPCs before their writer task starts (I9 starts at the task), and the 1 s
  // periodic work's duration (it holds the writer).
  metrics.histogram("parity_cc_routing_writer_inbox_wait_seconds", "Time a task waited in its shard's inbox before the writer ran it", [&] {
    const Engine::Totals t = engine.totals();
    Metrics::Hist h;
    for (int b = 0; b < 32; ++b) {
      h.count += t.inbox_wait_us_buckets[b];
      h.buckets.emplace_back(std::ldexp(1.0, b) / 1e6, h.count);
    }
    h.sum = t.inbox_wait_us_sum / 1e6;
    return h;
  });
  metrics.computed("parity_cc_routing_writer_inbox_wait_max_seconds", "gauge", "Longest inbox wait since start",
                   [&] { return engine.totals().inbox_wait_us_max / 1e6; });
  metrics.computed("parity_cc_routing_writer_periodic_max_seconds", "gauge", "Longest 1 s periodic tick (sweep, redelivery, orphans) since start",
                   [&] { return engine.totals().periodic_us_max / 1e6; });
  metrics.computed("parity_cc_routing_writer_periodic_seconds_sum", "counter", "Time spent in the 1 s periodic ticks",
                   [&] { return engine.totals().periodic_us_sum / 1e6; });
  metrics.computed("parity_cc_routing_writer_periodics_total", "counter", "Periodic ticks run",
                   [&] { return static_cast<double>(engine.totals().periodics); });
  // Phase 7: I9's distribution (log2 microsecond buckets: bucket b holds decisions < 2^b us).
  metrics.histogram("parity_cc_routing_offer_decision_seconds",
                    "Offer decision latency inside the writer (I9: the task that made the entry offerable or the agent ready)", [&] {
                      const Engine::Totals t = engine.totals();
                      Metrics::Hist h;
                      for (int b = 0; b < 32; ++b) {
                        h.count += t.decision_us_buckets[b];
                        h.buckets.emplace_back(std::ldexp(1.0, b) / 1e6, h.count);
                      }
                      h.sum = t.decision_us_sum / 1e6;
                      return h;
                    });
  if (optimizer) {
    auto opt_total = [&](const char* name, const char* help, auto pick) {
      metrics.computed(name, "counter", help, [o = optimizer.get(), pick] { return static_cast<double>(pick(o->totals())); });
    };
    opt_total("parity_cc_routing_model_solves_total", "Tenant model solves", [](const Optimizer::Totals& t) { return t.plans; });
    opt_total("parity_cc_routing_model_refused_total", "Model results refused at the boundary (NaN/inf, invalid)", [](const Optimizer::Totals& t) { return t.refused; });
    opt_total("parity_cc_routing_model_clamps_total", "Negative or >1 probabilities from round-off, clamped", [](const Optimizer::Totals& t) { return t.clamps; });
    opt_total("parity_cc_routing_model_approx_total", "Solves whose quality was approx", [](const Optimizer::Totals& t) { return t.approx; });
    opt_total("parity_cc_routing_model_snapshots_dropped_total", "Snapshots dropped (optimizer behind)", [](const Optimizer::Totals& t) { return t.dropped; });
    opt_total("parity_cc_routing_model_solve_seconds_sum", "Time spent solving", [](const Optimizer::Totals& t) { return t.solve_us_sum / 1e6; });
    opt_total("parity_cc_routing_lever_plans_total", "Lever plans posted (phase 5)", [](const Optimizer::Totals& t) { return t.lever_plans; });
    opt_total("parity_cc_routing_lever_refused_total", "Lever ticks the model refused (the last plan stays)", [](const Optimizer::Totals& t) { return t.lever_refused; });
    opt_total("parity_cc_routing_lever_changes_total", "Queue weight or delay changes the optimizer planned", [](const Optimizer::Totals& t) { return t.lever_changes; });
    opt_total("parity_cc_routing_lever_held_total", "Lever ticks whose best change was held (hysteresis, not confirmed)", [](const Optimizer::Totals& t) { return t.held; });
    opt_total("parity_cc_routing_lever_capped_total", "Lever searches stopped by the candidate budget or the time cap", [](const Optimizer::Totals& t) { return t.capped; });
    opt_total("parity_cc_routing_lever_seconds_sum", "Time spent in lever ticks", [](const Optimizer::Totals& t) { return t.lever_us_sum / 1e6; });
    opt_total("parity_cc_routing_staffing_events_total", "parity.staffing, queue.events and parity.forecast events handed to the producer", [](const Optimizer::Totals& t) { return t.events; });
#ifdef PCR_WITH_FORECASTER
    opt_total("parity_cc_routing_forecast_model_used_total", "Queue forecasts the learned model drove (phases 5b+5c)", [](const Optimizer::Totals& t) { return t.learned_used; });
    opt_total("parity_cc_routing_forecast_baseline_used_total", "Queue forecasts where the baseline stood with the learned forecaster on", [](const Optimizer::Totals& t) { return t.learned_fallback; });
#endif
  }
#ifdef PCR_WITH_FORECASTER
  if (learned) {
    auto fc_total = [&](const std::string& name, const char* type, const std::string& help, auto pick) {
      metrics.computed(name, type, help, [l = learned.get(), pick] { return static_cast<double>(pick(l->totals())); });
    };
    fc_total("parity_cc_routing_forecast_inferences_total", "counter", "Learned forecaster runs (one per tenant per tick or deviation)", [](const fc::LearnedSource::Totals& t) { return t.inferences; });
    fc_total("parity_cc_routing_forecast_inference_seconds_sum", "counter", "Time in learned forecaster runs", [](const fc::LearnedSource::Totals& t) { return t.us_sum / 1e6; });
    fc_total("parity_cc_routing_forecast_inference_max_seconds", "gauge", "Longest learned forecaster run", [](const fc::LearnedSource::Totals& t) { return t.us_max / 1e6; });
    fc_total("parity_cc_routing_forecast_shadow_runs_total", "counter", "Candidate models run in the shadow", [](const fc::LearnedSource::Totals& t) { return t.shadow_runs; });
    fc_total("parity_cc_routing_forecast_promoted_total", "counter", "Candidates promoted", [](const fc::LearnedSource::Totals& t) { return t.promoted; });
    fc_total("parity_cc_routing_forecast_rejected_total", "counter", "Candidates rejected", [](const fc::LearnedSource::Totals& t) { return t.rejected; });
    fc_total("parity_cc_routing_forecast_demoted_total", "counter", "Models demoted (worse than the baseline over 24 h)", [](const fc::LearnedSource::Totals& t) { return t.demoted; });
    metrics.computed("parity_cc_routing_forecast_models_refused_total", "counter", "Model rows refused (written_by, sha256, config)",
                     [&] { return static_cast<double>(models_refused.load()); });
    for (int r = 1; r < fc::kReasons; ++r) {
      const std::string rn = fc::reason_name(static_cast<fc::Reason>(r));
      fc_total("parity_cc_routing_forecast_fallback_" + rn + "_total", "counter", "Entries into the baseline fallback, reason " + rn,
               [r](const fc::LearnedSource::Totals& t) { return t.fallback_entries[static_cast<size_t>(r)]; });
    }
    for (int k = 0; k < fc::kSrcs; ++k) {
      const std::string sn = fc::src_name(static_cast<fc::Src>(k));
      fc_total("parity_cc_routing_forecast_wape_1min_" + sn, "gauge", "Arrivals WAPE of H1's first 15 minutes, source " + sn,
               [k](const fc::LearnedSource::Totals& t) { return t.err.m1[static_cast<size_t>(k)].wape(); });
      fc_total("parity_cc_routing_forecast_wape_15min_" + sn, "gauge", "Arrivals WAPE of the running bucket, source " + sn,
               [k](const fc::LearnedSource::Totals& t) { return t.err.m15[static_cast<size_t>(k)].wape(); });
    }
    total("parity_cc_routing_forecast_early_snapshots_total", "Snapshots handed over early on an arrival-guard breach (the event-driven re-forecast)", [](const Engine::Totals& t) { return t.early_snapshots; });
  }
#endif
  total("parity_cc_routing_moves_applied_total", "Active-set changes applied (phase 4)", [](const Engine::Totals& t) { return t.moves_applied; });
  total("parity_cc_routing_moves_pending_total", "Active-set changes held for the next offer", [](const Engine::Totals& t) { return t.moves_pending; });
  total("parity_cc_routing_moves_refused_total", "Moves refused (mode, pinned, excluded, dwell, hourly cap, change rate)", [](const Engine::Totals& t) { return t.moves_refused; });
  total("parity_cc_routing_moves_stale_total", "Moves dropped by the writer's re-check (CR-021)", [](const Engine::Totals& t) { return t.moves_stale; });
  total("parity_cc_routing_coverage_refused_total", "Changes refused because a queue would be left uncovered", [](const Engine::Totals& t) { return t.coverage_refused; });
  total("parity_cc_routing_foreign_refused_total", "Plans, pins or policies naming another tenant's agent or queue (refused)", [](const Engine::Totals& t) { return t.foreign_refused; });
  total("parity_cc_routing_config_refused_total", "Stale or invalid config.routing / config.routing_pin documents", [](const Engine::Totals& t) { return t.config_refused; });
  total("parity_cc_routing_forced_states_total", "Supervisor forced states (ForceState)", [](const Engine::Totals& t) { return t.forced_states; });
  total("parity_cc_routing_callbacks_created_total", "Callbacks made durable (ConvertToCallback)", [](const Engine::Totals& t) { return t.callbacks_created; });
  total("parity_cc_routing_callbacks_dispatched_total", "RunCallbacks sent (first time per attempt)", [](const Engine::Totals& t) { return t.callbacks_dispatched; });
  total("routing_engine_callbacks_flagged_total", "Callbacks expired or unknown: never redialled automatically (CONVENTIONS)", [](const Engine::Totals& t) { return t.callbacks_flagged; });
  total("parity_cc_routing_callbacks_refused_total", "Callback and voicemail-task writes Postgres couldn't take", [](const Engine::Totals& t) { return t.callbacks_refused; });
  total("parity_cc_routing_voicemail_tasks_total", "Voicemail tasks made durable", [](const Engine::Totals& t) { return t.voicemail_tasks; });
  total("parity_cc_routing_direct_offers_total", "Direct-agent entries", [](const Engine::Totals& t) { return t.direct_offers; });
  total("parity_cc_routing_consults_total", "Consult entries", [](const Engine::Totals& t) { return t.consults; });
  total("parity_cc_routing_levers_installed_total", "Lever plans the writers installed", [](const Engine::Totals& t) { return t.levers_installed; });
  total("parity_cc_routing_levers_refused_total", "Lever plans refused (stale, another tenant's)", [](const Engine::Totals& t) { return t.levers_refused; });
  total("parity_cc_routing_levers_applied_total", "Queue weights or delays the writers changed (auto, exact)", [](const Engine::Totals& t) { return t.levers_applied; });
  total("parity_cc_routing_levers_held_total", "Queue lever changes held (mode off/advise, approx)", [](const Engine::Totals& t) { return t.levers_held; });
  total("parity_cc_routing_stall_stale_total", "Lever plans gone stale (config weights and delays again)", [](const Engine::Totals& t) { return t.stall_stale; });
  total("parity_cc_routing_vip_enqueued_total", "Entries of the VIP class", [](const Engine::Totals& t) { return t.vip_enqueued; });
  total("parity_cc_routing_bounds_min_picks_total", "Lever (b): a READY agent took a queue below its min_agents first", [](const Engine::Totals& t) { return t.bounds_min_picks; });
  total("parity_cc_routing_bounds_max_defers_total", "Lever (b): a queue at its max_agents passed over for other work", [](const Engine::Totals& t) { return t.bounds_max_defers; });
  total("parity_cc_routing_vip_reserve_holds_total", "A VIP-eligible agent kept for VIP callers", [](const Engine::Totals& t) { return t.vip_reserve_holds; });
  total("parity_cc_routing_gw_outbox_coalesced_total", "Superseded QueuesChanged dropped from gateway outboxes (CR-137)", [](const Engine::Totals& t) { return t.gw_outbox_coalesced; });
  total("parity_cc_routing_gw_outbox_dropped_total", "Gateway outbox messages dropped at the cap (CR-137)", [](const Engine::Totals& t) { return t.gw_outbox_dropped; });
  total("parity_cc_routing_records_refused_total", "Tenant changes refused for a record over the size limit (CR-137)", [](const Engine::Totals& t) { return t.records_refused; });
  metrics.computed("parity_cc_routing_tenants_quarantined", "gauge", "Tenants refused until rebuilt from re.state (CR-137)",
                   [&] { return static_cast<double>(engine.totals().tenants_quarantined); });
  if (store) {
    metrics.computed("parity_cc_routing_postgres_up", "gauge", "1 while Postgres is connected", [s = store.get()] { return s->healthy() ? 1.0 : 0.0; });
    metrics.computed("parity_cc_routing_postgres_refused_total", "counter", "Writes refused at once (Postgres down or the queue full)", [s = store.get()] { return static_cast<double>(s->stats().refused); });
    metrics.computed("parity_cc_routing_postgres_failed_total", "counter", "Writes that failed", [s = store.get()] { return static_cast<double>(s->stats().failed); });
  }
  if (events) {
    metrics.computed("parity_cc_routing_events_dropped_total", "counter", "agent.events dropped (buffer full)", [e = events.get()] { return static_cast<double>(e->dropped()); });
    metrics.computed("parity_cc_routing_events_produced_total", "counter", "agent.events handed to the producer", [e = events.get()] { return static_cast<double>(e->produced()); });
  }
  metrics.computed("parity_cc_routing_queue_entries", "gauge", "Waiting entries", [&] { return static_cast<double>(engine.totals().waiting); });
  metrics.computed("parity_cc_routing_ready_agents", "gauge", "Ready, attached agents without an offer", [&] { return static_cast<double>(engine.totals().ready); });
  metrics.computed("parity_cc_routing_watch_streams", "gauge", "Open Watch streams", [&] { return static_cast<double>(hub.streams()); });

  std::atomic<bool> serving{false};
  HttpServer ops;
  if (!ops.start(f["ops-listen"], [&](const std::string&, const std::string& path, const std::string&) {
        if (path == "/livez") return HttpReply{200, "text/plain", "ok\n"};
        // --ha=on: ready = the leader serving (a Service routes to it); a caught-up standby is healthy only.
        if (path == "/readyz") return serving && (!node || node->serving()) ? HttpReply{200, "text/plain", "ready\n"} : HttpReply{503, "text/plain", "not ready\n"};
        if (path == "/metrics") return HttpReply{200, "text/plain; version=0.0.4", metrics.render()};
        return HttpReply{404, "text/plain", "not found\n"};
      }, 0)) {
    return 1;
  }
  std::unique_ptr<TestControl> control;
  HttpServer control_http;
  if (test) {
    control = std::make_unique<TestControl>(&engine, &hub, manual.get(), tenants, epochs, f["bin"]);
    std::string host;
    int port = 0;
    if (!split_addr(f["test-control"], &host, &port) || host != "127.0.0.1" ||
        !control_http.start(f["test-control"], [&](const std::string& m, const std::string& p, const std::string& b) {
          return control->handle(m, p, b);
        }, 4 << 20)) {
      LogRec("error", "startup.refused", "--test-control must be a free 127.0.0.1 port").error("invalid_request").attr("addr", f["test-control"]);
      return 2;
    }
  }

  ServerOptions so{f["listen"], f["bin"], nullptr, nullptr};
  if (node) {
    HaNode* np = node.get();
    so.serving = [np] { return np->serving(); };
    so.leader = [np] { return np->leader(); };
  }
  Server server(so, clock, &engine, &hub, &metrics);
  if (!server.start()) {
    LogRec("error", "startup.refused", "cannot serve gRPC").error("unavailable").attr("addr", f["listen"]);
    return 1;
  }
  if (node) {
    std::string herr;
    if (!node->start(&herr)) {
      LogRec("error", "startup.refused", "HA can't start").error("unavailable", herr).str("bin", f["bin"]);
      return 2;
    }
  }
  std::atomic<bool> stop_ticks{false};
  std::thread ticker([&] {
    // CR-069: what the counters show, also as a line on-call can find (at most once a second;
    // never from a shard writer).
    uint64_t sweep = 0, refused = 0, slow = 0, dropped = 0, lost = 0;
    int n = 0;
    bool healthy_written = !node;  // --ha=off: written below, once
    while (!stop_ticks) {
      if (!test) hub.tick(clock->now_ms());
      // --ha=on: healthy once leading, or as a standby that has caught up with re.state.
      if (!healthy_written && serving && (node->serving() || node->caught_up())) {
        write_file(healthy, "ok\n");
        healthy_written = true;
      }
      if (++n % 4 == 0) {
        const auto t = engine.totals();
        if (t.sweep_matches > sweep) LogRec("warn", "sweep.matched", "the 1 s sweep made offers dispatch missed (a bug)").attr("count", t.sweep_matches - sweep);
        if (t.inbox_refused > refused) LogRec("warn", "inbox.refused", "requests refused UNAVAILABLE by the inbox limits").attr("count", t.inbox_refused - refused);
        if (hub.closed_slow() > slow) LogRec("warn", "stream.closed_slow", "closed slow-consumer Watch streams (they replay on reconnect)").attr("count", hub.closed_slow() - slow);
        if (log_dropped() > dropped) LogRec("warn", "log.dropped", "log records dropped (queue full)").attr("dropped", log_dropped() - dropped);
        if (t.owner_lost > lost) LogRec("warn", "owner.lost_calls", "calls dropped: their IE owner was gone (no stream and no RPC, or nothing acknowledged)").attr("count", t.owner_lost - lost);
        lost = t.owner_lost;
        sweep = t.sweep_matches;
        refused = t.inbox_refused;
        slow = hub.closed_slow();
        dropped = log_dropped();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
  });
  serving = true;
  if (!node) {
    write_file(healthy, "ok\n");
    LogRec("info", "service.ready", "phase-1 RE started: restart interaction-engine and parity-gateway")
        .str("bin", f["bin"]).num("leader_epoch", static_cast<int64_t>(incarnation))
        .attr("incarnation", static_cast<int64_t>(incarnation)).attr("env", env).attr("port", server.port())
        .attr("tenants", static_cast<int64_t>(tenants.size())).attr("shards", static_cast<int64_t>(shards));
  } else {
    LogRec("info", "service.ready", "RE started (HA): following re.state, in the election")
        .str("bin", f["bin"]).attr("env", env).attr("port", server.port())
        .attr("tenants", static_cast<int64_t>(all_tenants.size())).attr("shards", static_cast<int64_t>(shards));
  }

  int sig = 0;
  sigwait(&sigs, &sig);
  LogRec("info", "service.draining", "draining").attr("signal", sig);
  serving = false;
  remove_file(healthy);
  if (node) {
    // Phase 6: no new offers, everything committed and released, then the election resigned so the
    // standby takes over at once.
    node->drain(3000);
  } else {
    engine.set_draining(true);  // no new offers
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));  // let acks in flight land
  server.shutdown(2000);
  stop_ticks = true;
  ticker.join();
  control_http.stop();
  ops.stop();  // its /metrics callbacks read the optimizer's totals
  // Phase 3: the store fails what it still holds (posted to the tenants, which still exist), then
  // the events flush.
#ifdef PCR_WITH_FORECASTER
  stop_models = true;
  if (model_poll.joinable()) model_poll.join();
#endif
  if (store) store->stop();
  if (events) events->stop();
  // CR-099: the optimizer goes before the engine. The shards stop feeding it, its workers stop
  // posting plans, and its threads are joined while the engine they post into still exists.
  engine.stop_model_feed();
  if (optimizer) {
    optimizer->attach(nullptr);
    optimizer.reset();
  }
  write_file(drained, "ok\n");
  LogRec("info", "service.stopped", "drained").attr("exit_code", 0);
  log_flush();
  return 0;
}
