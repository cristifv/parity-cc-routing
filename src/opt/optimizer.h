// The optimization layer (PLAN §5.1, §5.3, §3.4): every model tick each tenant's writer hands over
// an immutable snapshot; an optimization thread estimates arrivals, patience and handle time, runs
// the per-agent CTMC (model/agents.h; phase 5 redesign: no groups) and posts an EWT plan (per queue,
// requirement signature and caller class, by position) back into the tenant's inbox. Phase 5: the
// baseline forecast (or a learned one through ForecastSource), the levers (src/opt/levers.h), a lever
// plan for the writer and parity.staffing / queue.events / parity.forecast. The real-time
// layer never waits: submit() only queues (or drops, counted), plans arrive as ordinary tasks, and
// a missing or stale plan means phase 1's estimate. Tenant isolation: a tenant is pinned to one
// worker, its estimator state lives in that worker only, and a plan carries its tenant's id.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "model/ctmc.h"
#include "src/core/engine.h"
#include "src/core/model_feed.h"
#include "src/opt/forecast.h"
#include "src/opt/levers.h"

namespace pcr {

class Optimizer {
 public:
  struct Options {
    int threads = 2;
    size_t queue_per_thread = 4096;  // snapshots waiting; above it the newest is dropped (counted)
    int64_t tick_ms = 10'000;
    int valid_ticks = 3;              // a plan stays valid for 3 ticks (PLAN §6)
    int max_positions = 1000;         // table size; beyond it the step is added per position
    // Phase 5 (PLAN §5.3): the lever loop (forecast, per-agent CTMC, levers a/b/c, lever plans,
    // parity.staffing / queue.events / parity.forecast). Off: phase 2's EWT plans only.
    bool levers = false;
    opt::LeverOptions lever;
    opt::ForecastOptions forecast;
    int forecast_every = 6;           // parity.forecast and lever (b)/(c) advice once a minute (every 6 ticks)
    int64_t history_ms = 56LL * 86'400'000;  // routing_bucket_actuals read for the profile (8 weeks)
    // A learned short-term forecaster (phases 5b+5c; null: the baseline only). Called on the tenant's
    // optimization thread; must not block. With it: the EWT's lambda is the forecast in use, and the
    // writer gets an arrival guard (the event-driven re-forecast, PLAN §5.9).
    opt::ForecastSource* learned = nullptr;
    opt::Sanity sanity;
  };

  // Per tenant: the estimators (EWMA over a 15-minute time constant; PLAN §5.3's baseline is phase 5).
  struct TenantState {
    struct Q {
      bool seen = false;
      double lambda = 0, abandons = 0, waited = 0;  // per second, per second, seconds per second
      double vip = 0;                               // VIP arrivals per second
      int64_t mix_until_ms = 0;  // a priority mix was seen: approx until then (CR-098)
      // Phases 5b+5c: the learned forecast's rate now for this snapshot (the EWT's lambda then).
      double fc_lambda = -1;
      int64_t fc_at_ms = 0;
    };
    std::map<std::string, Q> q;
    uint64_t version = 0;
    // Phase 5.
    opt::Forecaster fc;
    opt::LeverMemory lever_mem;  // the last tick's proposal (hysteresis in time)
    uint64_t lever_version = 0;
    uint64_t ticks = 0;
    bool history_asked = false;
  };
  struct LeverOut {
    std::shared_ptr<LeverPlan> plan;  // null: the model refused (the last plan stays)
    std::shared_ptr<ArrivalGuard> guard;  // phases 5b+5c: with a learned source only
    uint64_t learned_used = 0, learned_fallback = 0;  // queues this tick
    std::vector<AgentEvent> events;
    opt::LeverResult result;
    std::vector<opt::LeverQueue> queues;
  };

  struct Totals {
    uint64_t snapshots = 0, plans = 0, dropped = 0, refused = 0, clamps = 0, approx = 0;
    uint64_t solve_us_sum = 0, solve_us_max = 0;
    uint64_t lever_plans = 0, lever_refused = 0, lever_changes = 0, held = 0, capped = 0, events = 0;
    uint64_t lever_us_sum = 0, lever_us_max = 0;
    uint64_t learned_used = 0, learned_fallback = 0, guards = 0;  // phases 5b+5c (per queue and tick)
  };

  explicit Optimizer(Options o);
  ~Optimizer();
  Optimizer(const Optimizer&) = delete;
  Optimizer& operator=(const Optimizer&) = delete;

  // Plans go to this engine from now on.
  void attach(Engine* engine) { engine_.store(engine, std::memory_order_release); }
  // From a shard writer: never blocks on a solve.
  void submit(std::shared_ptr<const ModelSnapshot> s);
  // Phase 5: where the staffing events go (null: dropped) and who loads a tenant's bucket history
  // (called on the tenant's worker until it accepts the request, once per tick at most; must not
  // block: the rows come back through add_actuals; false = not taken, e.g. Postgres not connected yet).
  void attach_events(EventSink* e) { events_.store(e, std::memory_order_release); }
  void set_history_loader(std::function<bool(const std::string& tenant, int64_t since_ms)> f) { history_ = std::move(f); }
  // routing_bucket_actuals rows of one tenant (any thread): queued for that tenant's worker.
  void add_actuals(const std::string& tenant, std::vector<BucketRow> rows);
  Totals totals() const;

  // One tenant's plan from its snapshot (pure apart from `state`; the budget bench and tests call
  // it directly). Null when the model refused the inputs (NaN/inf, invalid): the last plan stays.
  static std::shared_ptr<EwtPlan> plan_for(const ModelSnapshot& s, TenantState* state, const Options& o,
                                           model::Stats* st, bool* approx = nullptr);
  // Phase 5: one tenant's lever tick (pure apart from `state`; the closed-loop DES, the budget bench
  // and tests call it directly).
  static LeverOut levers_for(const ModelSnapshot& s, TenantState* state, const Options& o, model::Stats* st);
  // routing_bucket_actuals rows as a learned source's history.
  static std::vector<opt::HistoryRow> history_rows(const std::vector<BucketRow>& rows);

 private:
  struct Worker {
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::shared_ptr<const ModelSnapshot>> q;
    std::deque<std::pair<std::string, std::vector<BucketRow>>> actuals;
    std::map<std::string, TenantState> state;  // this worker's tenants only
    model::Stats stats;
    std::thread th;
  };
  void run(Worker* w);

  Options o_;
  std::atomic<Engine*> engine_{nullptr};
  std::atomic<EventSink*> events_{nullptr};
  std::function<bool(const std::string&, int64_t)> history_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> snapshots_{0}, plans_{0}, dropped_{0}, refused_{0}, clamps_{0}, approx_{0},
      solve_us_sum_{0}, solve_us_max_{0}, lever_plans_{0}, lever_refused_{0}, lever_changes_{0}, held_{0},
      capped_{0}, events_out_{0}, lever_us_sum_{0}, lever_us_max_{0}, learned_used_{0}, learned_fallback_{0}, guards_{0};
};

}  // namespace pcr
