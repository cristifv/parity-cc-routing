// Tenant shards (PLAN §5.1): S writer threads; a tenant is hashed to one shard and never split.
// Each shard owns its tenants, a bounded inbox (at most 25 % per tenant, CR-034) and a timer heap.
// Nothing a writer does blocks: messages go to the Sink, which only queues them.
#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/core/tenant.h"
#include "src/core/types.h"

namespace pcr {

class Engine {
 public:
  // fn(tenant or nullptr when the tenant isn't in this bin, now).
  using Fn = std::function<void(Tenant*, int64_t)>;

  struct Limits {
    size_t inbox = 10'000;
    size_t per_tenant = 2'500;  // 25 %
    // Phase 2 (PLAN §3.4, §5.1): the model's EWT in QueueEntry/PositionUpdate. Off by default:
    // then no snapshot is taken and every estimate is phase 1's.
    bool model_ewt = false;
    int64_t model_tick_ms = 10'000;
    // CR-056: PositionUpdates beyond the head 10 at most once per 5 s per entry. On in every
    // deployment; off only for the full-scale before/after measurement (tests/scenarios runner).
    bool position_throttle = true;
    // Phase 5 (PLAN §5.3): the optimization loop feeds on the same snapshots (staffing plans, events,
    // the stall clock of PLAN §6). Either flag makes the shards take snapshots.
    bool optimizer = false;
    // CR-137: a tenant's record above this is refused and the tenant quarantined (tests lower it).
    size_t max_record_bytes = kMaxRecordBytes;
  };
  // Where the writers hand each tenant's snapshot every model tick. Must not block (the
  // optimization layer queues it or drops it, counted): the real-time layer never waits for it.
  using ModelFeed = std::function<void(std::shared_ptr<const ModelSnapshot>)>;

  Engine(const Clock* clock, Sink* sink, int shards, Limits limits, ModelFeed feed = nullptr);
  Engine(const Clock* clock, Sink* sink, int shards) : Engine(clock, sink, shards, Limits{}) {}
  ~Engine();
  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  // Phase 3: where tenants write callbacks, tasks and bucket actuals, and publish agent.events.
  // Set before load(); either may be null (then callbacks and tasks are refused, events dropped).
  // Both are reached through the commit gate (phase 6: a write or an event happens only after the
  // records of the change that caused it are committed).
  void set_durable(Durable* d, EventSink* e);
  // Phase 6 (PLAN §5.5): the state log. Set before load(); from then on every tenant tracks its
  // changes, each writer task's records and effects go to the log as one batch, and the effects
  // (Watch messages, RPC answers, events, Postgres writes) run only after the batch is committed.
  // Above `max_backlog` uncommitted records the writers stop taking work (Redpanda slow or down:
  // the inboxes fill and RPCs get UNAVAILABLE).
  void set_state_log(StateLog* log, size_t max_backlog = 200'000) {
    log_ = log;
    max_backlog_ = max_backlog;
  }
  // Phase 6: adds a tenant built from cfg with the given tenant epoch; init runs on its writer
  // right after (a takeover's or an arrival's restore). Waits until done.
  void add_tenant(const TenantConfig& cfg, uint64_t tenant_epoch, std::function<void(Tenant*, int64_t)> init);
  // Phase 6: a tenant moving away. On its writer: before() (Tenant::leave), its last records, the
  // handover marker `{tenant}/handover/{epoch}`, then the tenant is dropped (new work gets
  // tenant_not_in_bin). Waits until done.
  void remove_tenant(const std::string& id, uint64_t new_epoch, const std::string& from_bin, const std::string& to_bin,
                     std::function<void(Tenant*, int64_t)> before);
  std::vector<std::string> tenant_ids() const;
  // Replaces every tenant (startup, or the test control's seed). Waits until applied.
  void load(const std::vector<TenantConfig>& tenants, Epochs epochs);
  // Runs fn on the tenant's writer. External requests are refused (false) when the inbox or the
  // tenant's share of it is full: the caller answers UNAVAILABLE (retryable).
  bool post(const std::string& tenant, Fn fn, bool external = true);
  // Runs fn on every tenant (stream opened, acks by tenant, drain).
  void post_all_tenants(Fn fn);
  // Waits until every shard has run everything posted before (test control's sync).
  void barrier();
  // Manual clock only: moves time to `t`, firing every timer due up to it in time order.
  void advance_to(ManualClock* clock, int64_t t);
  void set_draining(bool d);
  // CR-099: no shard calls the model feed after this returns (it waits for every shard), so the
  // optimization layer can be stopped and freed before the engine goes.
  void stop_model_feed();
  bool has_tenant(const std::string& tenant) const;
  // Phase 5 (CR-137): tenants whose change produced a record over kMaxRecordBytes. Such a change is
  // dropped with its effects, and the tenant answers UNAVAILABLE (post refuses it) until it is rebuilt
  // from re.state (the next takeover or restart); the bin and its other tenants carry on.
  bool quarantined(const std::string& tenant) const;
  const Epochs& epochs() const { return epochs_; }

  struct Totals {
    uint64_t offers, accepted, rejected, expired, revoked, overflows, sweep_matches, assign_expired,
        redeliveries, waiting, ready, inbox_refused, owner_lost, ewt_model, ewt_heuristic,
        plans_installed, plans_refused, decision_max_us, moves_applied, moves_pending, moves_refused, moves_stale,
        coverage_refused, foreign_refused, config_refused, forced_states, callbacks_created, callbacks_dispatched,
        callbacks_flagged, callbacks_refused, voicemail_tasks, direct_offers, consults, gw_outbox_coalesced,
        gw_outbox_dropped, tenants_quarantined, records_refused, levers_installed, levers_refused, levers_applied,
        levers_held, stall_stale, vip_enqueued, bounds_min_picks, bounds_max_defers, vip_reserve_holds;
    uint64_t decision_us_buckets[32];
    uint64_t decision_us_sum;
    // Phase 7: how long tasks waited in the shards' inboxes (RPC posted -> its writer task started),
    // and the 1 s periodic work's duration (it holds the writer).
    uint64_t inbox_wait_us_buckets[32];
    uint64_t inbox_wait_us_sum, inbox_wait_us_max, periodic_us_sum, periodic_us_max, periodics;
    uint64_t early_snapshots;  // phases 5b+5c: arrival-guard breaches handed over at once
  };
  Totals totals() const;
  // Property tests: every tenant's invariant violations (runs on the writers). *checked counts
  // the comparisons made (CR-055: a check that never compares anything proves nothing).
  std::vector<std::string> check_invariants(uint64_t* checked = nullptr);
  // Interactions RE holds anything for (tests: CR-083's cleanup).
  std::vector<std::string> held_interactions();
  // Every tenant's entries (tests: I3 reconciliation), as (tenant, entry).
  std::vector<std::pair<std::string, Tenant::EntrySnap>> entries_snapshot();
  // External requests waiting in the tenant's shard inbox (tests: the inbox limits).
  size_t queued(const std::string& tenant) const;

 private:
  struct TimerItem {
    int64_t due;
    uint64_t seq;
    TimerRef ref;
    bool operator>(const TimerItem& o) const { return due != o.due ? due > o.due : seq > o.seq; }
  };
  struct Task {
    std::string tenant;  // empty: a shard-wide task
    bool external = false;
    std::function<void(int64_t)> run;
    std::chrono::steady_clock::time_point posted = std::chrono::steady_clock::now();  // phase 7: inbox wait
  };
  class Shard final : public TimerSink {
   public:
    Shard(Engine* e, int index) : engine_(e), index_(index) {}
    int index() const { return index_; }
    void at(int64_t due_ms, TimerRef t) override;
    void loop();
    void run_due(int64_t upto, bool manual);
    void periodic(int64_t now);

    Engine* engine_;
    int index_;
    std::mutex mu;
    std::condition_variable cv;
    std::deque<Task> inbox;
    std::unordered_map<std::string, size_t> per_tenant;
    bool stop = false;
    // Writer-thread only:
    std::priority_queue<TimerItem, std::vector<TimerItem>, std::greater<>> timers;
    uint64_t timer_seq = 0;
    int64_t next_periodic = 0;
    int64_t next_model = 0;
    uint64_t periodic_count = 0;
    std::unordered_map<std::string, std::unique_ptr<Tenant>> tenants;
    std::unordered_set<std::string> quarantined;  // writer-thread copy of Engine::quarantined_
    Counters counters;
    std::thread thread;
    Batch batch;  // phase 6: the records and effects of the work done since the last submit
    // Runs fn with the batch current (when there is a state log), then collects the records of
    // `tenant` (all tenants when empty).
    void gated(const std::string& tenant, const std::function<void()>& fn);
    // CR-137: the records of `tenant` added to the batch since `rec_mark`; one over kMaxRecordBytes
    // drops them and the effects since `eff_mark` (when the task was the tenant's own) and quarantines
    // the tenant. True when it did.
    bool contain(const std::string& tenant, size_t rec_mark, size_t eff_mark, bool own_task);
    void flush();
  };

  Shard& shard_of(const std::string& tenant) const;
  void post_shard(Shard& s, Task t);
  void make_tenant(Shard* sp, const TenantConfig& cfg, uint64_t tenant_epoch);
  // The commit gate's wrappers (gate.h): pass through when no batch is current.
  struct GateSink final : Sink {
    Sink* inner = nullptr;
    void to_ie(const std::string& r, uint64_t g, const rv1::WatchResponse& m) override;
    void to_gw(const std::string& r, uint64_t g, const av1::WatchResponse& m) override;
    bool ie_live(const std::string& r) const override { return inner->ie_live(r); }
    std::vector<std::string> ie_replicas() const override { return inner->ie_replicas(); }
  };
  struct GateDurable final : Durable {
    Engine* engine = nullptr;
    Durable* inner = nullptr;
    bool put_callback(const CallbackRecord& r, Done done) override;
    bool put_task(const TaskRecord& r, TaskDone done) override;
    void put_buckets(std::vector<BucketRow> rows) override;
  };
  struct GateEvents final : EventSink {
    EventSink* inner = nullptr;
    void publish(AgentEvent e) override;
  };
  GateSink gate_sink_;
  GateDurable gate_durable_;
  GateEvents gate_events_;
  StateLog* log_ = nullptr;
  size_t max_backlog_ = 200'000;

  const Clock* clock_;
  Sink* sink_;
  Limits limits_;
  ModelFeed feed_;
  Durable* durable_ = nullptr;
  EventSink* events_ = nullptr;
  std::atomic<bool> feed_stopped_{false};
  Epochs epochs_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::atomic<uint64_t> inbox_refused_{0};
  std::atomic<uint64_t> quarantine_count_{0}, records_refused_{0};
  std::unordered_set<std::string> quarantined_;  // under tenants_mu_
  mutable std::mutex tenants_mu_;
  std::unordered_map<std::string, int> tenant_index_;  // which tenants this bin serves
};

}  // namespace pcr
