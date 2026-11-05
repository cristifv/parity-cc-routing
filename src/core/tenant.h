// One tenant's routing state and every decision about it (PLAN §4.2, §8 1.2). A Tenant is only
// ever touched by its shard's writer thread; nothing here blocks, locks or does I/O. Messages leave
// through a Sink (the stream hub) and timers through a TimerSink (the shard's timer heap).
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "parity/agents/v1/agents.pb.h"
#include "parity/routing/v1/routing.pb.h"
#include "src/core/gate.h"
#include "src/core/model_feed.h"
#include "src/core/types.h"

namespace parity::cc_routing::state::v1 {
class Record;
}

namespace pcr {

class LogRec;  // src/ops/log.h: one log-record.v1 record

namespace rv1 = ::parity::routing::v1;
namespace av1 = ::parity::agents::v1;

// Where messages go: the Watch streams of IE replicas (routing.v1) and gateway replicas (agents.v1).
// Each stream a replica opens gets a new generation (CR-053). A tenant sends with the generation
// it has replayed to (`on_*_stream_open`); the sink delivers only when that is the replica's live
// stream and drops the message otherwise. A dropped message is still unacknowledged, so the
// pending replay of the newer stream sends it, in delivery_seq order, before anything newer.
class Sink {
 public:
  virtual ~Sink() = default;
  virtual void to_ie(const std::string& replica, uint64_t gen, const rv1::WatchResponse& m) = 0;
  virtual void to_gw(const std::string& replica, uint64_t gen, const av1::WatchResponse& m) = 0;
  // True while the replica has a live routing Watch stream (L7's pause).
  virtual bool ie_live(const std::string& replica) const = 0;
  // Phase 3: the IE replicas with a live routing Watch stream, sorted (RunCallback goes to one).
  virtual std::vector<std::string> ie_replicas() const { return {}; }
};

enum class TimerKind : uint8_t {
  kOfferExpiry,     // key: offer_id
  kAssignDeadline,  // key: reservation_id (deadline_ms + 2 s, L7)
  kWrapUpLimit,     // key: reservation_id
  kEscalate,        // key: entry_id
  kMaxWait,         // key: entry_id
  kSkipExpiry,      // key: entry_id
  kDetachGrace,     // key: agent_id
  kSecondaryDelay,  // key: entry_id (phase 4: secondaries may take it)
  kPinExpiry,       // key: agent_id (phase 4: a pin's until_ms)
  kCallbackRedeliver,  // key: callback_id (phase 3: RunCallback unacknowledged for 5 s)
  kCallbackDue,     // key: callback_id (phase 3: a rescheduled attempt becomes offerable)
};

struct TimerRef {
  std::string tenant;
  TimerKind kind;
  std::string key;
};

class TimerSink {
 public:
  virtual ~TimerSink() = default;
  virtual void at(int64_t due_ms, TimerRef t) = 0;
};

// Counted per shard, read by /metrics (relaxed: only the writer thread increments).
struct Counters {
  std::atomic<uint64_t> offers{0};
  std::atomic<uint64_t> offers_accepted{0};
  std::atomic<uint64_t> offers_rejected{0};
  std::atomic<uint64_t> offers_expired{0};
  std::atomic<uint64_t> offers_revoked{0};
  std::atomic<uint64_t> overflows{0};
  std::atomic<uint64_t> sweep_matches{0};  // a bug if ever non-zero (PLAN §4.2)
  std::atomic<uint64_t> assign_expired{0};
  std::atomic<uint64_t> redeliveries{0};
  std::atomic<uint64_t> owner_lost{0};     // interactions cleaned up for a lost IE owner (CR-083)
  std::atomic<uint64_t> time_to_offer_ms_sum{0};
  std::atomic<uint64_t> waiting{0};        // gauges, refreshed every second
  std::atomic<uint64_t> ready{0};
  // Phase 2 (PLAN §3.4): which estimate each QueueEntry/PositionUpdate expected_wait_s used.
  std::atomic<uint64_t> ewt_model{0};
  std::atomic<uint64_t> ewt_heuristic{0};
  std::atomic<uint64_t> plans_installed{0};
  std::atomic<uint64_t> plans_refused{0};  // stale, or naming another tenant
  // Phase 4 (PLAN §4.3, §4.4).
  std::atomic<uint64_t> moves_applied{0};   // active sets changed at once
  std::atomic<uint64_t> moves_pending{0};   // changes held for the next offer
  std::atomic<uint64_t> moves_refused{0};   // dwell, hourly cap, change rate, mode, pinned, excluded
  std::atomic<uint64_t> moves_stale{0};     // CR-021: no longer a member or eligible, logged out
  std::atomic<uint64_t> coverage_refused{0};  // a change that would leave a queue uncovered
  std::atomic<uint64_t> foreign_refused{0};   // a plan, pin or policy naming another tenant's agent or queue
  std::atomic<uint64_t> config_refused{0};    // a stale or invalid config.routing / config.routing_pin
  std::atomic<uint64_t> forced_states{0};
  // Phase 5 (CR-137): superseded QueuesChanged dropped from gateway outboxes; messages dropped at the cap.
  std::atomic<uint64_t> gw_outbox_coalesced{0};
  std::atomic<uint64_t> gw_outbox_dropped{0};
  // Phase 5 redesign (PLAN §5.3, §6): the levers.
  std::atomic<uint64_t> levers_installed{0};
  std::atomic<uint64_t> levers_refused{0};     // stale, or another tenant's
  std::atomic<uint64_t> levers_applied{0};     // queues whose weight or delay changed (auto, exact)
  std::atomic<uint64_t> levers_held{0};        // queue changes held: off/advise, approx, or out of bounds (clamped)
  std::atomic<uint64_t> stall_stale{0};        // plans gone stale: config weights and delays again
  std::atomic<uint64_t> vip_enqueued{0};       // entries of the VIP class
  std::atomic<uint64_t> bounds_min_picks{0};   // lever (b): a READY agent took a queue below its min first
  std::atomic<uint64_t> bounds_max_defers{0};  // lever (b): a queue at its max was passed over for other work
  std::atomic<uint64_t> vip_reserve_holds{0};  // a VIP-eligible agent kept for VIP callers (normal call before d_i)
  // Phase 3.
  std::atomic<uint64_t> callbacks_created{0};
  std::atomic<uint64_t> callbacks_dispatched{0};  // RunCallback sent (first time per attempt)
  std::atomic<uint64_t> callbacks_flagged{0};     // CONVENTIONS: routing_engine_callbacks_flagged_total
  std::atomic<uint64_t> callbacks_refused{0};     // the store couldn't take it (Postgres down, queue full)
  std::atomic<uint64_t> voicemail_tasks{0};
  std::atomic<uint64_t> direct_offers{0};
  std::atomic<uint64_t> consults{0};
  // I9 (PLAN §6.1): offer-decision latency, wall clock from the start of the writer task that made
  // the entry offerable or the agent ready to the offer leaving the writer. Log2 buckets of
  // microseconds (bucket b: < 2^b us) and the maximum.
  std::atomic<uint64_t> decision_us_buckets[32] = {};
  std::atomic<uint64_t> decision_us_max{0};
  std::atomic<uint64_t> decision_us_sum{0};  // phase 7: the histogram's _sum
  // Phase 7 (shard counters only): inbox wait per task, the periodic tick's duration.
  std::atomic<uint64_t> inbox_wait_us_buckets[32] = {};
  std::atomic<uint64_t> inbox_wait_us_sum{0}, inbox_wait_us_max{0}, periodic_us_sum{0}, periodic_us_max{0}, periodics{0};
  // Phases 5b+5c: snapshots handed over early on an arrival-guard breach (the event-driven re-forecast).
  std::atomic<uint64_t> early_snapshots{0};
};

// Phase 3: what RE keeps in Postgres (callbacks, voicemail tasks) and the 15-minute actuals. The
// real-time layer only queues writes (never waits): `done` runs later on the tenant's writer, posted
// by the store; a refusal (false) means nothing was queued (the store is down or its queue full).
struct CallbackRecord {
  std::string tenant, callback_id, original_interaction_id, entry_id, queue_id;
  std::string number;  // PII: phone (E.164). Never logged.
  uint32_t priority = 0;
  std::vector<SkillReq> reqs;
  int64_t first_queued_at = 0, ordering_at = 0;
  // scheduled: waiting for an agent; dispatched: RunCallback sent for `attempt`, not yet claimed;
  // claimed: IE acknowledged it; done: an outcome ended it; flagged: expired or unknown after a
  // restart, never redialled automatically.
  std::string state = "scheduled";
  int attempt = 0;
  std::string attempt_interaction_id, reservation_id, agent_id, outcome;
  int64_t due_at = 0, created_at = 0, updated_at = 0;
};
struct TaskRecord {
  std::string tenant, task_id, interaction_id, queue_id, recording_id;
  int64_t recorded_at = 0, created_at = 0;
  uint32_t duration_s = 0;
};
struct BucketRow {  // routing_bucket_actuals (PLAN §5.3, §5.9): one queue, one 15-minute bucket
  std::string tenant, queue_id;
  int64_t bucket_start_ms = 0;
  uint64_t offered = 0, answered = 0, answered_in_sla = 0, abandoned = 0, overflowed = 0;
  double wait_answered_ms = 0, handle_ms = 0;
  uint64_t handled = 0;
  std::vector<uint16_t> offered_10s;  // phases 5b+5c (db/V3): arrivals per 10 s step (90), empty: none
};
class Tenant;
class Durable {
 public:
  virtual ~Durable() = default;
  using Done = std::function<void(Tenant*, bool ok, int64_t now)>;
  // Writes (inserts or updates) the callback; `done` gets ok once it is durable.
  virtual bool put_callback(const CallbackRecord& r, Done done) = 0;
  // Inserts the task, or finds the one with its (tenant, recording_id): *existing_id gets it.
  using TaskDone = std::function<void(Tenant*, bool ok, const std::string& task_id, bool existing, int64_t now)>;
  virtual bool put_task(const TaskRecord& r, TaskDone done) = 0;
  // Best effort: dropped (counted) when the store is down or busy.
  virtual void put_buckets(std::vector<BucketRow> rows) = 0;
};

// Phase 3: agent.events (PLAN §5.4): state_changed, wrap_up_ended (with the disposition),
// queues_changed. Never blocks: a full buffer drops and counts (events are not state).
struct AgentEvent {
  std::string kind;  // state_changed | wrap_up_ended | queues_changed | forced_state
  std::string tenant, agent_id;
  int64_t at_ms = 0;
  std::string json;  // the event's body (schemas/events/agent-events.v1.json), built by the writer
  // Phase 5: the optimizer's events (parity.staffing, queue.events, parity.forecast) carry their topic
  // and their complete body (event_id included); empty topic: agent.events, framed by the publisher.
  std::string topic;
};
class EventSink {
 public:
  virtual ~EventSink() = default;
  virtual void publish(AgentEvent e) = 0;
};

class Tenant {
 public:
  Tenant(TenantConfig cfg, const Epochs* epochs, Sink* sink, TimerSink* timers, Counters* counters);

  const std::string& id() const { return id_; }

  // ---- routing.v1 (tenant-id already checked against owner.tenant_id) ----
  Result<rv1::EnqueueResponse> enqueue(const rv1::EnqueueRequest& r, int64_t now);
  Result<rv1::DequeueResponse> dequeue(const rv1::DequeueRequest& r, int64_t now);
  Result<rv1::ReportOutcomeResponse> report_outcome(const rv1::ReportOutcomeRequest& r, int64_t now);
  Result<rv1::CallEndedResponse> call_ended(const rv1::CallEndedRequest& r, int64_t now);
  Result<rv1::ReattachResponse> reattach(const rv1::ReattachRequest& r, int64_t now);
  // A batched ack; false when stale or foreign (ignored).
  bool ack(const rv1::InteractionOwner& a, int64_t now);
  // A routing Watch stream of generation `gen` opened for `replica`: replay its unacknowledged
  // messages in order; from now on messages for it go straight to that stream (CR-053).
  void on_ie_stream_open(const std::string& replica, uint64_t gen, int64_t now);

  // ---- agents.v1 ----
  Result<av1::AttachAgentResponse> attach(const av1::AttachAgentRequest& r, int64_t now);
  Result<av1::DetachAgentResponse> detach(const av1::DetachAgentRequest& r, int64_t now);
  Result<av1::SetStateResponse> set_state(const av1::SetStateRequest& r, int64_t now);
  Result<av1::AnswerOfferResponse> answer_offer(const av1::AnswerOfferRequest& r, int64_t now);
  Result<av1::EndWrapUpResponse> end_wrap_up(const av1::EndWrapUpRequest& r, int64_t now);
  void gw_ack(const av1::Ack& a);
  void on_gw_stream_open(const std::string& replica, uint64_t gen, int64_t now);
  // A supervisor's forced not-ready or logout (agents.v1 0.11.0): applies at once while RESERVED
  // (the reservation is revoked, L8), pending while on a call or in wrap-up.
  Result<av1::ForceStateResponse> force_state(const av1::ForceStateRequest& r, int64_t now);

  // ---- phase 8: campaign lending (IE step 13a; CONVENTIONS "Campaign lending") ----
  // The mode flag is this registry's: an agent is inbound or lent to one campaign. A switch happens
  // at once when the agent is idle (no offer, no call), else when the offer resolves or its wrap-up
  // ends (pending, reported). Lending that would leave an open queue uncovered is refused. The
  // campaign's leader (CampaignOwner) is fenced by its owner_epoch; a higher one takes it over.
  Result<rv1::LendAgentsResponse> lend_agents(const rv1::LendAgentsRequest& r, int64_t now);
  Result<rv1::ReturnAgentsResponse> return_agents(const rv1::ReturnAgentsRequest& r, int64_t now);
  // The leader reserves an idle lent agent for a campaign call (owner: that call's interaction).
  Result<rv1::ReserveAgentResponse> reserve_agent(const rv1::ReserveAgentRequest& r, int64_t now);
  // For tests: the agent's campaign ("" inbound) and whether a switch is pending.
  std::string campaign_of(const std::string& agent) const;
  bool mode_pending_of(const std::string& agent) const;

  // ---- phase 4: agents moving between queues (PLAN §4) ----
  // config.routing. Refused (false, counted) when older than the one held or naming a queue this
  // tenant doesn't have.
  bool install_policy(const RoutingPolicy& p, int64_t now, std::string* why = nullptr);
  // config.routing_pin (or its tombstone: unpin). Refused when the agent or a queue isn't this
  // tenant's (tenant isolation, counted) or the version isn't newer. A pin ignores dwell; it applies
  // at the agent's next offer, never mid-call; one that would leave a queue uncovered waits (the 1 s
  // sweep retries it).
  bool install_pin(const RoutingPin& p, bool tombstone, int64_t now, std::string* why = nullptr);
  // An optimizer's decisions in contract terms (the scripts' `moves`; phase 5's planner calls the
  // same): each move is re-checked here against the state held now (CR-021).
  struct Move {
    std::string agent_id;
    std::vector<std::pair<std::string, Tier>> queues;
    std::map<std::string, double> weights;  // 0.25-4; missing: 1
  };
  struct MoveResult {
    int applied = 0, pending = 0, silent = 0, refused = 0;
    std::vector<std::string> why;  // per refused move: "<agent>: <reason>"
  };
  MoveResult apply_moves(const std::vector<Move>& moves, int64_t now);
  // For tests: the agent's active set and pending set (queue, tier) and its weights.
  std::vector<std::pair<std::string, Tier>> active_of(const std::string& agent) const;
  std::vector<std::pair<std::string, Tier>> pending_of(const std::string& agent) const;
  const RoutingPolicy& policy() const { return policy_; }

  // ---- phase 5 redesign: the levers (PLAN §5.3, §6) ----
  // The optimizer's lever plan (refused, counted, when another tenant's, not newer or already
  // expired). Applied only in auto mode, per queue only where that queue's result is exact (`approx`
  // never changes weights or delays), clamped to config.routing's bounds. Weights are silent (no
  // QueuesChanged, CR-018); a new delay re-arms the waiting entries' timers. No agent is moved.
  struct LeverResult {
    int applied = 0, held = 0, clamped = 0;
    std::string why;  // why nothing applied ("mode", "approx")
  };
  bool install_levers(std::shared_ptr<const LeverPlan> p, int64_t now, LeverResult* out = nullptr);
  // Phases 5b+5c (PLAN §5.9): the optimizer's arrival guard, and the writer's check of it (the shard's
  // 1 s periodic): true when a queue's arrivals since the last snapshot leave e +/- (4 sqrt(e) + 3),
  // at most once per 2 s. A guard of another tenant is refused.
  bool install_guard(std::shared_ptr<const ArrivalGuard> g, int64_t now);
  bool guard_breached(int64_t now);
  // An optimization layer feeds this tenant: the stall clock (PLAN §6) runs only then.
  void set_optimizer(bool on) { optimizer_on_ = on; }
  int plan_stage() const { return opt_.stage; }
  uint64_t plan_version() const { return opt_.version; }
  int64_t plan_valid_until() const { return opt_.valid_until; }
  // For tests: a queue's lever (a) values in force and lever (b)'s busy count.
  double queue_weight(const std::string& queue) const;
  int64_t queue_delay(const std::string& queue) const;
  int busy_on(const std::string& queue) const;
  std::map<std::string, double> weights_of(const std::string& agent) const;

  // ---- phase 3: callbacks and voicemail tasks (PLAN §8 row 3) ----
  // Durable before the effect: the answer goes out once Postgres has the record (`done` on this
  // writer, later). Postgres down: UNAVAILABLE / store_unavailable, the entry untouched.
  using CallbackDone = std::function<void(Result<rv1::ConvertToCallbackResponse>)>;
  void convert_to_callback(const rv1::ConvertToCallbackRequest& r, int64_t now, CallbackDone done);
  using TaskDone = std::function<void(Result<rv1::CreateVoicemailTaskResponse>)>;
  void create_voicemail_task(const rv1::CreateVoicemailTaskRequest& r, int64_t now, TaskDone done);
  // After a restart: the tenant's open callbacks from Postgres (idempotent by callback_id).
  void restore_callbacks(const std::vector<CallbackRecord>& rows, int64_t now);
  void set_durable(Durable* d) { durable_ = d; }
  void set_events(EventSink* e) { events_ = e; }
  // Every 15 minutes (the engine's periodic): the finished bucket's rows (routing_bucket_actuals).
  std::vector<BucketRow> take_buckets(int64_t now);
  uint64_t instance() const { return instance_; }
  // For tests: callback states by callback id.
  std::map<std::string, std::string> callback_states() const;

  // ---- timers and periodic work ----
  void on_timer(const TimerRef& t, int64_t now);
  void redeliver(int64_t now);           // unacknowledged for 2 s: send again
  void sweep(int64_t now);               // the 1 s safety net (PLAN §4.2)
  void prune(int64_t now);               // forget finished interactions and old commands
  // CR-083: every second. An IE replica's interactions are orphaned when (a) the replica has had
  // no live stream and sent no RPC for owner_loss_ms, or (b) its stream is live but one of their
  // messages has stayed unacknowledged for owner_loss_ms (a live IE acks within milliseconds and
  // re-acks replays it processed; a restarted IE never acks calls it doesn't know). An orphan's
  // entry leaves (a live offer is revoked CALLER_GONE), an ASSIGNED reservation ends REVOKED and
  // frees its agent, a CONNECTED one goes to wrap-up, and its messages are dropped. A later owner
  // (a higher owner_epoch) starts afresh: Reattach finds nothing, the same enqueue_seq enqueues anew.
  void reap_orphans(int64_t now);
  void set_draining(bool d) { draining_ = d; }

  // ---- phase 6: re.state (PLAN §5.5; src/core/tenant_state.cc) ----
  // From now on every change is tracked: collect() turns the changed objects into records. Turning
  // it on marks everything, so the first collect writes the whole tenant (a restore replaces that).
  void set_tracking(bool on) {
    const bool was = track_;
    track_ = on;
    if (on && !was) touch_all();
  }
  // The records of every object changed since the last collect (a tombstone for a removed one),
  // stamped with the tenant's epoch and the leader's. An object whose record didn't change is
  // skipped (by a hash of the last one written).
  void collect(std::vector<StateRec>* out, int64_t now);
  // Restores a tenant freshly built from its config from its re.state records (the latest value
  // per key `{tenant}/{kind}/{id}`): memory, indices, and timers from absolute deadlines (expired
  // ones fire at once); unacknowledged messages get the new leader_epoch and replay when streams
  // open. With restamp every record is written again at the next collect (a tenant move's epoch).
  struct RestoreStats {
    size_t records = 0, skipped = 0;
  };
  RestoreStats restore(const std::vector<std::pair<std::string, std::string>>& kvs, int64_t now, bool restamp);
  // After restore (a takeover, a restart, a tenant's arrival): the tenant's open callbacks from
  // Postgres, read after RE raised the tenant's fence there, so no write of an older leader can land
  // later. Postgres holds the promise (the row and its number), re.state the routing state:
  // numbers come from the rows; a conversion whose row exists completes, one whose row doesn't is
  // undone (the caller stays queued); a dispatch whose RunCallback never left is made again; rows
  // re.state never saw are restored as after a phase-3 restart.
  void reconcile_callbacks(const std::vector<CallbackRecord>& rows, int64_t now);
  bool callbacks_reconciled() const { return reconciled_; }
  void set_tenant_epoch(uint64_t e) { tenant_epoch_ = e; }
  uint64_t tenant_epoch() const { return tenant_epoch_; }
  // The tenant moves to another bin (ARCH §3.20, PLAN §6): no new offers; unanswered offers are
  // withdrawn DETACHED (re-offered there with the ordering time kept); reservations travel.
  void leave(int64_t now);
  // Tests: every collect also encodes every object and lists the keys that changed unmarked.
  void set_paranoid(bool on) { paranoid_ = on; }
  const std::vector<std::string>& unmarked() const { return unmarked_; }
  // Tests: the durable state as sorted `key=bytes` lines (what re.state must reproduce).
  std::string durable_dump() const;

  // ---- the model's EWT (phase 2, PLAN §3.4, §5.1; behind --model-ewt, off by default) ----
  // With the flag off nothing below runs and every expected_wait_s is phase 1's estimate.
  void set_model_ewt(bool on) { model_ewt_ = on; }
  void set_position_throttle(bool on) { position_throttle_ = on; }
  bool model_ewt() const { return model_ewt_; }
  // The writer's snapshot for the optimization layer (counts since the previous one; resets them).
  std::shared_ptr<const ModelSnapshot> model_snapshot(int64_t now);
  // A plan from the optimizer: kept until valid_until_ms; refused (counted) if it names another
  // tenant or is older than the one held.
  bool install_ewt(std::shared_ptr<const EwtPlan> plan, int64_t now);
  // I9: the writer task that is running started at this steady-clock time (Engine sets it).
  void set_task_start(std::chrono::steady_clock::time_point t) { task_start_ = t; }

  // ---- seeding (test control and the seed file) ----
  struct SeededSession {
    std::string agent_id, session_id;
    uint64_t attach_generation = 0;
  };
  // Attaches an agent to `replica` as if AttachAgent had run; READY with idle-since
  // `ready_since` when set, else NOT_READY{login}.
  SeededSession seed_attached(const std::string& agent, const std::string& replica,
                              const std::string& contact, std::optional<int64_t> ready_since,
                              int64_t now);
  // An unattached agent in a given state (agents.v1 scripts): NOT_READY{reason}, READY, or
  // ON_CALL with a CONNECTED reservation.
  void seed_state(const std::string& agent, av1::AgentState s, const std::string& reason,
                  const std::string& reservation_id, const std::string& interaction_id,
                  int64_t now);
  // Test stimuli (agents.v1 scripts' inject): a call queued by a stand-in owner, a call ended.
  Result<rv1::EnqueueResponse> inject_queue_call(const std::string& interaction,
                                                 const std::string& queue, int64_t now,
                                                 uint32_t priority = 0);
  Result<bool> inject_call_ended(const std::string& reservation_id, int64_t now);
  // The stand-in owner reports CONNECTED for the reservation (agents.v1 scripts: a call in progress).
  Result<bool> inject_call_connected(const std::string& reservation_id, int64_t now);

  // ---- checks (property tests, scenario runner) ----
  // Structural invariants. Availability is recomputed from the agents and entries themselves and
  // every dispatch index is compared with it (CR-055), so an index bug can't hide behind the
  // index. *checked (optional) counts the (entry, agent) and index comparisons made.
  std::vector<std::string> check_invariants(int64_t now, uint64_t* checked = nullptr) const;
  struct EntrySnap {
    std::string interaction, entry_id, queue;
    rv1::EntryState state;
  };
  std::vector<EntrySnap> entries_snapshot() const;
  // Test-only faults the scenario runner injects to show its invariants fire (CR-055):
  // "drop_entry" forgets one waiting entry silently; "unindex_agent" removes one available agent
  // from the longest-idle index only; "no_owner_loss" switches CR-083's cleanup off. Never called
  // outside tests.
  bool test_fault(const std::string& kind);
  // A config document keyed for this tenant but naming another (the config reader's check).
  void count_foreign() { ++counters_->foreign_refused; }
  // Snapshot numbers for /metrics.
  size_t waiting_entries() const;
  size_t ready_agents() const { return ready_idle_.size(); }
  // For tests: the agent's live offer id (empty if none) and state.
  std::string live_offer_of(const std::string& agent) const;
  av1::AgentState state_of(const std::string& agent) const;
  std::vector<std::string> live_reservations() const;
  std::vector<std::string> entry_ids() const;
  // Interactions RE still holds something for: an entry, an unacknowledged message or a live
  // reservation (tests: CR-083's cleanup).
  std::vector<std::string> held_interactions() const;

 private:
  struct OutIe {
    uint64_t seq;
    int64_t sent_at;
    rv1::WatchResponse msg;
    int64_t first_at = 0;  // first sent to this owner (CR-083's clock)
  };
  struct OutGw {
    uint64_t seq;
    int64_t sent_at;
    av1::WatchResponse msg;
  };
  // (-priority, ordering_at, order_seq, entry_id): the queue order.
  using EKey = std::tuple<int64_t, int64_t, uint64_t, std::string>;
  // (ordering_at, order_seq, entry_id): escalated entries, oldest first.
  using XKey = std::tuple<int64_t, uint64_t, std::string>;

  struct Entry {
    std::string id, interaction, queue, sig;
    std::string first_queue;  // the queue IE enqueued to (an overflow move changes `queue`)
    uint32_t enqueue_seq = 0;
    uint32_t priority = 0;
    std::vector<SkillReq> reqs;
    int64_t first_queued_at = 0, ordering_at = 0, queue_entered_at = 0, created_at = 0;
    int64_t original_ordering_at = 0;
    uint64_t order_seq = 0;
    rv1::EntryState state = rv1::ENTRY_STATE_WAITING;
    bool escalated = false;
    bool overflow_due = false;  // max wait passed while RESERVED
    std::string offer_id, reservation_id;
    std::map<std::string, int64_t> skips;  // agent -> skip until (a reject)
    std::optional<EKey> avail_key;         // in the per-signature index
    std::optional<XKey> esc_key;           // in the escalated index
    std::optional<EKey> wait_key;          // in the queue's position order
    uint32_t last_position = 0;
    int64_t position_sent_at = 0;  // CR-056: beyond the head, at most one PositionUpdate per 5 s
    // Phase 4: past the queue's secondary delay (secondaries may take it; d = 0: at once).
    bool delay_passed = false;
    std::optional<EKey> delayed_key;  // in the per-signature index of entries past the delay
    // Phase 3.
    rv1::Target target;         // as IE asked (queue, agent or consult); an overflow move: the queue
    std::string direct_agent;   // a direct or consult-to-agent entry: only this agent
    std::string consult_from;   // a consult: the connected agent who asked
    bool head = false;          // PLACEMENT_HEAD: ahead of every priority
    std::string callback_id;    // a scheduled callback (RE-owned; no IE caller on hold)
    bool converting = false;    // ConvertToCallback is being made durable: offered to no one
    bool callback_offered = false;  // CALLBACK_OFFER fired (L15): its max-wait duty is done
    // Phase 5 redesign: the VIP class (Enqueue caller_class VIP): ahead of every normal entry at
    // equal escalation status (escalation stays above it, so normal callers never starve).
    bool vip = false;
  };
  struct Offer {
    std::string id, agent, entry;
    int64_t expires_at = 0;
    Tier tier = Tier::kNone;
  };
  enum class ResSt { kAssigned, kConnected, kWrapUp, kEnded };
  struct Reservation {
    std::string id, interaction, entry_id, agent, contact, queue;
    int64_t deadline_ms = 0;
    ResSt st = ResSt::kAssigned;
    rv1::Outcome outcome = rv1::OUTCOME_UNSPECIFIED;
    bool connected_once = false;
    int64_t connected_at = 0, wrap_ends = 0, ended_at = 0;
    int64_t pause_since = 0;  // L7: when the bound replica was first seen without a stream
    bool consult = false;
    bool direct = false;
    std::string callback_id;  // a callback's reservation (RunCallback)
    std::string campaign;     // phase 8: a campaign call's reservation (ReserveAgent)
  };
  struct Interaction {
    std::string id;
    uint64_t owner_epoch = 0;
    std::string replica;
    std::string entry_id;                     // the live entry, if any
    std::set<std::string> past_entries;       // for already_gone
    std::map<uint32_t, rv1::QueueEntry> snaps;  // per enqueue_seq: last view (repeats)
    std::map<uint32_t, std::string> seq_queue;  // per enqueue_seq: the target IE asked for
    uint32_t max_enqueue_seq = 0;
    uint64_t next_seq = 0;
    uint64_t acked = 0;
    std::deque<OutIe> unacked;
    std::set<std::string> reservations;
    int64_t last_activity = 0;
    bool owner_lost = false;  // CR-083: cleaned up; nothing more is sent until a new owner
    std::string callback_id;  // phase 3: a callback's attempt (RE made the id; no owner until IE acks)
  };
  struct QueueSt {
    QueueConfig cfg;
    std::map<std::string, std::set<EKey>> avail;  // per requirement signature
    std::set<EKey> waiting;                        // every WAITING entry, for positions
    double aht_ms = 0;                             // EWMA of talk + wrap-up
    int logged_in = 0;  // eligible agents attached and logged in (CR-056: kept, not scanned)
    uint64_t arrivals = 0, abandons = 0;  // since the last model snapshot
    double waited_ms = 0;
    double wrap_ms = 0;  // phase 5: EWMA of wrap-up alone
    uint32_t prio_lo = UINT32_MAX, prio_hi = 0;  // arrivals' priorities since then (CR-098)
    bool positions_pending = false;  // a throttled PositionUpdate waits for the 1 s sweep
    // Phase 4: the queue's entries past its secondary delay, per signature (secondaries look here).
    std::map<std::string, std::set<EKey>> delayed;
    int64_t secondary_delay_ms = 10'000;  // d_i (config.routing)
    int64_t escalate_after_ms = 0;        // config.routing's override, else cfg.escalate_after_ms
    int min_agents = 0, max_agents = 0;
    // Phase 5 redesign: the queue's config.routing part (defaults and bounds), lever (a)'s weight in
    // force (the delay in force is secondary_delay_ms), lever (b)'s count of agents busy on it (kept
    // by refresh_agent from each agent's busy_queue), VIP arrivals since the last snapshot.
    QueuePolicy pol;
    double weight = 1;
    int busy = 0;
    uint64_t vip_arrivals = 0;
    uint64_t win_handled = 0;  // calls handled (talk + wrap-up) since the last snapshot
    double win_handle_ms = 0;  // ... and their handle time
    // Phase 3: the 15-minute bucket's actuals.
    BucketRow bucket;
  };
  struct AgentSt {
    AgentConfig cfg;
    std::set<std::string> member;
    av1::AgentState state = av1::AGENT_STATE_LOGGED_OUT;
    std::string reason;
    av1::TargetState pending = av1::TARGET_STATE_UNSPECIFIED;
    std::string pending_reason;
    int64_t since = 0;
    uint64_t version = 0;
    int64_t idle_since = 0;
    bool attached = false;
    std::string session_id, replica, contact;
    uint64_t generation = 0;
    int64_t detached_at = 0;
    std::string offer_id;
    std::set<std::string> reservations;  // live ones
    std::set<std::string> past_offers;
    uint64_t next_seq = 0;
    std::deque<OutGw> unacked;
    std::optional<std::pair<int64_t, std::string>> avail_key;
    bool counted = false;  // in its eligible queues' logged_in
    // Phase 4 (PLAN §4.1): the active set and its pending successor ("next offer, never mid-call").
    struct Member {
      Tier tier = Tier::kNone;
      bool pinned = false;
      int64_t since = 0;
    };
    using Set = std::map<std::string, Member>;
    Set active;
    std::optional<Set> pending_set;
    av1::MembershipReason pending_set_reason = av1::MEMBERSHIP_REASON_UNSPECIFIED;
    uint64_t queues_version = 0;
    uint64_t active_version = 0;       // bumps whenever `active` changes
    uint64_t active_version_busy = 0;  // `active_version` when the agent was offered or reserved
    std::optional<std::map<std::string, Tier>> plan;  // the optimizer's set (auto mode)
    std::map<std::string, double> weights;            // w_ai, default 1
    std::deque<int64_t> move_times;                   // optimizer/config moves, the last hour
    int64_t primary_since = 0;    // dwell: the last change of its primary queues
    int64_t secondary_since = 0;  // CR-018: the last change of its secondary queues
    bool retarget_due = false;    // a pin waiting for coverage: the sweep retries
    std::set<std::string> directs;  // direct and consult-to-agent entries waiting for it
    std::string busy_queue;       // lever (b): the queue of its offer or live reservation (derived)
    // Phase 8 (campaign lending): the mode flag, this registry its only owner. Empty: MODE_INBOUND;
    // else MODE_CAMPAIGN, lent to this campaign (no inbound active set, never offered a call, out of
    // the model and the logged-in counts).
    std::string campaign;
    // A switch waiting for the agent to be idle (its offer resolved, its wrap-up over): to
    // to_campaign (a lend) or, when that is empty, back to inbound (a return).
    bool mode_pending = false;
    std::string to_campaign;
    // The availability last sent to the campaign's leader (not in re.state: a stream open resends).
    int avail_sent = -1;
    int64_t avail_wrap_sent = 0;
  };
  // Phase 8: a campaign and its leader (routing.v1 CampaignOwner).
  struct Campaign {
    std::string id, replica;
    uint64_t owner_epoch = 0;
  };
  struct Cmd {
    std::string fingerprint;
    int64_t at = 0;
    std::optional<Error> error;
    std::string response;  // serialized
  };

  // helpers: owners and fences
  // The interaction under this owner. A higher owner_epoch rebinds it to the owner's replica,
  // re-stamps every unacknowledged message with the new fence and replays them (CR-054);
  // *rebound says whether that happened.
  Result<Interaction*> owned(const rv1::InteractionOwner& o, bool create, int64_t now,
                             bool* rebound = nullptr);
  void restamp(Interaction& ix);
  void replay_ie(Interaction& ix, int64_t now, bool new_owner = false);
  bool holds(const Interaction& ix) const;
  void lose_owner(Interaction& ix, int64_t now);
  void replay_gw(AgentSt& a, int64_t now);
  uint64_t ie_gen(const std::string& replica) const;
  uint64_t gw_gen(const std::string& replica) const;
  Result<AgentSt*> session(const av1::SessionRef& s);
  template <class Resp>
  std::optional<Result<Resp>> repeat(const std::string& cmd_id, const std::string& fp);
  template <class Resp>
  Result<Resp> remember(const std::string& cmd_id, const std::string& fp, Result<Resp> r, int64_t now);

  // messages
  void send_ie(Interaction& ix, rv1::WatchResponse m, int64_t now, bool replayable = true);
  void send_gw(AgentSt& a, av1::WatchResponse m, int64_t now);
  void compact_outbox(AgentSt& a);
  rv1::EventTarget target_of(const Interaction& ix) const;
  av1::AgentView view(const AgentSt& a) const;
  rv1::QueueEntry entry_view(const Entry& e, int64_t now) const;
  av1::Reservation res_view(const Reservation& r) const;

  // state
  void set_agent_state(AgentSt& a, av1::AgentState s, const std::string& reason, int64_t now,
                       bool echo);
  void become_free(AgentSt& a, int64_t now, bool echo);
  void ask(AgentSt& a, av1::TargetState target, const std::string& reason, int64_t now,
           bool echo);
  void refresh_agent(AgentSt& a);
  void refresh_entry(Entry& e);
  bool agent_available(const AgentSt& a) const;
  bool entry_available(const Entry& e) const;

  // phase 4: active sets
  using Set = AgentSt::Set;
  bool eligible(const AgentSt& a, const std::string& queue) const;
  bool excluded(const AgentSt& a, const std::string& queue) const;
  Tier tier_of(const AgentSt& a, const std::string& queue) const;
  bool serves_now(const AgentSt& a, const Entry& e) const;
  const Set& effective(const AgentSt& a) const { return a.pending_set ? *a.pending_set : a.active; }
  Set desired(const AgentSt& a, int64_t now) const;
  bool pinned(const AgentSt& a, int64_t now) const;
  bool uncovers(const AgentSt& a, const Set& from, const Set& to) const;
  // Sets the agent's active set to desired(): at once when it holds no offer and no call, else as
  // its pending set. With check_coverage, a change that would uncover a queue is refused (false).
  bool retarget(AgentSt& a, av1::MembershipReason why, int64_t now, bool check_coverage);
  void apply_pending(AgentSt& a, int64_t now);
  void send_queues(AgentSt& a, av1::MembershipReason why, int64_t now);
  bool try_move(AgentSt& a, const std::map<std::string, Tier>& to, const std::map<std::string, double>* w,
                av1::MembershipReason why, int64_t now, std::string* why_not, int* outcome);
  // Phase 5 redesign.
  void stall_check(int64_t now);
  void apply_policy_to_queues();
  void revert_levers();               // config defaults for every queue's weight and delay
  void rearm_delays(int64_t now);     // waiting entries get timers for their queue's delay in force
  bool levers_on() const { return policy_.mode == RoutingMode::kAuto; }
  int bounds_rank(const QueueSt& q) const;  // lever (b): 2 below min, 0 at or above max, else 1
  bool vip_agent(const AgentSt& a) const;
  // A VIP-eligible agent is kept for VIP callers: a normal entry of the queue before its delay while
  // no more than the queue's vip_reserved_min VIP-eligible agents are READY for it.
  bool reserve_holds(const AgentSt& a, const Entry& e) const;
  int ready_vip(const QueueSt& q) const;
  int64_t reject_skip_ms() const { return policy_.reject_skip_ms >= 0 ? policy_.reject_skip_ms : cfg_.reject_skip_ms; }
  void event_state(const AgentSt& a, int64_t now);
  void event_queues(const AgentSt& a, const Set& before, av1::MembershipReason why, int64_t now);

  // phase 8: log-record.v1 (a call-scoped record's correlation; agent.selected's decision)
  LogRec& lc(LogRec& r, const std::string& interaction) const;
  std::string decision_of(const Entry& e, const AgentSt& chosen, bool sampled, int64_t now) const;
  uint64_t decisions_ = 0;

  // phase 8: campaign lending
  // The mode the agent is in, or switching to (desired() and coverage count a pending lend as gone).
  const std::string& mode_target(const AgentSt& a) const { return a.mode_pending ? a.to_campaign : a.campaign; }
  Result<Campaign*> campaign_owned(const rv1::CampaignOwner& o, bool create);
  rv1::LendResult lend_one(AgentSt& a, const std::string& campaign, int64_t now);
  rv1::ReturnResult return_one(AgentSt& a, const std::string& campaign, int64_t now);
  // At an idle point (set_agent_state to a free state): a pending switch applies.
  void apply_mode(AgentSt& a, int64_t now);
  // A logout ends a lending (pending or not); the agent is inbound again at its next login.
  void end_lending(AgentSt& a, int64_t now);
  rv1::AvailabilityState availability(const AgentSt& a, int64_t* wrap_ends) const;
  // Tells the campaign's leader when the agent's availability changed (deduplicated).
  void note_availability(AgentSt& a);
  void send_availability(AgentSt& a, const std::string& campaign, rv1::AvailabilityState st, int64_t wrap_ends);
  void send_campaign_snapshot(const Campaign& c);
  void lent_list(const std::string& campaign, google::protobuf::RepeatedPtrField<rv1::AgentAvailability>* out) const;
  void event_mode(const AgentSt& a, const std::string& campaign, const char* reason, int64_t now);

  // phase 3
  void send_run_callback(const std::string& callback_id, int64_t now);
  void on_callback_timer(const TimerRef& t, int64_t now);
  void callback_ended(const std::string& callback_id, rv1::Outcome o, int64_t now);
  void requeue_callback(const std::string& callback_id, int64_t now);
  void create_callback_entry(CallbackRecord& c, int64_t now);
  void persist_callback(const std::string& callback_id);  // best effort
  void dispatch_callback(Entry& e, Reservation& r, int64_t now);
  void on_dispatch_persisted(const std::string& callback_id, const std::string& reservation_id, bool ok, int64_t now);
  void on_convert_persisted(const std::string& callback_id, bool ok, int64_t now, const CallbackDone& done);
  void claim_callback(Interaction& ix, int64_t now);
  void flag_callback(CallbackRecord& c, const std::string& why, int64_t now);

  // dispatch
  bool meets(const AgentSt& a, const std::vector<SkillReq>& reqs) const;
  bool eligible_queue(const AgentSt& a, const QueueSt& q, bool escalated) const;
  bool can_serve(const AgentSt& a, const Entry& e) const;
  bool skip_active(const Entry& e, const std::string& agent, int64_t now) const;
  AgentSt* pick_agent(const Entry& e, int64_t now);
  Entry* pick_entry(AgentSt& a, int64_t now);
  void settle(int64_t now);
  void make_offer(Entry& e, AgentSt& a, int64_t now);
  void expire_offer(const std::string& offer_id, int64_t now);
  void revoke_offer(const std::string& offer_id, av1::RevokeReason why, int64_t now);
  void accept(const std::string& offer_id, int64_t now, av1::Reservation* out);
  void reject(const std::string& offer_id, int64_t now, bool to_ready);

  // entries and reservations
  Entry& create_entry(Interaction& ix, const rv1::EnqueueRequest& r, const QueueSt& q, int64_t now);
  void remove_entry(Entry& e, int64_t now);
  void entry_back(Entry& e, int64_t now);
  void overflow(Entry& e, int64_t now);
  void end_reservation(Reservation& r, rv1::Outcome o, int64_t now);
  void start_wrap_up(Reservation& r, int64_t now);
  void finish_wrap_up(Reservation& r, int64_t now, bool echo, const std::string& disposition = {});
  void positions(const std::string& queue, int64_t now);
  void direct_positions(const std::string& agent, int64_t now);
  uint32_t position_of(const Entry& e) const;
  uint32_t expected_wait_s(const Entry& e, uint32_t position, int64_t now,
                           rv1::ExpectedWaitQuality* quality = nullptr) const;
  // Eligible agents attached and logged in for this entry (its queue and requirements).
  int eligible_logged_in(const QueueSt& q, const Entry& e) const;
  void left_waiting(Entry& e, int64_t now, bool abandoned);
  void apply_ack(Interaction& ix, uint64_t acked);

  std::string id_;
  TenantConfig cfg_;
  const Epochs* epochs_;
  Sink* sink_;
  TimerSink* timers_;
  Counters* counters_;
  bool draining_ = false;
  uint64_t order_seq_ = 0;

  std::map<std::string, QueueSt> queues_;
  std::map<std::string, AgentSt> agents_;
  std::unordered_map<std::string, Interaction> interactions_;
  // Phase 7 (CR-187): the interactions that may still hold something (an entry, a live reservation,
  // unacknowledged messages): the 1 s redelivery and orphan scans walk these, not every interaction
  // kept for its 10-minute replay window. A superset, pruned lazily by those scans.
  std::unordered_set<std::string> held_ix_;
  void hold_ix(const std::string& id) { held_ix_.insert(id); }
  std::unordered_map<std::string, Entry> entries_;
  std::unordered_map<std::string, Offer> offers_;
  std::unordered_map<std::string, Reservation> reservations_;
  std::unordered_map<std::string, Cmd> commands_;
  std::set<std::pair<int64_t, std::string>> ready_idle_;  // (idle_since, agent): longest idle first
  // Phase 7 (CR-186): where the 1 s sweep's bounded slice of ready_idle_ resumes (round robin).
  std::pair<int64_t, std::string> sweep_cursor_;
  static constexpr size_t kSweepSlice = 2048;
  std::set<XKey> escalated_;
  std::set<std::string> skip_entries_;
  std::deque<std::string> todo_entries_;
  std::deque<std::string> todo_agents_;
  // Per replica: the stream generation this tenant has replayed to (CR-053).
  std::unordered_map<std::string, uint64_t> ie_gen_, gw_gen_;
  // CR-083, per IE replica: since when it has had no live stream / a live one; its last RPC.
  std::unordered_map<std::string, int64_t> replica_gone_since_, replica_live_since_, replica_rpc_;
  bool test_no_owner_loss_ = false;
  bool test_drop_overflow_ = false;  // the drop_overflow fault: overflows happen, IE never hears
  // Phase 2.
  bool model_ewt_ = false;
  bool position_throttle_ = true;
  // Phase 4.
  RoutingPolicy policy_;
  std::map<std::string, RoutingPin> pins_;  // per agent
  std::map<std::string, uint64_t> pin_versions_;  // the newest pin or tombstone version per agent
  // Phase 5 redesign: the lever plan held (re.state kind `opt`) and the levers it put in force.
  struct Opt {
    uint64_t version = 0;
    int64_t computed_at = 0, valid_until = 0;
    uint8_t quality = 0;
    int stage = 0;  // 0 fresh (or none), 1 stale: config defaults in force
    std::map<std::string, std::pair<double, int64_t>> applied;  // queue -> (weight, delay) set by the plan
  };
  Opt opt_;
  bool optimizer_on_ = false;
  // Phase 3.
  Durable* durable_ = nullptr;
  EventSink* events_ = nullptr;
  std::map<std::string, Campaign> campaigns_;  // phase 8: by campaign id
  std::map<std::string, CallbackRecord> callbacks_;  // open callbacks, by callback_id
  std::map<std::string, std::string> callback_by_entry_;  // original entry_id -> callback_id
  std::map<std::string, TaskRecord> tasks_by_recording_;  // made in this incarnation (repeats)
  int64_t bucket_start_ = 0;
  uint64_t instance_ = 0;
  std::shared_ptr<const EwtPlan> ewt_plan_;
  int64_t last_snapshot_ = 0;
  std::shared_ptr<const ArrivalGuard> guard_;  // phases 5b+5c
  int64_t last_early_ms_ = 0;
  void count_offered_10s(BucketRow* b, int64_t now);
  uint64_t agents_version_ = 0;  // bumps when any agent's logged-in eligibility changes
  mutable std::map<std::pair<std::string, std::string>, std::pair<uint64_t, int>> sig_count_;
  std::chrono::steady_clock::time_point task_start_{};

  // Phase 6: what changed since the last collect, by kind (ids; the values are read at collect).
  struct Dirty {
    std::set<std::string> agent, entry, offer, res, out, cmd, pin, cb, queue, camp;
    bool plan = false;
    bool opt = false;
  };
  Dirty dirty_;
  bool track_ = false;
  bool paranoid_ = false;
  bool reconciled_ = true;  // false between a restore and reconcile_callbacks
  uint64_t tenant_epoch_ = 0;
  std::unordered_map<std::string, uint64_t> written_;  // key -> hash of the record body written last
  std::vector<std::string> unmarked_;
  void touch_agent(const std::string& id) { if (track_) dirty_.agent.insert(id); }
  void touch_entry(const std::string& id) { if (track_) dirty_.entry.insert(id); }
  void touch_offer(const std::string& id) { if (track_) dirty_.offer.insert(id); }
  void touch_res(const std::string& id) { if (track_) dirty_.res.insert(id); }
  void touch_ix(const std::string& id) { if (track_) dirty_.out.insert(id); }
  void touch_cmd(const std::string& id) { if (track_) dirty_.cmd.insert(id); }
  void touch_pin(const std::string& id) { if (track_) dirty_.pin.insert(id); }
  void touch_cb(const std::string& id) { if (track_) dirty_.cb.insert(id); }
  void touch_queue(const std::string& id) { if (track_) dirty_.queue.insert(id); }
  void touch_camp(const std::string& id) { if (track_) dirty_.camp.insert(id); }
  void touch_plan() { if (track_) dirty_.plan = true; }
  void touch_opt() { if (track_) dirty_.opt = true; }
  void touch_all();
  std::string state_key(const char* kind, const std::string& id) const;
  // Fills the record's body for the object, false when it doesn't exist (a tombstone).
  bool encode(const std::string& kind, const std::string& id, ::parity::cc_routing::state::v1::Record* r) const;
  void emit(const std::string& kind, const std::string& id, std::vector<StateRec>* out, int64_t now);
  void rearm(int64_t now);
  // True while the message may be sent: a RunCallback without its number (re.state never carries
  // it) waits for reconcile_callbacks.
  static bool sendable(const rv1::WatchResponse& m) { return !m.has_run_callback() || !m.run_callback().number().empty(); }
};

}  // namespace pcr
