// Basic types of the real-time layer: clock, errors, configuration (PLAN §4.2, §8 "Config in
// phase 1").
#pragma once

#include <atomic>
#include <cstdint>
#include <expected>
#include <map>
#include <string>
#include <vector>

namespace pcr {

// Milliseconds since the Unix epoch. The real-time layer never reads the system clock directly,
// so tests drive time (PLAN §8 1.4).
class Clock {
 public:
  virtual ~Clock() = default;
  virtual int64_t now_ms() const = 0;
  // True for a test clock: timers fire only when it is advanced.
  virtual bool manual() const { return false; }
};

class SystemClock final : public Clock {
 public:
  int64_t now_ms() const override;
};

class ManualClock final : public Clock {
 public:
  explicit ManualClock(int64_t start_ms) : t_(start_ms) {}
  int64_t now_ms() const override { return t_.load(std::memory_order_acquire); }
  bool manual() const override { return true; }
  void set(int64_t t) { t_.store(t, std::memory_order_release); }

 private:
  std::atomic<int64_t> t_;
};

// The canonical gRPC status codes RE answers with (numeric values = grpc::StatusCode).
enum class Code : int {
  kOk = 0,
  kInvalidArgument = 3,
  kNotFound = 5,
  kAlreadyExists = 6,
  kResourceExhausted = 8,
  kFailedPrecondition = 9,
  kAborted = 10,
  kUnimplemented = 12,
  kInternal = 13,
  kUnavailable = 14,
};

// An error: the status code plus the parity-error trailer (the ErrorCode name, lowercased,
// without its prefix).
struct Error {
  Code code = Code::kInternal;
  std::string parity;   // e.g. "stale_owner"; empty for none
  std::string message;  // for the status message; never personal data
};

template <class T>
using Result = std::expected<T, Error>;

inline std::unexpected<Error> err(Code c, std::string parity, std::string msg = {}) {
  return std::unexpected<Error>(Error{c, std::move(parity), std::move(msg)});
}

// What a queue does at max_wait_s (routing.v1 OverflowAction): QUEUE (L14), VOICEMAIL, DISCONNECT or
// FLOW_STEP (L13: OVERFLOWED until Dequeue(OVERFLOW); IE runs the action), and (phase 3)
// CALLBACK_OFFER (L15: the entry keeps its place; IE offers the caller a callback).
enum class MaxWaitAction { kQueue = 1, kVoicemail = 2, kCallbackOffer = 3, kDisconnect = 4, kFlowStep = 5 };

// A queue's tier in an agent's active set (PLAN §4.1; agents.v1 MembershipTier).
enum class Tier : uint8_t { kNone = 0, kPrimary = 1, kSecondary = 2 };

// The tenant's routing mode (PLAN §4.3, config.routing; phase 5 redesign): off = phase 4's
// behaviour (configured sets, config weights and delays); advise = the optimizer only recommends
// (parity.staffing); auto = levers (a) weights and delays and (b) min/max agents apply by themselves.
// No mode moves agents between queues automatically: pins stay a supervisor's.
enum class RoutingMode : uint8_t { kOff = 0, kAdvise = 1, kAuto = 2 };

struct SkillReq {
  std::string skill_id;
  uint32_t min_level = 0;  // 0: any level
  auto operator<=>(const SkillReq&) const = default;
};

struct QueueConfig {
  std::string id;
  std::vector<SkillReq> required;   // skills every agent of the queue needs
  uint32_t default_priority = 50;   // for Enqueue priority 0
  int64_t offer_timeout_ms = 15000;
  int64_t wrap_up_limit_ms = 60000;
  int64_t sla_ms = 20000;           // tau: the service-level threshold
  int64_t escalate_after_ms = 60000;  // default 3 tau
  int64_t max_wait_ms = 0;          // required (> 0)
  MaxWaitAction max_wait_action = MaxWaitAction::kDisconnect;
  std::string max_wait_queue;       // for kQueue
  std::string max_wait_flow_step;   // for kFlowStep
  uint32_t max_entries = 1000;      // queue_full above
  int64_t aht_ms = 180000;          // handle-time default for the wait estimate
};

struct AgentConfig {
  std::string id;
  std::string contact;  // the softphone URI; the gateway's AttachAgent value wins
  std::map<std::string, uint32_t> skills;
  std::vector<std::string> queues;  // explicit membership (CR-017)
};

// config.routing's per-queue part (PLAN §4.5; phase 5 redesign: the levers and VIP, 0.12.0).
struct QueuePolicy {
  std::string queue_id;
  double alpha = 1, beta = 1;      // the optimizer's importance weights (J, PLAN §3.4)
  // Lever (b), auto mode: an agent at its next READY prefers a queue with fewer than min_agents
  // agents busy on it, and takes a queue with max_agents or more busy on it only when it has nothing
  // else (never idling past a delay: PLAN §4.2's bounded wait stands). 0: no bound.
  int min_agents = 0;
  int max_agents = 0;
  int64_t secondary_delay_ms = 10'000;  // d_i's default: secondaries take an entry after it waited this long
  int64_t escalate_after_ms = 0;   // > 0 overrides the queue's escalate_after
  // Lever (a): the queue's dispatch weight w_i (w x age / tau) and delay d_i in force are the
  // optimizer's within these bounds (auto, exact results only), else these defaults.
  double weight = 1;
  double weight_min = 0.25, weight_max = 4;
  int64_t delay_min_ms = 0, delay_max_ms = 60'000;
  // Lever (c): staffing recommendations ask for agents while the predicted SL is below this.
  double sl_target = 0.8;
  // VIP callers (Enqueue caller_class VIP): their own threshold (0: the queue's) and importance in J;
  // VIP-eligible agents (RoutingPolicy.vip_skill) keep this many READY for VIP callers (a normal
  // call reaches them only past d_i).
  int64_t vip_sla_ms = 0;
  double vip_alpha = 2, vip_beta = 2;
  int vip_reserved_min = 0;
};

// config.routing: one document per tenant (PLAN §4.3, §4.5).
struct RoutingPolicy {
  uint64_t version = 0;
  RoutingMode mode = RoutingMode::kOff;
  int64_t dwell_min_ms = 300'000;  // D_min: 5 min (1-60 min)
  int max_moves_per_hour = 4;      // 0-12
  int64_t reject_skip_ms = -1;     // >= 0 overrides the tenant's reject_skip
  std::vector<QueuePolicy> queues;
  // Phase 5 redesign: VIP-eligible agents hold this skill (empty: any agent; no reserve).
  std::string vip_skill;
  // Configured proficiency -> relative speed (the model's per-agent speed per queue): (min level,
  // speed), ascending by level; an agent's speed on a queue is the last step its level of the
  // queue's skills reaches. Empty: every agent at speed 1. Never measured per person (AI Act, CR-022).
  std::vector<std::pair<uint32_t, double>> proficiency_speed;
};

// config.routing_pin: one document per (tenant, agent) (PLAN §4.3, CR-023). A pin with queues sets
// the agent's active set (the optimizer treats it as fixed capacity); exclusions keep queues out of
// any set the agent gets. until_ms 0: until unpinned.
struct RoutingPin {
  std::string agent_id;
  uint64_t version = 0;
  std::vector<std::pair<std::string, Tier>> queues;
  std::vector<std::string> exclusions;
  int64_t until_ms = 0;
  std::string reason_code;
};

struct TenantConfig {
  std::string id;
  std::vector<QueueConfig> queues;
  std::vector<AgentConfig> agents;
  RoutingPolicy routing;          // phase 4: config.routing (the seed's until admin-console publishes)
  std::vector<RoutingPin> pins;   // phase 4: config.routing_pin documents
  // Phase 3: a direct offer (Target.agent_id) waits at most this long, then this action
  // (VOICEMAIL, DISCONNECT or FLOW_STEP; L13).
  int64_t direct_max_wait_ms = 60'000;
  MaxWaitAction direct_max_wait_action = MaxWaitAction::kVoicemail;
  std::string direct_max_wait_flow_step;
  // Phase 3: callbacks. RunCallback.deadline_ms = the agent's accept + this; a RunCallback without
  // an ack goes to another IE stream after 5 s, never past the deadline + 10 s (CONVENTIONS).
  int64_t callback_deadline_ms = 120'000;
  int callback_max_attempts = 3;              // CUSTOMER_NO_ANSWER reschedules until this many
  int64_t callback_retry_after_ms = 900'000;  // 15 min between attempts
  int64_t reject_skip_ms = 30000;          // PLAN §4.2
  int64_t assignment_deadline_ms = 30000;  // Assigned.deadline_ms = accept + this
  int64_t grace_ms = 60000;                // detach grace
  // CR-083: an IE owner gone this long (no stream and no RPC, or a message unacknowledged this long
  // on a live stream) loses its calls here: entries, offers and reservations end, agents are freed.
  // At least the L7 adoption budget (10 s).
  int64_t owner_loss_ms = 60000;
};

// RE-wide numbers that a restart must never reuse (PLAN §8 restart rule).
struct Epochs {
  uint64_t leader_epoch = 1;
  uint64_t seq_base = 0;  // (incarnation - 1) * 2^32; 0 in seeded tests
};

bool valid_tenant_id(const std::string& s);
bool valid_uuid(const std::string& s);
bool valid_queue_id(const std::string& s);
bool valid_agent_id(const std::string& s);
bool valid_command_id(const std::string& s);
// A UUIDv7 (time-ordered, random tail): entry, offer and reservation ids.
std::string new_uuid_v7(int64_t now_ms);
// Tests only (CR-049: reproducible scenario runs for the invariant search): from now on every
// UUIDv7 tail comes from one process-wide counter mixed with `seed` instead of a random source,
// so the same calls in the same order give the same ids. 0 switches back to random tails.
void set_deterministic_ids(uint64_t seed);

}  // namespace pcr
