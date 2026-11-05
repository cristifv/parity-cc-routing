// What the real-time layer hands the optimization layer and gets back (PLAN §5.1, §5.3): an
// immutable per-tenant snapshot built by the tenant's writer every model tick, and a versioned EWT
// plan the optimizer posts back into the tenant's inbox. Plain data: //src/core does not depend on
// //model, and nothing here crosses tenants (a plan names its tenant; another tenant refuses it).
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace pcr {

struct ModelSnapshot {
  std::string tenant;
  int64_t at_ms = 0;
  int64_t window_ms = 0;  // since the previous snapshot
  struct Queue {
    std::string id;
    uint64_t arrivals = 0;   // entries that joined the queue in the window
    uint64_t abandons = 0;   // waiting entries the caller abandoned
    double waited_s = 0;     // time waited by every entry that left waiting (theta's MLE)
    double aht_s = 0;        // EWMA of talk + wrap-up
    int64_t sla_ms = 0, max_wait_ms = 0;
    uint32_t waiting = 0;
    int logged_in = 0;       // eligible agents logged in and attached
    bool priority_mix = false;  // entries of different priorities in the window (CR-098)
    // Phase 5 (PLAN §5.3, §5.4): the forecaster's and the levers' inputs, queue.events' numbers.
    double wrap_s = 0;           // EWMA of wrap-up alone (aht_s includes it)
    double head_age_s = 0;       // the oldest waiting entry's age
    uint64_t vip_arrivals = 0;   // VIP entries that joined in the window
    uint64_t handled = 0;        // calls whose wrap-up ended in the window
    double handle_s = 0;         // ... their handle time (talk + wrap-up)
    uint32_t vip_waiting = 0;
    int busy_agents = 0;         // agents offered, reserved, on a call or in wrap-up for this queue
    double alpha = 1, beta = 1;  // config.routing importance
    int min_agents = 0, max_agents = 0;
    // Lever (a): in force now, the config defaults and the bounds.
    double weight = 1, weight_default = 1, weight_min = 0.25, weight_max = 4;
    int64_t secondary_delay_ms = 10'000, delay_default_ms = 10'000, delay_min_ms = 0, delay_max_ms = 60'000;
    double sl_target = 0.8;
    int64_t vip_sla_ms = 0;
    double vip_alpha = 2, vip_beta = 2;
    int vip_reserved_min = 0;
    uint64_t bucket_offered = 0, bucket_answered = 0, bucket_in_sla = 0, bucket_abandoned = 0;  // this 15-min bucket
    int64_t bucket_start_ms = 0;
  };
  std::vector<Queue> queues;
  // Entries with extra requirements (CR-033): the share of the queue's logged-in agents that meet them.
  struct Sig {
    int queue = 0;
    std::string sig;
    double share = 0;
  };
  std::vector<Sig> sigs;
  // Phase 5 redesign: every agent individually (no groups). Logged in and attached only; never any
  // performance number (EU AI Act: configured proficiency, counts and times only).
  uint8_t mode = 0;           // RoutingMode: 0 off, 1 advise, 2 auto
  uint64_t plan_version = 0;  // the lever plan the writer holds (a new optimizer starts above it)
  int plan_stage = 0;         // 0 fresh (or none yet), 1 stale: config defaults in force (PLAN §6)
  bool tiers = false;         // some agent's active set has a SECONDARY queue (the model's dispatch)
  struct Agent {
    std::string id;
    bool counts = false;         // in N: logged in, attached, not NOT_READY (busy and wrap-up count)
    uint8_t activity = 0;        // 0 ready and idle, 1 offered or reserved, 2 on a call, 3 wrap-up, 4 not ready
    std::vector<std::pair<int, uint8_t>> set;  // what it takes now: its effective set (queue index, Tier),
                                               // eligible and not excluded
    std::vector<double> speed;   // per `set` entry: configured proficiency speed (1 nominal)
    std::vector<int> eligible;   // every queue it is a member of and eligible for, not excluded
    bool vip = false;            // holds the tenant's VIP skill
    bool pinned = false;
  };
  std::vector<Agent> agents;
};

struct EwtTable {
  std::vector<double> by_position;  // seconds, for positions 1..size
  double step_s = 0;                // added per position beyond the table
  bool long_wait = false;           // no estimate: the time left to max wait, a lower bound
  uint8_t quality = 0;              // model::Quality (exact, approx, overloaded, long)
};

// Phase 5 redesign (PLAN §5.3): the optimizer's lever plan for one tenant. The writer applies it only
// in auto mode, and per queue only where the queue's result is exact (`approx` never changes weights
// or delays), clamped to config.routing's bounds; a stale plan reverts to config defaults (PLAN §6).
struct LeverPlan {
  std::string tenant;
  uint64_t version = 0;
  int64_t computed_at_ms = 0, valid_until_ms = 0;
  uint8_t quality = 0;  // model::Quality of the worst queue
  struct Queue {
    std::string id;
    double weight = 1;
    int64_t delay_ms = 0;
    uint8_t quality = 0;  // this queue's result (current and planned): only exact applies
  };
  std::vector<Queue> queues;
};

// Phases 5b+5c (PLAN §5.9, the event-driven re-forecast): the arrivals per second the optimizer's
// forecast in use expects per queue. The writer's 1 s periodic compares the arrivals since the last
// snapshot with it and hands the tenant's snapshot over early on a significant deviation. Only with
// the learned forecaster on.
struct ArrivalGuard {
  std::string tenant;
  int64_t computed_at_ms = 0, valid_until_ms = 0;
  std::vector<std::pair<std::string, double>> per_s;
};

// The EWT plan's table key for the VIP class of a queue (no requirement signature starts with '@').
inline constexpr char kVipSig[] = "@vip";

struct EwtPlan {
  std::string tenant;
  uint64_t version = 0;
  int64_t computed_at_ms = 0, valid_until_ms = 0;
  std::map<std::pair<std::string, std::string>, EwtTable> tables;  // (queue, requirement signature)
};

}  // namespace pcr
