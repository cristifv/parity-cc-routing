// The per-agent model (phase 5 redesign, the user 2026-11-01; PLAN §3.1, §3.2): no skill groups.
// Every agent counts individually: what it takes now (its active set, eligible and not excluded),
// the tier of each of those queues, the queues' dispatch weights and its configured speed per queue
// (proficiency) decide its capacity share for each queue, recomputed every tick.
//
// The maths is the pooled reach of CR-087, per agent. Queues that share agents are solved as one
// pool (a pool's waits equalize under the real dispatch): each connected set of queues (linked by
// agents that take several of them) is split by complete resource pooling into pools (a parametric
// max flow from queues to agents; agents with the same queues are one node of the flow, an exact
// shortcut since the flow sees only capacities). A pool's chain has the pool's arrivals, its agents
// as servers and the arrival-weighted handle time at its agents' speeds; a queue that only its own
// agents serve is a pool of its own, so it is one chain per queue. On top of the pool:
//   - light traffic per queue: a call is served at once only if one of the agents that may take it
//     (R of them; under tiers its P primaries first, secondaries after the delay d) is idle;
//   - weights and SL thresholds (lever a): the dispatch takes the highest w x age / tau, so a pool's
//     queues wait in proportion to its time-dependent priority (Kleinrock's conservation formulas
//     on the all-busy regime): queue i's waiting callers progress at the pool's rate / kappa_i, with
//     kappa = 1 for equal w / tau (then exactly the pooled model);
//   - VIP callers (a priority class per queue, served before normal callers at equal escalation):
//     the pool's count chain is class-blind (exact for equal handle times and patience); VIP callers
//     queue only behind VIP callers (a birth-death chain of the VIP queue on the all-busy regime),
//     normal callers progress at the pool's rate minus the VIP arrivals that pass them (PLAN §3.4);
//   - per-agent shares x(a, i): each agent's busy time split over its queues (a scaling of the
//     pool's carried load onto agents, by tier and weight), so busy(i) = sum_a x(a, i).
//
// Quality (`approx` never changes weights or delays in auto): as the pooled model (split pools,
// nested reach, dispersion of handle times, patience or speeds, near-critical load, the tiers'
// secondary leg), plus: lever (b) bounds that bind, a VIP reserve (not modelled), weight ratios or
// VIP shares beyond what the §3.6 matrix validated. The thresholds are set from the matrix.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include "model/ctmc.h"

namespace pcr::model {

enum class Dispatch : uint8_t { kLongestIdle, kTiers };

struct AQueue {
  double lambda = 0;      // arrivals per second, VIP included
  double vip_lambda = 0;  // the VIP part of lambda
  double mu = 0;          // per agent at speed 1: 1 / (talk + wrap-up)
  double theta = 0;       // patience rate of a waiting caller
  double delay_s = 0;     // d_i (tiers): secondaries take a call after it waited this long
  double sla_s = 0;       // tau_i; 0: the options' sla_s
  double vip_sla_s = 0;   // the VIP threshold; 0: sla_s
  double weight = 1;      // lever (a): the queue's dispatch weight
  double escalate_s = 0;  // escalate_after: older entries go first, oldest first (0: 3 x sla_s)
  int min_agents = 0, max_agents = 0;  // lever (b): only to tell whether they bind (quality)
  int vip_reserved = 0;   // VIP-eligible agents held back for VIP callers (not modelled: approx)
};

struct AAgent {
  std::vector<int> queues;     // the queues it takes now (active set, eligible, not excluded)
  std::vector<uint8_t> tiers;  // per queue: 1 primary, 2 secondary (longest idle: ignored)
  std::vector<double> speeds;  // per queue: relative speed from configured proficiency (1 nominal)
  bool vip = false;            // VIP-eligible (the tenant's VIP skill)
};

struct AgentOptions {
  Dispatch dispatch = Dispatch::kLongestIdle;
  Options q;
  double near_critical = 0.95;
  double dispersion = 0.25;     // handle times, patience or agent speeds of a pool differing more
  double tier_shift = 0.10;     // tiers: the left-out handicap above this
  double tier_overflow = 0.05;  // tiers: more calls than this take the secondary-delay leg
  double weight_ratio = 4.0;    // a pool's w / tau differing by more than this factor
  // Kleinrock's waits per queue overstate the weights' effect under the real dispatch, where entries
  // past escalate_after go first and FCFS: kappa^weight_damping (fitted on the §3.6 weight cells).
  double weight_damping = 0.35;
  double vip_load = 0.5;        // VIP offered load above this share of the pool's agents
};

struct Share {
  int agent = 0, queue = 0;
  double x = 0;  // the share of the agent's time busy on the queue
};

struct AgentPool {
  std::vector<int> queues, agents;
  int c = 0;                         // agents
  double speed = 1;                  // their mean speed
  double lambda = 0, vip_lambda = 0;
  double mu = 0, theta = 0, load = 0;  // mu at the agents' speeds; load = offered agents
  double busy = 0;                   // E[agents busy]
  bool split = false;                // its connected set had no complete resource pooling
  Quality quality = Quality::kExact;
  std::string why;                   // why approx (the first reason)
};

struct AgentQueue {
  QueueMetrics m;       // every caller of the queue (both classes, by arrivals)
  QueueMetrics normal;  // normal callers
  QueueMetrics vip;     // VIP callers (ok = false without VIP traffic)
  int pool = -1;
  double P = 0;      // agents with it PRIMARY (longest idle: = R)
  double R = 0;      // agents that may take it
  double busy = 0;   // agents busy on it: the sum of the per-agent shares
  double kappa = 1;  // its waits relative to the pool's FCFS waits (weights, thresholds)
  double handicap = 0, overflow = 0;  // tiers
  bool nested = false;
  std::string why;  // why approx (the first reason)
};

struct AgentResult {
  std::vector<AgentQueue> q;
  std::vector<AgentPool> pools;
  std::vector<Share> shares;
  Quality quality = Quality::kExact;
  bool ok = false;
  std::string why;
};

class AgentModel {
 public:
  AgentModel(std::vector<AQueue> queues, std::vector<AAgent> agents, AgentOptions o, Stats* st);
  const AgentResult& base() const { return base_; }
  // Lever (a): every queue's metrics with other weights and delays (one per queue, the queues'
  // order). Who may take what doesn't change, so the pools and their chains are reused; tagged
  // tables are memoized. Shares are not recomputed.
  std::vector<AgentQueue> evaluate(const std::vector<double>& weights, const std::vector<double>& delays);
  // An entry of queue i that only `able` of the queue's R agents may take (extra requirements,
  // CR-033): its metrics (normal class). Exact when able = R (the queue's own), else approx.
  QueueMetrics signature(int i, double able);
  // Queue i's position table for announcements: the servers that serve its head next and the
  // patience of those ahead (ewt_by_position's inputs), per class.
  struct Tag {
    double s_tag = 0, mu = 0, theta = 0, lambda_hp = 0;
  };
  Tag tag(int i, bool vip) const;
  size_t evaluations() const { return evals_; }
  const std::vector<AQueue>& queues() const { return queues_; }

 private:
  struct PoolCache {
    Distribution d;
    bool ok = false;
    double pi_busy = 0;                 // P(all agents busy)
    std::vector<double> vip_pos;        // VIP arrival, all busy: P(position k + 1)
  };
  void solve_pool(size_t pi);
  std::vector<double> kappas(const AgentPool& p, const std::vector<double>& weights) const;
  AgentQueue queue_metrics(int i, double kappa, double delay_s, double esc_pool);
  // P(a normal caller of queue i is still waiting at its escalate_after): the escalated normal
  // callers that go ahead of VIP callers (escalation sits above the class).
  double still_at_escalation(int i, double kappa);
  const Tagged& table(size_t pi, double scale, double minus, double theta_own, double sla_s, double short_s = -1);

  std::vector<AQueue> queues_;
  std::vector<AAgent> agents_;
  AgentOptions o_;
  Stats* st_;
  AgentResult base_;
  std::vector<PoolCache> cache_;
  // Tagged tables per (rate scale, arrivals passing, own patience, threshold, short-abandon time, pool).
  std::map<std::tuple<long, long, long, long, long, size_t>, Tagged> tables_;
  size_t evals_ = 0;
};

// The pools a set of queues and agents splits into (connected sets, then complete resource pooling).
std::vector<AgentPool> agent_pools(const std::vector<AQueue>& queues, const std::vector<AAgent>& agents);

// Convenience: base() of a model built from these inputs.
AgentResult solve_agents(const std::vector<AQueue>& queues, const std::vector<AAgent>& agents,
                         const AgentOptions& o, Stats* st);

// J (PLAN §3.4): missed-SL and abandoned calls per second, weighted by importance, both classes.
struct Importance {
  double alpha = 1, beta = 1, vip_alpha = 1, vip_beta = 1;
};
double objective(const std::vector<AQueue>& q, const std::vector<AgentQueue>& m, const std::vector<Importance>& imp);

}  // namespace pcr::model
