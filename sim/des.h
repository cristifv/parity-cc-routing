// The discrete-event simulator (PLAN §3.6, `//sim:des`): many queues and skill groups, Poisson
// arrivals with a piecewise rate (thinning), exponential, lognormal or fixed talk, wrap-up and
// patience, and THE REAL DISPATCH: every decision is made by //src/core's pcr::Tenant (the same
// code the engine runs: offers, longest idle, SLA-normalized age, escalation, timers), driven on
// a virtual clock with simulated IE and agent clients that answer at once. Replications with seeds.
//
// What it measures per queue, over arrivals in the measurement window: service level (answered
// within tau / (offered - abandons under 5 s)), abandonment, the mean wait of answered callers, and
// the mean EWT: the virtual wait of every arrival (the wait it would have had if it never hung up).
// For a caller who is answered that is its wait; for one who hangs up it is resolved from the same
// trajectory (FCFS within a queue: it would have been offered the first agent that, after everyone
// ahead of it left, took a later caller of its queue or found its queue empty).
#pragma once

#include <cstdint>
#include <functional>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace pcr {
class Tenant;
}

namespace pcr::sim {

struct Dist {
  enum class Kind { kExponential, kLognormal, kFixed };
  Kind kind = Kind::kFixed;
  double mean = 0;  // seconds
  double cv = 0;    // lognormal: coefficient of variation
  double sample(std::mt19937_64& rng) const;
  static Dist exp(double mean) { return Dist{Kind::kExponential, mean, 1}; }
  static Dist lognormal(double mean, double cv) { return Dist{Kind::kLognormal, mean, cv}; }
  static Dist fixed(double v) { return Dist{Kind::kFixed, v, 0}; }
};

struct QueueSpec {
  std::string id;
  double bucket_s = 900;
  std::vector<double> per_s;  // arrival rate per bucket (calls per second); the last one holds
  Dist talk, wrap = Dist::fixed(0), patience;
  double sla_s = 20;
  // Phase 5 redesign: the VIP class (Enqueue caller_class VIP, this share of the arrivals) and the
  // queue's config.routing part (lever a's default weight, lever b's bounds, the VIP reserve).
  double vip_share = 0;
  double vip_sla_s = 0;  // 0: sla_s
  double weight = 1;
  int min_agents = 0, max_agents = 0, vip_reserved_min = 0;
  double alpha = 1, beta = 1, vip_alpha = 2, vip_beta = 2;  // config.routing importance (J)
  double max_wait_s = 86'400;  // a finite max wait overflows with DISCONNECT (counted per class)
  // Phases 5b+5c (the forecaster's OOD suite): bots (an extra rate per bucket; such a caller hangs up
  // after bot_patience_s and talks bot_talk_s) and redials (a caller who abandons calls again with
  // probability redial_p after an exponential delay: arrivals that depend on service).
  std::vector<double> bot_per_s;
  double bot_patience_s = 3, bot_talk_s = 5;
  double redial_p = 0, redial_delay_s = 120;
  double rate(double t) const;
  double bot_rate(double t) const;
};

// `count` agents eligible for `queues` (membership and a skill per queue, so escalation stays
// inside the membership). Login: all at t = 0, or spread linearly over [login_from_s, login_to_s].
// Phase 5 redesign: each of the `count` agents is its own agent (the model counts them one by one);
// `speed` (per queue, optional) divides its talk time on that queue (configured proficiency: the
// agent's skill level for the queue is set so config.routing's proficiency_speed maps it to this);
// `vip`: it holds the VIP skill.
struct GroupSpec {
  GroupSpec() = default;
  GroupSpec(int n, std::vector<std::string> q, double from = 0, double to = 0)
      : count(n), queues(std::move(q)), login_from_s(from), login_to_s(to) {}
  int count = 0;
  std::vector<std::string> queues;
  double login_from_s = 0, login_to_s = 0;
  std::vector<double> speed;
  bool vip = false;
};

struct Scenario {
  std::string name;
  std::vector<QueueSpec> queues;
  std::vector<GroupSpec> groups;
  double warmup_s = 3600;
  double measure_s = 3600;
  double drain_s = 1800;  // arrivals go on (not counted) so the window's EWTs resolve
  double short_abandon_s = 5;
  int64_t check_every_s = 300;  // pcr::Tenant::check_invariants on the virtual clock
  // Phase 4's dispatch (PLAN §4.1, §8 row 4): each multi-queue group's first queue PRIMARY, the
  // others SECONDARY (supervisor pins: fixed sets), secondaries after this delay. False: phase 1's
  // dispatch (every member queue primary, d = 0).
  bool tiers = false;
  int64_t secondary_delay_ms = 10'000;
  // Phase 5 redesign: config.routing mode (0 off, 1 advise, 2 auto: lever b in the dispatch) and,
  // every hook_every_s of virtual time, a hook on the tenant (the closed loop: snapshot -> optimizer
  // -> install_levers), called on the simulation's thread.
  int mode = 0;
  std::function<void(pcr::Tenant*, int64_t now_ms)> hook;
  double hook_every_s = 10;
  int64_t start_ms = 0;  // phases 5b+5c: epoch ms of t = 0 (0: the DES's default start)
};

struct QueueStats {
  uint64_t offered = 0, answered = 0, answered_in_sla = 0, abandoned = 0, short_abandoned = 0;
  double wait_answered_sum = 0;
  double wait_all_sum = 0;  // answered and abandoned: the exponential patience MLE's denominator
  double virtual_wait_sum = 0;
  uint64_t virtual_n = 0, virtual_unresolved = 0;
  double sl(double short_ab_excluded = true) const;
  double abandon() const { return offered ? static_cast<double>(abandoned) / offered : 0; }
  double asa() const { return answered ? wait_answered_sum / answered : 0; }
  double ewt() const { return virtual_n ? virtual_wait_sum / virtual_n : 0; }
};

struct RepResult {
  uint64_t redials = 0, bots = 0;       // phases 5b+5c
  std::vector<QueueStats> q;
  std::vector<QueueStats> normal, vip;  // the same split by caller class
  std::vector<uint64_t> overflowed;     // per queue: max-wait overflows (measured window)
  uint64_t past_max_wait = 0;           // callers still waiting 2 s past their max wait (a bug)
  double max_wait_seen_s = 0;           // the longest wait of any caller who left waiting (answered or overflowed)
  uint64_t calls = 0, offers = 0, invariant_checks = 0, invariant_violations = 0, sweep_matches = 0;
  uint64_t engine_errors = 0;
  std::vector<std::string> violations;  // first few
  double wall_s = 0;
};

RepResult run_replication(const Scenario& sc, uint64_t seed);

// Mean and 95 % confidence half-width over replications (Student t).
struct Estimate {
  double mean = 0, half = 0;
};
Estimate estimate(const std::vector<double>& x);

}  // namespace pcr::sim
