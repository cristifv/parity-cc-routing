// Fluid-coupled per-queue chains (PLAN §3.2 step 2, review M4): several queues share a multi-skill
// pool; each queue keeps its own 1-D chain and the chains are coupled through the agents one queue
// lends another. Lent capacity is MOVED, never copied: a server-equivalent busy on a lent call
// counts on the receiver and is taken from the donor, so sum(s_i) = sum(P_i) holds and is asserted
// after every solve (a failure makes the result approx).
//
// Phase-2 build note (measured against the exact N-design chain, model/oracle.h): the round-1
// estimate of the lent capacity (a share of the donor's mean idle) understated how long lent agents
// stay on the receiver's calls (they finish the call even after the receiver's own agents free up).
// The lent busy is therefore taken from a small exact chain of the receiver's own and lent servers
// (own preferred, lent kept until the call ends), and the donor's idle servers stay *reachable* by
// the receiver (pooled: idle, never busy, on both sides). Each queue's metrics use its reachable
// capacity c_i = s_i + its donors' idle share; a donor's waiting callers are served next by every
// one of its primaries (its lent agents come back to it first), so its tagged progress uses
// s_i + its lent-out busy.
#pragma once

#include <map>
#include <utility>
#include <vector>

#include "model/ctmc.h"

namespace pcr::model {

struct CQueue {
  double lambda = 0, mu = 0, theta = 0;  // as QueueParams
  double lambda_hp = 0;
};

// A skill group: agents with the same eligible queues. `primaries[k]` of them have `queues[k]`
// as their primary (sum <= agents); the group's other eligible queues are their secondaries,
// weighted equally. Speeds e(g,i) are 1 in phase 2 (configured proficiency comes with phase 5).
struct CGroup {
  double agents = 0;
  std::vector<int> queues;
  std::vector<double> primaries;
};

// How agents pick work (PLAN §4.2). kPrimaryFirst: tiers (phase 4+): a call goes to its queue's own
// agents first, and a lent agent goes back to its primary when it frees. kLongestIdle: phase 1's
// dispatch (every member queue PRIMARY): a call goes to the longest-idle eligible agent whatever
// its group, and a freed agent takes the oldest head among its queues.
enum class Policy : uint8_t { kPrimaryFirst, kLongestIdle };

struct CouplingOptions {
  Policy policy = Policy::kPrimaryFirst;
  bool conserve = true;      // false: the round-1 non-conserving formula (negative tests only)
  double damping = 0.5;
  double tol = 0.01;         // stop when every s and reach moves by less
  int max_rounds = 20;
  double approx_share = 0.30;  // more of a queue's capacity from secondaries: approx
  // Offered load at or above this share of a queue's capacity: approx. Set by the §3.6 matrix's
  // lognormal cells (2026-10-31): within 5 SL points at every size up to rho 0.9, 12-16 points off
  // at rho 1.0 with 200-1,000 agents (the patience distribution near zero dominates there).
  double near_critical = 0.95;
  int max_block = 64;          // the lent-busy chain's largest level; above: estimate, approx
  Options q;
  // Warm start: the lent busy (nq x nq, row = donor) of the tenant's previous solve, when its
  // queues are the same; consecutive ticks then converge in a round or two.
  const std::vector<double>* warm = nullptr;
};

struct CoupledQueue {
  double P = 0;         // primaries (server-equivalents)
  double s = 0;         // P + lent in - lent out (busy-equivalents; sums to sum P)
  double reach = 0;     // donors' idle servers this queue may take (pooled)
  double c = 0;         // s + reach: the capacity its chain sees
  double s_tag = 0;     // servers that serve its head next
  double lent_in = 0, lent_out = 0;
  double secondary_share = 0;  // (c - own) / c
  double lent_out_share = 0;   // lent_out / P: the share of its primaries busy on other queues
  QueueMetrics m;
};

struct CouplingResult {
  std::vector<CoupledQueue> q;
  int rounds = 0;
  bool converged = false;
  bool conserved = false;
  double conservation_error = 0;  // |sum s - sum P|
  Quality quality = Quality::kExact;
  bool ok = false;
  std::string why;
  std::vector<double> lent;  // nq x nq, row = donor: for the next solve's warm start
};

// Phase 2's allocation without an optimizer (PLAN §4.1: an agent takes every member queue): each
// multi-queue group's agents count as primaries of its queues in proportion to the load the
// single-queue groups leave uncovered (its other queues are then their secondaries). Phase 5's
// allocator replaces this.
void split_by_load(const std::vector<CQueue>& queues, std::vector<CGroup>* groups);

CouplingResult solve_coupled(const std::vector<CQueue>& queues, const std::vector<CGroup>& groups,
                             const CouplingOptions& o, Stats* st);

// The receiver's lent-busy chain: `own` own servers (preferred by arrivals) and `lent` borrowed
// ones (released only when their call ends and nothing waits), exponential talk (mu) and patience
// (theta). Returns E[borrowed servers busy]. Exact (level reduction) for integer sizes; real sizes
// interpolate. *approx is set when a level exceeds max_block (a cheaper estimate is used).
// `memo` (optional) caches the integer chains of one queue within a solve.
double lent_busy(double lambda, double mu, double theta, double own, double lent, int max_block,
                 bool* approx, bool prefer_own = true, std::map<std::pair<int, int>, double>* memo = nullptr);

}  // namespace pcr::model
