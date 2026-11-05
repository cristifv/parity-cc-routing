// The CTMC library (PLAN §3): one queue's birth–death chain (stationary in log space, transient by
// uniformization with Fox–Glynn weights), the tagged-customer chain (EWT by position, P(served),
// service level, short abandons) and the per-queue metrics with their quality and truncation mass.
// No I/O, no locks, no global state: a caller passes its own Stats. Every number it returns has
// passed the boundary check (finite, probabilities in [0, 1]); a result that can't pass is refused
// (ok = false, with the reason), never published.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pcr::model {

// Exact: the chain's own numbers. Approx: a cap, a fallback or the coupling made it an estimate.
// Overloaded: no stationary law (theta = 0, rho >= 1): transient numbers only. Long: the wait has
// no finite estimate (no servers, or higher-priority arrivals outpace service); EWT then carries a
// lower bound. Ordered by severity; a combined result takes the worst.
enum class Quality : uint8_t { kExact = 0, kApprox = 1, kOverloaded = 2, kLong = 3 };
const char* quality_name(Quality q);
inline Quality worst(Quality a, Quality b) { return a > b ? a : b; }

// Per solver thread (the optimization layer owns one per thread; tests their own).
struct Stats {
  uint64_t clamps = 0;   // negative probabilities from round-off set to 0 (and > 1 set to 1)
  uint64_t refused = 0;  // results refused at the boundary (NaN/inf, lost mass, truncation)
  uint64_t solves = 0;
};

struct QueueParams {
  double lambda = 0;     // arrivals per second
  double mu = 0;         // per server: 1 / (talk + wrap-up), per second
  double theta = 0;      // abandonment rate of a waiting caller, 1 / mean patience
  double s = 0;          // effective servers (real; results interpolate between floor and ceil)
  double s_tag = -1;     // servers that serve this queue's head next (tagged progress); < 0: s
  double lambda_hp = 0;  // higher-priority arrivals that pass a waiting caller, per second
};

struct Options {
  double tail_eps = 1e-9;      // truncation: stationary tail mass at most this (else approx)
  double tail_target = 1e-17;  // K grows until the dropped tail is below double precision
  double refuse_tail = 1e-6;   // refused above this
  int k_cap = 20000;           // past it: approx (mean-value numbers only)
  double fg_eps = 1e-10;       // Fox–Glynn truncation
  double max_lambda_t = 1e5;   // past it: steady state only, approx (CR-029: no Krylov solver)
  double sla_s = 20;           // tau, the service-level threshold
  double short_abandon_s = 5;  // abandons faster than this are left out of the SL denominator
  double long_wait_s = 3600;   // the lower bound reported for a `long` wait (the queue's max wait)
  double mass_tol = 1e-9;      // |sum p - 1| allowed before truncation loss
};

// ---- Poisson weights (Fox–Glynn) ----
struct PoissonWeights {
  int left = 0, right = 0;  // weights for k in [left, right]
  std::vector<double> w;    // normalized: sum = 1
};
// Left/right truncation for total lost mass <= eps; computed from the mode outwards, relative to
// the mode, so nothing underflows for large lambda*t (> 745).
PoissonWeights fox_glynn(double lambda_t, double eps);

// ---- one queue's birth–death chain (integer servers) ----
// Death rate at n >= 1: mu * min(n, s) + theta * max(0, n - s).
double down_rate(const QueueParams& q, int s, int n);

struct Distribution {
  std::vector<double> p;    // p[n], n = 0..K
  double tail_mass = 0;     // mass beyond K (a bound)
  Quality quality = Quality::kExact;
  bool ok = false;
  std::string why;          // when !ok or approx
};
// The stationary law, products in log space, K the smallest bound with tail <= tail_eps.
// theta = 0 with rho >= 1: no stationary law (ok = false, overloaded).
Distribution stationary(const QueueParams& q, int s, const Options& o, Stats* st);
// p(t) from p0 (its size fixes K; births at K are blocked) by uniformization with
// Lambda = max_n (lambda + down_rate(n)), arrivals included (CR-026).
Distribution transient(const QueueParams& q, int s, const std::vector<double>& p0, double t,
                       const Options& o, Stats* st);
// The truncation for a transient from the current state n0 over horizon t: lambda*t + 8 sqrt(lambda*t)
// above n0, and at least the stationary bound where one exists.
int transient_k(const QueueParams& q, int s, int n0, double t, const Options& o);

// ---- the tagged customer (PLAN §3.4) ----
// A caller at position k (k - 1 ahead) moves up at r_j = s_tag mu + (j - 1) theta - lambda_hp and
// abandons at theta. Index k - 1 holds position k.
struct Tagged {
  std::vector<double> V;   // EWT: the wait of a caller who stays, sum 1/r_j
  std::vector<double> Ps;  // P(served)
  std::vector<double> u;   // P(served within sla_s)
  std::vector<double> a;   // P(abandoned within short_abandon_s)
  std::vector<double> m;   // E[wait * 1{served}] (ASA)
  Quality quality = Quality::kExact;
};
Tagged tagged(double s_tag, double mu, double theta, double lambda_hp, int kmax, const Options& o,
              Stats* st);
// The general tagged customer (phase 5, the per-agent model): r[k] is the rate at which a caller at
// position k + 1 moves up (any positive rates: weights scale them, higher-priority arrivals reduce
// them) and theta_own its own patience. A non-positive r[0] gives a `long` table.
Tagged tagged_rates(const std::vector<double>& r, double theta_own, const Options& o, Stats* st);
// EWT by position only (closed form, real s_tag): the announcement table.
std::vector<double> ewt_by_position(double s_tag, double mu, double theta, double lambda_hp,
                                    int kmax, double long_wait_s, Quality* q);

// ---- metrics ----
struct QueueMetrics {
  bool ok = false;
  std::string why;
  Quality quality = Quality::kExact;
  double truncation_mass = 0;
  double s = 0;
  double sl = 0;          // answered within tau / (offered - abandons under short_abandon_s)
  double abandon = 0;     // P(an arrival abandons)
  double ewt_mean = 0;    // mean EWT (virtual wait) of a new caller
  double asa = 0;         // mean wait of answered callers
  double p_wait = 0;      // P(an arrival must wait)
  double busy = 0;        // E[servers busy]
  double lq = 0;          // E[waiting]
  double mean_wait_all = 0;  // Lq / lambda (every arrival)
};
// Stationary metrics; real s by interpolation; overloaded chains fall back to a transient over
// `horizon_s` from an empty queue (quality overloaded).
QueueMetrics stationary_metrics(const QueueParams& q, const Options& o, Stats* st,
                                double horizon_s = 900);
// Metrics for arrivals over [0, horizon] starting from n0 calls (the control loop's view),
// averaged over `points` instants (arrival-weighted: lambda is constant per bucket).
QueueMetrics transient_metrics(const QueueParams& q, int n0, double horizon_s, int points,
                               const Options& o, Stats* st);
// The same from a distribution p0 over calls present (integer servers: q.s is rounded), e.g. the
// stationary law before a burst; *p_end (optional) receives the law at the horizon, so piecewise
// buckets (a burst, a login ramp) chain.
QueueMetrics transient_from(const QueueParams& q, const std::vector<double>& p0, double horizon_s,
                            int points, const Options& o, Stats* st, std::vector<double>* p_end);
// The boundary: every field finite, probabilities clamped into [0, 1] (counted); a NaN or inf, or
// a truncation mass above refuse_tail, refuses the result (counted). Applied by the functions
// above before they return.
bool check_boundary(QueueMetrics* m, const Options& o, Stats* st);

}  // namespace pcr::model
