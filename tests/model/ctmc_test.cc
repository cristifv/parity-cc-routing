// PLAN §7 "CTMC numerics" (phase 2). The golden table (argv[1]) is a sanity check of the solver
// only (PLAN §2, §3.6; CR-027: 1e-6 relative or 1e-12 absolute).
#include <gtest/gtest.h>

#include <Eigen/Dense>
#include <cmath>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <unsupported/Eigen/MatrixFunctions>
#include <vector>

#include "model/coupling.h"
#include "model/ctmc.h"
#include "model/oracle.h"

using namespace pcr::model;

namespace {

std::string g_golden;

constexpr double kAht = 180, kPatience = 90, kTau = 20;

bool close_rel(double want, double got, double rel, double abs_tol) {
  return std::abs(want - got) <= std::max(abs_tol, rel * std::abs(want));
}

// The §3.6 gate: SL within 3 points, abandonment within 1 point or 15 %, mean EWT within 15 % or 5 s.
std::string gate(const char* what, double sl_ref, double sl, double ab_ref, double ab, double ewt_ref,
                 double ewt) {
  std::string bad;
  if (std::abs(sl_ref - sl) > 0.03) bad += std::string(" ") + what + ".sl";
  if (std::abs(ab_ref - ab) > 0.01 && std::abs(ab_ref - ab) > 0.15 * ab_ref) bad += std::string(" ") + what + ".abandon";
  if (std::abs(ewt_ref - ewt) > 5 && std::abs(ewt_ref - ewt) > 0.15 * ewt_ref) bad += std::string(" ") + what + ".ewt";
  return bad;
}

Options opts() {
  Options o;
  o.sla_s = kTau;
  return o;
}

}  // namespace

// ---- closed forms ----

TEST(ClosedForm, MM1) {
  const double lam = 0.5, mu = 1.0, rho = lam / mu;
  Stats st;
  const Distribution d = stationary(QueueParams{lam, mu, 0, 1}, 1, opts(), &st);
  ASSERT_TRUE(d.ok);
  for (int n = 0; n < 30; ++n) EXPECT_NEAR(d.p[n], (1 - rho) * std::pow(rho, n), 1e-12) << n;
  const QueueMetrics m = stationary_metrics(QueueParams{lam, mu, 0, 1}, opts(), &st);
  ASSERT_TRUE(m.ok) << m.why;
  EXPECT_EQ(m.quality, Quality::kExact);
  EXPECT_NEAR(m.p_wait, rho, 1e-9);
  EXPECT_NEAR(m.lq, rho * rho / (1 - rho), 1e-8);
  EXPECT_NEAR(m.ewt_mean, rho / (mu - lam), 1e-8);  // FCFS, nobody leaves: Wq
  EXPECT_NEAR(m.mean_wait_all, rho / (mu - lam), 1e-8);
  EXPECT_NEAR(m.sl, 1 - rho * std::exp(-(mu - lam) * kTau), 1e-8);
  EXPECT_EQ(m.abandon, 0);
}

TEST(ClosedForm, MMInfinity) {
  const double lam = 3, mu = 1;
  Stats st;
  const Distribution d = stationary(QueueParams{lam, mu, 0, 400}, 400, opts(), &st);
  ASSERT_TRUE(d.ok);
  for (int n = 0; n < 25; ++n) {
    const double pois = std::exp(-3.0 + n * std::log(3.0) - std::lgamma(n + 1.0));
    EXPECT_NEAR(d.p[n], pois, 1e-12) << n;
  }
  const QueueMetrics m = stationary_metrics(QueueParams{lam, mu, 0, 400}, opts(), &st);
  EXPECT_NEAR(m.busy, lam / mu, 1e-9);
  EXPECT_NEAR(m.p_wait, 0, 1e-12);
  EXPECT_NEAR(m.sl, 1, 1e-12);
}

TEST(ClosedForm, MMs) {
  const double lam = 2.4, mu = 1;
  const int s = 3;
  const double a = lam / mu;
  // The probability of waiting, from the textbook sums.
  double sum = 0;
  for (int k = 0; k < s; ++k) sum += std::pow(a, k) / std::tgamma(k + 1.0);
  const double top = std::pow(a, s) / std::tgamma(s + 1.0) * s / (s - a);
  const double pw = top / (sum + top);
  Stats st;
  const QueueMetrics m = stationary_metrics(QueueParams{lam, mu, 0, static_cast<double>(s)}, opts(), &st);
  ASSERT_TRUE(m.ok);
  EXPECT_NEAR(m.p_wait, pw, 1e-9);
  EXPECT_NEAR(m.ewt_mean, pw / (s * mu - lam), 1e-8);
  EXPECT_NEAR(m.sl, 1 - pw * std::exp(-(s * mu - lam) * kTau), 1e-8);
  EXPECT_NEAR(m.busy, a, 1e-9);
}

// ---- the golden table ----

TEST(Golden, EveryRowMatches) {
  std::ifstream in(g_golden);
  ASSERT_TRUE(in.good()) << g_golden;
  std::string line;
  std::getline(in, line);
  ASSERT_EQ(line, "source,N,lambda,mu,theta,asa_s,abandon,p_delay");
  int rows = 0, bad = 0;
  Stats st;
  while (std::getline(in, line)) {
    std::stringstream ss(line);
    std::string src, f;
    std::getline(ss, src, ',');
    std::vector<double> v;
    while (std::getline(ss, f, ',')) v.push_back(std::stod(f));
    ASSERT_EQ(v.size(), 7u) << line;
    const int N = static_cast<int>(v[0]);
    const double lam = v[1], mu = v[2], th = v[3];
    const Distribution d = stationary(QueueParams{lam, mu, th, static_cast<double>(N)}, N, opts(), &st);
    ASSERT_TRUE(d.ok) << line;
    EXPECT_LE(d.tail_mass, 1e-9);
    double lq = 0, pd = 0;
    for (size_t n = 0; n < d.p.size(); ++n) {
      if (static_cast<int>(n) >= N) {
        lq += d.p[n] * (static_cast<double>(n) - N);
        pd += d.p[n];
      }
    }
    const double asa = lq / lam, ab = th * lq / lam;
    const bool ok = close_rel(v[4], asa, 1e-6, 1e-12) && close_rel(v[5], ab, 1e-6, 1e-12) &&
                    close_rel(v[6], pd, 1e-6, 1e-12);
    if (!ok) {
      ++bad;
      ADD_FAILURE() << line << " -> asa " << asa << " abandon " << ab << " p_delay " << pd;
    }
    // The tagged-customer abandonment equals Little's theta Lq / lambda for the exact chain.
    if (th > 0) {
      const QueueMetrics m = stationary_metrics(QueueParams{lam, mu, th, static_cast<double>(N)}, opts(), &st);
      ASSERT_TRUE(m.ok);
      EXPECT_TRUE(close_rel(ab, m.abandon, 1e-6, 1e-12)) << line << " tagged " << m.abandon;
    }
    ++rows;
  }
  EXPECT_EQ(rows, 378);
  EXPECT_EQ(bad, 0);
  std::printf("golden: %d rows, %d off\n", rows, bad);
}

// ---- mass, monotonicity, edge cases ----

TEST(Numerics, MassIsConserved) {
  Stats st;
  for (double th : {0.0, 1 / 60.0, 1 / 600.0}) {
    for (int s : {1, 4, 30}) {
      const double lam = 0.9 * s / kAht;
      const Distribution d = stationary(QueueParams{lam, 1 / kAht, th, double(s)}, s, opts(), &st);
      ASSERT_TRUE(d.ok);
      double sum = 0;
      for (double p : d.p) sum += p;
      EXPECT_NEAR(sum, 1, 1e-12);
      std::vector<double> p0(d.p.size(), 0.0);
      p0[std::min<size_t>(3, p0.size() - 1)] = 1;
      const Distribution t = transient(QueueParams{lam, 1 / kAht, th, double(s)}, s, p0, 600, opts(), &st);
      ASSERT_TRUE(t.ok) << t.why;
      double tsum = 0;
      for (double p : t.p) tsum += p;
      EXPECT_NEAR(tsum, 1, 1e-9);
    }
  }
}

TEST(Numerics, MonotoneInServers) {
  Stats st;
  const double lam = 10 / kAht;
  double sl = -1, ab = 2;
  for (double s = 6; s <= 18; s += 0.25) {
    const QueueMetrics m = stationary_metrics(QueueParams{lam, 1 / kAht, 1 / kPatience, s}, opts(), &st);
    ASSERT_TRUE(m.ok) << s;
    EXPECT_GE(m.sl, sl - 1e-12) << s;
    EXPECT_LE(m.abandon, ab + 1e-12) << s;
    sl = m.sl;
    ab = m.abandon;
  }
}

TEST(Numerics, NoArrivalsMeanNoWait) {
  Stats st;
  const QueueMetrics m = stationary_metrics(QueueParams{0, 1 / kAht, 1 / kPatience, 3}, opts(), &st);
  ASSERT_TRUE(m.ok);
  EXPECT_EQ(m.ewt_mean, 0);
  EXPECT_EQ(m.abandon, 0);
  EXPECT_EQ(m.sl, 1);
}

TEST(Numerics, NoServersIsLong) {
  Stats st;
  Options o = opts();
  o.long_wait_s = 300;
  const QueueMetrics m = stationary_metrics(QueueParams{0.1, 1 / kAht, 1 / kPatience, 0}, o, &st);
  ASSERT_TRUE(m.ok);
  EXPECT_EQ(m.quality, Quality::kLong);
  EXPECT_EQ(m.sl, 0);
  EXPECT_EQ(m.abandon, 1);
  EXPECT_EQ(m.ewt_mean, 300);  // a lower bound, never 0 (CR-013)
  Quality q;
  const auto v = ewt_by_position(2, 0.1, 0.01, 0.5, 5, 300, &q);  // higher priority outpaces service
  EXPECT_EQ(q, Quality::kLong);
  for (double x : v) EXPECT_EQ(x, 300);
}

TEST(Numerics, ThetaZero) {
  Stats st;
  // rho < 1: a stationary law with a geometric tail.
  const QueueMetrics a = stationary_metrics(QueueParams{4.5 / kAht, 1 / kAht, 0, 5}, opts(), &st);
  ASSERT_TRUE(a.ok);
  EXPECT_EQ(a.quality, Quality::kExact);
  EXPECT_LE(a.truncation_mass, 1e-9);
  EXPECT_EQ(a.abandon, 0);
  // rho >= 1: no stationary law; transient numbers, quality overloaded, finite.
  const Distribution d = stationary(QueueParams{6 / kAht, 1 / kAht, 0, 5}, 5, opts(), &st);
  EXPECT_FALSE(d.ok);
  EXPECT_EQ(d.quality, Quality::kOverloaded);
  const QueueMetrics b = stationary_metrics(QueueParams{6 / kAht, 1 / kAht, 0, 5}, opts(), &st, 900);
  ASSERT_TRUE(b.ok) << b.why;
  EXPECT_EQ(b.quality, Quality::kOverloaded);
  EXPECT_TRUE(std::isfinite(b.ewt_mean));
  EXPECT_GT(b.ewt_mean, 0);
}

TEST(Numerics, TruncationMass) {
  Stats st;
  const Distribution d = stationary(QueueParams{12 / kAht, 1 / kAht, 1 / 600.0, 10}, 10, opts(), &st);
  ASSERT_TRUE(d.ok);
  EXPECT_LE(d.tail_mass, 1e-9);
  EXPECT_GT(d.tail_mass, 0);
  // A cap far too small: the tail can't be bounded under the refusal limit.
  Options o = opts();
  o.k_cap = 12;
  const QueueMetrics m = stationary_metrics(QueueParams{12 / kAht, 1 / kAht, 1 / 600.0, 10}, o, &st);
  EXPECT_FALSE(m.ok);
  EXPECT_GT(st.refused, 0u);
}

TEST(Numerics, BoundaryRefusesNaNAndCountsClamps) {
  Stats st;
  const QueueMetrics bad = stationary_metrics(QueueParams{std::nan(""), 1, 0, 1}, opts(), &st);
  EXPECT_FALSE(bad.ok);
  QueueMetrics m;
  m.ok = true;
  m.sl = 1 + 1e-9;  // round-off
  m.abandon = -1e-12;
  EXPECT_TRUE(check_boundary(&m, opts(), &st));
  EXPECT_EQ(m.sl, 1);
  EXPECT_EQ(m.abandon, 0);
  EXPECT_EQ(st.clamps, 2u);
  m.ewt_mean = std::numeric_limits<double>::infinity();
  const uint64_t refused = st.refused;
  EXPECT_FALSE(check_boundary(&m, opts(), &st));
  EXPECT_EQ(st.refused, refused + 1);
}

TEST(Numerics, RealServersInterpolate) {
  Stats st;
  const double lam = 4 / kAht;
  const QueueMetrics a = stationary_metrics(QueueParams{lam, 1 / kAht, 1 / kPatience, 4}, opts(), &st);
  const QueueMetrics b = stationary_metrics(QueueParams{lam, 1 / kAht, 1 / kPatience, 5}, opts(), &st);
  const QueueMetrics m = stationary_metrics(QueueParams{lam, 1 / kAht, 1 / kPatience, 4.25}, opts(), &st);
  EXPECT_NEAR(m.sl, 0.75 * a.sl + 0.25 * b.sl, 1e-12);
  EXPECT_NEAR(m.abandon, 0.75 * a.abandon + 0.25 * b.abandon, 1e-12);
}

// ---- uniformization and Fox–Glynn ----

TEST(Uniformization, MatchesDenseExpm) {
  const QueueParams q{0.3, 0.1, 0.05, 3};
  const int s = 3, K = 25;
  Eigen::MatrixXd Q = Eigen::MatrixXd::Zero(K + 1, K + 1);
  for (int n = 0; n <= K; ++n) {
    if (n < K) Q(n, n + 1) = q.lambda;
    if (n > 0) Q(n, n - 1) = down_rate(q, s, n);
    Q(n, n) = -Q.row(n).sum();
  }
  Stats st;
  for (double t : {0.5, 5.0, 30.0, 200.0}) {
    std::vector<double> p0(K + 1, 0.0);
    p0[2] = 1;
    const Distribution d = transient(q, s, p0, t, opts(), &st);
    ASSERT_TRUE(d.ok);
    Eigen::RowVectorXd e = Eigen::RowVectorXd::Zero(K + 1);
    e(2) = 1;
    const Eigen::RowVectorXd want = e * (Q * t).exp();
    double diff = 0;
    for (int n = 0; n <= K; ++n) diff = std::max(diff, std::abs(want(n) - d.p[n]));
    EXPECT_LE(diff, 1e-9) << "t=" << t;
  }
}

TEST(FoxGlynn, MatchesDirectWeightsForSmallLambdaT) {
  for (double lt : {0.01, 0.5, 3.0, 25.0, 120.0}) {
    const PoissonWeights fg = fox_glynn(lt, 1e-10);
    double kept = 0;
    for (int k = fg.left; k <= fg.right; ++k) kept += std::exp(-lt + k * std::log(lt) - std::lgamma(k + 1.0));
    EXPECT_GE(kept, 1 - 1e-10) << lt;  // what is dropped is within eps
    for (int k = fg.left; k <= fg.right; ++k) {
      const double direct = std::exp(-lt + k * std::log(lt) - std::lgamma(k + 1.0)) / kept;
      EXPECT_NEAR(fg.w[k - fg.left], direct, 1e-12) << lt << " k=" << k;
    }
  }
}

TEST(FoxGlynn, NoUnderflowForLargeLambdaT) {
  for (double lt : {800.0, 1e4, 1e5}) {
    const PoissonWeights fg = fox_glynn(lt, 1e-10);
    double sum = 0, mean = 0;
    for (int k = fg.left; k <= fg.right; ++k) {
      sum += fg.w[k - fg.left];
      mean += k * fg.w[k - fg.left];
    }
    EXPECT_NEAR(sum, 1, 1e-12) << lt;
    EXPECT_NEAR(mean, lt, 1e-6 * lt) << lt;
    EXPECT_GT(fg.left, 0);
  }
}

// ---- the tagged customer vs a simulated tagged caller ----

TEST(Tagged, MatchesSimulatedTaggedWaits) {
  const double s = 3, mu = 1 / kAht, th = 1 / kPatience;
  const int kmax = 8;
  Stats st;
  const Tagged t = tagged(s, mu, th, 0, kmax, opts(), &st);
  std::mt19937_64 rng(20261031);
  for (int k : {1, 3, 8}) {
    const int n = 200000;
    double served = 0, within = 0, wsum = 0, vsum = 0, short_ab = 0;
    for (int i = 0; i < n; ++i) {
      double w = 0, v = 0;
      bool ok = true;
      for (int j = k; j >= 1; --j) {  // the caller who stays: own patience off
        v += std::exponential_distribution<double>(s * mu + (j - 1) * th)(rng);
      }
      for (int j = k; j >= 1; --j) {
        const double r = s * mu + (j - 1) * th;
        w += std::exponential_distribution<double>(r + th)(rng);
        if (std::uniform_real_distribution<double>(0, 1)(rng) < th / (r + th)) {
          ok = false;
          break;
        }
      }
      if (ok) {
        served += 1;
        wsum += w;
        if (w <= kTau) within += 1;
      } else if (w <= 5) {
        short_ab += 1;
      }
      vsum += v;
    }
    auto expect_p = [&](double want, double got, const char* what) {
      const double se = std::sqrt(want * (1 - want) / n) + 1e-12;
      EXPECT_LE(std::abs(want - got), 5 * se) << what << " k=" << k << " model " << want << " sim " << got;
    };
    expect_p(t.Ps[k - 1], served / n, "P(served)");
    expect_p(t.u[k - 1], within / n, "P(served within tau)");
    expect_p(t.a[k - 1], short_ab / n, "P(abandoned within 5 s)");
    EXPECT_NEAR(t.V[k - 1], vsum / n, 0.01 * t.V[k - 1]) << "EWT k=" << k;
    EXPECT_NEAR(t.m[k - 1], wsum / n, 0.02 * t.m[k - 1] + 0.05) << "E[W served] k=" << k;
  }
}

// ---- the coupling ----

TEST(Coupling, ConservesAgents) {
  std::mt19937_64 rng(7);
  Stats st;
  for (int trial = 0; trial < 40; ++trial) {
    const int nq = 2 + trial % 3;
    std::vector<CQueue> qs;
    for (int i = 0; i < nq; ++i) {
      qs.push_back(CQueue{std::uniform_real_distribution<double>(1, 12)(rng) / kAht, 1 / kAht, 1 / kPatience, 0});
    }
    std::vector<CGroup> gs;
    for (int g = 0; g < 2; ++g) {
      CGroup cg;
      cg.agents = 3 + trial % 7;
      for (int i = 0; i < nq; ++i) {
        if (g == 0 ? i < 2 : i >= 1) cg.queues.push_back(i);
      }
      double left = cg.agents;
      for (size_t k = 0; k < cg.queues.size(); ++k) {
        const double x = k + 1 == cg.queues.size() ? left : std::floor(left / 2);
        cg.primaries.push_back(x);
        left -= x;
      }
      gs.push_back(cg);
    }
    const CouplingResult r = solve_coupled(qs, gs, CouplingOptions{}, &st);
    ASSERT_TRUE(r.ok) << r.why;
    double sum_s = 0, sum_p = 0;
    for (const auto& q : r.q) {
      sum_s += q.s;
      sum_p += q.P;
      EXPECT_GE(q.lent_in, 0);
      EXPECT_LE(q.lent_out, q.P + 1e-9);
    }
    EXPECT_TRUE(r.conserved);
    EXPECT_NEAR(sum_s, sum_p, 1e-9);  // agents moved, never copied
    CouplingOptions nc;
    nc.conserve = false;
    const CouplingResult bad = solve_coupled(qs, gs, nc, &st);
    if (bad.q[0].lent_in + bad.q.back().lent_in > 0.05) {
      EXPECT_FALSE(bad.conserved);  // the round-1 formula counts lent agents twice
      EXPECT_EQ(bad.quality, Quality::kApprox);
    }
  }
}

// ---- quality labels (CR-095): the rules the §3.6 matrix set, pinned so a changed rule fails ----

namespace {

CouplingResult solve_with(const std::vector<CQueue>& qs, std::vector<CGroup> gs, Policy p, bool split = true) {
  if (split) split_by_load(qs, &gs);
  CouplingOptions o;
  o.policy = p;
  o.q = opts();
  Stats st;
  return solve_coupled(qs, gs, o, &st);
}

}  // namespace

// Under phase 1's dispatch (longest idle) every queue that shares a group is approx: N and W designs.
TEST(CouplingQuality, SharedQueuesAreApproxUnderLongestIdle) {
  const double mu = 1 / kAht, th = 1 / kPatience;
  const std::vector<CQueue> qs{{0.7 * 10 * mu, mu, th, 0}, {0.7 * 10 * mu, mu, th, 0}};
  const CouplingResult n = solve_with(qs, {{10, {0}, {}}, {10, {1, 0}, {}}}, Policy::kLongestIdle);
  const CouplingResult w = solve_with(qs, {{10, {0}, {}}, {10, {0, 1}, {}}, {10, {1}, {}}}, Policy::kLongestIdle);
  for (const CouplingResult* r : {&n, &w}) {
    ASSERT_TRUE(r->ok) << r->why;
    EXPECT_TRUE(r->converged);
    EXPECT_TRUE(r->conserved);
    for (const auto& q : r->q) EXPECT_EQ(q.m.quality, Quality::kApprox);
    EXPECT_EQ(r->quality, Quality::kApprox);
  }
}

// A queue with only its own agents stays exact beside shared ones, wherever it sits in the list
// (CR-097: the tenant's approx used to leak into every queue after the first approx one).
TEST(CouplingQuality, ADedicatedQueueStaysExactInAnyOrder) {
  const double mu = 1 / kAht, th = 1 / kPatience;
  for (int pos = 0; pos < 3; ++pos) {
    std::vector<int> at(3);  // logical queue -> its index: 0 dedicated, 1 and 2 share a group
    at[0] = pos;
    for (int l = 1, k = 0; l < 3; ++l, ++k) {
      if (k == pos) ++k;
      at[l] = k;
    }
    std::vector<CQueue> qs(3);
    qs[at[0]] = {0.6 * 10 * mu, mu, th, 0};
    qs[at[1]] = {0.35 * 12 * mu, mu, th, 0};
    qs[at[2]] = {0.35 * 12 * mu, mu, th, 0};
    const CouplingResult r = solve_with(qs, {{10, {at[0]}, {}}, {12, {at[1], at[2]}, {}}}, Policy::kLongestIdle);
    ASSERT_TRUE(r.ok) << r.why;
    EXPECT_TRUE(r.converged);
    EXPECT_EQ(r.q[at[0]].m.quality, Quality::kExact) << "dedicated queue at index " << pos;
    EXPECT_EQ(r.q[at[1]].m.quality, Quality::kApprox) << pos;
    EXPECT_EQ(r.q[at[2]].m.quality, Quality::kApprox) << pos;
    EXPECT_EQ(r.quality, Quality::kApprox) << pos;  // the tenant's: its worst queue
  }
}

// Offered load at or above 0.95 of the capacity is approx (the lognormal cells), for both policies.
TEST(CouplingQuality, NearCriticalLoadIsApprox) {
  const double mu = 1 / kAht, th = 1 / kPatience;
  for (Policy p : {Policy::kLongestIdle, Policy::kPrimaryFirst}) {
    for (const auto& [rho, want] : std::vector<std::pair<double, Quality>>{
             {0.90, Quality::kExact}, {0.94, Quality::kExact}, {0.95, Quality::kApprox}, {1.10, Quality::kApprox}}) {
      const CouplingResult r = solve_with({{rho * 10 * mu, mu, th, 0}}, {{10, {0}, {}}}, p);
      ASSERT_TRUE(r.ok) << r.why;
      EXPECT_EQ(r.q[0].m.quality, want) << "rho " << rho;
      EXPECT_EQ(r.quality, want) << "rho " << rho;
    }
  }
}

// Under primary first (phase 4's tiers) a receiver stays exact up to 30 % of its capacity from
// secondaries and is approx above it; the donor (no capacity from others) stays exact.
TEST(CouplingQuality, PrimaryFirstSecondaryShareThreshold) {
  const double mu = 1 / kAht, th = 1 / kPatience;
  struct Case {
    double own, flex, rho0;
    bool over;
  };
  for (const Case& c : {Case{10, 4, 0.6, false}, Case{3, 10, 0.5, true}}) {
    const std::vector<CQueue> qs{{c.rho0 * c.own * mu, mu, th, 0}, {0.3 * c.flex * mu, mu, th, 0}};
    // Group A works queue 0 only; group B has queue 1 as its primary and queue 0 as a secondary.
    const CouplingResult r = solve_with(qs, {{c.own, {0}, {c.own}}, {c.flex, {1, 0}, {c.flex, 0}}},
                                        Policy::kPrimaryFirst, /*split=*/false);
    ASSERT_TRUE(r.ok) << r.why;
    EXPECT_TRUE(r.converged);
    EXPECT_TRUE(r.conserved);
    const double share = r.q[0].secondary_share;
    if (c.over) {
      EXPECT_GT(share, 0.30);
      EXPECT_EQ(r.q[0].m.quality, Quality::kApprox) << "share " << share;
    } else {
      EXPECT_GT(share, 0.0);  // it does receive capacity
      EXPECT_LE(share, 0.30);
      EXPECT_EQ(r.q[0].m.quality, Quality::kExact) << "share " << share;
    }
    EXPECT_EQ(r.q[1].m.quality, Quality::kExact);  // the donor
  }
}

namespace {

struct NCase {
  int n1, n2;
  double rho1, rho2;
};

CouplingResult coupled_n(const NCase& c, bool conserve) {
  const double mu = 1 / kAht, th = 1 / kPatience;
  std::vector<CQueue> qs{{c.rho1 * c.n1 * mu, mu, th, 0}, {c.rho2 * c.n2 * mu, mu, th, 0}};
  std::vector<CGroup> gs{{double(c.n1), {0}, {double(c.n1)}}, {double(c.n2), {1, 0}, {double(c.n2), 0}}};
  CouplingOptions o;
  o.conserve = conserve;
  o.q = opts();
  Stats st;
  return solve_coupled(qs, gs, o, &st);
}

NDesignResult exact_n(const NCase& c) {
  NDesign d;
  d.n1 = c.n1;
  d.n2 = c.n2;
  d.mu1 = d.mu2 = 1 / kAht;
  d.theta1 = d.theta2 = 1 / kPatience;
  d.lambda1 = c.rho1 * c.n1 * d.mu1;
  d.lambda2 = c.rho2 * c.n2 * d.mu2;
  d.sla_s = kTau;
  return solve_ndesign(d);
}

}  // namespace

// The oracle case that pins the coupling (PLAN §3.2, review M4): an N-design near rho = 1, solved
// exactly; the conserving model is within the §3.6 gate and the round-1 non-conserving formula is
// not (it counts lent agents twice and overstates the donor's service level).
TEST(Oracle, NDesignNearRhoOne) {
  for (const NCase& c : {NCase{5, 5, 1.05, 0.85}, NCase{5, 5, 0.95, 0.95}, NCase{4, 4, 1.0, 0.9}}) {
    const NDesignResult ex = exact_n(c);
    ASSERT_TRUE(ex.ok) << ex.why;
    const CouplingResult m = coupled_n(c, true);
    ASSERT_TRUE(m.ok) << m.why;
    EXPECT_TRUE(m.conserved);
    EXPECT_TRUE(m.converged);
    const auto& q1 = m.q[0].m;
    const auto& q2 = m.q[1].m;
    const std::string bad = gate("q1", ex.sl1, q1.sl, ex.abandon1, q1.abandon, ex.ewt1, q1.ewt_mean) +
                            gate("q2", ex.sl2, q2.sl, ex.abandon2, q2.abandon, ex.ewt2, q2.ewt_mean);
    EXPECT_EQ(bad, "") << "rho " << c.rho1 << "/" << c.rho2;
    EXPECT_NEAR(m.q[0].lent_in, ex.lent_busy, 0.15 * ex.lent_busy + 0.05);
    std::printf("N(%d,%d) rho %.2f/%.2f exact sl %.3f/%.3f ab %.3f/%.3f ewt %.1f/%.1f lent %.2f | model sl %.3f/%.3f "
                "ab %.3f/%.3f ewt %.1f/%.1f lent %.2f rounds %d (states %zu)\n",
                c.n1, c.n2, c.rho1, c.rho2, ex.sl1, ex.sl2, ex.abandon1, ex.abandon2, ex.ewt1, ex.ewt2,
                ex.lent_busy, q1.sl, q2.sl, q1.abandon, q2.abandon, q1.ewt_mean, q2.ewt_mean,
                m.q[0].lent_in, m.rounds, ex.states);
    // The negative test: the non-conserving formula must FAIL the gate.
    const CouplingResult nc = coupled_n(c, false);
    ASSERT_TRUE(nc.ok);
    const std::string nbad = gate("q1", ex.sl1, nc.q[0].m.sl, ex.abandon1, nc.q[0].m.abandon, ex.ewt1, nc.q[0].m.ewt_mean) +
                             gate("q2", ex.sl2, nc.q[1].m.sl, ex.abandon2, nc.q[1].m.abandon, ex.ewt2, nc.q[1].m.ewt_mean);
    EXPECT_NE(nbad, "") << "the non-conserving formula passed the gate";
    EXPECT_FALSE(nc.conserved);
    std::printf("   non-conserving fails:%s\n", nbad.c_str());
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (argc > 1) g_golden = argv[1];
  return RUN_ALL_TESTS();
}
