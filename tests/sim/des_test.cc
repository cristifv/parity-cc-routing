// The DES (PLAN §3.6) drives the real dispatch (pcr::Tenant), keeps the engine's invariants, and a
// small exponential single queue agrees with the CTMC (the full matrix runs optimized, as evidence).
#include <gtest/gtest.h>

#include <cmath>

#include "model/coupling.h"
#include "model/ctmc.h"
#include "sim/des.h"

using namespace pcr::sim;
namespace m = pcr::model;

namespace {

Scenario single(int n, double rho, double measure_s) {
  Scenario sc;
  sc.name = "single";
  QueueSpec q;
  q.id = "sales";
  q.per_s = {rho * n / 180.0};
  q.talk = Dist::exp(180);
  q.patience = Dist::exp(120);
  sc.queues = {q};
  sc.groups = {GroupSpec{n, {"sales"}}};
  sc.warmup_s = 1800;
  sc.measure_s = measure_s;
  sc.drain_s = 1800;
  return sc;
}

}  // namespace

TEST(Des, SingleQueueAgreesWithTheModel) {
  const Scenario sc = single(10, 0.9, 6 * 3600);
  std::vector<double> sl, ab, ewt;
  uint64_t calls = 0;
  for (uint64_t seed = 1; seed <= 4; ++seed) {
    const RepResult r = run_replication(sc, seed);
    EXPECT_EQ(r.engine_errors, 0u) << (r.violations.empty() ? "" : r.violations[0]);
    EXPECT_EQ(r.invariant_violations, 0u) << (r.violations.empty() ? "" : r.violations[0]);
    EXPECT_GT(r.invariant_checks, 0u);
    EXPECT_EQ(r.sweep_matches, 0u);  // the safety net never had to dispatch
    EXPECT_GT(r.offers, 0u);         // decisions came from the real dispatch
    sl.push_back(r.q[0].sl());
    ab.push_back(r.q[0].abandon());
    ewt.push_back(r.q[0].ewt());
    calls += r.q[0].offered;
    EXPECT_LT(r.q[0].virtual_unresolved, r.q[0].abandoned / 50 + 2);
  }
  m::Options o;
  o.sla_s = 20;
  m::Stats st;
  const m::QueueMetrics mm = m::stationary_metrics(m::QueueParams{0.9 * 10 / 180.0, 1 / 180.0, 1 / 120.0, 10}, o, &st);
  ASSERT_TRUE(mm.ok);
  const Estimate esl = estimate(sl), eab = estimate(ab), eewt = estimate(ewt);
  std::printf("des smoke: %llu calls; SL model %.4f sim %.4f +- %.4f; abandon model %.4f sim %.4f +- %.4f; "
              "EWT model %.2f sim %.2f +- %.2f\n",
              static_cast<unsigned long long>(calls), mm.sl, esl.mean, esl.half, mm.abandon, eab.mean, eab.half,
              mm.ewt_mean, eewt.mean, eewt.half);
  // The §3.6 gate, with the smoke run's own sampling error allowed on top.
  EXPECT_LE(std::abs(mm.sl - esl.mean), 0.03 + esl.half);
  EXPECT_LE(std::abs(mm.abandon - eab.mean), std::max(0.01, 0.15 * mm.abandon) + eab.half);
  EXPECT_LE(std::abs(mm.ewt_mean - eewt.mean), std::max(5.0, 0.15 * mm.ewt_mean) + eewt.half);
}

TEST(Des, SharedGroupRunsTheRealDispatch) {
  Scenario sc;
  sc.name = "w";
  for (const char* id : {"sales", "support"}) {
    QueueSpec q;
    q.id = id;
    q.per_s = {0.9 * 6 / 180.0};
    q.talk = Dist::lognormal(180, 0.8);
    q.wrap = Dist::fixed(10);
    q.patience = Dist::exp(120);
    sc.queues.push_back(q);
  }
  sc.groups = {GroupSpec{2, {"sales"}}, GroupSpec{4, {"sales", "support"}}, GroupSpec{2, {"support"}},
               GroupSpec{3, {"sales"}, 600, 1200}};  // a shift-start ramp
  sc.warmup_s = 1200;
  sc.measure_s = 3600;
  sc.drain_s = 600;
  sc.check_every_s = 60;
  const RepResult r = run_replication(sc, 7);
  EXPECT_EQ(r.engine_errors, 0u) << (r.violations.empty() ? "" : r.violations[0]);
  EXPECT_EQ(r.invariant_violations, 0u) << (r.violations.empty() ? "" : r.violations[0]);
  EXPECT_EQ(r.sweep_matches, 0u);
  EXPECT_GT(r.q[0].answered, 0u);
  EXPECT_GT(r.q[1].answered, 0u);
  // Same seed, same run: the DES is deterministic.
  const RepResult again = run_replication(sc, 7);
  EXPECT_EQ(again.q[0].answered, r.q[0].answered);
  EXPECT_EQ(again.q[1].abandoned, r.q[1].abandoned);
}
