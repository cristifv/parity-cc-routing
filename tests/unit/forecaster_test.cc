// Phases 5b+5c in the real-time path (PLAN §5.9): the writer's arrival guard, the early snapshot of
// the shard's 1 s periodic, and the learned forecast in use changing within 1 s of virtual time after
// a spike's onset; the pooled-AHT rule for a learned source that owns its handle time (CR-180); the
// 10 s counts of routing_bucket_actuals (db/V3).
#include <gtest/gtest.h>

#include <cmath>
#include <future>
#include <mutex>

#include "src/core/engine.h"
#include "src/core/tenant.h"
#include "src/forecast/source.h"
#include "src/opt/optimizer.h"
#include "tests/unit/world.h"

namespace pcr {
namespace {

using namespace testing_world;

// A scripted net: H1/H2 p50 = the mean normalized level of the window's last 1-min token.
struct EchoNet final : fc::Net {
  fc::NetConfig c;
  const fc::NetConfig& config() const override { return c; }
  size_t parameters() const override { return 1; }
  bool run(const std::vector<fc::NetInput>& in, std::vector<fc::NetOutput>* out) const override {
    out->clear();
    for (const auto& x : in) {
      double z = 0;
      for (int k = fc::kH1Inputs - fc::kH1Patch; k < fc::kH1Inputs; ++k) z += x.h1[static_cast<size_t>(k)];
      z /= fc::kH1Patch;
      fc::NetOutput o;
      for (int k = 0; k < fc::kH1Steps; ++k) {
        for (double q : {z - 0.05, z, z + 0.05}) o.h1.push_back(static_cast<float>(q));
      }
      for (int b = 0; b < fc::kH2Buckets; ++b) {
        for (double q : {z - 0.05, z, z + 0.05}) o.h2a.push_back(static_cast<float>(q));
        for (int q = 0; q < 3; ++q) o.h2h.push_back(0.5f);
      }
      out->push_back(std::move(o));
    }
    return true;
  }
};

rv1::EnqueueRequest enqueue_req(int n, int64_t now) {
  char ix[40];
  std::snprintf(ix, sizeof ix, "0192f6a1-7c3e-7b21-9a4d-%012d", n);
  rv1::EnqueueRequest r;
  r.mutable_owner()->set_tenant_id("acme");
  r.mutable_owner()->set_interaction_id(ix);
  r.mutable_owner()->set_owner_epoch(100);
  r.mutable_owner()->set_replica_id("ie-1");
  r.set_enqueue_seq(1);
  r.mutable_target()->set_queue_id("sales");
  r.set_first_queued_at_ms(now);
  r.set_ordering_at_ms(now);
  return r;
}

TEST(Forecaster, ASpikeOnsetChangesTheForecastInUseWithinOneSecond) {
  Rec rec;
  ManualClock clock(T0);
  std::mutex mu;
  std::vector<std::shared_ptr<const ModelSnapshot>> snaps;
  Engine::Limits lim;
  lim.optimizer = true;
  Engine engine(&clock, &rec, 1, lim, [&](std::shared_ptr<const ModelSnapshot> s) {
    std::lock_guard lk(mu);
    snaps.push_back(std::move(s));
  });
  TenantConfig c;
  c.id = "acme";
  c.queues = {queue("sales")};
  c.agents = {agent("a1", {"sales"})};
  engine.load({c}, Epochs{1, 0});

  // The tenant's model: a range for 60 calls a minute, a wide envelope (this test is the wiring).
  auto active = std::make_shared<fc::ModelSpec>();
  active->version = 1;
  active->created_at_ms = T0;
  active->net = std::make_shared<EchoNet>();
  fc::QueueNorm n;
  n.r = fc::Range::from_expected(3600);
  n.envelope = 10;
  active->norm["sales"] = n;
  active->envelope = 10;
  fc::LearnedSource src(fc::SourceOptions{}, nullptr);
  src.set_models("acme", active, nullptr);
  Optimizer::Options o;
  o.levers = true;
  o.learned = &src;
  Optimizer::TenantState st;
  model::Stats ms;
  size_t seen = 0;
  double lambda = -1;
  int64_t lambda_at = 0;
  std::string source;
  // The optimizer's side, synchronously: each new snapshot -> the lever tick -> the guard back.
  auto optimize = [&] {
    std::vector<std::shared_ptr<const ModelSnapshot>> todo;
    {
      std::lock_guard lk(mu);
      todo.assign(snaps.begin() + static_cast<long>(seen), snaps.end());
      seen = snaps.size();
    }
    for (const auto& s : todo) {
      Optimizer::LeverOut out = Optimizer::levers_for(*s, &st, o, &ms);
      ASSERT_FALSE(out.queues.empty());
      lambda = out.queues[0].lambda;
      lambda_at = s->at_ms;
      ASSERT_TRUE(out.guard);
      std::shared_ptr<const ArrivalGuard> g = std::move(out.guard);
      engine.post("acme", [g](Tenant* t, int64_t now) { t->install_guard(g, now); }, false);
      engine.barrier();
    }
  };
  int calls = 0;
  auto arrive = [&](int k) {
    engine.post("acme", [&, k](Tenant* t, int64_t now) {
      for (int i = 0; i < k; ++i) ASSERT_TRUE(t->enqueue(enqueue_req(++calls, now), now));
    }, false);
    engine.barrier();
  };
  // 13 minutes of one call a second: the model drives.
  for (int sec = 1; sec <= 780; ++sec) {
    engine.advance_to(&clock, T0 + sec * 1000);
    arrive(1);
    optimize();
  }
  ASSERT_TRUE(src.health("acme", "sales").driving);
  const double before = lambda;
  EXPECT_NEAR(before, 1.0, 0.15);
  EXPECT_EQ(engine.totals().early_snapshots, 0u);
  // The onset: 12 calls in 200 ms. The next 1 s periodic hands the snapshot over at once; the
  // re-forecast puts a higher rate in use, no 10 s model tick needed.
  const int64_t onset = T0 + 780'000 + 200;
  engine.advance_to(&clock, onset);
  arrive(12);
  engine.advance_to(&clock, T0 + 781'000);
  optimize();
  EXPECT_EQ(engine.totals().early_snapshots, 1u);
  EXPECT_LE(lambda_at - onset, 1000);
  EXPECT_GT(lambda, 1.3 * before);
  EXPECT_TRUE(src.health("acme", "sales").driving);
  // At most one early snapshot per 2 s.
  engine.advance_to(&clock, T0 + 781'500);
  arrive(20);  // the guard now expects ~1.5 a second: 20 in 2 s is still a breach
  engine.advance_to(&clock, T0 + 782'000);
  EXPECT_EQ(engine.totals().early_snapshots, 1u);
  engine.advance_to(&clock, T0 + 783'000);
  EXPECT_EQ(engine.totals().early_snapshots, 2u);
  engine.stop_model_feed();
}

TEST(Forecaster, AGuardOfAnotherTenantIsRefusedAndAQuietTenantNeverBreaches) {
  TenantConfig c;
  c.id = "acme";
  c.queues = {queue("sales")};
  World w(c);
  w.t->model_snapshot(w.now);
  auto g = std::make_shared<ArrivalGuard>();
  g->tenant = "globex";
  g->valid_until_ms = w.now + 30'000;
  g->per_s = {{"sales", 1.0}};
  EXPECT_FALSE(w.t->install_guard(g, w.now));
  g->tenant = "acme";
  EXPECT_TRUE(w.t->install_guard(g, w.now));
  w.advance(1000);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-000000000001", "sales"));
  EXPECT_FALSE(w.t->guard_breached(w.now));  // 1 call where 1 was expected
  // A collapse: nothing for 30 s where 30 were expected.
  w.t->model_snapshot(w.now);
  w.advance(29'000);
  EXPECT_TRUE(w.t->guard_breached(w.now));
}

TEST(Forecaster, BucketRowsCarryTheTenSecondCounts) {
  TenantConfig c;
  c.id = "acme";
  c.queues = {queue("sales")};
  World w(c);
  w.t->take_buckets(w.now);  // the bucket starts at T0
  w.advance(25'000);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-000000000001", "sales"));
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-000000000002", "sales"));
  w.advance(900'000);
  auto rows = w.t->take_buckets(w.now);
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].offered_10s.size(), 90u);
  EXPECT_EQ(rows[0].offered_10s[2], 2);  // 20-30 s
  EXPECT_EQ(rows[0].offered, 2u);
}

// CR-180: a learned source's handle time is used only when it owns it AND it differs from the
// tenant's pooled handle time by more than two standard errors.
TEST(Forecaster, ALearnedAhtPassesThePooledCheckOrThePooledValueStands) {
  struct L final : opt::ForecastSource {
    double aht = 0;
    bool owns = true;
    std::optional<opt::QueueForecast> forecast(const std::string&, const std::string&, int64_t,
                                               const opt::QueueForecast& b) override {
      opt::QueueForecast f = b;
      f.aht_s = aht;
      f.owns_aht = owns;
      return f;
    }
  } l;
  ModelSnapshot s;
  s.tenant = "acme";
  s.at_ms = T0;
  s.window_ms = 900'000;
  s.mode = 2;
  for (const char* id : {"sales", "support"}) {
    ModelSnapshot::Queue q;
    q.id = id;
    q.arrivals = 90;
    q.aht_s = 180;
    q.handled = 100;
    q.handle_s = 100 * 180.0;
    q.sla_ms = 20'000;
    q.max_wait_ms = 600'000;
    s.queues.push_back(q);
  }
  for (int i = 0; i < 6; ++i) {
    ModelSnapshot::Agent a;
    a.id = "a" + std::to_string(i);
    a.counts = true;
    a.set = {{0, 1}, {1, 1}};
    a.speed = {1.0, 1.0};
    a.eligible = {0, 1};
    s.agents.push_back(a);
  }
  Optimizer::Options o;
  o.levers = true;
  o.learned = &l;
  model::Stats ms;
  auto mu_of = [&](double aht, bool owns) {
    Optimizer::TenantState st;
    l.aht = aht;
    l.owns = owns;
    return Optimizer::levers_for(s, &st, o, &ms).queues.at(0).mu;
  };
  EXPECT_NEAR(1 / mu_of(185, true), 180, 1e-6);  // within 2 standard errors of the pooled 180 s
  EXPECT_NEAR(1 / mu_of(300, true), 300, 1e-6);  // clearly different: the model's
  EXPECT_NEAR(1 / mu_of(300, false), 180, 1e-6);  // not owned: never
}

}  // namespace
}  // namespace pcr
