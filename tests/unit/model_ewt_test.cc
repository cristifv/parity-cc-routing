// Phase 2 (PLAN §3.4, §5.1): the model's EWT reaches QueueEntry.expected_wait_s and PositionUpdate
// behind --model-ewt (off by default: phase 1's estimate, unchanged); plans come from the
// optimization threads and never cross tenants; a NaN never reaches the boundary.
#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <future>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <thread>

#include "model/coupling.h"
#include "model/ctmc.h"
#include "src/core/engine.h"
#include "src/core/tenant.h"
#include "src/opt/optimizer.h"

namespace pcr {
namespace {

constexpr int64_t T0 = 1'792'800'000'000;

struct Heap final : TimerSink {
  std::vector<std::pair<int64_t, TimerRef>> items;
  void at(int64_t due, TimerRef t) override { items.emplace_back(due, std::move(t)); }
};

struct Rec final : Sink {
  std::vector<rv1::WatchResponse> ie;
  void to_ie(const std::string&, uint64_t, const rv1::WatchResponse& m) override { ie.push_back(m); }
  void to_gw(const std::string&, uint64_t, const av1::WatchResponse&) override {}
  bool ie_live(const std::string&) const override { return true; }
};

TenantConfig config(const std::string& tenant) {
  TenantConfig c;
  c.id = tenant;
  QueueConfig q;
  q.id = "sales";
  q.max_wait_ms = 600'000;
  q.aht_ms = 180'000;
  c.queues = {q};
  for (int i = 1; i <= 4; ++i) {
    AgentConfig a;
    a.id = "agent-a" + std::to_string(i);
    a.queues = {"sales"};
    a.skills = {{"de", 1}};
    c.agents.push_back(a);
  }
  return c;
}

rv1::EnqueueRequest enq(const std::string& tenant, const std::string& ix, int64_t now, bool german = false) {
  rv1::EnqueueRequest r;
  auto* o = r.mutable_owner();
  o->set_tenant_id(tenant);
  o->set_interaction_id(ix);
  o->set_owner_epoch(100);
  o->set_replica_id("ie-1");
  r.set_enqueue_seq(1);
  r.mutable_target()->set_queue_id("sales");
  r.set_first_queued_at_ms(now);
  r.set_ordering_at_ms(now);
  if (german) {
    auto* q = r.add_requirements();
    q->set_skill_id("de");
    q->set_min_level(1);
  }
  return r;
}

std::string ix(int n) {
  char b[40];
  std::snprintf(b, sizeof b, "0192f6a1-7c3e-7b21-9a4d-%012d", n);
  return b;
}

struct World {
  Heap heap;
  Rec rec;
  Counters counters;
  Epochs epochs{1, 0};
  Tenant t;
  int64_t now = T0;
  explicit World(bool model) : t(config("acme"), &epochs, &rec, &heap, &counters) {
    t.set_model_ewt(model);
    // Two agents logged in but not ready: callers wait, positions grow.
    t.seed_attached("agent-a1", "gw-1", "", std::nullopt, now);
    t.seed_attached("agent-a2", "gw-1", "", std::nullopt, now);
  }
  std::shared_ptr<EwtPlan> plan(std::vector<double> v, const std::string& tenant = "acme", bool lng = false) {
    auto p = std::make_shared<EwtPlan>();
    p->tenant = tenant;
    p->version = ++version;
    p->computed_at_ms = now;
    p->valid_until_ms = now + 30'000;
    EwtTable tb;
    tb.by_position = std::move(v);
    tb.step_s = 50;
    tb.long_wait = lng;
    p->tables[{"sales", ""}] = tb;
    return p;
  }
  uint64_t version = 0;
};

TEST(ModelEwt, OffByDefaultKeepsPhaseOneEstimate) {
  World w(/*model=*/false);
  ASSERT_TRUE(w.t.install_ewt(w.plan({7, 14, 21}), w.now));  // ignored while the flag is off
  auto a = w.t.enqueue(enq("acme", ix(1), w.now), w.now);
  auto b = w.t.enqueue(enq("acme", ix(2), w.now), w.now);
  ASSERT_TRUE(a && b);
  // Phase 1: position x AHT / eligible agents logged in = 1 x 180 / 2, 2 x 180 / 2.
  EXPECT_EQ(a->entry().expected_wait_s(), 90u);
  EXPECT_EQ(b->entry().expected_wait_s(), 180u);
  EXPECT_EQ(w.counters.ewt_model.load(), 0u);
}

TEST(ModelEwt, OnUsesThePlanInEnqueueAndPositionUpdates) {
  World w(/*model=*/true);
  // No plan yet: phase 1's estimate.
  auto a = w.t.enqueue(enq("acme", ix(1), w.now), w.now);
  ASSERT_TRUE(a);
  EXPECT_EQ(a->entry().expected_wait_s(), 90u);
  ASSERT_TRUE(w.t.install_ewt(w.plan({7.2, 14.5, 21.9}), w.now));
  auto b = w.t.enqueue(enq("acme", ix(2), w.now), w.now);
  auto c = w.t.enqueue(enq("acme", ix(3), w.now), w.now);
  auto d = w.t.enqueue(enq("acme", ix(4), w.now), w.now);
  ASSERT_TRUE(b && c && d);
  EXPECT_EQ(b->entry().expected_wait_s(), 15u);   // position 2: ceil(14.5)
  EXPECT_EQ(c->entry().expected_wait_s(), 22u);   // position 3
  EXPECT_EQ(d->entry().expected_wait_s(), 72u);   // position 4: beyond the table, 21.9 + 50
  // The caller ahead hangs up: everyone behind moves up and hears the model's number.
  w.rec.ie.clear();
  rv1::DequeueRequest dq;
  *dq.mutable_owner() = enq("acme", ix(1), w.now).owner();
  dq.set_entry_id(a->entry().entry_id());
  dq.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
  ASSERT_TRUE(w.t.dequeue(dq, w.now));
  std::map<uint32_t, uint32_t> got;
  for (const auto& m : w.rec.ie) {
    if (m.has_position()) got[m.position().position()] = m.position().expected_wait_s();
  }
  EXPECT_EQ(got[1], 8u);
  EXPECT_EQ(got[2], 15u);
  EXPECT_EQ(got[3], 22u);
  EXPECT_GT(w.counters.ewt_model.load(), 0u);
  // A stale plan (past valid_until_ms): phase 1 again.
  w.now += 31'000;
  auto e = w.t.enqueue(enq("acme", ix(5), w.now), w.now);
  ASSERT_TRUE(e);
  EXPECT_EQ(e->entry().expected_wait_s(), 360u);  // 4 x 180 / 2
}

// CR-056: a head removal no longer sends every entry behind it a PositionUpdate at once: positions
// 1..10 hear it now, the rest at most once per 5 s (the 1 s sweep flushes them).
TEST(Positions, ThrottledBeyondTheHead) {
  World w(/*model=*/false);
  std::vector<std::string> entries;
  for (int i = 1; i <= 15; ++i) {
    auto r = w.t.enqueue(enq("acme", ix(i), w.now), w.now);
    ASSERT_TRUE(r);
    entries.push_back(r->entry().entry_id());
  }
  w.rec.ie.clear();
  rv1::DequeueRequest dq;
  *dq.mutable_owner() = enq("acme", ix(1), w.now).owner();
  dq.set_entry_id(entries[0]);
  dq.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
  ASSERT_TRUE(w.t.dequeue(dq, w.now));
  auto count = [&] {
    std::set<uint32_t> p;
    for (const auto& m : w.rec.ie) {
      if (m.has_position()) p.insert(m.position().position());
    }
    return p;
  };
  EXPECT_EQ(count(), (std::set<uint32_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
  w.t.sweep(w.now + 1000);  // inside the 5 s: still held
  EXPECT_EQ(count().size(), 10u);
  w.t.sweep(w.now + 5000);
  EXPECT_EQ(count(), (std::set<uint32_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}));
}

// CR-104: an entry that lands ahead of others (here a higher priority) moves each of them back one
// place; the 1 s sweep tells them (before the fix nobody heard until the next head removal).
TEST(Positions, InsertAheadTellsTheEntriesBehind) {
  World w(/*model=*/false);
  std::vector<std::string> behind;
  for (int i = 1; i <= 3; ++i) {
    auto r = w.t.enqueue(enq("acme", ix(i), w.now), w.now);
    ASSERT_TRUE(r);
    behind.push_back(r->entry().entry_id());
  }
  w.rec.ie.clear();
  rv1::EnqueueRequest hp = enq("acme", ix(4), w.now);
  hp.set_priority(90);
  auto r = w.t.enqueue(hp, w.now);
  ASSERT_TRUE(r);
  EXPECT_EQ(r->entry().position(), 1u);  // ahead of the three
  w.t.sweep(w.now + 1000);
  std::map<std::string, uint32_t> heard;
  for (const auto& m : w.rec.ie) {
    if (m.has_position()) heard[m.position().entry_id()] = m.position().position();
  }
  ASSERT_EQ(heard.size(), 3u);
  EXPECT_EQ(heard[behind[0]], 2u);
  EXPECT_EQ(heard[behind[1]], 3u);
  EXPECT_EQ(heard[behind[2]], 4u);
}

TEST(ModelEwt, LongCarriesTheLowerBoundNeverZero) {
  World w(/*model=*/true);
  ASSERT_TRUE(w.t.install_ewt(w.plan({1, 2}, "acme", /*lng=*/true), w.now));
  auto a = w.t.enqueue(enq("acme", ix(1), w.now), w.now);
  ASSERT_TRUE(a);
  EXPECT_EQ(a->entry().expected_wait_s(), 600u);  // the time left to max_wait_s (CR-013)
}

TEST(ModelEwt, AnotherTenantsPlanIsRefused) {
  World w(/*model=*/true);
  EXPECT_FALSE(w.t.install_ewt(w.plan({7, 14}, "globex"), w.now));
  EXPECT_EQ(w.counters.plans_refused.load(), 1u);
  auto a = w.t.enqueue(enq("acme", ix(1), w.now), w.now);
  EXPECT_EQ(a->entry().expected_wait_s(), 90u);  // still phase 1's
  // An older version than the one held is refused too.
  ASSERT_TRUE(w.t.install_ewt(w.plan({7, 14}), w.now));
  auto old = w.plan({1});
  old->version = 0;
  EXPECT_FALSE(w.t.install_ewt(old, w.now));
}

// Phase 5 redesign: n agents, each counted on its own, taking `queue` as PRIMARY at speed 1.
void add_agents(ModelSnapshot* s, int queue, int n) {
  for (int i = 0; i < n; ++i) {
    ModelSnapshot::Agent a;
    a.id = "a" + std::to_string(queue) + "-" + std::to_string(i);
    a.counts = true;
    a.set = {{queue, 1}};
    a.speed = {1.0};
    a.eligible = {queue};
    s->agents.push_back(std::move(a));
  }
}

TEST(ModelEwt, SnapshotCountsAndRequirementShares) {
  World w(/*model=*/true);
  auto a = w.t.enqueue(enq("acme", ix(1), w.now, /*german=*/true), w.now);
  ASSERT_TRUE(a);
  auto s = w.t.model_snapshot(w.now);
  ASSERT_EQ(s->queues.size(), 1u);
  EXPECT_EQ(s->tenant, "acme");
  EXPECT_EQ(s->queues[0].arrivals, 1u);
  EXPECT_EQ(s->queues[0].logged_in, 2);
  EXPECT_EQ(s->queues[0].waiting, 1u);
  // NOT_READY agents are not in N (CR-036): listed (logged in, attached) but not counted.
  for (const auto& a : s->agents) EXPECT_FALSE(a.counts) << a.id;
  ASSERT_EQ(s->sigs.size(), 1u);
  EXPECT_DOUBLE_EQ(s->sigs[0].share, 1.0);  // both logged-in agents speak German
  auto s2 = w.t.model_snapshot(w.now + 10'000);
  EXPECT_EQ(s2->queues[0].arrivals, 0u);  // counts reset per window
  EXPECT_EQ(s2->window_ms, 10'000);
}

TEST(Optimizer, PlanMatchesTheModelAndRefusesNaN) {
  ModelSnapshot s;
  s.tenant = "acme";
  s.at_ms = T0;
  s.window_ms = 10'000;
  ModelSnapshot::Queue q;
  q.id = "sales";
  q.arrivals = 4;      // 0.4 / s
  q.abandons = 1;
  q.waited_s = 120;
  q.aht_s = 180;
  q.max_wait_ms = 600'000;
  q.waiting = 3;
  q.logged_in = 10;
  s.queues = {q};
  add_agents(&s, 0, 10);
  Optimizer::TenantState st;
  model::Stats ms;
  auto p = Optimizer::plan_for(s, &st, Optimizer::Options{}, &ms);
  ASSERT_TRUE(p);
  EXPECT_EQ(p->tenant, "acme");
  EXPECT_EQ(p->valid_until_ms, T0 + 30'000);
  const EwtTable& t = p->tables.at({"sales", ""});
  ASSERT_EQ(t.by_position.size(), 53u);
  // One queue, ten agents each counted: s_tag = 10 servers, theta from the MLE with its prior.
  const double theta = (0.1 * 900 + 1) / (12.0 * 900 + 120);
  model::Quality qual;
  const auto v = model::ewt_by_position(10, 1 / 180.0, theta, 0, 53, 600, &qual);
  for (size_t k = 0; k < v.size(); ++k) EXPECT_NEAR(t.by_position[k], v[k], 1e-9) << k;
  EXPECT_NEAR(t.by_position[0], 18.0, 1e-9);  // position 1: one of 10 servers frees: 180 / 10
  // NaN at the boundary: refused, no plan.
  s.queues[0].aht_s = std::nan("");
  EXPECT_EQ(Optimizer::plan_for(s, &st, Optimizer::Options{}, &ms), nullptr);
}

namespace {

ModelSnapshot quiet_sales(int64_t at_ms) {
  ModelSnapshot s;
  s.tenant = "acme";
  s.at_ms = at_ms;
  s.window_ms = 900'000;  // 15 minutes: 27 calls = 0.03 / s, 5.4 busy agents of 10 (rho 0.54)
  ModelSnapshot::Queue q;
  q.id = "sales";
  q.arrivals = 27;
  q.abandons = 1;
  q.waited_s = 300;
  q.aht_s = 180;
  q.max_wait_ms = 600'000;
  q.waiting = 1;
  q.logged_in = 10;
  s.queues = {q};
  add_agents(&s, 0, 10);
  return s;
}

}  // namespace

// CR-098: what the model can't see makes a table approx: a priority mix (higher-priority arrivals
// aren't measured, PLAN §3.4) for 15 minutes after it was last seen, and every table for entries with
// extra requirements (servers scaled by a share, CR-033).
TEST(Optimizer, PriorityMixAndRequirementTablesAreApprox) {
  const auto exact = static_cast<uint8_t>(model::Quality::kExact);
  const auto approx = static_cast<uint8_t>(model::Quality::kApprox);
  ModelSnapshot s = quiet_sales(T0);
  s.sigs = {ModelSnapshot::Sig{0, "de>=2", 0.5}};
  Optimizer::TenantState st;
  model::Stats ms;
  auto p = Optimizer::plan_for(s, &st, Optimizer::Options{}, &ms);
  ASSERT_TRUE(p);
  EXPECT_EQ(p->tables.at({"sales", ""}).quality, exact);
  EXPECT_EQ(p->tables.at({"sales", "de>=2"}).quality, approx);
  s.sigs.clear();
  s.at_ms += 10'000;
  s.window_ms = 10'000;
  s.queues[0].arrivals = 0;
  s.queues[0].priority_mix = true;
  p = Optimizer::plan_for(s, &st, Optimizer::Options{}, &ms);
  ASSERT_TRUE(p);
  EXPECT_EQ(p->tables.at({"sales", ""}).quality, approx);
  s.queues[0].priority_mix = false;
  s.at_ms += 14 * 60'000;  // still inside the 15 minutes
  p = Optimizer::plan_for(s, &st, Optimizer::Options{}, &ms);
  EXPECT_EQ(p->tables.at({"sales", ""}).quality, approx);
  s.at_ms += 2 * 60'000;  // past them
  p = Optimizer::plan_for(s, &st, Optimizer::Options{}, &ms);
  EXPECT_EQ(p->tables.at({"sales", ""}).quality, exact);
}

// PLAN §3.7: a solve that fails (here a queue whose tail can't be truncated within the model's cap)
// publishes no plan at all, so the tenant keeps its last good one; the warm start isn't touched.
TEST(Optimizer, AFailedSolvePublishesNoPlan) {
  ModelSnapshot s = quiet_sales(T0);
  ModelSnapshot::Queue flood = s.queues[0];
  flood.id = "support";
  flood.arrivals = 900'000'000;  // 1e6 calls / s ...
  flood.abandons = 0;
  flood.waited_s = 1e15;         // ... and almost no abandoning: no stationary tail within 20,000
  s.queues.push_back(flood);
  add_agents(&s, 1, 10);
  Optimizer::TenantState st;
  model::Stats ms;
  const uint64_t refused = ms.refused;
  EXPECT_EQ(Optimizer::plan_for(s, &st, Optimizer::Options{}, &ms), nullptr);
  EXPECT_EQ(st.version, 0u);     // no plan version spent
  (void)refused;
}

// The whole path: the writer's snapshot -> an optimization thread -> the plan back through the
// inbox; the writer never waits for it (it keeps answering with phase 1's estimate meanwhile).
TEST(Optimizer, PlansReachTheEngineThroughTheInbox) {
  Rec rec;
  ManualClock clock(T0);
  auto opt_owner = std::make_unique<Optimizer>(Optimizer::Options{});
  Optimizer& opt = *opt_owner;
  Engine::Limits lim;
  lim.model_ewt = true;
  Engine engine(&clock, &rec, 2, lim, [&opt](std::shared_ptr<const ModelSnapshot> s) { opt.submit(std::move(s)); });
  opt.attach(&engine);
  TenantConfig acme = config("acme"), globex = config("globex");
  engine.load({acme, globex}, Epochs{1, 0});
  for (const char* t : {"acme", "globex"}) {
    std::promise<void> done;
    engine.post(t, [&](Tenant* tn, int64_t now) {
      tn->seed_attached("agent-a1", "gw-1", "", now, now);
      done.set_value();
    }, false);
    done.get_future().wait();
  }
  engine.advance_to(&clock, T0 + 10'000);  // the first model tick
  const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (engine.totals().plans_installed < 2 && std::chrono::steady_clock::now() < until) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    engine.barrier();
  }
  EXPECT_EQ(engine.totals().plans_installed, 2u);  // one per tenant, each its own
  EXPECT_EQ(engine.totals().plans_refused, 0u);
  EXPECT_GE(opt.totals().plans, 2u);
  EXPECT_EQ(opt.totals().refused, 0u);
  // CR-099: the optimizer goes first (main.cc's order): no feed into it, no plan into a freed engine.
  engine.stop_model_feed();
  opt.attach(nullptr);
  opt_owner.reset();
}

}  // namespace
}  // namespace pcr
