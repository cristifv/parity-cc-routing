// Phase 5 redesign (the user, 2026-11-01): VIP callers, lever (b) in the dispatch, lever plans in the
// writer (auto, exact only, bounds), the stall rule, re.state of the levers, the per-agent model and
// the optimizer's levers, the forecast interface, the events, and the stopped build's fixes (bounded
// gateway outbox, per-tenant quarantine). Every tenant here tracks its changes in paranoid mode: each
// check collects the records and fails on a change no record marked, then runs the writer's
// invariants (one live offer per agent and entry, no eligible READY agent idle past an entry's delay,
// every entry leaves by max wait, the kept busy counts, tenant isolation of offers).
#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
#include <map>
#include <future>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>

#include "model/agents.h"
#include "src/core/engine.h"
#include "src/core/gate.h"
#include "src/core/tenant.h"
#include "src/opt/events.h"
#include "src/opt/forecast.h"
#include "src/opt/levers.h"
#include "src/opt/optimizer.h"
#include "tests/unit/world.h"

namespace pcr {
namespace {

using namespace testing_world;
using json = nlohmann::json;

std::string ix(int n) {
  char b[40];
  std::snprintf(b, sizeof b, "0192f6a1-7c3e-7b21-9a4d-%012d", n);
  return b;
}

TenantConfig cfg(RoutingMode mode = RoutingMode::kAuto, const std::string& id = "acme") {
  TenantConfig c;
  c.id = id;
  c.queues = {queue("sales"), queue("support")};
  c.agents = {agent("a1", {"sales", "support"}), agent("a2", {"sales", "support"}), agent("a3", {"sales", "support"}),
              agent("v1", {"sales", "support"}, {{"vip", 1}})};
  c.routing.version = 1;
  c.routing.mode = mode;
  c.routing.vip_skill = "vip";
  QueuePolicy s, t;
  s.queue_id = "sales";
  t.queue_id = "support";
  c.routing.queues = {s, t};
  return c;
}

struct P5 : World {
  explicit P5(TenantConfig c) : World(std::move(c)) {
    t->set_tracking(true);
    t->set_paranoid(true);
  }
  std::vector<StateRec> recs;
  std::map<std::string, std::pair<std::string, std::string>> res_of;  // agent -> (reservation, interaction)
  // The writer's invariants plus change tracking: every change is in a record (paranoid).
  std::vector<std::string> bad() {
    t->collect(&recs, now);
    auto v = t->check_invariants(now);
    for (const auto& k : t->unmarked()) v.push_back("unmarked change: " + k);
    return v;
  }
  Result<rv1::EnqueueResponse> vip(const std::string& id, const std::string& q) {
    rv1::EnqueueRequest r;
    *r.mutable_owner() = owner(id);
    r.set_enqueue_seq(1);
    r.mutable_target()->set_queue_id(q);
    r.set_first_queued_at_ms(now);
    r.set_ordering_at_ms(now);
    r.set_caller_class(rv1::CALLER_CLASS_VIP);
    return t->enqueue(r, now);
  }
  std::string offered_ix(const std::string& agent) {
    for (auto it = rec.gw.rbegin(); it != rec.gw.rend(); ++it) {
      if (it->second.has_offer() && it->second.agent_id() == agent) return it->second.offer().interaction_id();
    }
    return {};
  }
  bool accept(const std::string& a) {
    if (!answer(a, true)) return false;
    for (auto it = rec.ie.rbegin(); it != rec.ie.rend(); ++it) {
      if (it->second.has_assigned() && it->second.assigned().agent_id() == a) {
        res_of[a] = {it->second.assigned().reservation_id(), it->second.assigned().target().interaction_id()};
        rv1::ReportOutcomeRequest r;
        *r.mutable_owner() = owner(res_of[a].second);
        r.set_reservation_id(res_of[a].first);
        r.set_outcome(rv1::OUTCOME_CONNECTED);
        return static_cast<bool>(t->report_outcome(r, now));
      }
    }
    return false;
  }
  bool hang_up(const std::string& a) {
    auto it = res_of.find(a);
    if (it == res_of.end()) return false;
    rv1::CallEndedRequest r;
    *r.mutable_owner() = owner(it->second.second);
    r.set_reservation_id(it->second.first);
    r.set_agent_id(a);
    return static_cast<bool>(t->call_ended(r, now));
  }
  bool wrap_done(const std::string& a) {
    auto it = res_of.find(a);
    if (it == res_of.end()) return false;
    av1::EndWrapUpRequest r;
    *r.mutable_session() = ref(a);
    r.set_command_id(next_cmd());
    r.set_reservation_id(it->second.first);
    r.set_disposition_code("done");
    const bool ok = static_cast<bool>(t->end_wrap_up(r, now));
    res_of.erase(it);
    return ok;
  }
  std::shared_ptr<LeverPlan> plan(uint64_t v, std::vector<LeverPlan::Queue> q, const std::string& tenant = "acme") {
    auto p = std::make_shared<LeverPlan>();
    p->tenant = tenant;
    p->version = v;
    p->computed_at_ms = now;
    p->valid_until_ms = now + 30'000;
    p->queues = std::move(q);
    return p;
  }
};

// ---------------------------------------------------------------- VIP and escalation (dispatch)

TEST(Vip, VipGoesBeforeAnOlderNormalCaller) {
  P5 w(cfg(RoutingMode::kOff));
  w.attach_not_ready("a1");
  ASSERT_TRUE(w.enqueue(ix(1), "sales"));
  w.advance(5'000);
  ASSERT_TRUE(w.vip(ix(2), "sales"));
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_READY));
  EXPECT_EQ(w.offered_ix("a1"), ix(2));
  EXPECT_EQ(w.counters.vip_enqueued.load(), 1u);
  EXPECT_TRUE(w.bad().empty());
}

// Escalation sits above the class: an entry past escalate_after goes first, so a stream of VIP
// callers never starves a normal one, and every entry leaves by its max wait.
TEST(Vip, AVipStreamNeverStarvesANormalCallerPastEscalation) {
  P5 w(cfg(RoutingMode::kOff));
  w.ready("a1", T0 - 1'000);
  ASSERT_TRUE(w.vip(ix(100), "sales"));
  ASSERT_TRUE(w.accept("a1"));
  w.advance(1'000);
  ASSERT_TRUE(w.enqueue(ix(1), "sales"));  // the normal caller
  int n = 101;
  for (int s = 0; s < 4; ++s) {
    w.advance(30'000);
    ASSERT_TRUE(w.vip(ix(n++), "sales"));
    ASSERT_TRUE(w.bad().empty());
  }
  // 121 s: a1 frees. Waiting: the normal caller (120 s, escalated) and VIP callers of 91, 61, 31, 1 s.
  ASSERT_TRUE(w.hang_up("a1"));
  ASSERT_TRUE(w.wrap_done("a1"));
  EXPECT_EQ(w.offered_ix("a1"), ix(1));
  auto v = w.bad();
  EXPECT_TRUE(v.empty()) << (v.empty() ? "" : v[0]);
  // And nobody waits past max wait: with no agent left, every entry overflows by 600 s.
  ASSERT_TRUE(w.accept("a1"));
  w.advance(700'000);
  v = w.bad();
  EXPECT_TRUE(v.empty()) << (v.empty() ? "" : v[0]);
  EXPECT_GE(w.counters.overflows.load(), 4u);
}

// The VIP reserve keeps a VIP-eligible agent from a normal call until the queue's delay, then it
// takes it (bounded wait: no READY agent idles past an entry's delay); a VIP call reaches it at once.
TEST(Vip, TheReserveHoldsAVipAgentOnlyUntilTheDelay) {
  TenantConfig c = cfg(RoutingMode::kOff);
  c.routing.queues[0].vip_reserved_min = 1;
  P5 w(c);
  w.ready("v1", T0 - 1'000);
  ASSERT_TRUE(w.enqueue(ix(1), "sales"));
  EXPECT_TRUE(w.t->live_offer_of("v1").empty());
  EXPECT_TRUE(w.bad().empty());
  w.advance(10'500);  // sales' secondary delay (10 s) passed
  EXPECT_EQ(w.offered_ix("v1"), ix(1));
  EXPECT_TRUE(w.bad().empty());
  ASSERT_TRUE(w.accept("v1"));
  ASSERT_TRUE(w.hang_up("v1"));
  ASSERT_TRUE(w.wrap_done("v1"));
  ASSERT_TRUE(w.vip(ix(2), "sales"));
  EXPECT_EQ(w.offered_ix("v1"), ix(2));
  EXPECT_TRUE(w.bad().empty());
}

// ---------------------------------------------------------------- lever (b): min/max in the dispatch

TEST(LeverB, BelowMinGoesFirstAndAtMaxOnlyWhenNothingElse) {
  TenantConfig c = cfg(RoutingMode::kAuto);
  c.routing.queues[1].min_agents = 1;  // support
  c.routing.queues[0].max_agents = 1;  // sales
  P5 w(c);
  w.attach_not_ready("a1");
  ASSERT_TRUE(w.enqueue(ix(1), "sales"));   // older
  w.advance(1'000);
  ASSERT_TRUE(w.enqueue(ix(2), "support"));  // newer, but support has no agent busy on it (min 1)
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_READY));
  EXPECT_EQ(w.offered_ix("a1"), ix(2));
  EXPECT_EQ(w.t->busy_on("support"), 1);
  EXPECT_GE(w.counters.bounds_min_picks.load(), 1u);
  ASSERT_TRUE(w.accept("a1"));
  // sales: a2 takes the waiting sales call; with a2 busy on it sales is at its max.
  w.attach_not_ready("a2");
  ASSERT_TRUE(w.set("a2", av1::TARGET_STATE_READY));
  EXPECT_EQ(w.offered_ix("a2"), ix(1));
  ASSERT_TRUE(w.accept("a2"));
  EXPECT_EQ(w.t->busy_on("sales"), 1);
  w.advance(1'000);
  ASSERT_TRUE(w.enqueue(ix(3), "sales"));   // older
  w.advance(1'000);
  ASSERT_TRUE(w.enqueue(ix(4), "support"));  // newer
  w.attach_not_ready("a3");
  ASSERT_TRUE(w.set("a3", av1::TARGET_STATE_READY));
  EXPECT_EQ(w.offered_ix("a3"), ix(4));  // sales at its max: other work first
  ASSERT_TRUE(w.accept("a3"));
  ASSERT_TRUE(w.hang_up("a3"));
  ASSERT_TRUE(w.wrap_done("a3"));
  EXPECT_EQ(w.offered_ix("a3"), ix(3));  // nothing else: at max, still taken (never idle)
  auto v = w.bad();
  EXPECT_TRUE(v.empty()) << (v.empty() ? "" : v[0]);
}

TEST(LeverB, OffModeIgnoresTheBounds) {
  TenantConfig c = cfg(RoutingMode::kOff);
  c.routing.queues[1].min_agents = 1;
  P5 w(c);
  w.attach_not_ready("a1");
  ASSERT_TRUE(w.enqueue(ix(1), "sales"));
  w.advance(1'000);
  ASSERT_TRUE(w.enqueue(ix(2), "support"));
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_READY));
  EXPECT_EQ(w.offered_ix("a1"), ix(1));  // the older one (equal weights and thresholds)
  EXPECT_TRUE(w.bad().empty());
}

// ---------------------------------------------------------------- lever plans in the writer

TEST(Levers, AutoAppliesExactQueuesOnlyWithinBounds) {
  TenantConfig c = cfg(RoutingMode::kAuto);
  c.routing.queues[0].weight_min = 0.5;
  c.routing.queues[0].weight_max = 2;
  c.routing.queues[0].delay_max_ms = 30'000;
  P5 w(c);
  Tenant::LeverResult r;
  // sales exact (weight 3 -> clamped to 2; delay 5 s); support approx (never applied).
  ASSERT_TRUE(w.t->install_levers(w.plan(1, {{"sales", 3.0, 5'000, 0}, {"support", 2.0, 0, 1}}), w.now, &r));
  EXPECT_EQ(r.applied, 1);
  EXPECT_EQ(r.held, 1);
  EXPECT_EQ(r.clamped, 1);
  EXPECT_DOUBLE_EQ(w.t->queue_weight("sales"), 2.0);
  EXPECT_EQ(w.t->queue_delay("sales"), 5'000);
  EXPECT_DOUBLE_EQ(w.t->queue_weight("support"), 1.0);
  EXPECT_EQ(w.t->queue_delay("support"), 10'000);
  EXPECT_TRUE(w.bad().empty());
}

TEST(Levers, AdviseAndOffApplyNothing) {
  for (RoutingMode m : {RoutingMode::kOff, RoutingMode::kAdvise}) {
    P5 w(cfg(m));
    Tenant::LeverResult r;
    ASSERT_TRUE(w.t->install_levers(w.plan(1, {{"sales", 2.0, 5'000, 0}}), w.now, &r));
    EXPECT_EQ(r.why, "mode");
    EXPECT_DOUBLE_EQ(w.t->queue_weight("sales"), 1.0);
    EXPECT_EQ(w.t->queue_delay("sales"), 10'000);
    EXPECT_TRUE(w.bad().empty());
  }
}

TEST(Levers, AnotherTenantsOrAnOldOrForeignQueuePlanIsRefused) {
  P5 w(cfg());
  EXPECT_FALSE(w.t->install_levers(w.plan(1, {{"sales", 2.0, 5'000, 0}}, "globex"), w.now));
  EXPECT_FALSE(w.t->install_levers(w.plan(1, {{"billing", 2.0, 5'000, 0}}), w.now));  // not acme's queue
  ASSERT_TRUE(w.t->install_levers(w.plan(2, {{"sales", 2.0, 5'000, 0}}), w.now));
  EXPECT_FALSE(w.t->install_levers(w.plan(2, {{"sales", 0.5, 0, 0}}), w.now));  // not newer
  EXPECT_DOUBLE_EQ(w.t->queue_weight("sales"), 2.0);
  EXPECT_GE(w.counters.foreign_refused.load(), 2u);
  EXPECT_TRUE(w.bad().empty());
}

// PLAN §6 (redesign): a stale plan's levers go back to config's; nothing else changes; a fresh plan
// puts them back.
TEST(Levers, AStalePlanRevertsToConfigDefaultsAndAFreshOneReturns) {
  P5 w(cfg());
  w.t->set_optimizer(true);
  w.ready("a1", T0 - 1'000);
  const auto set_before = w.t->active_of("a1");
  ASSERT_TRUE(w.t->install_levers(w.plan(1, {{"sales", 2.0, 5'000, 0}}), w.now));
  w.advance(29'000);
  EXPECT_EQ(w.t->plan_stage(), 0);
  w.advance(3'000);
  EXPECT_EQ(w.t->plan_stage(), 1);
  EXPECT_DOUBLE_EQ(w.t->queue_weight("sales"), 1.0);
  EXPECT_EQ(w.t->queue_delay("sales"), 10'000);
  EXPECT_EQ(w.t->active_of("a1"), set_before);  // nothing narrows, nothing moves
  EXPECT_EQ(w.counters.stall_stale.load(), 1u);
  ASSERT_TRUE(w.t->install_levers(w.plan(2, {{"sales", 1.5, 0, 0}}), w.now));
  EXPECT_EQ(w.t->plan_stage(), 0);
  EXPECT_DOUBLE_EQ(w.t->queue_weight("sales"), 1.5);
  EXPECT_TRUE(w.bad().empty());
}

// The stopped build's fix (b): the plan, its validity and the levers in force are in re.state, so a
// takeover keeps them (no stall revert after the replay).
TEST(Levers, ARestoreKeepsThePlanItsValidityAndTheLevers) {
  P5 w(cfg());
  w.t->set_optimizer(true);
  ASSERT_TRUE(w.t->install_levers(w.plan(7, {{"sales", 2.0, 5'000, 0}}), w.now));
  ASSERT_TRUE(w.bad().empty());
  std::map<std::string, std::string> kv;
  for (const auto& r : w.recs) {
    if (r.value) kv[r.key] = *r.value; else kv.erase(r.key);
  }
  P5 f(cfg());
  f.now = w.now + 5'000;
  f.t->set_optimizer(true);
  f.t->restore({kv.begin(), kv.end()}, f.now, /*restamp=*/false);
  EXPECT_EQ(f.t->plan_version(), 7u);
  EXPECT_EQ(f.t->plan_valid_until(), w.now + 30'000);
  EXPECT_EQ(f.t->plan_stage(), 0);
  EXPECT_DOUBLE_EQ(f.t->queue_weight("sales"), 2.0);
  EXPECT_EQ(f.t->queue_delay("sales"), 5'000);
  f.advance(1'000);
  EXPECT_EQ(f.t->plan_stage(), 0);
  EXPECT_TRUE(f.bad().empty());
}

// ---------------------------------------------------------------- the safety invariants, randomly

// Random operations with VIP callers, lever plans (some approx), bounds and a VIP reserve; after
// every step: no double offer, no eligible READY agent idle past an entry's delay, every entry
// leaves by its max wait, the busy counts, nothing unmarked.
TEST(Invariants, RandomOperationsKeepThem) {
  for (uint64_t seed = 1; seed <= 6; ++seed) {
    TenantConfig c = cfg(seed % 2 ? RoutingMode::kAuto : RoutingMode::kAdvise);
    c.routing.queues[0].min_agents = 1;
    c.routing.queues[1].max_agents = 1;
    c.routing.queues[0].vip_reserved_min = static_cast<int>(seed % 3);
    c.queues[0].max_wait_ms = 120'000;
    c.queues[1].max_wait_ms = 90'000;
    P5 w(c);
    w.t->set_optimizer(true);
    std::mt19937_64 rng(seed);
    const std::vector<std::string> agents{"a1", "a2", "a3", "v1"};
    for (const auto& a : agents) w.attach_not_ready(a);
    int n = 1;
    uint64_t version = 0;
    for (int step = 0; step < 400; ++step) {
      const std::string& a = agents[rng() % agents.size()];
      const std::string q = rng() % 2 ? "sales" : "support";
      switch (rng() % 9) {
        case 0: (void)w.enqueue(ix(n++), q); break;
        case 1: (void)w.vip(ix(n++), q); break;
        case 2:
          if (w.t->state_of(a) == av1::AGENT_STATE_NOT_READY) {
            (void)w.set(a, av1::TARGET_STATE_READY);
          } else {
            (void)w.set(a, av1::TARGET_STATE_NOT_READY, "break");
          }
          break;
        case 3:
          if (!w.t->live_offer_of(a).empty()) {
            if (rng() % 4) (void)w.accept(a); else (void)w.answer(a, false);
          }
          break;
        case 4:
          if (w.t->state_of(a) == av1::AGENT_STATE_ON_CALL) (void)w.hang_up(a);
          break;
        case 5:
          if (w.t->state_of(a) == av1::AGENT_STATE_WRAP_UP) (void)w.wrap_done(a);
          break;
        case 6:
          (void)w.t->install_levers(w.plan(++version, {{q, 0.25 + (rng() % 16) / 4.0, static_cast<int64_t>(rng() % 60) * 1000,
                                                        static_cast<uint8_t>(rng() % 2)}}),
                                    w.now);
          break;
        case 7: w.advance(static_cast<int64_t>(rng() % 20'000)); break;
        case 8: {
          const auto ids = w.t->entries_snapshot();
          if (!ids.empty()) {
            const auto& e = ids[rng() % ids.size()];
            rv1::DequeueRequest d;
            *d.mutable_owner() = w.owner(e.interaction);
            d.set_entry_id(e.entry_id);
            d.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
            (void)w.t->dequeue(d, w.now);
          }
          break;
        }
      }
      auto v = w.bad();
      ASSERT_TRUE(v.empty()) << "seed " << seed << " step " << step << ": " << v[0];
    }
  }
}

// ---------------------------------------------------------------- the per-agent model and the optimizer

ModelSnapshot two_queue_snapshot(int64_t at, double rho, int agents, double sales_alpha) {
  ModelSnapshot s;
  s.tenant = "acme";
  s.at_ms = at;
  s.window_ms = 900'000;
  s.mode = 2;
  for (const char* id : {"sales", "support"}) {
    ModelSnapshot::Queue q;
    q.id = id;
    const double lam = 0.5 * rho * agents / 180.0;
    q.arrivals = static_cast<uint64_t>(std::llround(lam * 900));
    q.abandons = q.arrivals / 20;
    q.waited_s = q.arrivals * 10.0;
    q.aht_s = 180;
    q.sla_ms = 20'000;
    q.max_wait_ms = 600'000;
    q.alpha = std::string(id) == "sales" ? sales_alpha : 1;
    q.delay_max_ms = 60'000;
    s.queues.push_back(q);
  }
  for (int i = 0; i < agents; ++i) {
    ModelSnapshot::Agent a;
    a.id = "a" + std::to_string(i);
    a.counts = true;
    a.set = {{0, 1}, {1, 1}};
    a.speed = {1.0, 1.0};
    a.eligible = {0, 1};
    s.agents.push_back(a);
  }
  return s;
}

TEST(PerAgentModel, OneQueueIsTheClassicChainAndEveryAgentCounts) {
  // One queue, 10 agents each on their own: the same metrics as one 10-server chain.
  std::vector<model::AQueue> q(1);
  q[0].lambda = 0.045;
  q[0].mu = 1 / 180.0;
  q[0].theta = 1 / 120.0;
  q[0].sla_s = 20;
  std::vector<model::AAgent> ag(10, model::AAgent{{0}, {1}, {1.0}, false});
  model::AgentOptions o;
  o.q.sla_s = 20;
  model::Stats st;
  const auto r = model::solve_agents(q, ag, o, &st);
  ASSERT_TRUE(r.ok);
  const auto m = model::stationary_metrics(model::QueueParams{0.045, 1 / 180.0, 1 / 120.0, 10, -1, 0}, o.q, &st);
  EXPECT_NEAR(r.q[0].m.sl, m.sl, 1e-9);
  EXPECT_NEAR(r.q[0].m.abandon, m.abandon, 1e-9);
  EXPECT_EQ(r.q[0].R, 10);
  double share = 0;
  for (const auto& x : r.shares) share += x.x;
  EXPECT_NEAR(share, r.q[0].busy, 1e-9);
  // A faster agent adds capacity: the pool's service rate grows with the speeds.
  ag[0].speeds = {1.2};
  ag[1].speeds = {1.2};
  const auto r2 = model::solve_agents(q, ag, o, &st);
  EXPECT_GT(r2.q[0].m.sl, r.q[0].m.sl);
}

TEST(PerAgentModel, VipIsATwoClassResult) {
  std::vector<model::AQueue> q(1);
  q[0].lambda = 0.045;
  q[0].vip_lambda = 0.009;
  q[0].mu = 1 / 180.0;
  q[0].theta = 1 / 120.0;
  q[0].sla_s = 20;
  q[0].vip_sla_s = 10;
  std::vector<model::AAgent> ag(10, model::AAgent{{0}, {1}, {1.0}, false});
  model::AgentOptions o;
  model::Stats st;
  const auto r = model::solve_agents(q, ag, o, &st);
  ASSERT_TRUE(r.ok);
  ASSERT_TRUE(r.q[0].vip.ok);
  EXPECT_GT(r.q[0].vip.sl, r.q[0].normal.sl);  // VIP waits less (and is judged at 10 s)
  EXPECT_LT(r.q[0].vip.abandon, r.q[0].normal.abandon);
  EXPECT_EQ(r.q[0].m.quality, model::Quality::kExact);  // rho 0.81: the matrix's validated range
}

// Lever (a): only exact results drive it, a change needs two ticks (hysteresis in time) and the
// writer applies it in auto.
TEST(Optimizer, LeverAChangesAfterTwoTicksAndOnlyFromExactResults) {
  Optimizer::Options o;
  o.levers = true;
  Optimizer::TenantState st;
  model::Stats ms;
  ModelSnapshot s = two_queue_snapshot(T0, 0.85, 10, /*sales_alpha=*/5);
  auto t1 = Optimizer::levers_for(s, &st, o, &ms);
  ASSERT_TRUE(t1.plan);
  ASSERT_EQ(t1.result.q_quality[0], model::Quality::kExact);
  EXPECT_EQ(t1.result.held, "not confirmed");
  EXPECT_DOUBLE_EQ(t1.plan->queues[0].weight, 1.0);
  s.at_ms += 10'000;
  s.window_ms = 10'000;
  for (auto& q : s.queues) {
    q.arrivals = static_cast<uint64_t>(std::llround(0.5 * 0.85 * 10 / 180.0 * 10));
    q.abandons = 0;
    q.waited_s = q.arrivals * 10.0;
  }
  auto t2 = Optimizer::levers_for(s, &st, o, &ms);
  ASSERT_TRUE(t2.plan);
  EXPECT_GT(t2.plan->queues[0].weight, 1.0);  // sales matters 5x: it gets the higher weight
  EXPECT_LT(t2.result.j_planned, t2.result.j_current);
  P5 w(cfg());
  ASSERT_TRUE(w.t->install_levers(t2.plan, w.now));
  EXPECT_GT(w.t->queue_weight("sales"), 1.0);
  EXPECT_TRUE(w.bad().empty());
}

TEST(Optimizer, AnApproxResultNeverChangesLevers) {
  Optimizer::Options o;
  o.levers = true;
  Optimizer::TenantState st;
  model::Stats ms;
  ModelSnapshot s = two_queue_snapshot(T0, 0.97, 10, /*sales_alpha=*/5);  // near critical: approx
  for (int k = 0; k < 3; ++k) {
    auto out = Optimizer::levers_for(s, &st, o, &ms);
    ASSERT_TRUE(out.plan);
    EXPECT_NE(out.result.q_quality[0], model::Quality::kExact);
    EXPECT_DOUBLE_EQ(out.plan->queues[0].weight, 1.0);
    P5 w(cfg());
    ASSERT_TRUE(w.t->install_levers(out.plan, w.now));
    EXPECT_DOUBLE_EQ(w.t->queue_weight("sales"), 1.0);
    s.at_ms += 10'000;
  }
}

// Lever (c): advice only, with the predicted effect; and the events carry the contract's fields.
TEST(Optimizer, RecommendsReadyAgentsWithTheirEffectAndPublishesTheEvents) {
  Optimizer::Options o;
  o.levers = true;
  Optimizer::TenantState st;
  model::Stats ms;
  ModelSnapshot s = two_queue_snapshot(T0, 1.15, 10, 1);
  s.mode = 1;  // advise
  auto out = Optimizer::levers_for(s, &st, o, &ms);
  ASSERT_TRUE(out.plan);
  bool found = false;
  for (const auto& r : out.result.recs) {
    if (r.kind != "ready_agents") continue;
    found = true;
    EXPECT_GT(r.delta, 0);
    EXPECT_GE(r.after.sl, 0.8 - 1e-9);
    EXPECT_LT(r.before.sl, 0.8);
  }
  EXPECT_TRUE(found);
  std::map<std::string, int> types;
  for (const auto& e : out.events) {
    const json j = json::parse(e.json);
    ++types[j.at("type").get<std::string>()];
    EXPECT_EQ(j.at("tenant_id"), "acme");
    EXPECT_EQ(j.at("schema_version"), 1);
    if (j.at("type") == "staffing_plan") {
      EXPECT_EQ(e.topic, std::string("parity.staffing"));
      EXPECT_EQ(j.at("mode"), "advise");
      EXPECT_EQ(j.at("queues").size(), 2u);
      EXPECT_TRUE(j.at("queues")[0].at("weight").contains("planned"));
    }
    if (j.at("type") == "staffing_recommendation") {
      EXPECT_EQ(j.at("recommendations")[0].at("kind"), "ready_agents");
      EXPECT_TRUE(j.at("recommendations")[0].at("predicted").contains("after"));
    }
    if (j.at("type") == "forecast") {
      EXPECT_EQ(e.topic, std::string("parity.forecast"));
    }
    if (j.at("type") == "queue_snapshot") {
      EXPECT_EQ(e.topic, std::string("queue.events"));
    }
  }
  EXPECT_EQ(types["staffing_plan"], 1);
  EXPECT_EQ(types["staffing_recommendation"], 1);
  EXPECT_EQ(types["queue_snapshot"], 2);
  EXPECT_EQ(types["forecast"], 2);  // the first tick is an advice tick
}

// ---------------------------------------------------------------- the forecast and its interface

struct Learned final : opt::ForecastSource {
  bool healthy = true;
  std::optional<opt::QueueForecast> forecast(const std::string&, const std::string&, int64_t,
                                             const opt::QueueForecast& base) override {
    opt::QueueForecast f = base;
    f.source = "whatever";
    for (double& x : f.per_s) x = healthy ? 2 * x + 0.1 : std::nan("");
    f.now_per_s = healthy ? 0.42 : std::nan("");
    return f;
  }
};

TEST(Forecast, SourcesFromConfigDefaultToProfileAndTheLearnedOneFallsBack) {
  opt::Forecaster f;
  EXPECT_EQ(f.forecast("sales", T0, 180).source, "config_default");
  for (int k = 0; k < 60; ++k) f.observe("sales", T0 + k * 10'000, 10, 1, 0, 0, 180, 10, 0);
  EXPECT_EQ(f.forecast("sales", T0 + 600'000, 180).source, "ewma");
  const int64_t now = T0 + 600'000, bucket = 900'000, week = 7LL * 86'400'000;
  for (int wk = 1; wk <= 3; ++wk) {
    for (int b = -4; b < 8; ++b) f.add_actual("sales", now / bucket * bucket + b * bucket - wk * week, 90);
  }
  const auto base = f.forecast("sales", now, 180);
  EXPECT_EQ(base.source, "ewma_profile");
  Learned l;
  const auto m = opt::choose_forecast(base, &l, "acme", now);
  EXPECT_EQ(m.source, "model");
  EXPECT_DOUBLE_EQ(m.now_per_s, 0.42);
  l.healthy = false;
  EXPECT_EQ(opt::choose_forecast(base, &l, "acme", now).source, "ewma_profile");  // bad numbers: the baseline
  EXPECT_EQ(opt::choose_forecast(base, nullptr, "acme", now).source, "ewma_profile");
}

// ---------------------------------------------------------------- the stopped build's fix (a)

// A gateway that never acknowledges: superseded QueuesChanged are coalesced and the outbox is capped.
TEST(Outbox, QueuesChangedCoalesceAndTheOutboxIsCapped) {
  P5 w(cfg(RoutingMode::kOff));
  w.ready("a1", T0 - 1'000);
  w.ready("a2", T0 - 1'000);  // keeps every queue covered
  for (uint64_t v = 1; v <= 40; ++v) {
    RoutingPin p;
    p.agent_id = "a1";
    p.version = v;
    p.queues = {{v % 2 ? "sales" : "support", Tier::kPrimary}};
    ASSERT_TRUE(w.t->install_pin(p, false, w.now));
  }
  EXPECT_GE(w.counters.gw_outbox_coalesced.load(), 30u);
  EXPECT_TRUE(w.bad().empty());
  // A lone agent rejecting the same call over and over: an offer and a state echo each time.
  P5 l(cfg(RoutingMode::kOff));
  l.ready("a1", T0 - 1'000);
  ASSERT_TRUE(l.enqueue(ix(1), "sales"));
  for (int k = 0; k < 200 && !l.t->live_offer_of("a1").empty(); ++k) ASSERT_TRUE(l.answer("a1", false));
  EXPECT_GT(l.counters.gw_outbox_dropped.load(), 0u);
  auto v = l.bad();
  EXPECT_TRUE(v.empty()) << (v.empty() ? "" : v[0]);
}

struct MemLog final : StateLog {
  std::mutex mu;
  size_t batches = 0;
  void submit(Batch&& b) override {
    {
      std::lock_guard lk(mu);
      ++batches;
    }
    for (auto& fx : b.effects) fx();
  }
  size_t backlog() const override { return 0; }
};

// A tenant whose record outgrows the limit is quarantined alone: its posts are refused (UNAVAILABLE)
// and the bin's other tenant carries on.
#if defined(__SANITIZE_THREAD__)
#define PCR_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define PCR_TSAN 1
#endif
#endif

TEST(Quarantine, AnOversizedRecordQuarantinesItsTenantOnly) {
#ifdef PCR_TSAN
  // CR-172: Engine::set_state_log writes log_ after the constructor started the shard threads, which
  // read it every loop (a race at startup, also in main.cc); logged, not fixed here.
  GTEST_SKIP() << "CR-172 (set_state_log after the shards started)";
#endif
  ManualClock clock(T0);
  Rec rec;
  MemLog log;
  Engine::Limits lim;
  lim.max_record_bytes = 3'000;
  Engine engine(&clock, &rec, 1, lim);
  engine.set_state_log(&log);
  engine.load({cfg(RoutingMode::kOff, "acme"), cfg(RoutingMode::kOff, "globex")}, Epochs{1, 0});
  // Tasks capture nothing of this stack (TSan): the engine's barrier waits for them.
  ASSERT_TRUE(engine.post("acme", [](Tenant* t, int64_t now) {
    if (!t) return;
    auto s = t->seed_attached("a1", "gw-1", "", now, now);
    (void)t->inject_queue_call(ix(1), "sales", now);
    // A lone agent rejecting the call 100 times, nobody acknowledging: a1's record (its outbox of
    // offers and state echoes) passes 3 kB in this one task.
    for (int k = 0; k < 100 && !t->live_offer_of("a1").empty(); ++k) {
      av1::AnswerOfferRequest r;
      r.mutable_session()->set_tenant_id("acme");
      r.mutable_session()->set_agent_id("a1");
      r.mutable_session()->set_session_id(s.session_id);
      r.mutable_session()->set_attach_generation(s.attach_generation);
      r.set_command_id("c" + std::to_string(k));
      r.set_offer_id(t->live_offer_of("a1"));
      r.set_decision(av1::DECISION_REJECT);
      (void)t->answer_offer(r, now);
    }
  }, false));
  engine.barrier();
  EXPECT_TRUE(engine.quarantined("acme"));
  EXPECT_FALSE(engine.quarantined("globex"));
  EXPECT_FALSE(engine.post("acme", [](Tenant*, int64_t) {}, true));
  auto served = std::make_shared<std::atomic<bool>>(false);
  ASSERT_TRUE(engine.post("globex", [served](Tenant* t, int64_t now) {
    if (t) {
      t->seed_attached("a1", "gw-1", "", now, now);
      served->store(true);
    }
  }, false));
  engine.barrier();
  EXPECT_TRUE(served->load());
  EXPECT_EQ(engine.totals().tenants_quarantined, 1u);
}

}  // namespace
}  // namespace pcr
