// Phase 4 (PLAN §4): active sets with tiers, the dispatch score, pins and exclusions, moves under
// dwell, caps, change rate and coverage, the "next offer, never mid-call" rule, tenant isolation of
// plans and pins, a supervisor's forced state. Every test ends with the writer's invariants
// (check_invariants: one live offer per agent and entry, offers inside the active set unless
// escalated, no eligible READY agent idle past an entry's delay, max wait, no move mid-call).
#include <gtest/gtest.h>

#include "src/core/tenant.h"
#include "tests/unit/world.h"

namespace pcr {
namespace {

using namespace testing_world;

constexpr const char* IX1 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819201";
constexpr const char* IX2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819202";
constexpr const char* IX3 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";

TenantConfig cfg() {
  TenantConfig c;
  c.id = "acme";
  c.queues = {queue("sales", {{"en", 0}}), queue("support")};
  c.agents = {agent("a1", {"sales", "support"}, {{"en", 2}}), agent("a2", {"sales", "support"}, {{"en", 2}}),
              agent("a3", {"support"})};
  c.routing.version = 1;
  c.routing.queues = {QueuePolicy{"sales", 1, 1, 0, 0, 10'000, 0}, QueuePolicy{"support", 1, 1, 0, 0, 10'000, 0}};
  return c;
}

RoutingPin pin(const std::string& agent, std::vector<std::pair<std::string, Tier>> q, uint64_t v,
               std::vector<std::string> excl = {}) {
  RoutingPin p;
  p.agent_id = agent;
  p.version = v;
  p.queues = std::move(q);
  p.exclusions = std::move(excl);
  return p;
}

std::vector<std::pair<std::string, Tier>> set(std::vector<std::pair<std::string, Tier>> v) { return v; }

struct P4 : World {
  using World::World;
  // The latest reservation id seen on the IE stream.
  std::string last_reservation() {
    for (auto it = rec.ie.rbegin(); it != rec.ie.rend(); ++it) {
      if (it->second.has_assigned()) return it->second.assigned().reservation_id();
    }
    return {};
  }
  void connected(const std::string& ix, const std::string& res) {
    rv1::ReportOutcomeRequest r;
    *r.mutable_owner() = owner(ix);
    r.set_reservation_id(res);
    r.set_outcome(rv1::OUTCOME_CONNECTED);
    ASSERT_TRUE(t->report_outcome(r, now));
  }
  void ended(const std::string& ix, const std::string& res, const std::string& agent) {
    rv1::CallEndedRequest r;
    *r.mutable_owner() = owner(ix);
    r.set_reservation_id(res);
    r.set_agent_id(agent);
    ASSERT_TRUE(t->call_ended(r, now));
  }
  void wrap_done(const std::string& agent, const std::string& res) {
    av1::EndWrapUpRequest r;
    *r.mutable_session() = ref(agent);
    r.set_command_id(next_cmd());
    r.set_reservation_id(res);
    r.set_disposition_code("sold");
    ASSERT_TRUE(t->end_wrap_up(r, now));
  }
  int queues_changed(const std::string& agent) {
    int n = 0;
    for (const auto& [r, m] : rec.gw) n += m.has_queues_changed() && m.agent_id() == agent ? 1 : 0;
    return n;
  }
  RoutingPolicy auto_policy(uint64_t v, int64_t dwell_ms = 300'000, int caps = 4) {
    RoutingPolicy p = t->policy();
    p.version = v;
    p.mode = RoutingMode::kAuto;
    p.dwell_min_ms = dwell_ms;
    p.max_moves_per_hour = caps;
    return p;
  }
};

TEST(Moves, ModeOffEveryMemberQueueIsPrimary) {
  P4 w(cfg());
  EXPECT_EQ(w.t->active_of("a1"), set({{"sales", Tier::kPrimary}, {"support", Tier::kPrimary}}));
  EXPECT_EQ(w.t->active_of("a3"), set({{"support", Tier::kPrimary}}));
}

// §4.3: a pin applies at the next offer, never mid-call; the reservation and the call are untouched.
TEST(Moves, PinWaitsForTheCallToEndAndNeverTouchesTheReservation) {
  P4 w(cfg());
  w.ready("a1", T0 - 30'000);
  w.ready("a3", T0 - 1'000);
  w.attach_not_ready("a2");  // logged in: sales stays covered when a1 leaves it
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  ASSERT_TRUE(w.answer("a1", true));
  const std::string res = w.last_reservation();
  ASSERT_FALSE(res.empty());
  ASSERT_TRUE(w.t->install_pin(pin("a1", {{"support", Tier::kPrimary}}, 1), false, w.now));
  EXPECT_EQ(w.t->active_of("a1"), set({{"sales", Tier::kPrimary}, {"support", Tier::kPrimary}}));
  EXPECT_EQ(w.t->pending_of("a1"), set({{"support", Tier::kPrimary}}));
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_RESERVED);
  EXPECT_EQ(w.t->live_reservations(), std::vector<std::string>{res});
  EXPECT_EQ(w.queues_changed("a1"), 1);  // the desktop shows "moving to support after this call"
  w.connected(IX1, res);
  w.ended(IX1, res, "a1");
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_WRAP_UP);
  EXPECT_FALSE(w.t->pending_of("a1").empty());  // wrap-up: still pending
  EXPECT_TRUE(w.broken().empty());
  w.wrap_done("a1", res);
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_READY);
  EXPECT_EQ(w.t->active_of("a1"), set({{"support", Tier::kPrimary}}));
  EXPECT_TRUE(w.t->pending_of("a1").empty());
  // The next sales call isn't a1's any more (a2 is the only sales agent ready).
  ASSERT_TRUE(w.set("a2", av1::TARGET_STATE_READY));
  ASSERT_TRUE(w.enqueue(IX2, "sales"));
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  EXPECT_FALSE(w.t->live_offer_of("a2").empty());
  EXPECT_TRUE(w.broken().empty());
}

TEST(Moves, PinDuringAnOfferKeepsTheOfferAndAppliesAfterTheReject) {
  P4 w(cfg());
  w.ready("a1", T0 - 30'000);
  w.ready("a3", T0 - 1'000);
  w.attach_not_ready("a2");
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  const std::string offer = w.t->live_offer_of("a1");
  ASSERT_FALSE(offer.empty());
  ASSERT_TRUE(w.t->install_pin(pin("a1", {{"support", Tier::kPrimary}}, 1), false, w.now));
  EXPECT_EQ(w.t->live_offer_of("a1"), offer);  // a move never revokes an offer
  EXPECT_TRUE(w.broken().empty());
  ASSERT_TRUE(w.answer("a1", false));
  EXPECT_EQ(w.t->active_of("a1"), set({{"support", Tier::kPrimary}}));
  EXPECT_TRUE(w.broken().empty());
}

// §4.3 coverage: a change that would leave a queue with no logged-in eligible agent waits.
TEST(Moves, ACoverageBreakingPinWaitsUntilSomeoneCoversTheQueue) {
  TenantConfig c = cfg();
  P4 w(c);
  w.ready("a1", T0 - 5'000);  // the only agent logged in: covers sales and support
  ASSERT_FALSE(w.t->install_pin(pin("a1", {{"sales", Tier::kPrimary}}, 1), false, w.now) &&
               w.t->active_of("a1") == set({{"sales", Tier::kPrimary}}));
  EXPECT_EQ(w.t->active_of("a1"), set({{"sales", Tier::kPrimary}, {"support", Tier::kPrimary}}));
  EXPECT_EQ(w.counters.coverage_refused.load(), 1u);
  w.ready("a3", w.now);  // now support has a3
  w.advance(1'000);       // the sweep retries the pin
  EXPECT_EQ(w.t->active_of("a1"), set({{"sales", Tier::kPrimary}}));
  EXPECT_TRUE(w.broken().empty());
}

// §4.3 tenant isolation: a pin, plan or policy naming another tenant's agent or a queue this tenant
// doesn't have changes nothing and is counted.
TEST(Moves, ForeignAgentsAndQueuesAreRefusedAndCounted) {
  P4 w(cfg());
  w.ready("a1", T0 - 5'000);
  const auto before = w.t->active_of("a1");
  std::string why;
  EXPECT_FALSE(w.t->install_pin(pin("globex-agent", {{"sales", Tier::kPrimary}}, 1), false, w.now, &why));
  EXPECT_FALSE(w.t->install_pin(pin("a1", {{"billing", Tier::kPrimary}}, 2), false, w.now, &why));
  ASSERT_TRUE(w.t->install_policy(w.auto_policy(2), w.now));
  auto res = w.t->apply_moves({Tenant::Move{"globex-agent", {{"sales", Tier::kPrimary}}, {}},
                               Tenant::Move{"a1", {{"billing", Tier::kPrimary}}, {}}},
                              w.now);
  EXPECT_EQ(res.refused, 2);
  RoutingPolicy bad = w.auto_policy(3);
  bad.queues.push_back(QueuePolicy{"billing", 1, 1, 0, 0, 0, 0});
  EXPECT_FALSE(w.t->install_policy(bad, w.now, &why));
  EXPECT_EQ(w.counters.foreign_refused.load(), 5u);
  EXPECT_EQ(w.t->active_of("a1"), before);
  EXPECT_TRUE(w.broken().empty());
}

// §4.2: a secondary takes a call only after the queue's delay; a primary who becomes ready first
// takes it.
TEST(Moves, SecondaryOnlyAfterTheDelayAndPrimaryFirst) {
  TenantConfig c = cfg();
  c.pins = {pin("a1", {{"sales", Tier::kPrimary}}, 1), pin("a2", {{"sales", Tier::kSecondary}, {"support", Tier::kPrimary}}, 1)};
  P4 w(c);
  w.ready("a2", T0 - 60'000);  // the longest idle, but a secondary for sales
  w.ready("a3", T0);
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  EXPECT_TRUE(w.t->live_offer_of("a2").empty());
  EXPECT_TRUE(w.broken().empty());
  w.advance(9'000);
  EXPECT_TRUE(w.t->live_offer_of("a2").empty());
  w.advance(1'000);  // d = 10 s
  EXPECT_FALSE(w.t->live_offer_of("a2").empty());
  EXPECT_TRUE(w.broken().empty());
  // A second call: the primary a1 becomes ready within the delay and takes it.
  ASSERT_TRUE(w.enqueue(IX2, "sales"));
  w.advance(2'000);
  w.ready("a1", w.now);
  w.t->sweep(w.now);
  EXPECT_FALSE(w.t->live_offer_of("a1").empty());
  EXPECT_TRUE(w.broken().empty());
}

// §4.3: dwell and the hourly cap bind the optimizer's moves, not a supervisor's pin.
TEST(Moves, DwellBlocksASecondMoveButNotAPin) {
  P4 w(cfg());
  w.ready("a1", T0 - 5'000);
  w.ready("a2", T0 - 5'000);
  w.ready("a3", T0 - 5'000);
  ASSERT_TRUE(w.t->install_policy(w.auto_policy(2), w.now));
  auto r1 = w.t->apply_moves({Tenant::Move{"a1", {{"support", Tier::kPrimary}}, {}}}, w.now);
  EXPECT_EQ(r1.applied, 1);
  w.advance(60'000);
  auto r2 = w.t->apply_moves({Tenant::Move{"a1", {{"sales", Tier::kPrimary}}, {}}}, w.now);
  EXPECT_EQ(r2.refused, 1);
  ASSERT_EQ(r2.why.size(), 1u);
  EXPECT_EQ(r2.why[0], "a1: dwell");
  EXPECT_EQ(w.t->active_of("a1"), set({{"support", Tier::kPrimary}}));
  ASSERT_TRUE(w.t->install_pin(pin("a1", {{"sales", Tier::kPrimary}}, 1), false, w.now));
  EXPECT_EQ(w.t->active_of("a1"), set({{"sales", Tier::kPrimary}}));
  EXPECT_EQ(w.t->apply_moves({Tenant::Move{"a1", {{"support", Tier::kPrimary}}, {}}}, w.now).refused, 1);  // pinned
  EXPECT_TRUE(w.broken().empty());
}

TEST(Moves, HourlyCapAndChangeRate) {
  P4 w(cfg());
  for (const char* a : {"a1", "a2", "a3"}) w.ready(a, T0 - 5'000);
  ASSERT_TRUE(w.t->install_policy(w.auto_policy(2, 60'000, 1), w.now));
  EXPECT_EQ(w.t->apply_moves({Tenant::Move{"a1", {{"support", Tier::kPrimary}}, {}}}, w.now).applied, 1);
  w.advance(61'000);
  auto capped = w.t->apply_moves({Tenant::Move{"a1", {{"sales", Tier::kPrimary}}, {}}}, w.now);
  ASSERT_EQ(capped.why.size(), 1u);
  EXPECT_EQ(capped.why[0], "a1: hourly cap");
  // CR-018: a secondary-only change at most once per D_min/2.
  auto sec = w.t->apply_moves({Tenant::Move{"a1", {{"support", Tier::kPrimary}, {"sales", Tier::kSecondary}}, {}}}, w.now);
  EXPECT_EQ(sec.applied, 1);
  w.advance(10'000);
  auto rate = w.t->apply_moves({Tenant::Move{"a1", {{"support", Tier::kPrimary}}, {}}}, w.now);
  ASSERT_EQ(rate.why.size(), 1u);
  EXPECT_EQ(rate.why[0], "a1: change rate");
  EXPECT_TRUE(w.broken().empty());
}

// CR-018: a weights-only change says nothing to the desktop; weights steer the dispatch score.
TEST(Moves, WeightsAreSilentAndSteerTheScore) {
  P4 w(cfg());
  w.ready("a3", T0 - 5'000);
  ASSERT_TRUE(w.t->install_policy(w.auto_policy(2), w.now));
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  ASSERT_TRUE(w.enqueue(IX2, "support"));  // a3 takes it at once
  ASSERT_FALSE(w.t->live_offer_of("a3").empty());
  w.advance(1'000);
  ASSERT_TRUE(w.enqueue(IX3, "support"));
  // Both waiting entries: sales (older) and support (newer). Equal tau: sales would win at w = 1.
  w.attach_not_ready("a1");
  const int before = w.queues_changed("a1");
  auto r = w.t->apply_moves({Tenant::Move{"a1", {{"sales", Tier::kPrimary}, {"support", Tier::kPrimary}}, {{"support", 4.0}, {"sales", 1.0}}}}, w.now);
  EXPECT_EQ(r.silent, 1);
  EXPECT_EQ(w.queues_changed("a1"), before);
  w.advance(1'000);
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_READY));
  ASSERT_FALSE(w.t->live_offer_of("a1").empty());
  EXPECT_EQ(w.rec.gw.back().second.offer().queue_id(), "support");  // 4 x 1 s > 1 x 2 s
  EXPECT_TRUE(w.broken().empty());
}

// Phase 5 redesign (the user, 2026-11-01): no mode moves agents between queues automatically.
// config.routing's min/max agents are lever (b) in the dispatch (tests/unit/phase5_test.cc), so an
// auto policy with max_agents leaves every active set as it was (phase 4 demoted primaries here).
TEST(Moves, AutoBoundsNeverMoveAgents) {
  TenantConfig c = cfg();
  P4 w(c);
  for (const char* a : {"a1", "a2", "a3"}) w.ready(a, T0 - 5'000);
  const auto before1 = w.t->active_of("a1"), before2 = w.t->active_of("a2");
  RoutingPolicy p = w.auto_policy(2, 60'000);
  p.queues[0].max_agents = 1;  // sales
  ASSERT_TRUE(w.t->install_policy(p, w.now));
  w.advance(30'000);
  w.t->sweep(w.now);
  EXPECT_EQ(w.t->active_of("a1"), before1);
  EXPECT_EQ(w.t->active_of("a2"), before2);
  EXPECT_TRUE(w.broken().empty());
}

// The "no starvation" rule over tiers: an entry past escalate_after goes to any eligible agent,
// even one without the queue in its active set, before a higher-priority entry.
TEST(Moves, EscalatedEntryReachesAnAgentOutsideItsActiveSet) {
  TenantConfig c = cfg();
  c.pins = {pin("a1", {{"support", Tier::kPrimary}}, 1)};
  P4 w(c);
  w.ready("a3", T0);  // covers support
  ASSERT_TRUE(w.enqueue(IX1, "sales", 10));
  w.advance(61'000);  // escalate_after = 3 tau
  ASSERT_TRUE(w.enqueue(IX2, "support", 100));  // a3 takes it
  w.ready("a1", w.now);
  w.t->sweep(w.now);
  ASSERT_FALSE(w.t->live_offer_of("a1").empty());
  EXPECT_EQ(w.rec.gw.back().second.offer().queue_id(), "sales");
  EXPECT_EQ(w.rec.gw.back().second.offer().tier(), av1::MEMBERSHIP_TIER_UNSPECIFIED);
  EXPECT_TRUE(w.broken().empty());
}

// agents.v1 ForceState (CR-072): RESERVED -> the reservation revoked (L8), the call back at its
// ordering time; ON_CALL -> pending, the call untouched.
TEST(Moves, ForcedStateRevokesAReservationButNeverACall) {
  P4 w(cfg());
  w.ready("a1", T0 - 30'000);
  w.ready("a2", T0 - 1'000);
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  ASSERT_TRUE(w.answer("a1", true));
  const std::string res = w.last_reservation();
  av1::ForceStateRequest f;
  f.set_tenant_id("acme");
  f.set_agent_id("a1");
  f.set_command_id("sup:1");
  f.set_target(av1::TARGET_STATE_NOT_READY);
  f.set_reason_code("supervisor");
  ASSERT_TRUE(w.t->force_state(f, w.now));
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_NOT_READY);
  ASSERT_TRUE(w.rec.ie.back().second.has_revoke() || w.t->live_offer_of("a2").size() > 0);
  bool revoked = false;
  for (const auto& [r, m] : w.rec.ie) revoked |= m.has_revoke() && m.revoke().reason() == "forced_state";
  EXPECT_TRUE(revoked);
  EXPECT_FALSE(w.t->live_offer_of("a2").empty());  // the call went back and on to a2
  ASSERT_TRUE(w.answer("a2", true));
  const std::string res2 = w.last_reservation();
  w.connected(IX1, res2);
  f.set_agent_id("a2");
  f.set_command_id("sup:2");
  f.set_target(av1::TARGET_STATE_LOGGED_OUT);
  ASSERT_TRUE(w.t->force_state(f, w.now));
  EXPECT_EQ(w.t->state_of("a2"), av1::AGENT_STATE_ON_CALL);  // pending: calls are never touched
  EXPECT_EQ(w.t->live_reservations(), std::vector<std::string>{res2});
  EXPECT_TRUE(w.broken().empty());
}

// The user's requirement (2026-11-01): more than two queues. An agent active on four queues (two
// primaries, two secondaries) takes from all of them in score order; a pin moves it across three
// queues at its next offer; coverage holds throughout.
TEST(Moves, FourQueuesTwoPrimariesTwoSecondariesInScoreOrder) {
  TenantConfig c;
  c.id = "acme";
  c.queues = {queue("billing"), queue("sales"), queue("support"), queue("retention")};
  c.agents = {agent("m", {"billing", "sales", "support", "retention"}), agent("cover", {"billing", "sales", "support", "retention"})};
  c.routing.version = 1;
  for (const char* q : {"billing", "sales", "support", "retention"}) c.routing.queues.push_back(QueuePolicy{q, 1, 1, 0, 0, 10'000, 0});
  c.pins = {pin("m", {{"billing", Tier::kPrimary}, {"sales", Tier::kPrimary}, {"support", Tier::kSecondary}, {"retention", Tier::kSecondary}}, 1)};
  P4 w(c);
  w.attach_not_ready("cover");  // logged in: every queue stays covered
  w.attach_not_ready("m");
  EXPECT_EQ(w.t->active_of("m"), set({{"billing", Tier::kPrimary}, {"retention", Tier::kSecondary},
                                      {"sales", Tier::kPrimary}, {"support", Tier::kSecondary}}));
  // Four waiting calls, one per queue, 1 s apart (billing oldest), all past the secondary delay.
  const char* ids[] = {"0192f6a1-7c3e-7b21-9a4d-5e6f70819211", "0192f6a1-7c3e-7b21-9a4d-5e6f70819212",
                       "0192f6a1-7c3e-7b21-9a4d-5e6f70819213", "0192f6a1-7c3e-7b21-9a4d-5e6f70819214"};
  const char* qs[] = {"billing", "sales", "support", "retention"};
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(w.enqueue(ids[i], qs[i]));
    w.advance(1'000);
  }
  w.advance(10'000);
  // m takes them one by one, oldest SLA-normalized age first (equal tau, w = 1): every queue in
  // turn, secondaries included once past their delay.
  std::vector<std::string> order;
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(w.set("m", av1::TARGET_STATE_READY));
    ASSERT_FALSE(w.t->live_offer_of("m").empty()) << i;
    order.push_back(w.rec.gw.back().second.offer().queue_id());
    EXPECT_EQ(w.rec.gw.back().second.offer().tier(), i < 2 ? av1::MEMBERSHIP_TIER_PRIMARY : av1::MEMBERSHIP_TIER_SECONDARY);
    ASSERT_TRUE(w.answer("m", false));  // rejected: m stays READY at the back; no one else is ready
    ASSERT_TRUE(w.set("m", av1::TARGET_STATE_NOT_READY, "break"));
    // The rejected call keeps its place; take it out of the race for the next round.
    rv1::DequeueRequest d;
    *d.mutable_owner() = w.owner(ids[i]);
    d.set_entry_id([&] {
      for (const auto& e : w.t->entries_snapshot()) {
        if (e.interaction == ids[i]) return e.entry_id;
      }
      return std::string();
    }());
    d.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
    ASSERT_TRUE(w.t->dequeue(d, w.now));
    EXPECT_TRUE(w.broken().empty());
  }
  EXPECT_EQ(order, (std::vector<std::string>{"billing", "sales", "support", "retention"}));
  // A pin across three queues, made while m holds an offer: pending until the offer resolves.
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819215", "billing"));
  ASSERT_TRUE(w.set("m", av1::TARGET_STATE_READY));
  ASSERT_FALSE(w.t->live_offer_of("m").empty());
  ASSERT_TRUE(w.t->install_pin(pin("m", {{"support", Tier::kPrimary}, {"retention", Tier::kPrimary}, {"sales", Tier::kSecondary}}, 2), false, w.now));
  EXPECT_EQ(w.t->pending_of("m"), set({{"retention", Tier::kPrimary}, {"sales", Tier::kSecondary}, {"support", Tier::kPrimary}}));
  EXPECT_EQ(w.t->active_of("m").size(), 4u);
  EXPECT_TRUE(w.broken().empty());
  ASSERT_TRUE(w.answer("m", false));
  EXPECT_EQ(w.t->active_of("m"), set({{"retention", Tier::kPrimary}, {"sales", Tier::kSecondary}, {"support", Tier::kPrimary}}));
  // Coverage: billing left m's set, and "cover" (logged in) still has it.
  w.t->sweep(w.now);
  EXPECT_TRUE(w.broken().empty());
  // A pin that would take billing from the last agent with it waits (coverage).
  ASSERT_TRUE(w.t->install_pin(pin("cover", {{"sales", Tier::kPrimary}}, 1), false, w.now));
  EXPECT_EQ(w.t->active_of("cover").size(), 4u);
  EXPECT_GE(w.counters.coverage_refused.load(), 1u);
  EXPECT_TRUE(w.broken().empty());
}

}  // namespace
}  // namespace pcr
