// Phase 8: campaign lending (IE step 13a; CONVENTIONS "Campaign lending"; PLAN §4.3 "Mode"). The main
// behaviour and the safety invariants: a lent agent never gets an inbound offer (escalated, direct and
// callback entries included); a switch happens only when the agent is idle or after its wrap-up;
// every queue keeps coverage (a pending lend counts as gone); tenant isolation; one reservation per
// agent; the mode and pending switches survive a takeover (restore from re.state).
#include <gtest/gtest.h>

#include "src/state/state.pb.h"
#include "tests/unit/world.h"

namespace pcr {
namespace {

using namespace testing_world;

const char* const kIx1 = "0192f6a1-7c3e-7b21-9a4d-000000000001";
const char* const kIx2 = "0192f6a1-7c3e-7b21-9a4d-000000000002";
const char* const kCampCall = "0192f6d0-3a1b-7c2d-8e3f-000000000001";
const char* const kCampCall2 = "0192f6d0-3a1b-7c2d-8e3f-000000000002";

TenantConfig cfg() {
  TenantConfig c;
  c.id = "acme";
  c.queues = {queue("sales"), queue("support")};
  c.queues[0].escalate_after_ms = 20'000;
  c.agents = {agent("a1", {"sales"}), agent("a2", {"sales"}), agent("a3", {"support", "sales"})};
  return c;
}

rv1::CampaignOwner camp(uint64_t epoch = 5, const std::string& campaign = "spring", const std::string& tenant = "acme") {
  rv1::CampaignOwner o;
  o.set_tenant_id(tenant);
  o.set_campaign_id(campaign);
  o.set_owner_epoch(epoch);
  o.set_replica_id("ie-1");
  return o;
}

Result<rv1::LendAgentsResponse> lend(World& w, std::vector<std::string> ids, uint64_t epoch = 5,
                                     const std::string& campaign = "spring") {
  rv1::LendAgentsRequest r;
  *r.mutable_owner() = camp(epoch, campaign);
  for (auto& id : ids) r.add_agent_ids(id);
  return w.t->lend_agents(r, w.now);
}

Result<rv1::ReturnAgentsResponse> give_back(World& w, std::vector<std::string> ids, uint64_t epoch = 5) {
  rv1::ReturnAgentsRequest r;
  *r.mutable_owner() = camp(epoch);
  for (auto& id : ids) r.add_agent_ids(id);
  return w.t->return_agents(r, w.now);
}

Result<rv1::ReserveAgentResponse> reserve(World& w, const std::string& agent, const std::string& ix) {
  rv1::ReserveAgentRequest r;
  *r.mutable_owner() = w.owner(ix, 300);
  *r.mutable_campaign() = camp();
  r.set_agent_id(agent);
  return w.t->reserve_agent(r, w.now);
}

// Accepts the offer a1 holds and connects the call (an inbound call in progress).
std::string connect(World& w, const std::string& agent, const std::string& ix) {
  EXPECT_FALSE(w.t->live_offer_of(agent).empty());
  auto a = w.answer(agent, true);
  EXPECT_TRUE(a);
  const std::string rid = a->reservation().reservation_id();
  rv1::ReportOutcomeRequest r;
  *r.mutable_owner() = w.owner(ix);
  r.set_reservation_id(rid);
  r.set_outcome(rv1::OUTCOME_CONNECTED);
  EXPECT_TRUE(w.t->report_outcome(r, w.now));
  return rid;
}

void end_call(World& w, const std::string& agent, const std::string& ix, const std::string& rid, uint64_t epoch = 100) {
  rv1::CallEndedRequest c;
  *c.mutable_owner() = w.owner(ix, epoch);
  c.set_agent_id(agent);
  c.set_reservation_id(rid);
  c.set_ended_at_ms(w.now);
  ASSERT_TRUE(w.t->call_ended(c, w.now));
}

void end_wrap_up(World& w, const std::string& agent, const std::string& rid) {
  av1::EndWrapUpRequest e;
  *e.mutable_session() = w.ref(agent);
  e.set_command_id(w.next_cmd());
  e.set_reservation_id(rid);
  ASSERT_TRUE(w.t->end_wrap_up(e, w.now));
}

std::vector<rv1::AgentAvailability> availability(const World& w) {
  std::vector<rv1::AgentAvailability> out;
  for (const auto& [r, m] : w.rec.ie) {
    if (m.has_agent_availability()) out.push_back(m.agent_availability());
  }
  return out;
}

TEST(Lending, LentAgentGetsNoInboundOfferEvenEscalatedOrDirect) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);  // longest idle
  w.ready("a2", T0 - 10'000);
  w.attach_not_ready("a3");
  auto r = lend(w, {"a1"});
  ASSERT_TRUE(r);
  ASSERT_EQ(r->agents(0).result(), rv1::LEND_RESULT_LENT);
  EXPECT_EQ(w.t->campaign_of("a1"), "spring");
  EXPECT_TRUE(w.t->active_of("a1").empty());
  ASSERT_TRUE(w.enqueue(kIx1, "sales"));
  EXPECT_TRUE(w.t->live_offer_of("a1").empty()) << "a lent agent was offered an inbound call";
  EXPECT_FALSE(w.t->live_offer_of("a2").empty());
  // A second call waits; past escalate_after it may go to any eligible agent: never the lent one.
  ASSERT_TRUE(w.enqueue(kIx2, "sales"));
  w.advance(30'000);
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  // A direct call (or a consult) to a lent agent is refused.
  rv1::EnqueueRequest d;
  *d.mutable_owner() = w.owner("0192f6a1-7c3e-7b21-9a4d-000000000003");
  d.set_enqueue_seq(1);
  d.mutable_target()->set_agent_id("a1");
  d.set_first_queued_at_ms(w.now);
  d.set_ordering_at_ms(w.now);
  auto de = w.t->enqueue(d, w.now);
  ASSERT_FALSE(de);
  EXPECT_EQ(de.error().parity, "agent_unavailable");
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
  // The leader heard READY; the agent is out of the logged-in counts (and so of the model).
  auto av = availability(w);
  ASSERT_FALSE(av.empty());
  EXPECT_EQ(av.back().state(), rv1::AVAILABILITY_STATE_READY);
  EXPECT_EQ(av.back().owner_epoch(), 5u);
}

TEST(Lending, SwitchWaitsForWrapUpAndNeverMovesMidCall) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);
  w.attach_not_ready("a2");
  w.attach_not_ready("a3");
  ASSERT_TRUE(w.enqueue(kIx1, "sales"));
  const std::string rid = connect(w, "a1", kIx1);
  auto r = lend(w, {"a1"});
  ASSERT_TRUE(r);
  EXPECT_EQ(r->agents(0).result(), rv1::LEND_RESULT_PENDING);
  EXPECT_TRUE(w.t->campaign_of("a1").empty()) << "switched mid-call";
  EXPECT_TRUE(w.t->mode_pending_of("a1"));
  EXPECT_FALSE(w.t->active_of("a1").empty()) << "the active set moved mid-call";
  EXPECT_TRUE(w.t->pending_of("a1").empty());
  // Pending is not lent: it can't be reserved.
  auto rs = reserve(w, "a1", kCampCall);
  ASSERT_FALSE(rs);
  EXPECT_EQ(rs.error().parity, "agent_not_lent");
  end_call(w, "a1", kIx1, rid);
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_WRAP_UP);
  EXPECT_TRUE(w.t->campaign_of("a1").empty()) << "switched during wrap-up";
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
  end_wrap_up(w, "a1", rid);
  EXPECT_EQ(w.t->campaign_of("a1"), "spring");
  EXPECT_FALSE(w.t->mode_pending_of("a1"));
  EXPECT_TRUE(w.t->active_of("a1").empty());
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
  auto av = availability(w);
  ASSERT_GE(av.size(), 2u);
  EXPECT_EQ(av.front().state(), rv1::AVAILABILITY_STATE_PENDING);
  EXPECT_EQ(av.back().state(), rv1::AVAILABILITY_STATE_READY);
}

TEST(Lending, CoverageIsRefusedAndAPendingLendCountsAsGone) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);
  w.ready("a2", T0 - 20'000);
  // support's only member is a3: lending it would leave support uncovered.
  w.attach_not_ready("a3");
  auto r = lend(w, {"a3"});
  ASSERT_TRUE(r);
  EXPECT_EQ(r->agents(0).result(), rv1::LEND_RESULT_COVERAGE);
  EXPECT_TRUE(w.t->campaign_of("a3").empty());
  // a1 on a call is lent pending; a2 is then sales' last cover besides a3 (also in sales).
  ASSERT_TRUE(w.enqueue(kIx1, "sales"));
  connect(w, "a1", kIx1);
  ASSERT_EQ(lend(w, {"a1"})->agents(0).result(), rv1::LEND_RESULT_PENDING);
  ASSERT_EQ(lend(w, {"a2"})->agents(0).result(), rv1::LEND_RESULT_LENT);  // a3 still covers sales
  // Now a3 is the last cover of both queues (a1 pending counts as gone).
  EXPECT_EQ(lend(w, {"a3"})->agents(0).result(), rv1::LEND_RESULT_COVERAGE);
  EXPECT_GE(w.counters.coverage_refused.load(), 2u);
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

TEST(Lending, ReturnGivesTheQueuesBackWithNoDwellAndTakesTheWaitingCall) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);
  w.attach_not_ready("a2");
  w.attach_not_ready("a3");
  ASSERT_EQ(lend(w, {"a1"})->agents(0).result(), rv1::LEND_RESULT_LENT);
  ASSERT_TRUE(w.enqueue(kIx1, "sales"));
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  auto g = give_back(w, {"a1"});
  ASSERT_TRUE(g);
  EXPECT_EQ(g->agents(0).result(), rv1::RETURN_RESULT_RETURNED);
  EXPECT_TRUE(w.t->campaign_of("a1").empty());
  EXPECT_FALSE(w.t->active_of("a1").empty());
  EXPECT_FALSE(w.t->live_offer_of("a1").empty()) << "returned, it takes the waiting call at once";
  EXPECT_EQ(availability(w).back().state(), rv1::AVAILABILITY_STATE_RETURNED);
  // A second return: not lent.
  EXPECT_EQ(give_back(w, {"a1"})->agents(0).result(), rv1::RETURN_RESULT_NOT_LENT);
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

TEST(Lending, CampaignReservationRunsLikeAnInboundOneAndNeverDoubleBooks) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);
  w.attach_not_ready("a2");
  w.attach_not_ready("a3");
  ASSERT_EQ(lend(w, {"a1"})->agents(0).result(), rv1::LEND_RESULT_LENT);
  auto r = reserve(w, "a1", kCampCall);
  ASSERT_TRUE(r);
  const std::string rid = r->reservation().reservation_id();
  EXPECT_EQ(r->reservation().state(), rv1::RESERVATION_STATE_ASSIGNED);
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_RESERVED);
  // The same call again: the same reservation. Another call: refused (one reservation per agent).
  auto again = reserve(w, "a1", kCampCall);
  ASSERT_TRUE(again);
  EXPECT_TRUE(again->existing());
  EXPECT_EQ(again->reservation().reservation_id(), rid);
  auto other = reserve(w, "a1", kCampCall2);
  ASSERT_FALSE(other);
  EXPECT_EQ(other.error().parity, "agent_not_lent");
  rv1::ReportOutcomeRequest o;
  *o.mutable_owner() = w.owner(kCampCall, 300);
  o.set_reservation_id(rid);
  o.set_outcome(rv1::OUTCOME_CONNECTED);
  ASSERT_TRUE(w.t->report_outcome(o, w.now));
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_ON_CALL);
  // A return during the campaign call waits for its wrap-up.
  EXPECT_EQ(give_back(w, {"a1"})->agents(0).result(), rv1::RETURN_RESULT_PENDING);
  EXPECT_EQ(w.t->campaign_of("a1"), "spring");
  end_call(w, "a1", kCampCall, rid, 300);
  EXPECT_EQ(availability(w).back().state(), rv1::AVAILABILITY_STATE_WRAP_UP);
  EXPECT_GT(availability(w).back().wrapup_ends_at_ms(), w.now);
  w.advance(61'000);  // the wrap-up limit
  EXPECT_TRUE(w.t->campaign_of("a1").empty());
  EXPECT_EQ(availability(w).back().state(), rv1::AVAILABILITY_STATE_RETURNED);
  bool wrap_ended = false;
  for (const auto& [rep, m] : w.rec.ie) wrap_ended = wrap_ended || (m.has_wrap_up_ended() && m.wrap_up_ended().reservation_id() == rid);
  EXPECT_TRUE(wrap_ended);
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

TEST(Lending, CustomerNoAnswerFreesTheAgentForTheNextDial) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);
  w.attach_not_ready("a2");
  w.attach_not_ready("a3");
  ASSERT_EQ(lend(w, {"a1"})->agents(0).result(), rv1::LEND_RESULT_LENT);
  auto r = reserve(w, "a1", kCampCall);
  ASSERT_TRUE(r);
  rv1::ReportOutcomeRequest o;
  *o.mutable_owner() = w.owner(kCampCall, 300);
  o.set_reservation_id(r->reservation().reservation_id());
  o.set_outcome(rv1::OUTCOME_CUSTOMER_NO_ANSWER);
  ASSERT_TRUE(w.t->report_outcome(o, w.now));
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_READY);
  EXPECT_EQ(availability(w).back().state(), rv1::AVAILABILITY_STATE_READY);
  EXPECT_TRUE(reserve(w, "a1", kCampCall2));
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

TEST(Lending, TenantIsolationAndTheLeadersFence) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);
  w.ready("a2", T0 - 20'000);
  w.attach_not_ready("a3");
  // Another tenant's agent id is simply unknown here; a campaign owner of another tenant is refused.
  auto r = lend(w, {"globex-agent", "a1"});
  ASSERT_TRUE(r);
  EXPECT_EQ(r->agents(0).result(), rv1::LEND_RESULT_UNKNOWN_AGENT);
  EXPECT_EQ(r->agents(1).result(), rv1::LEND_RESULT_LENT);
  rv1::LendAgentsRequest f;
  *f.mutable_owner() = camp(5, "spring", "globex");
  f.add_agent_ids("a2");
  auto fr = w.t->lend_agents(f, w.now);
  ASSERT_FALSE(fr);
  EXPECT_EQ(fr.error().parity, "tenant_mismatch");
  EXPECT_TRUE(w.t->campaign_of("a2").empty());
  // A new leader (fence 9) takes over; the old one (5) is stale; another campaign can't take a1.
  auto take = lend(w, {}, 9);
  ASSERT_TRUE(take);
  ASSERT_EQ(take->lent_size(), 1);
  EXPECT_EQ(take->lent(0).owner_epoch(), 9u);
  auto stale = give_back(w, {"a1"}, 5);
  ASSERT_FALSE(stale);
  EXPECT_EQ(stale.error().parity, "stale_owner");
  EXPECT_EQ(lend(w, {"a1"}, 1, "autumn")->agents(0).result(), rv1::LEND_RESULT_OTHER_CAMPAIGN);
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

TEST(Lending, LogoutEndsTheLendingAndLoginIsInbound) {
  World w(cfg());
  w.ready("a1", T0 - 30'000);
  w.attach_not_ready("a2");
  w.attach_not_ready("a3");
  ASSERT_EQ(lend(w, {"a1"})->agents(0).result(), rv1::LEND_RESULT_LENT);
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_LOGGED_OUT));
  EXPECT_TRUE(w.t->campaign_of("a1").empty());
  EXPECT_FALSE(w.t->active_of("a1").empty()) << "inbound queues back for the next login";
  EXPECT_EQ(availability(w).back().state(), rv1::AVAILABILITY_STATE_RETURNED);
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

TEST(Lending, CountPicksIdleSkilledAgentsAndARepeatLendsNothingMore) {
  TenantConfig c = cfg();
  c.agents = {agent("a1", {"sales"}, {{"out", 2}}), agent("a2", {"sales"}, {{"out", 1}}),
              agent("a3", {"support", "sales"}, {{"out", 1}})};
  World w(c);
  w.ready("a1", T0 - 30'000);
  w.ready("a2", T0 - 20'000);
  w.ready("a3", T0 - 10'000);
  rv1::LendAgentsRequest r;
  *r.mutable_owner() = camp();
  r.set_count(3);
  auto* q = r.add_requirements();
  q->set_skill_id("out");
  q->set_min_level(1);
  r.set_request_id("lend-1");
  auto a = w.t->lend_agents(r, w.now);
  ASSERT_TRUE(a);
  ASSERT_EQ(a->agents_size(), 2) << "a3 covers support alone: never picked";
  EXPECT_EQ(a->agents(0).agent_id(), "a1");
  EXPECT_EQ(a->agents(1).agent_id(), "a2");
  auto again = w.t->lend_agents(r, w.now);
  ASSERT_TRUE(again);
  EXPECT_TRUE(again->existing());
  EXPECT_EQ(again->agents_size(), 2);
  EXPECT_TRUE(w.t->campaign_of("a3").empty());
  r.set_count(1);  // the same request_id for another request
  auto conflict = w.t->lend_agents(r, w.now);
  ASSERT_FALSE(conflict);
  EXPECT_EQ(conflict.error().parity, "invalid_request");
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

// Lending survives a takeover: the mode, a pending switch and the leader's fence are in re.state;
// the restored tenant applies the pending switch at the wrap-up's end and refuses the stale leader.
TEST(Lending, ModeAndPendingSwitchSurviveATakeover) {
  World w(cfg());
  w.t->set_tracking(true);
  w.t->set_paranoid(true);
  std::map<std::string, std::string> kv;
  auto drain = [&] {
    std::vector<StateRec> recs;
    w.t->collect(&recs, w.now);
    for (auto& r : recs) {
      if (r.value) {
        kv[r.key] = *r.value;
      } else {
        kv.erase(r.key);
      }
    }
    ASSERT_TRUE(w.t->unmarked().empty()) << w.t->unmarked().front();
  };
  w.ready("a1", T0 - 30'000);
  w.ready("a2", T0 - 20'000);
  w.attach_not_ready("a3");
  ASSERT_TRUE(w.enqueue(kIx1, "sales"));
  const std::string rid = connect(w, "a1", kIx1);
  ASSERT_EQ(lend(w, {"a1"}, 7)->agents(0).result(), rv1::LEND_RESULT_PENDING);
  ASSERT_EQ(lend(w, {"a2"}, 7)->agents(0).result(), rv1::LEND_RESULT_LENT);
  drain();
  const std::string before = w.t->durable_dump();
  ASSERT_NE(before.find("acme/camp/spring"), std::string::npos);
  // The takeover: an empty memory rebuilt from the records alone.
  w.heap.q = {};
  auto fresh = std::make_unique<Tenant>(cfg(), &w.epochs, &w.rec, &w.heap, &w.counters);
  fresh->set_tracking(true);
  fresh->set_paranoid(true);
  fresh->restore({kv.begin(), kv.end()}, w.now, /*restamp=*/false);
  EXPECT_EQ(fresh->durable_dump(), before);
  w.t = std::move(fresh);
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
  EXPECT_EQ(w.t->campaign_of("a2"), "spring");
  EXPECT_TRUE(w.t->mode_pending_of("a1"));
  // Still no inbound offer to a2 after the restore.
  ASSERT_TRUE(w.enqueue(kIx2, "sales"));
  EXPECT_TRUE(w.t->live_offer_of("a2").empty());
  // The pending switch applies after the wrap-up, on the new leader.
  end_call(w, "a1", kIx1, rid);
  end_wrap_up(w, "a1", rid);
  EXPECT_EQ(w.t->campaign_of("a1"), "spring");
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  // The fence survived: the stale leader is refused.
  auto stale = give_back(w, {"a2"}, 6);
  ASSERT_FALSE(stale);
  EXPECT_EQ(stale.error().parity, "stale_owner");
  drain();
  EXPECT_TRUE(w.broken().empty()) << w.broken().front();
}

}  // namespace
}  // namespace pcr
