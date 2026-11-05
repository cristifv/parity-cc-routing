// Unit and property tests of one tenant's routing (PLAN §4.2, §7): dispatch order, bounded waits,
// the lifecycle rows, and the invariants over random operation sequences (seeds logged).
#include <gtest/gtest.h>

#include <cstdio>
#include <queue>
#include <random>

#include "src/core/tenant.h"
#include "tests/unit/world.h"

namespace pcr {
namespace {

using namespace testing_world;

TenantConfig two_queue_cfg() {
  TenantConfig c;
  c.id = "acme";
  c.queues = {queue("sales"), queue("support", {{"de", 0}})};
  c.agents = {agent("a1", {"sales"}), agent("a2", {"sales", "support"}, {{"de", 3}}),
              agent("a3", {"support"}, {{"de", 1}}), agent("a4", {}, {{"de", 5}})};
  return c;
}

TEST(Dispatch, LongestIdleGetsTheOffer) {
  World w(two_queue_cfg());
  w.ready("a1", T0 - 5000);
  w.ready("a2", T0 - 20000);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  EXPECT_FALSE(w.t->live_offer_of("a2").empty());
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  EXPECT_TRUE(w.broken().empty());
}

TEST(Dispatch, ExpiredOfferGoesToTheNextAgentKeepingOrdering) {
  World w(two_queue_cfg());
  w.ready("a1", T0 - 5000);
  w.ready("a2", T0 - 20000);
  auto e = w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales");
  ASSERT_TRUE(e);
  w.advance(15000);
  EXPECT_EQ(w.t->state_of("a2"), av1::AGENT_STATE_NOT_READY);
  EXPECT_FALSE(w.t->live_offer_of("a1").empty());
  auto acc = w.answer("a1", true);
  ASSERT_TRUE(acc);
  ASSERT_FALSE(w.rec.ie.empty());
  EXPECT_EQ(w.rec.ie.back().second.assigned().agent_id(), "a1");
  EXPECT_TRUE(w.broken().empty());
}

TEST(Dispatch, RejectSkipIsTemporaryAndNeverStrandsAOneAgentQueue) {
  World w(two_queue_cfg());
  w.ready("a1", T0 - 5000);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  auto r = w.answer("a1", false);
  ASSERT_TRUE(r);
  // No other eligible agent is READY: the skip doesn't hold, the caller isn't stranded.
  EXPECT_FALSE(w.t->live_offer_of("a1").empty());
  EXPECT_TRUE(w.broken().empty());
}

TEST(Dispatch, RejectSkipsWhileAnotherAgentIsReady) {
  World w(two_queue_cfg());
  w.ready("a1", T0 - 30000);
  w.ready("a2", T0 - 5000);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  ASSERT_FALSE(w.t->live_offer_of("a1").empty());
  ASSERT_TRUE(w.answer("a1", false));
  EXPECT_FALSE(w.t->live_offer_of("a2").empty());
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  EXPECT_TRUE(w.broken().empty());
}

TEST(Dispatch, EscalatedEntryGoesToAnEligibleNonMember) {
  World w(two_queue_cfg());
  w.ready("a4", T0 - 5000);  // has skill de, member of nothing
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "support"));
  EXPECT_TRUE(w.t->live_offer_of("a4").empty());
  w.advance(60000);  // escalate_after = 3 tau = 60 s
  EXPECT_FALSE(w.t->live_offer_of("a4").empty());
  EXPECT_TRUE(w.broken().empty());
}

TEST(Dispatch, EscalatedEntryBeatsHigherPriority) {
  World w(two_queue_cfg());
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819201", "sales", 10));
  w.advance(61000);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819202", "sales", 100));
  w.ready("a1", w.now);
  w.t->sweep(w.now);
  const std::string offer = w.t->live_offer_of("a1");
  ASSERT_FALSE(offer.empty());
  const auto& last = w.rec.gw.back().second;
  ASSERT_TRUE(last.has_offer());
  EXPECT_EQ(last.offer().interaction_id(), "0192f6a1-7c3e-7b21-9a4d-5e6f70819201");
}

TEST(BoundedWait, MaxWaitDisconnectOverflowsAndKeepsTheEntry) {
  TenantConfig c = two_queue_cfg();
  c.queues[0].max_wait_ms = 30000;
  World w(c);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  w.advance(30000);
  ASSERT_FALSE(w.rec.ie.empty());
  const auto& m = w.rec.ie.back().second;
  ASSERT_TRUE(m.has_overflow());
  EXPECT_EQ(m.overflow().action(), rv1::OVERFLOW_ACTION_DISCONNECT);
  EXPECT_EQ(m.delivery_seq(), 1u);
  EXPECT_EQ(w.t->waiting_entries(), 0u);
  EXPECT_EQ(w.t->entry_ids().size(), 1u);  // OVERFLOWED, kept until Dequeue(OVERFLOW)
}

TEST(BoundedWait, MaxWaitQueueMovesTheEntryWithItsTimes) {
  TenantConfig c = two_queue_cfg();
  c.queues[0].max_wait_ms = 30000;
  c.queues[0].max_wait_action = MaxWaitAction::kQueue;
  c.queues[0].max_wait_queue = "support";
  World w(c);
  w.ready("a3", T0 - 1);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  w.advance(30000);
  EXPECT_FALSE(w.t->live_offer_of("a3").empty());
  EXPECT_TRUE(w.broken().empty());
}

TEST(Lifecycle, DequeueWhileAssignedRecordsCallerGoneL17) {
  World w(two_queue_cfg());
  w.ready("a1", T0 - 5000);
  auto e = w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales");
  ASSERT_TRUE(e);
  ASSERT_TRUE(w.answer("a1", true));
  rv1::DequeueRequest d;
  *d.mutable_owner() = w.owner("0192f6a1-7c3e-7b21-9a4d-5e6f70819203");
  d.set_entry_id(e->entry().entry_id());
  d.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
  auto dr = w.t->dequeue(d, w.now);
  ASSERT_TRUE(dr);
  EXPECT_FALSE(dr->already_gone());
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_READY);
  rv1::ReportOutcomeRequest ro;
  *ro.mutable_owner() = w.owner("0192f6a1-7c3e-7b21-9a4d-5e6f70819203");
  ro.set_reservation_id(w.rec.ie.back().second.assigned().reservation_id());
  ro.set_outcome(rv1::OUTCOME_CALLER_GONE);
  auto out = w.t->report_outcome(ro, w.now);
  ASSERT_TRUE(out);
  EXPECT_TRUE(out->existing());
  EXPECT_EQ(out->recorded(), rv1::OUTCOME_CALLER_GONE);
  auto again = w.t->dequeue(d, w.now);
  ASSERT_TRUE(again);
  EXPECT_TRUE(again->already_gone());
}

TEST(Lifecycle, StaleOwnerIsFenced) {
  World w(two_queue_cfg());
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  rv1::DequeueRequest d;
  *d.mutable_owner() = w.owner("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", 99);
  d.set_entry_id(w.t->entry_ids().front());
  d.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
  auto r = w.t->dequeue(d, w.now);
  ASSERT_FALSE(r);
  EXPECT_EQ(r.error().parity, "stale_owner");
}

TEST(Lifecycle, AssignmentDeadlineExpiresL7AndPausesWithoutAStream) {
  World w(two_queue_cfg());
  w.ready("a1", T0 - 5000);
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  ASSERT_TRUE(w.answer("a1", true));
  w.rec.live = false;
  w.advance(32000);  // deadline + 2 s, but no stream: paused
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_RESERVED);
  w.advance(10000);  // the 10 s adoption budget is over
  EXPECT_NE(w.t->state_of("a1"), av1::AGENT_STATE_RESERVED);
  bool revoked = false;
  for (auto& [r, m] : w.rec.ie) revoked |= m.has_revoke();
  EXPECT_TRUE(revoked);
  EXPECT_TRUE(w.broken().empty());
}

// CR-053: a stream opened, its replay not yet run on this writer: a newer message must not
// overtake the replay. It waits (unacknowledged) and the replay sends everything in order.
TEST(Delivery, ReplayIsNeverOvertakenByANewerMessage) {
  TenantConfig c = two_queue_cfg();
  c.queues[0].max_wait_ms = 5'000;
  c.queues[0].max_wait_action = MaxWaitAction::kQueue;
  c.queues[0].max_wait_queue = "support";
  World w(c);
  w.rec.hub = true;
  w.rec.live_gen["ie-1"] = 1;
  w.t->on_ie_stream_open("ie-1", 1, w.now);
  w.ready("a1", T0 - 5000);
  const std::string ix = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
  ASSERT_TRUE(w.enqueue(ix, "sales"));
  ASSERT_TRUE(w.answer("a1", true));  // Assigned, delivery_seq 1, on stream 1
  ASSERT_EQ(w.rec.ie.size(), 1u);
  // The stream drops and IE reconnects: the hub registers stream 2; this writer hasn't run its
  // replay yet. A reservation outcome sends the entry back; its max wait moves it (Overflow, 2).
  w.rec.live_gen["ie-1"] = 2;
  rv1::ReportOutcomeRequest ro;
  *ro.mutable_owner() = w.owner(ix);
  ro.set_reservation_id(w.rec.ie[0].second.assigned().reservation_id());
  ro.set_outcome(rv1::OUTCOME_AGENT_LEG_FAILED);
  ASSERT_TRUE(w.t->report_outcome(ro, w.now));
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_NOT_READY, "break"));
  w.advance(6'000);
  EXPECT_EQ(w.rec.ie.size(), 1u) << "a message overtook the replay";
  EXPECT_GT(w.rec.held, 0u);
  w.t->on_ie_stream_open("ie-1", 2, w.now);  // the replay
  std::vector<uint64_t> seqs;
  for (size_t i = 1; i < w.rec.ie.size(); ++i) {
    if (w.rec.ie[i].second.delivery_seq() != 0) seqs.push_back(w.rec.ie[i].second.delivery_seq());
  }
  EXPECT_EQ(seqs, (std::vector<uint64_t>{1, 2}));
  // An older stream's replay that runs late changes nothing.
  const size_t n = w.rec.ie.size();
  w.t->on_ie_stream_open("ie-1", 1, w.now);
  EXPECT_EQ(w.rec.ie.size(), n);
}

// CR-054 (contract ruling): a replay carries the CURRENT owner fence; RE re-stamps at every
// epoch raise, also when the new owner's replica has no live stream yet.
TEST(Delivery, EpochRaiseRestampsAndReplaysUnacked) {
  TenantConfig c = two_queue_cfg();
  c.queues[0].max_wait_ms = 5'000;
  World w(c);
  w.rec.hub = true;
  w.rec.live_gen["ie-1"] = 1;
  w.t->on_ie_stream_open("ie-1", 1, w.now);
  const std::string ix = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
  ASSERT_TRUE(w.enqueue(ix, "sales"));
  w.advance(6'000);  // Overflow{DISCONNECT}, delivery_seq 1, owner_epoch 100: IE dies unacked
  ASSERT_EQ(w.rec.ie.back().second.overflow().target().owner_epoch(), 100u);
  // The adopter (ie-2, epoch 101) calls before its stream is up: nothing is sent yet ...
  rv1::ReattachRequest ra;
  *ra.mutable_owner() = w.owner(ix, 101);
  ra.mutable_owner()->set_replica_id("ie-2");
  auto out = w.t->reattach(ra, w.now);
  ASSERT_TRUE(out);
  EXPECT_EQ(out->entry().state(), rv1::ENTRY_STATE_OVERFLOWED);
  EXPECT_EQ(out->entry().position(), 0u);
  const size_t n = w.rec.ie.size();
  // ... its stream opens: the replay carries the new fence, the same delivery_seq.
  w.rec.live_gen["ie-2"] = 2;
  w.t->on_ie_stream_open("ie-2", 2, w.now);
  ASSERT_EQ(w.rec.ie.size(), n + 1);
  EXPECT_EQ(w.rec.ie.back().first, "ie-2");
  EXPECT_EQ(w.rec.ie.back().second.delivery_seq(), 1u);
  EXPECT_EQ(w.rec.ie.back().second.overflow().target().owner_epoch(), 101u);
  // A raise by any RPC (not only Reattach) re-stamps too: redeliveries carry epoch 102.
  rv1::DequeueRequest d;
  *d.mutable_owner() = w.owner(ix, 102);
  d.mutable_owner()->set_replica_id("ie-2");
  d.set_entry_id(out->entry().entry_id());
  d.set_reason(rv1::DEQUEUE_REASON_OVERFLOW);
  w.advance(2'500);
  EXPECT_EQ(w.rec.ie.back().second.overflow().target().owner_epoch(), 101u);
  auto dq = w.t->dequeue(d, w.now);
  ASSERT_TRUE(dq);
  EXPECT_EQ(w.rec.ie.back().second.overflow().target().owner_epoch(), 102u);
}

// CR-062: an entry back in WAITING (L4 here) or moved by an L14 overflow learns its position.
TEST(Positions, ReWaitAndMoveSendAPositionUpdate) {
  TenantConfig c = two_queue_cfg();
  c.queues[0].max_wait_ms = 60'000;
  c.queues[0].max_wait_action = MaxWaitAction::kQueue;
  c.queues[0].max_wait_queue = "support";
  World w(c);
  w.ready("a1", T0 - 5000);
  const std::string ix = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
  auto e = w.enqueue(ix, "sales");
  ASSERT_TRUE(e);
  ASSERT_TRUE(w.answer("a1", true));
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_NOT_READY, "break"));  // pending: the agent stays reserved
  auto positions = [&] {
    int n = 0;
    for (auto& [r, m] : w.rec.ie) n += m.has_position() && m.position().entry_id() == e->entry().entry_id();
    return n;
  };
  EXPECT_EQ(positions(), 0);
  rv1::ReportOutcomeRequest ro;
  *ro.mutable_owner() = w.owner(ix);
  ro.set_reservation_id(w.rec.ie.back().second.assigned().reservation_id());
  ro.set_outcome(rv1::OUTCOME_AGENT_LEG_FAILED);
  ASSERT_TRUE(w.t->report_outcome(ro, w.now));  // L4: back to WAITING, nobody ready
  ASSERT_EQ(positions(), 1);
  EXPECT_EQ(w.rec.ie.back().second.position().position(), 1u);
  const size_t before = w.rec.ie.size();
  w.advance(61'000);  // L14: moved to support; its place there follows the Overflow
  ASSERT_EQ(positions(), 2);
  size_t overflow_at = 0, position_at = 0;
  for (size_t i = before; i < w.rec.ie.size(); ++i) {  // first sightings (redeliveries follow)
    const auto& m = w.rec.ie[i].second;
    if (m.has_overflow() && !overflow_at) overflow_at = i;
    if (m.has_position() && !position_at) {
      position_at = i;
      EXPECT_EQ(m.position().position(), 1u);
    }
  }
  EXPECT_GT(overflow_at, 0u);
  EXPECT_GT(position_at, overflow_at);
}

// CR-061: no offers while draining, from the sweep either.
TEST(Drain, TheSweepMakesNoOffers) {
  World w(two_queue_cfg());
  ASSERT_TRUE(w.enqueue("0192f6a1-7c3e-7b21-9a4d-5e6f70819203", "sales"));
  w.t->set_draining(true);
  w.ready("a1", T0 - 5000);  // seeded straight into the index: only the sweep could find it
  w.advance(3'000);
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  EXPECT_EQ(w.counters.sweep_matches.load(), 0u);
}

// CR-083: an IE owner that is gone loses its calls in RE; agents are freed; nothing lingers.
struct OwnerLoss : ::testing::Test {
  static TenantConfig cfg() {
    TenantConfig c = two_queue_cfg();
    c.owner_loss_ms = 60'000;
    return c;
  }
  World w{cfg()};
  const std::string ix = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
  void tick_through(int64_t ms) {  // advance() runs the 1 s periodic work; reap it too
    const int64_t end = w.now + ms;
    while (w.now < end) {
      w.advance(1000);
      w.t->reap_orphans(w.now);
    }
  }
  rv1::ReportOutcomeRequest outcome(rv1::Outcome o, uint64_t acked) {
    rv1::ReportOutcomeRequest r;
    *r.mutable_owner() = w.owner(ix);
    r.mutable_owner()->set_acked_delivery_seq(acked);
    r.set_reservation_id(w.rec.ie.back().second.assigned().reservation_id());
    r.set_outcome(o);
    return r;
  }
};

TEST_F(OwnerLoss, NoStreamAndNoRpcEndsTheEntry) {
  ASSERT_TRUE(w.enqueue(ix, "sales"));
  w.rec.live = false;  // the replica's process is gone
  tick_through(58'000);
  EXPECT_EQ(w.t->held_interactions().size(), 1u);
  tick_through(4'000);
  EXPECT_TRUE(w.t->held_interactions().empty());
  EXPECT_EQ(w.counters.owner_lost.load(), 1u);
  EXPECT_TRUE(w.broken().empty());
}

TEST_F(OwnerLoss, RpcsKeepAnOwnerWithoutAStreamAlive) {
  ASSERT_TRUE(w.enqueue(ix, "sales"));
  w.rec.live = false;  // the stream is down, the process isn't: it keeps calling
  for (int i = 0; i < 6; ++i) {
    tick_through(20'000);
    rv1::DequeueRequest d;  // any RPC of that replica counts; an unknown entry is fine here
    *d.mutable_owner() = w.owner(ix);
    d.set_entry_id("0192f6a1-9000-7aaa-8bbb-0000000000aa");
    d.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
    (void)w.t->dequeue(d, w.now);
  }
  EXPECT_EQ(w.counters.owner_lost.load(), 0u);
  EXPECT_EQ(w.t->held_interactions().size(), 1u);
}

TEST_F(OwnerLoss, UnackedOnALiveStreamFreesTheAgent) {
  // An IE restarted under the same replica id: the stream is live, nobody acks the old call.
  w.ready("a1", T0 - 5000);
  ASSERT_TRUE(w.enqueue(ix, "sales"));
  ASSERT_TRUE(w.answer("a1", true));  // Assigned (delivery_seq 1) never acknowledged
  tick_through(61'000);
  EXPECT_EQ(w.counters.owner_lost.load(), 1u);
  EXPECT_TRUE(w.t->held_interactions().empty());
  EXPECT_NE(w.t->state_of("a1"), av1::AGENT_STATE_RESERVED);
  EXPECT_NE(w.t->state_of("a1"), av1::AGENT_STATE_OFFERED);  // no ghost offer left behind
  EXPECT_TRUE(w.t->live_reservations().empty());
  EXPECT_TRUE(w.broken().empty());
}

TEST_F(OwnerLoss, AConnectedCallGoesToWrapUpAndAnAdopterStartsAfresh) {
  w.ready("a1", T0 - 5000);
  ASSERT_TRUE(w.enqueue(ix, "sales"));
  ASSERT_TRUE(w.answer("a1", true));
  ASSERT_TRUE(w.t->report_outcome(outcome(rv1::OUTCOME_CONNECTED, 1), w.now));  // acks Assigned
  w.rec.live = false;
  tick_through(61'000);
  EXPECT_EQ(w.counters.owner_lost.load(), 1u);
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_WRAP_UP);
  const size_t ie_before = w.rec.ie.size();
  tick_through(61'000);  // the wrap-up limit: READY, and no WrapUpEnded to a lost owner
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_READY);
  for (size_t i = ie_before; i < w.rec.ie.size(); ++i) EXPECT_FALSE(w.rec.ie[i].second.has_wrap_up_ended());
  // An adopter (a higher epoch) finds nothing and its Enqueue makes a new entry.
  w.rec.live = true;
  rv1::ReattachRequest ra;
  *ra.mutable_owner() = w.owner(ix, 101);
  auto re = w.t->reattach(ra, w.now);
  ASSERT_TRUE(re);
  EXPECT_FALSE(re->has_entry());
  rv1::EnqueueRequest r;
  *r.mutable_owner() = w.owner(ix, 101);
  r.set_enqueue_seq(1);
  r.mutable_target()->set_queue_id("sales");
  r.set_first_queued_at_ms(w.now);
  r.set_ordering_at_ms(w.now);
  auto e = w.t->enqueue(r, w.now);
  ASSERT_TRUE(e);
  EXPECT_FALSE(e->existing());
}

// ---- property test: random operations, invariants after every step (PLAN §7) ----

TEST(Property, InvariantsHoldOverRandomOperations) {
  const char* env = std::getenv("PCR_PROPERTY_RUNS");
  const int runs = env ? std::atoi(env) : 300;
  const uint64_t base = std::random_device{}();
  for (int run = 0; run < runs; ++run) {
    const uint64_t seed = base + static_cast<uint64_t>(run);
    std::mt19937_64 rng(seed);
    TenantConfig c = two_queue_cfg();
    c.queues[0].max_wait_ms = 90'000;
    c.queues[1].max_wait_ms = 120'000;
    c.queues[1].max_wait_action = MaxWaitAction::kQueue;
    c.queues[1].max_wait_queue = "sales";
    c.reject_skip_ms = 30'000;
    World w(c);
    for (const char* a : {"a1", "a2", "a3", "a4"}) w.attach_not_ready(a);
    int calls = 0;
    std::vector<std::string> ixs;
    for (int step = 0; step < 400; ++step) {
      const int op = static_cast<int>(rng() % 12);
      const std::string a = std::string("a") + std::to_string(1 + rng() % 4);
      switch (op) {
        case 0:
        case 1: {
          char id[40];
          std::snprintf(id, sizeof id, "0192f6a1-7c3e-7b21-9a4d-%012d", ++calls);
          ixs.push_back(id);
          std::vector<SkillReq> reqs;
          if (rng() % 5 == 0) reqs.push_back({"de", static_cast<uint32_t>(1 + rng() % 5)});
          (void)w.enqueue(id, rng() % 2 ? "sales" : "support", static_cast<uint32_t>(rng() % 3) * 40, reqs);
          break;
        }
        case 2:
          (void)w.set(a, av1::TARGET_STATE_READY);
          break;
        case 3:
          (void)w.set(a, rng() % 2 ? av1::TARGET_STATE_NOT_READY : av1::TARGET_STATE_LOGGED_OUT, "break");
          break;
        case 4:
        case 5:
          if (!w.t->live_offer_of(a).empty()) (void)w.answer(a, rng() % 3 != 0);
          break;
        case 6: {
          // IE reports an outcome for a live reservation.
          auto live = w.t->live_reservations();
          if (live.empty()) break;
          const std::string rid = live[rng() % live.size()];
          for (auto it = w.rec.ie.rbegin(); it != w.rec.ie.rend(); ++it) {
            if (!it->second.has_assigned() || it->second.assigned().reservation_id() != rid) continue;
            rv1::ReportOutcomeRequest r;
            *r.mutable_owner() = w.owner(it->second.assigned().target().interaction_id());
            r.set_reservation_id(rid);
            static const rv1::Outcome outs[] = {rv1::OUTCOME_CONNECTED, rv1::OUTCOME_CONNECTED,
                                                rv1::OUTCOME_AGENT_NO_ANSWER, rv1::OUTCOME_AGENT_LEG_FAILED,
                                                rv1::OUTCOME_CALLER_GONE, rv1::OUTCOME_ALREADY_ASSIGNED};
            r.set_outcome(outs[rng() % 6]);
            (void)w.t->report_outcome(r, w.now);
            break;
          }
          break;
        }
        case 7: {
          // CallEnded for a connected reservation, then sometimes EndWrapUp.
          for (auto it = w.rec.ie.rbegin(); it != w.rec.ie.rend(); ++it) {
            if (!it->second.has_assigned()) continue;
            rv1::CallEndedRequest r;
            *r.mutable_owner() = w.owner(it->second.assigned().target().interaction_id());
            r.set_agent_id(it->second.assigned().agent_id());
            r.set_reservation_id(it->second.assigned().reservation_id());
            r.set_ended_at_ms(w.now);
            (void)w.t->call_ended(r, w.now);
            if (rng() % 2) {
              av1::EndWrapUpRequest e;
              *e.mutable_session() = w.ref(r.agent_id());
              e.set_command_id(w.next_cmd());
              e.set_reservation_id(r.reservation_id());
              (void)w.t->end_wrap_up(e, w.now);
            }
            break;
          }
          break;
        }
        case 8: {
          if (ixs.empty()) break;
          const std::string ix = ixs[rng() % ixs.size()];
          for (const auto& eid : w.t->entry_ids()) {
            rv1::DequeueRequest d;
            *d.mutable_owner() = w.owner(ix);
            d.set_entry_id(eid);
            d.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
            (void)w.t->dequeue(d, w.now);
          }
          break;
        }
        case 9: {
          av1::DetachAgentRequest d;
          *d.mutable_session() = w.ref(a);
          d.set_reason(av1::DETACH_REASON_SOCKET_CLOSED);
          (void)w.t->detach(d, w.now);
          break;
        }
        case 10: {
          av1::AttachAgentRequest r;
          r.set_tenant_id("acme");
          r.set_agent_id(a);
          r.set_replica_id("gw-1");
          r.set_session_id(w.sess[a].session_id);
          r.set_agent_contact("sip:x@acme.agents.example");
          auto res = w.t->attach(r, w.now);
          if (res) w.sess[a].attach_generation = res->attach_generation();
          break;
        }
        default:
          w.advance(static_cast<int64_t>(rng() % 20'000));
          break;
      }
      auto v = w.broken();
      if (w.counters.sweep_matches.load() != 0) v.push_back("the 1 s sweep found a missed dispatch");
      if (!v.empty()) {
        ADD_FAILURE() << "seed " << seed << " step " << step << " op " << op << ": " << v.front();
        return;
      }
    }
  }
  std::printf("property: %d runs from seed %llu\n", runs, static_cast<unsigned long long>(base));
}

}  // namespace
}  // namespace pcr
