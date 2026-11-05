// Phase 3 (PLAN §8 row 3), RE side, without Postgres: direct offers, consults, PLACEMENT_HEAD, and
// the callback lifecycle against an in-memory store whose writes complete when the test says (as
// the real store's completions arrive later on the writer). The Postgres store itself, a restart
// and a duplicate RunCallback across it: tests/store (scripts/store-it.sh).
#include <gtest/gtest.h>

#include "src/core/tenant.h"
#include "tests/unit/world.h"

namespace pcr {
namespace {

using namespace testing_world;

constexpr const char* IX1 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819201";
constexpr const char* IX2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819202";
constexpr const char* NUMBER = "+447700900123";

struct FakeStore final : Durable {
  bool refuse = false;
  std::map<std::string, CallbackRecord> rows;
  std::vector<std::function<void(Tenant*, int64_t)>> pending;
  bool put_callback(const CallbackRecord& r, Done done) override {
    if (refuse) return false;
    pending.push_back([this, r, done](Tenant* t, int64_t now) {
      rows[r.callback_id] = r;
      done(t, true, now);
    });
    return true;
  }
  bool put_task(const TaskRecord& r, TaskDone done) override {
    if (refuse) return false;
    pending.push_back([r, done](Tenant* t, int64_t now) { done(t, true, r.task_id, false, now); });
    return true;
  }
  void put_buckets(std::vector<BucketRow>) override {}
  void flush(Tenant* t, int64_t now) {
    auto p = std::move(pending);
    pending.clear();
    for (auto& f : p) f(t, now);
  }
};

TenantConfig cfg() {
  TenantConfig c;
  c.id = "acme";
  QueueConfig sales = queue("sales");
  sales.max_wait_ms = 30'000;
  sales.max_wait_action = MaxWaitAction::kCallbackOffer;
  c.queues = {sales, queue("support")};
  c.agents = {agent("a1", {"sales", "support"}), agent("a2", {"sales", "support"}), agent("a3", {"support"})};
  c.callback_deadline_ms = 60'000;
  c.callback_retry_after_ms = 600'000;
  return c;
}

struct P3 : World {
  FakeStore store;
  explicit P3(TenantConfig c) : World(std::move(c)) {
    t->set_durable(&store);
    rec.replicas = {"ie-1", "ie-2"};
  }
  std::string entry_of(const std::string& ix) {
    for (const auto& e : t->entries_snapshot()) {
      if (e.interaction == ix) return e.entry_id;
    }
    return {};
  }
  std::vector<rv1::RunCallback> runs(const std::string& replica) {
    std::vector<rv1::RunCallback> out;
    for (const auto& [r, m] : rec.ie) {
      if (r == replica && m.has_run_callback()) out.push_back(m.run_callback());
    }
    return out;
  }
  Result<rv1::ConvertToCallbackResponse> convert(const std::string& ix, bool flush_now) {
    rv1::ConvertToCallbackRequest r;
    *r.mutable_owner() = owner(ix);
    r.set_entry_id(entry_of(ix));
    r.set_callback_number(NUMBER);
    r.set_keep_position(true);
    std::optional<Result<rv1::ConvertToCallbackResponse>> got;
    t->convert_to_callback(r, now, [&](Result<rv1::ConvertToCallbackResponse> x) { got = std::move(x); });
    if (flush_now) store.flush(t.get(), now);
    if (!got) return err(Code::kInternal, "pending");
    return *got;
  }
};

TEST(Callbacks, OfferConvertDispatchRedeliverClaimAndRetry) {
  P3 w(cfg());
  w.attach_not_ready("a1");
  w.attach_not_ready("a2");
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  const std::string entry1 = w.entry_of(IX1);
  w.advance(30'000);  // max wait: CALLBACK_OFFER, the entry keeps its place (L15)
  ASSERT_TRUE(w.rec.ie.back().second.has_overflow());
  EXPECT_EQ(w.rec.ie.back().second.overflow().action(), rv1::OVERFLOW_ACTION_CALLBACK_OFFER);
  ASSERT_FALSE(w.entry_of(IX1).empty());
  // The conversion is durable before anything happens: while it is written, nobody is offered it.
  rv1::ConvertToCallbackRequest r;
  *r.mutable_owner() = w.owner(IX1);
  r.set_entry_id(w.entry_of(IX1));
  r.set_callback_number(NUMBER);
  r.set_keep_position(true);
  std::optional<Result<rv1::ConvertToCallbackResponse>> got;
  w.t->convert_to_callback(r, w.now, [&](Result<rv1::ConvertToCallbackResponse> x) { got = std::move(x); });
  EXPECT_FALSE(got);
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_READY));
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  EXPECT_TRUE(w.broken().empty());
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_NOT_READY, "break"));
  w.store.flush(w.t.get(), w.now);
  ASSERT_TRUE(got && *got);
  const std::string cb = (*got)->callback_id();
  EXPECT_FALSE((*got)->existing());
  EXPECT_EQ(w.store.rows.at(cb).state, "scheduled");
  EXPECT_EQ(w.store.rows.at(cb).number, NUMBER);
  rv1::ConvertToCallbackRequest rr = r;  // a repeat: the same callback
  rr.set_entry_id(entry1);
  std::optional<Result<rv1::ConvertToCallbackResponse>> again_got;
  w.t->convert_to_callback(rr, w.now, [&](Result<rv1::ConvertToCallbackResponse> x) { again_got = std::move(x); });
  ASSERT_TRUE(again_got);
  auto again = *again_got;
  ASSERT_TRUE(again);
  EXPECT_TRUE(again->existing());
  EXPECT_EQ(again->callback_id(), cb);
  // An agent accepts the callback: RunCallback only once the attempt is durable.
  ASSERT_TRUE(w.set("a1", av1::TARGET_STATE_READY));
  ASSERT_FALSE(w.t->live_offer_of("a1").empty());
  EXPECT_TRUE(w.rec.gw.back().second.offer().callback());
  ASSERT_TRUE(w.answer("a1", true));
  EXPECT_TRUE(w.runs("ie-1").empty());
  w.store.flush(w.t.get(), w.now);
  EXPECT_EQ(w.store.rows.at(cb).state, "dispatched");
  ASSERT_EQ(w.runs("ie-1").size(), 1u);  // to the replica the caller was on
  const rv1::RunCallback run = w.runs("ie-1")[0];
  EXPECT_EQ(run.callback_id(), cb);
  EXPECT_EQ(run.original_interaction_id(), IX1);
  EXPECT_EQ(run.agent_id(), "a1");
  EXPECT_EQ(run.number(), NUMBER);
  // No ack for 5 s: the same RunCallback (same interaction id) goes to another stream.
  w.advance(5'000);
  ASSERT_GE(w.runs("ie-2").size(), 1u);
  EXPECT_EQ(w.runs("ie-2")[0].interaction_id(), run.interaction_id());
  EXPECT_EQ(w.runs("ie-2")[0].reservation_id(), run.reservation_id());
  // ie-1 claimed it: its ack makes it the owner.
  rv1::InteractionOwner ack;
  ack.set_tenant_id("acme");
  ack.set_interaction_id(run.interaction_id());
  ack.set_owner_epoch(2001);
  ack.set_replica_id("ie-1");
  ack.set_acked_delivery_seq(1);
  EXPECT_TRUE(w.t->ack(ack, w.now));
  EXPECT_EQ(w.t->callback_states().at(cb), "claimed");
  // The customer didn't answer: another attempt later, with a new interaction id.
  rv1::ReportOutcomeRequest out;
  *out.mutable_owner() = ack;
  out.set_reservation_id(run.reservation_id());
  out.set_outcome(rv1::OUTCOME_CUSTOMER_NO_ANSWER);
  ASSERT_TRUE(w.t->report_outcome(out, w.now));
  EXPECT_EQ(w.t->callback_states().at(cb), "scheduled");
  w.store.flush(w.t.get(), w.now);
  EXPECT_NE(w.store.rows.at(cb).attempt_interaction_id, run.interaction_id());
  EXPECT_EQ(w.t->state_of("a1"), av1::AGENT_STATE_READY);
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());  // due in 10 min
  EXPECT_TRUE(w.broken().empty());
}

// Postgres down: new callbacks are refused; the caller is never stranded.
TEST(Callbacks, StoreDownRefusesAndTheCallerStaysQueued) {
  P3 w(cfg());
  w.store.refuse = true;
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  auto r = w.convert(IX1, true);
  ASSERT_FALSE(r);
  EXPECT_EQ(r.error().code, Code::kUnavailable);
  EXPECT_EQ(r.error().parity, "store_unavailable");
  w.ready("a1", w.now);
  w.t->sweep(w.now);
  EXPECT_FALSE(w.t->live_offer_of("a1").empty());
  EXPECT_TRUE(w.broken().empty());
}

// An expired RunCallback (no outcome by deadline + 10 s) is flagged, never redialled by itself.
TEST(Callbacks, AnExpiredDispatchIsFlaggedNotRedialled) {
  P3 w(cfg());
  w.attach_not_ready("a2");
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  auto c = w.convert(IX1, true);
  ASSERT_TRUE(c);
  w.ready("a1", w.now);
  w.t->sweep(w.now);
  ASSERT_TRUE(w.answer("a1", true));
  w.store.flush(w.t.get(), w.now);
  w.advance(75'000);  // deadline 60 s + 10 s
  w.store.flush(w.t.get(), w.now);
  EXPECT_EQ(w.t->callback_states().at(c->callback_id()), "flagged");
  EXPECT_EQ(w.counters.callbacks_flagged.load(), 1u);
  size_t runs = w.runs("ie-1").size() + w.runs("ie-2").size();
  w.advance(600'000);
  EXPECT_EQ(w.runs("ie-1").size() + w.runs("ie-2").size(), runs);  // nothing new
  EXPECT_TRUE(w.broken().empty());
}

// After a restart (RE's memory lost): scheduled callbacks queue again; a recent dispatch goes back
// with the SAME interaction id (IE drops it if it already placed it); a claimed one is flagged.
TEST(Callbacks, RestoreAfterARestart) {
  P3 w(cfg());
  auto row = [&](const std::string& id, const std::string& state, int64_t updated) {
    CallbackRecord c;
    c.tenant = "acme";
    c.callback_id = id;
    c.original_interaction_id = IX1;
    c.entry_id = new_uuid_v7(w.now);
    c.queue_id = "sales";
    c.number = NUMBER;
    c.priority = 50;
    c.first_queued_at = c.ordering_at = w.now - 600'000;
    c.state = state;
    c.attempt_interaction_id = new_uuid_v7(w.now);
    c.due_at = w.now - 1;
    c.created_at = w.now - 600'000;
    c.updated_at = updated;
    return c;
  };
  const auto s = row("0192f6b0-2000-7a00-8b00-000000000001", "scheduled", w.now - 1000);
  const auto d = row("0192f6b0-2000-7a00-8b00-000000000002", "dispatched", w.now - 60'000);
  const auto old = row("0192f6b0-2000-7a00-8b00-000000000003", "dispatched", w.now - 7'200'000);
  const auto cl = row("0192f6b0-2000-7a00-8b00-000000000004", "claimed", w.now - 60'000);
  w.t->restore_callbacks({s, d, old, cl}, w.now);
  auto st = w.t->callback_states();
  EXPECT_EQ(st.at(s.callback_id), "scheduled");
  EXPECT_EQ(st.at(d.callback_id), "scheduled");
  EXPECT_EQ(st.at(old.callback_id), "flagged");
  EXPECT_EQ(st.at(cl.callback_id), "flagged");
  w.attach_not_ready("a2");
  w.ready("a1", w.now);
  w.t->sweep(w.now);
  // a1 takes the older of the two (equal ordering: the restored dispatch keeps its interaction id).
  std::set<std::string> attempts;
  for (int i = 0; i < 2; ++i) {
    ASSERT_FALSE(w.t->live_offer_of("a1").empty());
    ASSERT_TRUE(w.answer("a1", true));
    w.store.flush(w.t.get(), w.now);
    ASSERT_FALSE(w.runs("ie-1").empty());
    attempts.insert(w.runs("ie-1").back().interaction_id());
    rv1::ReportOutcomeRequest out;
    auto* o = out.mutable_owner();
    o->set_tenant_id("acme");
    o->set_interaction_id(w.runs("ie-1").back().interaction_id());
    o->set_owner_epoch(3000 + i);
    o->set_replica_id("ie-1");
    out.set_reservation_id(w.runs("ie-1").back().reservation_id());
    out.set_outcome(rv1::OUTCOME_CALLER_GONE);
    ASSERT_TRUE(w.t->report_outcome(out, w.now));
  }
  EXPECT_TRUE(attempts.contains(d.attempt_interaction_id));
  EXPECT_TRUE(attempts.contains(s.attempt_interaction_id));
  EXPECT_TRUE(w.broken().empty());
}

TEST(Targets, DirectOfferGoesOnlyToItsAgent) {
  P3 w(cfg());
  w.ready("a1", T0 - 60'000);
  w.ready("a2", T0 - 1'000);
  rv1::EnqueueRequest r;
  *r.mutable_owner() = w.owner(IX1);
  r.set_enqueue_seq(1);
  r.mutable_target()->set_agent_id("a2");
  r.set_first_queued_at_ms(w.now);
  r.set_ordering_at_ms(w.now);
  auto e = w.t->enqueue(r, w.now);
  ASSERT_TRUE(e);
  EXPECT_EQ(e->entry().target().agent_id(), "a2");
  EXPECT_TRUE(w.t->live_offer_of("a1").empty());
  ASSERT_FALSE(w.t->live_offer_of("a2").empty());
  EXPECT_TRUE(w.rec.gw.back().second.offer().direct());
  // Another tenant's agent, or one logged out, can't take a direct offer.
  *r.mutable_owner() = w.owner(IX2);
  r.mutable_target()->set_agent_id("globex-agent");
  auto bad = w.t->enqueue(r, w.now);
  ASSERT_FALSE(bad);
  EXPECT_EQ(bad.error().parity, "agent_unavailable");
  EXPECT_TRUE(w.broken().empty());
}

TEST(Targets, DirectOfferOverflowsAtItsMaxWait) {
  TenantConfig c = cfg();
  c.direct_max_wait_ms = 20'000;
  P3 w(c);
  w.attach_not_ready("a2");
  rv1::EnqueueRequest r;
  *r.mutable_owner() = w.owner(IX1);
  r.set_enqueue_seq(1);
  r.mutable_target()->set_agent_id("a2");
  r.set_first_queued_at_ms(w.now);
  r.set_ordering_at_ms(w.now);
  ASSERT_TRUE(w.t->enqueue(r, w.now));
  w.advance(20'000);
  ASSERT_TRUE(w.rec.ie.back().second.has_overflow());
  EXPECT_EQ(w.rec.ie.back().second.overflow().action(), rv1::OVERFLOW_ACTION_VOICEMAIL);
  EXPECT_TRUE(w.broken().empty());
}

TEST(Targets, ConsultToAQueueAndHeadPlacement) {
  P3 w(cfg());
  w.ready("a1", T0 - 60'000);
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  ASSERT_TRUE(w.answer("a1", true));
  std::string res;
  for (const auto& [rp, m] : w.rec.ie) {
    if (m.has_assigned()) res = m.assigned().reservation_id();
  }
  rv1::ReportOutcomeRequest o;
  *o.mutable_owner() = w.owner(IX1);
  o.set_reservation_id(res);
  o.set_outcome(rv1::OUTCOME_CONNECTED);
  ASSERT_TRUE(w.t->report_outcome(o, w.now));
  w.ready("a3", w.now);
  rv1::EnqueueRequest r;
  *r.mutable_owner() = w.owner(IX1);
  r.set_enqueue_seq(2);
  r.mutable_target()->mutable_consult()->set_from_agent_id("a1");
  r.mutable_target()->mutable_consult()->set_queue_id("support");
  r.set_first_queued_at_ms(w.now);
  r.set_ordering_at_ms(w.now);
  r.set_reason(rv1::ENQUEUE_REASON_CONSULT);
  ASSERT_TRUE(w.t->enqueue(r, w.now));
  ASSERT_FALSE(w.t->live_offer_of("a3").empty());
  EXPECT_TRUE(w.rec.gw.back().second.offer().consult());
  ASSERT_TRUE(w.answer("a3", true));
  EXPECT_TRUE(w.rec.ie.back().second.assigned().consult());
  // PLACEMENT_HEAD (the agent was lost): next to be offered, ahead of a higher priority.
  ASSERT_TRUE(w.enqueue(IX2, "support", 100));
  rv1::EnqueueRequest h;
  *h.mutable_owner() = w.owner("0192f6a1-7c3e-7b21-9a4d-5e6f70819209");
  h.set_enqueue_seq(1);
  h.mutable_target()->set_queue_id("support");
  h.set_priority(10);
  h.set_first_queued_at_ms(w.now);
  h.set_ordering_at_ms(w.now);
  h.set_reason(rv1::ENQUEUE_REASON_AGENT_LOST);
  h.set_placement(rv1::PLACEMENT_HEAD);
  auto he = w.t->enqueue(h, w.now);
  ASSERT_TRUE(he);
  EXPECT_EQ(he->entry().position(), 1u);
  for (const auto& b : w.broken()) ADD_FAILURE() << b;
}

TEST(Buckets, TheFinishedBucketHasARowPerQueue) {
  P3 w(cfg());
  EXPECT_TRUE(w.t->take_buckets(w.now).empty());  // the first call starts the bucket
  w.ready("a1", w.now);
  ASSERT_TRUE(w.enqueue(IX1, "sales"));
  ASSERT_TRUE(w.answer("a1", true));
  w.advance(900'000);
  auto rows = w.t->take_buckets(w.now);
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0].queue_id, "sales");
  EXPECT_EQ(rows[0].offered, 1u);
  EXPECT_EQ(rows[0].answered, 1u);
  EXPECT_EQ(rows[1].offered, 0u);
}

}  // namespace
}  // namespace pcr
