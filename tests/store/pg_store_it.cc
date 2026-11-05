// Phase 3 acceptance against a real Postgres (scripts/store-it.sh brings one up) and phase 6's fenced
// path (an old leader's writes refused once a new owner raised the tenant's fence; numbers only from
// Postgres): a callback
// survives an RE restart; a duplicate RunCallback (redelivered to another stream, or re-sent after a
// restart) is never dialled twice; row-level security keeps tenants apart; Postgres down refuses new
// callbacks without stranding the caller. Skipped without PCR_PG_CONNINFO_FILE.
#include <gtest/gtest.h>
#include <libpq-fe.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <future>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

#include "src/core/engine.h"
#include "src/store/pg_store.h"

namespace pcr {
namespace {

constexpr int64_t T0 = 1'792'800'000'000;
constexpr const char* IX = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
constexpr const char* NUMBER = "+447700900123";

std::string conninfo() {
  const char* f = std::getenv("PCR_PG_CONNINFO_FILE");
  if (!f) return {};
  std::ifstream in(f);
  if (!in) {
    ADD_FAILURE() << "PCR_PG_CONNINFO_FILE is set but unreadable";
    return {};
  }
  std::stringstream ss;
  ss << in.rdbuf();
  std::string s = ss.str();
  while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
  return s;
}

// The IE side: RunCallbacks as they reach each replica's stream, and the claims a real IE makes
// (ie/owner/{interaction_id}): a second RunCallback for a claimed id places no call.
struct IeSide final : Sink {
  std::mutex mu;
  std::vector<std::pair<std::string, rv1::RunCallback>> runs;
  std::set<std::string> dialled;
  int dials = 0;
  void to_ie(const std::string& r, uint64_t, const rv1::WatchResponse& m) override {
    if (!m.has_run_callback()) return;
    std::lock_guard lk(mu);
    runs.emplace_back(r, m.run_callback());
    if (dialled.insert(m.run_callback().interaction_id()).second) ++dials;
  }
  void to_gw(const std::string&, uint64_t, const av1::WatchResponse&) override {}
  bool ie_live(const std::string&) const override { return true; }
  std::vector<std::string> ie_replicas() const override { return {"ie-1", "ie-2"}; }
  size_t count() {
    std::lock_guard lk(mu);
    return runs.size();
  }
};

TenantConfig acme(const std::string& id = "acme") {
  TenantConfig c;
  c.id = id;
  QueueConfig q;
  q.id = "sales";
  q.max_wait_ms = 600'000;
  q.max_wait_action = MaxWaitAction::kDisconnect;
  c.queues = {q};
  for (const char* a : {"a1", "a2"}) {
    AgentConfig ac;
    ac.id = a;
    ac.queues = {"sales"};
    c.agents.push_back(ac);
  }
  c.callback_deadline_ms = 60'000;
  return c;
}

template <class F>
auto on(Engine& e, const std::string& tenant, F f) {
  using R = decltype(f(static_cast<Tenant*>(nullptr), int64_t{}));
  std::promise<R> p;
  auto fut = p.get_future();
  e.post(tenant, [&](Tenant* t, int64_t now) { p.set_value(f(t, now)); }, false);
  return fut.get();
}

bool eventually(const std::function<bool()>& ok, int ms = 10'000) {
  for (int i = 0; i < ms / 10; ++i) {
    if (ok()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return ok();
}

// One RE incarnation: an engine on a manual clock with the Postgres store.
struct Re {
  ManualClock clock{T0};
  IeSide* ie;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<PgStore> store;
  std::map<std::string, Tenant::SeededSession> sess;
  Re(IeSide* side, uint64_t incarnation, const std::string& ci, int64_t at) : ie(side) {
    clock.set(at);
    engine = std::make_unique<Engine>(&clock, ie, 1);
    PgStore::Options o;
    o.offered_10s = true;  // db/V3 is applied (scripts/local-postgres.sh): the forecaster's path
    o.conninfo = ci;
    o.tenants = {"acme"};
    store = std::make_unique<PgStore>(o, engine.get(), &clock);
    engine->set_durable(store.get(), nullptr);
    engine->load({acme()}, Epochs{incarnation, (incarnation - 1) << 32});
    store->start();
  }
  ~Re() {
    store->stop();
    store.reset();
    engine.reset();
  }
  void ready(const std::string& a) {
    sess[a] = on(*engine, "acme", [&](Tenant* t, int64_t now) {
      auto s = t->seed_attached(a, "gw-1", "sip:" + a + "@acme.agents.example", now, now);
      t->sweep(now);
      return s;
    });
  }
  void logged_in(const std::string& a) {
    sess[a] = on(*engine, "acme", [&](Tenant* t, int64_t now) { return t->seed_attached(a, "gw-1", "", std::nullopt, now); });
  }
  bool accept(const std::string& a) {
    return on(*engine, "acme", [&](Tenant* t, int64_t now) {
      av1::AnswerOfferRequest r;
      r.mutable_session()->set_tenant_id("acme");
      r.mutable_session()->set_agent_id(a);
      r.mutable_session()->set_session_id(sess[a].session_id);
      r.mutable_session()->set_attach_generation(sess[a].attach_generation);
      r.set_command_id("it:" + std::to_string(now) + a);
      r.set_offer_id(t->live_offer_of(a));
      r.set_decision(av1::DECISION_ACCEPT);
      return static_cast<bool>(t->answer_offer(r, now));
    });
  }
  void advance(int64_t ms) { engine->advance_to(&clock, clock.now_ms() + ms); }
};

class Store : public ::testing::Test {
 protected:
  void SetUp() override {
    ci_ = conninfo();
    if (ci_.empty()) GTEST_SKIP() << "no PCR_PG_CONNINFO_FILE (scripts/store-it.sh runs this with a Postgres)";
  }
  std::string ci_;
};

TEST_F(Store, ACallbackSurvivesARestartAndADuplicateRunCallbackDialsOnce) {
  IeSide ie;
  std::string cb, attempt;
  {
    Re re(&ie, 1, ci_, T0);
    ASSERT_TRUE(eventually([&] { return re.store->healthy() && re.store->loaded(); }));
    re.logged_in("a2");
    auto entry = on(*re.engine, "acme", [&](Tenant* t, int64_t now) {
      rv1::EnqueueRequest r;
      auto* o = r.mutable_owner();
      o->set_tenant_id("acme");
      o->set_interaction_id(IX);
      o->set_owner_epoch(1042);
      o->set_replica_id("ie-1");
      r.set_enqueue_seq(1);
      r.mutable_target()->set_queue_id("sales");
      r.set_first_queued_at_ms(now);
      r.set_ordering_at_ms(now);
      return t->enqueue(r, now)->entry().entry_id();
    });
    std::promise<Result<rv1::ConvertToCallbackResponse>> p;
    auto fut = p.get_future();
    re.engine->post("acme", [&](Tenant* t, int64_t now) {
      rv1::ConvertToCallbackRequest r;
      auto* o = r.mutable_owner();
      o->set_tenant_id("acme");
      o->set_interaction_id(IX);
      o->set_owner_epoch(1042);
      o->set_replica_id("ie-1");
      r.set_entry_id(entry);
      r.set_callback_number(NUMBER);
      r.set_keep_position(true);
      t->convert_to_callback(r, now, [&](Result<rv1::ConvertToCallbackResponse> x) { p.set_value(std::move(x)); });
    }, false);
    auto res = fut.get();  // answered once Postgres has it
    ASSERT_TRUE(res) << res.error().parity;
    cb = res->callback_id();
  }
  // Restart 1 (RE's memory gone): the callback comes back from Postgres and is dispatched.
  {
    Re re(&ie, 2, ci_, T0 + 60'000);
    ASSERT_TRUE(eventually([&] { return re.store->loaded(); }));
    ASSERT_TRUE(eventually([&] {
      return on(*re.engine, "acme", [&](Tenant* t, int64_t) { return t->callback_states().contains(cb); });
    }));
    re.logged_in("a2");
    re.ready("a1");
    ASSERT_TRUE(re.accept("a1"));
    ASSERT_TRUE(eventually([&] { return ie.count() == 1; }));  // RunCallback once the attempt is durable
    attempt = ie.runs[0].second.interaction_id();
    EXPECT_EQ(ie.runs[0].second.callback_id(), cb);
    EXPECT_EQ(ie.runs[0].second.number(), NUMBER);
    // No ack for 5 s: redelivered to another stream with the same interaction id.
    re.advance(5'000);
    ASSERT_TRUE(eventually([&] { return ie.count() >= 2; }));
    for (const auto& [r, run] : ie.runs) EXPECT_EQ(run.interaction_id(), attempt);
  }
  // Restart 2 before any ack: Postgres says "dispatched": RE sends only the same interaction id
  // again, which IE drops (its claim fails): still one dial.
  {
    Re re(&ie, 3, ci_, T0 + 120'000);
    ASSERT_TRUE(eventually([&] { return re.store->loaded(); }));
    ASSERT_TRUE(eventually([&] {
      return on(*re.engine, "acme", [&](Tenant* t, int64_t) { return t->callback_states().contains(cb); });
    }));
    const size_t before = ie.count();
    re.logged_in("a2");
    re.ready("a1");
    ASSERT_TRUE(re.accept("a1"));
    ASSERT_TRUE(eventually([&] { return ie.count() > before; }));
    std::lock_guard lk(ie.mu);
    for (const auto& [r, run] : ie.runs) EXPECT_EQ(run.interaction_id(), attempt);
    EXPECT_EQ(ie.dials, 1);
  }
}

TEST_F(Store, RowLevelSecurityKeepsTenantsApart) {
  PGconn* c = PQconnectdb(ci_.c_str());
  ASSERT_EQ(PQstatus(c), CONNECTION_OK);
  auto count = [&](const char* tenant) {
    PQclear(PQexec(c, "BEGIN"));
    if (tenant) {
      const char* p[] = {tenant};
      PQclear(PQexecParams(c, "SELECT set_config('parity.tenant_id', $1, true)", 1, nullptr, p, nullptr, nullptr, 0));
    }
    PGresult* r = PQexec(c, "SELECT count(*) FROM routing_callbacks");
    const int n = PQresultStatus(r) == PGRES_TUPLES_OK ? std::atoi(PQgetvalue(r, 0, 0)) : -1;
    PQclear(r);
    PQclear(PQexec(c, "COMMIT"));
    return n;
  };
  EXPECT_GT(count("acme"), 0);   // the callbacks of the test above (or of an earlier run)
  EXPECT_EQ(count("globex"), 0);  // another tenant sees none
  EXPECT_EQ(count(nullptr), 0);   // no tenant set: nothing
  // The service role can't change the schema.
  PGresult* ddl = PQexec(c, "ALTER TABLE routing_callbacks ADD COLUMN x int");
  EXPECT_NE(PQresultStatus(ddl), PGRES_COMMAND_OK);
  PQclear(ddl);
  PQfinish(c);
}

// Phase 5: the forecaster's history (routing_bucket_actuals, PLAN §5.3) written by the writer's
// buckets and read back for the profile: the tenant's own rows only, from `since`.
TEST_F(Store, BucketHistoryLoadsForTheForecaster) {
  IeSide ie;
  Re r(&ie, 1, ci_, T0);
  const int64_t b0 = (T0 / 900'000) * 900'000 - 7LL * 86'400'000;  // a week ago, bucket-aligned
  std::vector<BucketRow> rows;
  for (int k = 0; k < 3; ++k) {
    BucketRow b;
    b.tenant = "acme";
    b.queue_id = "sales";
    b.bucket_start_ms = b0 + k * 900'000;
    b.offered = 10 + k;
    b.answered = 9;
    if (k == 0) {  // db/V3: the 10 s counts (the other rows predate them: NULL)
      b.offered_10s.assign(90, 0);
      b.offered_10s[3] = 7;
      b.offered_10s[89] = 3;
    }
    rows.push_back(b);
  }
  std::vector<BucketRow> got;
  const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (got.size() < 3 && std::chrono::steady_clock::now() < until) {
    // Both are refused until the store has connected: the writes are best effort (an upsert, so
    // again is harmless) and the optimizer asks for the history again on its next tick.
    r.store->put_buckets(rows);
    auto p = std::make_shared<std::promise<std::vector<BucketRow>>>();
    if (r.store->load_actuals("acme", b0, [p](std::vector<BucketRow> x) { p->set_value(std::move(x)); })) {
      got = p->get_future().get();
    }
    if (got.size() < 3) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_GE(got.size(), 3u);
  for (const auto& b : got) {
    EXPECT_EQ(b.tenant, "acme");
    EXPECT_GE(b.bucket_start_ms, b0);
  }
  EXPECT_EQ(got.front().offered, 10u);
  ASSERT_EQ(got.front().offered_10s.size(), 90u);
  EXPECT_EQ(got.front().offered_10s[3], 7);
  EXPECT_EQ(got.front().offered_10s[89], 3);
  EXPECT_TRUE(got.back().offered_10s.empty());
  // From a later `since`, the older rows stay out.
  auto later = std::make_shared<std::promise<std::vector<BucketRow>>>();
  ASSERT_TRUE(r.store->load_actuals("acme", b0 + 900'000, [later](std::vector<BucketRow> x) { later->set_value(std::move(x)); }));
  for (const auto& b : later->get_future().get()) EXPECT_GE(b.bucket_start_ms, b0 + 900'000);
}

// Postgres down: ConvertToCallback is refused at once (UNAVAILABLE / store_unavailable) and the
// caller stays queued and is offered as usual.
TEST(StoreDown, NewCallbacksAreRefusedAndNoCallIsStranded) {
  IeSide ie;
  Re re(&ie, 1, "host=127.0.0.1 port=1 dbname=routing user=x password=x connect_timeout=1", T0);
  re.logged_in("a2");
  auto entry = on(*re.engine, "acme", [&](Tenant* t, int64_t now) {
    rv1::EnqueueRequest r;
    auto* o = r.mutable_owner();
    o->set_tenant_id("acme");
    o->set_interaction_id(IX);
    o->set_owner_epoch(1042);
    o->set_replica_id("ie-1");
    r.set_enqueue_seq(1);
    r.mutable_target()->set_queue_id("sales");
    r.set_first_queued_at_ms(now);
    r.set_ordering_at_ms(now);
    return t->enqueue(r, now)->entry().entry_id();
  });
  std::promise<Result<rv1::ConvertToCallbackResponse>> p;
  auto fut = p.get_future();
  re.engine->post("acme", [&](Tenant* t, int64_t now) {
    rv1::ConvertToCallbackRequest r;
    auto* o = r.mutable_owner();
    o->set_tenant_id("acme");
    o->set_interaction_id(IX);
    o->set_owner_epoch(1042);
    o->set_replica_id("ie-1");
    r.set_entry_id(entry);
    r.set_callback_number(NUMBER);
    t->convert_to_callback(r, now, [&](Result<rv1::ConvertToCallbackResponse> x) { p.set_value(std::move(x)); });
  }, false);
  auto res = fut.get();
  ASSERT_FALSE(res);
  EXPECT_EQ(res.error().parity, "store_unavailable");
  re.ready("a1");
  EXPECT_TRUE(on(*re.engine, "acme", [&](Tenant* t, int64_t) { return !t->live_offer_of("a1").empty(); }));
  EXPECT_TRUE(re.engine->check_invariants().empty());
}

// The HA test's own tenant: the phase-3 tests above leave acme's callbacks in this Postgres.
const std::string kHaTenant = "initech-de";

// Phase 6: an in-memory re.state (the latest value per key) that commits at once.
struct MemLog final : StateLog {
  std::mutex mu;
  std::map<std::string, std::string> kv;
  void submit(Batch&& b) override {
    {
      std::lock_guard lk(mu);
      for (auto& r : b.records) {
        if (r.value) {
          kv[r.key] = *r.value;
        } else {
          kv.erase(r.key);
        }
      }
    }
    for (auto& fx : b.effects) fx();
  }
  size_t backlog() const override { return 0; }
  std::vector<std::pair<std::string, std::string>> all() {
    std::lock_guard lk(mu);
    return {kv.begin(), kv.end()};
  }
};

// One HA leader: the engine with the state log, the fenced store, the tenant acquired at
// (tenant_epoch 0, leader_epoch).
struct Leader {
  ManualClock clock{T0};
  std::unique_ptr<Engine> engine;
  std::unique_ptr<PgStore> store;
  Leader(IeSide* ie, MemLog* log, uint64_t epoch, const std::string& ci, int64_t at,
         const std::vector<std::pair<std::string, std::string>>* restore_from) {
    clock.set(at);
    engine = std::make_unique<Engine>(&clock, ie, 1);
    PgStore::Options o;
    o.conninfo = ci;
    o.fenced = true;
    store = std::make_unique<PgStore>(o, engine.get(), &clock);
    engine->set_state_log(log);
    engine->set_durable(store.get(), nullptr);
    engine->load({}, Epochs{epoch, 0});
    auto recs = restore_from ? *restore_from : std::vector<std::pair<std::string, std::string>>{};
    engine->add_tenant(acme(kHaTenant), 0, [recs](Tenant* t, int64_t now) { t->restore(recs, now, false); });
    store->start();
    store->load_tenant(kHaTenant, 0, epoch);
  }
  ~Leader() {
    store->stop();
    store.reset();
    engine.reset();
  }
  Result<rv1::ConvertToCallbackResponse> convert(const std::string& ix) {
    const std::string entry = on(*engine, kHaTenant, [&](Tenant* t, int64_t now) {
      rv1::EnqueueRequest r;
      auto* o = r.mutable_owner();
      o->set_tenant_id(kHaTenant);
      o->set_interaction_id(ix);
      o->set_owner_epoch(7);
      o->set_replica_id("ie-1");
      r.set_enqueue_seq(1);
      r.mutable_target()->set_queue_id("sales");
      r.set_first_queued_at_ms(now);
      r.set_ordering_at_ms(now);
      return t->enqueue(r, now)->entry().entry_id();
    });
    std::promise<Result<rv1::ConvertToCallbackResponse>> p;
    auto fut = p.get_future();
    engine->post(kHaTenant, [&](Tenant* t, int64_t now) {
      rv1::ConvertToCallbackRequest r;
      auto* o = r.mutable_owner();
      o->set_tenant_id(kHaTenant);
      o->set_interaction_id(ix);
      o->set_owner_epoch(7);
      o->set_replica_id("ie-1");
      r.set_entry_id(entry);
      r.set_callback_number(NUMBER);
      t->convert_to_callback(r, now, [&](Result<rv1::ConvertToCallbackResponse> x) { p.set_value(std::move(x)); });
    }, false);
    return fut.get();
  }
};

// Phase 6 (PLAN §5.5, the Postgres/re.state ordering): a new owner raises the tenant's fence before it
// reads; from then on the old leader's callback writes land nowhere (its convert is refused and no row
// appears). The new leader rebuilds from re.state alone, takes each number from Postgres (re.state
// never holds one) and dispatches the callback once.
TEST_F(Store, HaFenceRefusesTheOldLeaderAndATakeoverReconcilesCallbacks) {
  IeSide ie;
  MemLog log;
  const std::string ix1 = "0192f6a1-7c3e-7b21-9a4d-6e6f70819301", ix2 = "0192f6a1-7c3e-7b21-9a4d-6e6f70819302";
  Leader old_leader(&ie, &log, 5, ci_, T0 + 300'000, nullptr);
  ASSERT_TRUE(eventually([&] { return old_leader.store->loads() == 1; }));
  auto first = old_leader.convert(ix1);
  ASSERT_TRUE(first) << first.error().parity;
  for (const auto& [k, v] : log.all()) EXPECT_EQ(v.find(NUMBER), std::string::npos) << "a number in re.state: " << k;
  // The new leader acquires the tenant: the fence rises to (0, 9), then it reads.
  const auto recs = log.all();
  Leader new_leader(&ie, &log, 9, ci_, T0 + 310'000, &recs);
  ASSERT_TRUE(eventually([&] { return new_leader.store->loads() == 1; }));
  ASSERT_TRUE(eventually([&] {
    return on(*new_leader.engine, kHaTenant, [&](Tenant* t, int64_t) { return t->callbacks_reconciled(); });
  }));
  // The old leader (not yet stepped down) tries another conversion: fenced in Postgres.
  auto second = old_leader.convert(ix2);
  EXPECT_FALSE(second);
  if (!second) {
    EXPECT_EQ(second.error().parity, "store_unavailable");
  }
  PGconn* c = PQconnectdb(ci_.c_str());
  ASSERT_EQ(PQstatus(c), CONNECTION_OK);
  PQclear(PQexec(c, "BEGIN"));
  const char* p[] = {kHaTenant.c_str(), ix2.c_str()};
  PQclear(PQexecParams(c, "SELECT set_config('parity.tenant_id', $1, true)", 1, nullptr, p, nullptr, nullptr, 0));
  PGresult* r = PQexecParams(c, "SELECT count(*) FROM routing_callbacks WHERE tenant_id = $1 AND original_interaction_id = $2::uuid",
                             2, nullptr, p, nullptr, nullptr, 0);
  ASSERT_EQ(PQresultStatus(r), PGRES_TUPLES_OK);
  EXPECT_EQ(std::atoi(PQgetvalue(r, 0, 0)), 0) << "a fenced leader's write landed";
  PQclear(r);
  PQclear(PQexec(c, "COMMIT"));
  PQfinish(c);
  // The new leader holds the first callback (from re.state) with its number (from Postgres) and
  // dispatches it once.
  ASSERT_TRUE(on(*new_leader.engine, kHaTenant, [&](Tenant* t, int64_t) { return t->callback_states().contains(first->callback_id()); }));
  const size_t before = ie.count();
  const auto sess = on(*new_leader.engine, kHaTenant, [&](Tenant* t, int64_t now) {
    auto ss = t->seed_attached("a1", "gw-1", "sip:a1@initech-de.agents.example", now, now);
    t->sweep(now);
    return ss;
  });
  ASSERT_TRUE(on(*new_leader.engine, kHaTenant, [&](Tenant* t, int64_t now) {
    av1::AnswerOfferRequest q;
    q.mutable_session()->set_tenant_id(kHaTenant);
    q.mutable_session()->set_agent_id("a1");
    q.mutable_session()->set_session_id(sess.session_id);
    q.mutable_session()->set_attach_generation(sess.attach_generation);
    q.set_command_id("it-ha-accept");
    q.set_offer_id(t->live_offer_of("a1"));
    q.set_decision(av1::DECISION_ACCEPT);
    return static_cast<bool>(t->answer_offer(q, now));
  }));
  ASSERT_TRUE(eventually([&] { return ie.count() == before + 1; }));  // the attempt durable, then RunCallback
  std::lock_guard lk(ie.mu);
  EXPECT_EQ(ie.runs.back().second.callback_id(), first->callback_id());
  EXPECT_EQ(ie.runs.back().second.number(), NUMBER);  // from Postgres: re.state never had it
  for (const auto& [k, v] : log.all()) EXPECT_EQ(v.find(NUMBER), std::string::npos) << "a number in re.state: " << k;
}

}  // namespace
}  // namespace pcr
