#include "src/store/pg_store.h"

#include <libpq-fe.h>

#include <algorithm>

#include <nlohmann/json.hpp>
#include <optional>

#include "src/ops/log.h"

namespace pcr {

namespace {

using json = nlohmann::json;
using Param = std::optional<std::string>;

PGconn* pg(void* c) { return static_cast<PGconn*>(c); }

// One statement with text parameters; true when it succeeded. *rows gets the result (caller clears).
bool exec(void* conn, const char* sql, const std::vector<Param>& params, PGresult** rows = nullptr) {
  std::vector<const char*> v;
  v.reserve(params.size());
  for (const auto& p : params) v.push_back(p ? p->c_str() : nullptr);
  PGresult* r = PQexecParams(pg(conn), sql, static_cast<int>(v.size()), nullptr, v.data(), nullptr, nullptr, 0);
  const ExecStatusType st = PQresultStatus(r);
  const bool ok = st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK;
  if (!ok) {
    // The SQLSTATE only: a message may quote a value (a phone number).
    const char* code = PQresultErrorField(r, PG_DIAG_SQLSTATE);
    LogRec("warn", "store.statement_failed", "postgres statement failed").error("unavailable").attr("sqlstate", code ? code : "?");
  }
  if (rows && ok) {
    *rows = r;
  } else {
    PQclear(r);
  }
  return ok;
}

// A transaction for one tenant: row-level security reads parity.tenant_id.
bool in_tenant(void* conn, const std::string& tenant, const std::function<bool()>& body) {
  if (!exec(conn, "BEGIN", {})) return false;
  if (!exec(conn, "SELECT set_config('parity.tenant_id', $1, true)", {tenant}) || !body()) {
    exec(conn, "ROLLBACK", {});
    return false;
  }
  return exec(conn, "COMMIT", {});
}

std::string reqs_json(const std::vector<SkillReq>& reqs) {
  json a = json::array();
  for (const auto& r : reqs) a.push_back({{"skill_id", r.skill_id}, {"min_level", r.min_level}});
  return a.dump();
}

Param opt(const std::string& s) { return s.empty() ? Param{} : Param{s}; }

// offered_10s as a Postgres integer[] literal (NULL when there is none).
Param int_array(const std::vector<uint16_t>& v) {
  if (v.empty()) return Param{};
  std::string s = "{";
  for (size_t i = 0; i < v.size(); ++i) {
    if (i) s += ',';
    s += std::to_string(v[i]);
  }
  return Param{s + "}"};
}

std::vector<uint16_t> parse_int_array(const char* s) {
  std::vector<uint16_t> out;
  if (!s || *s != '{') return out;
  uint32_t cur = 0;
  bool any = false;
  for (const char* p = s + 1; *p; ++p) {
    if (*p >= '0' && *p <= '9') {
      cur = std::min<uint32_t>(65535, cur * 10 + static_cast<uint32_t>(*p - '0'));
      any = true;
    } else if (*p == ',' || *p == '}') {
      out.push_back(static_cast<uint16_t>(any ? cur : 0));
      cur = 0;
      any = false;
      if (*p == '}') break;
    }
  }
  return out;
}

}  // namespace

PgStore::PgStore(Options opts, Engine* engine, const Clock* clock)
    : opts_(std::move(opts)), engine_(engine), clock_(clock) {}

PgStore::~PgStore() { stop(); }

void PgStore::start() {
  th_ = std::thread([this] { loop(); });
}

void PgStore::stop() {
  {
    std::lock_guard lk(mu_);
    if (stop_ && !th_.joinable()) return;
    stop_ = true;
  }
  cv_.notify_all();
  if (th_.joinable()) th_.join();
  std::deque<Op> left;
  {
    std::lock_guard lk(mu_);
    left.swap(q_);
  }
  for (auto& op : left) op.finish(false);
  if (conn_) {
    PQfinish(pg(conn_));
    conn_ = nullptr;
  }
  healthy_ = false;
}

bool PgStore::enqueue(Op op) {
  {
    std::lock_guard lk(mu_);
    if (stop_ || !healthy_.load() || q_.size() >= opts_.max_queued) {
      refused_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    q_.push_back(std::move(op));
  }
  cv_.notify_one();
  return true;
}

bool PgStore::put_callback(const CallbackRecord& c, Done done) {
  Engine* e = engine_;
  Op op;
  op.tenant = c.tenant;
  const bool fenced = opts_.fenced;
  op.run = [this, c, fenced](void* conn) {
    std::vector<Param> params = {c.tenant, c.callback_id, c.original_interaction_id, c.entry_id, c.queue_id, c.number,
                                 std::to_string(c.priority), reqs_json(c.reqs), std::to_string(c.first_queued_at),
                                 std::to_string(c.ordering_at), c.state, std::to_string(c.attempt), c.attempt_interaction_id,
                                 opt(c.reservation_id), opt(c.agent_id), opt(c.outcome), std::to_string(c.due_at),
                                 std::to_string(c.created_at), std::to_string(c.updated_at)};
    static const char* kSet =
        "ON CONFLICT (tenant_id, callback_id) DO UPDATE SET state = EXCLUDED.state, attempt = EXCLUDED.attempt, "
        "attempt_interaction_id = EXCLUDED.attempt_interaction_id, reservation_id = EXCLUDED.reservation_id, "
        "agent_id = EXCLUDED.agent_id, outcome = EXCLUDED.outcome, due_at_ms = EXCLUDED.due_at_ms, "
        "updated_at_ms = EXCLUDED.updated_at_ms WHERE routing_callbacks.tenant_id = $1";
    if (!fenced) {
      return in_tenant(conn, c.tenant, [&] {
        return exec(conn,
                    (std::string("INSERT INTO routing_callbacks (tenant_id, callback_id, original_interaction_id, entry_id, queue_id, number, "
                                 "priority, requirements, first_queued_at_ms, ordering_at_ms, state, attempt, attempt_interaction_id, "
                                 "reservation_id, agent_id, outcome, due_at_ms, created_at_ms, updated_at_ms) "
                                 "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) ") +
                     kSet).c_str(),
                    params);
      });
    }
    // Phase 6: written only while the tenant's fence is still this owner's (FOR SHARE: a new
    // owner's raise waits for this statement, and a later one sees the raised fence).
    auto f = fence_.find(c.tenant);
    if (f == fence_.end()) return false;  // not (or no longer) this process's tenant
    params.push_back(std::to_string(f->second.first));
    params.push_back(std::to_string(f->second.second));
    return in_tenant(conn, c.tenant, [&] {
      PGresult* r = nullptr;
      if (!exec(conn,
                (std::string("WITH f AS (SELECT 1 FROM routing_fence WHERE tenant_id = $1 AND tenant_epoch = $20::bigint "
                             "AND leader_epoch = $21::bigint FOR SHARE) "
                             "INSERT INTO routing_callbacks (tenant_id, callback_id, original_interaction_id, entry_id, queue_id, number, "
                             "priority, requirements, first_queued_at_ms, ordering_at_ms, state, attempt, attempt_interaction_id, "
                             "reservation_id, agent_id, outcome, due_at_ms, created_at_ms, updated_at_ms) "
                             "SELECT $1::text, $2::uuid, $3::uuid, $4::uuid, $5::text, $6::text, $7::integer, $8::jsonb, $9::bigint, "
                             "$10::bigint, $11::text, $12::integer, $13::uuid, $14::uuid, $15::text, $16::text, $17::bigint, $18::bigint, "
                             "$19::bigint FROM f ") +
                 kSet + " RETURNING 1")
                    .c_str(),
                params, &r)) {
        return false;
      }
      const bool wrote = PQntuples(r) == 1;
      PQclear(r);
      if (!wrote) LogRec("warn", "store.write_fenced", "a callback write was fenced (a newer owner holds the tenant)").tenant(c.tenant);
      return wrote;
    });
  };
  const std::string tenant = c.tenant;
  op.finish = [e, tenant, done](bool ok) {
    e->post(tenant, [done, ok](Tenant* t, int64_t now) { done(t, ok, now); }, /*external=*/false);
  };
  return enqueue(std::move(op));
}

void PgStore::load_tenant(const std::string& tenant, uint64_t tenant_epoch, uint64_t leader_epoch) {
  {
    std::lock_guard lk(mu_);
    pending_loads_[tenant] = {tenant_epoch, leader_epoch};
    forgotten_.erase(tenant);
  }
  cv_.notify_one();
}

void PgStore::forget_tenant(const std::string& tenant) {
  {
    std::lock_guard lk(mu_);
    pending_loads_.erase(tenant);
    forgotten_.insert(tenant);
  }
  cv_.notify_one();
}

namespace {
bool read_open(void* conn, const std::string& tenant, std::vector<CallbackRecord>* rows) {
  PGresult* r = nullptr;
  if (!exec(conn,
            "SELECT callback_id, original_interaction_id, entry_id, queue_id, number, priority, requirements::text, "
            "first_queued_at_ms, ordering_at_ms, state, attempt, attempt_interaction_id, coalesce(reservation_id::text, ''), "
            "coalesce(agent_id, ''), coalesce(outcome, ''), due_at_ms, created_at_ms, updated_at_ms "
            "FROM routing_callbacks WHERE tenant_id = $1 AND state IN ('scheduled', 'dispatched', 'claimed')",
            {tenant}, &r)) {
    return false;
  }
  for (int i = 0; i < PQntuples(r); ++i) {
    CallbackRecord c;
    c.tenant = tenant;
    int k = 0;
    c.callback_id = PQgetvalue(r, i, k++);
    c.original_interaction_id = PQgetvalue(r, i, k++);
    c.entry_id = PQgetvalue(r, i, k++);
    c.queue_id = PQgetvalue(r, i, k++);
    c.number = PQgetvalue(r, i, k++);
    c.priority = static_cast<uint32_t>(std::stoul(PQgetvalue(r, i, k++)));
    for (const auto& q : json::parse(PQgetvalue(r, i, k++))) {
      c.reqs.push_back(SkillReq{q.at("skill_id").get<std::string>(), q.at("min_level").get<uint32_t>()});
    }
    c.first_queued_at = std::stoll(PQgetvalue(r, i, k++));
    c.ordering_at = std::stoll(PQgetvalue(r, i, k++));
    c.state = PQgetvalue(r, i, k++);
    c.attempt = std::stoi(PQgetvalue(r, i, k++));
    c.attempt_interaction_id = PQgetvalue(r, i, k++);
    c.reservation_id = PQgetvalue(r, i, k++);
    c.agent_id = PQgetvalue(r, i, k++);
    c.outcome = PQgetvalue(r, i, k++);
    c.due_at = std::stoll(PQgetvalue(r, i, k++));
    c.created_at = std::stoll(PQgetvalue(r, i, k++));
    c.updated_at = std::stoll(PQgetvalue(r, i, k++));
    rows->push_back(std::move(c));
  }
  PQclear(r);
  return true;
}
}  // namespace

bool PgStore::load_one(const std::string& tenant, uint64_t te, uint64_t le, bool* stale) {
  std::vector<CallbackRecord> rows;
  *stale = false;
  const bool ok = in_tenant(conn_, tenant, [&] {
    PGresult* r = nullptr;
    if (!exec(conn_,
              "INSERT INTO routing_fence (tenant_id, tenant_epoch, leader_epoch, updated_at_ms) VALUES ($1, $2, $3, $4) "
              "ON CONFLICT (tenant_id) DO UPDATE SET tenant_epoch = EXCLUDED.tenant_epoch, leader_epoch = EXCLUDED.leader_epoch, "
              "updated_at_ms = EXCLUDED.updated_at_ms WHERE (routing_fence.tenant_epoch, routing_fence.leader_epoch) <= "
              "(EXCLUDED.tenant_epoch, EXCLUDED.leader_epoch) AND routing_fence.tenant_id = $1 RETURNING 1",
              {tenant, std::to_string(te), std::to_string(le), std::to_string(clock_->now_ms())}, &r)) {
      return false;
    }
    const bool raised = PQntuples(r) == 1;
    PQclear(r);
    if (!raised) {
      *stale = true;  // a newer owner holds the tenant
      return false;
    }
    return read_open(conn_, tenant, &rows);
  });
  if (!ok) return false;
  fence_[tenant] = {te, le};
  ++loads_;
  LogRec("info", "callbacks.loaded", "callbacks loaded under the tenant's fence").tenant(tenant)
      .num("leader_epoch", static_cast<int64_t>(le)).attr("open", static_cast<int64_t>(rows.size()));
  engine_->post(tenant, [rows = std::move(rows)](Tenant* t, int64_t now) {
    if (t) t->reconcile_callbacks(rows, now);
  }, /*external=*/false);
  return true;
}

void PgStore::run_loads() {
  std::map<std::string, std::pair<uint64_t, uint64_t>> todo;
  {
    std::lock_guard lk(mu_);
    for (const auto& t : forgotten_) fence_.erase(t);
    forgotten_.clear();
    todo = pending_loads_;
  }
  for (const auto& [tenant, e] : todo) {
    bool stale = false;
    const bool ok = load_one(tenant, e.first, e.second, &stale);
    std::lock_guard lk(mu_);
    auto it = pending_loads_.find(tenant);
    if (it == pending_loads_.end() || it->second != e) continue;  // replaced meanwhile
    if (ok) {
      pending_loads_.erase(it);
    } else if (stale) {
      LogRec("warn", "store.write_fenced", "the tenant's Postgres fence is held by a newer owner: its callbacks are not loaded here").tenant(tenant);
      pending_loads_.erase(it);
    }
  }
}

bool PgStore::put_task(const TaskRecord& t, TaskDone done) {
  Engine* e = engine_;
  auto result = std::make_shared<std::pair<std::string, bool>>();  // task id, existing
  Op op;
  op.tenant = t.tenant;
  op.run = [t, result](void* conn) {
    return in_tenant(conn, t.tenant, [&] {
      PGresult* rows = nullptr;
      if (!exec(conn,
                "INSERT INTO routing_voicemail_tasks (tenant_id, task_id, interaction_id, queue_id, recording_id, "
                "recorded_at_ms, duration_s, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                "ON CONFLICT (tenant_id, recording_id) DO NOTHING RETURNING task_id",
                {t.tenant, t.task_id, t.interaction_id, t.queue_id, t.recording_id, std::to_string(t.recorded_at),
                 std::to_string(t.duration_s), std::to_string(t.created_at)},
                &rows)) {
        return false;
      }
      const bool inserted = PQntuples(rows) == 1;
      PQclear(rows);
      if (inserted) {
        *result = {t.task_id, false};
        return true;
      }
      // A repeat (the same recording): the task made first, also across restarts.
      if (!exec(conn, "SELECT task_id FROM routing_voicemail_tasks WHERE tenant_id = $1 AND recording_id = $2",
                {t.tenant, t.recording_id}, &rows)) {
        return false;
      }
      const bool found = PQntuples(rows) == 1;
      if (found) *result = {PQgetvalue(rows, 0, 0), true};
      PQclear(rows);
      return found;
    });
  };
  const std::string tenant = t.tenant;
  op.finish = [e, tenant, done, result](bool ok) {
    e->post(tenant, [done, ok, result](Tenant* tn, int64_t now) { done(tn, ok, result->first, result->second, now); },
            /*external=*/false);
  };
  return enqueue(std::move(op));
}

void PgStore::put_buckets(std::vector<BucketRow> rows) {
  if (rows.empty()) return;
  Op op;
  op.tenant = rows.front().tenant;
  const bool with10 = opts_.offered_10s;
  op.run = [rows, with10](void* conn) {
    return in_tenant(conn, rows.front().tenant, [&] {
      for (const auto& b : rows) {
        if (b.tenant != rows.front().tenant) return false;  // one tenant per transaction
        std::vector<Param> p = {b.tenant, b.queue_id, std::to_string(b.bucket_start_ms), std::to_string(b.offered),
                                std::to_string(b.answered), std::to_string(b.answered_in_sla), std::to_string(b.abandoned),
                                std::to_string(b.overflowed), std::to_string(b.wait_answered_ms), std::to_string(b.handle_ms),
                                std::to_string(b.handled)};
        if (with10) p.push_back(int_array(b.offered_10s));
        const char* sql = with10
            ? "INSERT INTO routing_bucket_actuals (tenant_id, queue_id, bucket_start_ms, offered, answered, "
              "answered_in_sla, abandoned, overflowed, wait_answered_ms, handle_ms, handled, offered_10s) "
              "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::integer[]) "
              "ON CONFLICT (tenant_id, queue_id, bucket_start_ms) "
              "DO UPDATE SET offered = EXCLUDED.offered, answered = EXCLUDED.answered, "
              "answered_in_sla = EXCLUDED.answered_in_sla, abandoned = EXCLUDED.abandoned, overflowed = EXCLUDED.overflowed, "
              "wait_answered_ms = EXCLUDED.wait_answered_ms, handle_ms = EXCLUDED.handle_ms, handled = EXCLUDED.handled, "
              "offered_10s = EXCLUDED.offered_10s "
              "WHERE routing_bucket_actuals.tenant_id = $1"
            : "INSERT INTO routing_bucket_actuals (tenant_id, queue_id, bucket_start_ms, offered, answered, "
              "answered_in_sla, abandoned, overflowed, wait_answered_ms, handle_ms, handled) "
              "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (tenant_id, queue_id, bucket_start_ms) "
              "DO UPDATE SET offered = EXCLUDED.offered, answered = EXCLUDED.answered, "
              "answered_in_sla = EXCLUDED.answered_in_sla, abandoned = EXCLUDED.abandoned, overflowed = EXCLUDED.overflowed, "
              "wait_answered_ms = EXCLUDED.wait_answered_ms, handle_ms = EXCLUDED.handle_ms, handled = EXCLUDED.handled "
              "WHERE routing_bucket_actuals.tenant_id = $1";
        if (!exec(conn, sql, p)) return false;
      }
      return true;
    });
  };
  op.finish = [](bool) {};
  if (!enqueue(std::move(op))) dropped_.fetch_add(1, std::memory_order_relaxed);
}

bool PgStore::load_actuals(const std::string& tenant, int64_t since_ms,
                           std::function<void(std::vector<BucketRow>)> done) {
  Op op;
  op.tenant = tenant;
  auto rows = std::make_shared<std::vector<BucketRow>>();
  const bool with10 = opts_.offered_10s;
  op.run = [tenant, since_ms, rows, with10](void* conn) {
    rows->clear();
    return in_tenant(conn, tenant, [&] {
      PGresult* res = nullptr;
      if (!exec(conn,
                with10 ? "SELECT queue_id, bucket_start_ms, offered, answered, answered_in_sla, abandoned, handle_ms, handled, "
                         "offered_10s FROM routing_bucket_actuals WHERE tenant_id = $1 AND bucket_start_ms >= $2 "
                         "ORDER BY bucket_start_ms"
                       : "SELECT queue_id, bucket_start_ms, offered, answered, answered_in_sla, abandoned, handle_ms, handled "
                         "FROM routing_bucket_actuals WHERE tenant_id = $1 AND bucket_start_ms >= $2 ORDER BY bucket_start_ms",
                {tenant, std::to_string(since_ms)}, &res)) {
        return false;
      }
      for (int i = 0; i < PQntuples(res); ++i) {
        BucketRow b;
        b.tenant = tenant;
        b.queue_id = PQgetvalue(res, i, 0);
        b.bucket_start_ms = std::stoll(PQgetvalue(res, i, 1));
        b.offered = std::stoull(PQgetvalue(res, i, 2));
        b.answered = std::stoull(PQgetvalue(res, i, 3));
        b.answered_in_sla = std::stoull(PQgetvalue(res, i, 4));
        b.abandoned = std::stoull(PQgetvalue(res, i, 5));
        b.handle_ms = std::stod(PQgetvalue(res, i, 6));
        b.handled = std::stoull(PQgetvalue(res, i, 7));
        if (with10 && !PQgetisnull(res, i, 8)) b.offered_10s = parse_int_array(PQgetvalue(res, i, 8));
        rows->push_back(std::move(b));
      }
      PQclear(res);
      return true;
    });
  };
  op.finish = [rows, done = std::move(done)](bool ok) {
    if (ok) done(std::move(*rows));
  };
  return enqueue(std::move(op));
}

bool PgStore::load_models(const std::string& tenant, std::function<void(std::vector<ModelRow>)> done) {
  Op op;
  op.tenant = tenant;
  auto rows = std::make_shared<std::vector<ModelRow>>();
  op.run = [tenant, rows](void* conn) {
    rows->clear();
    return in_tenant(conn, tenant, [&] {
      PGresult* res = nullptr;
      if (!exec(conn,
                "SELECT tenant_id, version, config::text, normalization::text, sha256, bytes, written_by, status, created_at_ms "
                "FROM routing_forecast_model WHERE tenant_id = $1 AND status IN ('active', 'candidate') "
                "ORDER BY version DESC LIMIT 4",
                {tenant}, &res)) {
        return false;
      }
      for (int i = 0; i < PQntuples(res); ++i) {
        if (tenant != PQgetvalue(res, i, 0)) continue;  // never (RLS and the WHERE clause)
        ModelRow m;
        m.version = std::stoull(PQgetvalue(res, i, 1));
        m.config = PQgetvalue(res, i, 2);
        m.normalization = PQgetvalue(res, i, 3);
        m.sha256 = PQgetvalue(res, i, 4);
        size_t n = 0;
        unsigned char* b = PQunescapeBytea(reinterpret_cast<const unsigned char*>(PQgetvalue(res, i, 5)), &n);
        if (b) {
          m.bytes.assign(reinterpret_cast<const char*>(b), n);
          PQfreemem(b);
        }
        m.written_by = PQgetvalue(res, i, 6);
        m.status = PQgetvalue(res, i, 7);
        m.created_at_ms = std::stoll(PQgetvalue(res, i, 8));
        rows->push_back(std::move(m));
      }
      PQclear(res);
      return true;
    });
  };
  op.finish = [rows, done = std::move(done)](bool ok) {
    if (ok) done(std::move(*rows));
  };
  return enqueue(std::move(op));
}

bool PgStore::set_model_status(const std::string& tenant, uint64_t version, const std::string& status,
                               const std::string& shadow_json, int64_t at_ms) {
  Op op;
  op.tenant = tenant;
  op.run = [=](void* conn) {
    return in_tenant(conn, tenant, [&] {
      return exec(conn,
                  "UPDATE routing_forecast_model SET status = $3, shadow = coalesce($4::jsonb, shadow), "
                  "status_changed_at_ms = $5 WHERE tenant_id = $1 AND version = $2",
                  {tenant, std::to_string(version), status, opt(shadow_json), std::to_string(at_ms)});
    });
  };
  op.finish = [](bool) {};
  return enqueue(std::move(op));
}

bool PgStore::connect() {
  if (conn_) {
    if (PQstatus(pg(conn_)) == CONNECTION_OK) return true;
    PQfinish(pg(conn_));
    conn_ = nullptr;
  }
  PGconn* c = PQconnectdb(opts_.conninfo.c_str());
  if (PQstatus(c) != CONNECTION_OK) {
    PQfinish(c);
    return false;
  }
  conn_ = c;
  connects_.fetch_add(1);
  return true;
}

// After a restart: every open callback of the bin's tenants goes back to its tenant (restore).
void PgStore::load_open() {
  bool all = true;
  for (const auto& tenant : opts_.tenants) {
    std::vector<CallbackRecord> rows;
    const bool ok = in_tenant(conn_, tenant, [&] {
      PGresult* r = nullptr;
      if (!exec(conn_,
                "SELECT callback_id, original_interaction_id, entry_id, queue_id, number, priority, requirements::text, "
                "first_queued_at_ms, ordering_at_ms, state, attempt, attempt_interaction_id, coalesce(reservation_id::text, ''), "
                "coalesce(agent_id, ''), coalesce(outcome, ''), due_at_ms, created_at_ms, updated_at_ms "
                "FROM routing_callbacks WHERE tenant_id = $1 AND state IN ('scheduled', 'dispatched', 'claimed')",
                {tenant}, &r)) {
        return false;
      }
      for (int i = 0; i < PQntuples(r); ++i) {
        CallbackRecord c;
        c.tenant = tenant;
        int k = 0;
        c.callback_id = PQgetvalue(r, i, k++);
        c.original_interaction_id = PQgetvalue(r, i, k++);
        c.entry_id = PQgetvalue(r, i, k++);
        c.queue_id = PQgetvalue(r, i, k++);
        c.number = PQgetvalue(r, i, k++);
        c.priority = static_cast<uint32_t>(std::stoul(PQgetvalue(r, i, k++)));
        for (const auto& q : json::parse(PQgetvalue(r, i, k++))) {
          c.reqs.push_back(SkillReq{q.at("skill_id").get<std::string>(), q.at("min_level").get<uint32_t>()});
        }
        c.first_queued_at = std::stoll(PQgetvalue(r, i, k++));
        c.ordering_at = std::stoll(PQgetvalue(r, i, k++));
        c.state = PQgetvalue(r, i, k++);
        c.attempt = std::stoi(PQgetvalue(r, i, k++));
        c.attempt_interaction_id = PQgetvalue(r, i, k++);
        c.reservation_id = PQgetvalue(r, i, k++);
        c.agent_id = PQgetvalue(r, i, k++);
        c.outcome = PQgetvalue(r, i, k++);
        c.due_at = std::stoll(PQgetvalue(r, i, k++));
        c.created_at = std::stoll(PQgetvalue(r, i, k++));
        c.updated_at = std::stoll(PQgetvalue(r, i, k++));
        rows.push_back(std::move(c));
      }
      PQclear(r);
      return true;
    });
    if (!ok) {
      all = false;
      continue;
    }
    LogRec("info", "callbacks.loaded", "callbacks loaded").tenant(tenant).attr("open", static_cast<int64_t>(rows.size()));
    engine_->post(tenant, [rows = std::move(rows)](Tenant* t, int64_t now) {
      if (t) t->restore_callbacks(rows, now);
    }, /*external=*/false);
  }
  if (all) loaded_ = true;
}

// GDPR: an ended callback's number is kept only for the retention window.
void PgStore::purge() {
  const int64_t cutoff = clock_->now_ms() - opts_.retention_ms;
  std::vector<std::string> tenants = opts_.tenants;
  if (opts_.fenced) {
    tenants.clear();
    for (const auto& [t, f] : fence_) tenants.push_back(t);
  }
  for (const auto& tenant : tenants) {
    in_tenant(conn_, tenant, [&] {
      return exec(conn_,
                  "DELETE FROM routing_callbacks WHERE tenant_id = $1 AND state IN ('done', 'flagged') AND updated_at_ms < $2",
                  {tenant, std::to_string(cutoff)});
    });
  }
}

void PgStore::loop() {
  for (;;) {
    {
      std::unique_lock lk(mu_);
      if (stop_) return;
    }
    if (!conn_ || PQstatus(pg(conn_)) != CONNECTION_OK) {
      const bool was = healthy_.exchange(false);
      if (was) LogRec("warn", "dependency.unavailable", "postgres connection lost: callbacks and voicemail tasks are refused until it is back").error("unavailable").attr("dependency", "postgres");
      if (connect()) {
        healthy_ = true;
        LogRec("info", "dependency.restored", "postgres connected").attr("dependency", "postgres");
        if (!opts_.fenced && !loaded_) load_open();
        run_loads();
      } else {
        std::deque<Op> failed;
        {
          std::lock_guard lk(mu_);
          failed.swap(q_);
        }
        for (auto& op : failed) {
          failed_.fetch_add(1);
          op.finish(false);
        }
        std::unique_lock lk(mu_);
        cv_.wait_for(lk, std::chrono::seconds(1), [&] { return stop_; });
        continue;
      }
    }
    Op op;
    bool have = false;
    {
      std::unique_lock lk(mu_);
      cv_.wait_for(lk, std::chrono::milliseconds(500), [&] { return stop_ || !q_.empty(); });
      if (stop_) return;
      if (!q_.empty()) {
        op = std::move(q_.front());
        q_.pop_front();
        have = true;
      }
    }
    if (!have) {
      const int64_t now = clock_->now_ms();
      if (now >= next_purge_) {
        next_purge_ = now + 3'600'000;
        purge();
      }
      if (!opts_.fenced && !loaded_) load_open();
      run_loads();
      continue;
    }
    const bool ok = op.run(conn_);
    (ok ? ok_ : failed_).fetch_add(1);
    op.finish(ok);
  }
}

PgStore::Stats PgStore::stats() const {
  return Stats{ok_.load(), failed_.load(), refused_.load(), dropped_.load(), connects_.load()};
}

}  // namespace pcr
