#include "src/trainer/tenant_db.h"

#include <libpq-fe.h>

#include <chrono>
#include <cmath>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <sstream>

#include "src/forecast/features.h"
#include "src/ops/log.h"

namespace pcr::trainer {

namespace {

using Param = std::optional<std::string>;
PGconn* pg(void* c) { return static_cast<PGconn*>(c); }

bool exec(void* conn, const char* sql, const std::vector<Param>& params, PGresult** rows = nullptr,
          const std::vector<int>& binary = {}) {
  std::vector<const char*> v;
  std::vector<int> len, fmt;
  for (size_t i = 0; i < params.size(); ++i) {
    v.push_back(params[i] ? params[i]->c_str() : nullptr);
    const bool bin = i < binary.size() && binary[i];
    len.push_back(bin && params[i] ? static_cast<int>(params[i]->size()) : 0);
    fmt.push_back(bin ? 1 : 0);
  }
  PGresult* r = PQexecParams(pg(conn), sql, static_cast<int>(v.size()), nullptr, v.data(), len.data(), fmt.data(), 0);
  const ExecStatusType st = PQresultStatus(r);
  const bool ok = st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK;
  if (!ok) {
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

bool in_tenant(void* conn, const std::string& tenant, const std::function<bool()>& body) {
  if (!conn || !exec(conn, "BEGIN", {})) return false;
  if (!exec(conn, "SELECT set_config('parity.tenant_id', $1, true)", {tenant}) || !body()) {
    exec(conn, "ROLLBACK", {});
    return false;
  }
  return exec(conn, "COMMIT", {});
}

// A Postgres integer[] in text form ("{1,2,NULL,3}").
std::vector<double> int_array(const char* s) {
  std::vector<double> out;
  if (!s || *s != '{') return out;
  std::string cur;
  for (const char* p = s + 1; *p; ++p) {
    if (*p == ',' || *p == '}') {
      out.push_back(cur == "NULL" || cur.empty() ? std::nan("") : std::stod(cur));
      cur.clear();
      if (*p == '}') break;
    } else {
      cur += *p;
    }
  }
  return out;
}

std::string unescape_bytea(const char* s) {
  size_t n = 0;
  unsigned char* b = PQunescapeBytea(reinterpret_cast<const unsigned char*>(s), &n);
  std::string out(reinterpret_cast<const char*>(b), n);
  PQfreemem(b);
  return out;
}

}  // namespace

TenantDb::~TenantDb() {
  if (conn_) PQfinish(pg(conn_));
}

bool TenantDb::connect_file(const std::string& path) {
  std::ifstream in(path);
  if (!in) return false;
  std::stringstream ss;
  ss << in.rdbuf();
  std::string c = ss.str();
  while (!c.empty() && (c.back() == '\n' || c.back() == '\r')) c.pop_back();
  return connect(c);
}

bool TenantDb::connect(const std::string& conninfo) {
  if (conn_) PQfinish(pg(conn_));
  PGconn* c = PQconnectdb(conninfo.c_str());
  if (PQstatus(c) != CONNECTION_OK) {
    PQfinish(c);
    conn_ = nullptr;
    return false;
  }
  conn_ = c;
  return true;
}

bool TenantDb::load_series(const std::string& tenant, int64_t from_ms, int64_t to_ms, std::vector<QSeries>* out) {
  out->clear();
  last_rows_ = 0;
  if (to_ms <= from_ms) return false;
  const int64_t steps = (to_ms - from_ms) / fc::kStepMs;
  const int64_t nb = (to_ms - from_ms) / fc::kBucketMs;
  const float nan = std::numeric_limits<float>::quiet_NaN();
  std::map<std::string, QSeries> qs;
  bool foreign = false;
  const bool ok = in_tenant(conn_, tenant, [&] {
    PGresult* res = nullptr;
    if (!exec(conn_,
              "SELECT tenant_id, queue_id, bucket_start_ms, offered, abandoned, handled, handle_ms, offered_10s "
              "FROM routing_bucket_actuals WHERE tenant_id = $1 AND bucket_start_ms >= $2 AND bucket_start_ms < $3 "
              "ORDER BY queue_id, bucket_start_ms",
              {tenant, std::to_string(from_ms), std::to_string(to_ms)}, &res)) {
      return false;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
      if (tenant != PQgetvalue(res, i, 0)) {
        foreign = true;  // never: RLS and the WHERE clause both say so
        break;
      }
      ++last_rows_;
      const std::string queue = PQgetvalue(res, i, 1);
      QSeries& q = qs[queue];
      if (q.c10.empty()) {
        q.tenant = tenant;
        q.queue = queue;
        q.start_ms = from_ms;
        q.c10.assign(static_cast<size_t>(steps), nan);
        q.b_off.assign(static_cast<size_t>(nb), nan);
        q.b_ab.assign(static_cast<size_t>(nb), nan);
        q.b_aht.assign(static_cast<size_t>(nb), nan);
      }
      const int64_t start = std::stoll(PQgetvalue(res, i, 2));
      const int64_t b = (start - from_ms) / fc::kBucketMs;
      if (b < 0 || b >= nb) continue;
      const double offered = std::stod(PQgetvalue(res, i, 3)), ab = std::stod(PQgetvalue(res, i, 4));
      const double handled = std::stod(PQgetvalue(res, i, 5)), handle_ms = std::stod(PQgetvalue(res, i, 6));
      q.b_off[static_cast<size_t>(b)] = static_cast<float>(offered);
      q.b_ab[static_cast<size_t>(b)] = static_cast<float>(ab);
      q.b_aht[static_cast<size_t>(b)] = handled > 0 ? static_cast<float>(handle_ms / 1000.0 / handled) : 0.0f;
      if (!PQgetisnull(res, i, 7)) {
        const std::vector<double> c = int_array(PQgetvalue(res, i, 7));
        for (size_t k = 0; k < c.size() && k < static_cast<size_t>(fc::kStepsPerBucket); ++k) {
          const int64_t s = b * fc::kStepsPerBucket + static_cast<int64_t>(k);
          if (s < steps) q.c10[static_cast<size_t>(s)] = static_cast<float>(c[k]);
        }
      }
    }
    PQclear(res);
    return !foreign;
  });
  if (!ok || foreign) {
    out->clear();
    return false;
  }
  for (auto& [id, q] : qs) {
    // The first step with data (a queue that appears later starts there).
    int64_t first = 0;
    while (first < steps && !std::isfinite(q.c10[static_cast<size_t>(first)])) ++first;
    q.first_step = first < steps ? first : 0;
    q.holiday.assign(static_cast<size_t>(nb), 0);
    q.event.assign(static_cast<size_t>(nb), 0);
    out->push_back(std::move(q));
  }
  return true;
}

bool TenantDb::insert_candidate(const ModelRow& r, uint64_t* version) {
  bool done = false;
  const bool ok = in_tenant(conn_, r.tenant, [&] {
    PGresult* res = nullptr;
    if (!exec(conn_,
              "INSERT INTO routing_forecast_model (tenant_id, version, config, normalization, sha256, bytes, base_sha256, "
              "train_from_ms, train_to_ms, holdout, status, created_at_ms) "
              "SELECT $1, coalesce(max(version), 0) + 1, $2::jsonb, $3::jsonb, $4, $5::bytea, $6, $7, $8, $9::jsonb, "
              "'candidate', $10 FROM routing_forecast_model WHERE tenant_id = $1 RETURNING version",
              {r.tenant, r.config.dump(), r.normalization.dump(), r.sha256, r.bytes, r.base_sha256,
               std::to_string(r.train_from_ms), std::to_string(r.train_to_ms), r.holdout.dump(),
               std::to_string(r.created_at_ms)},
              &res, {0, 0, 0, 0, 1})) {
      return false;
    }
    if (PQntuples(res) == 1) {
      *version = std::stoull(PQgetvalue(res, 0, 0));
      done = true;
    }
    PQclear(res);
    return done;
  });
  return ok && done;
}

bool TenantDb::rollback(const std::string& tenant, uint64_t* from, uint64_t* to) {
  bool found = false;
  const bool ok = in_tenant(conn_, tenant, [&] {
    PGresult* res = nullptr;
    if (!exec(conn_,
              "SELECT version FROM routing_forecast_model WHERE tenant_id = $1 AND status = 'active' FOR UPDATE", {tenant},
              &res)) {
      return false;
    }
    if (PQntuples(res) != 1) {
      PQclear(res);
      return false;
    }
    *from = std::stoull(PQgetvalue(res, 0, 0));
    PQclear(res);
    if (!exec(conn_,
              "SELECT version FROM routing_forecast_model WHERE tenant_id = $1 AND status = 'retired' AND version < $2 "
              "ORDER BY version DESC LIMIT 1 FOR UPDATE",
              {tenant, std::to_string(*from)}, &res)) {
      return false;
    }
    if (PQntuples(res) != 1) {
      PQclear(res);
      return false;
    }
    *to = std::stoull(PQgetvalue(res, 0, 0));
    PQclear(res);
    const std::string now = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    if (!exec(conn_,
              "UPDATE routing_forecast_model SET status = 'retired', status_changed_at_ms = $3 "
              "WHERE tenant_id = $1 AND version = $2",
              {tenant, std::to_string(*from), now})) {
      return false;
    }
    if (!exec(conn_,
              "UPDATE routing_forecast_model SET status = 'active', status_changed_at_ms = $3 "
              "WHERE tenant_id = $1 AND version = $2",
              {tenant, std::to_string(*to), now})) {
      return false;
    }
    found = true;
    return true;
  });
  return ok && found;
}

bool TenantDb::models(const std::string& tenant, bool with_bytes, std::vector<ModelRow>* out) {
  out->clear();
  return in_tenant(conn_, tenant, [&] {
    PGresult* res = nullptr;
    const std::string sql = std::string("SELECT tenant_id, version, config, normalization, sha256, ") +
                            (with_bytes ? "bytes" : "''::bytea") +
                            ", base_sha256, train_from_ms, train_to_ms, holdout, status, shadow, written_by, created_at_ms, "
                            "status_changed_at_ms FROM routing_forecast_model WHERE tenant_id = $1 ORDER BY version DESC";
    if (!exec(conn_, sql.c_str(), {tenant}, &res)) return false;
    for (int i = 0; i < PQntuples(res); ++i) {
      if (tenant != PQgetvalue(res, i, 0)) {
        PQclear(res);
        out->clear();
        return false;
      }
      ModelRow m;
      m.tenant = tenant;
      m.version = std::stoull(PQgetvalue(res, i, 1));
      m.config = nlohmann::json::parse(PQgetvalue(res, i, 2), nullptr, false);
      m.normalization = nlohmann::json::parse(PQgetvalue(res, i, 3), nullptr, false);
      m.sha256 = PQgetvalue(res, i, 4);
      m.bytes = unescape_bytea(PQgetvalue(res, i, 5));
      m.base_sha256 = PQgetisnull(res, i, 6) ? "" : PQgetvalue(res, i, 6);
      m.train_from_ms = std::stoll(PQgetvalue(res, i, 7));
      m.train_to_ms = std::stoll(PQgetvalue(res, i, 8));
      m.holdout = nlohmann::json::parse(PQgetvalue(res, i, 9), nullptr, false);
      m.status = PQgetvalue(res, i, 10);
      m.shadow = nlohmann::json::parse(PQgetvalue(res, i, 11), nullptr, false);
      m.written_by = PQgetvalue(res, i, 12);
      m.created_at_ms = std::stoll(PQgetvalue(res, i, 13));
      m.status_changed_at_ms = std::stoll(PQgetvalue(res, i, 14));
      out->push_back(std::move(m));
    }
    PQclear(res);
    return true;
  });
}

}  // namespace pcr::trainer
