// The trainer against a real Postgres (scripts/store-it.sh; PLAN §5.9): one tenant's training never
// reads another tenant's rows (row-level security AND the WHERE clause AND a check of every row),
// the trainer's role reaches nothing else, model rows are written only by it (written_by forced by
// a trigger, the bytes/sha256/config fixed), RE's role changes status and shadow scores only, a
// rollback re-activates the previous version, and `trainer finetune` writes a candidate whose bytes
// match its sha256. Skipped without PCR_PG_CONNINFO_FILE and PCR_PG_TRAINER_CONNINFO_FILE.
#include <gtest/gtest.h>
#include <libpq-fe.h>

#include <cmath>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include "src/forecast/sha256.h"
#include "src/trainer/tenant_db.h"

namespace pcr::trainer {
namespace {

constexpr int64_t kDay = 86'400'000;
constexpr int64_t T_END = 1'772'409'600'000;  // 2026-03-02 00:00 UTC (bucket-aligned)

std::string read(const char* env) {
  const char* f = std::getenv(env);
  if (!f) return "";
  std::ifstream in(f);
  std::stringstream ss;
  ss << in.rdbuf();
  std::string s = ss.str();
  while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
  return s;
}

struct Pg {
  PGconn* c = nullptr;
  explicit Pg(const std::string& ci) : c(PQconnectdb(ci.c_str())) {}
  ~Pg() { PQfinish(c); }
  bool ok() const { return PQstatus(c) == CONNECTION_OK; }
  // One statement in a transaction with parity.tenant_id = tenant ("" = unset); the SQLSTATE on error.
  std::string run(const std::string& tenant, const std::string& sql, std::string* first = nullptr) {
    PQclear(PQexec(c, "BEGIN"));
    if (!tenant.empty()) {
      const char* p[] = {tenant.c_str()};
      PQclear(PQexecParams(c, "SELECT set_config('parity.tenant_id', $1, true)", 1, nullptr, p, nullptr, nullptr, 0));
    }
    PGresult* r = PQexec(c, sql.c_str());
    std::string state;
    const ExecStatusType st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
      const char* s = PQresultErrorField(r, PG_DIAG_SQLSTATE);
      state = s ? s : "?";
    } else if (first && PQntuples(r) > 0) {
      *first = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQclear(PQexec(c, state.empty() ? "COMMIT" : "ROLLBACK"));
    return state;
  }
};

class Trainer : public ::testing::Test {
 protected:
  void SetUp() override {
    app_ = read("PCR_PG_CONNINFO_FILE");
    tr_ = read("PCR_PG_TRAINER_CONNINFO_FILE");
    if (app_.empty() || tr_.empty()) GTEST_SKIP() << "no Postgres (scripts/store-it.sh)";
    Pg a(app_);
    ASSERT_TRUE(a.ok());
    // Four days of two queues for acme and one queue for globex (the app role writes them, as RE).
    std::string sql;
    auto add = [&](const std::string& tenant, const std::string& q, int64_t start, int per10) {
      std::string arr = "{";
      int offered = 0;
      for (int k = 0; k < 90; ++k) {
        const int c = (k * 7 + static_cast<int>(start / 900'000)) % (per10 + 1);
        offered += c;
        arr += (k ? "," : "") + std::to_string(c);
      }
      arr += "}";
      sql += "INSERT INTO routing_bucket_actuals (tenant_id, queue_id, bucket_start_ms, offered, answered, answered_in_sla, "
             "abandoned, overflowed, wait_answered_ms, handle_ms, handled, offered_10s) VALUES ('" + tenant + "', '" + q + "', " +
             std::to_string(start) + ", " + std::to_string(offered) + ", " + std::to_string(offered) + ", 0, 1, 0, 0, " +
             std::to_string(offered * 180'000.0) + ", " + std::to_string(offered) + ", '" + arr + "') ON CONFLICT DO NOTHING;";
    };
    for (int64_t b = T_END - 4 * kDay; b < T_END; b += 900'000) {
      add("acme", "sales", b, 3);
      add("acme", "support", b, 2);
    }
    ASSERT_EQ(a.run("acme", sql), "");
    sql.clear();
    for (int64_t b = T_END - 4 * kDay; b < T_END; b += 900'000) add("globex", "sales", b, 9);
    ASSERT_EQ(a.run("globex", sql), "");
  }
  std::string app_, tr_;
};

TEST_F(Trainer, OneTenantsTrainingReadsOnlyItsOwnRows) {
  TenantDb db;
  ASSERT_TRUE(db.connect(tr_));
  std::vector<QSeries> s;
  ASSERT_TRUE(db.load_series("acme", T_END - 4 * kDay, T_END, &s));
  ASSERT_EQ(s.size(), 2u);  // sales and support: acme's only (globex has a "sales" too)
  EXPECT_EQ(db.last_rows(), 2u * 4 * 96);
  for (const auto& q : s) {
    EXPECT_EQ(q.tenant, "acme");
    EXPECT_EQ(q.c10.size(), static_cast<size_t>(4 * 8640));
    EXPECT_TRUE(std::isfinite(q.c10[100]));
  }
  // The role itself: nothing without parity.tenant_id, nothing of globex under acme's, no other table.
  Pg t(tr_);
  ASSERT_TRUE(t.ok());
  std::string n;
  EXPECT_EQ(t.run("", "SELECT count(*) FROM routing_bucket_actuals", &n), "");
  EXPECT_EQ(n, "0");
  EXPECT_EQ(t.run("acme", "SELECT count(*) FROM routing_bucket_actuals WHERE tenant_id = 'globex'", &n), "");
  EXPECT_EQ(n, "0");
  EXPECT_EQ(t.run("acme", "SELECT count(*) FROM routing_bucket_actuals", &n), "");
  EXPECT_EQ(n, std::to_string(2 * 4 * 96));
  EXPECT_EQ(t.run("acme", "SELECT count(*) FROM routing_callbacks"), "42501");
  EXPECT_EQ(t.run("acme", "SELECT count(*) FROM routing_fence"), "42501");
  EXPECT_EQ(t.run("acme", "UPDATE routing_bucket_actuals SET offered = 0"), "42501");
}

TEST_F(Trainer, ModelRowsAreTheTrainersAndReCanChangeTheStatusOnly) {
  TenantDb db;
  ASSERT_TRUE(db.connect(tr_));
  ModelRow r;
  r.tenant = "acme";
  r.config = {{"arch", "nnmpc-forecaster-v1"}};
  r.normalization = nlohmann::json::object();
  r.holdout = nlohmann::json::object();
  r.bytes = std::string("\x00\x01model-bytes", 13);
  r.sha256 = fc::sha256_hex(r.bytes);
  r.created_at_ms = T_END;
  uint64_t v1 = 0, v2 = 0;
  ASSERT_TRUE(db.insert_candidate(r, &v1));
  ASSERT_TRUE(db.insert_candidate(r, &v2));
  EXPECT_EQ(v2, v1 + 1);
  std::vector<ModelRow> rows;
  ASSERT_TRUE(db.models("acme", true, &rows));
  ASSERT_GE(rows.size(), 2u);
  EXPECT_EQ(rows[0].written_by, "parity_cc_routing_trainer");
  EXPECT_EQ(rows[0].bytes, r.bytes);
  EXPECT_EQ(fc::sha256_hex(rows[0].bytes), rows[0].sha256);
  Pg a(app_), t(tr_);
  const std::string w1 = " WHERE tenant_id = 'acme' AND version = " + std::to_string(v1);
  const std::string w2 = " WHERE tenant_id = 'acme' AND version = " + std::to_string(v2);
  // RE's role: no insert, no change of the bytes or the sha256; status and shadow yes.
  EXPECT_EQ(a.run("acme", "INSERT INTO routing_forecast_model (tenant_id, version, config, normalization, sha256, bytes, "
                          "train_from_ms, train_to_ms, holdout, created_at_ms) VALUES ('acme', 999999, '{}', '{}', '" +
                              std::string(64, 'a') + "', '\\x00', 0, 0, '{}', 0)"),
            "42501");
  EXPECT_EQ(a.run("acme", "UPDATE routing_forecast_model SET bytes = '\\x01'" + w1), "42501");
  EXPECT_EQ(a.run("acme", "UPDATE routing_forecast_model SET sha256 = '" + std::string(64, 'b') + "'" + w1), "42501");
  EXPECT_EQ(a.run("acme", "UPDATE routing_forecast_model SET status = 'retired'"
                          " WHERE tenant_id = 'acme' AND status = 'active'"), "");
  EXPECT_EQ(a.run("acme", "UPDATE routing_forecast_model SET status = 'active', shadow = '{\"x\": 1}'" + w1), "");
  // The trainer may not rewrite its own bytes either (the trigger): only the status.
  EXPECT_EQ(t.run("acme", "UPDATE routing_forecast_model SET sha256 = '" + std::string(64, 'c') + "'" + w1), "42501");
  // Another tenant's rows are invisible to both.
  std::string n;
  EXPECT_EQ(a.run("globex", "SELECT count(*) FROM routing_forecast_model WHERE tenant_id = 'acme'", &n), "");
  EXPECT_EQ(n, "0");
  // A rollback: v2 active, then back to v1.
  EXPECT_EQ(a.run("acme", "UPDATE routing_forecast_model SET status = 'retired'" + w1), "");
  EXPECT_EQ(a.run("acme", "UPDATE routing_forecast_model SET status = 'active'" + w2), "");
  uint64_t from = 0, to = 0;
  ASSERT_TRUE(db.rollback("acme", &from, &to));
  EXPECT_EQ(from, v2);
  EXPECT_EQ(to, v1);
  EXPECT_EQ(a.run("acme", "SELECT status FROM routing_forecast_model" + w1, &n), "");
  EXPECT_EQ(n, "active");
  EXPECT_EQ(a.run("acme", "SELECT status FROM routing_forecast_model" + w2, &n), "");
  EXPECT_EQ(n, "retired");
  // The owner of the table (a superuser) is not the trainer: its rows would be refused by RE.
}

TEST_F(Trainer, FinetuneWritesACandidateFromTheTenantsOwnHistory) {
  const char* bin = std::getenv("PCR_TRAINER_BIN");
  const char* trainer_file = std::getenv("PCR_PG_TRAINER_CONNINFO_FILE");
  ASSERT_TRUE(bin && trainer_file);
  TenantDb db;
  ASSERT_TRUE(db.connect(tr_));
  std::vector<ModelRow> before;
  ASSERT_TRUE(db.models("acme", false, &before));
  const std::string cmd = std::string(bin) + " finetune --tenant=acme --conninfo-file=" + trainer_file +
                          " --base=model/forecast --steps=30 --days=4 --threads=2 --to-ms=" + std::to_string(T_END) +
                          " --log-level=warn > /dev/null";
  ASSERT_EQ(std::system(cmd.c_str()), 0);
  std::vector<ModelRow> after;
  ASSERT_TRUE(db.models("acme", true, &after));
  ASSERT_EQ(after.size(), before.size() + 1);
  const ModelRow& m = after.front();
  EXPECT_EQ(m.status, "candidate");
  EXPECT_EQ(m.written_by, "parity_cc_routing_trainer");
  EXPECT_EQ(fc::sha256_hex(m.bytes), m.sha256);
  EXPECT_TRUE(m.normalization.contains("sales"));
  EXPECT_TRUE(m.normalization.contains("support"));
  EXPECT_FALSE(m.normalization.contains("globex"));
  EXPECT_GT(m.holdout.value("anchors", 0), 0);
  EXPECT_EQ(m.train_to_ms, T_END);
}

}  // namespace
}  // namespace pcr::trainer
