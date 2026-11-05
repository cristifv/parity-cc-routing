// The trainer's Postgres access (PLAN §5.9): one tenant per run, under the trainer's role
// (parity_cc_routing_trainer: SELECT routing_bucket_actuals, SELECT/INSERT routing_forecast_model,
// UPDATE(status) only). Every statement runs in a transaction that sets parity.tenant_id (row-level
// security) AND names the tenant in its WHERE clause; every row read is checked to be the tenant's
// (a row of another tenant fails the load: defence in depth behind RLS).
#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "src/trainer/data.h"

namespace pcr::trainer {

struct ModelRow {
  std::string tenant;
  uint64_t version = 0;
  nlohmann::json config, normalization, holdout, shadow;
  std::string sha256, bytes, base_sha256, status, written_by;
  int64_t train_from_ms = 0, train_to_ms = 0, created_at_ms = 0, status_changed_at_ms = 0;
};

class TenantDb {
 public:
  TenantDb() = default;
  ~TenantDb();
  TenantDb(const TenantDb&) = delete;
  TenantDb& operator=(const TenantDb&) = delete;

  // The libpq conninfo from a file (mode 600 expected; the secret is never logged).
  bool connect_file(const std::string& path);
  bool connect(const std::string& conninfo);

  // The tenant's routing_bucket_actuals in [from_ms, to_ms) as one series per queue (10 s steps from
  // offered_10s; NaN where a bucket is missing or predates V3).
  bool load_series(const std::string& tenant, int64_t from_ms, int64_t to_ms, std::vector<QSeries>* out);
  // Inserts a `candidate` row (version = the tenant's highest + 1); *version gets it.
  bool insert_candidate(const ModelRow& r, uint64_t* version);
  // Re-activates the previous model: the active one retires, the newest retired one older than it
  // becomes active (one transaction). False when there is nothing to roll back to.
  bool rollback(const std::string& tenant, uint64_t* from, uint64_t* to);
  // The tenant's model rows (newest first), bytes included only when `with_bytes`.
  bool models(const std::string& tenant, bool with_bytes, std::vector<ModelRow>* out);

  // Tests: rows the last load_series read (all of them the tenant's, or the load failed).
  uint64_t last_rows() const { return last_rows_; }

 private:
  void* conn_ = nullptr;  // PGconn*
  uint64_t last_rows_ = 0;
};

}  // namespace pcr::trainer
