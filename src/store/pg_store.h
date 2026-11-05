// parity-cc-routing's Postgres (PLAN §8 row 3): callbacks, voicemail tasks, routing_bucket_actuals.
// One thread owns the connection; tenant writers only queue work (bounded) and get the result later
// on their own writer (posted through the Engine), so the real-time path never waits on Postgres.
// Postgres down or the queue full: the write is refused at once (new callbacks are refused, calls
// carry on). Every statement runs in a transaction with parity.tenant_id set (row-level security)
// and a tenant_id predicate. The connection string is read from a file and never logged.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "src/core/engine.h"
#include "src/core/tenant.h"

namespace pcr {

class PgStore final : public Durable {
 public:
  struct Options {
    std::string conninfo;          // libpq conninfo (secret: never logged)
    std::vector<std::string> tenants;  // the bin's tenants: open callbacks are loaded for these
    size_t max_queued = 10'000;
    int64_t retention_ms = 30LL * 86'400'000;  // done/flagged callbacks (their numbers) kept this long
    // Phase 6 (HA): callback writes are fenced per tenant (db/V2, routing_fence) and the open
    // callbacks are loaded per tenant when the tenant is acquired (load_tenant), not at connect.
    bool fenced = false;
    // Phases 5b+5c: write and read routing_bucket_actuals.offered_10s (db/V3). Off: phase 8's
    // statements exactly (a database without V3 keeps working).
    bool offered_10s = false;
  };
  PgStore(Options opts, Engine* engine, const Clock* clock);
  ~PgStore() override;
  PgStore(const PgStore&) = delete;
  PgStore& operator=(const PgStore&) = delete;

  void start();
  // Fails what is still queued (each `done` gets ok = false) and stops the thread.
  void stop();

  // Phase 6: raises the tenant's fence to (tenant_epoch, leader_epoch), then reads its open
  // callbacks and hands them to Tenant::reconcile_callbacks, in one transaction. Retried until it
  // succeeds (Postgres down: later); refused for good when a newer owner holds the fence.
  void load_tenant(const std::string& tenant, uint64_t tenant_epoch, uint64_t leader_epoch);
  // Phase 6: the tenant moved away: no more writes for it from this process.
  void forget_tenant(const std::string& tenant);

  bool put_callback(const CallbackRecord& r, Done done) override;
  bool put_task(const TaskRecord& r, TaskDone done) override;
  void put_buckets(std::vector<BucketRow> rows) override;
  // Phase 5 (PLAN §5.3): the tenant's routing_bucket_actuals since `since_ms` for the forecaster's
  // (weekday, 15-min slot) profile, read with the tenant's row-level security. `done` runs on the DB
  // thread (it must not block; the optimizer only queues the rows). Best effort: refused when the
  // store is down or busy (the forecaster then runs on its EWMA alone).
  bool load_actuals(const std::string& tenant, int64_t since_ms, std::function<void(std::vector<BucketRow>)> done);
  // Phases 5b+5c (db/V3): the tenant's active and candidate forecaster models (newest first), bytes
  // included; `done` runs on the DB thread (it must not block). And a status change by RE (promotion,
  // rejection, demotion): status and shadow scores only (RE's role may change nothing else).
  struct ModelRow {
    uint64_t version = 0;
    std::string config, normalization, sha256, bytes, written_by, status;
    int64_t created_at_ms = 0;
  };
  bool load_models(const std::string& tenant, std::function<void(std::vector<ModelRow>)> done);
  bool set_model_status(const std::string& tenant, uint64_t version, const std::string& status,
                        const std::string& shadow_json, int64_t at_ms);

  bool healthy() const { return healthy_.load(); }
  // Phase 6: fenced loads done (load_tenant), for tests and metrics.
  uint64_t loads() const { return loads_.load(); }
  // True once the open callbacks of every tenant were loaded and handed to the tenants.
  bool loaded() const { return loaded_.load(); }
  struct Stats {
    uint64_t ok = 0, failed = 0, refused = 0, dropped_buckets = 0, connects = 0;
  };
  Stats stats() const;

 private:
  struct Op {
    std::string tenant;
    std::function<bool(void* conn)> run;      // on the DB thread
    std::function<void(bool ok)> finish;      // posts the result to the tenant's writer
  };
  bool enqueue(Op op);
  void loop();
  bool connect();
  void load_open();
  void purge();
  void run_loads();
  bool load_one(const std::string& tenant, uint64_t tenant_epoch, uint64_t leader_epoch, bool* stale);

  Options opts_;
  Engine* engine_;
  const Clock* clock_;
  void* conn_ = nullptr;  // PGconn*, DB thread only
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Op> q_;
  bool stop_ = false;
  std::thread th_;
  std::atomic<bool> healthy_{false};
  std::atomic<bool> loaded_{false};
  std::atomic<uint64_t> ok_{0}, failed_{0}, refused_{0}, dropped_{0}, connects_{0}, loads_{0};
  int64_t next_purge_ = 0;
  // Phase 6. pending_loads_ under mu_; fence_ on the DB thread only (forget_tenant goes through
  // forgotten_, under mu_).
  std::map<std::string, std::pair<uint64_t, uint64_t>> pending_loads_;
  std::set<std::string> forgotten_;
  std::map<std::string, std::pair<uint64_t, uint64_t>> fence_;
};

}  // namespace pcr
