// Phase 6 (PLAN §5.5): one bin's leader or standby. A standby follows re.state and answers every RPC
// not_leader; the coordinator's election makes one instance the leader, which then takes over:
//   initTransactions (fences the old leader) -> a fence record per partition, read back (re.state
//   read to the end) -> the bin's incarnation bumped -> every owned tenant rebuilt from its records
//   (memory, indices, timers from absolute deadlines) -> serving; then, per tenant, Postgres' fence
//   raised and its callbacks reconciled.
// Restart is the same path from an empty memory. Stepping down never tries to recover in place:
// the process exits (code 3) and comes back as a standby (restart: on-failure), so nothing of the old
// leadership's memory survives. Tenant moves follow tp/tenants/{tenant} (ARCH §3.20).
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "src/coord/coordinator.h"
#include "src/core/engine.h"
#include "src/ops/metrics.h"
#include "src/state/kafka_state.h"
#include "src/store/pg_store.h"

namespace pcr {

class HaNode {
 public:
  struct Options {
    std::string bin;
    std::string advertise;  // this instance's gRPC address (the leader key's value)
    std::string etcd;
    std::string brokers;
    std::string topic = "re.state";
    int ttl_s = 5;
    bool first_start = false;  // an empty re.state for this bin is a new deployment (else refused)
    int64_t handover_wait_ms = 5'000;
  };
  HaNode(Options o, Engine* engine, PgStore* store, std::vector<std::pair<std::string, TenantConfig>> tenants,
         Metrics* metrics);
  ~HaNode();

  // Follows re.state and joins the election. False with *err when re.state or etcd can't be reached.
  bool start(std::string* err);
  // Serving as the leader (takeover finished, not stepping down).
  bool serving() const { return serving_.load(); }
  std::string leader() const { return coord_ ? coord_->leader() : std::string(); }
  bool caught_up() { return state_ && state_->caught_up(); }
  StateLog* log() { return state_.get(); }
  // SIGTERM: no new offers, everything committed and released, the election resigned (the standby
  // takes over at once). Then the caller stops the servers.
  void drain(int64_t timeout_ms);
  uint64_t incarnation() const { return incarnation_.load(); }
  // Tests and tools: tp/tenants/{tenant} = {bin, epoch}.
  bool move_tenant(const std::string& tenant, const std::string& bin, uint64_t epoch, std::string* err);

 private:
  void takeover(uint64_t leader_epoch);
  void step_down(const std::string& why);
  void mover_loop();
  void arrive(const std::string& tenant, uint64_t epoch);
  void leave(const std::string& tenant, const std::string& to_bin, uint64_t epoch);
  const TenantConfig* config_of(const std::string& tenant) const;
  std::string static_bin(const std::string& tenant) const;

  Options opts_;
  Engine* engine_;
  PgStore* store_;
  std::vector<std::pair<std::string, TenantConfig>> tenants_;
  Metrics* metrics_;
  std::unique_ptr<KafkaState> state_;
  std::unique_ptr<Coordinator> coord_;
  std::atomic<bool> serving_{false};
  std::atomic<bool> leading_{false};
  std::atomic<uint64_t> incarnation_{0};
  std::atomic<int64_t> takeover_ms_{0};
  std::atomic<uint64_t> moves_in_{0}, moves_out_{0};
  struct MoveEvent {
    std::string tenant;
    Coordinator::TenantAt at;
    bool deleted = false;
  };
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<MoveEvent> moves_;
  std::map<std::string, uint64_t> owned_;  // tenant -> tenant_epoch (leader only)
  bool stop_ = false;
  std::thread mover_;
};

}  // namespace pcr
