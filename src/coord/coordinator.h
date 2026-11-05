// Coordination (PLAN §5.1 "Coordination", §5.5 "Leader per bin", "Tenant moves"): one etcd lease
// (TTL 5 s, keep-alive every 1 s), the election of `re/{bin}/leader` = this instance's gRPC address,
// and the watch of `tp/tenants/`. Everything here runs on its own threads and talks to etcd through
// its v3 gRPC API (vendored protos, our grpc); the real-time path never waits on etcd: it only reads
// may_lead(), an atomic comparison with the lease's step-down deadline.
//
// leader_epoch = the leader key's create revision (unique and rising across the etcd cluster).
// Step-down: when no keep-alive has been answered within TTL - 1.5 s, measured on the monotonic
// clock from when that request was SENT, may_lead() turns false and `lost` runs; the leader then
// stops at once (the state log releases nothing more). A process that paused (SIGSTOP, a long GC
// pause) sees the same thing as soon as it runs again.
#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace grpc {
class Channel;
class ClientContext;
}  // namespace grpc

namespace pcr {

class Coordinator {
 public:
  struct Options {
    std::string endpoint;   // etcd's gRPC address (host:port)
    std::string bin;
    std::string advertise;  // this instance's gRPC address: the leader key's value (parity-leader)
    int ttl_s = 5;
    int64_t keepalive_ms = 1000;
    int64_t margin_ms = 1500;
  };
  struct TenantAt {
    std::string bin;
    uint64_t epoch = 0;
  };
  struct Hooks {
    // This process won the election (on the election thread; the takeover runs there).
    std::function<void(uint64_t leader_epoch)> won;
    // This process must stop leading (lease gone, key gone or replaced). Called at most once.
    std::function<void(const std::string& why)> lost;
    // The current leader's address ("" while there is none).
    std::function<void(const std::string& leader)> leader_changed;
    // tp/tenants/{tenant} changed (deleted: the static assignment applies again).
    std::function<void(const std::string& tenant, const TenantAt& at, bool deleted)> tenant_changed;
  };
  Coordinator(Options o, Hooks h);
  ~Coordinator();
  Coordinator(const Coordinator&) = delete;
  Coordinator& operator=(const Coordinator&) = delete;

  // Connects, reads tp/tenants/ (tenants()), grants the lease and starts the keep-alive, the
  // election and the watches. False with *err when etcd can't be reached.
  bool start(std::string* err);
  // resign: deletes the leader key if it is still ours and revokes the lease (SIGTERM: the standby
  // takes over at once instead of after the TTL).
  void stop(bool resign);

  // Thread-safe, lock-free: this process holds the leadership and the lease surely hasn't expired.
  bool may_lead() const;
  bool leading() const { return leading_.load(); }
  uint64_t leader_epoch() const { return epoch_.load(); }
  std::string leader() const;
  // Milliseconds left before the step-down deadline (negative: past).
  int64_t lease_left_ms() const;
  std::map<std::string, TenantAt> tenants() const;
  // Writes a key (tests and tools: a tenant move's tp/tenants/{tenant}).
  bool put(const std::string& key, const std::string& value, std::string* err);
  uint64_t keepalives() const { return keepalives_.load(); }
  uint64_t stepdowns() const { return stepdowns_.load(); }

 private:
  void keepalive_loop();
  void election_loop();
  void tenants_loop();
  bool campaign(std::string* holder, int64_t* rev);
  void watch_leader(int64_t from_rev);
  void lose(const std::string& why);
  void set_leader(const std::string& who);
  static bool parse_tenant(const std::string& value, TenantAt* out);

  Options opts_;
  Hooks hooks_;
  std::shared_ptr<grpc::Channel> ch_;
  std::atomic<int64_t> lease_{0};
  std::atomic<int64_t> deadline_{0};  // monotonic ms
  std::atomic<bool> leading_{false};
  std::atomic<bool> lost_{false};
  std::atomic<uint64_t> epoch_{0};
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> keepalives_{0}, stepdowns_{0};
  mutable std::mutex mu_;
  std::string leader_;
  std::map<std::string, TenantAt> tenants_;
  int64_t tenants_rev_ = 0;
  std::mutex ctx_mu_;
  grpc::ClientContext* ka_ctx_ = nullptr;
  grpc::ClientContext* el_ctx_ = nullptr;
  grpc::ClientContext* tw_ctx_ = nullptr;
  std::thread ka_th_, el_th_, tw_th_;
};

}  // namespace pcr
