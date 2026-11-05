// re.state on Redpanda (PLAN §5.5, phase 6).
//
// The state log: shard writers submit batches (records + effects); one commit thread puts every
// batch queued so far into ONE Kafka transaction (group commit, at most kMaxRecords records), commits
// it, then runs the batches' effects in submit order. Before each commit and again before releasing
// effects it asks the coordinator whether this process may still act as leader (the lease's
// step-down deadline): if not, or if a commit is fenced or fails 3 times, it steps down at once and
// nothing of the batch becomes visible. The producer's transactional.id is
// `parity-cc-routing.<bin>`: a new leader's initTransactions fences the old leader's producer, so
// nothing the old one commits afterwards counts.
//
// The follower: one consumer (read_committed) reads every partition from the start and keeps the
// latest value per key (a standby's catch-up, a restart's rebuild, a tenant's arrival). Per tenant it
// keeps only the records of the newest tenant_epoch it has seen. Keys go to partition
// fnv1a(tenant) % partitions, so a tenant's records stay in order (its handover marker comes after
// everything the leaving bin wrote for it).
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "src/core/gate.h"

namespace pcr {

class KafkaState final : public StateLog {
 public:
  struct Options {
    std::string brokers;
    std::string topic = "re.state";
    std::string bin;
    size_t max_records_per_txn = 1000;
  };
  // may_lead: asked before each commit and before releasing effects (false: the lease is gone).
  // step_down: called once (from the commit thread) when this process must stop acting as leader.
  KafkaState(Options o, std::function<bool()> may_lead, std::function<void(const std::string&)> step_down);
  ~KafkaState() override;
  KafkaState(const KafkaState&) = delete;
  KafkaState& operator=(const KafkaState&) = delete;

  // Starts the follower (reads the topic from its start). False with *err when the topic can't be
  // read (missing, unreachable).
  bool start_follower(std::string* err);
  // A standby has caught up: its position is at the end of every partition (approximate).
  bool caught_up();
  // The takeover: initTransactions (fences the previous leader), a fence record per partition, and
  // a wait until the follower has read them all (the topic read to the end). Then the commit thread
  // runs. False with *err on failure (the caller steps down).
  bool become_leader(uint64_t leader_epoch, int64_t timeout_ms, std::string* err);
  // After become_leader and the takeover's own commit_now calls: the group commit starts (batches
  // submitted meanwhile wait for it).
  void start_commits();
  // Commits records at once, outside the group commit (the takeover's meta record). Leader only.
  bool commit_now(const std::vector<StateRec>& recs, std::string* err);
  // Waits until every submitted batch is committed and its effects ran (drain), at most timeout.
  bool flush(int64_t timeout_ms);
  void stop();

  // The follower's view.
  std::vector<std::pair<std::string, std::string>> records_of(const std::string& tenant) const;
  std::optional<std::string> get(const std::string& key) const;
  uint64_t tenant_epoch_seen(const std::string& tenant) const;
  uint64_t consumed() const { return consumed_.load(); }

  // StateLog
  void submit(Batch&& b) override;
  size_t backlog() const override { return backlog_.load(std::memory_order_relaxed); }

  struct Stats {
    uint64_t commits = 0, records = 0, aborts = 0, retries = 0;
    uint64_t commit_ms_buckets[16] = {};  // log2 buckets of milliseconds (b: < 2^b ms)
    uint64_t commit_ms_max = 0;
  };
  Stats stats() const;
  int partitions() const { return partitions_; }
  int partition_of(const std::string& tenant_or_key) const;

 private:
  struct Entry {
    uint64_t epoch = 0;
    std::string value;
  };
  void commit_loop();
  void follow_loop();
  bool produce(void* rk, const StateRec& r, std::string* err);
  bool commit_records(const std::vector<const StateRec*>& recs, std::string* err);
  void apply(int partition, const std::string& key, const char* value, size_t len);
  void fail(const std::string& why);

  Options opts_;
  std::function<bool()> may_lead_;
  std::function<void(const std::string&)> step_down_;
  void* producer_ = nullptr;  // rd_kafka_t*
  void* consumer_ = nullptr;  // rd_kafka_t*
  int partitions_ = 0;

  mutable std::mutex mu_;
  std::condition_variable cv_, idle_cv_;
  std::deque<Batch> q_;
  bool stop_ = false;
  bool leading_ = false;
  bool busy_ = false;
  std::atomic<bool> failed_{false};
  std::atomic<size_t> backlog_{0};
  std::thread commit_th_, follow_th_;

  mutable std::mutex cache_mu_;
  std::condition_variable fence_cv_;
  std::unordered_map<std::string, Entry> cache_;
  std::unordered_map<std::string, uint64_t> epoch_of_;  // tenant -> newest tenant_epoch seen
  std::map<int, std::string> fence_seen_;               // partition -> nonce of our bin's last fence
  std::atomic<uint64_t> consumed_{0};
  std::atomic<bool> follow_stop_{false};

  mutable std::mutex stats_mu_;
  Stats stats_;
};

}  // namespace pcr
