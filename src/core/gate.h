// Phase 6 (PLAN §5.5): "every change is a record appended before its effect is visible outside".
// A shard writer runs each task with a Batch current on its thread: the tenants append the records
// of what the task changed, and everything the task would make visible (Watch messages, RPC
// answers, agent.events, Postgres writes) is queued in the batch as an effect instead of happening.
// The state log commits the batch's records in one Kafka transaction and only then runs its effects,
// in order. Without a state log (phase 1, unit tests) no batch is current and effects run at once.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace pcr {

// One re.state record: key `{tenant}/{kind}/{id}` (or a `~` bin-wide key); no value = a tombstone.
struct StateRec {
  std::string tenant;  // the partition key (empty for bin-wide keys: the key itself)
  std::string key;
  std::optional<std::string> value;
};

// Phase 5 (p6 review finding 6, CR-137): the largest record value a tenant may write (Kafka's message
// limit is 1 MB). A task whose records exceed it is refused for its tenant alone: its records and
// effects are dropped and the tenant is quarantined (UNAVAILABLE) until it is rebuilt from re.state;
// the bin never steps down for it.
constexpr size_t kMaxRecordBytes = 900'000;

struct Batch {
  std::vector<StateRec> records;
  std::vector<std::function<void()>> effects;
  bool empty() const { return records.empty() && effects.empty(); }
};

namespace gate {
// The batch of the task running on this thread (null: no state log, effects run at once).
Batch* current();
void set_current(Batch* b);
// Runs fn now, or after the current batch's records are committed.
void after_commit(std::function<void()> fn);
}  // namespace gate

// Where shard writers hand their batches. submit never blocks; the log commits batches in the order
// they were submitted (across shards too) and runs each batch's effects after its commit.
class StateLog {
 public:
  virtual ~StateLog() = default;
  virtual void submit(Batch&& b) = 0;
  // Records submitted and not yet committed (shards stop taking work above a limit: backpressure).
  virtual size_t backlog() const = 0;
};

}  // namespace pcr
