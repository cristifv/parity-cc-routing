// The scenario runner as a library (PLAN §7.1): plays one routing-scenario.v1 document against the
// real engine (gRPC services, test control, in-process tap) and returns the report with every
// invariant's violations and exercise counts. Used by the canonical set, the regression replays,
// the full-scale runs and the invariant search.
#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>

namespace pcr::scenario {

struct Fault {
  std::string kind;  // drop_entry, unindex_agent, no_owner_loss (Tenant::test_fault)
  int64_t at_ms = -1;
};

struct RunOptions {
  Fault fault;
  int shards = 2;
  // CR-049: one shard and counter-based ids, so a scenario replays bit for bit (the invariant
  // search and its shrinking depend on it).
  bool deterministic = false;
  bool model_ewt = false;          // the engine with --model-ewt on
  bool position_throttle = true;   // CR-056's PositionUpdate throttle (off: the before measurement)
  int64_t check_every_ms = 1000;   // the per-step checks (I1, I3-I5); full-scale runs thin them
  double decision_p99_ms = -1;     // I9 per PLAN §6.1 (offer decision inside the writer); < 0: report only
  double decision_max_ms = -1;
};

nlohmann::json run(const nlohmann::json& scenario, const RunOptions& o);

}  // namespace pcr::scenario
