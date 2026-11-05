// Logs in parity-contracts' log-record.v1 (0.13.0; CONVENTIONS "Logging" and "The xsid"; PLAN §5.6):
// one JSON object per line (NDJSON) on stderr, or in a file for a test's capture. Every record has
// ts (RFC 3339, ms, UTC), level, service, version, instance, incarnation, seq (per process, +1 per
// record made, dropped ones included), event (logging/events.parity-cc-routing.v1.json or the common
// catalogue) and msg (a constant per call site: values go in fields, never in the text). Numbers stay
// numbers. Every string field goes through the shared masking rules (src/ops/pii: phone-class names
// keep their last 3 digits, secret-class names are "[redacted]"). Call-scoped records carry the xsid
// (the interaction id) and tenant_id, and parent_xsid where a call caused this one.
//
// Lines are queued and written by one background thread, so a caller (a shard writer, a gRPC thread)
// never blocks on I/O; a full queue drops lines and counts them (log.dropped, a seq gap). Below the
// level set by log_init (warn until then, so tests stay quiet) a record costs one atomic load.
//
//   LogRec("info", "offer.sent", "offer sent").tenant(t).xsid(ix).str("agent_id", a).attr("timeout_ms", 15000);
#pragma once

#include <cstdint>
#include <initializer_list>
#include <string>
#include <utility>

namespace pcr {

struct LogInit {
  std::string instance;     // this process's stable name (the pod, container or replica); default: hostname
  std::string incarnation;  // a process start id; default: the start time in ms
  std::string version;      // the build; default kRouterVersion
  std::string level;        // trace|debug|info|warn|error|fatal; default info
  std::string path;         // write here (appending) instead of stderr: a test's capture
  int decision_every = 100; // agent.selected carries its decision once in this many (sample_rate 0.01)
};
// Once, at start (main, a test's capture). Records made before it use the defaults at level warn.
void log_init(const LogInit& o);
// The level's records are written (cheap: one atomic load).
bool log_on(const char* level);
// agent.selected's sampling: one decision in this many offers (log_init's decision_every).
int log_decision_every();
// Writes what is queued (before exit, before reading a capture).
void log_flush();
uint64_t log_dropped();

inline constexpr const char* kRouterVersion = "0.8.0";  // parity-cc-routing phase 8

// One record; written when it goes out of scope. Below the level every call is a no-op.
class LogRec {
 public:
  LogRec(const char* level, const char* event, const char* msg);
  ~LogRec();
  LogRec(const LogRec&) = delete;
  LogRec& operator=(const LogRec&) = delete;

  bool on() const { return on_; }
  // A top-level string field of log-record.v1 (tenant_id, agent_id, queue_id, entry_id, ...),
  // masked by its name; skipped when empty.
  LogRec& str(const char* key, const std::string& v);
  // A top-level integer field (leader_epoch, owner_epoch, attempt, ...).
  LogRec& num(const char* key, int64_t v);
  LogRec& tenant(const std::string& t) { return str("tenant_id", t); }
  // The call: `xsid` when the id is a UUIDv7 (every minter's since 0.13.0), else `interaction_id`.
  LogRec& xsid(const std::string& interaction_id);
  LogRec& parent_xsid(const std::string& interaction_id);
  LogRec& transition(const std::string& from, const std::string& to);
  LogRec& op(const std::string& rpc) { return str("op", rpc); }
  LogRec& cause(const std::string& c) { return str("cause", c); }
  LogRec& outcome(const char* o) { return str("outcome", o); }
  LogRec& duration_ms(double ms);
  // error: {code (the contract's), kind, message (masked, at most 1024 characters)}.
  LogRec& error(const std::string& code, const std::string& message = {}, const std::string& kind = {});
  // A routing decision, already a JSON object (at most 4096 bytes; a larger one is dropped).
  LogRec& decision(const std::string& json_object);
  // Where the record was made (C++ __FILE__/__LINE__): use PCR_HERE.
  LogRec& where(const char* file, int line, const char* func);
  // The event's own fields (attrs), masked like everything else; at most 32 keys.
  LogRec& attr(const char* key, const std::string& v);
  LogRec& attr(const char* key, const char* v) { return attr(key, std::string(v)); }
  LogRec& attr(const char* key, int64_t v);
  LogRec& attr(const char* key, int v) { return attr(key, static_cast<int64_t>(v)); }
  LogRec& attr(const char* key, uint64_t v) { return attr(key, static_cast<int64_t>(v)); }
  LogRec& attr(const char* key, double v);
  LogRec& attr(const char* key, bool v);
  // A rate limiter's held-back records: repeat_count and repeat_since (the first one's time).
  LogRec& repeat(uint64_t count, int64_t since_ms);

 private:
  bool on_ = false;
  const char* level_;
  const char* event_;
  const char* msg_;
  std::string top_, attrs_;
  int nattrs_ = 0;
};

#define PCR_HERE __FILE__, __LINE__, __func__

// At most one record per key per interval (CR-069: refusals, slow consumers). An allowed record
// reports the ones held back since the last: `suppressed`, and `since_ms` the first one's wall time
// (0 when none), for LogRec::repeat.
class RateLimit {
 public:
  explicit RateLimit(int64_t interval_ms = 1000) : interval_ms_(interval_ms) {}
  bool allow(const std::string& key, uint64_t* suppressed, int64_t* since_ms = nullptr);

 private:
  int64_t interval_ms_;
};

// RFC 3339 UTC with milliseconds (log-record.v1's ts).
std::string rfc3339_ms(int64_t epoch_ms);

}  // namespace pcr
