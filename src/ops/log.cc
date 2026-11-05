#include "src/ops/log.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cctype>
#include <cmath>
#include <condition_variable>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "src/ops/pii.h"

namespace pcr {

namespace {

constexpr size_t kQueueMax = 10'000;
constexpr size_t kMaxLine = 16'000;     // log-record.v1: Docker's log drivers split longer lines
constexpr size_t kMaxDecision = 4'096;  // CONVENTIONS "Logging": decision
constexpr size_t kMaxAttrText = 1'024;
constexpr int kMaxAttrs = 32;

int level_rank(const char* l) {
  static const char* const names[] = {"trace", "debug", "info", "warn", "error", "fatal"};
  for (int i = 0; i < 6; ++i) {
    if (std::strcmp(l, names[i]) == 0) return i;
  }
  return 2;
}

void esc(std::string& out, const std::string& s) {
  for (unsigned char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      default:
        if (c < 0x20) {
          char b[8];
          std::snprintf(b, sizeof b, "\\u%04x", c);
          out += b;
        } else {
          out += static_cast<char>(c);
        }
    }
  }
}

void quoted(std::string& out, const std::string& s) {
  out += '"';
  esc(out, s);
  out += '"';
}

std::string hostname() {
  char buf[256] = {};
  if (gethostname(buf, sizeof buf - 1) != 0 || buf[0] == '\0') return "parity-cc-routing";
  // The schema's opaque id: letters, digits and . _ : - (at most 128).
  std::string out;
  for (const char* p = buf; *p && out.size() < 128; ++p) {
    const char c = *p;
    out += (std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '_' || c == ':' || c == '-') ? c : '-';
  }
  return out;
}

int64_t wall_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int64_t mono_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

bool is_uuid7(const std::string& s) {
  if (s.size() != 36) return false;
  for (size_t i = 0; i < 36; ++i) {
    const char c = s[i];
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (c != '-') return false;
    } else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
      return false;
    }
  }
  return s[14] == '7' && (s[19] == '8' || s[19] == '9' || s[19] == 'a' || s[19] == 'b');
}

// The process's identity in every record, fixed by log_init (or the defaults on first use).
struct Identity {
  std::string service = "parity-cc-routing";
  std::string version = kRouterVersion;
  std::string instance = hostname();
  std::string incarnation = std::to_string(wall_ms());
};

// One writer thread; started on the first line, flushed and joined at exit.
class Writer {
 public:
  void push(std::string line) {
    {
      std::lock_guard lk(mu_);
      if (!started_) {
        started_ = true;
        th_ = std::thread([this] { run(); });
      }
      if (q_.size() >= kQueueMax) {
        dropped_.fetch_add(1, std::memory_order_relaxed);
        return;
      }
      q_.push_back(std::move(line));
    }
    cv_.notify_one();
  }
  void flush() {
    std::unique_lock lk(mu_);
    if (!started_) return;
    idle_.wait(lk, [&] { return q_.empty() && !writing_; });
  }
  void set_out(std::FILE* f) {
    std::lock_guard lk(mu_);
    out_ = f;
  }
  ~Writer() {
    {
      std::lock_guard lk(mu_);
      stop_ = true;
    }
    cv_.notify_one();
    if (th_.joinable()) th_.join();
    if (out_ != stderr && out_) std::fclose(out_);
  }
  std::atomic<uint64_t> dropped_{0};
  Identity id;
  std::atomic<int> min_level{3};  // warn until log_init
  std::atomic<uint64_t> seq{0};
  std::atomic<int> decision_every{100};

 private:
  void run() {
    std::unique_lock lk(mu_);
    for (;;) {
      cv_.wait(lk, [&] { return stop_ || !q_.empty(); });
      while (!q_.empty()) {
        std::deque<std::string> batch;
        batch.swap(q_);
        writing_ = true;
        std::FILE* out = out_;
        lk.unlock();
        for (const auto& l : batch) std::fwrite(l.data(), 1, l.size(), out);
        std::fflush(out);
        lk.lock();
        writing_ = false;
      }
      idle_.notify_all();
      if (stop_) return;
    }
  }
  std::mutex mu_;
  std::condition_variable cv_, idle_;
  std::deque<std::string> q_;
  bool started_ = false, stop_ = false, writing_ = false;
  std::thread th_;
  std::FILE* out_ = stderr;
};

Writer& writer() {
  static Writer w;
  return w;
}

}  // namespace

std::string rfc3339_ms(int64_t ms) {
  const std::time_t secs = static_cast<std::time_t>(ms / 1000);
  std::tm tm{};
  gmtime_r(&secs, &tm);
  char buf[96];
  std::snprintf(buf, sizeof buf, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", (tm.tm_year + 1900) % 10000, (tm.tm_mon + 1) % 100,
                tm.tm_mday % 100, tm.tm_hour % 100, tm.tm_min % 100, tm.tm_sec % 100, static_cast<int>(ms % 1000));
  return buf;
}

void log_init(const LogInit& o) {
  Writer& w = writer();
  if (!o.instance.empty()) w.id.instance = o.instance;
  if (!o.incarnation.empty()) w.id.incarnation = o.incarnation;
  if (!o.version.empty()) w.id.version = o.version;
  w.min_level.store(level_rank(o.level.empty() ? "info" : o.level.c_str()), std::memory_order_relaxed);
  w.decision_every.store(o.decision_every < 1 ? 1 : o.decision_every, std::memory_order_relaxed);
  if (!o.path.empty()) {
    if (std::FILE* f = std::fopen(o.path.c_str(), "a")) w.set_out(f);
  }
}

int log_decision_every() { return writer().decision_every.load(std::memory_order_relaxed); }
bool log_on(const char* level) { return level_rank(level) >= writer().min_level.load(std::memory_order_relaxed); }
void log_flush() { writer().flush(); }
uint64_t log_dropped() { return writer().dropped_.load(std::memory_order_relaxed); }

LogRec::LogRec(const char* level, const char* event, const char* msg)
    : on_(log_on(level)), level_(level), event_(event), msg_(msg) {}

LogRec& LogRec::str(const char* key, const std::string& v) {
  if (!on_ || v.empty()) return *this;
  top_ += ",\"";
  top_ += key;
  top_ += "\":";
  quoted(top_, pii::mask_field(key, v));
  return *this;
}

LogRec& LogRec::num(const char* key, int64_t v) {
  if (!on_) return *this;
  top_ += ",\"";
  top_ += key;
  top_ += "\":";
  top_ += std::to_string(v < 0 ? 0 : v);
  return *this;
}

LogRec& LogRec::xsid(const std::string& ix) {
  if (!on_ || ix.empty()) return *this;
  return is_uuid7(ix) ? str("xsid", ix) : str("interaction_id", ix);
}

LogRec& LogRec::parent_xsid(const std::string& ix) {
  if (!on_ || !is_uuid7(ix)) return *this;
  return str("parent_xsid", ix);
}

LogRec& LogRec::transition(const std::string& from, const std::string& to) {
  return str("from_state", from).str("to_state", to);
}

LogRec& LogRec::duration_ms(double ms) {
  if (!on_ || !(ms >= 0) || !std::isfinite(ms)) return *this;
  char b[32];
  std::snprintf(b, sizeof b, ",\"duration_ms\":%.3f", ms);
  top_ += b;
  return *this;
}

LogRec& LogRec::error(const std::string& code, const std::string& message, const std::string& kind) {
  if (!on_) return *this;
  top_ += ",\"error\":{";
  bool first = true;
  auto put = [&](const char* k, const std::string& v) {
    if (v.empty()) return;
    if (!first) top_ += ',';
    first = false;
    top_ += '"';
    top_ += k;
    top_ += "\":";
    quoted(top_, pii::mask_field(k, v.size() > kMaxAttrText ? v.substr(0, kMaxAttrText) : v));
  };
  put("code", code.empty() ? std::string("internal") : code);
  put("kind", kind);
  put("message", message);
  top_ += '}';
  return *this;
}

LogRec& LogRec::decision(const std::string& json_object) {
  if (!on_ || json_object.size() > kMaxDecision || json_object.empty() || json_object.front() != '{') return *this;
  top_ += ",\"decision\":";
  top_ += json_object;
  return *this;
}

LogRec& LogRec::where(const char* file, int line, const char* func) {
  if (!on_) return *this;
  top_ += ",\"code\":{\"file\":";
  quoted(top_, file);
  top_ += ",\"line\":" + std::to_string(line < 1 ? 1 : line) + ",\"func\":";
  quoted(top_, func);
  top_ += '}';
  return *this;
}

LogRec& LogRec::attr(const char* key, const std::string& v) {
  if (!on_ || nattrs_ >= kMaxAttrs) return *this;
  ++nattrs_;
  if (!attrs_.empty()) attrs_ += ',';
  quoted(attrs_, key);
  attrs_ += ':';
  quoted(attrs_, pii::mask_field(key, v.size() > kMaxAttrText ? v.substr(0, kMaxAttrText) : v));
  return *this;
}

LogRec& LogRec::attr(const char* key, int64_t v) {
  if (!on_ || nattrs_ >= kMaxAttrs) return *this;
  ++nattrs_;
  if (!attrs_.empty()) attrs_ += ',';
  quoted(attrs_, key);
  attrs_ += ':';
  attrs_ += std::to_string(v);
  return *this;
}

LogRec& LogRec::attr(const char* key, double v) {
  if (!on_ || nattrs_ >= kMaxAttrs || !std::isfinite(v)) return *this;
  ++nattrs_;
  if (!attrs_.empty()) attrs_ += ',';
  quoted(attrs_, key);
  char b[40];
  std::snprintf(b, sizeof b, ":%.6g", v);
  attrs_ += b;
  return *this;
}

LogRec& LogRec::attr(const char* key, bool v) {
  if (!on_ || nattrs_ >= kMaxAttrs) return *this;
  ++nattrs_;
  if (!attrs_.empty()) attrs_ += ',';
  quoted(attrs_, key);
  attrs_ += v ? ":true" : ":false";
  return *this;
}

LogRec& LogRec::repeat(uint64_t count, int64_t since_ms) {
  if (!on_ || count == 0) return *this;
  top_ += ",\"repeat_count\":" + std::to_string(count);
  if (since_ms > 0) {
    top_ += ",\"repeat_since\":";
    quoted(top_, rfc3339_ms(since_ms));
  }
  return *this;
}

LogRec::~LogRec() {
  if (!on_) return;
  Writer& w = writer();
  const uint64_t seq = w.seq.fetch_add(1, std::memory_order_relaxed) + 1;
  std::string out;
  out.reserve(256 + top_.size() + attrs_.size());
  out += "{\"ts\":\"";
  out += rfc3339_ms(wall_ms());
  out += "\",\"level\":\"";
  out += level_;
  out += "\",\"service\":";
  quoted(out, w.id.service);
  out += ",\"version\":";
  quoted(out, w.id.version);
  out += ",\"instance\":";
  quoted(out, w.id.instance);
  out += ",\"incarnation\":";
  quoted(out, w.id.incarnation);
  out += ",\"seq\":" + std::to_string(seq) + ",\"event\":";
  quoted(out, event_);
  out += ",\"msg\":";
  quoted(out, msg_);
  out += top_;
  if (!attrs_.empty()) {
    out += ",\"attrs\":{";
    out += attrs_;
    out += '}';
  }
  out += "}\n";
  if (out.size() > kMaxLine + 1 && !attrs_.empty()) {
    // Over the line limit (never with RE's fields): the attrs go, the record stays.
    out.resize(out.size() - attrs_.size() - std::strlen(",\"attrs\":{}") - 2);
    out += ",\"attrs\":{\"truncated\":true}}\n";
  }
  if (out.size() > kMaxLine + 1) {
    w.dropped_.fetch_add(1, std::memory_order_relaxed);  // a seq gap shows it
    return;
  }
  w.push(std::move(out));
}

bool RateLimit::allow(const std::string& key, uint64_t* suppressed, int64_t* since_ms) {
  struct Slot {
    int64_t last = INT64_MIN / 2;
    uint64_t held = 0;
    int64_t first_held = 0;  // wall ms of the first record held back since the last allowed
  };
  static std::mutex mu;
  static std::unordered_map<std::string, Slot> slots;
  std::lock_guard lk(mu);
  if (slots.size() > 4096) slots.clear();  // bounded: a flood of distinct keys only costs lines
  Slot& s = slots[key];
  const int64_t now = mono_ms();
  if (now - s.last < interval_ms_) {
    if (s.held++ == 0) s.first_held = wall_ms();
    return false;
  }
  s.last = now;
  *suppressed = s.held;
  if (since_ms) *since_ms = s.held ? s.first_held : 0;
  s.held = 0;
  s.first_held = 0;
  return true;
}

}  // namespace pcr
