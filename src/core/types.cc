#include "src/core/types.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>

namespace pcr {

int64_t SystemClock::now_ms() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

namespace {
bool lower_alnum(char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); }
bool alnum(char c) { return lower_alnum(c) || (c >= 'A' && c <= 'Z'); }
bool hex(char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }
}  // namespace

// ^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$ (CONVENTIONS: a DNS label)
bool valid_tenant_id(const std::string& s) {
  if (s.empty() || s.size() > 63) return false;
  if (!lower_alnum(s.front()) || !lower_alnum(s.back())) return false;
  for (char c : s) {
    if (!lower_alnum(c) && c != '-') return false;
  }
  return true;
}

// Lowercase canonical RFC 9562 text.
bool valid_uuid(const std::string& s) {
  if (s.size() != 36) return false;
  for (size_t i = 0; i < s.size(); ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (s[i] != '-') return false;
    } else if (!hex(s[i])) {
      return false;
    }
  }
  return true;
}

// ^[A-Za-z0-9_.-]{1,64}$
bool valid_queue_id(const std::string& s) {
  if (s.empty() || s.size() > 64) return false;
  for (char c : s) {
    if (!alnum(c) && c != '_' && c != '.' && c != '-') return false;
  }
  return true;
}

// ^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$
bool valid_agent_id(const std::string& s) {
  if (s.empty() || s.size() > 128 || !alnum(s.front())) return false;
  for (char c : s) {
    if (!alnum(c) && c != '.' && c != '_' && c != ':' && c != '-') return false;
  }
  return true;
}

// ^[A-Za-z0-9._:-]{1,200}$ (the gateway's derived ids are 46 characters)
bool valid_command_id(const std::string& s) {
  if (s.empty() || s.size() > 200) return false;
  for (char c : s) {
    if (!alnum(c) && c != '.' && c != '_' && c != ':' && c != '-') return false;
  }
  return true;
}

namespace {
std::atomic<uint64_t> g_det_seed{0};
std::atomic<uint64_t> g_det_counter{0};
uint64_t splitmix(uint64_t x) {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}
}  // namespace

void set_deterministic_ids(uint64_t seed) {
  g_det_counter.store(0);
  g_det_seed.store(seed);
}

std::string new_uuid_v7(int64_t now_ms) {
  thread_local std::mt19937_64 rng{std::random_device{}()};
  uint64_t r1, r2;
  if (const uint64_t seed = g_det_seed.load(std::memory_order_relaxed); seed != 0) {
    const uint64_t n = g_det_counter.fetch_add(1, std::memory_order_relaxed);
    r1 = splitmix(seed ^ (2 * n));
    r2 = splitmix(seed ^ (2 * n + 1));
  } else {
    r1 = rng();
    r2 = rng();
  }
  const uint64_t ts = static_cast<uint64_t>(now_ms) & 0xFFFFFFFFFFFFULL;
  const uint64_t hi = (ts << 16) | 0x7000ULL | (r1 & 0x0FFFULL);
  const uint64_t lo = (r2 & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
  char buf[37];
  std::snprintf(buf, sizeof buf, "%08x-%04x-%04x-%04x-%012llx",
                static_cast<unsigned>(hi >> 32), static_cast<unsigned>((hi >> 16) & 0xFFFF),
                static_cast<unsigned>(hi & 0xFFFF), static_cast<unsigned>(lo >> 48),
                static_cast<unsigned long long>(lo & 0xFFFFFFFFFFFFULL));
  return std::string(buf);
}

}  // namespace pcr
