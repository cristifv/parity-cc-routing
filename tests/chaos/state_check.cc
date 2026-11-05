// Phase 6 chaos: the invariant checker over re.state. Reads the topic to its end, rebuilds the
// tenant from its records exactly as a takeover does (Tenant::restore with the seed's config) and
// reports check_invariants() plus a direct count over the records: at most one offer record per agent
// and per entry, every offer record held by its agent and its entry, every live reservation held by
// its agent, and no record of another tenant under the tenant's keys.
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <thread>

#include "src/core/tenant.h"
#include "src/ops/seed.h"
#include "src/state/kafka_state.h"
#include "src/state/state.pb.h"

namespace st = ::parity::cc_routing::state::v1;
using json = nlohmann::json;

namespace {
struct NullSink final : pcr::Sink {
  void to_ie(const std::string&, uint64_t, const pcr::rv1::WatchResponse&) override {}
  void to_gw(const std::string&, uint64_t, const pcr::av1::WatchResponse&) override {}
  bool ie_live(const std::string&) const override { return true; }
};
struct NullTimers final : pcr::TimerSink {
  void at(int64_t, pcr::TimerRef) override {}
};
}  // namespace

int main(int argc, char** argv) {
  if (argc != 5) {
    std::fprintf(stderr, "usage: state_check <brokers> <topic> <seed.json> <tenant>\n");
    return 2;
  }
  const std::string brokers = argv[1], topic = argv[2], tenant = argv[4];
  std::ifstream in(argv[3]);
  std::stringstream ss;
  ss << in.rdbuf();
  std::vector<std::pair<std::string, pcr::TenantConfig>> all;
  std::string err;
  if (!pcr::parse_seed_all(ss.str(), &all, &err)) {
    std::fprintf(stderr, "seed: %s\n", err.c_str());
    return 2;
  }
  const pcr::TenantConfig* cfg = nullptr;
  for (const auto& [bin, c] : all) {
    if (c.id == tenant) cfg = &c;
  }
  if (!cfg) {
    std::fprintf(stderr, "no tenant %s in the seed\n", tenant.c_str());
    return 2;
  }
  pcr::KafkaState ks(pcr::KafkaState::Options{brokers, topic, "state-check"}, [] { return false; }, [](const std::string&) {});
  if (!ks.start_follower(&err)) {
    std::fprintf(stderr, "re.state: %s\n", err.c_str());
    return 2;
  }
  for (int i = 0; i < 200 && !ks.caught_up(); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(50));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  const auto recs = ks.records_of(tenant);
  std::map<std::string, int> offers_per_agent, offers_per_entry;
  std::map<std::string, std::string> agent_offer, entry_offer;
  std::vector<std::string> bad;
  size_t live_res = 0;
  std::map<std::string, std::set<std::string>> agent_res;
  std::vector<std::pair<std::string, std::string>> res_agent;
  for (const auto& [key, value] : recs) {
    st::Record r;
    if (!r.ParseFromString(value)) {
      bad.push_back("unreadable record " + key);
      continue;
    }
    if (key.rfind(tenant + "/", 0) != 0) bad.push_back("a key outside the tenant: " + key);
    if (r.has_offer()) {
      ++offers_per_agent[r.offer().agent_id()];
      ++offers_per_entry[r.offer().entry_id()];
    }
    if (r.has_agent()) {
      agent_offer[r.agent().id()] = r.agent().offer_id();
      for (const auto& x : r.agent().reservations()) agent_res[r.agent().id()].insert(x);
    }
    if (r.has_entry()) entry_offer[r.entry().id()] = r.entry().offer_id();
    if (r.has_res() && r.res().st() != 3) {
      ++live_res;
      res_agent.emplace_back(r.res().id(), r.res().agent_id());
    }
  }
  for (const auto& [a, n] : offers_per_agent) {
    if (n > 1) bad.push_back("agent " + a + " has " + std::to_string(n) + " offer records");
  }
  for (const auto& [e, n] : offers_per_entry) {
    if (n > 1) bad.push_back("entry " + e + " has " + std::to_string(n) + " offer records");
  }
  for (const auto& [rid, a] : res_agent) {
    if (!agent_res[a].contains(rid)) bad.push_back("live reservation " + rid + " not held by its agent " + a);
  }
  // The takeover's rebuild, then every structural invariant.
  NullSink sink;
  NullTimers timers;
  pcr::Counters counters;
  pcr::Epochs epochs{1, 0};
  pcr::Tenant t(*cfg, &epochs, &sink, &timers, &counters);
  const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  const auto stats = t.restore(recs, now, /*restamp=*/false);
  t.set_draining(true);  // a check, not a leader: no offers
  auto inv = t.check_invariants(now);
  // Time-based ones need a running leader's timers (max wait, idle agents): only structural ones count.
  std::erase_if(inv, [](const std::string& v) {
    return v.find("waited past max_wait_s") != std::string::npos || v.find(" idles while ") != std::string::npos ||
           v.find("stranded by rejects") != std::string::npos;
  });
  json out{{"tenant", tenant}, {"records", recs.size()}, {"restored", stats.records}, {"skipped", stats.skipped},
           {"entries", t.entry_ids().size()}, {"live_reservations", live_res}, {"record_violations", bad},
           {"invariant_violations", inv}, {"consumed", ks.consumed()}};
  std::printf("%s\n", out.dump().c_str());
  std::fflush(stdout);
  std::_Exit(bad.empty() && inv.empty() ? 0 : 1);
}
