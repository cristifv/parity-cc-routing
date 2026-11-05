// The scenario runner's command line (PLAN §7.1): the canonical set from parity-contracts, or the
// files and directories given (regressions, full-scale runs), against the real engine.
//   runner --contracts=<VERSION path> [--scenario=<name>] [--dir=<dir>] [file.json ...]
//          [--fault=kind@s --expect-violation=I3 [--expect-count=<report counter>]] [--deterministic]
//          [--shards=N] [--model-ewt]
//          [--check-every-ms=N] [--decision-p99-ms=5 --decision-max-ms=50] [--position-throttle=off]
//          [--log=<path> [--log-level=info] [--log-decisions=N]] (phase 8: the engine's log-record.v1
//          records written there, every N-th agent.selected with its decision; ci.sh validates them)
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "src/ops/log.h"
#include "tests/scenarios/runner.h"

using json = nlohmann::json;

int main(int argc, char** argv) {
  std::string dir;
  std::vector<std::string> only;
  pcr::scenario::RunOptions opt;
  std::string expect_violation;  // fault mode: the invariant that must catch the fault
  std::string expect_count;      // ... and a report counter that must be above 0 (which check caught it)
  bool listed = false;
  pcr::LogInit log;
  log.instance = "scenario-runner";
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--contracts=", 0) == 0) {
      dir = a.substr(12);
      if (dir.ends_with("/VERSION")) dir = dir.substr(0, dir.size() - 8);
    } else if (a.rfind("--fault=", 0) == 0) {
      const std::string f = a.substr(8);  // kind@seconds
      opt.fault.kind = f.substr(0, f.find('@'));
      opt.fault.at_ms = static_cast<int64_t>(std::stod(f.substr(f.find('@') + 1)) * 1000);
    } else if (a.rfind("--expect-violation=", 0) == 0) {
      expect_violation = a.substr(19);
    } else if (a.rfind("--expect-count=", 0) == 0) {
      expect_count = a.substr(15);
    } else if (a.rfind("--scenario=", 0) == 0) {
      only.push_back(dir + "/scenarios/routing/" + a.substr(11) + ".json");
    } else if (a.rfind("--dir=", 0) == 0) {
      listed = true;
      std::error_code ec;  // an empty directory has no runfiles: nothing to replay
      for (const auto& e : std::filesystem::directory_iterator(a.substr(6), ec)) {
        if (e.path().extension() == ".json") only.push_back(e.path().string());
      }
    } else if (a.rfind("--log=", 0) == 0) {
      log.path = a.substr(6);
    } else if (a.rfind("--log-level=", 0) == 0) {
      log.level = a.substr(12);
    } else if (a.rfind("--log-decisions=", 0) == 0) {
      log.decision_every = std::stoi(a.substr(16));
    } else if (a == "--deterministic") {
      opt.deterministic = true;
    } else if (a == "--model-ewt") {
      opt.model_ewt = true;
    } else if (a == "--position-throttle=off") {
      opt.position_throttle = false;
    } else if (a.rfind("--shards=", 0) == 0) {
      opt.shards = std::stoi(a.substr(9));
    } else if (a.rfind("--check-every-ms=", 0) == 0) {
      opt.check_every_ms = std::stoll(a.substr(17));
    } else if (a.rfind("--decision-p99-ms=", 0) == 0) {
      opt.decision_p99_ms = std::stod(a.substr(18));
    } else if (a.rfind("--decision-max-ms=", 0) == 0) {
      opt.decision_max_ms = std::stod(a.substr(18));
    } else {
      only.push_back(a);  // a scenario file to play instead of the canonical set
    }
  }
  if (!log.path.empty()) pcr::log_init(log);
  std::vector<std::string> files = only;
  if (files.empty() && !listed) {
    for (const auto& e : std::filesystem::directory_iterator(dir + "/scenarios/routing")) {
      if (e.path().extension() == ".json") files.push_back(e.path().string());
    }
  }
  std::sort(files.begin(), files.end());
  int failed = 0;
  json all = json::array();
  std::map<std::string, uint64_t> total_exercised;
  for (const auto& f : files) {
    std::ifstream in(f);
    const json sc = json::parse(in);
    if (sc.value("schema_version", 0) != 1) {
      std::printf("FAIL %s: not routing-scenario.v1\n", f.c_str());
      ++failed;
      continue;
    }
    const auto t0 = std::chrono::steady_clock::now();
    json r;
    try {
      r = pcr::scenario::run(sc, opt);
    } catch (const std::exception& e) {
      r = {{"scenario", sc.value("name", f)}, {"violation_count", 1}, {"violations", {std::string("crash: ") + e.what()}}};
    }
    r["wall_s"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    bool ok = r.at("violation_count").get<uint64_t>() == 0;
    if (!expect_violation.empty()) {
      // Fault mode: the run passes only if the named invariant caught the injected bug.
      const json v = r.value("violated", json::object());
      ok = v.contains(expect_violation) && v.at(expect_violation).get<uint64_t>() > 0;
      if (!expect_count.empty()) ok = ok && r.value(expect_count, uint64_t{0}) > 0;
      std::printf("fault %s caught by %s%s: %s\n", opt.fault.kind.c_str(), expect_violation.c_str(),
                  expect_count.empty() ? "" : (" (" + expect_count + " > 0)").c_str(), ok ? "yes" : "NO");
    }
    for (const auto& [k, v] : r.value("exercised", json::object()).items()) total_exercised[k] += v.get<uint64_t>();
    std::printf("%s %s %s\n", ok ? "PASS" : "FAIL", sc.value("name", f).c_str(), r.dump().c_str());
    std::fflush(stdout);
    failed += ok ? 0 : 1;
    all.push_back(r);
  }
  if (const char* out = std::getenv("TEST_UNDECLARED_OUTPUTS_DIR")) {
    std::ofstream(std::string(out) + "/scenarios.json") << all.dump(2);
  }
  std::string ex_line;
  for (const auto& [k, v] : total_exercised) ex_line += " " + k + "=" + std::to_string(v);
  std::printf("exercised:%s\n", ex_line.c_str());
  if (only.empty() && !listed) {
    // The full set must exercise every phase-1 invariant (I8, queue moves, is phase 4).
    for (const char* inv : {"I1", "I2", "I3", "I4", "I5", "I6", "I7", "I9", "I10", "I11"}) {
      if (total_exercised[inv] == 0) {
        std::printf("FAIL %s was never exercised by the scenario set\n", inv);
        ++failed;
      }
    }
  }
  std::printf("scenarios: %zu run, %d failed\n", files.size(), failed);
  pcr::log_flush();
  return failed == 0 && (!files.empty() || listed) ? 0 : 1;
}
