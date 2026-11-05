// The §3.5 compute budget, measured (PLAN §3.5, phase 2): per-tenant model work per tick on one
// core for synthetic SMB, mid and enterprise tenants, and the estimate for a bin holding all
// 2,000 tenants of a region (85 % SMB, 14 % mid, 1 % enterprise).
//   bazel run -c opt //sim:budget -- --out=<dir>
// Phase 5 redesign: the per-agent model (every agent individually, speeds 0.9-1.1, 10 % VIP callers,
// mode auto). Numbers per tier: the EWT tick (Optimizer::plan_for: estimators, the per-agent CTMC,
// the EWT tables), the lever tick (Optimizer::levers_for: the baseline forecast, the per-agent CTMC,
// lever (a)'s bounded search, events), the advice tick (the same once a minute with lever (b)/(c)'s
// recommendations) and the full tick §3.5 describes (EWT + lever tick + one 5-minute transient per
// queue from its current state).
// Phase 7 (CR-171): the engine's own tick is the EWT tick + the lever tick (Optimizer::run does
// nothing else; the per-queue transients are §3.5's notional work, which the phase-5 optimizer never
// runs), reported as engine_tick_ms and used for the bin's cores; full_tick_ms stays for comparison.
// --only=smb,mid,enterprise limits the tiers.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <map>
#include <nlohmann/json.hpp>
#include <random>
#include <string>
#include <vector>

#include "model/ctmc.h"
#include "src/opt/optimizer.h"

using json = nlohmann::json;
using namespace pcr;

namespace {

struct SizeTier {
  std::string name;
  int queues, shapes, agents, queues_per_group;  // shapes: distinct agent skill sets (agents still count one by one)
  double budget_ms, cap_ms;
  int runs;
};

// A tenant at about rho 0.85 overall: queues of mixed size, each group eligible for a few queues.
// The snapshot covers 15 minutes so the estimators start at the intended rates: over a 10-s window
// an SMB queue's arrival count (0.013 calls/s) rounds to zero and the bench would time an empty
// model (the first phase-2 measurement did, 2026-10-31).
constexpr double kWindowS = 900;
ModelSnapshot tenant(const SizeTier& t, uint64_t seed) {
  std::mt19937_64 rng(seed);
  ModelSnapshot s;
  s.tenant = "acme";
  s.at_ms = 1'792'800'000'000;
  s.window_ms = static_cast<int64_t>(kWindowS * 1000);
  std::vector<double> w(t.queues);
  double wsum = 0;
  for (int i = 0; i < t.queues; ++i) {
    w[i] = std::uniform_real_distribution<double>(0.5, 2.0)(rng);
    wsum += w[i];
  }
  const double aht = 180;
  for (int i = 0; i < t.queues; ++i) {
    ModelSnapshot::Queue q;
    q.id = "q" + std::to_string(i + 1);
    const double offered = 0.85 * t.agents * w[i] / wsum;
    const double lam = offered / aht;
    q.arrivals = static_cast<uint64_t>(std::llround(lam * kWindowS));
    q.aht_s = aht;
    q.abandons = static_cast<uint64_t>(std::llround(lam * kWindowS * 0.05));
    q.waited_s = lam * kWindowS * 8;
    q.max_wait_ms = 600'000;
    q.sla_ms = 20'000;
    q.waiting = static_cast<uint32_t>(std::max(0.0, std::round(offered * 0.05)));
    q.vip_arrivals = q.arrivals / 10;
    q.wrap_s = 0;
    s.queues.push_back(q);
  }
  s.mode = 2;  // auto: lever (a)'s search runs
  for (int g = 0; g < t.shapes; ++g) {
    std::vector<int> qs;
    for (int k = 0; k < t.queues_per_group; ++k) qs.push_back((g * t.queues / t.shapes + k) % t.queues);
    std::sort(qs.begin(), qs.end());
    qs.erase(std::unique(qs.begin(), qs.end()), qs.end());
    for (int a = 0; a < t.agents / t.shapes; ++a) {
      ModelSnapshot::Agent x;
      x.id = "a" + std::to_string(g) + "-" + std::to_string(a);
      x.counts = true;
      for (int qi : qs) {
        x.set.emplace_back(qi, 1);
        x.speed.push_back(std::uniform_real_distribution<double>(0.9, 1.1)(rng));
        x.eligible.push_back(qi);
      }
      x.vip = a % 5 == 0;
      s.agents.push_back(std::move(x));
    }
  }
  return s;
}

struct Times {
  std::vector<double> ewt_us, lever_us, advice_us, full_us, cold_us, engine_us;
};

double pct(std::vector<double> v, double p) {
  if (v.empty()) return 0;
  std::sort(v.begin(), v.end());
  return v[std::min(v.size() - 1, static_cast<size_t>(p * v.size()))];
}

std::string load_avg() {
  std::ifstream in("/proc/loadavg");
  std::string a, b, c;
  in >> a >> b >> c;
  return a + " " + b + " " + c;
}

}  // namespace

int main(int argc, char** argv) {
  std::string out_dir = ".", only;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a.rfind("--out=", 0) == 0) out_dir = a.substr(6);
    if (a.rfind("--only=", 0) == 0) only = "," + a.substr(7) + ",";
  }
  const std::vector<SizeTier> tiers = {
      {"smb", 3, 2, 8, 2, 0.5, 0.5, 400},
      {"mid", 15, 6, 120, 4, 5, 5, 100},
      {"enterprise", 200, 50, 3000, 6, 100, 200, 12},
  };
  json out = {{"loadavg_before", load_avg()}};
  json tj = json::object();
  std::map<std::string, double> median_full, median_engine;
  for (const SizeTier& t : tiers) {
    if (!only.empty() && only.find("," + t.name + ",") == std::string::npos) continue;
    Times tm;
    uint64_t approx = 0, refused = 0;
    Optimizer::Options oo;
    oo.levers = true;
    for (int r = 0; r < t.runs; ++r) {
      const ModelSnapshot s = tenant(t, 100 + r);
      Optimizer::TenantState st;
      model::Stats ms;
      bool ap = false;
      // The first tick of a tenant (an advice tick: recommendations too) solves cold.
      const auto c0 = std::chrono::steady_clock::now();
      Optimizer::plan_for(s, &st, oo, &ms, &ap);
      const auto c1 = std::chrono::steady_clock::now();
      Optimizer::levers_for(s, &st, oo, &ms);
      const auto c2 = std::chrono::steady_clock::now();
      tm.cold_us.push_back(std::chrono::duration<double, std::micro>(c2 - c0).count());
      tm.advice_us.push_back(std::chrono::duration<double, std::micro>(c2 - c1).count());
      const auto a = std::chrono::steady_clock::now();
      auto plan = Optimizer::plan_for(s, &st, oo, &ms, &ap);
      const auto b = std::chrono::steady_clock::now();
      auto lo = Optimizer::levers_for(s, &st, oo, &ms);  // a later tick: lever (a) only
      const auto bl = std::chrono::steady_clock::now();
      if (!plan || !lo.plan) ++refused;
      if (ap) ++approx;
      // §3.5's transient per queue: the next 5 minutes from the queue as it is now.
      for (size_t i = 0; i < s.queues.size(); ++i) {
        const auto& q = s.queues[i];
        const double lam = q.arrivals / kWindowS, mu = 1 / q.aht_s;
        double servers = 0;
        for (const auto& ag : s.agents) {
          for (const auto& [qi, tier] : ag.set) {
            if (qi == static_cast<int>(i)) servers += 1.0 / static_cast<double>(ag.set.size());
          }
        }
        const int n0 = static_cast<int>(std::lround(std::min(servers, lam / mu))) + static_cast<int>(q.waiting);
        model::Options mo;
        const model::QueueMetrics m = model::transient_metrics(
            model::QueueParams{lam, mu, 1.0 / 120, std::max(1.0, std::round(servers)), -1, 0}, n0, 300, 10, mo, &ms);
        if (!m.ok) ++refused;
      }
      const auto c = std::chrono::steady_clock::now();
      tm.ewt_us.push_back(std::chrono::duration<double, std::micro>(b - a).count());
      tm.lever_us.push_back(std::chrono::duration<double, std::micro>(bl - b).count());
      tm.full_us.push_back(std::chrono::duration<double, std::micro>(c - a).count());
      tm.engine_us.push_back(std::chrono::duration<double, std::micro>(bl - a).count());
    }
    const double med = pct(tm.full_us, 0.5) / 1000, p99 = pct(tm.full_us, 0.99) / 1000, mx = pct(tm.full_us, 1.0) / 1000;
    median_full[t.name] = med;
    const double emed = pct(tm.engine_us, 0.5) / 1000, ep99 = pct(tm.engine_us, 0.99) / 1000, emx = pct(tm.engine_us, 1.0) / 1000;
    median_engine[t.name] = emed;
    tj[t.name] = {{"queues", t.queues}, {"agent_skill_sets", t.shapes}, {"agents", t.agents}, {"runs", t.runs},
                  {"budget_ms", t.budget_ms}, {"cap_ms", t.cap_ms},
                  {"ewt_tick_ms", {{"p50", pct(tm.ewt_us, 0.5) / 1000}, {"p99", pct(tm.ewt_us, 0.99) / 1000}, {"max", pct(tm.ewt_us, 1.0) / 1000}}},
                  {"lever_tick_ms", {{"p50", pct(tm.lever_us, 0.5) / 1000}, {"p99", pct(tm.lever_us, 0.99) / 1000}, {"max", pct(tm.lever_us, 1.0) / 1000}}},
                  {"advice_tick_ms", {{"p50", pct(tm.advice_us, 0.5) / 1000}, {"max", pct(tm.advice_us, 1.0) / 1000}}},
                  {"first_tick_cold_ms", {{"p50", pct(tm.cold_us, 0.5) / 1000}, {"max", pct(tm.cold_us, 1.0) / 1000}}},
                  {"full_tick_ms", {{"p50", med}, {"p99", p99}, {"max", mx}}},
                  {"engine_tick_ms", {{"p50", emed}, {"p99", ep99}, {"max", emx}}},
                  {"within_budget", ep99 <= t.budget_ms}, {"within_cap", emx <= t.cap_ms},
                  {"full_tick_within_budget", p99 <= t.budget_ms},
                  {"approx_solves", approx}, {"refused", refused}};
    std::printf("%-10s queues %3d skill sets %2d agents %4d: cold tick p50 %.3f ms; EWT tick p50 %.3f ms; lever tick p50 %.3f ms; "
                "advice tick p50 %.3f max %.3f ms; engine tick p50 %.3f p99 %.3f max %.3f ms; full tick (+ transients) p50 %.3f p99 %.3f max %.3f ms "
                "(budget %.1f ms, cap %.1f ms) approx %llu refused %llu\n",
                t.name.c_str(), t.queues, t.shapes, t.agents, pct(tm.cold_us, 0.5) / 1000, pct(tm.ewt_us, 0.5) / 1000,
                pct(tm.lever_us, 0.5) / 1000, pct(tm.advice_us, 0.5) / 1000, pct(tm.advice_us, 1.0) / 1000, emed, ep99, emx, med, p99, mx, t.budget_ms,
                t.cap_ms, static_cast<unsigned long long>(approx), static_cast<unsigned long long>(refused));
  }
  // 2,000 tenants in one bin: 85 % SMB, 14 % mid, 1 % enterprise, each once per 10-s tick.
  const double cpu_full = (1700 * median_full["smb"] + 280 * median_full["mid"] + 20 * median_full["enterprise"]) / 1000;
  const double cpu_s = (1700 * median_engine["smb"] + 280 * median_engine["mid"] + 20 * median_engine["enterprise"]) / 1000;
  out["tiers"] = tj;
  out["bin_2000"] = {{"cpu_s_per_tick", cpu_s}, {"tick_s", 10}, {"cores", cpu_s / 10}, {"cores_with_transients", cpu_full / 10},
                     {"plan_estimate_cores", 0.45}, {"threads_available", 2}};
  out["loadavg_after"] = load_avg();
  std::printf("2,000-tenant bin: %.2f CPU-s per 10-s tick = %.3f cores (engine ticks; %.3f with §3.5's transients; PLAN estimate 0.45; "
              "2 threads available)\n", cpu_s, cpu_s / 10, cpu_full / 10);
  std::ofstream(out_dir + "/budget.json") << out.dump(2);
  return 0;
}
