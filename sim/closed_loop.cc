// The closed loop (PLAN §3.6, §8 row 5; phase 5 redesign): the DES runs the real dispatch while the
// real optimizer acts on it every 10 s of virtual time (the tenant's snapshot -> Optimizer::levers_for
// -> Tenant::install_levers), against two baselines on the same seeds:
//   longest_idle   mode off, every weight 1, no bounds (phase 1's dispatch);
//   static_config  mode off, the scenario's configured weights and delays (what a supervisor set);
//   optimizer      mode auto: lever (a) tunes weights and delays (exact results only, bounds,
//                  hysteresis), lever (b) applies the configured min/max agents.
// The score is J over the measurement window (PLAN §3.4): sum over queues and classes of
// arrivals x [alpha (1 - SL) + beta abandonment] per second, with each scenario's importance, so a
// lower J is better. Scenarios: multi-skill (a W design plus a dedicated queue, unequal importance,
// a load shift), VIP (two queues sharing agents, 15 % VIP callers with their own threshold and
// importance), VIP under heavy load (a 90 s max wait, a VIP reserve) and multi-skill under tiers
// (lever a's delays).
//   bazel run -c opt //sim:closed_loop -- --out=<dir> [--reps=12] [--threads=12]
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <vector>

#include "sim/des.h"
#include "src/core/tenant.h"
#include "src/opt/optimizer.h"

using json = nlohmann::json;
using namespace pcr;
using namespace pcr::sim;

namespace {

constexpr double kAht = 180, kPatience = 120;
bool g_explain = false;  // --explain: every 30 min of one run, each queue's label and why

struct Imp {
  double alpha = 1, beta = 1, vip_alpha = 2, vip_beta = 2;
};

struct Case {
  std::string name;
  Scenario sc;
  std::vector<Imp> imp;  // per queue (the optimizer gets the same through config.routing)
  std::vector<double> config_weight;
};

QueueSpec q(const std::string& id, std::vector<double> per_s) {
  QueueSpec x;
  x.id = id;
  x.per_s = std::move(per_s);
  x.bucket_s = 1800;
  x.talk = Dist::exp(kAht);
  x.patience = Dist::exp(kPatience);
  x.sla_s = 20;
  return x;
}

std::vector<Case> cases() {
  std::vector<Case> out;
  {
    // Multi-skill: billing (important, alpha 4) and sales share 8 flexible agents (a W design; their
    // configured proficiency makes them 10 % slower on sales) next to 6 dedicated agents each;
    // support has its own 6. Sales' load rises by 30 % after the first half hour.
    Case c;
    c.name = "multi-skill";
    const double lb = 0.9 * 10 / kAht, ls = 0.85 * 10 / kAht, lp = 0.85 * 6 / kAht;
    c.sc.queues = {q("billing", {lb}), q("sales", {ls, 1.3 * ls, 1.3 * ls, ls, ls}), q("support", {lp})};
    GroupSpec flex(8, {"billing", "sales"});
    flex.speed = {1.0, 0.9};
    c.sc.groups = {GroupSpec(6, {"billing"}), flex, GroupSpec(6, {"sales"}), GroupSpec(6, {"support"})};
    c.imp = {Imp{4, 4, 2, 2}, Imp{1, 1, 2, 2}, Imp{1, 1, 2, 2}};
    c.config_weight = {1.5, 1, 1};
    c.sc.queues[0].min_agents = 8;
    out.push_back(c);
  }
  {
    // VIP: billing and sales share 14 agents (each takes both) plus 3 dedicated each; 15 % VIP
    // callers on billing (threshold 10 s, importance 4).
    Case c;
    c.name = "vip";
    const double l = 0.92 * 20 / kAht / 2;
    c.sc.queues = {q("billing", {l}), q("sales", {l})};
    c.sc.queues[0].vip_share = 0.15;
    c.sc.queues[0].vip_sla_s = 10;
    c.sc.groups = {GroupSpec(14, {"billing", "sales"}), GroupSpec(3, {"billing"}), GroupSpec(3, {"sales"})};
    c.imp = {Imp{2, 2, 4, 4}, Imp{1, 1, 2, 2}};
    c.config_weight = {1, 1};
    out.push_back(c);
  }
  {
    // VIP under heavy load (PLAN §8 row 5, the user's AC 3): billing and sales at 92 % of their 20
    // agents, patient callers (5 min mean), a 90-second max wait (DISCONNECT) on both queues, 10 % VIP
    // callers on billing (threshold 10 s, importance 4), 4 VIP-eligible agents with a reserve of 2 (held
    // until a normal call waited 45 s): VIP callers meet an 80 % target within their threshold while
    // normal callers leave by max wait at the latest, and escalation (60 s) keeps them from starving
    // behind VIP callers.
    Case c;
    c.name = "vip-heavy";
    const double l = 0.92 * 20 / kAht / 2;
    c.sc.queues = {q("billing", {l}), q("sales", {l})};
    c.sc.queues[0].vip_share = 0.1;
    c.sc.queues[0].vip_sla_s = 10;
    c.sc.queues[0].vip_reserved_min = 2;
    // The reserve holds a VIP-eligible agent from a normal call until the queue's delay: 45 s here.
    c.sc.secondary_delay_ms = 45'000;
    for (auto& x : c.sc.queues) {
      x.max_wait_s = 90;
      x.patience = Dist::exp(300);
    }
    GroupSpec vipg(4, {"billing", "sales"});
    vipg.vip = true;
    c.sc.groups = {vipg, GroupSpec(12, {"billing", "sales"}), GroupSpec(2, {"billing"}), GroupSpec(2, {"sales"})};
    c.imp = {Imp{2, 2, 4, 4}, Imp{1, 1, 2, 2}};
    c.config_weight = {1, 1};
    out.push_back(c);
  }
  {
    // Multi-skill under tiers (phase 4's dispatch: each flexible agent's first queue PRIMARY, its
    // second SECONDARY after d): lever (a) moves the delays too.
    Case c = out.front();
    c.name = "multi-skill-tiers";
    c.sc.tiers = true;
    c.sc.secondary_delay_ms = 20'000;
    out.push_back(c);
  }
  for (auto& c : out) {
    c.sc.name = c.name;
    c.sc.warmup_s = 1800;
    c.sc.measure_s = 4 * 3600.0;
    c.sc.drain_s = 900;
    c.sc.check_every_s = 300;
  }
  return out;
}

// J over the window from the DES's per-class counts (rates per second over measure_s).
double j_of(const Case& c, const RepResult& r) {
  double j = 0;
  for (size_t i = 0; i < c.sc.queues.size(); ++i) {
    for (int k = 0; k < 2; ++k) {
      const QueueStats& s = k == 0 ? r.normal[i] : r.vip[i];
      if (s.offered == 0) continue;
      const double lam = s.offered / c.sc.measure_s;
      const double a = k == 0 ? c.imp[i].alpha : c.imp[i].vip_alpha, b = k == 0 ? c.imp[i].beta : c.imp[i].vip_beta;
      // SL here counts overflows as missed: (answered within tau) / (offered - short abandons).
      j += lam * (a * (1 - s.sl()) + b * s.abandon());
    }
  }
  return j;
}

struct ArmOut {
  std::vector<double> j;
  std::vector<double> sl_vip, sl_normal;  // queue 0
  uint64_t violations = 0, sweep = 0, errors = 0, past_max_wait = 0, overflowed = 0;
  double max_wait_seen = 0;
  uint64_t plans = 0, plans_exact = 0, changes = 0;
  double wall_s = 0;
};

Scenario arm(const Case& c, const std::string& name, std::shared_ptr<std::atomic<uint64_t>> plans,
             std::shared_ptr<std::atomic<uint64_t>> exact, std::shared_ptr<std::atomic<uint64_t>> changes) {
  Scenario sc = c.sc;
  for (size_t i = 0; i < sc.queues.size(); ++i) {
    // The importance is config.routing's (the snapshot carries it to the optimizer).
    sc.queues[i].alpha = c.imp[i].alpha;
    sc.queues[i].beta = c.imp[i].beta;
    sc.queues[i].vip_alpha = c.imp[i].vip_alpha;
    sc.queues[i].vip_beta = c.imp[i].vip_beta;
    sc.queues[i].weight = name == "longest_idle" ? 1.0 : c.config_weight[i];
    if (name != "optimizer") sc.queues[i].min_agents = sc.queues[i].max_agents = 0;
  }
  if (name == "longest_idle") sc.tiers = false;
  sc.mode = name == "optimizer" ? 2 : 0;
  if (name == "optimizer") {
    auto state = std::make_shared<Optimizer::TenantState>();
    auto stats = std::make_shared<model::Stats>();
    Optimizer::Options o;
    o.levers = true;
    sc.hook = [state, stats, o, plans, exact, changes](Tenant* t, int64_t now) {
      auto snap = t->model_snapshot(now);
      Optimizer::LeverOut out = Optimizer::levers_for(*snap, state.get(), o, stats.get());
      if (!out.plan) return;
      if (g_explain && state->ticks % 180 == 0) {
        std::string line = snap->tenant + " t=" + std::to_string(state->ticks) + ":";
        for (size_t i = 0; i < out.queues.size(); ++i) {
          const auto& cq = out.result.current[i];
          line += " " + out.queues[i].id + "[" + model::quality_name(out.result.q_quality[i]) + " " + cq.why +
                  " sl=" + std::to_string(cq.m.sl).substr(0, 5) + " w=" + std::to_string(out.result.w_planned[i]).substr(0, 4) + "]";
        }
        std::printf("%s\n", line.c_str());
      }
      ++*plans;
      if (out.plan->quality == 0) ++*exact;
      for (size_t i = 0; i < out.queues.size(); ++i) {
        if (out.result.w_planned[i] != out.queues[i].weight || out.result.d_planned[i] != out.queues[i].delay_s) ++*changes;
      }
      t->install_levers(out.plan, now);
    };
  }
  return sc;
}

}  // namespace

int main(int argc, char** argv) {
  std::string out_dir = ".";
  int reps = 12, threads = 12;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a.rfind("--out=", 0) == 0) out_dir = a.substr(6);
    else if (a.rfind("--reps=", 0) == 0) reps = std::stoi(a.substr(7));
    else if (a.rfind("--threads=", 0) == 0) threads = std::stoi(a.substr(10));
    else if (a == "--explain") g_explain = true;
  }
  const auto t0 = std::chrono::steady_clock::now();
  const std::vector<Case> cs = cases();
  const std::vector<std::string> arms{"longest_idle", "static_config", "optimizer"};
  struct Job {
    size_t c, a;
    int rep;
  };
  std::vector<Job> jobs;
  for (size_t c = 0; c < cs.size(); ++c) {
    for (size_t a = 0; a < arms.size(); ++a) {
      for (int r = 0; r < reps; ++r) jobs.push_back({c, a, r});
    }
  }
  std::vector<std::vector<std::vector<RepResult>>> res(cs.size(), std::vector<std::vector<RepResult>>(arms.size(), std::vector<RepResult>(reps)));
  struct Counts {
    std::shared_ptr<std::atomic<uint64_t>> plans = std::make_shared<std::atomic<uint64_t>>(0),
                                           exact = std::make_shared<std::atomic<uint64_t>>(0),
                                           changes = std::make_shared<std::atomic<uint64_t>>(0);
  };
  std::vector<Counts> counts(cs.size());
  std::atomic<size_t> next{0};
  std::vector<std::thread> pool;
  for (int t = 0; t < std::max(1, threads); ++t) {
    pool.emplace_back([&] {
      for (size_t k = next++; k < jobs.size(); k = next++) {
        const Job& j = jobs[k];
        // Each replication its own scenario copy (and so its own optimizer state).
        const Scenario sc = arm(cs[j.c], arms[j.a], counts[j.c].plans, counts[j.c].exact, counts[j.c].changes);
        res[j.c][j.a][j.rep] = run_replication(sc, 5000 + j.rep);  // the same seeds for every arm
      }
    });
  }
  for (auto& t : pool) t.join();
  json all = json::object();
  std::string md = "| scenario | arm | J (mean ± 95 % CI) | J vs longest idle | VIP SL q0 | normal SL q0 | violations | past max wait | overflowed | longest wait (s) |\n|---|---|---|---|---|---|---|---|---|---|\n";
  for (size_t c = 0; c < cs.size(); ++c) {
    json cj = json::object();
    std::vector<double> base_j;
    for (size_t a = 0; a < arms.size(); ++a) {
      ArmOut o;
      for (const auto& r : res[c][a]) {
        o.j.push_back(j_of(cs[c], r));
        if (r.vip[0].offered) o.sl_vip.push_back(r.vip[0].sl());
        o.sl_normal.push_back(r.normal[0].sl());
        o.violations += r.invariant_violations;
        o.sweep += r.sweep_matches;
        o.errors += r.engine_errors;
        o.past_max_wait += r.past_max_wait;
        for (uint64_t x : r.overflowed) o.overflowed += x;
        o.max_wait_seen = std::max(o.max_wait_seen, r.max_wait_seen_s);
        o.wall_s += r.wall_s;
      }
      if (a == 0) base_j = o.j;
      // Paired by seed against longest idle.
      std::vector<double> diff;
      for (size_t k = 0; k < o.j.size(); ++k) diff.push_back(o.j[k] - base_j[k]);
      const Estimate je = estimate(o.j), de = estimate(diff), sv = estimate(o.sl_vip), sn = estimate(o.sl_normal);
      cj[arms[a]] = {{"j", je.mean}, {"j_ci", je.half}, {"j_minus_longest_idle", de.mean}, {"diff_ci", de.half},
                     {"sl_vip_q0", sv.mean}, {"sl_normal_q0", sn.mean}, {"invariant_violations", o.violations},
                     {"sweep_matches", o.sweep}, {"engine_errors", o.errors}, {"past_max_wait", o.past_max_wait},
                     {"overflowed", o.overflowed}, {"max_wait_seen_s", o.max_wait_seen},
                     {"cpu_s", o.wall_s}};
      char row[400];
      std::snprintf(row, sizeof row, "| %s | %s | %.5f ± %.5f | %+.5f ± %.5f | %s | %.3f | %llu | %llu | %llu | %.0f |\n",
                    cs[c].name.c_str(), arms[a].c_str(), je.mean, je.half, de.mean, de.half,
                    o.sl_vip.empty() ? "-" : (std::to_string(sv.mean).substr(0, 5)).c_str(), sn.mean,
                    static_cast<unsigned long long>(o.violations), static_cast<unsigned long long>(o.past_max_wait),
                    static_cast<unsigned long long>(o.overflowed), o.max_wait_seen);
      md += row;
      std::printf("%s", row);
    }
    cj["optimizer_plans"] = counts[c].plans->load();
    cj["optimizer_plans_exact"] = counts[c].exact->load();
    cj["optimizer_planned_changes"] = counts[c].changes->load();
    all[cs[c].name] = cj;
  }
  const double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  all["wall_s"] = wall;
  all["reps"] = reps;
  std::ofstream(out_dir + "/closed-loop.json") << all.dump(2);
  std::ofstream(out_dir + "/closed-loop.md") << "# Closed loop (J, lower is better)\n\n" << md;
  std::printf("wall %.0f s\n", wall);
  return 0;
}
