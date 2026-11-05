// The §3.6 scenario matrix (PLAN §3.6, §7 "Simulator comparisons"): the CTMC model against the DES
// (which runs the real dispatch), seeded replications, 95 % confidence intervals.
//   bazel run -c opt //sim:matrix -- --out=<dir> [--reps=10] [--threads=4] [--quick] [--only=<prefix>[,<prefix>]]
//                                     [--seed-base=1000] [--policy=primary-first] [--dispatch=tiers]
//                                     [--model=agents|coupled]
// --model=agents (the default; phase 5 redesign): the per-agent model (model/agents.h), every agent
// counted individually. VIP cells report each class against the gate on its own row; weighted cells
// (queues' dispatch weights differ) check lever (a)'s weight model.
// Writes <dir>/des-matrix.json and <dir>/des-matrix.md.
//
// Gate (exponential cells): |SL_model - SL_sim| <= 3 points; abandonment within 1 point or 15 %;
// mean EWT within 15 % or 5 s. Lognormal cells are reported against +-5 SL points (CR-024: the
// gate `auto` mode will depend on) and set the `approx` thresholds.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <vector>

#include "model/coupling.h"
#include "model/ctmc.h"
#include "model/agents.h"
#include "sim/des.h"

using json = nlohmann::json;
using namespace pcr::sim;
namespace m = pcr::model;

namespace {

constexpr double kAht = 180, kPatience = 120, kTau = 20;
m::Policy g_policy = m::Policy::kLongestIdle;
bool g_tiers = false;  // --dispatch=tiers: phase 4's dispatch in the DES (PLAN §8 row 4)
// --model=agents (phase 5 redesign; the default) or --model=coupled (phase 2's fluid coupling).
bool g_pooled = true;
std::string g_only;
uint64_t g_seed_base = 1000;  // replication i runs seed g_seed_base + i

enum class Kind { kSteady, kBurst, kRamp };

struct Cell {
  std::string id, design, dist;
  double rho = 0;
  int size = 0;
  Kind kind = Kind::kSteady;
  Scenario sc;
  int reps = 10;
  // burst / ramp
  double base_rate = 0, burst_mult = 2, burst_s = 900, ramp_s = 900;
};

Dist talk_of(const std::string& dist) { return dist == "exponential" ? Dist::exp(kAht) : Dist::lognormal(kAht - 15, 0.8); }
Dist wrap_of(const std::string& dist) { return dist == "exponential" ? Dist::fixed(0) : Dist::fixed(15); }
Dist patience_of(const std::string& dist) { return dist == "exponential" ? Dist::exp(kPatience) : Dist::lognormal(kPatience, 1.0); }

QueueSpec queue(const std::string& id, double per_s, const std::string& dist) {
  QueueSpec q;
  q.id = id;
  q.per_s = {per_s};
  q.talk = talk_of(dist);
  q.wrap = wrap_of(dist);
  q.patience = patience_of(dist);
  q.sla_s = kTau;
  return q;
}

// Durations sized so the whole matrix fits one run of at most 5 minutes on 8 threads (the user's
// rule, 2026-10-31): the engine's cost per call grows with the number of agents, so the big cells
// get shorter windows (their relaxation time is short too); the CIs say where that is too little.
void timing(Scenario* sc, double total_rate, double target_calls, bool quick, int agents) {
  const bool big = agents >= 1000;
  sc->warmup_s = big ? 900 : 1800;
  sc->drain_s = big ? 900 : 1800;
  const double want = target_calls / std::max(1e-9, total_rate);
  sc->measure_s = std::clamp(want, big ? 900.0 : 3600.0, quick ? 40 * 3600.0 : 100 * 3600.0);
  sc->check_every_s = 600;
}

std::vector<Cell> build(bool quick, int reps) {
  std::vector<Cell> cells;
  const std::vector<double> loads{0.7, 0.9, 1.0, 1.1, 1.3};
  const std::vector<int> sizes = quick ? std::vector<int>{3, 10, 50} : std::vector<int>{3, 10, 50, 200, 1000};
  auto target_for = [&](int n) { return quick ? 4000.0 : n >= 1000 ? 6000.0 : 8000.0; };
  for (const std::string dist : {"exponential", "lognormal"}) {
    for (int n : sizes) {
      for (double rho : loads) {
        Cell c;
        c.design = "single";
        c.dist = dist;
        c.rho = rho;
        c.size = n;
        // Near critical load the big single queues need more replications for a CI that can judge.
        c.reps = n >= 200 ? 2 * reps : reps;
        const double lam = rho * n / kAht;
        c.sc.queues = {queue("q1", lam, dist)};
        c.sc.groups = {GroupSpec{n, {"q1"}}};
        timing(&c.sc, lam, target_for(n), quick, n);
        cells.push_back(c);
      }
    }
    for (int n : {3, 10, 50}) {
      for (double rho : loads) {
        // N design: q1 has its own group; group 2 serves q2 and q1.
        Cell c;
        c.design = "N";
        c.dist = dist;
        c.rho = rho;
        c.size = n;
        c.reps = reps;
        const double l1 = 1.1 * rho * n / kAht, l2 = 0.9 * rho * n / kAht;
        c.sc.queues = {queue("q1", l1, dist), queue("q2", l2, dist)};
        c.sc.groups = {GroupSpec{n, {"q1"}}, GroupSpec{n, {"q2", "q1"}}};
        timing(&c.sc, l1 + l2, target_for(n), quick, 2 * n);
        cells.push_back(c);
        // W design: two dedicated groups and a flexible one between them.
        Cell w = c;
        w.design = "W";
        const double l = 1.5 * rho * n / kAht;
        w.sc.queues = {queue("q1", l, dist), queue("q2", l, dist)};
        w.sc.groups = {GroupSpec{n, {"q1"}}, GroupSpec{n, {"q1", "q2"}}, GroupSpec{n, {"q2"}}};
        timing(&w.sc, 2 * l, target_for(n), quick, 3 * n);
        cells.push_back(w);
      }
    }
    if (!quick || dist == "exponential") {
      for (double rho : loads) {
        // Enterprise-like: 20 queues x 8 groups of 25 (each group 5 queues, each queue 2 groups).
        Cell c;
        c.design = "enterprise";
        c.dist = dist;
        c.rho = rho;
        c.size = 200;
        c.reps = reps;
        double wsum = 0;
        for (int i = 0; i < 20; ++i) wsum += 1 + i % 4;
        double total = 0;
        for (int i = 0; i < 20; ++i) {
          const double lam = rho * 200 * (1 + i % 4) / wsum / kAht;
          c.sc.queues.push_back(queue("q" + std::to_string(i + 1), lam, dist));
          total += lam;
        }
        for (int g = 0; g < 8; ++g) {
          GroupSpec gs;
          gs.count = 25;
          const int start = g * 5 / 2;
          for (int k = 0; k < 5; ++k) gs.queues.push_back("q" + std::to_string((start + k) % 20 + 1));
          c.sc.groups.push_back(gs);
        }
        timing(&c.sc, total, quick ? 10000 : 16000, quick, 200);
        cells.push_back(c);
      }
    }
  }
  // Phase 5 redesign: VIP cells (two classes, VIP threshold 10 s, normal 20 s) and weighted cells
  // (lever a: queues' dispatch weights differ). Exponential only.
  for (int n : {10, 50}) {
    for (double rho : {0.7, 0.9, 1.0}) {
      for (double share : {0.1, 0.3}) {
        Cell c;
        c.design = share < 0.2 ? "vip10" : "vip30";
        c.dist = "exponential";
        c.rho = rho;
        c.size = n;
        c.reps = reps;
        const double lam = rho * n / kAht;
        QueueSpec q = queue("q1", lam, "exponential");
        q.vip_share = share;
        q.vip_sla_s = 10;
        c.sc.queues = {q};
        c.sc.groups = {GroupSpec{n, {"q1"}}};
        timing(&c.sc, lam, target_for(n), quick, n);
        cells.push_back(c);
      }
    }
  }
  for (int n : {10, 50}) {
    for (double rho : {0.9, 1.0}) {
      // VIP in a W design: both queues 20 % VIP, the flexible group shared.
      Cell w;
      w.design = "vipW";
      w.dist = "exponential";
      w.rho = rho;
      w.size = n;
      w.reps = reps;
      const double l = 1.5 * rho * n / kAht;
      QueueSpec q1 = queue("q1", l, "exponential"), q2 = queue("q2", l, "exponential");
      q1.vip_share = q2.vip_share = 0.2;
      q1.vip_sla_s = q2.vip_sla_s = 10;
      w.sc.queues = {q1, q2};
      w.sc.groups = {GroupSpec{n, {"q1"}}, GroupSpec{n, {"q1", "q2"}}, GroupSpec{n, {"q2"}}};
      timing(&w.sc, 2 * l, target_for(n), quick, 3 * n);
      cells.push_back(w);
    }
  }
  for (int n : {10, 50}) {
    for (double rho : {0.9, 1.0}) {
      for (double wt : {2.0, 4.0}) {
        // A single pool of two queues (one group serving both) with q1's weight wt: its callers wait
        // less, q2's more (the time-dependent priority of w x age / tau).
        Cell c;
        c.design = wt < 3 ? "weight2" : "weight4";
        c.dist = "exponential";
        c.rho = rho;
        c.size = n;
        c.reps = reps;
        const double l = 0.5 * rho * n / kAht;
        QueueSpec q1 = queue("q1", l, "exponential"), q2 = queue("q2", l, "exponential");
        q1.weight = wt;
        c.sc.queues = {q1, q2};
        c.sc.groups = {GroupSpec{n, {"q1", "q2"}}};
        timing(&c.sc, 2 * l, target_for(n), quick, n);
        cells.push_back(c);
      }
    }
  }
  // Bursts: x2 arrivals for 15 min on a stationary single queue at rho 0.9; shift-start ramps:
  // agents log in evenly over 15 min while calls arrive at the full rate.
  for (int n : quick ? std::vector<int>{10, 50} : std::vector<int>{10, 50, 200}) {
    Cell b;
    b.design = "burst";
    b.dist = "exponential";
    b.rho = 0.9;
    b.size = n;
    b.kind = Kind::kBurst;
    b.reps = quick ? 16 : 64;
    b.base_rate = 0.9 * n / kAht;
    QueueSpec q = queue("q1", b.base_rate, "exponential");
    q.bucket_s = 900;
    // The x2 bucket starts right after the warm-up (1,800 s) and is the measured window.
    q.per_s = {b.base_rate, b.base_rate, 2 * b.base_rate, b.base_rate};
    b.sc.queues = {q};
    b.sc.groups = {GroupSpec{n, {"q1"}}};
    b.sc.warmup_s = 1800;
    b.sc.measure_s = 900;
    b.sc.drain_s = 1800;
    b.sc.check_every_s = 300;
    cells.push_back(b);
  }
  for (int n : quick ? std::vector<int>{50} : std::vector<int>{50, 200}) {
    Cell r;
    r.design = "shift-start";
    r.dist = "exponential";
    r.rho = 0.9;
    r.size = n;
    r.kind = Kind::kRamp;
    r.reps = quick ? 16 : 64;
    r.base_rate = 0.9 * n / kAht;
    r.sc.queues = {queue("q1", r.base_rate, "exponential")};
    r.sc.groups = {GroupSpec{n, {"q1"}, 0, r.ramp_s}};
    r.sc.warmup_s = 0;
    r.sc.measure_s = r.ramp_s;
    r.sc.drain_s = 1800;
    r.sc.check_every_s = 60;
    cells.push_back(r);
  }
  for (auto& c : cells) {
    char buf[96];
    std::snprintf(buf, sizeof buf, "%s-%s-n%d-rho%.1f", c.design.c_str(), c.dist.substr(0, 3).c_str(), c.size, c.rho);
    c.id = buf;
    c.sc.name = c.id;
  }
  return cells;
}

struct ModelOut {
  std::vector<m::QueueMetrics> q, normal, vip;
  std::vector<double> s, secondary_share, lent_out_share;
  m::Quality quality = m::Quality::kExact;
  bool conserved = true;
  double max_secondary_share = 0;
  std::string why;
};

m::Options opts() {
  m::Options o;
  o.sla_s = kTau;
  o.short_abandon_s = 5;
  return o;
}

// The model for a steady cell: per queue lambda, mu = 1 / (talk + wrap), theta; the groups split by
// load (no optimizer: every member queue active), coupled.
ModelOut model_steady(const Cell& c, const std::vector<double>& theta) {
  ModelOut out;
  std::vector<m::CQueue> qs;
  std::map<std::string, int> idx;
  for (size_t i = 0; i < c.sc.queues.size(); ++i) {
    const QueueSpec& q = c.sc.queues[i];
    idx[q.id] = static_cast<int>(i);
    qs.push_back(m::CQueue{q.per_s[0], 1 / (q.talk.mean + q.wrap.mean), theta[i], 0});
  }
  std::vector<m::CGroup> gs;
  for (const auto& g : c.sc.groups) {
    m::CGroup cg;
    cg.agents = g.count;
    for (const auto& q : g.queues) cg.queues.push_back(idx.at(q));
    gs.push_back(cg);
  }
  if (g_pooled) {
    // Phase 5 redesign: the per-agent model. Every agent of the cell individually; under tiers the
    // DES pins each multi-queue agent's first queue PRIMARY and the others SECONDARY (des.cc), so
    // each agent gets exactly that set.
    std::vector<m::AQueue> aq;
    for (size_t i = 0; i < qs.size(); ++i) {
      const QueueSpec& sq = c.sc.queues[i];
      m::AQueue a;
      a.lambda = qs[i].lambda;
      a.vip_lambda = qs[i].lambda * sq.vip_share;
      a.mu = qs[i].mu;
      a.theta = qs[i].theta;
      a.delay_s = g_tiers ? c.sc.secondary_delay_ms / 1000.0 : 0;
      a.sla_s = sq.sla_s;
      a.vip_sla_s = sq.vip_sla_s;
      a.weight = sq.weight;
      a.escalate_s = 3 * sq.sla_s;  // des.cc: escalate_after = 3 tau
      aq.push_back(a);
    }
    std::vector<m::AAgent> ag;
    for (const auto& g : c.sc.groups) {
      for (int k = 0; k < g.count; ++k) {
        m::AAgent a;
        for (size_t x = 0; x < g.queues.size(); ++x) {
          a.queues.push_back(idx.at(g.queues[x]));
          a.tiers.push_back(g.queues.size() == 1 || x == 0 ? 1 : 2);
          a.speeds.push_back(x < g.speed.size() ? g.speed[x] : 1.0);
        }
        a.vip = g.vip;
        ag.push_back(std::move(a));
      }
    }
    m::AgentOptions ao;
    ao.q = opts();
    ao.dispatch = g_tiers ? m::Dispatch::kTiers : m::Dispatch::kLongestIdle;
    m::Stats st;
    const m::AgentResult r = m::solve_agents(aq, ag, ao, &st);
    out.quality = r.quality;
    out.conserved = true;
    out.why = r.why;
    for (const auto& q : r.q) {
      out.q.push_back(q.m);
      out.normal.push_back(q.normal);
      out.vip.push_back(q.vip);
      out.s.push_back(q.R);
      out.secondary_share.push_back(q.R > 0 ? 1 - q.P / q.R : 0);
      out.lent_out_share.push_back(q.handicap);
      out.max_secondary_share = std::max(out.max_secondary_share, q.R > 0 ? 1 - q.P / q.R : 0);
    }
    return out;
  }
  m::split_by_load(qs, &gs);
  m::CouplingOptions co;
  co.q = opts();
  co.policy = g_policy;  // phase 1's dispatch (longest idle, oldest head) unless --dispatch=tiers
  m::Stats st;
  const m::CouplingResult r = m::solve_coupled(qs, gs, co, &st);
  out.quality = r.quality;
  out.conserved = r.conserved;
  out.why = r.why;
  for (const auto& q : r.q) {
    out.q.push_back(q.m);
    out.s.push_back(q.c);
    out.secondary_share.push_back(q.secondary_share);
    out.lent_out_share.push_back(q.lent_out_share);
    out.max_secondary_share = std::max(out.max_secondary_share, q.secondary_share);
  }
  return out;
}

ModelOut model_burst(const Cell& c) {
  ModelOut out;
  const QueueSpec& q = c.sc.queues[0];
  const double mu = 1 / (q.talk.mean + q.wrap.mean), th = 1 / q.patience.mean;
  m::Stats st;
  const m::Distribution d = m::stationary(m::QueueParams{c.base_rate, mu, th, double(c.size)}, c.size, opts(), &st);
  const m::QueueMetrics mm =
      m::transient_from(m::QueueParams{c.burst_mult * c.base_rate, mu, th, double(c.size)}, d.p, c.burst_s, 30, opts(), &st, nullptr);
  out.q = {mm};
  out.s = {double(c.size)};
  out.quality = mm.quality;
  return out;
}

// The expected remaining virtual wait from each position j at a bucket's start, given the value at
// its end (h_next) and the bucket's servers: E[time not yet served in the bucket + h_next(J)],
// the tagged chain's backward equation with a running cost, by uniformization (own patience off).
std::vector<double> carry_back(const std::vector<double>& h_next, double s, double mu, double th, double dt) {
  const int kmax = static_cast<int>(h_next.size()) - 1;  // index 0 = served
  std::vector<double> r(kmax + 1, 0.0);
  for (int j = 1; j <= kmax; ++j) r[j] = s * mu + (j - 1) * th;
  const double lam = r[kmax];
  const m::PoissonWeights fg = m::fox_glynn(lam * dt, 1e-12);
  std::vector<double> f = h_next, one(kmax + 1, 1.0), out(kmax + 1, 0.0), cost(kmax + 1, 0.0);
  one[0] = 0;
  double cum = 0;
  auto step = [&](std::vector<double>& v) {
    std::vector<double> nx(v.size());
    nx[0] = 0;
    for (int j = 1; j <= kmax; ++j) nx[j] = (1 - r[j] / lam) * v[j] + (r[j] / lam) * v[j - 1];
    v.swap(nx);
  };
  for (int n = 0; n <= fg.right; ++n) {
    const double w = n >= fg.left ? fg.w[n - fg.left] : 0;
    cum += w;
    const double tail = std::max(0.0, 1 - cum);  // P(Pois(lam dt) > n)
    for (int j = 0; j <= kmax; ++j) {
      out[j] += w * f[j];
      cost[j] += tail * one[j];
    }
    step(f);
    step(one);
  }
  for (int j = 0; j <= kmax; ++j) out[j] += cost[j] / lam;
  out[0] = 0;
  return out;
}

// Shift start: agents log in evenly over the ramp while calls arrive at the full rate. Piecewise
// per minute (PLAN §3.3): the occupancy by uniformization bucket to bucket; an arrival's virtual
// wait with the servers that will be there while it waits (backward over the buckets);
// abandonment as theta * E[waiting] / lambda over the window; SL with the arrival's bucket.
ModelOut model_ramp(const Cell& c) {
  ModelOut out;
  const QueueSpec& q = c.sc.queues[0];
  const double mu = 1 / (q.talk.mean + q.wrap.mean), th = 1 / q.patience.mean, lam = c.base_rate;
  m::Stats st;
  const int buckets = static_cast<int>(c.ramp_s / 60);
  const double dt = 60;
  auto servers = [&](int b) {
    return std::max(1, static_cast<int>(std::lround(c.size * ((b + 0.5) * dt) / c.ramp_s)));
  };
  const int kmax = 800;
  m::Quality qq;
  std::vector<double> h(kmax + 1, 0.0);
  {
    const auto v = m::ewt_by_position(c.size, mu, th, 0, kmax, 3600, &qq);
    for (int j = 1; j <= kmax; ++j) h[j] = v[j - 1];
  }
  std::vector<std::vector<double>> half(buckets);
  for (int b = buckets - 1; b >= 0; --b) {
    // At the bucket's end new agents arrive: a caller at position j is at j - (new servers) then.
    const int ds = (b + 1 < buckets ? servers(b + 1) : c.size) - servers(b);
    std::vector<double> shifted(kmax + 1, 0.0);
    for (int j = 1; j <= kmax; ++j) shifted[j] = j - ds <= 0 ? 0 : h[std::min(kmax, j - ds)];
    half[b] = carry_back(shifted, servers(b), mu, th, dt / 2);
    h = carry_back(shifted, servers(b), mu, th, dt);
  }
  std::vector<double> p{1.0};
  double sl = 0, ewt = 0, lq = 0;
  int points = 0;
  for (int b = 0; b < buckets; ++b) {
    const int s = servers(b);
    const m::QueueParams qp{lam, mu, th, double(s)};
    const int K = std::max<int>(p.size() - 1, m::transient_k(qp, s, static_cast<int>(p.size()) - 1, dt, opts()));
    p.resize(K + 1, 0.0);
    // Within tau (20 s, inside the bucket) agents keep logging in: each login takes the head at
    // once, so the tagged progress rate gains the login rate.
    const double logins = c.size / c.ramp_s;
    const m::Tagged tg = m::tagged(s + logins / mu, mu, th, 0, std::max(1, K - s + 1), opts(), &st);
    // Arrivals at the bucket's middle: occupancy there, positions with this bucket's servers.
    m::Distribution mid = m::transient(qp, s, p, dt / 2, opts(), &st);
    double sl_b = 0, sh = 0, ewt_b = 0, lq_b = 0;
    for (int n = 0; n <= K; ++n) {
      const double pn = mid.p[n];
      if (n < s) {
        sl_b += pn;
        continue;
      }
      const int k = n - s + 1;
      sl_b += pn * tg.u[k - 1];
      sh += pn * tg.a[k - 1];
      ewt_b += pn * half[b][std::min(k, kmax)];
      lq_b += pn * (n - s);
    }
    sl += sl_b / (1 - sh);
    ewt += ewt_b;
    lq += lq_b;
    ++points;
    m::Distribution end = m::transient(qp, s, p, dt, opts(), &st);
    if (!mid.ok || !end.ok) {
      out.why = "transient refused";
      break;
    }
    p = end.p;
  }
  m::QueueMetrics acc;
  acc.ok = out.why.empty();
  acc.sl = sl / points;
  acc.ewt_mean = ewt / points;
  acc.abandon = th * (lq / points) / lam;
  acc.quality = m::Quality::kExact;
  out.q = {acc};
  out.s = {double(c.size)};
  return out;
}

struct SimAgg {
  std::vector<Estimate> sl, ab, ewt, asa;
  std::vector<double> theta_mle;  // abandons / total time waited (what the engine would measure)
  uint64_t calls = 0, violations = 0, sweep = 0, errors = 0, unresolved = 0, checks = 0;
  double wall_s = 0;
};

// Every (cell, replication) of the run in one pool, the costliest first (calls x agents), so the
// threads stay busy across cells.
std::vector<std::vector<RepResult>> run_all(const std::vector<const Cell*>& cells, int threads) {
  std::vector<std::vector<RepResult>> out(cells.size());
  struct Job {
    size_t cell;
    int rep;
    double cost;
  };
  std::vector<Job> jobs;
  for (size_t i = 0; i < cells.size(); ++i) {
    const Scenario& sc = cells[i]->sc;
    double rate = 0;
    for (const auto& q : sc.queues) {
      for (double r : q.per_s) rate = std::max(rate, r * sc.queues.size());
    }
    int agents = 0;
    for (const auto& g : sc.groups) agents += g.count;
    const double cost = rate * (sc.warmup_s + sc.measure_s + sc.drain_s) * (1 + agents / 200.0);
    out[i].resize(cells[i]->reps);
    for (int r = 0; r < cells[i]->reps; ++r) jobs.push_back({i, r, cost});
  }
  std::stable_sort(jobs.begin(), jobs.end(), [](const Job& a, const Job& b) { return a.cost > b.cost; });
  std::atomic<size_t> next{0};
  std::vector<std::thread> pool;
  for (int t = 0; t < std::max(1, threads); ++t) {
    pool.emplace_back([&] {
      for (size_t j = next++; j < jobs.size(); j = next++) {
        out[jobs[j].cell][jobs[j].rep] = run_replication(cells[jobs[j].cell]->sc, g_seed_base + jobs[j].rep);
      }
    });
  }
  for (auto& t : pool) t.join();
  return out;
}

// cls 0: every caller; 1: normal callers; 2: VIP callers.
SimAgg aggregate(const Cell& c, const std::vector<RepResult>& reps, int cls = 0) {
  SimAgg a;
  const size_t nq = c.sc.queues.size();
  auto of = [&](const RepResult& r) -> const std::vector<QueueStats>& { return cls == 0 ? r.q : cls == 1 ? r.normal : r.vip; };
  for (size_t q = 0; q < nq; ++q) {
    std::vector<double> sl, ab, ewt, asa;
    double abandons = 0, waited = 0;
    for (const auto& r : reps) {
      const QueueStats& x = of(r)[q];
      sl.push_back(x.sl());
      ab.push_back(x.abandon());
      ewt.push_back(x.ewt());
      asa.push_back(x.asa());
      abandons += x.abandoned;
      waited += x.wait_all_sum;
    }
    a.sl.push_back(estimate(sl));
    a.ab.push_back(estimate(ab));
    a.ewt.push_back(estimate(ewt));
    a.asa.push_back(estimate(asa));
    a.theta_mle.push_back(waited > 0 ? abandons / waited : 1 / kPatience);
  }
  for (const auto& r : reps) {
    for (const auto& q : of(r)) {
      a.calls += q.offered;
      a.unresolved += q.virtual_unresolved;
    }
    a.violations += r.invariant_violations;
    a.checks += r.invariant_checks;
    a.sweep += r.sweep_matches;
    a.errors += r.engine_errors;
    a.wall_s += r.wall_s;
  }
  return a;
}

}  // namespace

int main(int argc, char** argv) {
  std::string out_dir = ".";
  int reps = 8, threads = 8;
  bool quick = false;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a.rfind("--out=", 0) == 0) out_dir = a.substr(6);
    else if (a.rfind("--reps=", 0) == 0) reps = std::stoi(a.substr(7));
    else if (a.rfind("--threads=", 0) == 0) threads = std::stoi(a.substr(10));
    else if (a == "--quick") quick = true;
    else if (a.rfind("--only=", 0) == 0) g_only = a.substr(7);
    else if (a == "--policy=primary-first") g_policy = m::Policy::kPrimaryFirst;
    else if (a == "--dispatch=tiers") g_tiers = true;  // phase 4 (and the model's primary-first)
    else if (a.rfind("--seed-base=", 0) == 0) g_seed_base = std::stoull(a.substr(12));
    else if (a == "--model=coupled") g_pooled = false;
    else if (a == "--model=pooled" || a == "--model=agents") g_pooled = true;
  }
  const auto t0 = std::chrono::steady_clock::now();
  std::vector<Cell> cells = build(quick, reps);
  if (g_tiers) {
    for (Cell& c : cells) c.sc.tiers = true;
    g_policy = m::Policy::kPrimaryFirst;  // the model's view of tiers
  }
  json all = json::array();
  int gate_cells = 0, gate_pass = 0, logn_cells = 0, logn_pass = 0;
  // Per queue result, split by the model's own quality label: the gate is meant to hold where the
  // model says exact; approx results are reported (and never drive automatic moves, PLAN §4.3).
  int exp_exact = 0, exp_exact_pass = 0, exp_approx = 0, exp_approx_pass = 0;
  int log_exact = 0, log_exact_pass = 0, log_approx = 0, log_approx_pass = 0;
  int n_pass[2] = {0, 0}, n_miss[2] = {0, 0}, n_undecided[2] = {0, 0};  // [exponential, lognormal]
  std::vector<const Cell*> todo;
  for (const Cell& c : cells) {
    // --only=<prefix>[,<prefix>...]: the cells whose id starts with one of them.
    bool want = g_only.empty();
    for (size_t at = 0; !want && at <= g_only.size();) {
      const size_t comma = g_only.find(',', at);
      const std::string p = g_only.substr(at, comma == std::string::npos ? std::string::npos : comma - at);
      want = !p.empty() && c.id.rfind(p, 0) == 0;
      if (comma == std::string::npos) break;
      at = comma + 1;
    }
    if (want) todo.push_back(&c);
  }
  const std::vector<std::vector<RepResult>> results = run_all(todo, threads);
  const double sim_wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  std::printf("simulated %zu cells in %.0f s wall\n", todo.size(), sim_wall);
  std::string md;
  md += "| cell | queue | s (model) | quality | SL model | SL sim (95 % CI) | abandon model | abandon sim | EWT model (s) | EWT sim (s) | gate |\n";
  md += "|---|---|---|---|---|---|---|---|---|---|---|\n";
  for (size_t ci = 0; ci < todo.size(); ++ci) {
    const Cell& c = *todo[ci];
    const SimAgg sim = aggregate(c, results[ci]);
    bool vip_cell = false;
    for (const auto& q : c.sc.queues) vip_cell = vip_cell || q.vip_share > 0;
    const std::vector<int> classes = vip_cell ? std::vector<int>{1, 2} : std::vector<int>{0};
    std::vector<SimAgg> by_class(3);
    for (int k : classes) by_class[k] = k == 0 ? sim : aggregate(c, results[ci], k);
    ModelOut mo;
    if (c.kind == Kind::kBurst) {
      mo = model_burst(c);
    } else if (c.kind == Kind::kRamp) {
      mo = model_ramp(c);
    } else {
      std::vector<double> theta;
      for (size_t q = 0; q < c.sc.queues.size(); ++q) {
        // Exponential patience: theta = 1/mean. Lognormal: the exponential MLE the engine measures
        // (abandons / total time waited; PLAN §3.1), from the same runs.
        theta.push_back(c.dist == "exponential" ? 1 / c.sc.queues[q].patience.mean : sim.theta_mle[q]);
      }
      mo = model_steady(c, theta);
    }
    json cj = {{"cell", c.id}, {"design", c.design}, {"dist", c.dist}, {"rho", c.rho}, {"size", c.size},
               {"reps", c.reps}, {"calls", sim.calls}, {"measure_s", c.sc.measure_s},
               {"model_quality", m::quality_name(mo.quality)}, {"conserved", mo.conserved},
               {"max_secondary_share", mo.max_secondary_share},
               {"sim", {{"invariant_violations", sim.violations}, {"invariant_checks", sim.checks},
                        {"sweep_matches", sim.sweep}, {"engine_errors", sim.errors},
                        {"ewt_unresolved", sim.unresolved}, {"cpu_s", sim.wall_s}}}};
    json qj = json::array();
    bool cell_ok = true;
    // The big multi-queue cells report every queue; the gate counts them all. VIP cells: one row per
    // (queue, class).
    for (size_t q = 0; q < c.sc.queues.size(); ++q) for (int k : classes) {
      const SimAgg& sim = by_class[k];
      const std::vector<m::QueueMetrics>& mv = k == 0 ? mo.q : k == 1 ? mo.normal : mo.vip;
      const m::QueueMetrics& mm = mv.size() > q ? mv[q] : m::QueueMetrics{};
      const std::string qname = c.sc.queues[q].id + (k == 1 ? "/normal" : k == 2 ? "/vip" : "");
      const double dsl = mm.sl - sim.sl[q].mean, dab = mm.abandon - sim.ab[q].mean, dewt = mm.ewt_mean - sim.ewt[q].mean;
      const bool sl_ok = std::abs(dsl) <= 0.03;
      const bool ab_ok = std::abs(dab) <= 0.01 || std::abs(dab) <= 0.15 * sim.ab[q].mean;
      const bool ewt_ok = std::abs(dewt) <= 5 || std::abs(dewt) <= 0.15 * sim.ewt[q].mean;
      const bool logn_ok = std::abs(dsl) <= 0.05;
      const bool ok = mm.ok && (c.dist == "exponential" ? (sl_ok && ab_ok && ewt_ok) : logn_ok);
      // A miss is "beyond the CI" when it stays outside the gate after the simulation's own 95 %
      // half-width is allowed for; otherwise the run was too short to decide it.
      const bool sl_far = std::abs(dsl) - sim.sl[q].half > (c.dist == "exponential" ? 0.03 : 0.05);
      const bool ab_far = std::abs(dab) - sim.ab[q].half > std::max(0.01, 0.15 * sim.ab[q].mean);
      const bool ewt_far = std::abs(dewt) - sim.ewt[q].half > std::max(5.0, 0.15 * sim.ewt[q].mean);
      const bool far = !mm.ok || sl_far || (c.dist == "exponential" && (ab_far || ewt_far));
      const char* status = ok ? "pass" : far ? "miss" : "undecided";
      (ok ? n_pass : far ? n_miss : n_undecided)[c.dist == "exponential" ? 0 : 1] += 1;
      cell_ok = cell_ok && ok;
      const bool exact = mm.ok && mm.quality == m::Quality::kExact;
      if (c.dist == "exponential") {
        (exact ? exp_exact : exp_approx) += 1;
        (exact ? exp_exact_pass : exp_approx_pass) += ok;
      } else {
        (exact ? log_exact : log_approx) += 1;
        (exact ? log_exact_pass : log_approx_pass) += ok;
      }
      qj.push_back({{"queue", qname}, {"class", k == 0 ? "all" : k == 1 ? "normal" : "vip"}, {"model_ok", mm.ok}, {"why", mm.why}, {"s", mo.s.size() > q ? mo.s[q] : 0},
                    {"secondary_share", mo.secondary_share.size() > q ? mo.secondary_share[q] : 0},
                    {"lent_out_share", mo.lent_out_share.size() > q ? mo.lent_out_share[q] : 0},
                    {"quality", m::quality_name(mm.quality)},
                    {"model", {{"sl", mm.sl}, {"abandon", mm.abandon}, {"ewt", mm.ewt_mean}, {"asa", mm.asa}}},
                    {"sim", {{"sl", sim.sl[q].mean}, {"sl_ci", sim.sl[q].half}, {"abandon", sim.ab[q].mean},
                             {"abandon_ci", sim.ab[q].half}, {"ewt", sim.ewt[q].mean}, {"ewt_ci", sim.ewt[q].half},
                             {"asa", sim.asa[q].mean}, {"theta_mle", sim.theta_mle[q]}}},
                    {"pass", {{"sl", sl_ok}, {"abandon", ab_ok}, {"ewt", ewt_ok}, {"lognormal_sl5", logn_ok}}},
                    {"status", status}});
      if (q < 3 || c.sc.queues.size() <= 3) {
        char row[512];
        std::snprintf(row, sizeof row,
                      "| %s | %s | %.2f | %s | %.3f | %.3f ± %.3f | %.3f | %.3f ± %.3f | %.1f | %.1f ± %.1f | %s |\n",
                      c.id.c_str(), qname.c_str(), mo.s.size() > q ? mo.s[q] : 0.0, m::quality_name(mm.quality),
                      mm.sl, sim.sl[q].mean, sim.sl[q].half, mm.abandon, sim.ab[q].mean, sim.ab[q].half, mm.ewt_mean,
                      sim.ewt[q].mean, sim.ewt[q].half,
                      c.dist == "exponential" ? (ok ? "pass" : far ? "MISS" : "undecided (CI)")
                                              : (ok ? "within ±5" : far ? "OUTSIDE ±5" : "undecided (CI)"));
        md += row;
      }
    }
    cj["queues"] = qj;
    cj["pass"] = cell_ok;
    if (c.dist == "exponential") {
      ++gate_cells;
      gate_pass += cell_ok;
    } else {
      ++logn_cells;
      logn_pass += cell_ok;
    }
    all.push_back(cj);
    std::printf("%-28s %s calls=%llu violations=%llu cpu=%.1fs\n", c.id.c_str(), cell_ok ? "pass" : "FAIL",
                static_cast<unsigned long long>(sim.calls), static_cast<unsigned long long>(sim.violations), sim.wall_s);
    std::fflush(stdout);
  }
  const double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  json out = {{"cells", all}, {"exponential_cells", gate_cells}, {"exponential_pass", gate_pass},
              {"lognormal_cells", logn_cells}, {"lognormal_within_5", logn_pass}, {"wall_s", wall},
              {"by_quality", {{"exponential", {{"exact", exp_exact}, {"exact_pass", exp_exact_pass},
                                               {"approx", exp_approx}, {"approx_pass", exp_approx_pass}}},
                              {"lognormal", {{"exact", log_exact}, {"exact_within_5", log_exact_pass},
                                             {"approx", log_approx}, {"approx_within_5", log_approx_pass}}}}},
              {"policy", g_policy == m::Policy::kLongestIdle ? "longest-idle" : "primary-first"},
              {"model", g_pooled ? "per-agent (phase 5 redesign, model/agents.h)" : "coupled (phase 2)"},
              {"dispatch", g_tiers ? "tiers (phase 4: first queue primary, others secondary, d = 10 s)" : "phase 1"},
              {"queue_results", {{"exponential", {{"pass", n_pass[0]}, {"miss", n_miss[0]}, {"undecided", n_undecided[0]}}},
                                 {"lognormal", {{"within_5", n_pass[1]}, {"outside", n_miss[1]}, {"undecided", n_undecided[1]}}}}},
              {"reps", reps}, {"quick", quick}, {"threads", threads}, {"seed_base", g_seed_base}};
  std::ofstream(out_dir + "/des-matrix.json") << out.dump(2);
  char head[1536];
  std::snprintf(head, sizeof head,
                "# DES matrix\n\nExponential cells passing the gate: %d / %d. Lognormal cells within ±5 SL points: %d / %d. "
                "Wall %.0f s.\n\nPer queue result by the model's label: exponential exact %d / %d pass, approx %d / %d; "
                "lognormal exact %d / %d within ±5, approx %d / %d.\n\nPer queue result, misses split by the CI: "
                "exponential pass %d, miss beyond the CI %d, undecided %d; lognormal within ±5 %d, outside beyond the CI %d, "
                "undecided %d.\n\n",
                gate_pass, gate_cells, logn_pass, logn_cells, wall, exp_exact_pass, exp_exact, exp_approx_pass, exp_approx,
                log_exact_pass, log_exact, log_approx_pass, log_approx, n_pass[0], n_miss[0], n_undecided[0], n_pass[1],
                n_miss[1], n_undecided[1]);
  std::ofstream(out_dir + "/des-matrix.md") << head << md;
  std::printf("exponential gate: %d/%d; lognormal within 5 points: %d/%d; wall %.0f s\n", gate_pass, gate_cells,
              logn_pass, logn_cells, wall);
  std::printf("by label: exponential exact %d/%d approx %d/%d; lognormal exact %d/%d approx %d/%d\n", exp_exact_pass,
              exp_exact, exp_approx_pass, exp_approx, log_exact_pass, log_exact, log_approx_pass, log_approx);
  return 0;
}
