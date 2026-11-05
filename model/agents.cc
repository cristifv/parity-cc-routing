#include "model/agents.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <numeric>
#include <set>
#include <tuple>

namespace pcr::model {

namespace {

// ---- complete resource pooling: a small max flow (Dinic, real capacities) ----
struct Flow {
  struct E {
    int to;
    double cap;
  };
  std::vector<E> e;
  std::vector<std::vector<int>> adj;
  std::vector<int> level, it;
  explicit Flow(int n) : adj(n), level(n), it(n) {}
  void add(int a, int b, double c) {
    adj[a].push_back(static_cast<int>(e.size()));
    e.push_back({b, c});
    adj[b].push_back(static_cast<int>(e.size()));
    e.push_back({a, 0});
  }
  bool bfs(int s, int t) {
    std::fill(level.begin(), level.end(), -1);
    std::vector<int> q{s};
    level[s] = 0;
    for (size_t h = 0; h < q.size(); ++h) {
      for (int id : adj[q[h]]) {
        if (e[id].cap > 1e-12 && level[e[id].to] < 0) {
          level[e[id].to] = level[q[h]] + 1;
          q.push_back(e[id].to);
        }
      }
    }
    return level[t] >= 0;
  }
  double dfs(int v, int t, double f) {
    if (v == t) return f;
    for (int& i = it[v]; i < static_cast<int>(adj[v].size()); ++i) {
      const int id = adj[v][i];
      if (e[id].cap <= 1e-12 || level[e[id].to] != level[v] + 1) continue;
      const double d = dfs(e[id].to, t, std::min(f, e[id].cap));
      if (d > 0) {
        e[id].cap -= d;
        e[id ^ 1].cap += d;
        return d;
      }
    }
    return 0;
  }
  double run(int s, int t) {
    double total = 0;
    while (bfs(s, t)) {
      std::fill(it.begin(), it.end(), 0);
      for (double f; (f = dfs(s, t, 1e300)) > 0;) total += f;
    }
    return total;
  }
  std::vector<bool> reach(int s) {
    std::vector<bool> r(adj.size(), false);
    std::vector<int> q{s};
    r[s] = true;
    for (size_t h = 0; h < q.size(); ++h) {
      for (int id : adj[q[h]]) {
        if (e[id].cap > 1e-12 && !r[e[id].to]) {
          r[e[id].to] = true;
          q.push_back(e[id].to);
        }
      }
    }
    return r;
  }
};

double load_of(const AQueue& q) { return q.mu > 0 ? q.lambda / q.mu : (q.lambda > 0 ? 1e12 : 0); }

// Agents with the same queues: one node of the flow (capacity = the sum of their mean speeds).
struct Cls {
  std::vector<int> queues, agents;
  double cap = 0;
};

double mean_speed(const AAgent& a) {
  if (a.speeds.empty()) return 1;
  double s = 0;
  for (double x : a.speeds) s += x > 0 && std::isfinite(x) ? x : 1;
  return s / static_cast<double>(a.speeds.size());
}

// The subset of `qs` with the highest load per reachable capacity (among classes `cs`).
bool bottleneck(const std::vector<AQueue>& queues, const std::vector<Cls>& cls, const std::vector<int>& qs,
                const std::vector<int>& cs, std::vector<int>* sq, std::vector<int>* sc) {
  double total = 0, nmin = 1e300;
  for (int i : qs) total += load_of(queues[i]);
  for (int c : cs) {
    if (cls[c].cap > 0) nmin = std::min(nmin, cls[c].cap);
  }
  if (total <= 0 || nmin >= 1e300) return false;
  const int n = static_cast<int>(qs.size() + cs.size()) + 2, src = n - 2, snk = n - 1;
  std::vector<int> qi(queues.size(), -1), ci(cls.size(), -1);
  for (size_t k = 0; k < qs.size(); ++k) qi[qs[k]] = static_cast<int>(k);
  for (size_t k = 0; k < cs.size(); ++k) ci[cs[k]] = static_cast<int>(qs.size() + k);
  auto build = [&](double r) {
    Flow f(n);
    for (int i : qs) f.add(src, qi[i], load_of(queues[i]));
    for (int c : cs) {
      for (int i : cls[c].queues) {
        if (qi[i] >= 0) f.add(qi[i], ci[c], 1e300);
      }
      f.add(ci[c], snk, r * cls[c].cap);
    }
    return f;
  };
  double lo = 0, hi = total / nmin + 1;
  for (int k = 0; k < 60 && hi - lo > 1e-9 * hi; ++k) {
    const double mid = (lo + hi) / 2;
    Flow f = build(mid);
    if (f.run(src, snk) >= total * (1 - 1e-9)) hi = mid; else lo = mid;
  }
  Flow f = build(hi * (1 - 1e-6));
  f.run(src, snk);
  const std::vector<bool> r = f.reach(src);
  sq->clear();
  sc->clear();
  for (int i : qs) {
    if (r[qi[i]]) sq->push_back(i);
  }
  for (int c : cs) {
    if (r[ci[c]]) sc->push_back(c);
  }
  double sload = 0;
  for (int i : *sq) sload += load_of(queues[i]);
  if (sq->empty() || sq->size() == qs.size() || sload >= total * (1 - 1e-9)) return false;
  return true;
}

// The blocked-calls-cleared loss probability of x servers at offered load a (real x interpolated):
// under tiers a queue's primaries act as a loss system for immediate service.
double loss(double x, double a) {
  if (a <= 0) return 0;
  if (x <= 0) return 1;
  auto at = [&](int n) {
    double b = 1;
    for (int k = 1; k <= n; ++k) b = a * b / (k + a * b);
    return b;
  };
  const int lo = static_cast<int>(std::floor(x));
  const double f = x - lo;
  return f > 1e-12 ? (1 - f) * at(lo) + f * at(lo + 1) : at(lo);
}

void mark(Quality* q, std::string* why, const std::string& reason) {
  if (*q == Quality::kExact && why->empty()) *why = reason;
  *q = worst(*q, Quality::kApprox);
}

}  // namespace

std::vector<AgentPool> agent_pools(const std::vector<AQueue>& queues, const std::vector<AAgent>& agents) {
  const int nq = static_cast<int>(queues.size());
  std::vector<int> parent(nq);
  std::iota(parent.begin(), parent.end(), 0);
  std::function<int(int)> find = [&](int x) { return parent[x] == x ? x : parent[x] = find(parent[x]); };
  std::map<std::vector<int>, int> cls_of;
  std::vector<Cls> cls;
  for (size_t a = 0; a < agents.size(); ++a) {
    std::vector<int> qs;
    for (int i : agents[a].queues) {
      if (i >= 0 && i < nq) qs.push_back(i);
    }
    std::sort(qs.begin(), qs.end());
    qs.erase(std::unique(qs.begin(), qs.end()), qs.end());
    if (qs.empty()) continue;
    for (size_t k = 1; k < qs.size(); ++k) parent[find(qs[k])] = find(qs[0]);
    auto [it, fresh] = cls_of.emplace(qs, static_cast<int>(cls.size()));
    if (fresh) cls.push_back(Cls{qs, {}, 0});
    cls[it->second].agents.push_back(static_cast<int>(a));
    cls[it->second].cap += mean_speed(agents[a]);
  }
  std::map<int, std::pair<std::vector<int>, std::vector<int>>> comp;
  for (int i = 0; i < nq; ++i) comp[find(i)].first.push_back(i);
  for (size_t c = 0; c < cls.size(); ++c) comp[find(cls[c].queues[0])].second.push_back(static_cast<int>(c));
  std::vector<AgentPool> out;
  auto emit = [&](const std::vector<int>& qs, const std::vector<int>& cs, bool split) {
    AgentPool p;
    p.queues = qs;
    for (int c : cs) p.agents.insert(p.agents.end(), cls[c].agents.begin(), cls[c].agents.end());
    std::sort(p.agents.begin(), p.agents.end());
    p.split = split;
    out.push_back(std::move(p));
  };
  for (auto& [root, qc] : comp) {
    std::vector<int> qs = qc.first, cs = qc.second;
    bool split = false;
    for (int guard = 0; guard < 1000 && !qs.empty(); ++guard) {
      std::vector<int> sq, sc;
      if (!bottleneck(queues, cls, qs, cs, &sq, &sc)) {
        emit(qs, cs, split);
        break;
      }
      split = true;
      emit(sq, sc, true);
      std::vector<int> rq, rc;
      for (int i : qs) {
        if (std::find(sq.begin(), sq.end(), i) == sq.end()) rq.push_back(i);
      }
      for (int c : cs) {
        if (std::find(sc.begin(), sc.end(), c) == sc.end()) rc.push_back(c);
      }
      qs.swap(rq);
      cs.swap(rc);
    }
  }
  return out;
}

AgentModel::AgentModel(std::vector<AQueue> queues, std::vector<AAgent> agents, AgentOptions o, Stats* st)
    : queues_(std::move(queues)), agents_(std::move(agents)), o_(o), st_(st) {
  const size_t nq = queues_.size();
  base_.q.resize(nq);
  for (const auto& q : queues_) {
    if (!std::isfinite(q.lambda) || !std::isfinite(q.vip_lambda) || !std::isfinite(q.mu) || !std::isfinite(q.theta) ||
        !std::isfinite(q.delay_s) || !std::isfinite(q.weight) || q.lambda < 0 || q.vip_lambda < 0 || q.mu < 0 ||
        q.theta < 0 || !(q.weight > 0)) {
      base_.why = "invalid queue parameters";
      if (st_) ++st_->refused;
      return;
    }
  }
  for (auto& a : agents_) {
    if (a.tiers.size() != a.queues.size()) a.tiers.assign(a.queues.size(), 1);
    if (a.speeds.size() != a.queues.size()) a.speeds.assign(a.queues.size(), 1.0);
    for (double& s : a.speeds) {
      if (!(s > 0) || !std::isfinite(s)) {
        base_.why = "invalid agent speed";
        if (st_) ++st_->refused;
        return;
      }
    }
  }
  base_.pools = agent_pools(queues_, agents_);
  cache_.resize(base_.pools.size());
  const bool tiers = o_.dispatch == Dispatch::kTiers;
  base_.ok = true;
  for (size_t pi = 0; pi < base_.pools.size(); ++pi) {
    AgentPool& pool = base_.pools[pi];
    std::vector<char> in_pool(nq, 0);
    for (int i : pool.queues) in_pool[i] = 1;
    // Reach, primaries and speeds per queue, from each agent of the pool.
    std::vector<double> speed_sum(nq, 0.0);
    double smin = 1e300, smax = 0, ssum = 0;
    for (int a : pool.agents) {
      const AAgent& ag = agents_[a];
      double as = 0;
      int n = 0;
      for (size_t k = 0; k < ag.queues.size(); ++k) {
        const int i = ag.queues[k];
        if (i < 0 || static_cast<size_t>(i) >= nq || !in_pool[i]) continue;
        base_.q[i].R += 1;
        base_.q[i].P += tiers ? (ag.tiers[k] == 1 ? 1 : 0) : 1;
        speed_sum[i] += ag.speeds[k];
        as += ag.speeds[k];
        ++n;
      }
      if (n == 0) continue;
      as /= n;
      smin = std::min(smin, as);
      smax = std::max(smax, as);
      ssum += as;
    }
    pool.c = static_cast<int>(pool.agents.size());
    pool.speed = pool.c > 0 ? ssum / pool.c : 1;
    double lam = 0, vlam = 0, occ = 0, thw = 0, mumin = 1e300, mumax = 0, thmin = 1e300, thmax = 0;
    for (int i : pool.queues) {
      const AQueue& q = queues_[i];
      base_.q[i].pool = static_cast<int>(pi);
      lam += q.lambda;
      vlam += std::min(q.vip_lambda, q.lambda);
      const double h = base_.q[i].R > 0 ? speed_sum[i] / base_.q[i].R : 1;
      const double mu_eff = q.mu * h;
      if (q.lambda > 0 && mu_eff > 0) {
        occ += q.lambda / mu_eff;
        thw += q.lambda * q.theta;
        mumin = std::min(mumin, mu_eff);
        mumax = std::max(mumax, mu_eff);
        thmin = std::min(thmin, q.theta);
        thmax = std::max(thmax, q.theta);
      }
    }
    pool.lambda = lam;
    pool.vip_lambda = vlam;
    pool.mu = occ > 0 ? lam / occ : 0;
    pool.theta = lam > 0 ? thw / lam : 0;
    pool.load = occ;
    Quality pq = Quality::kExact;
    if (pool.split) mark(&pq, &pool.why, "split pool");
    if (mumax > 0 && (mumax - mumin) > o_.dispersion * mumax) mark(&pq, &pool.why, "handle times differ");
    if (thmax > 0 && (thmax - thmin) > o_.dispersion * thmax) mark(&pq, &pool.why, "patience differs");
    if (smax > 0 && (smax - smin) > o_.dispersion * smax) mark(&pq, &pool.why, "agent speeds differ");
    if (pool.c > 0 && occ >= o_.near_critical * pool.c) mark(&pq, &pool.why, "near critical load");
    if (pool.c > 0 && pool.mu > 0 && vlam / pool.mu > o_.vip_load * pool.c) mark(&pq, &pool.why, "VIP load");
    pool.quality = pq;
    solve_pool(pi);
    // Nested reach (the §3.6 matrix, N design): one queue's agents a strict subset of another's.
    std::map<int, std::set<std::vector<int>>> reach;  // queue -> the queue sets of the agents reaching it
    for (int a : pool.agents) {
      std::vector<int> qs;
      for (int i : agents_[a].queues) {
        if (i >= 0 && static_cast<size_t>(i) < nq && in_pool[i]) qs.push_back(i);
      }
      std::sort(qs.begin(), qs.end());
      for (int i : qs) reach[i].insert(qs);
    }
    for (auto& [i, ri] : reach) {
      if (queues_[i].lambda <= 0) continue;
      for (auto& [j, rj] : reach) {
        if (i == j || queues_[j].lambda <= 0 || ri.size() >= rj.size()) continue;
        if (std::includes(rj.begin(), rj.end(), ri.begin(), ri.end())) {
          base_.q[i].nested = true;
          base_.q[j].nested = true;
        }
      }
    }
  }
  std::vector<double> w(nq), d(nq);
  for (size_t i = 0; i < nq; ++i) {
    w[i] = queues_[i].weight;
    d[i] = queues_[i].delay_s;
  }
  std::vector<AgentQueue> qs = evaluate(w, d);
  for (size_t i = 0; i < nq; ++i) {
    const double P = base_.q[i].P, R = base_.q[i].R;
    const bool nested = base_.q[i].nested;
    base_.q[i] = qs[i];
    base_.q[i].P = P;
    base_.q[i].R = R;
    base_.q[i].nested = nested;
    if (!base_.q[i].m.ok) {
      base_.ok = false;
      if (base_.why.empty()) base_.why = "queue " + std::to_string(i) + ": " + base_.q[i].m.why;
    }
  }
  // Per-agent shares: each pool's carried load on agents, by tier and weight (a matrix scaling to
  // the agents' busy time and the queues' carried load).
  for (size_t pi = 0; pi < base_.pools.size(); ++pi) {
    AgentPool& pool = base_.pools[pi];
    const PoolCache& pc = cache_[pi];
    double busy = 0;
    if (pc.ok) {
      for (size_t n = 0; n < pc.d.p.size(); ++n) busy += pc.d.p[n] * std::min<double>(static_cast<double>(n), pool.c);
    } else {
      busy = pool.c;
    }
    pool.busy = busy;
    if (pool.c == 0 || pool.agents.empty()) continue;
    std::vector<double> col(nq, 0.0);
    double ctot = 0;
    for (int i : pool.queues) {
      const AQueue& q = queues_[i];
      const double h = base_.q[i].R > 0 ? 1.0 : 0.0;
      col[i] = h * (q.mu > 0 ? q.lambda * (1 - std::clamp(base_.q[i].m.abandon, 0.0, 1.0)) / (q.mu * pool.speed) : 0);
      ctot += col[i];
    }
    if (ctot <= 0) continue;
    for (int i : pool.queues) col[i] *= busy / ctot;  // the chain's busy agents, split by carried load
    struct Cell {
      int agent, queue;
      double k, x;
    };
    std::vector<Cell> cells;
    std::vector<std::vector<size_t>> by_agent(pool.agents.size()), by_queue(nq);
    for (size_t ai = 0; ai < pool.agents.size(); ++ai) {
      const AAgent& ag = agents_[pool.agents[ai]];
      for (size_t k = 0; k < ag.queues.size(); ++k) {
        const int i = ag.queues[k];
        if (i < 0 || static_cast<size_t>(i) >= nq || base_.q[i].pool != static_cast<int>(pi) || col[i] <= 0) continue;
        const double tier = tiers && ag.tiers[k] != 1 ? 0.2 : 1.0;
        by_agent[ai].push_back(cells.size());
        by_queue[i].push_back(cells.size());
        cells.push_back(Cell{pool.agents[ai], i, tier * queues_[i].weight, tier * queues_[i].weight});
      }
    }
    const double row = busy / pool.c;
    for (int round = 0; round < 60; ++round) {
      double err = 0;
      for (int i : pool.queues) {
        double s = 0;
        for (size_t c : by_queue[i]) s += cells[c].x;
        if (s <= 0) continue;
        for (size_t c : by_queue[i]) cells[c].x *= col[i] / s;
      }
      for (const auto& list : by_agent) {
        double s = 0;
        for (size_t c : list) s += cells[c].x;
        if (s <= 0) continue;
        err = std::max(err, std::abs(s - row) / std::max(1e-9, row));
        // An agent can't be busy more than all the time: rows are capped, never inflated past 1.
        const double target = std::min(row, 1.0);
        for (size_t c : list) cells[c].x *= target / s;
      }
      if (err < 1e-3) break;
    }
    for (const auto& c : cells) base_.shares.push_back(Share{c.agent, c.queue, c.x});
    for (int i : pool.queues) {
      double s = 0;
      for (size_t c : by_queue[i]) s += cells[c].x;
      base_.q[i].busy = s;
      base_.q[i].m.busy = s;
      if (base_.q[i].normal.ok) base_.q[i].normal.busy = s;
      if (base_.q[i].vip.ok) base_.q[i].vip.busy = s;
    }
  }
  // Lever (b): a bound that binds changes the dispatch the model assumes.
  for (size_t i = 0; i < nq; ++i) {
    AgentQueue& r = base_.q[i];
    const AQueue& q = queues_[i];
    if (!r.m.ok) continue;
    if (q.min_agents > 0 && r.busy < q.min_agents && r.m.p_wait > 0.05) mark(&r.m.quality, &r.why, "min_agents binds");
    if (q.max_agents > 0 && r.busy > 0.8 * q.max_agents && r.pool >= 0 && base_.pools[r.pool].queues.size() > 1) {
      mark(&r.m.quality, &r.why, "max_agents binds");
    }
    r.normal.quality = worst(r.normal.quality, r.m.quality);
    if (r.vip.ok) r.vip.quality = worst(r.vip.quality, r.m.quality);
  }
  base_.quality = Quality::kExact;
  for (const auto& q : base_.q) {
    if (q.m.ok) base_.quality = worst(base_.quality, q.m.quality);
  }
}

void AgentModel::solve_pool(size_t pi) {
  const AgentPool& pool = base_.pools[pi];
  PoolCache& pc = cache_[pi];
  if (pool.lambda <= 0 || pool.c <= 0 || pool.mu <= 0) return;
  const QueueParams qp{pool.lambda, pool.mu, pool.theta, static_cast<double>(pool.c), -1, 0};
  pc.d = stationary(qp, pool.c, o_.q, st_);
  if (!pc.d.ok) return;
  pc.ok = true;
  const int K = static_cast<int>(pc.d.p.size()) - 1, c = pool.c;
  for (int n = c; n <= K; ++n) pc.pi_busy += pc.d.p[n];
  // VIP callers queue only behind VIP callers: on the all-busy regime the VIP queue is a birth-death
  // chain (births lambda_v, deaths c mu + m theta); at m = 0 a VIP waits only if all agents are
  // busy, with b = P(all busy | no VIP waiting) consistent with the pool's P(all busy).
  const int kv = std::max(1, K - c + 1);
  pc.vip_pos.assign(kv, 0.0);
  if (pool.vip_lambda > 0) {
    std::vector<double> g(kv, 0.0);
    double G = 0, gm = 1;
    for (int m = 1; m < kv; ++m) {
      gm *= pool.vip_lambda / (c * pool.mu + m * pool.theta);
      g[m] = gm;
      G += gm;
      if (gm < 1e-18 * std::max(1.0, G)) break;
    }
    const double pb = pc.pi_busy;
    const double b = pb / (1 + G * (1 - pb));
    const double p0 = 1 / (1 + b * G);
    pc.vip_pos[0] = p0 * b;
    for (int m = 1; m < kv; ++m) pc.vip_pos[m] = p0 * b * g[m];
  } else {
    pc.vip_pos[0] = pc.pi_busy;
  }
}

// Kleinrock's time-dependent priority on the all-busy regime: queue p's waits relative to FCFS's,
// with b = w / tau (higher first) and rho the normal callers' load per agent; equal b gives 1.
std::vector<double> AgentModel::kappas(const AgentPool& pool, const std::vector<double>& weights) const {
  const size_t n = pool.queues.size();
  std::vector<double> rho(n, 0.0), b(n, 1.0), W(n, 1.0);
  double rsum = 0;
  for (size_t k = 0; k < n; ++k) {
    const AQueue& q = queues_[pool.queues[k]];
    const double tau = q.sla_s > 0 ? q.sla_s : o_.q.sla_s;
    const double lam_n = std::max(0.0, q.lambda - std::min(q.vip_lambda, q.lambda));
    rho[k] = pool.mu > 0 && pool.c > 0 ? lam_n / (pool.mu * pool.c) : 0;
    rsum += rho[k];
    b[k] = weights[pool.queues[k]] / std::max(1e-6, tau);
  }
  bool equal = true;
  for (size_t k = 1; k < n; ++k) equal = equal && std::abs(b[k] - b[0]) <= 1e-12 * b[0];
  if (equal || rsum <= 0) return W;
  if (rsum > 0.98) {
    for (double& r : rho) r *= 0.98 / rsum;
  }
  std::vector<size_t> ord(n);
  std::iota(ord.begin(), ord.end(), 0);
  std::sort(ord.begin(), ord.end(), [&](size_t x, size_t y) { return b[x] < b[y]; });
  for (size_t pi = 0; pi < n; ++pi) {
    const size_t p = ord[pi];
    double num = 1, den = 1;
    for (size_t li = 0; li < pi; ++li) num -= rho[ord[li]] * W[ord[li]] * (1 - b[ord[li]] / b[p]);
    for (size_t hi = pi + 1; hi < n; ++hi) den -= rho[ord[hi]] * (1 - b[p] / b[ord[hi]]);
    W[p] = std::clamp(num / std::max(1e-3, den), 0.05, 20.0);
  }
  for (double& x : W) x = std::pow(x, o_.weight_damping);
  return W;
}

const Tagged& AgentModel::table(size_t pi, double scale, double minus, double theta_own, double sla_s, double short_s) {
  if (short_s < 0) short_s = o_.q.short_abandon_s;
  const long ks = std::lround(std::log(std::max(1e-6, scale)) * 400);
  const auto key = std::make_tuple(ks, std::lround(minus * 1e7), std::lround(theta_own * 1e7), std::lround(sla_s * 1000),
                                   std::lround(short_s * 1000), pi);
  auto it = tables_.find(key);
  if (it != tables_.end()) return it->second;
  const AgentPool& pool = base_.pools[pi];
  const PoolCache& pc = cache_[pi];
  const double sc = std::exp(ks / 400.0);
  const int K = static_cast<int>(pc.d.p.size()) - 1;
  const int kmax = std::max(1, K - pool.c + 1);
  std::vector<double> r(kmax);
  for (int j = 0; j < kmax; ++j) r[j] = (pool.c * pool.mu + j * pool.theta - minus) / sc;
  Options oq = o_.q;
  oq.sla_s = sla_s;
  oq.short_abandon_s = short_s;
  return tables_.emplace(key, tagged_rates(r, theta_own, oq, st_)).first->second;
}

namespace {

// One class's metrics in its pool: `idle` P(n) for n < c (light traffic per queue), `pos` P(all
// busy and the arrival's position k + 1), the class's tagged table.
struct ClassIn {
  const std::vector<double>* p = nullptr;  // the pool's distribution (n < c used)
  const std::vector<double>* pos = nullptr;
  const Tagged* tg = nullptr;
  int c = 0;
  double P = 0, R = 0, mu = 0, theta_own = 0, delay_s = 0, sla_s = 20, short_s = 5;
  bool tiers = false;
  double lambda_q = 0;  // the class's arrivals at this queue (for the loss formula)
};

QueueMetrics class_metrics(const ClassIn& in, double* handicap, double* overflow) {
  QueueMetrics m;
  m.ok = true;
  m.quality = in.tg->quality;
  m.s = in.R;
  const int c = in.c;
  const Tagged& tg = *in.tg;
  double P = in.tiers ? std::clamp(in.P, 0.0, in.R) : in.R;
  const double th = in.theta_own, mu = in.mu, tau = in.sla_s, dly = std::max(0.0, in.delay_s);
  // The secondary-delay leg (tiers): a call that found no idle primary but an idle secondary.
  const double nu = P * mu, tot = nu + th;
  double s_srv = 1, s_ab = 0, s_sl = 1, s_sh = 0, s_v = 0, s_m = 0;
  if (in.tiers && dly > 0) {
    const double e = tot > 0 ? std::exp(-tot * dly) : 1;
    s_ab = tot > 0 ? th / tot * (1 - e) : 0;
    const double by_primary = tot > 0 ? nu / tot * (1 - e) : 0;
    s_srv = by_primary + e;
    s_sl = dly <= tau ? s_srv : (tot > 0 ? nu / tot * (1 - std::exp(-tot * tau)) : 0);
    s_sh = tot > 0 ? th / tot * (1 - std::exp(-tot * std::min(in.short_s, dly))) : 0;
    s_v = nu > 0 ? (1 - std::exp(-nu * dly)) / nu : dly;
    s_m = (tot > 0 ? nu * (1 - e * (1 + tot * dly)) / (tot * tot) : 0) + dly * e;
  }
  const double b_prim = in.tiers && P < in.R ? loss(P, mu > 0 ? in.lambda_q / mu : 0) : 0;
  double sl = 0, ab = 0, ewt = 0, sh = 0, msum = 0, served = 0, pw = 0, hc = 0, ov = 0;
  const std::vector<double>& p = *in.p;
  // P(none of the idle agents may take it) for idle = 1, 2, ...: one running product each (the
  // hypergeometric terms), so the light-traffic part is O(c), not O(c^2), at 3,000 agents.
  double hr_run = 1, hpp_run = 1;
  for (int idle = 1; idle <= c; ++idle) {
    const int j = idle - 1;
    hr_run = c - in.R - j > 0 && c - j > 0 ? hr_run * (c - in.R - j) / (c - j) : 0;
    hpp_run = c - P - j > 0 && c - j > 0 ? hpp_run * (c - P - j) / (c - j) : 0;
    const int n = c - idle;
    if (n >= static_cast<int>(p.size())) continue;
    const double pn = p[n];
    if (pn <= 0) continue;
    const double hr = hr_run, hp = std::max(hpp_run, std::max(hr, b_prim));
    const double imm = std::max(0.0, 1 - hp), sec = std::max(0.0, hp - hr), none = std::max(0.0, hr);
    sl += pn * (imm + sec * s_sl + none * tg.u[0]);
    served += pn * (imm + sec * s_srv + none * tg.Ps[0]);
    ab += pn * (sec * s_ab + none * (1 - tg.Ps[0]));
    sh += pn * (sec * s_sh + none * tg.a[0]);
    ewt += pn * (sec * s_v + none * tg.V[0]);
    msum += pn * (sec * s_m + none * tg.m[0]);
    pw += pn * (sec + none);
    ov += pn * sec;
  }
  const std::vector<double>& pos = *in.pos;
  for (size_t k = 0; k < pos.size() && k < tg.u.size(); ++k) {
    const double pk = pos[k];
    if (pk <= 0) continue;
    sl += pk * tg.u[k];
    served += pk * tg.Ps[k];
    ab += pk * (1 - tg.Ps[k]);
    sh += pk * tg.a[k];
    ewt += pk * tg.V[k];
    msum += pk * tg.m[k];
    pw += pk;
    if (in.tiers && in.R > 0) hc += pk * (1 - P / in.R);
  }
  m.sl = sh < 1 ? sl / (1 - sh) : 0;
  m.abandon = ab;
  m.ewt_mean = ewt;
  m.asa = served > 0 ? msum / served : 0;
  m.p_wait = pw;
  if (handicap) *handicap = hc;
  if (overflow) *overflow = ov;
  return m;
}

QueueMetrics mix(const QueueMetrics& a, double wa, const QueueMetrics& b, double wb) {
  if (!b.ok || wb <= 0) return a;
  if (!a.ok || wa <= 0) return b;
  QueueMetrics m = a;
  const double t = wa + wb;
  auto f = [&](double x, double y) { return (wa * x + wb * y) / t; };
  m.quality = worst(a.quality, b.quality);
  m.sl = f(a.sl, b.sl);
  m.abandon = f(a.abandon, b.abandon);
  m.ewt_mean = f(a.ewt_mean, b.ewt_mean);
  m.asa = f(a.asa, b.asa);
  m.p_wait = f(a.p_wait, b.p_wait);
  m.lq = a.lq + b.lq;
  m.mean_wait_all = f(a.mean_wait_all, b.mean_wait_all);
  return m;
}

}  // namespace

double AgentModel::still_at_escalation(int i, double kappa) {
  const AgentQueue& b = base_.q[i];
  const AQueue& q = queues_[i];
  if (b.pool < 0 || q.lambda <= 0) return 0;
  const AgentPool& pool = base_.pools[b.pool];
  const PoolCache& pc = cache_[b.pool];
  if (!pc.ok || pool.c <= 0) return 0;
  const double tau = q.sla_s > 0 ? q.sla_s : o_.q.sla_s;
  const double E = q.escalate_s > 0 ? q.escalate_s : 3 * tau;
  // served within E (u) and abandoned within E (a), by position: what is left is escalated.
  const Tagged& tg = table(b.pool, kappa, pool.vip_lambda, q.theta, E, E);
  const int K = static_cast<int>(pc.d.p.size()) - 1, c = pool.c;
  double still = 0, none = 1;
  for (int idle = 1; idle <= c; ++idle) {
    const int j = idle - 1;
    none = c - b.R - j > 0 && c - j > 0 ? none * (c - b.R - j) / (c - j) : 0;
    if (none <= 0) break;
    const int n = c - idle;
    if (n <= K) still += pc.d.p[n] * none * std::max(0.0, 1 - tg.u[0] - tg.a[0]);
  }
  for (int n = c; n <= K; ++n) {
    const size_t k = static_cast<size_t>(n - c);
    if (k >= tg.u.size()) break;
    still += pc.d.p[n] * std::max(0.0, 1 - tg.u[k] - tg.a[k]);
  }
  return std::clamp(still, 0.0, 1.0);
}

AgentQueue AgentModel::queue_metrics(int i, double kappa, double delay_s, double esc_pool) {
  ++evals_;
  AgentQueue out;
  const AgentQueue& b = base_.q[i];
  out.pool = b.pool;
  out.P = b.P;
  out.R = b.R;
  out.busy = b.busy;
  out.nested = b.nested;
  out.kappa = kappa;
  const AQueue& q = queues_[i];
  const double lam_v = std::min(q.vip_lambda, q.lambda), lam_n = q.lambda - lam_v;
  const double tau = q.sla_s > 0 ? q.sla_s : o_.q.sla_s, tau_v = q.vip_sla_s > 0 ? q.vip_sla_s : tau;
  const bool tiers = o_.dispatch == Dispatch::kTiers;
  Options oq = o_.q;
  oq.sla_s = tau;
  if (q.lambda <= 0) {
    out.m.ok = out.normal.ok = true;
    out.m.sl = out.normal.sl = 1;
    out.m.s = out.R;
    return out;
  }
  if (b.pool < 0 || b.R <= 0) {
    out.m = stationary_metrics(QueueParams{q.lambda, q.mu, q.theta, 0, -1, 0}, oq, st_);
    out.normal = out.m;
    if (lam_v > 0) out.vip = out.m;
    out.why = "no agents";
    return out;
  }
  const AgentPool& pool = base_.pools[b.pool];
  const PoolCache& pc = cache_[b.pool];
  if (!pc.ok) {
    // No stationary law (an overloaded pool without abandonment): what the pool's arrivals meet.
    out.m = stationary_metrics(QueueParams{pool.lambda, pool.mu, pool.theta, static_cast<double>(pool.c), -1, 0}, oq, st_);
    out.normal = out.m;
    if (lam_v > 0) out.vip = out.m;
    return out;
  }
  const int K = static_cast<int>(pc.d.p.size()) - 1, c = pool.c;
  std::vector<double> pos_n(std::max(0, K - c + 1), 0.0);
  double lq_all = 0;
  for (int n = c; n <= K; ++n) {
    pos_n[n - c] = pc.d.p[n];
    lq_all += pc.d.p[n] * (n - c);
  }
  double lq_vip = 0;
  for (size_t m = 1; m < pc.vip_pos.size(); ++m) lq_vip += pc.vip_pos[m] * static_cast<double>(m);
  if (pool.vip_lambda <= 0) lq_vip = 0;
  ClassIn in;
  in.p = &pc.d.p;
  in.c = c;
  in.P = b.P;
  in.R = b.R;
  in.mu = pool.mu;
  in.theta_own = q.theta;
  in.delay_s = delay_s;
  in.short_s = o_.q.short_abandon_s;
  in.tiers = tiers;
  double hc = 0, ov = 0;
  if (lam_n > 0 || lam_v <= 0) {
    in.pos = &pos_n;
    in.tg = &table(b.pool, kappa, pool.vip_lambda, q.theta, tau);
    in.sla_s = tau;
    in.lambda_q = q.lambda;  // the primaries are busy with both classes
    out.normal = class_metrics(in, &hc, &ov);
    const double nshare = pool.lambda - pool.vip_lambda > 0 ? lam_n / (pool.lambda - pool.vip_lambda) : 0;
    out.normal.lq = std::max(0.0, lq_all - lq_vip) * nshare;
    out.normal.mean_wait_all = lam_n > 0 ? out.normal.lq / lam_n : 0;
    out.normal.busy = b.busy;
    out.normal.s = b.R;
    check_boundary(&out.normal, oq, st_);
  }
  if (lam_v > 0) {
    Options ov2 = o_.q;
    ov2.sla_s = tau_v;
    in.pos = &pc.vip_pos;
    // Escalated normal callers go ahead of VIP callers: they take that much of the service rate; and
    // only the R agents that reach the queue serve its VIP callers (the pool's rate scaled by R / c).
    in.tg = &table(b.pool, b.R > 0 ? pool.c / b.R : 1.0, esc_pool, q.theta, tau_v);
    in.sla_s = tau_v;
    in.lambda_q = q.lambda;
    double hv = 0, ovv = 0;
    out.vip = class_metrics(in, &hv, &ovv);
    out.vip.lq = lq_vip * (pool.vip_lambda > 0 ? lam_v / pool.vip_lambda : 0);
    out.vip.mean_wait_all = out.vip.lq / lam_v;
    out.vip.busy = b.busy;
    out.vip.s = b.R;
    check_boundary(&out.vip, ov2, st_);
  }
  out.m = mix(out.normal, lam_n, out.vip, lam_v);
  out.m.busy = b.busy;
  out.m.s = b.R;
  out.m.truncation_mass = pc.d.tail_mass;
  out.handicap = hc;
  out.overflow = ov;
  if (!out.m.ok) return out;
  Quality ql = pool.quality;
  std::string why = pool.why;
  if (b.nested) mark(&ql, &why, "nested reach");
  if (tiers && (hc > o_.tier_shift || ov > o_.tier_overflow)) mark(&ql, &why, "secondary-delay leg");
  if (q.vip_reserved > 0 && lam_v > 0) mark(&ql, &why, "VIP reserve");
  out.m.quality = worst(out.m.quality, ql);
  out.normal.quality = worst(out.normal.quality, ql);
  if (out.vip.ok) out.vip.quality = worst(out.vip.quality, ql);
  out.why = why;
  return out;
}

std::vector<AgentQueue> AgentModel::evaluate(const std::vector<double>& weights, const std::vector<double>& delays) {
  const size_t nq = queues_.size();
  std::vector<AgentQueue> out(nq);
  std::vector<double> kappa(nq, 1.0);
  std::vector<std::string> wwhy(nq);
  for (const AgentPool& pool : base_.pools) {
    const std::vector<double> k = kappas(pool, weights);
    double bmin = 1e300, bmax = 0;
    for (size_t x = 0; x < pool.queues.size(); ++x) {
      const int i = pool.queues[x];
      kappa[i] = k[x];
      if (queues_[i].lambda <= 0) continue;
      const double tau = queues_[i].sla_s > 0 ? queues_[i].sla_s : o_.q.sla_s;
      const double bb = weights[i] / std::max(1e-6, tau);
      bmin = std::min(bmin, bb);
      bmax = std::max(bmax, bb);
    }
    if (bmax > 0 && bmax > o_.weight_ratio * bmin) {
      for (int i : pool.queues) wwhy[i] = "weight ratio";
    }
  }
  // The escalated normal callers per pool (only where VIP callers are).
  std::vector<double> esc(base_.pools.size(), 0.0);
  for (size_t pi = 0; pi < base_.pools.size(); ++pi) {
    if (base_.pools[pi].vip_lambda <= 0) continue;
    for (int i : base_.pools[pi].queues) {
      const AQueue& q = queues_[i];
      const double lam_n = q.lambda - std::min(q.vip_lambda, q.lambda);
      if (lam_n > 0) esc[pi] += lam_n * still_at_escalation(i, kappa[i]);
    }
  }
  for (size_t i = 0; i < nq; ++i) {
    const int pi = base_.q[i].pool;
    out[i] = queue_metrics(static_cast<int>(i), kappa[i], i < delays.size() ? delays[i] : queues_[i].delay_s,
                           pi >= 0 ? esc[pi] : 0.0);
    if (!wwhy[i].empty() && out[i].m.ok) {
      mark(&out[i].m.quality, &out[i].why, wwhy[i]);
      out[i].normal.quality = worst(out[i].normal.quality, Quality::kApprox);
    }
  }
  return out;
}

QueueMetrics AgentModel::signature(int i, double able) {
  const AgentQueue& b = base_.q[i];
  const AQueue& q = queues_[i];
  QueueMetrics m;
  if (b.pool < 0 || able <= 0) {
    Options oq = o_.q;
    m = stationary_metrics(QueueParams{q.lambda, q.mu, q.theta, 0, -1, 0}, oq, st_);
    return m;
  }
  const PoolCache& pc = cache_[b.pool];
  const AgentPool& pool = base_.pools[b.pool];
  if (!pc.ok) return b.normal.ok ? b.normal : b.m;
  const double R = std::min(able, b.R);
  const double tau = q.sla_s > 0 ? q.sla_s : o_.q.sla_s;
  const int K = static_cast<int>(pc.d.p.size()) - 1, c = pool.c;
  std::vector<double> pos_n(std::max(0, K - c + 1), 0.0);
  for (int n = c; n <= K; ++n) pos_n[n - c] = pc.d.p[n];
  ClassIn in;
  in.p = &pc.d.p;
  in.pos = &pos_n;
  in.c = c;
  in.R = R;
  in.P = std::min(b.P, R);
  in.mu = pool.mu;
  in.theta_own = q.theta;
  in.delay_s = q.delay_s;
  in.short_s = o_.q.short_abandon_s;
  in.tiers = o_.dispatch == Dispatch::kTiers;
  in.sla_s = tau;
  in.lambda_q = q.lambda;
  in.tg = &table(b.pool, b.kappa * (b.R > 0 ? b.R / R : 1), pool.vip_lambda, q.theta, tau);
  m = class_metrics(in, nullptr, nullptr);
  Options oq = o_.q;
  oq.sla_s = tau;
  check_boundary(&m, oq, st_);
  if (m.ok) {
    m.quality = worst(m.quality, b.m.quality);
    if (R < b.R - 1e-9) m.quality = worst(m.quality, Quality::kApprox);  // nested reach within the queue
  }
  return m;
}

AgentModel::Tag AgentModel::tag(int i, bool vip) const {
  Tag t;
  const AgentQueue& b = base_.q[i];
  const AQueue& q = queues_[i];
  t.theta = q.theta;
  if (b.pool < 0) {
    t.mu = q.mu;
    return t;
  }
  const AgentPool& pool = base_.pools[b.pool];
  t.mu = pool.mu > 0 ? pool.mu : q.mu;
  const double lam_v = std::min(q.vip_lambda, q.lambda);
  if (vip) {
    t.s_tag = pool.vip_lambda > 0 ? pool.c * lam_v / pool.vip_lambda : b.R;
    return t;
  }
  // A caller of queue i at position k: its queue is served at its share of the pool's completions
  // (by arrivals, slowed or sped by kappa); same-queue VIP callers pass it.
  const double share = pool.lambda > 0 ? q.lambda / pool.lambda : (pool.c > 0 ? b.R / pool.c : 0);
  t.s_tag = pool.c * share / std::max(1e-6, b.kappa);
  t.lambda_hp = lam_v;
  return t;
}

AgentResult solve_agents(const std::vector<AQueue>& queues, const std::vector<AAgent>& agents, const AgentOptions& o,
                         Stats* st) {
  AgentModel m(queues, agents, o, st);
  return m.base();
}

double objective(const std::vector<AQueue>& q, const std::vector<AgentQueue>& m, const std::vector<Importance>& imp) {
  double j = 0;
  for (size_t i = 0; i < q.size() && i < m.size(); ++i) {
    const Importance w = i < imp.size() ? imp[i] : Importance{};
    const double lam_v = std::min(q[i].vip_lambda, q[i].lambda), lam_n = q[i].lambda - lam_v;
    if (m[i].normal.ok && lam_n > 0) j += lam_n * (w.alpha * (1 - m[i].normal.sl) + w.beta * m[i].normal.abandon);
    if (m[i].vip.ok && lam_v > 0) j += lam_v * (w.vip_alpha * (1 - m[i].vip.sl) + w.vip_beta * m[i].vip.abandon);
    if (!m[i].normal.ok && !m[i].vip.ok && m[i].m.ok) j += q[i].lambda * (w.alpha * (1 - m[i].m.sl) + w.beta * m[i].m.abandon);
  }
  return j;
}

}  // namespace pcr::model
