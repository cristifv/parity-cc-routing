#include "model/coupling.h"

#include <Eigen/Dense>
#include <algorithm>
#include <cmath>

namespace pcr::model {

namespace {

// E[min(n, s)] of the stationary chain at real s (interpolated); every server busy when the chain
// has no stationary law.
double busy_at(const CQueue& q, double s, const Options& o, Stats* st) {
  if (s <= 0 || q.lambda == 0) return 0;
  auto one = [&](int si) -> double {
    if (si == 0) return 0;
    QueueParams p{q.lambda, q.mu, q.theta, static_cast<double>(si), -1, 0};
    const Distribution d = stationary(p, si, o, st);
    if (!d.ok) return si;
    double b = 0;
    for (size_t n = 0; n < d.p.size(); ++n) b += d.p[n] * std::min<double>(n, si);
    return b;
  };
  const int lo = static_cast<int>(std::floor(s));
  const double f = s - lo;
  const double a = one(lo);
  return f > 1e-12 ? (1 - f) * a + f * one(lo + 1) : a;
}

// Level reduction (linear level reduction for a level-dependent QBD): levels n = o + l.
double lent_busy_int(double lambda, double mu, double theta, int own, int lent, int max_block,
                     bool* approx, bool prefer_own) {
  if (lent <= 0 || lambda <= 0) return 0;
  const int N = own + lent;
  if (std::min(own, lent) + 1 > max_block) {
    *approx = true;
    return -1;
  }
  auto omin = [&](int n) { return std::max(0, n - lent); };
  auto omax = [&](int n) { return std::min(n, own); };
  auto size = [&](int n) { return omax(n) - omin(n) + 1; };
  using M = Eigen::MatrixXd;
  std::vector<M> up(N + 1), dn(N + 1), diag(N + 1);
  for (int n = 0; n <= N; ++n) {
    const int b = size(n);
    diag[n] = M::Zero(b, b);
    if (n < N) up[n] = M::Zero(b, size(n + 1));
    if (n > 0) dn[n] = M::Zero(b, size(n - 1));
    for (int i = 0; i < b; ++i) {
      const int o = omin(n) + i, l = n - o;
      double out = 0;
      if (n < N) {  // the corner's arrivals go to the waiting tail (an excursion back to it)
        if (prefer_own || o == own || l == lent) {
          const int o2 = o < own ? o + 1 : o;
          up[n](i, o2 - omin(n + 1)) += lambda;
        } else {
          // Longest idle: any idle server, own or lent, in proportion to how many are idle.
          const double fo = static_cast<double>(own - o) / ((own - o) + (lent - l));
          up[n](i, (o + 1) - omin(n + 1)) += lambda * fo;
          up[n](i, o - omin(n + 1)) += lambda * (1 - fo);
        }
        out += lambda;
      }
      if (o > 0) {
        dn[n](i, (o - 1) - omin(n - 1)) += o * mu;
        out += o * mu;
      }
      if (l > 0) {
        dn[n](i, o - omin(n - 1)) += l * mu;
        out += l * mu;
      }
      diag[n](i, i) = -out;
    }
  }
  std::vector<M> R(N);  // pi_{n+1} = pi_n R_n
  M S = diag[N];
  for (int n = N - 1; n >= 0; --n) {
    const M inv = (-S).partialPivLu().inverse();
    R[n] = up[n] * inv;
    S = diag[n] + R[n] * dn[n + 1];
  }
  std::vector<Eigen::RowVectorXd> pi(N + 1);
  pi[0] = Eigen::RowVectorXd::Ones(1);  // level 0 is the single empty state
  for (int n = 0; n < N; ++n) pi[n + 1] = pi[n] * R[n];
  // The waiting tail hangs off the corner (own, lent): births lambda, deaths N mu + q theta.
  double tail = 0, m = 1;
  if (theta == 0 && lambda >= N * mu) return lent;
  for (int q = 1; q < 1'000'000; ++q) {
    m *= lambda / (N * mu + q * theta);
    tail += m;
    if (m < 1e-16 * (1 + tail)) break;
  }
  double z = 0, el = 0;
  for (int n = 0; n <= N; ++n) {
    for (int i = 0; i < pi[n].size(); ++i) {
      const double p = std::max(0.0, pi[n](i));
      z += p;
      el += p * (n - (omin(n) + i));
    }
  }
  const double corner = std::max(0.0, pi[N](0));
  z += corner * tail;
  el += corner * tail * lent;
  return z > 0 ? el / z : 0;
}

}  // namespace

double lent_busy(double lambda, double mu, double theta, double own, double lent, int max_block,
                 bool* approx, bool prefer_own, std::map<std::pair<int, int>, double>* memo) {
  own = std::max(0.0, own);
  lent = std::max(0.0, lent);
  if (lent <= 0 || lambda <= 0 || mu <= 0) return 0;
  const int o0 = static_cast<int>(std::floor(own)), l0 = static_cast<int>(std::floor(lent));
  const double fo = own - o0, fl = lent - l0;
  bool est = false;
  auto at = [&](int o, int l) {
    if (memo) {
      auto it = memo->find({o, l});
      if (it != memo->end()) return it->second;
    }
    double v = lent_busy_int(lambda, mu, theta, o, l, max_block, &est, prefer_own);
    if (v >= 0) {
      if (memo) (*memo)[{o, l}] = v;
      return v;
    }
    if (v >= 0) return v;
    // Too large for the exact chain: the borrowed servers' share of the aggregate busy.
    const CQueue q{lambda, mu, theta, 0};
    const double all = busy_at(q, o + l, Options{}, nullptr);
    v = std::max(0.0, all - busy_at(q, o, Options{}, nullptr));
    if (memo) (*memo)[{o, l}] = v;
    return v;
  };
  double v = (1 - fo) * (1 - fl) * at(o0, l0);
  if (fl > 1e-12) v += (1 - fo) * fl * at(o0, l0 + 1);
  if (fo > 1e-12) v += fo * (1 - fl) * at(o0 + 1, l0);
  if (fo > 1e-12 && fl > 1e-12) v += fo * fl * at(o0 + 1, l0 + 1);
  if (est) *approx = true;
  return std::min(v, lent);
}

void split_by_load(const std::vector<CQueue>& queues, std::vector<CGroup>* groups) {
  const size_t nq = queues.size();
  std::vector<double> residual(nq, 0.0);
  for (size_t i = 0; i < nq; ++i) residual[i] = queues[i].mu > 0 ? queues[i].lambda / queues[i].mu : 0;
  for (const auto& g : *groups) {
    if (g.queues.size() == 1 && g.queues[0] >= 0 && static_cast<size_t>(g.queues[0]) < nq) {
      residual[g.queues[0]] -= g.agents;
    }
  }
  for (auto& g : *groups) {
    g.primaries.assign(g.queues.size(), 0.0);
    if (g.queues.size() == 1) {
      g.primaries[0] = g.agents;
      continue;
    }
    double sum = 0;
    std::vector<double> w(g.queues.size());
    for (size_t k = 0; k < g.queues.size(); ++k) {
      const int i = g.queues[k];
      w[k] = (i >= 0 && static_cast<size_t>(i) < nq) ? std::max(1e-6, residual[i]) : 0;
      sum += w[k];
    }
    for (size_t k = 0; k < g.queues.size(); ++k) g.primaries[k] = sum > 0 ? g.agents * w[k] / sum : 0;
  }
}

CouplingResult solve_coupled(const std::vector<CQueue>& queues, const std::vector<CGroup>& groups,
                             const CouplingOptions& o, Stats* st) {
  CouplingResult res;
  const size_t nq = queues.size();
  res.q.resize(nq);
  for (const auto& q : queues) {
    if (!std::isfinite(q.lambda) || !std::isfinite(q.mu) || !std::isfinite(q.theta) || q.lambda < 0 ||
        q.mu < 0 || q.theta < 0) {
      res.why = "invalid queue parameters";
      if (st) ++st->refused;
      return res;
    }
  }
  std::vector<double> P(nq, 0.0);
  std::vector<std::vector<double>> share(nq, std::vector<double>(nq, 0.0));
  std::vector<bool> shared(nq, false);  // in a group with another queue
  for (const auto& g : groups) {
    if (g.queues.size() != g.primaries.size()) {
      res.why = "a group's primaries don't match its queues";
      return res;
    }
    double sum = 0;
    for (size_t k = 0; k < g.queues.size(); ++k) {
      const int j = g.queues[k];
      if (j < 0 || static_cast<size_t>(j) >= nq || !(g.primaries[k] >= 0)) {
        res.why = "a group names an unknown queue";
        return res;
      }
      sum += g.primaries[k];
      P[j] += g.primaries[k];
      if (g.queues.size() > 1 && g.agents > 0) shared[j] = true;
      const size_t others = g.queues.size() - 1;
      if (others == 0) continue;
      for (int i : g.queues) {
        if (i != j) share[j][i] += g.primaries[k] / others;
      }
    }
    if (sum > g.agents + 1e-9) {
      res.why = "a group's primaries exceed its agents";
      return res;
    }
  }
  for (size_t j = 0; j < nq; ++j) {
    for (size_t i = 0; i < nq; ++i) share[j][i] = P[j] > 0 ? share[j][i] / P[j] : 0;
  }
  std::vector<std::vector<double>> L(nq, std::vector<double>(nq, 0.0));
  if (o.warm && o.warm->size() == nq * nq) {
    for (size_t j = 0; j < nq; ++j) {
      for (size_t i = 0; i < nq; ++i) L[j][i] = std::isfinite((*o.warm)[j * nq + i]) ? std::max(0.0, (*o.warm)[j * nq + i]) : 0;
    }
  }
  std::vector<std::map<std::pair<int, int>, double>> memo(nq);
  std::vector<double> reach(nq, 0.0), s(nq), own(nq), c(nq), idle(nq);
  bool split_approx = false;
  auto compute = [&]() {
    for (size_t i = 0; i < nq; ++i) {
      double in = 0, out = 0;
      for (size_t j = 0; j < nq; ++j) {
        in += L[j][i];
        out += L[i][j];
      }
      own[i] = o.conserve ? P[i] - out : P[i];
      s[i] = own[i] + in;
      c[i] = s[i] + reach[i];
    }
  };
  for (int round = 1; round <= o.max_rounds; ++round) {
    compute();
    for (size_t j = 0; j < nq; ++j) idle[j] = std::max(0.0, s[j] - busy_at(queues[j], c[j], o.q, st));
    std::vector<std::vector<double>> nl(nq, std::vector<double>(nq, 0.0));
    std::vector<double> nr(nq, 0.0);
    for (size_t i = 0; i < nq; ++i) {
      double avail = 0;
      std::vector<double> a(nq, 0.0);
      for (size_t j = 0; j < nq; ++j) {
        if (j == i || share[j][i] == 0) continue;
        a[j] = L[j][i] + share[j][i] * idle[j];
        avail += a[j];
        nr[i] += share[j][i] * idle[j];
      }
      if (avail <= 0) continue;
      const double el = lent_busy(queues[i].lambda, queues[i].mu, queues[i].theta, own[i], avail,
                                  o.max_block, &split_approx, o.policy == Policy::kPrimaryFirst, &memo[i]);
      for (size_t j = 0; j < nq; ++j) nl[j][i] = el * a[j] / avail;
    }
    // A donor can't lend more than its primaries.
    for (size_t j = 0; j < nq; ++j) {
      double out = 0;
      for (size_t i = 0; i < nq; ++i) out += nl[j][i];
      if (out > P[j] && out > 0) {
        for (size_t i = 0; i < nq; ++i) nl[j][i] *= P[j] / out;
      }
    }
    double moved = 0;
    for (size_t j = 0; j < nq; ++j) {
      for (size_t i = 0; i < nq; ++i) {
        const double v = (1 - o.damping) * L[j][i] + o.damping * nl[j][i];
        moved = std::max(moved, std::abs(v - L[j][i]));
        L[j][i] = v;
      }
      const double r = (1 - o.damping) * reach[j] + o.damping * nr[j];
      moved = std::max(moved, std::abs(r - reach[j]));
      reach[j] = r;
    }
    res.rounds = round;
    if (moved < o.tol) {
      res.converged = true;
      break;
    }
  }
  compute();
  res.lent.assign(nq * nq, 0.0);
  for (size_t j = 0; j < nq; ++j) {
    for (size_t i = 0; i < nq; ++i) res.lent[j * nq + i] = L[j][i];
  }
  double sum_s = 0, sum_p = 0;
  for (size_t i = 0; i < nq; ++i) {
    sum_s += s[i];
    sum_p += P[i];
  }
  res.conservation_error = std::abs(sum_s - sum_p);
  res.conserved = res.conservation_error <= 1e-9 * std::max(1.0, sum_p);
  for (size_t j = 0; j < nq; ++j) {
    double out = 0;
    for (size_t i = 0; i < nq; ++i) {
      if (L[j][i] < -1e-12) res.conserved = false;
      out += L[j][i];
    }
    if (out > P[j] + 1e-9) res.conserved = false;
  }
  // The solve's own flags apply to every queue alike; each queue then adds its own rules, and the
  // tenant's quality is the worst queue's, computed after the loop (CR-097: folding it in while
  // looping made a dedicated queue approx whenever an earlier queue was).
  const Quality global = (!res.converged || !res.conserved || split_approx) ? Quality::kApprox : Quality::kExact;
  res.ok = true;
  for (size_t i = 0; i < nq; ++i) {
    CoupledQueue& cq = res.q[i];
    cq.P = P[i];
    cq.s = s[i];
    cq.reach = reach[i];
    cq.c = c[i];
    double in = 0, out = 0;
    for (size_t j = 0; j < nq; ++j) {
      in += L[j][i];
      out += L[i][j];
    }
    cq.lent_in = in;
    cq.lent_out = o.conserve ? out : 0;
    // Who serves this queue's head next: its reachable capacity, plus its lent-out agents when they
    // come back to it first (tiers); under longest idle a freed agent takes the older head, about
    // half the time this queue's.
    cq.s_tag = c[i] + (o.policy == Policy::kPrimaryFirst ? 1.0 : 0.5) * cq.lent_out;
    cq.secondary_share = c[i] > 0 ? (c[i] - own[i]) / c[i] : 0;
    cq.lent_out_share = P[i] > 0 ? cq.lent_out / P[i] : 0;
    const CQueue& q = queues[i];
    cq.m = stationary_metrics(QueueParams{q.lambda, q.mu, q.theta, c[i], cq.s_tag, q.lambda_hp}, o.q, st);
    if (!cq.m.ok) {
      res.ok = false;
      res.why = "queue " + std::to_string(i) + ": " + cq.m.why;
      continue;
    }
    // Phase 1's dispatch (longest idle, oldest head) is not the primary-first policy the coupling
    // models. The §3.6 matrix (2026-10-31) showed every side of it off: receivers, donors (N design:
    // the flexible group's own queue 7-9 SL points high while labelled exact) and, in overload, the
    // flexible group's split itself (oldest head shares it by waiting time, not by residual load).
    // So under kLongestIdle any queue that shares a group with another queue is approx (CR-087).
    if (o.policy == Policy::kLongestIdle ? shared[i] : cq.secondary_share > o.approx_share) {
      cq.m.quality = worst(cq.m.quality, Quality::kApprox);
    }
    if (q.mu > 0 && c[i] > 0 && q.lambda >= o.near_critical * q.mu * c[i]) {
      cq.m.quality = worst(cq.m.quality, Quality::kApprox);
    }
    cq.m.quality = worst(cq.m.quality, global);
  }
  res.quality = global;
  for (const auto& cq : res.q) {
    if (cq.m.ok) res.quality = worst(res.quality, cq.m.quality);
  }
  return res;
}

}  // namespace pcr::model
