#include "model/oracle.h"

#include <Eigen/Sparse>
#include <Eigen/SparseLU>
#include <algorithm>
#include <cmath>
#include <map>
#include <tuple>
#include <vector>

#include "model/ctmc.h"

namespace pcr::model {

namespace {

using Sp = Eigen::SparseMatrix<double>;
using Trip = Eigen::Triplet<double>;
using Vec = Eigen::VectorXd;

// A transient chain with two absorbing outcomes (served, abandoned): the block over transient
// states (diagonal included) and the rates into each outcome.
struct Absorbing {
  Sp g;
  Vec to_served, to_abandoned;
};

// P(absorbed in `served` within t) (f_served = 1, f_ab = 0) or in `abandoned` (0, 1), for every
// starting state at once: backward uniformization.
Vec absorbed_within(const Absorbing& a, double t, double f_served, double f_ab) {
  const int n = static_cast<int>(a.g.rows());
  double lam = 0;
  for (int i = 0; i < n; ++i) lam = std::max(lam, -a.g.coeff(i, i));
  Vec out = Vec::Zero(n);
  if (lam == 0 || t <= 0) return out;
  const PoissonWeights fg = fox_glynn(lam * t, 1e-12);
  Vec f = Vec::Zero(n);
  const Vec c = (a.to_served * f_served + a.to_abandoned * f_ab) / lam;
  for (int k = 0; k <= fg.right; ++k) {
    if (k >= fg.left) out += fg.w[k - fg.left] * f;
    if (k == fg.right) break;
    f = f + (a.g * f) / lam + c;
  }
  return out;
}

// Expected time to absorption (the virtual wait when the tagged caller's own patience is off).
Vec time_to_absorb(const Absorbing& a) {
  Sp m = -a.g;
  Eigen::SparseLU<Sp> lu;
  lu.compute(m);
  if (lu.info() != Eigen::Success) return Vec();
  return lu.solve(Vec::Ones(a.g.rows()));
}

}  // namespace

NDesignResult solve_ndesign(const NDesign& d) {
  NDesignResult r;
  using St = std::tuple<int, int, int, int, int>;  // b11, b21, b22, q1, q2
  std::vector<St> states;
  std::map<St, int> idx;
  auto add = [&](St s) {
    idx.emplace(s, static_cast<int>(states.size()));
    states.push_back(s);
  };
  for (int b11 = 0; b11 <= d.n1; ++b11)
    for (int b21 = 0; b21 <= d.n2; ++b21)
      for (int b22 = 0; b22 + b21 <= d.n2; ++b22) add({b11, b21, b22, 0, 0});
  for (int q2 = 1; q2 <= d.k2; ++q2)
    for (int b11 = 0; b11 <= d.n1; ++b11)
      for (int b21 = 0; b21 <= d.n2; ++b21) add({b11, b21, d.n2 - b21, 0, q2});
  for (int q1 = 1; q1 <= d.k1; ++q1)
    for (int q2 = 0; q2 <= d.k2; ++q2)
      for (int b21 = 0; b21 <= d.n2; ++b21) add({d.n1, b21, d.n2 - b21, q1, q2});
  const int n = static_cast<int>(states.size());
  r.states = states.size();
  std::vector<Trip> tr;
  std::vector<double> out(n, 0.0);
  auto go = [&](int from, St to, double rate) {
    if (rate <= 0) return;
    auto it = idx.find(to);
    if (it == idx.end()) return;  // truncation: blocked
    tr.emplace_back(it->second, from, rate);  // Q^T
    out[from] += rate;
  };
  for (int i = 0; i < n; ++i) {
    const auto [b11, b21, b22, q1, q2] = states[i];
    // Arrivals to queue 1: group 1 first, then an idle group-2 agent, else wait.
    if (b11 < d.n1) go(i, {b11 + 1, b21, b22, q1, q2}, d.lambda1);
    else if (b21 + b22 < d.n2) go(i, {b11, b21 + 1, b22, q1, q2}, d.lambda1);
    else go(i, {b11, b21, b22, q1 + 1, q2}, d.lambda1);
    // Arrivals to queue 2: group 2 only.
    if (b21 + b22 < d.n2) go(i, {b11, b21, b22 + 1, q1, q2}, d.lambda2);
    else go(i, {b11, b21, b22, q1, q2 + 1}, d.lambda2);
    // Group 1 ends a call: the next queue-1 caller, else idle.
    if (b11 > 0) go(i, q1 > 0 ? St{b11, b21, b22, q1 - 1, q2} : St{b11 - 1, b21, b22, q1, q2}, b11 * d.mu1);
    // Group 2 ends a queue-1 call: queue 2 first, then queue 1, else idle.
    if (b21 > 0) {
      const St to = q2 > 0 ? St{b11, b21 - 1, b22 + 1, q1, q2 - 1}
                  : q1 > 0 ? St{b11, b21, b22, q1 - 1, q2}
                           : St{b11, b21 - 1, b22, q1, q2};
      go(i, to, b21 * d.mu1);
    }
    if (b22 > 0) {
      const St to = q2 > 0 ? St{b11, b21, b22, q1, q2 - 1}
                  : q1 > 0 ? St{b11, b21 + 1, b22 - 1, q1 - 1, q2}
                           : St{b11, b21, b22 - 1, q1, q2};
      go(i, to, b22 * d.mu2);
    }
    if (q1 > 0) go(i, {b11, b21, b22, q1 - 1, q2}, q1 * d.theta1);
    if (q2 > 0) go(i, {b11, b21, b22, q1, q2 - 1}, q2 * d.theta2);
  }
  // pi Q = 0, sum pi = 1: Q^T pi = 0 with the last equation replaced by the normalization.
  std::vector<Trip> a;
  a.reserve(tr.size() + 2 * n);
  for (const auto& t : tr) {
    if (t.row() != n - 1) a.push_back(t);
  }
  for (int i = 0; i < n; ++i) {
    if (i != n - 1) a.emplace_back(i, i, -out[i]);
    a.emplace_back(n - 1, i, 1.0);
  }
  Sp A(n, n);
  A.setFromTriplets(a.begin(), a.end());
  Eigen::SparseLU<Sp> lu;
  lu.analyzePattern(A);
  lu.factorize(A);
  if (lu.info() != Eigen::Success) {
    r.why = "SparseLU failed";
    return r;
  }
  Vec rhs = Vec::Zero(n);
  rhs(n - 1) = 1;
  Vec pi = lu.solve(rhs);
  double tot = 0;
  for (int i = 0; i < n; ++i) {
    pi(i) = std::max(0.0, pi(i));
    tot += pi(i);
  }
  pi /= tot;
  double eq1 = 0, eq2 = 0, lent = 0, busy2 = 0;
  for (int i = 0; i < n; ++i) {
    const auto [b11, b21, b22, q1, q2] = states[i];
    eq1 += pi(i) * q1;
    eq2 += pi(i) * q2;
    lent += pi(i) * b21;
    busy2 += pi(i) * (b21 + b22);
    if (q1 == d.k1 || q2 == d.k2) r.boundary_mass += pi(i);
  }
  r.abandon1 = d.lambda1 > 0 ? d.theta1 * eq1 / d.lambda1 : 0;
  r.abandon2 = d.lambda2 > 0 ? d.theta2 * eq2 / d.lambda2 : 0;
  r.lent_busy = lent;
  r.idle2 = d.n2 - busy2;

  // Queue 1's tagged caller: (position j, queue-2 waiting, group 2 on queue 2), everyone busy.
  auto q1_chain = [&](bool own_patience) {
    using T = std::tuple<int, int, int>;
    std::map<T, int> id;
    for (int j = 1; j <= d.k1 + 1; ++j)
      for (int q2 = 0; q2 <= d.k2; ++q2)
        for (int b22 = 0; b22 <= d.n2; ++b22) id.emplace(T{j, q2, b22}, static_cast<int>(id.size()));
    const int m = static_cast<int>(id.size());
    Absorbing ab{Sp(m, m), Vec::Zero(m), Vec::Zero(m)};
    std::vector<Trip> t;
    std::vector<double> o(m, 0.0);
    for (const auto& [k, i] : id) {
      const auto [j, q2, b22] = k;
      const int b21 = d.n2 - b22;
      auto move = [&](int dst_j, int dst_q2, int dst_b22, double rate) {
        if (rate <= 0) return;
        o[i] += rate;
        if (dst_j == 0) {
          ab.to_served(i) += rate;
          return;
        }
        auto it = id.find(T{dst_j, dst_q2, dst_b22});
        if (it == id.end()) {
          o[i] -= rate;  // blocked at the truncation
          return;
        }
        t.emplace_back(i, it->second, rate);
      };
      move(j - 1, q2, b22, d.n1 * d.mu1);  // group 1 takes queue 1's head
      if (q2 > 0) move(j, q2 - 1, b22 + 1, b21 * d.mu1);
      else move(j - 1, q2, b22, b21 * d.mu1);
      if (q2 > 0) move(j, q2 - 1, b22, b22 * d.mu2);
      else move(j - 1, q2, b22 - 1, b22 * d.mu2);
      move(j, q2 + 1, b22, d.lambda2);
      if (q2 > 0) move(j, q2 - 1, b22, q2 * d.theta2);
      if (j > 1) move(j - 1, q2, b22, (j - 1) * d.theta1);
      if (own_patience) {
        ab.to_abandoned(i) += d.theta1;
        o[i] += d.theta1;
      }
    }
    for (int i = 0; i < m; ++i) t.emplace_back(i, i, -o[i]);
    ab.g.setFromTriplets(t.begin(), t.end());
    return std::make_pair(id, ab);
  };
  // Queue 2's tagged caller: (position j, group 2 on queue 2), group 2 all busy.
  auto q2_chain = [&](bool own_patience) {
    using T = std::tuple<int, int>;
    std::map<T, int> id;
    for (int j = 1; j <= d.k2 + 1; ++j)
      for (int b22 = 0; b22 <= d.n2; ++b22) id.emplace(T{j, b22}, static_cast<int>(id.size()));
    const int m = static_cast<int>(id.size());
    Absorbing ab{Sp(m, m), Vec::Zero(m), Vec::Zero(m)};
    std::vector<Trip> t;
    std::vector<double> o(m, 0.0);
    for (const auto& [k, i] : id) {
      const auto [j, b22] = k;
      const int b21 = d.n2 - b22;
      auto move = [&](int dst_j, int dst_b22, double rate) {
        if (rate <= 0) return;
        o[i] += rate;
        if (dst_j == 0) {
          ab.to_served(i) += rate;
          return;
        }
        t.emplace_back(i, id.at(T{dst_j, dst_b22}), rate);
      };
      move(j - 1, b22 + 1, b21 * d.mu1);  // a queue-1 call ends: that agent takes queue 2's head
      move(j - 1, b22, b22 * d.mu2);
      if (j > 1) move(j - 1, b22, (j - 1) * d.theta2);
      if (own_patience) {
        ab.to_abandoned(i) += d.theta2;
        o[i] += d.theta2;
      }
    }
    for (int i = 0; i < m; ++i) t.emplace_back(i, i, -o[i]);
    ab.g.setFromTriplets(t.begin(), t.end());
    return std::make_pair(id, ab);
  };

  const auto [id1, c1] = q1_chain(true);
  const auto [id1v, c1v] = q1_chain(false);
  const Vec u1 = absorbed_within(c1, d.sla_s, 1, 0), a1 = absorbed_within(c1, d.short_abandon_s, 0, 1);
  const Vec v1 = time_to_absorb(c1v);
  const auto [id2, c2] = q2_chain(true);
  const auto [id2v, c2v] = q2_chain(false);
  const Vec u2 = absorbed_within(c2, d.sla_s, 1, 0), a2 = absorbed_within(c2, d.short_abandon_s, 0, 1);
  const Vec v2 = time_to_absorb(c2v);
  if (v1.size() == 0 || v2.size() == 0) {
    r.why = "virtual wait solve failed";
    return r;
  }
  double sl1 = 0, sh1 = 0, w1 = 0, sl2 = 0, sh2 = 0, w2 = 0;
  for (int i = 0; i < n; ++i) {
    const auto [b11, b21, b22, q1, q2] = states[i];
    const double p = pi(i);
    if (b11 < d.n1 || b21 + b22 < d.n2) {
      sl1 += p;
    } else {
      const int k = id1.at({q1 + 1, q2, b22});
      sl1 += p * u1(k);
      sh1 += p * a1(k);
      w1 += p * v1(id1v.at({q1 + 1, q2, b22}));
    }
    if (b21 + b22 < d.n2) {
      sl2 += p;
    } else {
      const int k = id2.at({q2 + 1, b22});
      sl2 += p * u2(k);
      sh2 += p * a2(k);
      w2 += p * v2(id2v.at({q2 + 1, b22}));
    }
  }
  r.sl1 = sl1 / (1 - sh1);
  r.sl2 = sl2 / (1 - sh2);
  r.ewt1 = w1;
  r.ewt2 = w2;
  r.ok = r.boundary_mass < 1e-6;
  if (!r.ok) r.why = "truncation too small: raise k1/k2";
  return r;
}

}  // namespace pcr::model
