#include "model/ctmc.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace pcr::model {

namespace {

bool finite_nonneg(double x) { return std::isfinite(x) && x >= 0; }

bool valid(const QueueParams& q) {
  return finite_nonneg(q.lambda) && finite_nonneg(q.mu) && finite_nonneg(q.theta) &&
         finite_nonneg(q.s) && std::isfinite(q.s_tag) && finite_nonneg(q.lambda_hp);
}

// One integer-server chain's metrics (no interpolation, no boundary check).
// The tagged tables don't depend on time: a transient computes them once for every instant.
Tagged tagged_for(const QueueParams& q, int s, int K, const Options& o, Stats* st) {
  const int kmax = std::max(1, K - s + 1);
  const double s_tag = q.s_tag >= 0 ? q.s_tag : static_cast<double>(s);
  return tagged(s_tag, q.mu, q.theta, q.lambda_hp, kmax, o, st);
}

QueueMetrics metrics_int(const QueueParams& q, int s, const Distribution& d, const Options& o,
                         Stats* st, const Tagged* pre = nullptr) {
  QueueMetrics m;
  m.ok = true;
  m.quality = d.quality;
  m.truncation_mass = d.tail_mass;
  m.s = s;
  const int K = static_cast<int>(d.p.size()) - 1;
  Tagged local;
  if (!pre || static_cast<int>(pre->V.size()) < std::max(1, K - s + 1)) {
    local = tagged_for(q, s, K, o, st);
    pre = &local;
  }
  const Tagged& tg = *pre;
  m.quality = worst(m.quality, tg.quality);
  double sl = 0, ab = 0, ewt = 0, shrt = 0, msum = 0, served = 0, pw = 0, busy = 0, lq = 0;
  for (int n = 0; n <= K; ++n) {
    const double p = d.p[n];
    busy += p * std::min(n, s);
    if (n < s) {
      sl += p;
      served += p;
      continue;
    }
    const int k = n - s + 1;
    lq += p * (n - s);
    pw += p;
    sl += p * tg.u[k - 1];
    ab += p * (1 - tg.Ps[k - 1]);
    ewt += p * tg.V[k - 1];
    shrt += p * tg.a[k - 1];
    msum += p * tg.m[k - 1];
    served += p * tg.Ps[k - 1];
  }
  m.sl = shrt < 1 ? sl / (1 - shrt) : 0;
  m.abandon = ab;
  m.ewt_mean = ewt;
  m.asa = served > 0 ? msum / served : 0;
  m.p_wait = pw;
  m.busy = busy;
  m.lq = lq;
  m.mean_wait_all = q.lambda > 0 ? lq / q.lambda : 0;
  return m;
}

// No servers (or no service): every caller abandons or waits forever (PLAN §3.7).
QueueMetrics no_service(const QueueParams& q, const Options& o) {
  QueueMetrics m;
  m.ok = true;
  m.quality = Quality::kLong;
  m.why = "no servers";
  m.sl = 0;
  m.abandon = q.theta > 0 ? 1 : 0;
  m.ewt_mean = o.long_wait_s;
  m.asa = 0;
  m.p_wait = 1;
  m.busy = 0;
  // Calls present: lambda/theta waiting (all abandoning) or growing without bound.
  m.lq = q.theta > 0 ? q.lambda / q.theta : q.lambda * o.long_wait_s;
  m.mean_wait_all = q.theta > 0 ? 1 / q.theta : o.long_wait_s;
  return m;
}

QueueMetrics lerp(const QueueMetrics& a, const QueueMetrics& b, double f) {
  QueueMetrics m = a;
  auto mix = [f](double x, double y) { return (1 - f) * x + f * y; };
  m.ok = a.ok && b.ok;
  m.why = !a.ok ? a.why : b.why;
  m.quality = worst(a.quality, b.quality);
  m.truncation_mass = std::max(a.truncation_mass, b.truncation_mass);
  m.sl = mix(a.sl, b.sl);
  m.abandon = mix(a.abandon, b.abandon);
  m.ewt_mean = mix(a.ewt_mean, b.ewt_mean);
  m.asa = mix(a.asa, b.asa);
  m.p_wait = mix(a.p_wait, b.p_wait);
  m.busy = mix(a.busy, b.busy);
  m.lq = mix(a.lq, b.lq);
  m.mean_wait_all = mix(a.mean_wait_all, b.mean_wait_all);
  return m;
}

QueueMetrics refuse(std::string why, Stats* st) {
  QueueMetrics m;
  m.ok = false;
  m.why = std::move(why);
  if (st) ++st->refused;
  return m;
}

}  // namespace

const char* quality_name(Quality q) {
  switch (q) {
    case Quality::kExact: return "exact";
    case Quality::kApprox: return "approx";
    case Quality::kOverloaded: return "overloaded";
    case Quality::kLong: return "long";
  }
  return "?";
}

// ======================================================================================
// Fox–Glynn
// ======================================================================================

PoissonWeights fox_glynn(double lt, double eps) {
  PoissonWeights out;
  if (!(lt > 0) || !std::isfinite(lt)) {
    out.w = {1.0};
    return out;
  }
  const int mode = static_cast<int>(std::floor(lt));
  // Weights relative to the mode (w[mode] = 1): never underflow, normalized at the end.
  std::vector<double> down;  // mode-1, mode-2, ...
  double sum = 1, w = 1;
  int k = mode;
  while (k > 0) {
    const double q = k / lt;  // ratio of the next term down; decreasing further down
    if (q < 1 && w * q / (1 - q) <= 0.5 * eps * sum) break;
    w *= q;
    down.push_back(w);
    sum += w;
    --k;
  }
  const int left = k;
  std::vector<double> up;  // mode+1, ...
  w = 1;
  k = mode;
  for (;;) {
    const double q = lt / (k + 1);  // ratio of the next term up; decreasing further up
    if (q < 1 && w * q / (1 - q) <= 0.5 * eps * sum) break;
    w *= q;
    up.push_back(w);
    sum += w;
    ++k;
  }
  out.left = left;
  out.right = k;
  out.w.reserve(down.size() + 1 + up.size());
  for (auto it = down.rbegin(); it != down.rend(); ++it) out.w.push_back(*it / sum);
  out.w.push_back(1 / sum);
  for (double x : up) out.w.push_back(x / sum);
  return out;
}

// ======================================================================================
// Birth–death chain
// ======================================================================================

double down_rate(const QueueParams& q, int s, int n) {
  if (n <= 0) return 0;
  return q.mu * std::min(n, s) + q.theta * std::max(0, n - s);
}

Distribution stationary(const QueueParams& q, int s, const Options& o, Stats* st) {
  Distribution d;
  if (st) ++st->solves;
  if (!valid(q) || s < 0) {
    d.why = "invalid parameters";
    return d;
  }
  if (q.lambda == 0) {
    d.p = {1.0};
    d.ok = true;
    return d;
  }
  const bool serves = (s > 0 && q.mu > 0) || q.theta > 0;
  if (!serves) {
    d.quality = Quality::kLong;
    d.why = "no service and no abandonment";
    return d;
  }
  if (q.theta == 0 && q.lambda >= s * q.mu) {
    d.quality = Quality::kOverloaded;
    d.why = "theta = 0 and rho >= 1: no stationary law";
    return d;
  }
  const double ll = std::log(q.lambda);
  std::vector<double> lp{0.0};
  double lmax = 0, sum = 1;  // sum of exp(lp - lmax)
  int K = o.k_cap;
  double tail = 1;
  for (int n = 1; n <= o.k_cap; ++n) {
    const double v = lp.back() + ll - std::log(down_rate(q, s, n));
    lp.push_back(v);
    if (v > lmax) {
      sum = sum * std::exp(lmax - v) + 1;
      lmax = v;
    } else {
      sum += std::exp(v - lmax);
    }
    if (n < s) continue;
    const double r = q.lambda / down_rate(q, s, n + 1);  // the ratios only fall from here on
    if (r >= 1) continue;
    const double rel = std::exp(v - lmax) / sum * r / (1 - r);
    if (rel <= o.tail_target) {
      K = n;
      tail = rel;
      break;
    }
    tail = rel;
  }
  if (static_cast<int>(lp.size()) - 1 >= o.k_cap && tail > o.tail_eps) {
    d.quality = Quality::kApprox;
    d.why = "truncation cap reached";
  }
  K = static_cast<int>(lp.size()) - 1;
  d.p.resize(K + 1);
  double total = 0;
  for (int n = 0; n <= K; ++n) {
    d.p[n] = std::exp(lp[n] - lmax);
    total += d.p[n];
  }
  for (double& x : d.p) x /= total;
  d.tail_mass = tail;
  d.ok = tail <= o.refuse_tail;
  if (!d.ok) d.why = "truncation mass above the refusal bound";
  return d;
}

int transient_k(const QueueParams& q, int s, int n0, double t, const Options& o) {
  const double lt = q.lambda * t;
  int k = n0 + static_cast<int>(std::ceil(lt + 8 * std::sqrt(lt))) + 1;
  k = std::max(k, s + 1);
  Options so = o;
  so.k_cap = std::min(o.k_cap, 4 * std::max(k, 64));
  const Distribution d = stationary(q, s, so, nullptr);
  if (d.ok) k = std::max(k, static_cast<int>(d.p.size()) - 1);
  return std::min(k, o.k_cap);
}

Distribution transient(const QueueParams& q, int s, const std::vector<double>& p0, double t,
                       const Options& o, Stats* st) {
  Distribution d;
  if (st) ++st->solves;
  if (!valid(q) || s < 0 || p0.empty() || !(t >= 0) || !std::isfinite(t)) {
    d.why = "invalid parameters";
    return d;
  }
  const int K = static_cast<int>(p0.size()) - 1;
  std::vector<double> up(K + 1), dn(K + 1), out(K + 1, 0.0);
  double lam = 0;
  for (int n = 0; n <= K; ++n) {
    up[n] = n < K ? q.lambda : 0;  // births blocked at the truncation
    dn[n] = down_rate(q, s, n);
    lam = std::max(lam, up[n] + dn[n]);  // CR-026: the arrival rate is part of every exit rate
  }
  if (lam == 0 || t == 0) {
    d.p = p0;
    d.ok = true;
    return d;
  }
  const double lt = lam * t;
  if (lt > o.max_lambda_t) {
    d.quality = Quality::kApprox;
    d.why = "Lambda t above the uniformization cap";
    return d;
  }
  const PoissonWeights fg = fox_glynn(lt, o.fg_eps);
  std::vector<double> v = p0, nx(K + 1);
  for (int k = 0; k <= fg.right; ++k) {
    if (k >= fg.left) {
      const double w = fg.w[k - fg.left];
      for (int n = 0; n <= K; ++n) out[n] += w * v[n];
    }
    if (k == fg.right) break;
    for (int n = 0; n <= K; ++n) {
      double x = v[n] * (1 - (up[n] + dn[n]) / lam);
      if (n > 0) x += v[n - 1] * up[n - 1] / lam;
      if (n < K) x += v[n + 1] * dn[n + 1] / lam;
      nx[n] = x;
    }
    v.swap(nx);
  }
  double mass = 0, mass0 = 0;
  for (double x : p0) mass0 += x;
  for (double& x : out) {
    if (x < 0) {
      x = 0;
      if (st) ++st->clamps;
    }
    mass += x;
  }
  d.p = std::move(out);
  d.tail_mass = d.p[K];
  d.ok = std::abs(mass - mass0) <= o.mass_tol + 2 * o.fg_eps;
  if (!d.ok) {
    d.quality = Quality::kApprox;
    d.why = "probability mass not conserved";
  }
  return d;
}

// ======================================================================================
// Tagged customer
// ======================================================================================

std::vector<double> ewt_by_position(double s_tag, double mu, double theta, double lambda_hp,
                                    int kmax, double long_wait_s, Quality* qual) {
  std::vector<double> v(std::max(0, kmax));
  *qual = Quality::kExact;
  const double r1 = s_tag * mu - lambda_hp;
  if (!(r1 > 0) || !std::isfinite(r1)) {
    *qual = Quality::kLong;
    std::fill(v.begin(), v.end(), long_wait_s);  // a lower bound, never 0 (CR-013)
    return v;
  }
  double acc = 0;
  for (int j = 1; j <= kmax; ++j) {
    acc += 1 / (r1 + (j - 1) * theta);
    v[j - 1] = acc;
  }
  return v;
}

Tagged tagged(double s_tag, double mu, double theta, double lambda_hp, int kmax, const Options& o,
              Stats* st) {
  Tagged t;
  kmax = std::max(1, kmax);
  t.V = ewt_by_position(s_tag, mu, theta, lambda_hp, kmax, o.long_wait_s, &t.quality);
  t.Ps.assign(kmax, 0);
  t.u.assign(kmax, 0);
  t.a.assign(kmax, 0);
  t.m.assign(kmax, 0);
  if (t.quality == Quality::kLong) {
    const double ab = theta > 0 ? 1 - std::exp(-theta * o.short_abandon_s) : 0;
    std::fill(t.a.begin(), t.a.end(), ab);
    return t;
  }
  std::vector<double> r(kmax), qk(kmax);
  for (int j = 0; j < kmax; ++j) {
    r[j] = s_tag * mu - lambda_hp + j * theta;
    qk[j] = r[j] + theta;
  }
  double ps_prev = 1, m_prev = 0;
  for (int k = 0; k < kmax; ++k) {
    const double q = qk[k];
    t.m[k] = (r[k] / q) * (ps_prev / q + m_prev);
    t.Ps[k] = ps_prev * r[k] / q;
    ps_prev = t.Ps[k];
    m_prev = t.m[k];
  }
  // u(tau) and a(short): the backward equation by uniformization over every position at once.
  // (P f)_k = (1 - q_k / L) f_k + (r_k / L) f_{k-1} + (theta / L) f_abandoned, f_0 = f_served.
  const double lam = qk[kmax - 1];
  auto backward = [&](double tau, double f_served, double f_ab, std::vector<double>* res) {
    const double lt = lam * tau;
    const PoissonWeights fg = fox_glynn(lt, o.fg_eps);
    std::vector<double> f(kmax, 0.0), nx(kmax);
    std::vector<double>& out = *res;
    for (int n = 0; n <= fg.right; ++n) {
      if (n >= fg.left) {
        const double w = fg.w[n - fg.left];
        for (int k = 0; k < kmax; ++k) out[k] += w * f[k];
      }
      if (n == fg.right) break;
      for (int k = 0; k < kmax; ++k) {
        const double below = k == 0 ? f_served : f[k - 1];
        nx[k] = (1 - qk[k] / lam) * f[k] + (r[k] / lam) * below + (theta / lam) * f_ab;
      }
      f.swap(nx);
    }
    for (double& x : out) {
      if (x < 0 || x > 1) {
        x = std::clamp(x, 0.0, 1.0);
        if (st) ++st->clamps;
      }
    }
  };
  if (lam * o.sla_s > o.max_lambda_t) {
    t.quality = worst(t.quality, Quality::kApprox);
  } else {
    backward(o.sla_s, 1.0, 0.0, &t.u);
  }
  if (theta > 0 && lam * o.short_abandon_s <= o.max_lambda_t) backward(o.short_abandon_s, 0.0, 1.0, &t.a);
  return t;
}

Tagged tagged_rates(const std::vector<double>& r_in, double theta_own, const Options& o, Stats* st) {
  Tagged t;
  const int kmax = std::max<int>(1, static_cast<int>(r_in.size()));
  std::vector<double> r(kmax, 0.0), qk(kmax);
  for (int k = 0; k < kmax && k < static_cast<int>(r_in.size()); ++k) r[k] = r_in[k];
  t.V.assign(kmax, 0);
  t.Ps.assign(kmax, 0);
  t.u.assign(kmax, 0);
  t.a.assign(kmax, 0);
  t.m.assign(kmax, 0);
  bool ok = std::isfinite(theta_own) && theta_own >= 0;
  for (int k = 0; k < kmax && ok; ++k) ok = std::isfinite(r[k]) && r[k] > 0;
  if (!ok) {
    t.quality = Quality::kLong;
    std::fill(t.V.begin(), t.V.end(), o.long_wait_s);  // a lower bound, never 0 (CR-013)
    const double ab = theta_own > 0 ? 1 - std::exp(-theta_own * o.short_abandon_s) : 0;
    std::fill(t.a.begin(), t.a.end(), ab);
    return t;
  }
  double acc = 0, ps_prev = 1, m_prev = 0;
  for (int k = 0; k < kmax; ++k) {
    acc += 1 / r[k];
    t.V[k] = acc;
    const double q = r[k] + theta_own;
    qk[k] = q;
    t.m[k] = (r[k] / q) * (ps_prev / q + m_prev);
    t.Ps[k] = ps_prev * r[k] / q;
    ps_prev = t.Ps[k];
    m_prev = t.m[k];
  }
  double lam = 0;
  for (double q : qk) lam = std::max(lam, q);
  auto backward = [&](double tau, double f_served, double f_ab, std::vector<double>* res) {
    const PoissonWeights fg = fox_glynn(lam * tau, o.fg_eps);
    std::vector<double> f(kmax, 0.0), nx(kmax);
    std::vector<double>& out = *res;
    for (int n = 0; n <= fg.right; ++n) {
      if (n >= fg.left) {
        const double w = fg.w[n - fg.left];
        for (int k = 0; k < kmax; ++k) out[k] += w * f[k];
      }
      if (n == fg.right) break;
      for (int k = 0; k < kmax; ++k) {
        const double below = k == 0 ? f_served : f[k - 1];
        nx[k] = (1 - qk[k] / lam) * f[k] + (r[k] / lam) * below + (theta_own / lam) * f_ab;
      }
      f.swap(nx);
    }
    for (double& x : out) {
      if (x < 0 || x > 1) {
        x = std::clamp(x, 0.0, 1.0);
        if (st) ++st->clamps;
      }
    }
  };
  if (lam * o.sla_s > o.max_lambda_t) {
    t.quality = worst(t.quality, Quality::kApprox);
  } else {
    backward(o.sla_s, 1.0, 0.0, &t.u);
  }
  if (theta_own > 0 && lam * o.short_abandon_s <= o.max_lambda_t) backward(o.short_abandon_s, 0.0, 1.0, &t.a);
  return t;
}

// ======================================================================================
// Metrics
// ======================================================================================

bool check_boundary(QueueMetrics* m, const Options& o, Stats* st) {
  if (!m->ok) return false;
  const double vals[] = {m->sl, m->abandon, m->ewt_mean, m->asa, m->p_wait,
                         m->busy, m->lq, m->mean_wait_all, m->truncation_mass, m->s};
  for (double v : vals) {
    if (!std::isfinite(v)) {
      m->ok = false;
      m->why = "NaN or inf at the model's boundary";
      if (st) ++st->refused;
      return false;
    }
  }
  if (m->truncation_mass > o.refuse_tail) {
    m->ok = false;
    m->why = "truncation mass above the refusal bound";
    if (st) ++st->refused;
    return false;
  }
  for (double* p : {&m->sl, &m->abandon, &m->p_wait}) {
    if (*p < -1e-6 || *p > 1 + 1e-6) {
      m->ok = false;
      m->why = "a probability outside [0, 1]";
      if (st) ++st->refused;
      return false;
    }
    if (*p < 0 || *p > 1) {
      *p = std::clamp(*p, 0.0, 1.0);
      if (st) ++st->clamps;
    }
  }
  for (double* x : {&m->ewt_mean, &m->asa, &m->busy, &m->lq, &m->mean_wait_all}) {
    if (*x < 0) {
      if (*x < -1e-6) {
        m->ok = false;
        m->why = "a negative mean";
        if (st) ++st->refused;
        return false;
      }
      *x = 0;
      if (st) ++st->clamps;
    }
  }
  return true;
}

QueueMetrics transient_metrics(const QueueParams& q, int n0, double horizon_s, int points,
                               const Options& o, Stats* st) {
  if (!valid(q) || n0 < 0 || !(horizon_s > 0) || points < 1) return refuse("invalid parameters", st);
  auto one = [&](int s) -> QueueMetrics {
    if (s == 0 || q.mu == 0) return no_service(q, o);
    const int K = std::max(transient_k(q, s, n0, horizon_s, o), n0 + 1);
    if (K > o.k_cap || n0 > o.k_cap) return refuse("the current queue exceeds the truncation cap", st);
    std::vector<double> p(K + 1, 0.0);
    p[n0] = 1;
    QueueMetrics acc;
    acc.ok = true;
    acc.s = s;
    double t_prev = 0;
    const Tagged tg = tagged_for(q, s, K, o, st);
    for (int i = 0; i < points; ++i) {
      const double t = horizon_s * (i + 0.5) / points;
      Distribution d = transient(q, s, p, t - t_prev, o, st);
      if (!d.ok) return refuse("transient: " + d.why, st);
      p = d.p;
      t_prev = t;
      QueueMetrics m = metrics_int(q, s, d, o, st, &tg);
      m.truncation_mass = d.tail_mass;
      acc.quality = worst(acc.quality, m.quality);
      acc.truncation_mass = std::max(acc.truncation_mass, m.truncation_mass);
      const double w = 1.0 / points;
      acc.sl += w * m.sl;
      acc.abandon += w * m.abandon;
      acc.ewt_mean += w * m.ewt_mean;
      acc.asa += w * m.asa;
      acc.p_wait += w * m.p_wait;
      acc.busy += w * m.busy;
      acc.lq += w * m.lq;
      acc.mean_wait_all += w * m.mean_wait_all;
    }
    return acc;
  };
  const int lo = static_cast<int>(std::floor(q.s));
  const double f = q.s - lo;
  QueueMetrics m = one(lo);
  if (f > 1e-12 && m.ok) m = lerp(m, one(lo + 1), f);
  m.s = q.s;
  if (q.theta == 0 && q.lambda >= q.s * q.mu && q.lambda > 0) m.quality = worst(m.quality, Quality::kOverloaded);
  check_boundary(&m, o, st);
  return m;
}

QueueMetrics transient_from(const QueueParams& q, const std::vector<double>& p0, double horizon_s,
                            int points, const Options& o, Stats* st, std::vector<double>* p_end) {
  if (!valid(q) || p0.empty() || !(horizon_s > 0) || points < 1) return refuse("invalid parameters", st);
  const int s = static_cast<int>(std::llround(q.s));
  QueueParams qi = q;
  qi.s = s;
  if (s == 0 || q.mu == 0) {
    QueueMetrics m = no_service(q, o);
    check_boundary(&m, o, st);
    return m;
  }
  int hi = 0;
  double cum = 0;
  for (size_t n = 0; n < p0.size(); ++n) {
    cum += p0[n];
    if (cum >= 1 - 1e-12) {
      hi = static_cast<int>(n);
      break;
    }
    hi = static_cast<int>(n);
  }
  const int K = std::max(static_cast<int>(p0.size()) - 1, transient_k(qi, s, hi, horizon_s, o));
  std::vector<double> p(K + 1, 0.0);
  std::copy(p0.begin(), p0.end(), p.begin());
  QueueMetrics acc;
  acc.ok = true;
  acc.s = s;
  double t_prev = 0;
  const Tagged tg = tagged_for(qi, s, K, o, st);
  for (int i = 0; i < points; ++i) {
    const double t = horizon_s * (i + 0.5) / points;
    Distribution d = transient(qi, s, p, t - t_prev, o, st);
    if (!d.ok) return refuse("transient: " + d.why, st);
    p = d.p;
    t_prev = t;
    const QueueMetrics m = metrics_int(qi, s, d, o, st, &tg);
    acc.quality = worst(acc.quality, m.quality);
    acc.truncation_mass = std::max(acc.truncation_mass, d.tail_mass);
    const double w = 1.0 / points;
    acc.sl += w * m.sl;
    acc.abandon += w * m.abandon;
    acc.ewt_mean += w * m.ewt_mean;
    acc.asa += w * m.asa;
    acc.p_wait += w * m.p_wait;
    acc.busy += w * m.busy;
    acc.lq += w * m.lq;
    acc.mean_wait_all += w * m.mean_wait_all;
  }
  if (p_end) {
    Distribution d = transient(qi, s, p, horizon_s - t_prev, o, st);
    if (!d.ok) return refuse("transient: " + d.why, st);
    *p_end = d.p;
  }
  check_boundary(&acc, o, st);
  return acc;
}

QueueMetrics stationary_metrics(const QueueParams& q, const Options& o, Stats* st,
                                double horizon_s) {
  if (!valid(q)) return refuse("invalid parameters", st);
  if (q.lambda == 0) {
    // No arrivals: zero wait, zero abandonment (the PoC's 9999 s is not repeated).
    QueueMetrics m;
    m.ok = true;
    m.sl = 1;
    m.s = q.s;
    return m;
  }
  auto one = [&](int s) -> QueueMetrics {
    if (s == 0 || q.mu == 0) return no_service(q, o);
    const Distribution d = stationary(q, s, o, st);
    if (d.ok) return metrics_int(q, s, d, o, st);
    if (d.quality == Quality::kOverloaded) {
      // No stationary law: what an arrival meets over the horizon, from an empty queue.
      QueueParams qi = q;
      qi.s = s;
      QueueMetrics m = transient_metrics(qi, 0, horizon_s, 10, o, st);
      m.quality = worst(m.quality, Quality::kOverloaded);
      return m;
    }
    return refuse(d.why, st);
  };
  const int lo = static_cast<int>(std::floor(q.s));
  const double f = q.s - lo;
  QueueMetrics m = one(lo);
  if (f > 1e-12) {
    const QueueMetrics hi = one(lo + 1);
    m = (m.ok && hi.ok) ? lerp(m, hi, f) : (m.ok ? hi : m);
  }
  m.s = q.s;
  check_boundary(&m, o, st);
  return m;
}

}  // namespace pcr::model
