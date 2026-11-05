#include "src/trainer/data.h"

#include <algorithm>
#include <cmath>
#include <limits>

#include "src/opt/forecast.h"

namespace pcr::trainer {

namespace {
constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
constexpr int kSPB = fc::kStepsPerBucket;

double fill_mean(const std::vector<double>& v) {
  double s = 0;
  int n = 0;
  for (double x : v) {
    if (std::isfinite(x)) {
      s += x;
      ++n;
    }
  }
  return n ? s / n : 0;
}
}  // namespace

fc::QueueNorm norm_from(const QSeries& q, double envelope) {
  std::vector<double> per_min;
  for (size_t m = 0; m + fc::kStepsPerMin <= q.c10.size(); m += fc::kStepsPerMin) {
    double s = 0;
    bool ok = true;
    for (int k = 0; k < fc::kStepsPerMin; ++k) {
      const float c = q.c10[m + static_cast<size_t>(k)];
      if (!std::isfinite(c)) {
        ok = false;
        break;
      }
      s += c;
    }
    if (ok) per_min.push_back(s);
  }
  std::vector<double> aht;
  for (float a : q.b_aht) {
    if (a > 0 && std::isfinite(a)) aht.push_back(a);
  }
  fc::QueueNorm n;
  n.r = fc::Range::from_history(per_min);
  n.aht = fc::AhtRange::from_history(aht);
  n.envelope = envelope;
  std::vector<double> z;
  z.reserve(per_min.size());
  for (double v : per_min) z.push_back(n.r.norm(v));
  n.psi_ref = fc::psi_hist(z);
  return n;
}

std::vector<QSeries> from_sim(const sim::SeriesTenant& t, const std::vector<QSeries>* hist, std::mt19937_64& rng) {
  std::vector<QSeries> out;
  for (const auto& sq : t.q) {
    QSeries q;
    q.tenant = t.name;
    q.queue = sq.id;
    q.start_ms = t.start_ms;
    q.first_step = sq.first_step;
    q.c10.resize(sq.c10.size());
    for (size_t i = 0; i < sq.c10.size(); ++i) q.c10[i] = sq.miss[i] ? kNaN : static_cast<float>(sq.c10[i]);
    q.b_off = sq.b_off;
    q.b_ab = sq.b_ab;
    q.b_aht.resize(sq.b_off.size());
    for (size_t b = 0; b < sq.b_off.size(); ++b) {
      q.b_aht[b] = !std::isfinite(sq.b_hand[b]) ? kNaN : sq.b_hand[b] > 0 ? sq.b_handle_s[b] / sq.b_hand[b] : 0.0f;
    }
    q.holiday = t.holiday;
    q.event = t.event;
    const QSeries* h = nullptr;
    if (hist) {
      for (const auto& x : *hist) {
        if (x.queue == q.queue) h = &x;
      }
    }
    if (sq.renamed) {
      // A queue new under its name: its expected volume (plan/seats), as a new tenant's.
      const double noise = std::exp(std::uniform_real_distribution<double>(std::log(0.5), std::log(2.0))(rng));
      q.norm.r = fc::Range::from_expected(std::max(1.0, sq.peak_per_hour * noise));
      q.norm.expected = true;
      q.norm.psi_ref.assign(fc::kPsiBins + 2, 1.0 / (fc::kPsiBins + 2));
    } else {
      q.norm = norm_from(h ? *h : q, 2.0);
    }
    out.push_back(std::move(q));
  }
  return out;
}

int64_t first_anchor(const QSeries& q) {
  const int64_t need_h1 = q.first_step + fc::kH1Inputs - 1;
  return q.first_step > 0 ? need_h1 : std::max<int64_t>(need_h1, fc::kCtxTokens * kSPB);
}

int64_t last_anchor(const QSeries& q) {
  const int64_t steps = static_cast<int64_t>(q.c10.size());
  const int64_t nb = static_cast<int64_t>(q.b_off.size());
  return std::min(steps - 1 - fc::kH1Steps * fc::kStepsPerMin, (nb - fc::kH2Buckets) * kSPB - 1);
}

bool make_example(const QSeries& q, int64_t s, Example* out, bool targets) {
  const int64_t steps = static_cast<int64_t>(q.c10.size());
  const int64_t nb = static_cast<int64_t>(q.b_off.size());
  if (s < fc::kH1Inputs - 1 || s >= steps) return false;
  fc::Recent r;
  r.t_ms = q.start_ms + (s + 1) * fc::kStepMs;
  std::vector<double> c(static_cast<size_t>(fc::kH1Inputs));
  for (int i = 0; i < fc::kH1Inputs; ++i) c[static_cast<size_t>(i)] = q.c10[static_cast<size_t>(s - fc::kH1Inputs + 1 + i)];
  const double cm = fill_mean(c);
  for (double& x : c) {
    if (!std::isfinite(x)) x = cm;
  }
  r.c10 = std::move(c);
  const int64_t cb = (s + 1) / kSPB;
  std::vector<double> off, ab, aht;
  for (int64_t b = cb - fc::kCtxTokens; b < cb; ++b) {
    const bool in = b >= 0 && b < nb;
    off.push_back(in ? q.b_off[static_cast<size_t>(b)] : std::nan(""));
    ab.push_back(in ? q.b_ab[static_cast<size_t>(b)] : std::nan(""));
    aht.push_back(in ? q.b_aht[static_cast<size_t>(b)] : std::nan(""));
  }
  const double om = fill_mean(off), am = fill_mean(ab);
  for (size_t i = 0; i < off.size(); ++i) {
    if (!std::isfinite(off[i])) off[i] = i > 0 ? off[i - 1] : om;
    if (!std::isfinite(ab[i])) ab[i] = i > 0 ? ab[i - 1] : am;
    if (!std::isfinite(aht[i])) aht[i] = 0;
  }
  r.b_arr = std::move(off);
  r.b_ab = std::move(ab);
  r.b_aht = std::move(aht);
  if (cb < nb && cb < static_cast<int64_t>(q.holiday.size())) r.holiday = q.holiday[static_cast<size_t>(cb)];
  if (cb < nb && cb < static_cast<int64_t>(q.event.size())) r.event = q.event[static_cast<size_t>(cb)];
  out->in = fc::build_input(r, q.norm);
  if (!targets) return true;
  out->t1.assign(fc::kH1Steps, kNaN);
  for (int k = 0; k < fc::kH1Steps; ++k) {
    double sum = 0;
    bool ok = true;
    for (int j = 0; j < fc::kStepsPerMin; ++j) {
      const int64_t i = s + 1 + k * fc::kStepsPerMin + j;
      if (i >= steps || !std::isfinite(q.c10[static_cast<size_t>(i)])) {
        ok = false;
        break;
      }
      sum += q.c10[static_cast<size_t>(i)];
    }
    if (ok) out->t1[static_cast<size_t>(k)] = static_cast<float>(q.norm.r.norm(sum));
  }
  out->t2a.assign(fc::kH2Buckets, kNaN);
  out->t2h.assign(fc::kH2Buckets, kNaN);
  for (int b = 0; b < fc::kH2Buckets; ++b) {
    const int64_t i = cb + b;
    if (i >= nb) break;
    const float o = q.b_off[static_cast<size_t>(i)];
    if (std::isfinite(o)) out->t2a[static_cast<size_t>(b)] = static_cast<float>(q.norm.r.norm(o / 15.0));
    const float a = q.b_aht[static_cast<size_t>(i)];
    if (std::isfinite(a) && a > 0) out->t2h[static_cast<size_t>(b)] = static_cast<float>(q.norm.aht.norm(a));
  }
  return true;
}

void Wape::add(const Wape& o) {
  h1_abs += o.h1_abs;
  h1_act += o.h1_act;
  h2_abs += o.h2_abs;
  h2_act += o.h2_act;
  b_h1_abs += o.b_h1_abs;
  b_h1_act += o.b_h1_act;
  b_h2_abs += o.b_h2_abs;
  b_h2_act += o.b_h2_act;
  anchors += o.anchors;
}

Wape evaluate(const fc::Net* net, const std::vector<QSeries>& eval, const std::vector<QSeries>& hist, int every_steps,
              int64_t from_step) {
  Wape w;
  constexpr int kH1Eval = 15;
  for (const auto& q : eval) {
    opt::Forecaster base;
    for (const auto& h : hist) {
      if (h.queue != q.queue || h.tenant != q.tenant) continue;
      for (size_t b = 0; b < h.b_off.size(); ++b) {
        if (std::isfinite(h.b_off[b])) base.add_actual(q.queue, h.start_ms + static_cast<int64_t>(b) * fc::kBucketMs, h.b_off[b]);
      }
    }
    const int64_t a0 = std::max(first_anchor(q), from_step), a1 = last_anchor(q);
    const int64_t nb = static_cast<int64_t>(q.b_off.size());
    std::vector<Example> ex;
    struct Pending {
      int64_t s;
      std::vector<double> a1, b1, a2, b2;  // actuals and baseline forecasts
    };
    std::vector<Pending> pend;
    auto flush = [&] {
      if (pend.empty()) return;
      std::vector<fc::NetInput> in;
      for (auto& e : ex) in.push_back(std::move(e.in));
      std::vector<fc::NetOutput> out;
      const bool ok = net && net->run(in, &out) && out.size() == pend.size();
      for (size_t i = 0; i < pend.size(); ++i) {
        const Pending& p = pend[i];
        fc::Denorm d;
        if (ok) d = fc::denormalize(out[i], q.norm);
        for (size_t k = 0; k < p.a1.size(); ++k) {
          w.b_h1_abs += std::abs(p.b1[k] - p.a1[k]);
          w.b_h1_act += p.a1[k];
          if (ok) {
            w.h1_abs += std::abs(d.h1[1][k] - p.a1[k]);
            w.h1_act += p.a1[k];
          }
        }
        for (size_t b = 0; b < p.a2.size(); ++b) {
          w.b_h2_abs += std::abs(p.b2[b] - p.a2[b]);
          w.b_h2_act += p.a2[b];
          if (ok) {
            w.h2_abs += std::abs(d.h2[1][b] * 900 - p.a2[b]);
            w.h2_act += p.a2[b];
          }
        }
        ++w.anchors;
      }
      ex.clear();
      pend.clear();
    };
    for (int64_t s = 0; s <= a1 && s < static_cast<int64_t>(q.c10.size()); ++s) {
      const int64_t at = q.start_ms + (s + 1) * fc::kStepMs;
      const float c = q.c10[static_cast<size_t>(s)];
      const int64_t b = s / kSPB;
      const double aht = b < nb && q.b_aht[static_cast<size_t>(b)] > 0 ? q.b_aht[static_cast<size_t>(b)] : 0.0;
      if (std::isfinite(c)) base.observe(q.queue, at, 10, c, 0, 0, aht, 0);
      if (s < a0 || (s - a0) % every_steps != 0) continue;
      Pending p;
      p.s = s;
      bool ok = true;
      for (int k = 0; k < kH1Eval && ok; ++k) {
        double sum = 0;
        for (int j = 0; j < fc::kStepsPerMin; ++j) {
          const float x = q.c10[static_cast<size_t>(s + 1 + k * fc::kStepsPerMin + j)];
          if (!std::isfinite(x)) ok = false;
          sum += x;
        }
        p.a1.push_back(sum);
      }
      const int64_t cb = (s + 1) / kSPB;
      for (int k = 0; k < fc::kH2Buckets && ok; ++k) {
        const float o = q.b_off[static_cast<size_t>(cb + k)];
        if (!std::isfinite(o)) ok = false;
        p.a2.push_back(o);
      }
      if (!ok) continue;
      const opt::QueueForecast f = base.forecast(q.queue, at, 180);
      for (int k = 0; k < kH1Eval; ++k) p.b1.push_back(f.now_per_s * 60);
      for (int k = 0; k < fc::kH2Buckets; ++k) p.b2.push_back(k < static_cast<int>(f.per_s.size()) ? f.per_s[static_cast<size_t>(k)] * 900 : 0);
      Example e;
      if (!make_example(q, s, &e, false)) continue;
      ex.push_back(std::move(e));
      pend.push_back(std::move(p));
      if (pend.size() >= 512) flush();
    }
    flush();
  }
  return w;
}

}  // namespace pcr::trainer
