#include "src/opt/forecast.h"

#include <algorithm>
#include <cmath>

namespace pcr::opt {

namespace {
constexpr int64_t kWeekMs = 7LL * 86'400'000;
constexpr int64_t kDayMs = 86'400'000;
// The (weekday, 15-min slot) of an epoch-ms instant, UTC (the tenant's time zone comes with
// config.holidays in 5b; the profile is keyed and read in the same clock, so it is consistent).
int slot_of(int64_t t_ms, int64_t bucket_ms) {
  const int64_t day = t_ms / kDayMs;
  const int weekday = static_cast<int>((day + 4) % 7);  // 1970-01-01 was a Thursday (4)
  const int slot = static_cast<int>((t_ms % kDayMs) / bucket_ms);
  return weekday * static_cast<int>(kDayMs / bucket_ms) + slot;
}
}  // namespace

void Forecaster::observe(const std::string& queue, int64_t at_ms, double window_s, double arrivals, double abandons,
                         double waited_s, double aht_s, double wrap_s, double vip_arrivals, double handled,
                         double handle_s) {
  Q& q = q_[queue];
  if (aht_s > 0 && std::isfinite(aht_s)) q.aht_s = aht_s;
  if (wrap_s >= 0 && std::isfinite(wrap_s)) q.wrap_s = wrap_s;
  if (!(window_s > 0) || !std::isfinite(arrivals) || !std::isfinite(abandons) || !std::isfinite(waited_s)) return;
  const double x = arrivals / window_s, ab = abandons / window_s, wt = waited_s / window_s;
  const double vx = std::isfinite(vip_arrivals) && vip_arrivals > 0 ? std::min(vip_arrivals, arrivals) / window_s : 0;
  if (!q.seen) {
    q.seen = true;
    q.e1 = q.e5 = q.e15 = q.e60 = x;
    q.abandons = ab;
    q.waited = wt;
    q.vip = vx;
    q.hn = std::max(0.0, handled) / window_s;
    q.hs = std::max(0.0, handle_s) / window_s;
  } else {
    auto upd = [&](double* e, double tau) { *e += (1 - std::exp(-window_s / tau)) * (x - *e); };
    upd(&q.e1, 60);
    upd(&q.e5, 300);
    upd(&q.e15, 900);
    upd(&q.e60, 3600);
    const double a = 1 - std::exp(-window_s / 900);
    q.abandons += a * (ab - q.abandons);
    q.waited += a * (wt - q.waited);
    q.vip += a * (vx - q.vip);
    if (std::isfinite(handled) && std::isfinite(handle_s)) {
      q.hn += a * (std::max(0.0, handled) / window_s - q.hn);
      q.hs += a * (std::max(0.0, handle_s) / window_s - q.hs);
    }
  }
  q.observed_s += window_s;
}

void Forecaster::add_actual(const std::string& queue, int64_t bucket_start_ms, double offered) {
  if (bucket_start_ms <= 0 || !(offered >= 0)) return;
  Q& q = q_[queue];
  q.profile[slot_of(bucket_start_ms, o_.bucket_ms)][bucket_start_ms / kWeekMs] = offered;
  ++rows_;
}

double Forecaster::profile_at(const Q& q, int64_t t_ms, bool* ok) const {
  auto it = q.profile.find(slot_of(t_ms, o_.bucket_ms));
  if (it == q.profile.end() || static_cast<int>(it->second.size()) < o_.profile_min_weeks) {
    *ok = false;
    return 0;
  }
  // The most recent weeks weigh more (weights 1, 1/2, 1/4 ... from the newest), the current week's
  // own slot (a bucket still running) excluded.
  const int64_t this_week = t_ms / kWeekMs;
  double num = 0, den = 0, w = 1;
  for (auto r = it->second.rbegin(); r != it->second.rend(); ++r) {
    if (r->first >= this_week) continue;
    num += w * r->second;
    den += w;
    w /= 2;
  }
  if (den <= 0) {
    *ok = false;
    return 0;
  }
  return num / den / (o_.bucket_ms / 1000.0);
}

Forecaster::Raw Forecaster::raw(const std::string& queue) const {
  Raw r;
  auto it = q_.find(queue);
  if (it == q_.end()) return r;
  const Q& q = it->second;
  r.calls = q.hn * 900;
  r.aht_s = q.hn > 0 ? q.hs / q.hn : q.aht_s;
  r.abandons = q.abandons;
  r.waited = q.waited;
  return r;
}

double Forecaster::theta(const std::string& queue) const {
  auto it = q_.find(queue);
  const double tau = 900;
  if (it == q_.end()) return 1 / o_.patience_prior_s;
  const Q& q = it->second;
  return (q.abandons * tau + o_.patience_prior_weight_s / o_.patience_prior_s) / (q.waited * tau + o_.patience_prior_weight_s);
}

QueueForecast Forecaster::forecast(const std::string& queue, int64_t now_ms, double default_aht_s) const {
  QueueForecast f;
  f.queue = queue;
  f.first_bucket_ms = now_ms / o_.bucket_ms * o_.bucket_ms;
  auto it = q_.find(queue);
  static const Q kEmpty;
  const Q& q = it == q_.end() ? kEmpty : it->second;
  f.aht_s = q.aht_s > 0 ? q.aht_s : default_aht_s;
  f.wrap_s = q.wrap_s;
  const double th = theta(queue);
  f.patience_s = th > 0 ? 1 / th : 0;
  f.vip_share = q.e15 > 0 ? std::clamp(q.vip / q.e15, 0.0, 1.0) : 0;
  // Trend: the 15-min EWMA's lead over the 60-min one, over the lag between their centres (22.5 min).
  f.trend_per_s2 = (q.e15 - q.e60) / ((3600 - 900) / 2.0);
  bool have_profile = false;
  {
    bool ok = true;
    for (int b = -4; b < o_.buckets && ok; ++b) profile_at(q, f.first_bucket_ms + b * o_.bucket_ms, &ok);
    have_profile = ok && q.seen;
  }
  // The level ratio: how today runs against the profile over the last hour.
  double ratio = 1;
  if (have_profile) {
    double prof = 0;
    bool ok = true;
    for (int b = -4; b < 0; ++b) prof += profile_at(q, f.first_bucket_ms + b * o_.bucket_ms, &ok);
    prof /= 4;
    ratio = prof > 0 ? std::clamp(q.e60 / prof, o_.level_ratio_min, o_.level_ratio_max) : 1;
  }
  f.source = !q.seen || q.observed_s < o_.warm_s ? "config_default" : have_profile ? "ewma_profile" : "ewma";
  for (int b = 0; b < o_.buckets; ++b) {
    // The bucket's middle (the current one: the rest of it).
    const int64_t mid = b == 0 ? (now_ms + f.first_bucket_ms + o_.bucket_ms) / 2 : f.first_bucket_ms + b * o_.bucket_ms + o_.bucket_ms / 2;
    const double h = std::max(0.0, (mid - now_ms) / 1000.0);
    const double damped = std::min(h, o_.trend_horizon_s);
    const double ewma = std::max(0.0, q.e15 + f.trend_per_s2 * damped);
    double v = ewma;
    if (have_profile) {
      bool ok = true;
      const double prof = profile_at(q, mid, &ok) * ratio;
      const double w = std::exp(-h / o_.blend_tau_s);
      v = ok ? w * ewma + (1 - w) * prof : ewma;
    }
    f.per_s.push_back(std::isfinite(v) ? std::max(0.0, v) : 0.0);
  }
  // The CTMC's rate now: the short EWMA's view of the present, blended with the current bucket.
  f.now_per_s = q.seen ? std::max(0.0, 0.5 * q.e5 + 0.5 * f.per_s.front()) : 0;
  f.observed_per_s = q.seen ? std::max(0.0, q.e1) : -1;
  return f;
}

}  // namespace pcr::opt

namespace pcr::opt {

bool within_sanity(const QueueForecast& f, const QueueForecast& baseline, const Sanity& s) {
  const double obs = baseline.observed_per_s;
  auto ok = [&](double v, double b) {
    const double hi_ref = std::max(b, obs), lo_ref = obs >= 0 ? std::min(b, obs) : b;
    if (std::max(v, hi_ref) < s.min_per_s) return true;  // both near zero: noise
    if (v > s.hi * std::max(hi_ref, s.min_per_s)) return false;
    return !(lo_ref >= s.min_per_s && v < s.lo * lo_ref);
  };
  if (!ok(f.now_per_s, baseline.now_per_s)) return false;
  for (size_t b = 0; b < baseline.per_s.size() && b < f.per_s.size(); ++b) {
    if (!ok(f.per_s[b], baseline.per_s[b])) return false;
  }
  return true;
}

QueueForecast choose_forecast(const QueueForecast& baseline, ForecastSource* learned, const std::string& tenant,
                              int64_t now_ms, const Sanity& s) {
  if (!learned) return baseline;
  std::optional<QueueForecast> f = learned->forecast(tenant, baseline.queue, now_ms, baseline);
  QueueForecast out = baseline;
  if (!f) {
    out.fallback = learned->why_not(tenant, baseline.queue);
    if (out.fallback.empty()) out.fallback = "no_model";
    return out;
  }
  out.fallback = "nan";
  if (f->queue != baseline.queue || f->per_s.size() < baseline.per_s.size()) return out;
  auto bad = [](double x) { return !std::isfinite(x) || x < 0; };
  if (bad(f->now_per_s) || bad(f->aht_s) || bad(f->vip_share) || f->vip_share > 1) return out;
  for (double x : f->per_s) {
    if (bad(x)) return out;
  }
  if (!within_sanity(*f, baseline, s)) {
    out.fallback = "ratio";
    return out;
  }
  f->source = "model";
  f->first_bucket_ms = baseline.first_bucket_ms;
  f->fallback.clear();
  if (!f->owns_aht) f->aht_s = baseline.aht_s;  // the source owns arrival rates only (CR-180)
  return *f;
}

}  // namespace pcr::opt
