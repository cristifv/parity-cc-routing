#include "src/forecast/features.h"

#include <algorithm>
#include <cmath>
#include <numbers>

namespace pcr::fc {

namespace {
double pct(std::vector<double> v, double p) {
  if (v.empty()) return 0;
  std::sort(v.begin(), v.end());
  const double k = p * static_cast<double>(v.size() - 1);
  const size_t i = static_cast<size_t>(std::floor(k));
  const size_t j = std::min(v.size() - 1, i + 1);
  return v[i] + (k - static_cast<double>(i)) * (v[j] - v[i]);
}
double finite_or(double x, double d) { return std::isfinite(x) ? x : d; }
}  // namespace

double Range::norm(double per_min_v) const {
  const double x = std::log1p(std::max(0.0, finite_or(per_min_v, 0)));
  return (x - lo) / span();
}

double Range::inv(double z) const {
  const double x = lo + finite_or(z, 0) * span();
  return std::max(0.0, std::expm1(std::min(x, 30.0)));
}

Range Range::from_history(const std::vector<double>& per_min_v) {
  std::vector<double> x;
  x.reserve(per_min_v.size());
  for (double v : per_min_v) {
    if (std::isfinite(v) && v >= 0) x.push_back(std::log1p(v));
  }
  Range r;
  if (x.empty()) return r;
  r.lo = pct(x, 0.01);
  r.hi = pct(x, 0.99);
  if (r.hi < r.lo + kMinSpan) r.hi = r.lo + kMinSpan;
  return r;
}

Range Range::from_expected(double peak_per_hour) {
  // The normal peak sits at the top of the range; the bottom is a quiet hour (a tenth of it).
  const double peak = std::max(0.1, finite_or(peak_per_hour, 6)) / 60.0;
  Range r;
  r.lo = std::log1p(peak / 10);
  r.hi = std::log1p(peak);
  if (r.hi < r.lo + kMinSpan) r.hi = r.lo + kMinSpan;
  return r;
}

double AhtRange::norm(double aht_s) const {
  if (!(aht_s > 0) || !std::isfinite(aht_s)) return 0;
  return (std::log(aht_s) - lo) / span();
}

double AhtRange::inv(double z) const { return std::exp(std::clamp(lo + finite_or(z, 0) * span(), 0.0, 12.0)); }

AhtRange AhtRange::from_history(const std::vector<double>& aht_s) {
  std::vector<double> x;
  for (double v : aht_s) {
    if (v > 0 && std::isfinite(v)) x.push_back(std::log(v));
  }
  AhtRange r;
  if (x.size() < 4) return r;
  r.lo = pct(x, 0.01);
  r.hi = pct(x, 0.99);
  if (r.hi < r.lo + 0.2) r.hi = r.lo + 0.2;
  return r;
}

nlohmann::json QueueNorm::to_json() const {
  return {{"lo", r.lo}, {"hi", r.hi}, {"aht_lo", aht.lo}, {"aht_hi", aht.hi}, {"envelope", envelope},
          {"psi_ref", psi_ref}, {"expected", expected}};
}

QueueNorm QueueNorm::from_json(const nlohmann::json& j) {
  QueueNorm n;
  n.r.lo = j.value("lo", 0.0);
  n.r.hi = j.value("hi", 1.0);
  n.aht.lo = j.value("aht_lo", 3.4);
  n.aht.hi = j.value("aht_hi", 6.8);
  n.envelope = j.value("envelope", 2.0);
  if (j.contains("psi_ref") && j["psi_ref"].is_array()) n.psi_ref = j["psi_ref"].get<std::vector<double>>();
  n.expected = j.value("expected", false);
  return n;
}

int psi_bin(double z) {
  if (!(z >= 0)) return 0;
  if (z >= 1) return kPsiBins + 1;
  return 1 + std::min(kPsiBins - 1, static_cast<int>(z * kPsiBins));
}

std::vector<double> psi_hist(const std::vector<double>& z) {
  std::vector<double> h(kPsiBins + 2, 0);
  for (double v : z) h[static_cast<size_t>(psi_bin(v))] += 1;
  const double n = static_cast<double>(z.size());
  if (n > 0) {
    for (double& v : h) v /= n;
  }
  return h;
}

double psi(const std::vector<double>& ref, const std::vector<double>& cur) {
  if (ref.size() != cur.size() || ref.empty()) return 0;
  constexpr double kEps = 1e-3;
  double s = 0;
  for (size_t i = 0; i < ref.size(); ++i) {
    const double e = std::max(ref[i], kEps), a = std::max(cur[i], kEps);
    s += (a - e) * std::log(a / e);
  }
  return s;
}

double time_of_day(int64_t t_ms) {
  constexpr int64_t kDay = 86'400'000;
  return static_cast<double>(((t_ms % kDay) + kDay) % kDay) / static_cast<double>(kDay);
}

int weekday(int64_t t_ms) {
  constexpr int64_t kDay = 86'400'000;
  const int64_t d = t_ms >= 0 ? t_ms / kDay : (t_ms - kDay + 1) / kDay;
  return static_cast<int>(((d + 3) % 7 + 7) % 7);  // 1970-01-01 was a Thursday (Monday = 0)
}

NetInput build_input(const Recent& r, const QueueNorm& n) {
  NetInput in;
  in.h1.resize(kH1Inputs);
  for (int i = 0; i < kH1Inputs; ++i) {
    const size_t k = r.c10.size() >= static_cast<size_t>(kH1Inputs) ? r.c10.size() - kH1Inputs + i : 0;
    const double c = k < r.c10.size() ? r.c10[k] : 0;
    in.h1[static_cast<size_t>(i)] = static_cast<float>(n.r.norm(per_min(c, kStepMs)));
  }
  in.ctx.resize(kCtxTokens * kCtxFeats);
  for (int b = 0; b < kCtxTokens; ++b) {
    const size_t k = r.b_arr.size() >= static_cast<size_t>(kCtxTokens) ? r.b_arr.size() - kCtxTokens + b : 0;
    const double arr = k < r.b_arr.size() ? r.b_arr[k] : 0;
    const double aht = k < r.b_aht.size() ? r.b_aht[k] : 0;
    const double ab = k < r.b_ab.size() ? r.b_ab[k] : 0;
    in.ctx[static_cast<size_t>(b * kCtxFeats + 0)] = static_cast<float>(n.r.norm(per_min(arr, kBucketMs)));
    in.ctx[static_cast<size_t>(b * kCtxFeats + 1)] = static_cast<float>(aht > 0 ? n.aht.norm(aht) : 0.5);
    in.ctx[static_cast<size_t>(b * kCtxFeats + 2)] = static_cast<float>(arr > 0 ? std::clamp(ab / arr, 0.0, 1.0) : 0);
  }
  const double tod = time_of_day(r.t_ms) * 2 * std::numbers::pi;
  const double dow = weekday(r.t_ms) / 7.0 * 2 * std::numbers::pi;
  in.glob = {static_cast<float>(std::sin(tod)), static_cast<float>(std::cos(tod)), static_cast<float>(std::sin(dow)),
             static_cast<float>(std::cos(dow)), r.holiday ? 1.0f : 0.0f, r.event ? 1.0f : 0.0f,
             static_cast<float>(n.r.hi / 8.0), static_cast<float>(n.r.lo / 8.0)};
  return in;
}

Denorm denormalize(const NetOutput& o, const QueueNorm& n) {
  Denorm d;
  for (int q = 0; q < kQuantiles; ++q) {
    d.h1[q].resize(kH1Steps);
    d.h2[q].resize(kH2Buckets);
    d.aht[q].resize(kH2Buckets);
    for (int k = 0; k < kH1Steps; ++k) {
      const size_t i = static_cast<size_t>(k * kQuantiles + q);
      d.h1[q][static_cast<size_t>(k)] = i < o.h1.size() ? n.r.inv(o.h1[i]) : 0;  // arrivals in that minute
    }
    for (int b = 0; b < kH2Buckets; ++b) {
      const size_t i = static_cast<size_t>(b * kQuantiles + q);
      d.h2[q][static_cast<size_t>(b)] = i < o.h2a.size() ? n.r.inv(o.h2a[i]) / 60.0 : 0;  // per second
      d.aht[q][static_cast<size_t>(b)] = i < o.h2h.size() ? n.aht.inv(o.h2h[i]) : 0;
    }
  }
  return d;
}

}  // namespace pcr::fc
