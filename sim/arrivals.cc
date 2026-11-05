#include "sim/arrivals.h"

#include <algorithm>
#include <cmath>

namespace pcr::sim {

RateModel RateModel::from_json(const nlohmann::json& a) {
  RateModel m;
  const std::string k = a.value("kind", std::string("piecewise"));
  if (k == "piecewise") {
    m.kind = Kind::kPiecewise;
    m.bucket_s = a.value("bucket_s", 3600.0);
    for (const auto& x : a.at("per_hour")) m.per_hour.push_back(x.get<double>());
  } else if (k == "profile") {
    m.kind = Kind::kProfile;
    m.bucket_s = a.value("bucket_s", 3600.0);
    double peak = 0;
    for (const auto& x : a.at("shape")) peak = std::max(peak, x.get<double>());
    const double scale = a.value("scale_per_hour", 0.0);
    for (const auto& x : a.at("shape")) m.per_hour.push_back(peak > 0 ? x.get<double>() / peak * scale : 0);
  } else if (k == "poisson") {
    m.kind = Kind::kPoisson;
    for (const auto& p : a.at("points")) m.points.emplace_back(p.at("t_s").get<double>(), p.at("per_hour").get<double>());
  } else {
    m.kind = Kind::kList;
    for (const auto& x : a.at("at_s")) m.at_s.push_back(x.get<double>());
  }
  for (const auto& s : a.value("spikes", nlohmann::json::array())) {
    m.spikes.push_back(Spike{s.at("start_s").get<double>(), s.at("duration_s").get<double>(), s.at("multiplier").get<double>(),
                             s.value("ramp_s", 0.0), s.contains("decay_s") ? s.at("decay_s").get<double>() : -1.0});
  }
  for (const auto& s : a.value("level_steps", nlohmann::json::array())) {
    m.steps.push_back(Step{s.at("at_s").get<double>(), s.at("multiplier").get<double>(), s.value("ramp_s", 0.0)});
  }
  m.overdispersion = a.value("overdispersion", 0.0);
  if (a.contains("bursts")) {
    m.burst_per_hour = a["bursts"].value("per_hour", 0.0);
    m.burst_mean = std::max(1.0, a["bursts"].value("mean_size", 1.0));
  }
  for (const auto& b : a.value("bots", nlohmann::json::array())) {
    m.bots.push_back(Bots{b.at("start_s").get<double>(), b.at("duration_s").get<double>(), b.at("per_hour").get<double>(),
                          b.value("patience_s", 3.0), b.value("talk_s", 5.0)});
  }
  return m;
}

double RateModel::planned(double t) const {
  double ph = 0;
  if (kind == Kind::kPiecewise || kind == Kind::kProfile) {
    if (!per_hour.empty()) {
      const size_t i = std::min(per_hour.size() - 1, static_cast<size_t>(std::max(0.0, t) / bucket_s));
      ph = per_hour[i];
    }
  } else if (kind == Kind::kPoisson && !points.empty()) {
    if (t <= points.front().first) {
      ph = points.front().second;
    } else if (t >= points.back().first) {
      ph = points.back().second;
    } else {
      auto it = std::upper_bound(points.begin(), points.end(), t,
                                 [](double v, const std::pair<double, double>& p) { return v < p.first; });
      const auto& b = *it;
      const auto& a = *(it - 1);
      const double x = (t - a.first) / std::max(1e-9, b.first - a.first);
      ph = a.second * (1 - x) + b.second * x;
    }
  }
  double m = 1;
  for (const auto& s : steps) {
    if (t < s.at) continue;
    const double f = s.ramp > 0 ? std::min(1.0, (t - s.at) / s.ramp) : 1.0;
    m *= 1 + (s.mult - 1) * f;
  }
  return std::max(0.0, ph * m / 3600.0);
}

double RateModel::rate(double t) const {
  double m = 1;
  for (const auto& s : spikes) {
    const double a = s.start, e = s.start + s.duration;
    double f = 0;
    if (s.decay < 0) {
      if (t < a || t >= e) continue;
      f = s.ramp > 0 ? std::min({1.0, (t - a) / s.ramp, (e - t) / s.ramp}) : 1.0;
    } else {
      if (t < a || t >= e + s.decay) continue;
      if (t < e) {
        f = s.ramp > 0 ? std::min(1.0, (t - a) / s.ramp) : 1.0;
      } else {
        f = s.decay > 0 ? 1.0 - (t - e) / s.decay : 0.0;
      }
    }
    m *= 1 + (s.mult - 1) * std::max(0.0, f);
  }
  return planned(t) * m;
}

double RateModel::bot_rate(double t) const {
  double r = 0;
  for (const auto& b : bots) {
    if (t >= b.start && t < b.start + b.duration) r += b.per_hour / 3600.0;
  }
  return r;
}

double RateModel::max_rate(double horizon, double step) const {
  double mx = 0;
  for (double t = 0; t <= horizon; t += step) mx = std::max(mx, rate(t));
  for (const auto& s : spikes) mx = std::max(mx, rate(s.start + std::min(s.duration, s.ramp)));
  return mx;
}

std::vector<double> gamma_factors(double cv, size_t n, std::mt19937_64& rng) {
  std::vector<double> f(n, 1.0);
  if (!(cv > 0)) return f;
  const double k = 1 / (cv * cv);  // shape; scale 1/k: mean 1, cv
  std::gamma_distribution<double> g(k, 1 / k);
  for (auto& x : f) x = g(rng);
  return f;
}

}  // namespace pcr::sim
