// routing-scenario.v1 arrivals as a rate function (PLAN §7.1; phase 5b+5c additions: spike decay,
// level steps, overdispersion, bursts, bots, redials): shared by the series DES (training series) and
// the forecaster's OOD runner, so both read a scenario file the same way.
#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <random>
#include <string>
#include <vector>

namespace pcr::sim {

struct RateModel {
  enum class Kind { kPiecewise, kProfile, kPoisson, kList };
  Kind kind = Kind::kPiecewise;
  double bucket_s = 3600;
  std::vector<double> per_hour;  // piecewise / profile (already scaled)
  std::vector<std::pair<double, double>> points;  // poisson: (t_s, per_hour), linear between
  std::vector<double> at_s;      // list
  struct Spike {
    double start = 0, duration = 0, mult = 1, ramp = 0, decay = -1;  // decay < 0: ramp down inside
  };
  std::vector<Spike> spikes;
  struct Step {
    double at = 0, mult = 1, ramp = 0;
  };
  std::vector<Step> steps;
  double overdispersion = 0;            // cv of a per-minute gamma factor
  double burst_per_hour = 0, burst_mean = 1;
  struct Bots {
    double start = 0, duration = 0, per_hour = 0, patience_s = 3, talk_s = 5;
  };
  std::vector<Bots> bots;

  static RateModel from_json(const nlohmann::json& arrivals);
  // The planned rate (per second): the base shape and level steps, no spikes (what staffing plans for).
  double planned(double t) const;
  // The true rate (per second) without overdispersion, bursts or bots.
  double rate(double t) const;
  double bot_rate(double t) const;
  // The largest rate over [0, horizon] (thinning bound), sampled every `step` seconds.
  double max_rate(double horizon, double step = 10) const;
};

// The per-minute gamma factors of an overdispersed rate (mean 1, cv), for minutes [0, n).
std::vector<double> gamma_factors(double cv, size_t n, std::mt19937_64& rng);

}  // namespace pcr::sim
