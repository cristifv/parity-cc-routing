// The learned forecaster's features and normalization (PLAN §3.8, §5.9; the user's design,
// 2026-11-01). Torch-free: the trainer and RE build the model's inputs with the same code, so what the
// model saw in training is what it sees in production.
//
// Normalization (binding): counts are put on one per-minute basis (a 10 s count x 6, a 15-min count
// / 15), then log1p, then a FIXED min-max range per (tenant, queue) from history (p1/p99 of the
// per-minute values), stored with the model and never recomputed per window. NO RevIN and NO
// clipping: a spike stays above 1 and feeds the OOD check; the inverse uses the same range. A new
// tenant (or queue) gets a range from its expected volume until it has a few days of data. Handle
// time: log seconds with its own fixed range.
#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace pcr::fc {

inline constexpr int64_t kStepMs = 10'000;     // H1 input step
inline constexpr int64_t kMinuteMs = 60'000;
inline constexpr int64_t kBucketMs = 900'000;  // 15-min bucket (routing_bucket_actuals)
inline constexpr int kStepsPerMin = 6;
inline constexpr int kStepsPerBucket = 90;
// The model's fixed input layout (NetConfig may change widths and depths, never these).
inline constexpr int kH1Tokens = 12;                       // the last 12 minutes ...
inline constexpr int kH1Patch = kStepsPerMin;              // ... each its six 10 s counts
inline constexpr int kH1Inputs = kH1Tokens * kH1Patch;     // 72 steps
inline constexpr int kCtxTokens = 8;                       // the last 8 completed 15-min buckets
inline constexpr int kCtxFeats = 3;                        // arrivals, AHT, abandon ratio
inline constexpr int kGlobFeats = 8;                       // calendar + level
inline constexpr int kH1Steps = 60;                        // H1: the next 60 one-minute steps
inline constexpr int kH2Buckets = 8;                       // H2: the next 8 fifteen-minute buckets
inline constexpr int kQuantiles = 3;                       // p10, p50, p90
inline constexpr double kQ[kQuantiles] = {0.1, 0.5, 0.9};

// The fixed min-max range of log1p(per-minute arrivals) of one (tenant, queue).
struct Range {
  double lo = 0, hi = 1;
  static constexpr double kMinSpan = 0.5;  // a near-constant history still gets a usable scale
  double span() const { return hi - lo > kMinSpan ? hi - lo : kMinSpan; }
  double norm(double per_min) const;   // per_min >= 0 -> normalized (may exceed 1: no clipping)
  double inv(double z) const;          // normalized -> per minute (>= 0)
  // p1/p99 of log1p(per-minute values) of the history.
  static Range from_history(const std::vector<double>& per_min);
  // A new tenant or queue: the expected volume (calls per hour at a normal peak, from plan or seats).
  static Range from_expected(double peak_per_hour);
};

struct AhtRange {
  double lo = 3.4, hi = 6.8;  // log(30 s) .. log(900 s) until history says otherwise
  double span() const { return hi - lo > 0.2 ? hi - lo : 0.2; }
  double norm(double aht_s) const;
  double inv(double z) const;
  static AhtRange from_history(const std::vector<double>& aht_s);
};

// One queue's normalization, stored with its model (normalization jsonb, base.json).
struct QueueNorm {
  Range r;
  AhtRange aht;
  // The trained envelope: inputs above this (normalized) were never seen in training (OOD).
  double envelope = 2.0;
  // PSI reference: the share of normalized 1-min inputs per bin (kPsiBins bins over [0, 1) plus
  // below 0 and at or above 1), from the training history.
  std::vector<double> psi_ref;
  bool expected = false;  // the range came from expected volume (no history yet)
  nlohmann::json to_json() const;
  static QueueNorm from_json(const nlohmann::json& j);
};
inline constexpr int kPsiBins = 10;
// The PSI histogram bin of a normalized value (kPsiBins + 2 bins).
int psi_bin(double z);
std::vector<double> psi_hist(const std::vector<double>& z);
double psi(const std::vector<double>& ref, const std::vector<double>& cur);

// What the model needs at an anchor time t (the end of a complete 10 s step), oldest first.
struct Recent {
  int64_t t_ms = 0;
  std::vector<double> c10;      // kH1Inputs counts per 10 s step (complete steps up to t)
  std::vector<double> b_arr;    // kCtxTokens completed buckets: offered
  std::vector<double> b_aht;    //   AHT in seconds (0: no call handled)
  std::vector<double> b_ab;     //   abandoned
  bool holiday = false;         // config.holidays (or the catalogue's holiday days)
  bool event = false;           // a known event (campaign) is running
};

// One queue's inputs in the model's layout (normalized).
struct NetInput {
  std::vector<float> h1;    // kH1Inputs
  std::vector<float> ctx;   // kCtxTokens * kCtxFeats
  std::vector<float> glob;  // kGlobFeats
};
NetInput build_input(const Recent& r, const QueueNorm& n);

// One queue's outputs (normalized; quantiles in kQ order, monotone by construction).
struct NetOutput {
  std::vector<float> h1;   // kH1Steps * kQuantiles: log1p per-minute arrivals, normalized
  std::vector<float> h2a;  // kH2Buckets * kQuantiles: log1p per-minute arrivals (bucket mean), normalized
  std::vector<float> h2h;  // kH2Buckets * kQuantiles: log AHT, normalized
};

// The denormalized forecast of one queue.
struct Denorm {
  std::vector<double> h1[kQuantiles];   // per minute: arrivals in that minute
  std::vector<double> h2[kQuantiles];   // per bucket: arrivals per second
  std::vector<double> aht[kQuantiles];  // per bucket: AHT seconds
};
Denorm denormalize(const NetOutput& o, const QueueNorm& n);

// Calendar (UTC; the tenant's time zone arrives with config.holidays later, PLAN §5.3).
double time_of_day(int64_t t_ms);  // [0, 1)
int weekday(int64_t t_ms);         // 0 = Monday
// The per-minute-equivalent of a count over `ms` milliseconds.
inline double per_min(double count, int64_t ms) { return ms > 0 ? count * 60'000.0 / static_cast<double>(ms) : 0; }

}  // namespace pcr::fc
