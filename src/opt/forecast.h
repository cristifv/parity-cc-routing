// The baseline forecaster (PLAN §5.3, phase 5; always computed, the libtorch model of phase 5b falls
// back to it): per queue, arrivals for the next 8 buckets of 15 min from an EWMA of recent arrivals
// over 5, 15 and 60 minutes with a damped trend, blended with a per-(weekday, 15-min slot) profile
// of the tenant's own routing_bucket_actuals; handle time, wrap-up and patience by EWMA. Every
// forecast carries its source: `config_default` (a new queue: nothing observed yet), `ewma`, or
// `ewma_profile` (at least `profile_min_weeks` of history at the slots involved). One instance per
// tenant, on its optimization thread only (no locks): tenant isolation by construction.
//
// Phase 5 redesign (the user, 2026-11-01): the CTMC drives every decision; the NNMPC becomes a
// learned SHORT-TERM forecaster only (phases 5b/5c) feeding it. It plugs in through ForecastSource:
// per queue it answers a forecast (source `model`) or nothing (missing, stale, out of bounds,
// drifting), and then the baseline is used. The baseline is always computed.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace pcr::opt {

struct ForecastOptions {
  int buckets = 8;
  int64_t bucket_ms = 900'000;
  int profile_min_weeks = 2;
  double level_ratio_min = 0.5, level_ratio_max = 2.0;  // EWMA / profile, clamped
  double blend_tau_s = 1800;   // the EWMA's weight decays with the horizon: exp(-h / tau)
  double trend_horizon_s = 1800;  // the trend is damped beyond this
  double warm_s = 300;         // observed less than this: config_default
  double patience_prior_s = 120, patience_prior_weight_s = 120;  // theta's prior (as phase 2)
};

struct QueueForecast {
  std::string queue;
  std::string source;          // config_default | ewma | ewma_profile
  int64_t first_bucket_ms = 0; // the bucket now running
  std::vector<double> per_s;   // arrivals per second, one per bucket (the first: the bucket now)
  double now_per_s = 0;        // the rate the CTMC uses this tick (the current bucket's blend)
  double trend_per_s2 = 0;     // the EWMA's slope (per second, per second)
  double aht_s = 0, wrap_s = 0, patience_s = 0;
  double vip_share = 0;        // the VIP class's share of arrivals (EWMA, 15 min)
  // Phases 5b+5c. The baseline: the last minute's observed arrival rate (an EWMA over 60 s; -1 before
  // anything was observed), the sanity window's second reference.
  double observed_per_s = -1;
  // A learned source: it owns the arrival rates; its handle time only when it opted in (owns_aht,
  // --forecast-aht=on), and the optimizer still applies the pooled-AHT check to it (CR-180).
  bool owns_aht = false;
  bool aht_from_model = false;             // the optimizer used the model's aht_s (owned and past the check)
  uint64_t model_version = 0;              // source model: the tenant's model version (0: the base)
  std::vector<double> p10_per_s, p90_per_s;  // source model: per bucket
  std::string fallback;                    // a learned source is on but the baseline stands: why
};

// What a learned source sees of every snapshot window (the observe/actuals hook, CR-181).
struct ObservedWindow {
  std::string queue;
  int64_t at_ms = 0, window_ms = 0;  // the window [at_ms - window_ms, at_ms]
  double arrivals = 0, abandons = 0, handled = 0, handle_s = 0;
  double expected_per_hour = 0;      // seats-based expected volume (a new queue's range)
};
// One routing_bucket_actuals row of the tenant's history (oldest first).
struct HistoryRow {
  std::string queue;
  int64_t bucket_start_ms = 0;
  double offered = 0, abandoned = 0, handled = 0, handle_ms = 0;
  std::vector<double> offered_10s;   // 90 counts, empty for rows written before db/V3
};

// A learned forecaster (phases 5b+5c) behind one interface. Called on the tenant's optimization
// thread with the baseline's forecast for the same queue; returns its own forecast (any `source` it
// sets is replaced by "model") or nothing, and then the baseline stands.
//
// Thread contract (CR-181): every call for one tenant comes from that tenant's optimization thread
// (a tenant is pinned to one worker); calls for different tenants run at the same time on different
// workers. An implementation keeps tenants apart and guards only what they share.
// Ownership (CR-180): the source owns ARRIVAL RATES only. Its aht_s is used only when it sets
// owns_aht, and then only where it differs from the tenant's pooled handle time by more than two
// standard errors (the rule a queue's own estimate follows).
class ForecastSource {
 public:
  virtual ~ForecastSource() = default;
  virtual std::optional<QueueForecast> forecast(const std::string& tenant, const std::string& queue, int64_t now_ms,
                                                const QueueForecast& baseline) = 0;
  // The actuals hook: every queue's snapshot window, before the same tick's forecast() calls.
  virtual void observe(const std::string& /*tenant*/, const std::vector<ObservedWindow>& /*w*/) {}
  // The tenant's routing_bucket_actuals history (as the baseline's profile gets it).
  virtual void history(const std::string& /*tenant*/, const std::vector<HistoryRow>& /*rows*/) {}
  // After forecast() answered nothing: why (a fallback reason), or "".
  virtual std::string why_not(const std::string& /*tenant*/, const std::string& /*queue*/) const { return ""; }
};

// The sanity window (the p5 review, CR-180): a learned forecast is refused when its rate now or any
// bucket's is outside [lo, hi] times the baseline's. The reference is widened by the last minute's
// observed rate (a spike the model already sees isn't "insane" while the baseline's EWMA catches up);
// rates below min_per_s on both sides are not compared (noise).
struct Sanity {
  double lo = 0.2, hi = 5.0;
  double min_per_s = 1.0 / 120;  // half a call a minute
};
bool within_sanity(const QueueForecast& f, const QueueForecast& baseline, const Sanity& s = {});

// The forecast the CTMC uses: the learned one when it answers with finite, non-negative rates for as
// many buckets as the baseline, inside the sanity window (source "model"), else the baseline (with
// `fallback` set to the learned source's reason, or "ratio" for the sanity window).
QueueForecast choose_forecast(const QueueForecast& baseline, ForecastSource* learned, const std::string& tenant,
                              int64_t now_ms, const Sanity& s = {});

class Forecaster {
 public:
  explicit Forecaster(ForecastOptions o = {}) : o_(o) {}
  // A snapshot window of one queue: arrivals, abandons and time waited over window_s seconds, and
  // the writer's EWMAs of handle (talk + wrap-up) and wrap-up time.
  void observe(const std::string& queue, int64_t at_ms, double window_s, double arrivals, double abandons,
               double waited_s, double aht_s, double wrap_s, double vip_arrivals = 0, double handled = 0,
               double handle_s = 0);
  // The raw 15-minute estimates behind a queue's handle time and patience, with their sample sizes
  // (the optimizer pools queues whose differences are within their noise).
  struct Raw {
    double aht_s = 0, calls = 0;       // handle time from the window sums; calls in 15 min
    double abandons = 0, waited = 0;   // per second, seconds per second (15 min)
  };
  Raw raw(const std::string& queue) const;
  // History: one routing_bucket_actuals row (offered in a 15-min bucket).
  void add_actual(const std::string& queue, int64_t bucket_start_ms, double offered);
  QueueForecast forecast(const std::string& queue, int64_t now_ms, double default_aht_s) const;
  // theta, the exponential MLE of patience (abandons / time waited) with a weak prior.
  double theta(const std::string& queue) const;
  size_t profile_rows() const { return rows_; }

 private:
  struct Q {
    bool seen = false;
    double observed_s = 0;
    double e1 = 0, e5 = 0, e15 = 0, e60 = 0;  // arrivals per second
    double abandons = 0, waited = 0;  // per second, seconds per second (15 min)
    double aht_s = 0, wrap_s = 0;
    double vip = 0;  // VIP arrivals per second (15 min)
    double hn = 0, hs = 0;  // calls handled per second, handle seconds per second (15 min)
    // (weekday * 96 + slot) -> week index (bucket_start / week) -> offered
    std::map<int, std::map<int64_t, double>> profile;
  };
  double profile_at(const Q& q, int64_t t_ms, bool* ok) const;
  ForecastOptions o_;
  std::map<std::string, Q> q_;
  size_t rows_ = 0;
};

}  // namespace pcr::opt
