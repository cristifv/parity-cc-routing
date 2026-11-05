// The learned short-term forecaster inside RE (PLAN §3.8, §5.9; phases 5b+5c): an
// opt::ForecastSource that runs the NNMPC forecaster net per tenant (its queues are the batch),
// watches its own health per (tenant, queue) and answers nothing whenever the baseline must stand.
// Torch-free: the net comes in through fc::Net (src/forecast/net.h), so this builds and is tested
// in every lane, sanitizers included.
//
// What it owns (the p5 review's seam, CR-180/181):
//   - arrival rates only; handle time only when SourceOptions::owns_aht (--forecast-aht=on), and the
//     optimizer still applies the pooled-AHT check to it;
//   - thread contract: observe(), forecast() and why_not() of one tenant run on that tenant's
//     optimization thread only (a tenant is pinned to one worker). Tenants are kept apart: the map of
//     tenants is the only shared state (a mutex), each tenant's state is touched by its worker only,
//     models handed over by set_models() (any thread) are swapped in by the worker under the mutex.
//     A loaded Net is immutable and run() is const (the base net is shared by every tenant);
//   - the observe/actuals hook: every snapshot window feeds the inputs, the realized-error monitor
//     and the scoring (shadow, promotion, demotion, the prediction error per source);
//   - the 0.2-5x sanity window against the baseline (opt::within_sanity, shared with choose_forecast).
//
// Fallback (OOD, per (tenant, queue), the same evaluation that detects it): no_model, stale,
// above_range, drift, ratio, width, realized_error, nan, negative, slow. Hysteresis: 3 healthy
// evaluations before the model drives again. Every entry into fallback is counted per reason and
// logged (forecast.fallback); every change of source too (forecast.source_changed).
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

#include "src/forecast/features.h"
#include "src/forecast/net.h"
#include "src/opt/forecast.h"

namespace pcr::fc {

enum class Reason : uint8_t {
  kNone = 0, kNoModel, kStale, kAboveRange, kDrift, kRatio, kWidth, kRealizedError, kNan, kNegative, kSlow, kCount
};
const char* reason_name(Reason r);
inline constexpr int kReasons = static_cast<int>(Reason::kCount);

struct SourceOptions {
  int hysteresis = 3;                     // healthy evaluations before driving again
  int64_t stale_input_ms = 30'000;        // the last input older than this: stale
  double max_missing = 0.10;              // of the 72-step input window
  int64_t max_model_age_ms = 14LL * 86'400'000;  // a tenant model older than this: stale
  double ratio_lo = 0.2, ratio_hi = 5.0;  // forecast / baseline (opt::within_sanity)
  double max_width = 3.0;                 // (p90 - p10) / p50 over H1's first 5 minutes
  double psi_max = 0.25;
  // PSI needs the whole 6 h of 1-min inputs: on fewer, sampling noise and empty bins alone push it
  // past 0.25 on normal traffic (the OOD suite's first run, 2026-11-02).
  int psi_min_minutes = 360;
  double z_max = 4.0;                     // realized error over the last minute (6 steps) ...
  int z_persist = 3;                      // ... for this many evaluations in a row
  double slow_ms = 20.0;                  // inference above this: slow (this tick)
  bool owns_aht = false;                  // --forecast-aht=on
  int64_t shadow_ms = 3LL * 86'400'000;   // a candidate's shadow window
  double promote_margin = 0.05;           // promoted when its WAPE is >= 5 % better than both
  int64_t demote_window_ms = 86'400'000;  // the active model vs the baseline over this window
  double min_actual = 50;                 // calls a scoring window needs before it decides anything
  double partial_min_ms = 1'000;          // a partial input step counts (scaled) after this long
};

// One loaded model. version 0 is the shared base model (no per-queue norms: they come from the
// tenant's history, else its expected volume).
struct ModelSpec {
  uint64_t version = 0;
  std::string sha256;
  int64_t created_at_ms = 0;
  std::shared_ptr<const Net> net;
  std::map<std::string, QueueNorm> norm;
  double envelope = 2.0;
  std::vector<double> psi_ref;
};

// Status changes of the tenant's model rows (RE: Postgres; tests: a fake). Called on the tenant's
// optimization thread: must only queue.
class ModelStore {
 public:
  virtual ~ModelStore() = default;
  virtual void set_status(const std::string& tenant, uint64_t version, const std::string& status,
                          const nlohmann::json& shadow, int64_t at_ms) = 0;
};

// A WAPE accumulator: sum |forecast - actual| and sum actual.
struct WapeSum {
  double abs = 0, act = 0;
  double wape() const { return act > 0 ? abs / act : 0; }
  void add(double f, double a) {
    abs += std::abs(f - a);
    act += a;
  }
  void add(const WapeSum& o) {
    abs += o.abs;
    act += o.act;
  }
};
// The prediction error per source (PLAN §5.9 "Measured"): arrivals at 1 min (H1's first 15 one-minute
// steps after each minute anchor) and at 15 min (the bucket running at each bucket start).
enum class Src : uint8_t { kBaseline = 0, kModel, kInUse, kCandidate, kCount };
inline constexpr int kSrcs = static_cast<int>(Src::kCount);
const char* src_name(Src s);
struct PredError {
  std::array<WapeSum, kSrcs> m1, m15;
  void add(const PredError& o);
};

class LearnedSource final : public opt::ForecastSource {
 public:
  LearnedSource(SourceOptions o, std::shared_ptr<const ModelSpec> base, ModelStore* store = nullptr);
  ~LearnedSource() override;

  // opt::ForecastSource.
  std::optional<opt::QueueForecast> forecast(const std::string& tenant, const std::string& queue, int64_t now_ms,
                                             const opt::QueueForecast& baseline) override;
  void observe(const std::string& tenant, const std::vector<opt::ObservedWindow>& w) override;
  void history(const std::string& tenant, const std::vector<opt::HistoryRow>& rows) override;
  std::string why_not(const std::string& tenant, const std::string& queue) const override;

  // The tenant's models from the store (any thread; null: none). Swapped in at the tenant's next
  // evaluation. Only models RE already checked (written_by, sha256, config) get here.
  void set_models(const std::string& tenant, std::shared_ptr<const ModelSpec> active,
                  std::shared_ptr<const ModelSpec> candidate);
  // The versions the source holds for a tenant (the loader skips unchanged rows).
  std::pair<uint64_t, uint64_t> versions(const std::string& tenant) const;

  struct QueueHealth {
    bool known = false, driving = false;
    Reason reason = Reason::kNone;       // the last evaluation's first failing reason
    int streak = 0;                      // healthy evaluations in a row
    uint64_t evaluations = 0, entries = 0;  // fallback entries (flapping)
    std::array<uint64_t, kReasons> fired{};  // evaluations each reason fired in
    std::array<int64_t, kReasons> first_ms{};  // the first time each reason fired (0: never)
    int64_t last_change_ms = 0;
    double last_input_max = 0;           // the largest normalized 1-min input in the window
  };
  QueueHealth health(const std::string& tenant, const std::string& queue) const;
  PredError error(const std::string& tenant) const;  // the tenant's prediction error so far
  uint64_t active_version(const std::string& tenant) const;

  struct Totals {
    uint64_t inferences = 0, inference_failed = 0, slow = 0, shadow_runs = 0;
    uint64_t promoted = 0, rejected = 0, demoted = 0, models_set = 0;
    std::array<uint64_t, kReasons> fallback_entries{}, fallback_evaluations{};
    // Inference time per tenant run (µs): sum, max, and a histogram of 64 µs buckets up to ~33 ms.
    uint64_t us_sum = 0, us_max = 0;
    std::array<uint64_t, 512> us_hist{};
    double us_quantile(double q) const;
    PredError err;
  };
  Totals totals() const;

 private:
  struct TState;
  TState* state(const std::string& tenant);
  const TState* find(const std::string& tenant) const;

  SourceOptions o_;
  std::shared_ptr<const ModelSpec> base_;
  ModelStore* store_;
  mutable std::mutex mu_;  // tenants_ (the map) and each TState's pending models only
  std::map<std::string, std::unique_ptr<TState>> tenants_;
  struct Atomics;
  std::unique_ptr<Atomics> a_;
};

}  // namespace pcr::fc
