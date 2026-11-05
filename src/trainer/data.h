// The trainer's data (PLAN §5.9), torch-free: series -> normalization, training examples (inputs
// through the same fc::build_input RE uses, normalized targets) and the holdout evaluation of a net
// against the baseline (the real opt::Forecaster fed the same observations).
#pragma once

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "sim/series.h"
#include "src/forecast/features.h"
#include "src/forecast/net.h"

namespace pcr::trainer {

// One queue's series in the trainer's shape (from the series DES or from routing_bucket_actuals).
struct QSeries {
  std::string tenant, queue;
  int64_t start_ms = 0;          // step 0
  std::vector<float> c10;        // per 10 s step; NaN = missing
  std::vector<float> b_off, b_ab, b_aht;  // per 15-min bucket; NaN = missing; aht 0 = no call
  std::vector<uint8_t> holiday, event;    // per bucket
  fc::QueueNorm norm;
  int64_t first_step = 0;        // the first step with data
};

// Series DES output -> the trainer's queues (normalization from `hist` when given, else from itself;
// a renamed queue gets an expected-volume range).
std::vector<QSeries> from_sim(const sim::SeriesTenant& t, const std::vector<QSeries>* hist, std::mt19937_64& rng);

// Normalization of one queue from its own history (p1/p99, AHT range, PSI reference).
fc::QueueNorm norm_from(const QSeries& q, double envelope);

// The model's inputs at anchor step s (steps <= s complete) and the normalized targets.
struct Example {
  fc::NetInput in;
  std::vector<float> t1, t2a, t2h;
};
bool make_example(const QSeries& q, int64_t s, Example* out, bool targets = true);
int64_t first_anchor(const QSeries& q);
int64_t last_anchor(const QSeries& q);

// The holdout evaluation: WAPE of H1 (first 15 one-minute steps) and H2 (8 buckets) for the net and
// the baseline at anchors every `every_steps` steps. The baseline gets `hist` as its profile.
struct Wape {
  double h1_abs = 0, h1_act = 0, h2_abs = 0, h2_act = 0;
  double b_h1_abs = 0, b_h1_act = 0, b_h2_abs = 0, b_h2_act = 0;
  uint64_t anchors = 0;
  double h1() const { return h1_act > 0 ? h1_abs / h1_act : 0; }
  double h2() const { return h2_act > 0 ? h2_abs / h2_act : 0; }
  double b_h1() const { return b_h1_act > 0 ? b_h1_abs / b_h1_act : 0; }
  double b_h2() const { return b_h2_act > 0 ? b_h2_abs / b_h2_act : 0; }
  void add(const Wape& o);
};
// Anchors start at `from_step` (a fine-tune's holdout day) or the queue's first anchor.
Wape evaluate(const fc::Net* net, const std::vector<QSeries>& eval, const std::vector<QSeries>& hist, int every_steps,
              int64_t from_step = 0);

}  // namespace pcr::trainer
