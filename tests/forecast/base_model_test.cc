// The shipped base model (model/forecast/base.*; PLAN §5.9) inside the learned source: it loads
// only with its sha256, a pinned sha256 that differs is refused, and the trained net reacts to a
// spike's onset in the same evaluation (the running 10 s step counts once it is 1 s old).
#include <gtest/gtest.h>

#include <random>

#include "src/forecast/model_file.h"
#include "src/forecast/source.h"
#include "src/forecast/torch_net.h"

namespace pcr::fc {
namespace {

constexpr int64_t T0 = 1'772'438'400'000;  // 2026-03-02 08:00 UTC, a Monday

opt::QueueForecast baseline(double per_min) {
  opt::QueueForecast b;
  b.queue = "q";
  b.source = "ewma";
  b.per_s.assign(8, per_min / 60);
  b.now_per_s = per_min / 60;
  b.observed_per_s = per_min / 60;
  b.aht_s = 180;
  return b;
}

TEST(BaseModel, LoadsWithItsSha256AndAWrongPinIsRefused) {
  torch_threads(1, 1);
  std::string why;
  auto base = load_base_spec("model/forecast", "", &why);
  ASSERT_TRUE(base) << why;
  EXPECT_EQ(base->version, 0u);
  EXPECT_GT(base->envelope, 1.0);
  EXPECT_EQ(base->psi_ref.size(), static_cast<size_t>(kPsiBins + 2));
  EXPECT_LT(base->net->parameters(), 250'000u);  // < 1 MB serialized
  EXPECT_FALSE(load_base_spec("model/forecast", std::string(64, 'a'), &why));
  EXPECT_EQ(why, "sha256_pin");
  EXPECT_FALSE(load_base_spec("model/no-such-dir", "", &why));
}

TEST(BaseModel, TheTrainedNetFollowsASpikeInTheSameEvaluation) {
  torch_threads(1, 1);
  std::string why;
  auto base = load_base_spec("model/forecast", "", &why);
  ASSERT_TRUE(base) << why;
  LearnedSource src(SourceOptions{}, base);
  std::mt19937_64 rng(3);
  int64_t now = T0;
  auto window = [&](int64_t len, double per_min) {
    opt::ObservedWindow w;
    w.queue = "q";
    w.at_ms = now;
    w.window_ms = len;
    w.arrivals = std::poisson_distribution<int>(per_min * static_cast<double>(len) / 60'000.0)(rng);
    w.expected_per_hour = 600;  // a busy queue (10 a minute at its peak)
    src.observe("acme", {w});
  };
  double before = 0;
  for (int i = 0; i < 90; ++i) {
    now += 10'000;
    window(10'000, 8);
    auto f = src.forecast("acme", "q", now, baseline(8));
    if (f) before = f->now_per_s * 60;
  }
  ASSERT_TRUE(src.health("acme", "q").driving) << reason_name(src.health("acme", "q").reason);
  EXPECT_GT(before, 4);
  EXPECT_LT(before, 14);
  // A 4x onset: 2 s at 32 a minute, then the evaluation 2 s into the step (an early snapshot).
  now += 2'000;
  opt::ObservedWindow w;
  w.queue = "q";
  w.at_ms = now;
  w.window_ms = 2'000;
  w.arrivals = 4;  // 2 s at ~120 a minute: the burst a guard breach hands over
  w.expected_per_hour = 600;
  src.observe("acme", {w});
  auto f = src.forecast("acme", "q", now, baseline(8));
  const auto h = src.health("acme", "q");
  if (f) {
    EXPECT_GT(f->now_per_s * 60, 1.1 * before);  // the model's rate now moved at once
  } else {
    // Or the burst is beyond what it was trained on: the same-evaluation fallback.
    EXPECT_NE(h.reason, Reason::kNone);
  }
  std::printf("base model: before %.2f/min, 2 s into a 4x onset %s (reason %s, input max %.2f, envelope %.2f)\n", before,
              f ? std::to_string(f->now_per_s * 60).c_str() : "fallback", reason_name(h.reason), h.last_input_max,
              base->envelope);
  // The spike goes on at 32 a minute: after 30 s the model's rate is well above where it was.
  now += 8'000;
  window(8'000, 32);
  double after30 = 0;
  for (int i = 0; i < 3; ++i) {
    now += 10'000;
    window(10'000, 32);
    auto g = src.forecast("acme", "q", now, baseline(8));
    after30 = g ? g->now_per_s * 60 : -1;
  }
  std::printf("base model: 30 s into the spike %.2f/min (reason %s)\n", after30, reason_name(src.health("acme", "q").reason));
  // Measured 2026-11-02 (the 20-min base): 8.3 -> 10.1 (2 s) -> 9.3 a minute (40 s into a 4x spike):
  // the base model follows a sustained onset only partly (CR-215); the realized-error monitor then
  // hands the queue to the baseline. This pins "it moves the right way", not the size.
  EXPECT_GT(after30, before);
}

}  // namespace
}  // namespace pcr::fc
