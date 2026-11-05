// The learned source's policy (PLAN §5.9), torch-free with a scripted net: every fallback trigger
// falls back in the same evaluation and the model drives again only after 3 healthy evaluations;
// shadow scoring, promotion, rejection, demotion and a rollback; the prediction error per source.
#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <random>
#include <thread>

#include "src/forecast/source.h"

namespace pcr::fc {
namespace {

constexpr int64_t T0 = 1'772'438'400'000;  // 2026-03-02 08:00 UTC
constexpr int64_t kTick = 10'000;

// A scripted net: per queue, H1/H2 p50 = the mean normalized level of the input window's last
// token (kEcho), of the whole window (kSmooth), a fixed level (kConstant), or the echo shifted up
// (kScale: `shift` in normalized units); bands of +/- `d`; or NaN, negative, slow.
struct FakeNet final : Net {
  enum class Mode { kEcho, kSmooth, kConstant, kScale, kNan, kNegative, kSlow };
  Mode mode = Mode::kEcho;
  double d = 0.05, level = 1.0, shift = 1.0;
  NetConfig c;
  const NetConfig& config() const override { return c; }
  size_t parameters() const override { return 1; }
  bool run(const std::vector<NetInput>& in, std::vector<NetOutput>* out) const override {
    if (mode == Mode::kSlow) std::this_thread::sleep_for(std::chrono::milliseconds(25));
    out->clear();
    for (const auto& x : in) {
      double z = 0;
      if (mode == Mode::kSmooth) {
        // log1p of the window's mean rate, on the same scale as one 10 s input (x6 per minute).
        double s = 0;
        for (float v : x.h1) s += v;
        z = s / static_cast<double>(x.h1.size());
      } else {
        for (int k = kH1Inputs - kH1Patch; k < kH1Inputs; ++k) z += x.h1[static_cast<size_t>(k)];
        z /= kH1Patch;
      }
      if (mode == Mode::kConstant) z = level;
      if (mode == Mode::kScale) z += shift;
      if (mode == Mode::kNegative) z = -5;
      const float nan = std::nanf("");
      NetOutput o;
      for (int k = 0; k < kH1Steps; ++k) {
        for (double q : {z - d, z, z + d}) o.h1.push_back(mode == Mode::kNan ? nan : static_cast<float>(q));
      }
      for (int b = 0; b < kH2Buckets; ++b) {
        for (double q : {z - d, z, z + d}) o.h2a.push_back(static_cast<float>(q));
        for (int q = 0; q < 3; ++q) o.h2h.push_back(0.5f);
      }
      out->push_back(std::move(o));
    }
    return true;
  }
};

std::shared_ptr<ModelSpec> spec(std::shared_ptr<FakeNet> n, uint64_t version = 0, double envelope = 2.0) {
  auto s = std::make_shared<ModelSpec>();
  s->version = version;
  s->net = std::move(n);
  s->envelope = envelope;
  s->created_at_ms = T0;
  return s;
}

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

struct Drive {
  LearnedSource* src;
  int64_t now = T0;
  double per_min = 4;          // the arrivals the source observes
  double baseline_per_min = 4;
  std::mt19937_64 rng{7};
  bool poisson = false;
  // One tick: a 10 s window ending at now + 10 s, then the forecast.
  std::optional<opt::QueueForecast> tick(bool observe = true) {
    now += kTick;
    if (observe) {
      opt::ObservedWindow w;
      w.queue = "q";
      w.at_ms = now;
      w.window_ms = kTick;
      w.arrivals = poisson ? static_cast<double>(std::poisson_distribution<int>(per_min / 6)(rng)) : per_min / 6;
      w.expected_per_hour = 240;
      src->observe("acme", {w});
    }
    return src->forecast("acme", "q", now, baseline(baseline_per_min));
  }
  // Ticks until the model drives (bounded).
  int warm(int max = 200) {
    for (int i = 0; i < max; ++i) {
      if (tick()) return i + 1;
    }
    return -1;
  }
};

TEST(Source, NoModelIsAFallback) {
  LearnedSource src(SourceOptions{}, nullptr);
  Drive d{&src};
  for (int i = 0; i < 100; ++i) EXPECT_FALSE(d.tick());
  EXPECT_EQ(src.why_not("acme", "q"), "no_model");
  EXPECT_EQ(opt::choose_forecast(baseline(4), &src, "acme", d.now).fallback, "no_model");
}

TEST(Source, DrivesOnceTheWindowIsFullAndThreeEvaluationsWereHealthy) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  // 72 steps of inputs (10 % may miss: 65), then 3 healthy evaluations.
  const int n = d.warm();
  EXPECT_GE(n, 65 + 2);
  EXPECT_LE(n, 72 + 3);
  auto f = d.tick();
  ASSERT_TRUE(f);
  EXPECT_EQ(f->source, "model");
  EXPECT_NEAR(f->now_per_s * 60, 4.0, 0.3);  // the echo of 4 a minute
  EXPECT_FALSE(f->owns_aht);
  EXPECT_DOUBLE_EQ(f->aht_s, 180);            // arrival rates only (CR-180)
  ASSERT_EQ(f->p10_per_s.size(), 8u);
  EXPECT_LE(f->p10_per_s[0], f->per_s[0]);
  EXPECT_LE(f->per_s[0], f->p90_per_s[0]);
}

// After a trigger: the same evaluation answers nothing; it drives again on the 3rd healthy one, and
// each further entry within 60 min doubles that (anti-flapping: 3, 6, 12, ...).
void expect_hysteresis(Drive& d, LearnedSource& src, int nth_entry = 1) {
  int healthy = 0;
  while (healthy < 200) {
    ++healthy;
    if (d.tick()) break;
  }
  EXPECT_EQ(healthy, 3 << (nth_entry - 1));
  EXPECT_EQ(src.health("acme", "q").reason, Reason::kNone);
}

TEST(Source, StaleInputsFallBackAndRecoverWithHysteresis) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  // Nothing observed for 40 s: stale in that evaluation.
  d.now += 30'000;
  EXPECT_FALSE(d.tick(false));
  EXPECT_EQ(src.health("acme", "q").reason, Reason::kStale);
  EXPECT_EQ(src.why_not("acme", "q"), "stale");
  EXPECT_EQ(src.health("acme", "q").entries, 1u);
  // The 4 missing steps are within the 10 % allowance: back after 3 healthy evaluations.
  expect_hysteresis(d, src);
}

TEST(Source, AnInputClockAheadOfOursIsStale) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  opt::ObservedWindow w;
  w.queue = "q";
  w.at_ms = d.now + 3'600'000;  // the input clock jumped an hour ahead
  w.window_ms = kTick;
  w.arrivals = 1;
  src.observe("acme", {w});
  EXPECT_FALSE(src.forecast("acme", "q", d.now + kTick, baseline(4)));
  EXPECT_EQ(src.health("acme", "q").reason, Reason::kStale);
}

TEST(Source, AnInputAboveTheTrainedEnvelopeFallsBackTheSameTick) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  d.per_min = 4 * 30;  // a 30x burst
  d.baseline_per_min = 4;
  EXPECT_FALSE(d.tick());
  EXPECT_EQ(src.health("acme", "q").reason, Reason::kAboveRange);
  EXPECT_GT(src.health("acme", "q").last_input_max, 2.0);
}

TEST(Source, TheRatioWindow) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  net->mode = FakeNet::Mode::kScale;
  net->shift = 2.5;  // ~ x15 the inputs: outside 0.2-5x of the baseline and the observed rate
  EXPECT_FALSE(d.tick());
  EXPECT_EQ(src.health("acme", "q").reason, Reason::kRatio);
  net->mode = FakeNet::Mode::kEcho;
  expect_hysteresis(d, src);
}

TEST(Source, TheWidthLimitAndTheFlappingBackoff) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  for (int nth = 1; nth <= 3; ++nth) {
    net->d = 1.5;  // p10..p90 spans most of the range
    EXPECT_FALSE(d.tick());
    EXPECT_EQ(src.health("acme", "q").reason, Reason::kWidth);
    net->d = 0.05;
    expect_hysteresis(d, src, nth);  // 3, then 6, then 12 healthy evaluations (entries within 60 min)
  }
  EXPECT_EQ(src.health("acme", "q").entries, 3u);
}

TEST(Source, NanNegativeAndSlowRunsFallBack) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  int nth = 0;
  for (auto [mode, reason] : {std::pair{FakeNet::Mode::kNan, Reason::kNan}, std::pair{FakeNet::Mode::kNegative, Reason::kNegative},
                              std::pair{FakeNet::Mode::kSlow, Reason::kSlow}}) {
    net->mode = mode;
    EXPECT_FALSE(d.tick());
    EXPECT_EQ(src.health("acme", "q").reason, reason) << reason_name(reason);
    net->mode = FakeNet::Mode::kEcho;
    expect_hysteresis(d, src, ++nth);
  }
  const auto t = src.totals();
  EXPECT_EQ(t.fallback_entries[static_cast<size_t>(Reason::kNan)], 1u);
  EXPECT_EQ(t.fallback_entries[static_cast<size_t>(Reason::kNegative)], 1u);
  EXPECT_EQ(t.fallback_entries[static_cast<size_t>(Reason::kSlow)], 1u);
  EXPECT_GE(t.slow, 1u);
}

TEST(Source, TheRealizedErrorMonitorCatchesAModelThatStopsFollowing) {
  auto net = std::make_shared<FakeNet>();
  net->mode = FakeNet::Mode::kConstant;
  net->level = 1.0;  // = 4 a minute on the expected-volume range of 240/h
  LearnedSource src(SourceOptions{}, spec(net, 0, /*envelope=*/6.0));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  d.per_min = 20;  // inside the (wide) envelope; the model keeps saying 4
  d.baseline_per_min = 20;
  int ticks = 0;
  while (d.tick() && ticks < 30) ++ticks;
  EXPECT_EQ(src.health("acme", "q").reason, Reason::kRealizedError);
  EXPECT_LE(ticks, 10);  // the last minute's residuals, 3 evaluations in a row: ~1.5 min at most
}

TEST(Source, DriftOfTheInputDistribution) {
  auto net = std::make_shared<FakeNet>();
  auto active = spec(net, 1);
  QueueNorm n;
  n.r = Range::from_expected(240);
  n.envelope = 2.0;
  n.psi_ref.assign(kPsiBins + 2, 0.0);
  n.psi_ref[kPsiBins + 1] = 1.0;  // training: always at the top of the range
  active->norm["q"] = n;
  LearnedSource src(SourceOptions{}, spec(std::make_shared<FakeNet>()));
  src.set_models("acme", active, nullptr);
  Drive d{&src};
  d.per_min = 2;  // the middle of the range: a different distribution
  d.baseline_per_min = 2;
  bool drift = false;
  for (int i = 0; i < 6 * 370 && !drift; ++i) {
    d.tick();
    drift = src.health("acme", "q").reason == Reason::kDrift;
  }
  EXPECT_TRUE(drift);
  EXPECT_EQ(src.active_version("acme"), 1u);
}

TEST(Source, AModelOlderThan14DaysIsStale) {
  auto active = spec(std::make_shared<FakeNet>(), 4);
  active->created_at_ms = T0 - 15LL * 86'400'000;
  LearnedSource src(SourceOptions{}, spec(std::make_shared<FakeNet>()));
  src.set_models("acme", active, nullptr);
  Drive d{&src};
  for (int i = 0; i < 100; ++i) EXPECT_FALSE(d.tick());
  EXPECT_EQ(src.health("acme", "q").reason, Reason::kStale);
}

struct FakeStore final : ModelStore {
  std::vector<std::pair<uint64_t, std::string>> calls;
  void set_status(const std::string& tenant, uint64_t version, const std::string& status, const nlohmann::json&,
                  int64_t) override {
    EXPECT_EQ(tenant, "acme");
    calls.emplace_back(version, status);
  }
};

SourceOptions scoring() {
  SourceOptions o;
  o.shadow_ms = 2 * 3'600'000;
  o.demote_window_ms = 2 * 3'600'000;
  return o;
}

TEST(Source, ACandidateBetterThanBothIsPromotedAfterItsShadowWindow) {
  auto good = std::make_shared<FakeNet>();
  good->mode = FakeNet::Mode::kSmooth;
  auto bad = std::make_shared<FakeNet>();
  bad->mode = FakeNet::Mode::kScale;
  bad->shift = 0.6;  // ~x2: worse than the baseline, inside the sanity window
  FakeStore store;
  LearnedSource src(scoring(), spec(bad), &store);
  src.set_models("acme", nullptr, spec(good, 2));
  Drive d{&src};
  d.poisson = true;
  d.baseline_per_min = 6;  // the baseline over-forecasts by 50 %
  for (int i = 0; i < 2 * 360 + 30; ++i) d.tick();
  ASSERT_EQ(store.calls.size(), 1u);
  EXPECT_EQ(store.calls[0], (std::pair<uint64_t, std::string>{2, "active"}));
  EXPECT_EQ(src.active_version("acme"), 2u);
  EXPECT_EQ(src.totals().promoted, 1u);
  EXPECT_GT(src.totals().shadow_runs, 0u);
  // The prediction error per source: the candidate beat the baseline and the base model.
  const PredError e = src.error("acme");
  const double c = e.m1[static_cast<size_t>(Src::kCandidate)].wape(), b = e.m1[static_cast<size_t>(Src::kBaseline)].wape();
  EXPECT_GT(e.m1[static_cast<size_t>(Src::kCandidate)].act, 50);
  EXPECT_LT(c, 0.95 * b);
  EXPECT_LT(c, 0.95 * e.m1[static_cast<size_t>(Src::kModel)].wape());
  EXPECT_GT(e.m15[static_cast<size_t>(Src::kBaseline)].act, 0);
}

TEST(Source, AWorseCandidateIsRejected) {
  auto good = std::make_shared<FakeNet>();
  good->mode = FakeNet::Mode::kSmooth;
  auto bad = std::make_shared<FakeNet>();
  bad->mode = FakeNet::Mode::kScale;
  bad->shift = 0.6;
  FakeStore store;
  LearnedSource src(scoring(), spec(good), &store);
  src.set_models("acme", nullptr, spec(bad, 3));
  Drive d{&src};
  d.poisson = true;
  d.baseline_per_min = 6;
  for (int i = 0; i < 2 * 360 + 30; ++i) d.tick();
  ASSERT_GE(store.calls.size(), 1u);
  EXPECT_EQ(store.calls[0], (std::pair<uint64_t, std::string>{3, "rejected"}));
  EXPECT_EQ(src.active_version("acme"), 0u);
  EXPECT_EQ(src.totals().rejected, 1u);
}

TEST(Source, AnActiveModelWorseThanTheBaselineIsDemotedToTheBaseThenRolledBack) {
  auto good = std::make_shared<FakeNet>();
  good->mode = FakeNet::Mode::kSmooth;
  auto bad = std::make_shared<FakeNet>();
  bad->mode = FakeNet::Mode::kScale;
  bad->shift = 0.6;
  FakeStore store;
  LearnedSource src(scoring(), spec(good), &store);
  src.set_models("acme", spec(bad, 5), nullptr);
  Drive d{&src};
  d.poisson = true;
  d.baseline_per_min = 4.5;
  for (int i = 0; i < 2 * 360 + 30; ++i) d.tick();
  ASSERT_GE(store.calls.size(), 1u);
  EXPECT_EQ(store.calls[0], (std::pair<uint64_t, std::string>{5, "demoted"}));
  EXPECT_EQ(src.active_version("acme"), 0u);  // the base model now
  EXPECT_EQ(src.totals().demoted, 1u);
  // The base drives again after its hysteresis.
  bool drives = false;
  for (int i = 0; i < 10 && !drives; ++i) drives = d.tick().has_value();
  EXPECT_TRUE(drives);
  // A rollback (trainer rollback re-activates version 4): the poller hands it over.
  src.set_models("acme", spec(good, 4), nullptr);
  d.tick();
  EXPECT_EQ(src.active_version("acme"), 4u);
  EXPECT_EQ(src.versions("acme").first, 4u);
}

TEST(Source, TenantsAreKeptApart) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  Drive d{&src};
  ASSERT_GT(d.warm(), 0);
  // Another tenant with a queue of the same name: nothing of acme's state reaches it.
  EXPECT_FALSE(src.forecast("globex", "q", d.now, baseline(4)));
  EXPECT_EQ(src.why_not("globex", "q"), "no_model");  // nothing observed: no range, no inputs
  src.set_models("globex", spec(net, 9), nullptr);
  EXPECT_EQ(src.active_version("acme"), 0u);
}

TEST(Seam, TheSanityWindowIsWidenedByTheObservedRateAndAhtIsTheBaselines) {
  opt::QueueForecast b = baseline(4);
  struct L final : opt::ForecastSource {
    opt::QueueForecast f;
    std::optional<opt::QueueForecast> forecast(const std::string&, const std::string&, int64_t, const opt::QueueForecast&) override {
      return f;
    }
  } l;
  l.f = b;
  l.f.now_per_s = 6 * b.now_per_s;  // 6x the baseline ...
  l.f.aht_s = 999;
  EXPECT_EQ(opt::choose_forecast(b, &l, "acme", T0).fallback, "ratio");
  b.observed_per_s = 5 * b.now_per_s;  // ... but the last minute already ran at 5x: sane
  const auto m = opt::choose_forecast(b, &l, "acme", T0);
  EXPECT_EQ(m.source, "model");
  EXPECT_DOUBLE_EQ(m.aht_s, 180);  // not owned: the baseline's
  l.f.owns_aht = true;
  EXPECT_DOUBLE_EQ(opt::choose_forecast(b, &l, "acme", T0).aht_s, 999);
  l.f.now_per_s = 0.1 * 4 / 60;  // a collapse to a tenth
  b.observed_per_s = 4.0 / 60;
  EXPECT_EQ(opt::choose_forecast(b, &l, "acme", T0).fallback, "ratio");
}

// The thread contract: tenants on different workers at once (TSan-clean in the sanitizer lanes).
TEST(Source, ConcurrentTenantsOnTheirOwnThreads) {
  auto net = std::make_shared<FakeNet>();
  LearnedSource src(SourceOptions{}, spec(net));
  std::vector<std::thread> th;
  for (int k = 0; k < 4; ++k) {
    th.emplace_back([&src, k] {
      const std::string tenant = "t" + std::to_string(k);
      int64_t now = T0;
      for (int i = 0; i < 200; ++i) {
        now += kTick;
        opt::ObservedWindow w;
        w.queue = "q";
        w.at_ms = now;
        w.window_ms = kTick;
        w.arrivals = 1;
        w.expected_per_hour = 240;
        src.observe(tenant, {w});
        src.forecast(tenant, "q", now, baseline(6));
      }
    });
  }
  for (int i = 0; i < 50; ++i) {
    src.set_models("t0", nullptr, nullptr);
    (void)src.totals();
  }
  for (auto& t : th) t.join();
  EXPECT_GE(src.totals().inferences, 4u * 100);
}

}  // namespace
}  // namespace pcr::fc
