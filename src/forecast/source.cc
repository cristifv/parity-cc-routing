#include "src/forecast/source.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <set>

#include "src/ops/log.h"

namespace pcr::fc {

namespace {

constexpr int kRing = 128;          // 10 s steps kept per queue (21 min: the 72-step window + slack)
constexpr int kPsiKeep = 360;       // 1-min inputs kept for the PSI (6 h)
constexpr int kMinutesKeep = 20;    // actual arrivals per minute kept for scoring
constexpr int kH1Score = 15;        // H1's first 15 minutes are scored
// The realized-error monitor's windows (10 s steps): 1 min (fast), 3 min (a collapse at low rates),
// 15 min (a persistent bias the model does not follow).
constexpr int kZWin[3] = {6, 18, 90};
constexpr int kZSteps = 90;
constexpr int64_t kFlapWindowMs = 60 * 60'000;  // fallback entries this recent lengthen the hysteresis
constexpr int kFlapMaxShift = 6;                // up to 3 << 6 = 192 evaluations (32 min)
constexpr double kZClear = 2.0;                 // a fired realized-error window re-arms below this |z|
constexpr int64_t kDayMs = 86'400'000;

int64_t floor_div(int64_t a, int64_t b) { return a >= 0 ? a / b : -((-a + b - 1) / b); }

struct Bucket {
  int64_t start_ms = 0;
  double off = 0, ab = 0, handled = 0, handle_s = 0;
};

// One pending prediction of the next 15 minutes from a minute anchor (per source), and of the
// bucket running at a bucket start.
struct Pending1 {
  int64_t minute = 0;  // absolute minute index of the anchor (predictions cover minute+0 .. +14)
  std::array<std::array<double, kH1Score>, kSrcs> f{};
  std::array<bool, kSrcs> have{};
};
struct Pending15 {
  int64_t bucket = 0;
  std::array<double, kSrcs> f{};
  std::array<bool, kSrcs> have{};
};

}  // namespace

const char* reason_name(Reason r) {
  switch (r) {
    case Reason::kNone: return "none";
    case Reason::kNoModel: return "no_model";
    case Reason::kStale: return "stale";
    case Reason::kAboveRange: return "above_range";
    case Reason::kDrift: return "drift";
    case Reason::kRatio: return "ratio";
    case Reason::kWidth: return "width";
    case Reason::kRealizedError: return "realized_error";
    case Reason::kNan: return "nan";
    case Reason::kNegative: return "negative";
    case Reason::kSlow: return "slow";
    default: return "?";
  }
}

const char* src_name(Src s) {
  switch (s) {
    case Src::kBaseline: return "baseline";
    case Src::kModel: return "model";
    case Src::kInUse: return "in_use";
    case Src::kCandidate: return "candidate";
    default: return "?";
  }
}

void PredError::add(const PredError& o) {
  for (int i = 0; i < kSrcs; ++i) {
    m1[static_cast<size_t>(i)].add(o.m1[static_cast<size_t>(i)]);
    m15[static_cast<size_t>(i)].add(o.m15[static_cast<size_t>(i)]);
  }
}

double LearnedSource::Totals::us_quantile(double q) const {
  uint64_t n = 0;
  for (uint64_t c : us_hist) n += c;
  if (n == 0) return 0;
  const double target = q * static_cast<double>(n);
  uint64_t acc = 0;
  for (size_t i = 0; i < us_hist.size(); ++i) {
    acc += us_hist[i];
    if (static_cast<double>(acc) >= target) return static_cast<double>((i + 1) * 64);
  }
  return static_cast<double>(us_max);
}

struct LearnedSource::Atomics {
  std::atomic<uint64_t> inferences{0}, inference_failed{0}, slow{0}, shadow_runs{0}, promoted{0}, rejected{0},
      demoted{0}, models_set{0}, us_sum{0}, us_max{0};
  std::array<std::atomic<uint64_t>, kReasons> entries{}, evals{};
  std::array<std::atomic<uint64_t>, 512> hist{};
};

// ------------------------------------------------------------------------------------ per tenant

struct LearnedSource::TState {
  std::string tenant;
  // Models (the worker's copy; `pending_*` under the source's mutex, swapped in by the worker).
  std::shared_ptr<const ModelSpec> active, candidate;
  std::shared_ptr<const ModelSpec> pending_active, pending_candidate;
  bool pending = false;
  bool base_demoted = false;  // the base model lost to the baseline over 24 h for this tenant
  uint64_t active_version_seen = 0, candidate_version_seen = 0;

  struct Q {
    std::string id;
    // Inputs: 10 s steps (absolute step index = floor(t / 10 s)), fractional counts.
    std::array<float, kRing> cnt{};
    std::array<int64_t, kRing> idx{};
    int64_t covered_ms = INT64_MIN;  // inputs are complete up to here
    int64_t start_ms = 0;            // the first input
    double expected_per_hour = 0;
    // Buckets (15 min): completed, oldest first (last 9 kept) and the one running.
    std::deque<Bucket> buckets;
    Bucket cur;
    bool cur_open = false;
    // PSI: the last 6 h of normalized 1-min inputs (from completed minutes).
    std::deque<float> psi;
    int64_t psi_minute = INT64_MIN;
    // Scoring: actual arrivals per completed minute (minute -> count), pending predictions.
    std::deque<std::pair<int64_t, double>> minutes;
    std::deque<Pending1> p1;
    std::deque<Pending15> p15;
    int64_t last_anchor_minute = INT64_MIN, last_anchor_bucket = INT64_MIN;
    // Normalization for the base model (history, else expected volume).
    QueueNorm derived;
    bool have_derived = false;
    std::vector<double> hist_per_min;  // 1-min arrivals from offered_10s history (for the range)
    // The realized-error monitor: one-step-ahead predictions (step -> mean, var) and their residuals.
    std::deque<std::array<double, 3>> z;  // step, residual, variance
    std::map<int64_t, std::pair<double, double>> pred;  // step -> (mean, var) for steps not yet complete
    std::array<int, 3> z_hits{};
    std::array<bool, 3> z_fired{};  // Schmitt trigger: fired until |z| < kZClear
    std::deque<int64_t> entry_ms;  // recent entries into fallback (the hysteresis backs off)
    std::set<int64_t> hist_days;   // days with 10 s history
    // Health.
    bool driving = false;
    int streak = 0;
    Reason reason = Reason::kNone;
    Reason held = Reason::kStale;  // what started the current fallback (stale: warming up)
    uint64_t evaluations = 0, entries = 0;
    std::array<uint64_t, kReasons> fired{};
    std::array<int64_t, kReasons> first_ms{};
    int64_t last_change_ms = 0;
    double last_input_max = 0;
    Q() { idx.fill(INT64_MIN); }
  };
  std::map<std::string, Q> q;

  // This tick's inference (all queues at once), cached for the per-queue forecast() calls.
  int64_t run_at_ms = INT64_MIN;
  struct Out {
    bool ok = false;
    Reason pre = Reason::kNone;  // a reason found before or by the run (stale, no_model, nan, ...)
    Denorm d, cd;                // the model's and the candidate's (cd valid when has_cand)
    bool has_cand = false;
    double max_in = 0;           // the largest normalized 1-min input
    double psi = 0;
    bool psi_ok = false;
    bool negative = false;
  };
  std::map<std::string, Out> out;
  double run_ms = 0;

  // Scoring windows: the candidate's shadow, the active model's demotion window. `err` is read by
  // totals() from any thread: under err_mu.
  mutable std::mutex err_mu;
  PredError err;
  PredError shadow, demote;
  int64_t shadow_from_ms = 0, demote_from_ms = 0;
  uint64_t shadow_version = 0;
};

LearnedSource::LearnedSource(SourceOptions o, std::shared_ptr<const ModelSpec> base, ModelStore* store)
    : o_(o), base_(std::move(base)), store_(store), a_(std::make_unique<Atomics>()) {}

LearnedSource::~LearnedSource() = default;

LearnedSource::TState* LearnedSource::state(const std::string& tenant) {
  std::lock_guard lk(mu_);
  auto& p = tenants_[tenant];
  if (!p) {
    p = std::make_unique<TState>();
    p->tenant = tenant;
  }
  if (p->pending) {
    p->active = std::move(p->pending_active);
    p->candidate = std::move(p->pending_candidate);
    p->pending = false;
    p->base_demoted = false;
  }
  return p.get();
}

const LearnedSource::TState* LearnedSource::find(const std::string& tenant) const {
  std::lock_guard lk(mu_);
  auto it = tenants_.find(tenant);
  return it == tenants_.end() ? nullptr : it->second.get();
}

void LearnedSource::set_models(const std::string& tenant, std::shared_ptr<const ModelSpec> active,
                               std::shared_ptr<const ModelSpec> candidate) {
  std::lock_guard lk(mu_);
  auto& p = tenants_[tenant];
  if (!p) {
    p = std::make_unique<TState>();
    p->tenant = tenant;
  }
  p->active_version_seen = active ? active->version : 0;
  p->candidate_version_seen = candidate ? candidate->version : 0;
  p->pending_active = std::move(active);
  p->pending_candidate = std::move(candidate);
  p->pending = true;
  a_->models_set++;
}

std::pair<uint64_t, uint64_t> LearnedSource::versions(const std::string& tenant) const {
  std::lock_guard lk(mu_);
  auto it = tenants_.find(tenant);
  if (it == tenants_.end()) return {0, 0};
  return {it->second->active_version_seen, it->second->candidate_version_seen};
}

// ------------------------------------------------------------------------------------- inputs

namespace {

void put_step(std::array<float, kRing>& cnt, std::array<int64_t, kRing>& idx, int64_t step, double add) {
  const size_t k = static_cast<size_t>(((step % kRing) + kRing) % kRing);
  if (idx[k] != step) {
    idx[k] = step;
    cnt[k] = 0;
  }
  cnt[k] += static_cast<float>(add);
}

bool get_step(const std::array<float, kRing>& cnt, const std::array<int64_t, kRing>& idx, int64_t step, double* v) {
  const size_t k = static_cast<size_t>(((step % kRing) + kRing) % kRing);
  if (idx[k] != step) return false;
  *v = cnt[k];
  return true;
}

}  // namespace

void LearnedSource::observe(const std::string& tenant, const std::vector<opt::ObservedWindow>& ws) {
  TState* t = state(tenant);
  for (const auto& w : ws) {
    TState::Q& q = t->q[w.queue];
    q.id = w.queue;
    if (w.expected_per_hour > 0) q.expected_per_hour = w.expected_per_hour;
    if (w.window_ms <= 0 || !std::isfinite(w.arrivals) || w.arrivals < 0) continue;
    const int64_t from = w.at_ms - w.window_ms, to = w.at_ms;
    if (q.covered_ms != INT64_MIN && from < q.covered_ms - 1000) {
      // Time went backwards (a clock jump back, a replayed window): start the inputs over.
      q.idx.fill(INT64_MIN);
      q.covered_ms = INT64_MIN;
      q.pred.clear();
      q.z.clear();
      q.z_hits = {};
      q.z_fired = {};
      q.minutes.clear();
      q.p1.clear();
      q.p15.clear();
      q.cur_open = false;
    }
    if (q.covered_ms == INT64_MIN) q.start_ms = from;
    // Spread the window's arrivals over the 10 s steps it overlaps (proportionally).
    const double len = static_cast<double>(to - from);
    for (int64_t s = floor_div(from, kStepMs); s * kStepMs < to; ++s) {
      const int64_t a = std::max(from, s * kStepMs), b = std::min(to, (s + 1) * kStepMs);
      if (b <= a) continue;
      put_step(q.cnt, q.idx, s, w.arrivals * static_cast<double>(b - a) / len);
    }
    const int64_t prev_cov = q.covered_ms;
    q.covered_ms = to;
    // Buckets: the window's counts go to the bucket of its end (windows are <= 10 s: good enough).
    const int64_t bstart = floor_div(to - 1, kBucketMs) * kBucketMs;
    if (!q.cur_open || q.cur.start_ms != bstart) {
      if (q.cur_open && q.cur.start_ms < bstart) {
        q.buckets.push_back(q.cur);
        while (q.buckets.size() > 9) q.buckets.pop_front();
      }
      q.cur = Bucket{bstart, 0, 0, 0, 0};
      q.cur_open = true;
    }
    q.cur.off += w.arrivals;
    q.cur.ab += std::max(0.0, w.abandons);
    q.cur.handled += std::max(0.0, w.handled);
    q.cur.handle_s += std::max(0.0, w.handle_s);
    // Completed steps: the realized-error residuals against the one-step-ahead predictions.
    const int64_t s0 = prev_cov == INT64_MIN ? floor_div(from, kStepMs) : floor_div(prev_cov, kStepMs);
    for (int64_t s = s0; (s + 1) * kStepMs <= to; ++s) {
      auto it = q.pred.find(s);
      double v = 0;
      if (it != q.pred.end() && get_step(q.cnt, q.idx, s, &v)) {
        q.z.push_back({it->second.first, v - it->second.first, it->second.second});  // mean, residual, variance
        while (q.z.size() > static_cast<size_t>(kZSteps)) q.z.pop_front();
      }
    }
    while (!q.pred.empty() && (q.pred.begin()->first + 1) * kStepMs <= to) q.pred.erase(q.pred.begin());
    // Completed minutes: actuals for scoring, normalized inputs for the PSI.
    const int64_t m0 = prev_cov == INT64_MIN ? floor_div(from, kMinuteMs) : floor_div(prev_cov, kMinuteMs);
    for (int64_t m = m0; (m + 1) * kMinuteMs <= to; ++m) {
      double sum = 0;
      bool ok = true;
      for (int k = 0; k < kStepsPerMin && ok; ++k) {
        double v = 0;
        ok = get_step(q.cnt, q.idx, m * kStepsPerMin + k, &v);
        sum += v;
      }
      if (!ok || m <= q.psi_minute) continue;
      q.psi_minute = m;
      q.minutes.emplace_back(m, sum);
      while (q.minutes.size() > static_cast<size_t>(kMinutesKeep)) q.minutes.pop_front();
      const QueueNorm* n = nullptr;
      if (t->active) {
        auto nit = t->active->norm.find(q.id);
        if (nit != t->active->norm.end()) n = &nit->second;
      }
      if (!n && q.have_derived) n = &q.derived;
      if (n) {
        q.psi.push_back(static_cast<float>(n->r.norm(sum)));
        while (q.psi.size() > static_cast<size_t>(kPsiKeep)) q.psi.pop_front();
      }
    }
    // Score the predictions whose 15 minutes are complete.
    auto actual_of = [&](int64_t m, double* v) {
      for (const auto& [mm, c] : q.minutes) {
        if (mm == m) {
          *v = c;
          return true;
        }
      }
      return false;
    };
    while (!q.p1.empty() && (q.p1.front().minute + kH1Score) * kMinuteMs <= to) {
      const Pending1 p = q.p1.front();
      q.p1.pop_front();
      std::array<double, kH1Score> act{};
      bool ok = true;
      for (int k = 0; k < kH1Score && ok; ++k) ok = actual_of(p.minute + k, &act[static_cast<size_t>(k)]);
      if (!ok) continue;
      for (int s = 0; s < kSrcs; ++s) {
        if (!p.have[static_cast<size_t>(s)]) continue;
        WapeSum w;
        for (int k = 0; k < kH1Score; ++k) w.add(p.f[static_cast<size_t>(s)][static_cast<size_t>(k)], act[static_cast<size_t>(k)]);
        {
          std::lock_guard lk(t->err_mu);
          t->err.m1[static_cast<size_t>(s)].add(w);
        }
        t->shadow.m1[static_cast<size_t>(s)].add(w);
        t->demote.m1[static_cast<size_t>(s)].add(w);
      }
    }
    while (!q.p15.empty() && !q.buckets.empty() && q.buckets.back().start_ms >= q.p15.front().bucket * kBucketMs) {
      const Pending15 p = q.p15.front();
      q.p15.pop_front();
      for (const auto& b : q.buckets) {
        if (b.start_ms != p.bucket * kBucketMs) continue;
        for (int s = 0; s < kSrcs; ++s) {
          if (!p.have[static_cast<size_t>(s)]) continue;
          std::lock_guard lk(t->err_mu);
          t->err.m15[static_cast<size_t>(s)].add(p.f[static_cast<size_t>(s)], b.off);
        }
      }
    }
  }
}

void LearnedSource::history(const std::string& tenant, const std::vector<opt::HistoryRow>& rows) {
  TState* t = state(tenant);
  std::map<std::string, std::vector<const opt::HistoryRow*>> by_q;
  for (const auto& r : rows) by_q[r.queue].push_back(&r);
  for (auto& [qid, rs] : by_q) {
    TState::Q& q = t->q[qid];
    q.id = qid;
    std::sort(rs.begin(), rs.end(), [](auto* a, auto* b) { return a->bucket_start_ms < b->bucket_start_ms; });
    for (const auto* r : rs) {
      if (r->offered_10s.size() == static_cast<size_t>(kStepsPerBucket)) {
        q.hist_days.insert(floor_div(r->bucket_start_ms, kDayMs));
        for (int m = 0; m < kStepsPerBucket / kStepsPerMin; ++m) {
          double s = 0;
          for (int k = 0; k < kStepsPerMin; ++k) s += r->offered_10s[static_cast<size_t>(m * kStepsPerMin + k)];
          q.hist_per_min.push_back(s);
        }
      }
      // The most recent buckets seed the context tokens (live observations take over).
      if (!q.cur_open || r->bucket_start_ms < q.cur.start_ms) {
        q.buckets.push_back(Bucket{r->bucket_start_ms, r->offered, r->abandoned, r->handled, r->handle_ms / 1000.0});
        while (q.buckets.size() > 9) q.buckets.pop_front();
      }
    }
    // Three days with 1-min history: the queue's own fixed range (else expected volume, at first use).
    if (q.hist_days.size() >= 3 && q.hist_per_min.size() >= 3 * 60) {
      q.derived.r = Range::from_history(q.hist_per_min);
      q.derived.expected = false;
      q.derived.envelope = base_ ? base_->envelope : 2.0;
      std::vector<double> z;
      for (double v : q.hist_per_min) z.push_back(q.derived.r.norm(v));
      q.derived.psi_ref = psi_hist(z);
      q.have_derived = true;
    }
    q.hist_per_min.clear();
    q.hist_per_min.shrink_to_fit();
  }
}

// ------------------------------------------------------------------------------------ forecast

std::optional<opt::QueueForecast> LearnedSource::forecast(const std::string& tenant, const std::string& queue,
                                                          int64_t now_ms, const opt::QueueForecast& baseline) {
  TState* t = state(tenant);
  auto qit = t->q.find(queue);
  if (qit == t->q.end()) {
    TState::Q& q = t->q[queue];
    q.id = queue;
    qit = t->q.find(queue);
  }
  TState::Q& q = qit->second;
  // The model this tenant uses: its active fine-tuned one, else the shared base (unless demoted).
  const ModelSpec* m = t->active ? t->active.get() : (t->base_demoted ? nullptr : base_.get());

  // ---- one run per tenant per tick: every queue's inputs, the batch, the candidate beside it.
  if (t->run_at_ms != now_ms) {
    t->run_at_ms = now_ms;
    t->out.clear();
    std::vector<std::string> ids;
    std::vector<NetInput> in;
    std::vector<const QueueNorm*> norms;
    std::vector<Recent> recents;
    for (auto& [id, x] : t->q) {
      TState::Out o;
      const QueueNorm* n = nullptr;
      if (m && m->net) {
        auto nit = m->norm.find(id);
        if (nit != m->norm.end()) n = &nit->second;
        if (!n && x.have_derived) n = &x.derived;
        if (!n && x.expected_per_hour > 0) {
          x.derived.r = Range::from_expected(x.expected_per_hour);
          x.derived.expected = true;
          x.derived.envelope = m->envelope;
          x.derived.psi_ref.clear();
          x.have_derived = true;
          n = &x.derived;
        }
      }
      if (!n) {
        o.pre = Reason::kNoModel;
        t->out[id] = o;
        continue;
      }
      // Stale inputs: nothing for 30 s, the input clock ahead of ours, > 10 % of the window missing.
      if (x.covered_ms == INT64_MIN || now_ms - x.covered_ms > o_.stale_input_ms || x.covered_ms > now_ms + 1000) {
        o.pre = Reason::kStale;
        t->out[id] = o;
        continue;
      }
      if (m->version > 0 && m->created_at_ms > 0 && now_ms - m->created_at_ms > o_.max_model_age_ms) {
        o.pre = Reason::kStale;
        t->out[id] = o;
        continue;
      }
      // The 72-step window ending now: the running step counts (scaled to 10 s) once it is 1 s old.
      const int64_t cur = floor_div(std::min(now_ms, x.covered_ms), kStepMs);
      const int64_t elapsed = std::min(now_ms, x.covered_ms) - cur * kStepMs;
      const bool partial = static_cast<double>(elapsed) >= o_.partial_min_ms;
      const int64_t last = partial ? cur : cur - 1;
      Recent r;
      r.t_ms = (last + 1) * kStepMs;
      r.c10.resize(static_cast<size_t>(kH1Inputs));
      int missing = 0;
      for (int i = 0; i < kH1Inputs; ++i) {
        const int64_t s = last - kH1Inputs + 1 + i;
        double v = 0;
        if (!get_step(x.cnt, x.idx, s, &v) || s * kStepMs < x.start_ms) {
          ++missing;
          v = std::nan("");
        } else if (s == cur && partial) {
          v *= static_cast<double>(kStepMs) / static_cast<double>(elapsed);
        }
        r.c10[static_cast<size_t>(i)] = v;
      }
      if (missing > static_cast<int>(o_.max_missing * kH1Inputs)) {
        o.pre = Reason::kStale;
        t->out[id] = o;
        continue;
      }
      double sum = 0;
      int have = 0;
      for (double v : r.c10) {
        if (std::isfinite(v)) {
          sum += v;
          ++have;
        }
      }
      for (double& v : r.c10) {
        if (!std::isfinite(v)) v = have ? sum / have : 0;
      }
      // The largest normalized 1-min input (token sums) and the PSI of the last 6 h.
      for (int k = 0; k < kH1Tokens; ++k) {
        double s = 0;
        for (int j = 0; j < kH1Patch; ++j) s += r.c10[static_cast<size_t>(k * kH1Patch + j)];
        o.max_in = std::max(o.max_in, n->r.norm(s));
      }
      if (!n->expected && !n->psi_ref.empty() && static_cast<int>(x.psi.size()) >= o_.psi_min_minutes) {
        std::vector<double> z(x.psi.begin(), x.psi.end());
        o.psi = psi(n->psi_ref, psi_hist(z));
        o.psi_ok = true;
      }
      // Context: the last 8 completed buckets (missing: the previous one, as the trainer fills them).
      const int64_t cb = floor_div(r.t_ms, kBucketMs);
      for (int b = 0; b < kCtxTokens; ++b) {
        const int64_t start = (cb - kCtxTokens + b) * kBucketMs;
        const Bucket* hit = nullptr;
        for (const auto& bk : x.buckets) {
          if (bk.start_ms == start) hit = &bk;
        }
        if (hit) {
          r.b_arr.push_back(hit->off);
          r.b_ab.push_back(hit->ab);
          r.b_aht.push_back(hit->handled > 0 ? hit->handle_s / hit->handled : 0);
        } else {
          r.b_arr.push_back(r.b_arr.empty() ? sum / std::max(1, have) * kStepsPerBucket : r.b_arr.back());
          r.b_ab.push_back(r.b_ab.empty() ? 0 : r.b_ab.back());
          r.b_aht.push_back(0);
        }
      }
      ids.push_back(id);
      in.push_back(build_input(r, *n));
      norms.push_back(n);
      recents.push_back(std::move(r));
      t->out[id] = o;
    }
    if (!in.empty()) {
      std::vector<NetOutput> res, cres;
      const auto t0 = std::chrono::steady_clock::now();
      const bool ok = m->net->run(in, &res) && res.size() == in.size();
      const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
      t->run_ms = ms;
      const uint64_t us = static_cast<uint64_t>(ms * 1000);
      a_->inferences++;
      a_->us_sum += us;
      uint64_t prev = a_->us_max.load();
      while (us > prev && !a_->us_max.compare_exchange_weak(prev, us)) {
      }
      a_->hist[std::min<size_t>(511, us / 64)]++;
      if (!ok) a_->inference_failed++;
      if (ms > o_.slow_ms) a_->slow++;
      // The candidate in the shadow (never used): its own norms where it has them.
      bool cand_ok = false;
      std::vector<const QueueNorm*> cnorms = norms;
      if (t->candidate && t->candidate->net) {
        std::vector<NetInput> cin = in;
        for (size_t i = 0; i < ids.size(); ++i) {
          auto nit = t->candidate->norm.find(ids[i]);
          if (nit != t->candidate->norm.end()) {
            cnorms[i] = &nit->second;
            cin[i] = build_input(recents[i], nit->second);
          }
        }
        cand_ok = t->candidate->net->run(cin, &cres) && cres.size() == cin.size();
        a_->shadow_runs++;
      }
      for (size_t i = 0; i < ids.size(); ++i) {
        TState::Out& o = t->out[ids[i]];
        if (!ok) {
          o.pre = Reason::kNan;
          continue;
        }
        const NetOutput& no = res[i];
        bool finite = true, negative = false;
        for (const auto* v : {&no.h1, &no.h2a, &no.h2h}) {
          for (float x : *v) finite = finite && std::isfinite(x);
        }
        const Range& rr = norms[i]->r;
        for (int k = 0; k < kH1Steps; ++k) {
          const float z = no.h1[static_cast<size_t>(k * kQuantiles + 1)];
          if (rr.lo + z * rr.span() < -0.5) negative = true;
        }
        if (!finite) {
          o.pre = Reason::kNan;
          continue;
        }
        o.negative = negative;
        o.d = denormalize(no, *norms[i]);
        o.ok = true;
        if (cand_ok) {
          o.cd = denormalize(cres[i], *cnorms[i]);
          o.has_cand = true;
        }
      }
    }
  }

  // ---- this queue's evaluation.
  TState::Out& o = t->out[queue];
  opt::QueueForecast f = baseline;
  Reason why = o.pre;
  if (why == Reason::kNone && !o.ok) why = Reason::kNoModel;
  if (why == Reason::kNone && t->run_ms > o_.slow_ms) why = Reason::kSlow;
  if (why == Reason::kNone && o.negative) why = Reason::kNegative;
  if (o.ok) {
    // The model's forecast in the baseline's shape: the CTMC's rate now is H1's next minute (p50);
    // the buckets are H2's (p50, p10, p90); handle time only when owned.
    f.now_per_s = o.d.h1[1][0] / 60.0;
    f.per_s.assign(baseline.per_s.size(), 0);
    f.p10_per_s.assign(baseline.per_s.size(), 0);
    f.p90_per_s.assign(baseline.per_s.size(), 0);
    for (size_t b = 0; b < baseline.per_s.size(); ++b) {
      const size_t k = std::min<size_t>(b, kH2Buckets - 1);
      f.per_s[b] = o.d.h2[1][k];
      f.p10_per_s[b] = o.d.h2[0][k];
      f.p90_per_s[b] = o.d.h2[2][k];
    }
    if (o_.owns_aht && o.d.aht[1][0] > 0) f.aht_s = o.d.aht[1][0];
    f.owns_aht = o_.owns_aht;
    f.model_version = m ? m->version : 0;
    f.source = "model";
    // The one-step-ahead prediction for the next 10 s step (the realized-error monitor).
    const int64_t next = floor_div(now_ms, kStepMs) + (now_ms % kStepMs == 0 ? 0 : 1);
    const double mean = std::max(0.0, o.d.h1[1][0] / kStepsPerMin);
    const double band = std::max(0.0, o.d.h1[2][0] - o.d.h1[0][0]) / 2.563 / kStepsPerMin;
    q.pred[next] = {mean, mean + band * band + 0.05};
    // The later checks, in order.
    const QueueNorm* n = nullptr;
    if (m) {
      auto nit = m->norm.find(queue);
      n = nit != m->norm.end() ? &nit->second : (q.have_derived ? &q.derived : nullptr);
    }
    q.last_input_max = o.max_in;
    if (why == Reason::kNone && n && o.max_in > n->envelope) why = Reason::kAboveRange;
    if (why == Reason::kNone && o.psi_ok && o.psi > o_.psi_max) why = Reason::kDrift;
    opt::Sanity sn;
    sn.lo = o_.ratio_lo;
    sn.hi = o_.ratio_hi;
    if (why == Reason::kNone && !opt::within_sanity(f, baseline, sn)) why = Reason::kRatio;
    if (why == Reason::kNone) {
      double w = 0, p50 = 0;
      for (int k = 0; k < 5; ++k) {
        w += o.d.h1[2][static_cast<size_t>(k)] - o.d.h1[0][static_cast<size_t>(k)];
        p50 += o.d.h1[1][static_cast<size_t>(k)];
      }
      if (p50 / 5 >= 0.5 && w / std::max(1e-9, p50) > o_.max_width) why = Reason::kWidth;
    }
  }
  // The realized-error monitor (independent of what drives): the residuals of the model's one-step-
  // ahead predictions over 1, 3 and 15 minutes, on the variance-stabilized (Anscombe) scale, so a
  // collapse at a low rate counts as much as a surge: z = 2 (sqrt(A + 3/8) - sqrt(F + 3/8)) with A the
  // actual and F the predicted arrivals (plus the band's variance in F). Each window needs
  // z_persist evaluations in a row beyond z_max.
  for (int w = 0; w < 3; ++w) {
    const size_t n = static_cast<size_t>(kZWin[w]);
    if (q.z.size() < n) continue;
    double a = 0, f = 0, band = 0;
    for (size_t i = q.z.size() - n; i < q.z.size(); ++i) {
      const auto& z = q.z[i];  // mean, residual (actual - mean), variance (mean + band^2 + 0.05)
      a += std::max(0.0, z[0] + z[1]);
      f += z[0];
      band += std::max(0.0, z[2] - z[0]);
    }
    const double zz = 2 * (std::sqrt(a + 0.375) - std::sqrt(f + 0.375)) / std::sqrt(1 + band / std::max(1.0, f));
    const size_t wi = static_cast<size_t>(w);
    q.z_hits[wi] = std::abs(zz) > o_.z_max ? q.z_hits[wi] + 1 : 0;
    if (q.z_hits[wi] >= o_.z_persist) q.z_fired[wi] = true;
    if (q.z_fired[wi] && std::abs(zz) < kZClear) q.z_fired[wi] = false;
    if (why == Reason::kNone && q.z_fired[wi]) why = Reason::kRealizedError;
  }

  // ---- health, hysteresis, counts and logs.
  ++q.evaluations;
  q.reason = why;
  if (why != Reason::kNone) {
    const size_t ri = static_cast<size_t>(why);
    ++q.fired[ri];
    if (q.first_ms[ri] == 0) q.first_ms[ri] = now_ms;
    a_->evals[ri]++;
    q.streak = 0;
    if (q.driving) {
      q.held = why;
      q.driving = false;
      ++q.entries;
      q.entry_ms.push_back(now_ms);
      q.last_change_ms = now_ms;
      a_->entries[ri]++;
      LogRec("warn", "forecast.fallback", "the learned forecast fell back to the baseline")
          .tenant(tenant).str("queue_id", queue).attr("reason", reason_name(why));
      LogRec("info", "forecast.source_changed", "forecast source changed").tenant(tenant).str("queue_id", queue)
          .transition("model", baseline.source);
    }
  } else {
    ++q.streak;
    // Anti-flapping: every recent entry (60 min) doubles the healthy evaluations needed (3, 6, 12, ...
    // up to 192: 32 min).
    while (!q.entry_ms.empty() && now_ms - q.entry_ms.front() > kFlapWindowMs) q.entry_ms.pop_front();
    const int recent = static_cast<int>(q.entry_ms.size());
    const int need = o_.hysteresis << std::min(kFlapMaxShift, std::max(0, recent - 1));
    if (!q.driving && q.streak >= need) {
      q.driving = true;
      q.last_change_ms = now_ms;
      LogRec("info", "forecast.source_changed", "forecast source changed").tenant(tenant).str("queue_id", queue)
          .transition(baseline.source, "model");
    }
  }

  // ---- scoring: a minute anchor (H1's 15 min) and a bucket anchor (the running bucket).
  const int64_t minute = floor_div(now_ms, kMinuteMs) + 1;  // the first complete minute ahead
  if (minute != q.last_anchor_minute) {
    q.last_anchor_minute = minute;
    Pending1 p;
    p.minute = minute;
    for (int k = 0; k < kH1Score; ++k) p.f[static_cast<size_t>(Src::kBaseline)][static_cast<size_t>(k)] = baseline.now_per_s * 60;
    p.have[static_cast<size_t>(Src::kBaseline)] = true;
    if (o.ok) {
      for (int k = 0; k < kH1Score; ++k) p.f[static_cast<size_t>(Src::kModel)][static_cast<size_t>(k)] = o.d.h1[1][static_cast<size_t>(k + 1)];
      p.have[static_cast<size_t>(Src::kModel)] = true;
    }
    if (o.has_cand) {
      for (int k = 0; k < kH1Score; ++k) p.f[static_cast<size_t>(Src::kCandidate)][static_cast<size_t>(k)] = o.cd.h1[1][static_cast<size_t>(k + 1)];
      p.have[static_cast<size_t>(Src::kCandidate)] = true;
    }
    const bool use_model = q.driving && why == Reason::kNone && o.ok;
    p.f[static_cast<size_t>(Src::kInUse)] = use_model ? p.f[static_cast<size_t>(Src::kModel)] : p.f[static_cast<size_t>(Src::kBaseline)];
    p.have[static_cast<size_t>(Src::kInUse)] = true;
    q.p1.push_back(p);
    while (q.p1.size() > kH1Score + 2) q.p1.pop_front();
  }
  const int64_t bucket = floor_div(now_ms, kBucketMs);
  if (bucket != q.last_anchor_bucket && now_ms - bucket * kBucketMs < kMinuteMs) {
    q.last_anchor_bucket = bucket;
    Pending15 p;
    p.bucket = bucket;
    const double b0 = baseline.per_s.empty() ? baseline.now_per_s : baseline.per_s[0];
    p.f[static_cast<size_t>(Src::kBaseline)] = b0 * 900;
    p.have[static_cast<size_t>(Src::kBaseline)] = true;
    if (o.ok) {
      p.f[static_cast<size_t>(Src::kModel)] = o.d.h2[1][0] * 900;
      p.have[static_cast<size_t>(Src::kModel)] = true;
    }
    if (o.has_cand) {
      p.f[static_cast<size_t>(Src::kCandidate)] = o.cd.h2[1][0] * 900;
      p.have[static_cast<size_t>(Src::kCandidate)] = true;
    }
    const bool use_model = q.driving && why == Reason::kNone && o.ok;
    p.f[static_cast<size_t>(Src::kInUse)] = use_model ? p.f[static_cast<size_t>(Src::kModel)] : p.f[static_cast<size_t>(Src::kBaseline)];
    p.have[static_cast<size_t>(Src::kInUse)] = true;
    q.p15.push_back(p);
    while (q.p15.size() > 4) q.p15.pop_front();
  }

  // ---- promotion / rejection of the candidate, demotion of the model in use (once per tick).
  if (queue == t->q.begin()->first) {
    if (t->candidate) {
      if (t->shadow_version != t->candidate->version) {
        t->shadow_version = t->candidate->version;
        t->shadow = PredError{};
        t->shadow_from_ms = now_ms;
      }
      if (now_ms - t->shadow_from_ms >= o_.shadow_ms) {
        const double c = t->shadow.m1[static_cast<size_t>(Src::kCandidate)].wape();
        const double b = t->shadow.m1[static_cast<size_t>(Src::kBaseline)].wape();
        const double a = t->shadow.m1[static_cast<size_t>(Src::kModel)].wape();
        const bool enough = t->shadow.m1[static_cast<size_t>(Src::kCandidate)].act >= o_.min_actual;
        const bool any_model = t->shadow.m1[static_cast<size_t>(Src::kModel)].act > 0;
        const double best = any_model ? std::min(a, b) : b;
        const nlohmann::json sh = {{"candidate_wape", c}, {"baseline_wape", b}, {"active_wape", any_model ? a : -1.0},
                                   {"actual", t->shadow.m1[static_cast<size_t>(Src::kCandidate)].act},
                                   {"from_ms", t->shadow_from_ms}, {"to_ms", now_ms}};
        if (enough && c <= (1 - o_.promote_margin) * best) {
          if (store_ && t->active) store_->set_status(tenant, t->active->version, "retired", nlohmann::json::object(), now_ms);
          if (store_) store_->set_status(tenant, t->candidate->version, "active", sh, now_ms);
          LogRec("info", "forecast.model_promoted", "candidate model promoted").tenant(tenant)
              .attr("version", t->candidate->version).attr("candidate_wape", c).attr("baseline_wape", b);
          t->active = t->candidate;
          a_->promoted++;
        } else {
          if (store_) store_->set_status(tenant, t->candidate->version, "rejected", sh, now_ms);
          LogRec("info", "forecast.model_rejected", "candidate model rejected").tenant(tenant)
              .attr("version", t->candidate->version).attr("candidate_wape", c).attr("baseline_wape", b);
          a_->rejected++;
        }
        t->candidate.reset();
        t->demote = PredError{};
        t->demote_from_ms = now_ms;
      }
    }
    if (t->demote_from_ms == 0) t->demote_from_ms = now_ms;
    if (now_ms - t->demote_from_ms >= o_.demote_window_ms) {
      const WapeSum& mw = t->demote.m1[static_cast<size_t>(Src::kModel)];
      const WapeSum& bw = t->demote.m1[static_cast<size_t>(Src::kBaseline)];
      if (m && mw.act >= o_.min_actual && mw.wape() > bw.wape()) {
        const uint64_t v = m->version;
        if (t->active) {
          if (store_) {
            store_->set_status(tenant, v, "demoted", {{"model_wape", mw.wape()}, {"baseline_wape", bw.wape()}}, now_ms);
          }
          t->active.reset();
        } else {
          t->base_demoted = true;
        }
        LogRec("warn", "forecast.model_demoted", "the model was worse than the baseline over 24 h").tenant(tenant)
            .attr("version", v).attr("model_wape", mw.wape()).attr("baseline_wape", bw.wape());
        a_->demoted++;
      }
      t->demote = PredError{};
      t->demote_from_ms = now_ms;
    }
  }

  if (why != Reason::kNone || !q.driving) return std::nullopt;
  return f;
}

std::string LearnedSource::why_not(const std::string& tenant, const std::string& queue) const {
  const TState* t = find(tenant);
  if (!t) return "no_model";
  auto it = t->q.find(queue);
  if (it == t->q.end()) return "no_model";
  // Healthy but inside the hysteresis: the reason that started the fallback (stale while warming up).
  if (it->second.reason == Reason::kNone) return reason_name(it->second.held);
  return reason_name(it->second.reason);
}

LearnedSource::QueueHealth LearnedSource::health(const std::string& tenant, const std::string& queue) const {
  QueueHealth h;
  const TState* t = find(tenant);
  if (!t) return h;
  auto it = t->q.find(queue);
  if (it == t->q.end()) return h;
  const auto& q = it->second;
  h.known = true;
  h.driving = q.driving;
  h.reason = q.reason;
  h.streak = q.streak;
  h.evaluations = q.evaluations;
  h.entries = q.entries;
  h.fired = q.fired;
  h.first_ms = q.first_ms;
  h.last_change_ms = q.last_change_ms;
  h.last_input_max = q.last_input_max;
  return h;
}

PredError LearnedSource::error(const std::string& tenant) const {
  const TState* t = find(tenant);
  if (!t) return PredError{};
  std::lock_guard lk(t->err_mu);
  return t->err;
}

uint64_t LearnedSource::active_version(const std::string& tenant) const {
  const TState* t = find(tenant);
  return t && t->active ? t->active->version : 0;
}

LearnedSource::Totals LearnedSource::totals() const {
  Totals t;
  t.inferences = a_->inferences;
  t.inference_failed = a_->inference_failed;
  t.slow = a_->slow;
  t.shadow_runs = a_->shadow_runs;
  t.promoted = a_->promoted;
  t.rejected = a_->rejected;
  t.demoted = a_->demoted;
  t.models_set = a_->models_set;
  t.us_sum = a_->us_sum;
  t.us_max = a_->us_max;
  for (int i = 0; i < kReasons; ++i) {
    t.fallback_entries[static_cast<size_t>(i)] = a_->entries[static_cast<size_t>(i)];
    t.fallback_evaluations[static_cast<size_t>(i)] = a_->evals[static_cast<size_t>(i)];
  }
  for (size_t i = 0; i < t.us_hist.size(); ++i) t.us_hist[i] = a_->hist[i];
  std::lock_guard lk(mu_);
  for (const auto& [id, s] : tenants_) {
    std::lock_guard lk2(s->err_mu);
    t.err.add(s->err);
  }
  return t;
}

}  // namespace pcr::fc
