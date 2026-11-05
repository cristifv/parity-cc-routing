#include "src/opt/optimizer.h"

#include <pthread.h>
#include <chrono>
#include <cmath>
#include <functional>
#include <optional>

#include "model/agents.h"
#include "src/opt/events.h"

namespace pcr {

namespace {
constexpr double kTauS = 900;               // the estimators' time constant (15 min)
constexpr double kPriorPatienceS = 120;     // theta's prior until abandons are observed
constexpr double kPriorWeightS = 120;       // ... worth this much waited time
}  // namespace

Optimizer::Optimizer(Options o) : o_(o) {
  const int n = std::max(1, o_.threads);
  for (int i = 0; i < n; ++i) workers_.push_back(std::make_unique<Worker>());
  for (size_t i = 0; i < workers_.size(); ++i) {
    Worker* raw = workers_[i].get();
    workers_[i]->th = std::thread([this, raw, i] {
      pthread_setname_np(pthread_self(), ("pcr-opt-" + std::to_string(i)).c_str());  // phase 7: CPU per thread
      run(raw);
    });
  }
}

Optimizer::~Optimizer() {
  stop_ = true;
  for (auto& w : workers_) {
    {
      std::lock_guard lk(w->mu);
    }
    w->cv.notify_all();
  }
  for (auto& w : workers_) {
    if (w->th.joinable()) w->th.join();
  }
}

void Optimizer::submit(std::shared_ptr<const ModelSnapshot> s) {
  if (!s) return;
  ++snapshots_;
  Worker& w = *workers_[std::hash<std::string>{}(s->tenant) % workers_.size()];
  {
    std::lock_guard lk(w.mu);
    if (w.q.size() >= o_.queue_per_thread) {
      ++dropped_;  // the optimizer is behind: this tenant keeps its last plan (or phase 1's estimate)
      return;
    }
    w.q.push_back(std::move(s));
  }
  w.cv.notify_one();
}

Optimizer::Totals Optimizer::totals() const {
  Totals t;
  t.snapshots = snapshots_.load();
  t.plans = plans_.load();
  t.dropped = dropped_.load();
  t.refused = refused_.load();
  t.clamps = clamps_.load();
  t.approx = approx_.load();
  t.solve_us_sum = solve_us_sum_.load();
  t.solve_us_max = solve_us_max_.load();
  t.lever_plans = lever_plans_.load();
  t.lever_refused = lever_refused_.load();
  t.lever_changes = lever_changes_.load();
  t.held = held_.load();
  t.capped = capped_.load();
  t.events = events_out_.load();
  t.lever_us_sum = lever_us_sum_.load();
  t.lever_us_max = lever_us_max_.load();
  t.learned_used = learned_used_.load();
  t.learned_fallback = learned_fallback_.load();
  t.guards = guards_.load();
  return t;
}

std::vector<opt::HistoryRow> Optimizer::history_rows(const std::vector<BucketRow>& rows) {
  std::vector<opt::HistoryRow> out;
  out.reserve(rows.size());
  for (const auto& b : rows) {
    opt::HistoryRow h;
    h.queue = b.queue_id;
    h.bucket_start_ms = b.bucket_start_ms;
    h.offered = static_cast<double>(b.offered);
    h.abandoned = static_cast<double>(b.abandoned);
    h.handled = static_cast<double>(b.handled);
    h.handle_ms = b.handle_ms;
    h.offered_10s.assign(b.offered_10s.begin(), b.offered_10s.end());
    out.push_back(std::move(h));
  }
  return out;
}

void Optimizer::add_actuals(const std::string& tenant, std::vector<BucketRow> rows) {
  Worker& w = *workers_[std::hash<std::string>{}(tenant) % workers_.size()];
  {
    std::lock_guard lk(w.mu);
    // Tenant isolation: only the rows of the tenant they were loaded for.
    rows.erase(std::remove_if(rows.begin(), rows.end(), [&](const BucketRow& b) { return b.tenant != tenant; }), rows.end());
    w.actuals.emplace_back(tenant, std::move(rows));
  }
  w.cv.notify_one();
}

void Optimizer::run(Worker* w) {
  for (;;) {
    std::shared_ptr<const ModelSnapshot> s;
    std::deque<std::pair<std::string, std::vector<BucketRow>>> actuals;
    {
      std::unique_lock lk(w->mu);
      w->cv.wait(lk, [&] { return stop_.load() || !w->q.empty() || !w->actuals.empty(); });
      if (stop_) return;
      actuals.swap(w->actuals);
      if (!w->q.empty()) {
        s = std::move(w->q.front());
        w->q.pop_front();
      }
    }
    for (auto& [tenant, rows] : actuals) {
      TenantState& ts = w->state[tenant];
      for (const auto& b : rows) ts.fc.add_actual(b.queue_id, b.bucket_start_ms, static_cast<double>(b.offered));
      if (o_.learned) o_.learned->history(tenant, history_rows(rows));
    }
    if (!s) continue;
    // With a learned source the lever tick runs first: its forecast is the EWT's lambda (plan_for).
    LeverOut early;
    bool early_done = false;
    if (o_.levers && o_.learned) {
      TenantState& ts = w->state[s->tenant];
      if (!ts.history_asked && history_) ts.history_asked = history_(s->tenant, s->at_ms - o_.history_ms);
      const auto t1 = std::chrono::steady_clock::now();
      early = levers_for(*s, &ts, o_, &w->stats);
      early_done = true;
      const auto us2 = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1).count());
      lever_us_sum_ += us2;
      uint64_t prev2 = lever_us_max_.load();
      while (us2 > prev2 && !lever_us_max_.compare_exchange_weak(prev2, us2)) {
      }
    }
    const auto t0 = std::chrono::steady_clock::now();
    const uint64_t clamps0 = w->stats.clamps;
    bool approx = false;
    std::shared_ptr<EwtPlan> plan = plan_for(*s, &w->state[s->tenant], o_, &w->stats, &approx);
    const auto us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count());
    solve_us_sum_ += us;
    uint64_t prev = solve_us_max_.load();
    while (us > prev && !solve_us_max_.compare_exchange_weak(prev, us)) {
    }
    clamps_ += w->stats.clamps - clamps0;
    if (approx) ++approx_;
    if (!plan) {
      ++refused_;  // NaN/inf or invalid inputs: not published, the last good plan stays
      continue;
    }
    ++plans_;
    Engine* e = engine_.load(std::memory_order_acquire);
    if (e) {
      std::shared_ptr<const EwtPlan> p = std::move(plan);
      e->post(p->tenant, [p](Tenant* t, int64_t now) {
        if (t) t->install_ewt(p, now);
      }, /*external=*/false);
    }
    if (!o_.levers) continue;
    TenantState& ts = w->state[s->tenant];
    if (!ts.history_asked && history_) {
      // Once per tenant per process once the store takes it (later buckets arrive as snapshots age);
      // refused (Postgres not connected yet, its queue full): asked again next tick.
      ts.history_asked = history_(s->tenant, s->at_ms - o_.history_ms);
    }
    LeverOut lo;
    if (early_done) {
      lo = std::move(early);
    } else {
      const auto t1 = std::chrono::steady_clock::now();
      lo = levers_for(*s, &ts, o_, &w->stats);
      const auto us2 = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1).count());
      lever_us_sum_ += us2;
      uint64_t prev2 = lever_us_max_.load();
      while (us2 > prev2 && !lever_us_max_.compare_exchange_weak(prev2, us2)) {
      }
    }
    if (lo.guard && e) {
      ++guards_;
      std::shared_ptr<const ArrivalGuard> g = std::move(lo.guard);
      e->post(g->tenant, [g](Tenant* t, int64_t now) {
        if (t) t->install_guard(g, now);
      }, /*external=*/false);
    }
    learned_used_ += lo.learned_used;
    learned_fallback_ += lo.learned_fallback;
    if (EventSink* ev = events_.load(std::memory_order_acquire)) {
      for (auto& x : lo.events) ev->publish(std::move(x));
      events_out_ += lo.events.size();
    }
    if (!lo.plan) {
      ++lever_refused_;
      continue;
    }
    ++lever_plans_;
    for (size_t i = 0; i < lo.queues.size() && i < lo.result.w_planned.size(); ++i) {
      if (lo.result.w_planned[i] != lo.queues[i].weight || lo.result.d_planned[i] != lo.queues[i].delay_s) ++lever_changes_;
    }
    if (lo.result.capped) ++capped_;
    if (!lo.result.held.empty()) ++held_;
    if (e) {
      std::shared_ptr<const LeverPlan> p = std::move(lo.plan);
      e->post(p->tenant, [p](Tenant* t, int64_t now) {
        if (t) t->install_levers(p, now);
      }, /*external=*/false);
    }
  }
}

Optimizer::LeverOut Optimizer::levers_for(const ModelSnapshot& s, TenantState* state, const Options& o, model::Stats* st) {
  LeverOut out;
  const double window = s.window_ms / 1000.0;
  std::vector<opt::QueueForecast> fcs;
  std::string source = "model";
  auto rank = [](const std::string& x) { return x == "config_default" ? 0 : x == "ewma" ? 1 : x == "ewma_profile" ? 2 : 3; };
  for (const auto& q : s.queues) {
    if (!std::isfinite(q.aht_s) || !std::isfinite(q.waited_s) || !std::isfinite(q.wrap_s) || !std::isfinite(q.handle_s)) {
      return out;
    }
    state->fc.observe(q.id, s.at_ms, window, static_cast<double>(q.arrivals), static_cast<double>(q.abandons), q.waited_s,
                      q.aht_s, q.wrap_s, static_cast<double>(q.vip_arrivals), static_cast<double>(q.handled), q.handle_s);
  }
  // Phases 5b+5c: the learned source's actuals hook (CR-181), before this tick's forecasts.
  if (o.learned) {
    std::vector<opt::ObservedWindow> ws;
    ws.reserve(s.queues.size());
    for (const auto& q : s.queues) {
      opt::ObservedWindow w;
      w.queue = q.id;
      w.at_ms = s.at_ms;
      w.window_ms = s.window_ms;
      w.arrivals = static_cast<double>(q.arrivals);
      w.abandons = static_cast<double>(q.abandons);
      w.handled = static_cast<double>(q.handled);
      w.handle_s = q.handle_s;
      // Seats-based expected volume (a new queue's range until it has history): its eligible
      // agents at 85 % occupancy.
      w.expected_per_hour = q.logged_in > 0 ? q.logged_in * 3600.0 / std::max(30.0, q.aht_s > 0 ? q.aht_s : 180.0) * 0.85 : 0;
      ws.push_back(std::move(w));
    }
    o.learned->observe(s.tenant, ws);
  }
  // A queue's own handle time and patience are used only where they differ from the tenant's by more
  // than their noise (two standard errors: a mean of n handle times, a rate of n abandons); within it
  // the tenant's pooled value stands, so a few calls never make equal queues look different.
  double hs = 0, hn = 0, ab = 0, wt = 0;
  for (const auto& q : s.queues) {
    const opt::Forecaster::Raw r = state->fc.raw(q.id);
    hs += r.aht_s * r.calls;
    hn += r.calls;
    ab += r.abandons;
    wt += r.waited;
  }
  const double aht_t = hn > 0 ? hs / hn : 0;
  const double theta_t = (ab * 900 + kPriorWeightS / kPriorPatienceS) / (wt * 900 + kPriorWeightS);
  for (const auto& q : s.queues) {
    const opt::QueueForecast base = state->fc.forecast(q.id, s.at_ms, q.aht_s > 0 ? q.aht_s : 180);
    opt::QueueForecast f = opt::choose_forecast(base, o.learned, s.tenant, s.at_ms, o.sanity);
    if (rank(f.source) < rank(source)) source = f.source;
    if (o.learned) {
      if (f.source == "model") {
        ++out.learned_used;
        TenantState::Q& e = state->q[q.id];
        e.fc_lambda = f.now_per_s;
        e.fc_at_ms = s.at_ms;
      } else {
        ++out.learned_fallback;
      }
    }
    const opt::Forecaster::Raw r = state->fc.raw(q.id);
    double aht = r.calls >= 1 ? r.aht_s : (aht_t > 0 ? aht_t : base.aht_s);
    if (aht_t > 0 && std::abs(aht - aht_t) <= 2 * aht_t / std::sqrt(std::max(1.0, r.calls))) aht = aht_t;
    // A learned source's handle time (CR-180): only when it owns it (opt-in) AND it passes the
    // pooled-AHT check: it replaces the pooled value only where it differs by more than its noise.
    f.aht_from_model = false;
    if (f.source == "model" && f.owns_aht && f.aht_s > 0 &&
        !(aht_t > 0 && std::abs(f.aht_s - aht_t) <= 2 * aht_t / std::sqrt(std::max(1.0, r.calls)))) {
      aht = f.aht_s;
      f.aht_from_model = true;
    }
    double theta = state->fc.theta(q.id);
    if (std::abs(theta - theta_t) <= 2 * theta_t / std::sqrt(std::max(1.0, r.abandons * 900))) theta = theta_t;
    f.aht_s = aht;
    opt::LeverQueue x;
    x.id = q.id;
    x.lambda = f.now_per_s;
    x.vip_lambda = f.now_per_s * f.vip_share;
    x.mu = aht > 0 ? 1 / aht : 0;
    x.theta = theta;
    x.sla_s = q.sla_ms > 0 ? q.sla_ms / 1000.0 : 20;
    x.vip_sla_s = q.vip_sla_ms > 0 ? q.vip_sla_ms / 1000.0 : 0;
    x.alpha = q.alpha;
    x.beta = q.beta;
    x.vip_alpha = q.vip_alpha;
    x.vip_beta = q.vip_beta;
    x.weight = q.weight;
    x.weight_default = q.weight_default;
    x.weight_min = q.weight_min;
    x.weight_max = q.weight_max;
    x.delay_s = q.secondary_delay_ms / 1000.0;
    x.delay_default_s = q.delay_default_ms / 1000.0;
    x.delay_min_s = q.delay_min_ms / 1000.0;
    x.delay_max_s = q.delay_max_ms / 1000.0;
    x.min_agents = q.min_agents;
    x.max_agents = q.max_agents;
    x.vip_reserved = q.vip_reserved_min;
    x.sl_target = q.sl_target;
    // Lever (c)'s horizon: the busier of the rest of this bucket and the next one.
    const double h = f.per_s.size() > 1 ? std::max(f.per_s[0], f.per_s[1]) : f.now_per_s;
    x.horizon_lambda = std::max(h, f.now_per_s);
    x.horizon_vip_lambda = x.horizon_lambda * f.vip_share;
    out.queues.push_back(x);
    fcs.push_back(std::move(f));
  }
  if (o.learned) {
    // The arrival guard: the rate now each queue's forecast in use expects (PLAN §5.9).
    auto g = std::make_shared<ArrivalGuard>();
    g->tenant = s.tenant;
    g->computed_at_ms = s.at_ms;
    g->valid_until_ms = s.at_ms + o.valid_ticks * o.tick_ms;
    // A queue still on config defaults (nothing observed yet) gets no guard: its 0 would trip on
    // every third call.
    for (const auto& f : fcs) {
      if (f.source != "config_default") g->per_s.emplace_back(f.queue, f.now_per_s);
    }
    out.guard = std::move(g);
  }
  ++state->ticks;
  const bool auto_mode = s.mode == static_cast<uint8_t>(RoutingMode::kAuto);
  const bool advice_tick = o.forecast_every > 0 && (state->ticks - 1) % static_cast<uint64_t>(o.forecast_every) == 0;
  // Lever (a)'s search runs in advise too (the plan says what auto would do); recommendations in
  // advise and auto, once a minute.
  out.result = opt::plan_levers(s, out.queues, &state->lever_mem, /*search=*/s.mode != 0,
                                /*recommend=*/s.mode != 0 && advice_tick, o.lever, st);
  const opt::LeverResult& r = out.result;
  if (!r.ok) return out;
  auto plan = std::make_shared<LeverPlan>();
  plan->tenant = s.tenant;
  plan->version = std::max(state->lever_version, s.plan_version) + 1;
  state->lever_version = plan->version;
  plan->computed_at_ms = s.at_ms;
  plan->valid_until_ms = s.at_ms + o.valid_ticks * o.tick_ms;
  plan->quality = static_cast<uint8_t>(r.quality);
  bool changes = false;
  for (size_t i = 0; i < out.queues.size(); ++i) {
    LeverPlan::Queue pq;
    pq.id = out.queues[i].id;
    pq.weight = r.w_planned[i];
    pq.delay_ms = static_cast<int64_t>(std::llround(r.d_planned[i] * 1000));
    pq.quality = static_cast<uint8_t>(r.q_quality[i]);
    changes = changes || r.w_planned[i] != out.queues[i].weight || r.d_planned[i] != out.queues[i].delay_s;
    plan->queues.push_back(pq);
  }
  opt::PlanInfo info;
  info.version = plan->version;
  info.valid_until_ms = plan->valid_until_ms;
  info.mode = s.mode == 0 ? "off" : s.mode == 1 ? "advise" : "auto";
  info.source = source;
  if (s.mode == 0) {
    info.applies = "none";
  } else if (!auto_mode) {
    info.applies = changes ? "held: advise" : "none";
  } else if (r.quality != model::Quality::kExact && changes) {
    info.applies = "levers (exact queues only)";
  } else {
    info.applies = changes ? "levers" : !r.held.empty() ? "held: " + r.held : "none";
  }
  out.events.push_back(opt::staffing_plan_event(s, out.queues, r, info));
  if (!r.recs.empty()) out.events.push_back(opt::recommendation_event(s, r, info, o.lever.horizon_s));
  for (size_t i = 0; i < s.queues.size() && i < r.current.size(); ++i) {
    out.events.push_back(opt::queue_snapshot_event(s, i, r.current[i].m));
  }
  if (advice_tick) {
    for (const auto& f : fcs) out.events.push_back(opt::forecast_event(s, f));
  }
  out.plan = std::move(plan);
  return out;
}

std::shared_ptr<EwtPlan> Optimizer::plan_for(const ModelSnapshot& s, TenantState* state, const Options& o,
                                             model::Stats* st, bool* approx) {
  const double window = std::max(1.0, s.window_ms / 1000.0);
  const double a = s.window_ms > 0 ? 1 - std::exp(-window / kTauS) : 1.0;
  std::vector<opt::LeverQueue> lq;
  std::vector<bool> mix(s.queues.size(), false);
  for (size_t i = 0; i < s.queues.size(); ++i) {
    const auto& q = s.queues[i];
    if (!std::isfinite(q.aht_s) || !std::isfinite(q.waited_s)) return nullptr;
    TenantState::Q& e = state->q[q.id];
    const double lam = s.window_ms > 0 ? q.arrivals / window : 0;
    const double vip = s.window_ms > 0 ? std::min(q.vip_arrivals, q.arrivals) / window : 0;
    const double ab = s.window_ms > 0 ? q.abandons / window : 0;
    const double wt = s.window_ms > 0 ? q.waited_s / window : 0;
    if (!e.seen && s.window_ms > 0) {
      e = TenantState::Q{true, lam, ab, wt, vip};
    } else if (s.window_ms > 0) {
      e.lambda += a * (lam - e.lambda);
      e.abandons += a * (ab - e.abandons);
      e.waited += a * (wt - e.waited);
      e.vip += a * (vip - e.vip);
    }
    // CR-098: numeric priorities aren't modelled (the VIP class is), so a queue that has seen a
    // numeric priority mix is approx for one estimator time constant after it.
    if (q.priority_mix) e.mix_until_ms = s.at_ms + static_cast<int64_t>(kTauS * 1000);
    mix[i] = e.mix_until_ms > s.at_ms;
    opt::LeverQueue x;
    x.id = q.id;
    x.lambda = e.lambda;
    // Phases 5b+5c: the learned forecast this snapshot's lever tick used (it runs first then).
    if (o.learned && e.fc_at_ms == s.at_ms && e.fc_lambda >= 0) x.lambda = e.fc_lambda;
    x.vip_lambda = std::min(e.vip, x.lambda);
    x.mu = q.aht_s > 0 ? 1 / q.aht_s : 0;
    // theta: abandons / time waited (the exponential MLE, PLAN §3.1), with a weak prior.
    x.theta = (e.abandons * kTauS + kPriorWeightS / kPriorPatienceS) / (e.waited * kTauS + kPriorWeightS);
    x.sla_s = q.sla_ms > 0 ? q.sla_ms / 1000.0 : 20;
    x.vip_sla_s = q.vip_sla_ms > 0 ? q.vip_sla_ms / 1000.0 : 0;
    x.weight = q.weight;
    x.delay_s = q.secondary_delay_ms / 1000.0;
    x.min_agents = q.min_agents;
    x.max_agents = q.max_agents;
    x.vip_reserved = q.vip_reserved_min;
    lq.push_back(x);
  }
  std::vector<model::AQueue> aq;
  std::vector<model::AAgent> ag;
  model::AgentOptions mo = o.lever.model;
  opt::model_inputs(s, lq, &aq, &ag, &mo.dispatch);
  model::AgentModel m(std::move(aq), std::move(ag), mo, st);
  const model::AgentResult& r = m.base();
  // PLAN §3.7: a failed solve publishes nothing, so the tenant's last good plan stays valid.
  if (!r.ok) return nullptr;
  auto plan = std::make_shared<EwtPlan>();
  plan->tenant = s.tenant;
  plan->version = ++state->version;
  plan->computed_at_ms = s.at_ms;
  plan->valid_until_ms = s.at_ms + o.valid_ticks * o.tick_ms;
  if (approx) *approx = r.quality == model::Quality::kApprox;
  // `forced_approx`: the estimate leaves out something the model can't see (CR-098).
  auto table = [&](const model::AgentModel::Tag& tg, model::Quality ql, int64_t max_wait_ms, uint32_t waiting,
                   bool forced_approx) {
    EwtTable t;
    model::Quality q = model::Quality::kExact;
    const int kmax = std::min<int>(o.max_positions, static_cast<int>(waiting) + 50);
    t.by_position = model::ewt_by_position(tg.s_tag, tg.mu, tg.theta, tg.lambda_hp, kmax, max_wait_ms / 1000.0, &q);
    t.long_wait = q == model::Quality::kLong;
    const double r_next = tg.s_tag * tg.mu - tg.lambda_hp + kmax * tg.theta;
    t.step_s = r_next > 0 ? 1 / r_next : 0;
    q = model::worst(q, ql);
    if (forced_approx) q = model::worst(q, model::Quality::kApprox);
    t.quality = static_cast<uint8_t>(q);
    for (double v : t.by_position) {
      if (!std::isfinite(v) || v < 0) return std::optional<EwtTable>();  // refused at the boundary
    }
    if (!std::isfinite(t.step_s)) return std::optional<EwtTable>();
    return std::optional<EwtTable>(std::move(t));
  };
  for (size_t i = 0; i < s.queues.size(); ++i) {
    const auto& q = s.queues[i];
    const model::AgentQueue& rq = r.q[i];
    if (!rq.m.ok) continue;  // that queue keeps phase 1's estimate
    const model::QueueMetrics& normal = rq.normal.ok ? rq.normal : rq.m;
    auto t = table(m.tag(static_cast<int>(i), false), normal.quality, q.max_wait_ms, q.waiting, mix[i]);
    if (!t) {
      if (st) ++st->refused;
      continue;
    }
    plan->tables[{q.id, ""}] = std::move(*t);
    // The VIP class: positions count VIP entries only (they go first).
    if (lq[i].vip_lambda > 0 || q.vip_waiting > 0) {
      const model::QueueMetrics& vm = rq.vip.ok ? rq.vip : rq.m;
      auto tv = table(m.tag(static_cast<int>(i), true), vm.quality, q.max_wait_ms, q.vip_waiting, mix[i]);
      if (tv) plan->tables[{q.id, kVipSig}] = std::move(*tv);
    }
  }
  // Entries with extra requirements (CR-033, CR-098): the agents that meet them, counted
  // individually; exact when every agent of the queue does, else approx (a reach nested in the
  // queue's). Nobody eligible -> LONG with the time to max wait as the lower bound.
  for (const auto& sig : s.sigs) {
    const size_t i = static_cast<size_t>(sig.queue);
    if (i >= s.queues.size() || i >= r.q.size() || !r.q[i].m.ok) continue;
    const double R = r.q[i].R, able = sig.share * R;
    const model::QueueMetrics sm = m.signature(static_cast<int>(i), able);
    model::AgentModel::Tag tg = m.tag(static_cast<int>(i), false);
    tg.s_tag *= R > 0 ? std::min(1.0, able / R) : 0;
    auto t = table(tg, sm.ok ? sm.quality : model::Quality::kApprox, s.queues[i].max_wait_ms, s.queues[i].waiting,
                   mix[i] || !sm.ok);
    if (t) plan->tables[{s.queues[i].id, sig.sig}] = std::move(*t);
  }
  return plan;
}

}  // namespace pcr
