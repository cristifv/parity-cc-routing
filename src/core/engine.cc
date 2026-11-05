#include "src/core/engine.h"

#include <pthread.h>
#include <algorithm>
#include <chrono>
#include <future>
#include <thread>

#include "src/state/state.pb.h"

namespace pcr {

namespace {
constexpr int64_t kPeriodMs = 1'000;  // sweep + redelivery
constexpr uint64_t kPruneEvery = 60;  // ticks
}  // namespace

Engine::Engine(const Clock* clock, Sink* sink, int shards, Limits limits, ModelFeed feed)
    : clock_(clock), sink_(sink), limits_(limits), feed_(std::move(feed)) {
  gate_sink_.inner = sink_;
  gate_durable_.engine = this;
  if (shards < 1) shards = 1;
  for (int i = 0; i < shards; ++i) shards_.push_back(std::make_unique<Shard>(this, i));
  for (auto& s : shards_) {
    s->next_periodic = clock_->now_ms() + kPeriodMs;
    s->next_model = clock_->now_ms() + limits_.model_tick_ms;
    Shard* raw = s.get();
    s->thread = std::thread([raw, i = s->index()] {
      pthread_setname_np(pthread_self(), ("pcr-shard-" + std::to_string(i)).c_str());  // phase 7: CPU per thread
      raw->loop();
    });
  }
}

Engine::~Engine() {
  for (auto& s : shards_) {
    {
      std::lock_guard lk(s->mu);
      s->stop = true;
    }
    s->cv.notify_all();
  }
  for (auto& s : shards_) {
    if (s->thread.joinable()) s->thread.join();
  }
}

Engine::Shard& Engine::shard_of(const std::string& tenant) const {
  const size_t h = std::hash<std::string>{}(tenant);
  return *shards_[h % shards_.size()];
}

void Engine::post_shard(Shard& s, Task t) {
  {
    std::lock_guard lk(s.mu);
    if (!t.tenant.empty()) ++s.per_tenant[t.tenant];
    s.inbox.push_back(std::move(t));
  }
  s.cv.notify_one();
}

bool Engine::quarantined(const std::string& tenant) const {
  if (quarantine_count_.load(std::memory_order_acquire) == 0) return false;
  std::lock_guard lk(tenants_mu_);
  return quarantined_.contains(tenant);
}

bool Engine::post(const std::string& tenant, Fn fn, bool external) {
  Shard& s = shard_of(tenant);
  if (quarantined(tenant)) {
    // CR-137: retryable; the tenant comes back when it is rebuilt from re.state.
    inbox_refused_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  {
    std::lock_guard lk(s.mu);
    if (external) {
      auto it = s.per_tenant.find(tenant);
      const size_t mine = it == s.per_tenant.end() ? 0 : it->second;
      if (s.inbox.size() >= limits_.inbox || mine >= limits_.per_tenant) {
        inbox_refused_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
    }
    Shard* sp = &s;
    Task t;
    t.tenant = tenant;
    t.external = external;
    t.run = [sp, tenant, fn = std::move(fn)](int64_t now) {
      auto it = sp->tenants.find(tenant);
      fn(it == sp->tenants.end() || sp->quarantined.contains(tenant) ? nullptr : it->second.get(), now);
    };
    ++s.per_tenant[tenant];
    s.inbox.push_back(std::move(t));
  }
  s.cv.notify_one();
  return true;
}

void Engine::post_all_tenants(Fn fn) {
  for (auto& s : shards_) {
    Shard* sp = s.get();
    Task t;
    t.run = [sp, fn](int64_t now) {
      for (auto& [id, tenant] : sp->tenants) fn(tenant.get(), now);
    };
    post_shard(*s, std::move(t));
  }
}

void Engine::barrier() {
  std::vector<std::future<void>> done;
  for (auto& s : shards_) {
    auto p = std::make_shared<std::promise<void>>();
    done.push_back(p->get_future());
    Task t;
    t.run = [p](int64_t) { p->set_value(); };
    post_shard(*s, std::move(t));
  }
  for (auto& f : done) f.wait();
}

void Engine::load(const std::vector<TenantConfig>& tenants, Epochs epochs) {
  barrier();
  epochs_ = epochs;
  {
    std::lock_guard lk(tenants_mu_);
    tenant_index_.clear();
    for (const auto& t : tenants) tenant_index_[t.id] = 1;
  }
  for (auto& s : shards_) {
    Shard* sp = s.get();
    std::vector<TenantConfig> mine;
    for (const auto& t : tenants) {
      if (&shard_of(t.id) == sp) mine.push_back(t);
    }
    Task t;
    t.run = [this, sp, mine = std::move(mine)](int64_t now) {
      sp->tenants.clear();
      sp->timers = {};
      sp->next_periodic = now + kPeriodMs;
      for (const auto& cfg : mine) make_tenant(sp, cfg, 0);
    };
    post_shard(*s, std::move(t));
  }
  barrier();
}

bool Engine::has_tenant(const std::string& tenant) const {
  std::lock_guard lk(tenants_mu_);
  return tenant_index_.contains(tenant);
}

std::vector<std::string> Engine::tenant_ids() const {
  std::lock_guard lk(tenants_mu_);
  std::vector<std::string> out;
  for (const auto& [id, n] : tenant_index_) out.push_back(id);
  std::sort(out.begin(), out.end());
  return out;
}

void Engine::make_tenant(Shard* sp, const TenantConfig& cfg, uint64_t tenant_epoch) {
  auto t = std::make_unique<Tenant>(cfg, &epochs_, &gate_sink_, sp, &sp->counters);
  t->set_model_ewt(limits_.model_ewt);
  t->set_optimizer(limits_.optimizer && feed_ != nullptr);
  t->set_position_throttle(limits_.position_throttle);
  t->set_durable(durable_);
  t->set_events(events_);
  t->set_tenant_epoch(tenant_epoch);
  t->set_tracking(log_ != nullptr);
  sp->tenants[cfg.id] = std::move(t);
}

void Engine::set_durable(Durable* d, EventSink* e) {
  gate_durable_.inner = d;
  gate_events_.inner = e;
  durable_ = d ? &gate_durable_ : nullptr;
  events_ = e ? &gate_events_ : nullptr;
}

void Engine::add_tenant(const TenantConfig& cfg, uint64_t tenant_epoch, std::function<void(Tenant*, int64_t)> init) {
  Shard& s = shard_of(cfg.id);
  Shard* sp = &s;
  auto p = std::make_shared<std::promise<void>>();
  auto done = p->get_future();
  Task t;
  t.run = [this, sp, cfg, tenant_epoch, init = std::move(init), p](int64_t now) {
    make_tenant(sp, cfg, tenant_epoch);
    if (init) init(sp->tenants.at(cfg.id).get(), now);
    // Phase 5 (p6 review): a takeover's or arrival's tenant gets a fresh plan within a second, well
    // inside the restored plan's validity, so a failover never looks like a stalled optimizer.
    sp->next_model = std::min(sp->next_model, now + 1'000);
    p->set_value();
  };
  post_shard(s, std::move(t));
  done.wait();
  std::lock_guard lk(tenants_mu_);
  tenant_index_[cfg.id] = 1;
}

void Engine::remove_tenant(const std::string& id, uint64_t new_epoch, const std::string& from_bin,
                           const std::string& to_bin, std::function<void(Tenant*, int64_t)> before) {
  {
    std::lock_guard lk(tenants_mu_);
    tenant_index_.erase(id);  // new acknowledgements are refused from now on
  }
  Shard& s = shard_of(id);
  Shard* sp = &s;
  auto p = std::make_shared<std::promise<void>>();
  auto done = p->get_future();
  Task t;
  t.run = [this, sp, id, new_epoch, from_bin, to_bin, before = std::move(before), p](int64_t now) {
    auto it = sp->tenants.find(id);
    if (it != sp->tenants.end()) {
      Tenant* tn = it->second.get();
      if (before) before(tn, now);
      if (Batch* b = gate::current()) {
        tn->collect(&b->records, now);
        ::parity::cc_routing::state::v1::Record rec;
        rec.set_schema_version(1);
        rec.set_tenant_epoch(tn->tenant_epoch());
        rec.set_leader_epoch(epochs_.leader_epoch);
        rec.set_written_at_ms(now);
        auto* h = rec.mutable_handover();
        h->set_tenant_id(id);
        h->set_tenant_epoch(new_epoch);
        h->set_from_bin(from_bin);
        h->set_to_bin(to_bin);
        b->records.push_back(StateRec{id, id + "/handover/" + std::to_string(new_epoch), rec.SerializeAsString()});
      }
      sp->tenants.erase(it);
    }
    p->set_value();
  };
  post_shard(s, std::move(t));
  done.wait();
}

// ---- the commit gate's wrappers ----

void Engine::GateSink::to_ie(const std::string& r, uint64_t g, const rv1::WatchResponse& m) {
  if (Batch* b = gate::current()) {
    b->effects.push_back([in = inner, r, g, m] { in->to_ie(r, g, m); });
  } else {
    inner->to_ie(r, g, m);
  }
}

void Engine::GateSink::to_gw(const std::string& r, uint64_t g, const av1::WatchResponse& m) {
  if (Batch* b = gate::current()) {
    b->effects.push_back([in = inner, r, g, m] { in->to_gw(r, g, m); });
  } else {
    inner->to_gw(r, g, m);
  }
}

bool Engine::GateDurable::put_callback(const CallbackRecord& r, Done done) {
  Batch* b = gate::current();
  if (!b) return inner->put_callback(r, std::move(done));
  // After the commit: a refusal then (Postgres down, its queue full) reaches the tenant as a
  // failed write, as the store's own failures do.
  b->effects.push_back([e = engine, in = inner, r, done = std::move(done)] {
    if (in->put_callback(r, done)) return;
    e->post(r.tenant, [done](Tenant* t, int64_t now) { done(t, false, now); }, /*external=*/false);
  });
  return true;
}

bool Engine::GateDurable::put_task(const TaskRecord& r, TaskDone done) {
  Batch* b = gate::current();
  if (!b) return inner->put_task(r, std::move(done));
  b->effects.push_back([e = engine, in = inner, r, done = std::move(done)] {
    if (in->put_task(r, done)) return;
    e->post(r.tenant, [done](Tenant* t, int64_t now) { done(t, false, "", false, now); }, /*external=*/false);
  });
  return true;
}

void Engine::GateDurable::put_buckets(std::vector<BucketRow> rows) {
  if (Batch* b = gate::current()) {
    b->effects.push_back([in = inner, rows = std::move(rows)]() mutable { in->put_buckets(std::move(rows)); });
  } else {
    inner->put_buckets(std::move(rows));
  }
}

void Engine::GateEvents::publish(AgentEvent e) {
  if (Batch* b = gate::current()) {
    b->effects.push_back([in = inner, e = std::move(e)]() mutable { in->publish(std::move(e)); });
  } else {
    inner->publish(std::move(e));
  }
}

void Engine::advance_to(ManualClock* clock, int64_t t) {
  barrier();
  if (t < clock->now_ms()) return;
  clock->set(t);
  std::vector<std::future<void>> done;
  for (auto& s : shards_) {
    auto p = std::make_shared<std::promise<void>>();
    done.push_back(p->get_future());
    Shard* sp = s.get();
    Task task;
    task.run = [sp, t, p](int64_t) {
      sp->run_due(t, /*manual=*/true);
      p->set_value();
    };
    post_shard(*s, std::move(task));
  }
  for (auto& f : done) f.wait();
}

void Engine::stop_model_feed() {
  feed_stopped_.store(true);
  barrier();  // a shard inside periodic() finishes its feed call before it runs the barrier task
}

void Engine::set_draining(bool d) {
  post_all_tenants([d](Tenant* t, int64_t) {
    if (t) t->set_draining(d);
  });
  barrier();
}

Engine::Totals Engine::totals() const {
  Totals t{};
  for (const auto& s : shards_) {
    const Counters& c = s->counters;
    t.offers += c.offers.load(std::memory_order_relaxed);
    t.accepted += c.offers_accepted.load(std::memory_order_relaxed);
    t.rejected += c.offers_rejected.load(std::memory_order_relaxed);
    t.expired += c.offers_expired.load(std::memory_order_relaxed);
    t.revoked += c.offers_revoked.load(std::memory_order_relaxed);
    t.overflows += c.overflows.load(std::memory_order_relaxed);
    t.sweep_matches += c.sweep_matches.load(std::memory_order_relaxed);
    t.assign_expired += c.assign_expired.load(std::memory_order_relaxed);
    t.redeliveries += c.redeliveries.load(std::memory_order_relaxed);
    t.owner_lost += c.owner_lost.load(std::memory_order_relaxed);
    t.ewt_model += c.ewt_model.load(std::memory_order_relaxed);
    t.ewt_heuristic += c.ewt_heuristic.load(std::memory_order_relaxed);
    t.plans_installed += c.plans_installed.load(std::memory_order_relaxed);
    t.plans_refused += c.plans_refused.load(std::memory_order_relaxed);
    t.decision_max_us = std::max(t.decision_max_us, c.decision_us_max.load(std::memory_order_relaxed));
    t.moves_applied += c.moves_applied.load(std::memory_order_relaxed);
    t.moves_pending += c.moves_pending.load(std::memory_order_relaxed);
    t.moves_refused += c.moves_refused.load(std::memory_order_relaxed);
    t.moves_stale += c.moves_stale.load(std::memory_order_relaxed);
    t.coverage_refused += c.coverage_refused.load(std::memory_order_relaxed);
    t.foreign_refused += c.foreign_refused.load(std::memory_order_relaxed);
    t.config_refused += c.config_refused.load(std::memory_order_relaxed);
    t.forced_states += c.forced_states.load(std::memory_order_relaxed);
    t.callbacks_created += c.callbacks_created.load(std::memory_order_relaxed);
    t.callbacks_dispatched += c.callbacks_dispatched.load(std::memory_order_relaxed);
    t.callbacks_flagged += c.callbacks_flagged.load(std::memory_order_relaxed);
    t.callbacks_refused += c.callbacks_refused.load(std::memory_order_relaxed);
    t.voicemail_tasks += c.voicemail_tasks.load(std::memory_order_relaxed);
    t.direct_offers += c.direct_offers.load(std::memory_order_relaxed);
    t.consults += c.consults.load(std::memory_order_relaxed);
    t.gw_outbox_coalesced += c.gw_outbox_coalesced.load(std::memory_order_relaxed);
    t.gw_outbox_dropped += c.gw_outbox_dropped.load(std::memory_order_relaxed);
    t.levers_installed += c.levers_installed.load(std::memory_order_relaxed);
    t.levers_refused += c.levers_refused.load(std::memory_order_relaxed);
    t.levers_applied += c.levers_applied.load(std::memory_order_relaxed);
    t.levers_held += c.levers_held.load(std::memory_order_relaxed);
    t.stall_stale += c.stall_stale.load(std::memory_order_relaxed);
    t.vip_enqueued += c.vip_enqueued.load(std::memory_order_relaxed);
    t.bounds_min_picks += c.bounds_min_picks.load(std::memory_order_relaxed);
    t.bounds_max_defers += c.bounds_max_defers.load(std::memory_order_relaxed);
    t.vip_reserve_holds += c.vip_reserve_holds.load(std::memory_order_relaxed);
    for (int b = 0; b < 32; ++b) t.decision_us_buckets[b] += c.decision_us_buckets[b].load(std::memory_order_relaxed);
    t.decision_us_sum += c.decision_us_sum.load(std::memory_order_relaxed);
    for (int b = 0; b < 32; ++b) t.inbox_wait_us_buckets[b] += c.inbox_wait_us_buckets[b].load(std::memory_order_relaxed);
    t.inbox_wait_us_sum += c.inbox_wait_us_sum.load(std::memory_order_relaxed);
    t.inbox_wait_us_max = std::max(t.inbox_wait_us_max, c.inbox_wait_us_max.load(std::memory_order_relaxed));
    t.periodic_us_sum += c.periodic_us_sum.load(std::memory_order_relaxed);
    t.periodic_us_max = std::max(t.periodic_us_max, c.periodic_us_max.load(std::memory_order_relaxed));
    t.periodics += c.periodics.load(std::memory_order_relaxed);
    t.early_snapshots += c.early_snapshots.load(std::memory_order_relaxed);
    t.waiting += c.waiting.load(std::memory_order_relaxed);
    t.ready += c.ready.load(std::memory_order_relaxed);
  }
  t.inbox_refused = inbox_refused_.load(std::memory_order_relaxed);
  t.tenants_quarantined = quarantine_count_.load(std::memory_order_relaxed);
  t.records_refused = records_refused_.load(std::memory_order_relaxed);
  return t;
}

std::vector<std::string> Engine::check_invariants(uint64_t* checked) {
  auto out = std::make_shared<std::vector<std::string>>();
  auto mu = std::make_shared<std::mutex>();
  auto n = std::make_shared<std::atomic<uint64_t>>(0);
  post_all_tenants([out, mu, n](Tenant* t, int64_t now) {
    if (!t) return;
    uint64_t c = 0;
    auto v = t->check_invariants(now, &c);
    *n += c;
    std::lock_guard lk(*mu);
    out->insert(out->end(), v.begin(), v.end());
  });
  barrier();
  if (checked) *checked += n->load();
  return *out;
}

std::vector<std::string> Engine::held_interactions() {
  auto out = std::make_shared<std::vector<std::string>>();
  auto mu = std::make_shared<std::mutex>();
  post_all_tenants([out, mu](Tenant* t, int64_t) {
    if (!t) return;
    auto v = t->held_interactions();
    std::lock_guard lk(*mu);
    out->insert(out->end(), v.begin(), v.end());
  });
  barrier();
  return *out;
}

std::vector<std::pair<std::string, Tenant::EntrySnap>> Engine::entries_snapshot() {
  auto out = std::make_shared<std::vector<std::pair<std::string, Tenant::EntrySnap>>>();
  auto mu = std::make_shared<std::mutex>();
  post_all_tenants([out, mu](Tenant* t, int64_t) {
    if (!t) return;
    auto v = t->entries_snapshot();
    std::lock_guard lk(*mu);
    for (auto& e : v) out->emplace_back(t->id(), std::move(e));
  });
  barrier();
  return *out;
}

size_t Engine::queued(const std::string& tenant) const {
  Shard& s = shard_of(tenant);
  std::lock_guard lk(s.mu);
  auto it = s.per_tenant.find(tenant);
  return it == s.per_tenant.end() ? 0 : it->second;
}

// ---- Shard ----

void Engine::Shard::at(int64_t due_ms, TimerRef t) {
  timers.push(TimerItem{due_ms, ++timer_seq, std::move(t)});
}

void Engine::Shard::gated(const std::string& tenant, const std::function<void()>& fn) {
  if (!engine_->log_) {
    fn();
    return;
  }
  Batch* prev = gate::current();
  gate::set_current(&batch);
  const size_t rec_mark = batch.records.size(), eff_mark = batch.effects.size();
  fn();
  const int64_t now = engine_->clock_->now_ms();
  if (!tenant.empty()) {
    auto it = tenants.find(tenant);
    if (it != tenants.end() && !quarantined.contains(tenant)) {
      it->second->collect(&batch.records, now);
      contain(tenant, rec_mark, eff_mark, /*own_task=*/true);
    }
  } else {
    for (auto& [id, t] : tenants) {
      if (quarantined.contains(id)) continue;
      const size_t mark = batch.records.size();
      t->collect(&batch.records, now);
      // A shard-wide task's effects are not the tenant's alone: only its records are dropped.
      contain(id, mark, batch.effects.size(), /*own_task=*/false);
    }
  }
  gate::set_current(prev);
}

bool Engine::Shard::contain(const std::string& tenant, size_t rec_mark, size_t eff_mark, bool own_task) {
  bool over = false;
  for (size_t i = rec_mark; i < batch.records.size() && !over; ++i) {
    const StateRec& r = batch.records[i];
    over = r.tenant == tenant && r.key.size() + (r.value ? r.value->size() : 0) > engine_->limits_.max_record_bytes;
  }
  if (!over) return false;
  // Every record of this tenant since the mark goes; another tenant's (a shard-wide task) stays.
  std::vector<StateRec> kept(batch.records.begin(), batch.records.begin() + static_cast<ptrdiff_t>(rec_mark));
  for (size_t i = rec_mark; i < batch.records.size(); ++i) {
    if (batch.records[i].tenant != tenant) kept.push_back(std::move(batch.records[i]));
  }
  batch.records.swap(kept);
  if (own_task) batch.effects.resize(std::min(batch.effects.size(), eff_mark));
  quarantined.insert(tenant);
  {
    std::lock_guard lk(engine_->tenants_mu_);
    if (engine_->quarantined_.insert(tenant).second) engine_->quarantine_count_.fetch_add(1, std::memory_order_release);
  }
  // Counted (parity_cc_routing_tenants_quarantined, records_refused_total; //src/core does no I/O).
  engine_->records_refused_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

void Engine::Shard::flush() {
  if (!engine_->log_ || batch.empty()) return;
  engine_->log_->submit(std::move(batch));
  batch = Batch{};
}

void Engine::Shard::periodic(int64_t now) {
  ++periodic_count;
  uint64_t waiting = 0, ready = 0;
  for (auto& [id, t] : tenants) {
    if (quarantined.contains(id)) continue;
    // Each tenant's part of the tick is contained on its own (CR-137): its records and effects.
    const size_t rec_mark = batch.records.size(), eff_mark = batch.effects.size();
    if (engine_->log_) gate::set_current(&batch);
    t->sweep(now);
    t->redeliver(now);
    t->reap_orphans(now);
    if (periodic_count % kPruneEvery == 0) t->prune(now);
    if (engine_->durable_) {  // routing_bucket_actuals at each 15-minute bucket end (best effort)
      auto rows = t->take_buckets(now);
      if (!rows.empty()) engine_->durable_->put_buckets(std::move(rows));
    }
    if (engine_->log_) {
      t->collect(&batch.records, now);
      if (contain(id, rec_mark, eff_mark, /*own_task=*/true)) continue;
    }
    waiting += t->waiting_entries();
    ready += t->ready_agents();
  }
  counters.waiting.store(waiting, std::memory_order_relaxed);
  counters.ready.store(ready, std::memory_order_relaxed);
  // Phase 2: every model tick, each tenant's snapshot to the optimization layer (never waits).
  if (engine_->feed_ && (engine_->limits_.model_ewt || engine_->limits_.optimizer) && !engine_->feed_stopped_.load() &&
      now >= next_model) {
    next_model = now + engine_->limits_.model_tick_ms;
    for (auto& [id, t] : tenants) engine_->feed_(t->model_snapshot(now));
  } else if (engine_->feed_ && engine_->limits_.optimizer && !engine_->feed_stopped_.load()) {
    // Phases 5b+5c: a tenant whose arrivals left its guard (the forecast in use) re-forecasts now.
    for (auto& [id, t] : tenants) {
      if (quarantined.contains(id) || !t->guard_breached(now)) continue;
      counters.early_snapshots.fetch_add(1, std::memory_order_relaxed);
      engine_->feed_(t->model_snapshot(now));
    }
  }
}

// Fires timers and the periodic tick due up to `upto` in time order. With the manual clock each
// runs at its own due time, so a test that jumps 15 s sees the same sequence as 15 s of real time.
void Engine::Shard::run_due(int64_t upto, bool manual) {
  for (int guard = 0; guard < 1'000'000; ++guard) {
    const int64_t tdue = timers.empty() ? INT64_MAX : timers.top().due;
    const int64_t due = std::min(tdue, next_periodic);
    if (due > upto) return;
    const int64_t now = manual ? due : upto;
    if (tdue <= next_periodic) {
      TimerItem it = timers.top();
      timers.pop();
      auto t = tenants.find(it.ref.tenant);
      if (t != tenants.end() && !quarantined.contains(it.ref.tenant)) {
        t->second->set_task_start(std::chrono::steady_clock::now());
        Tenant* tn = t->second.get();
        gated(it.ref.tenant, [&] { tn->on_timer(it.ref, now); });
      }
    } else {
      next_periodic += kPeriodMs;
      if (next_periodic <= upto - 10 * kPeriodMs && !manual) next_periodic = upto + kPeriodMs;
      const auto start = std::chrono::steady_clock::now();
      for (auto& [id, t] : tenants) t->set_task_start(start);
      gated("", [&] { periodic(now); });
      const uint64_t u = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
      counters.periodic_us_sum.fetch_add(u, std::memory_order_relaxed);
      counters.periodics.fetch_add(1, std::memory_order_relaxed);
      if (u > counters.periodic_us_max.load(std::memory_order_relaxed)) counters.periodic_us_max.store(u, std::memory_order_relaxed);
    }
  }
}

void Engine::Shard::loop() {
  const bool manual = engine_->clock_->manual();
  for (;;) {
    Task task;
    bool have = false;
    if (engine_->log_ && engine_->log_->backlog() > engine_->max_backlog_) {
      // Phase 6 backpressure: commits are behind (Redpanda slow or down); take no more work until
      // they catch up, so the inbox fills and new RPCs get UNAVAILABLE.
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      std::lock_guard lk(mu);
      if (stop) return;
      continue;
    }
    {
      std::unique_lock lk(mu);
      if (inbox.empty() && !stop) {
        if (manual) {
          cv.wait(lk, [&] { return stop || !inbox.empty(); });
        } else {
          // Writer-thread data (timers, next_periodic) is read here only by this thread.
          const int64_t now = engine_->clock_->now_ms();
          const int64_t tdue = timers.empty() ? INT64_MAX : timers.top().due;
          const int64_t due = std::min(tdue, next_periodic);
          const int64_t wait = std::clamp<int64_t>(due - now, 0, 100);
          if (wait > 0) cv.wait_for(lk, std::chrono::milliseconds(wait));
        }
      }
      if (stop) return;
      if (!inbox.empty()) {
        task = std::move(inbox.front());
        inbox.pop_front();
        if (!task.tenant.empty()) {
          auto it = per_tenant.find(task.tenant);
          if (it != per_tenant.end() && --it->second == 0) per_tenant.erase(it);
        }
        have = true;
      }
    }
    if (have) {
      // I9: decisions made by this task are timed from its start (the inbox wait is the RPC's).
      const auto start = std::chrono::steady_clock::now();
      {
        const uint64_t u = static_cast<uint64_t>(
            std::max<int64_t>(0, std::chrono::duration_cast<std::chrono::microseconds>(start - task.posted).count()));
        counters.inbox_wait_us_buckets[std::min(31, u == 0 ? 0 : 64 - __builtin_clzll(u))].fetch_add(1, std::memory_order_relaxed);
        counters.inbox_wait_us_sum.fetch_add(u, std::memory_order_relaxed);
        if (u > counters.inbox_wait_us_max.load(std::memory_order_relaxed)) counters.inbox_wait_us_max.store(u, std::memory_order_relaxed);
      }
      if (!task.tenant.empty()) {
        auto it = tenants.find(task.tenant);
        if (it != tenants.end()) it->second->set_task_start(start);
      } else {
        for (auto& [id, t] : tenants) t->set_task_start(start);
      }
      gated(task.tenant, [&] { task.run(engine_->clock_->now_ms()); });
    }
    if (!manual) run_due(engine_->clock_->now_ms(), /*manual=*/false);
    flush();
  }
}

}  // namespace pcr
