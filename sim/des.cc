#include "sim/des.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <unordered_map>
#include <variant>

#include "src/core/tenant.h"
#include "src/core/types.h"

namespace pcr::sim {

double Dist::sample(std::mt19937_64& rng) const {
  switch (kind) {
    case Kind::kFixed:
      return mean;
    case Kind::kExponential:
      return std::exponential_distribution<double>(1.0 / mean)(rng);
    case Kind::kLognormal: {
      const double s2 = std::log(1 + cv * cv);
      return std::lognormal_distribution<double>(std::log(mean) - s2 / 2, std::sqrt(s2))(rng);
    }
  }
  return mean;
}

double QueueSpec::rate(double t) const {
  if (per_s.empty()) return 0;
  const size_t i = std::min(per_s.size() - 1, static_cast<size_t>(std::max(0.0, t) / bucket_s));
  return per_s[i];
}

double QueueSpec::bot_rate(double t) const {
  if (bot_per_s.empty()) return 0;
  const size_t i = std::min(bot_per_s.size() - 1, static_cast<size_t>(std::max(0.0, t) / bucket_s));
  return bot_per_s[i];
}

double QueueStats::sl(double) const {
  const double denom = static_cast<double>(offered) - static_cast<double>(short_abandoned);
  return denom > 0 ? answered_in_sla / denom : 0;
}

Estimate estimate(const std::vector<double>& x) {
  Estimate e;
  if (x.empty()) return e;
  double m = 0;
  for (double v : x) m += v;
  m /= x.size();
  e.mean = m;
  if (x.size() < 2) return e;
  double ss = 0;
  for (double v : x) ss += (v - m) * (v - m);
  const double sd = std::sqrt(ss / (x.size() - 1));
  // Student t, 97.5 % quantile by degrees of freedom.
  static const double t975[] = {0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262,
                                2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093,
                                2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042};
  const size_t df = x.size() - 1;
  const double t = df < sizeof(t975) / sizeof(t975[0]) ? t975[df] : 1.96;
  e.half = t * sd / std::sqrt(static_cast<double>(x.size()));
  return e;
}

namespace {

namespace rv1 = ::parity::routing::v1;
namespace av1 = ::parity::agents::v1;

constexpr int64_t kStartMs = 1'792'800'000'000;
constexpr char kTenant[] = "acme";
constexpr char kIe[] = "ie-sim-1";
constexpr char kGw[] = "gw-sim-1";

std::string uuid_of(uint64_t kind, uint64_t n) {
  char buf[37];
  std::snprintf(buf, sizeof buf, "0192f6a1-%04x-7000-8000-%012llx", static_cast<unsigned>(kind & 0xffff),
                static_cast<unsigned long long>(n & 0xFFFFFFFFFFFFULL));
  return buf;
}

struct Msg {
  std::variant<rv1::WatchResponse, av1::WatchResponse> m;
};

class SimSink final : public Sink {
 public:
  std::vector<Msg> out;
  void to_ie(const std::string&, uint64_t, const rv1::WatchResponse& m) override { out.push_back(Msg{m}); }
  void to_gw(const std::string&, uint64_t, const av1::WatchResponse& m) override { out.push_back(Msg{m}); }
  bool ie_live(const std::string&) const override { return true; }
};

enum class Ev : uint8_t { kArrival, kAbandon, kCallEnd, kWrapEnd, kTimer, kTick, kLogin, kCheck, kHook, kRedial };

struct Event {
  int64_t t;
  uint64_t seq;
  Ev kind;
  int a = 0;  // queue, call or agent index
  std::shared_ptr<TimerRef> timer;
  bool operator>(const Event& o) const { return t != o.t ? t > o.t : seq > o.seq; }
};

class Driver;

class SimTimers final : public TimerSink {
 public:
  explicit SimTimers(Driver* d) : d_(d) {}
  void at(int64_t due_ms, TimerRef t) override;

 private:
  Driver* d_;
};

class Driver {
 public:
  Driver(const Scenario& sc, uint64_t seed) : sc_(sc), rng_(seed), timers_(this) {
    TenantConfig cfg;
    cfg.id = kTenant;
    cfg.owner_loss_ms = 86'400'000;  // acks are immediate; L18 never applies here
    for (const auto& q : sc.queues) {
      QueueConfig qc;
      qc.id = q.id;
      qc.required = {SkillReq{"skill-" + q.id, 0}};  // eligibility = membership, escalation included
      qc.sla_ms = static_cast<int64_t>(q.sla_s * 1000);
      qc.escalate_after_ms = 3 * qc.sla_ms;
      qc.max_wait_ms = static_cast<int64_t>(q.max_wait_s * 1000);  // default a day: no overflow (the model has none)
      qc.max_wait_action = MaxWaitAction::kDisconnect;
      qc.max_entries = 1'000'000;
      qc.wrap_up_limit_ms = 3'600'000;
      qc.aht_ms = static_cast<int64_t>((q.talk.mean + q.wrap.mean) * 1000);
      q_index_[q.id] = static_cast<int>(cfg.queues.size());
      cfg.queues.push_back(qc);
    }
    // Configured proficiency: a speed s is skill level round(10 s), and config.routing's
    // proficiency_speed maps each level used back to its speed (no speeds: every agent at 1).
    std::map<uint32_t, double> levels;
    for (const auto& gs : sc.groups) {
      for (double sp : gs.speed) levels[static_cast<uint32_t>(std::lround(sp * 10))] = sp;
    }
    for (size_t g = 0; g < sc.groups.size(); ++g) {
      const GroupSpec& gs = sc.groups[g];
      for (int i = 0; i < gs.count; ++i) {
        AgentConfig a;
        a.id = "g" + std::to_string(g) + "-a" + std::to_string(i + 1);
        a.contact = "sip:" + a.id + "@acme.agents.example";
        a.queues = gs.queues;
        AgentSim s;
        for (size_t k = 0; k < gs.queues.size(); ++k) {
          const double sp = k < gs.speed.size() ? gs.speed[k] : 1.0;
          a.skills["skill-" + gs.queues[k]] = levels.empty() ? 1 : static_cast<uint32_t>(std::lround(sp * 10));
          s.speed[q_index_.at(gs.queues[k])] = sp;
        }
        if (gs.vip) a.skills["vip"] = 1;
        s.id = a.id;
        s.session = uuid_of(2, agents_.size() + 1);
        for (const auto& q : gs.queues) s.queues.push_back(q_index_.at(q));
        const double at = gs.login_to_s > gs.login_from_s
                              ? gs.login_from_s + (gs.login_to_s - gs.login_from_s) * i / std::max(1, gs.count - 1)
                              : gs.login_from_s;
        s.login_ms = static_cast<int64_t>(at * 1000);
        by_agent_[a.id] = static_cast<int>(agents_.size());
        agents_.push_back(s);
        cfg.agents.push_back(a);
      }
    }
    cfg.routing.version = 1;
    cfg.routing.mode = static_cast<RoutingMode>(sc.mode);
    cfg.routing.vip_skill = "vip";
    for (const auto& [level, sp] : levels) cfg.routing.proficiency_speed.emplace_back(level, sp);
    for (const auto& q : sc.queues) {
      QueuePolicy qp;
      qp.queue_id = q.id;
      qp.secondary_delay_ms = sc.secondary_delay_ms;
      qp.delay_max_ms = std::max<int64_t>(qp.delay_max_ms, sc.secondary_delay_ms);
      qp.weight = q.weight;
      qp.min_agents = q.min_agents;
      qp.max_agents = q.max_agents;
      qp.vip_reserved_min = q.vip_reserved_min;
      qp.vip_sla_ms = static_cast<int64_t>(q.vip_sla_s * 1000);
      qp.alpha = q.alpha;
      qp.beta = q.beta;
      qp.vip_alpha = q.vip_alpha;
      qp.vip_beta = q.vip_beta;
      cfg.routing.queues.push_back(qp);
    }
    if (sc.tiers) {
      for (const auto& a : cfg.agents) {
        if (a.queues.size() < 2) continue;
        RoutingPin p;
        p.agent_id = a.id;
        p.version = 1;
        for (size_t k = 0; k < a.queues.size(); ++k) p.queues.emplace_back(a.queues[k], k == 0 ? Tier::kPrimary : Tier::kSecondary);
        cfg.pins.push_back(p);
      }
    }
    if (sc.start_ms > 0) start_ms_ = sc.start_ms;
    tenant_ = std::make_unique<Tenant>(cfg, &epochs_, &sink_, &timers_, &counters_);
    if (sc.hook) tenant_->set_optimizer(true);
    stats_.resize(sc.queues.size());
    nstats_.resize(sc.queues.size());
    vstats_.resize(sc.queues.size());
    res_.overflowed.assign(sc.queues.size(), 0);
    qv_.resize(sc.queues.size());
    end_ms_ = static_cast<int64_t>((sc.warmup_s + sc.measure_s + sc.drain_s) * 1000);
    m_from_ = static_cast<int64_t>(sc.warmup_s * 1000);
    m_to_ = static_cast<int64_t>((sc.warmup_s + sc.measure_s) * 1000);
  }

  void push(int64_t t, Ev k, int a = 0, std::shared_ptr<TimerRef> tr = nullptr) {
    heap_.push(Event{t, ++seq_, k, a, std::move(tr)});
  }

  RepResult run() {
    const auto w0 = std::chrono::steady_clock::now();
    tenant_->on_ie_stream_open(kIe, 1, abs(0));
    tenant_->on_gw_stream_open(kGw, 1, abs(0));
    for (size_t i = 0; i < agents_.size(); ++i) push(agents_[i].login_ms, Ev::kLogin, static_cast<int>(i));
    for (size_t q = 0; q < sc_.queues.size(); ++q) next_arrival(static_cast<int>(q), 0);
    push(1000, Ev::kTick);
    push(sc_.check_every_s * 1000, Ev::kCheck);
    if (sc_.hook) push(static_cast<int64_t>(sc_.hook_every_s * 1000), Ev::kHook);
    while (!heap_.empty()) {
      Event e = heap_.top();
      heap_.pop();
      if (e.t > end_ms_) break;
      now_ = e.t;
      switch (e.kind) {
        case Ev::kArrival: arrival(e.a); break;
        case Ev::kRedial: arrival(e.a, /*redial=*/true); break;
        case Ev::kAbandon: abandon(e.a); break;
        case Ev::kCallEnd: call_end(e.a); break;
        case Ev::kWrapEnd: wrap_end(e.a); break;
        case Ev::kTimer: tenant_->on_timer(*e.timer, abs(now_)); break;
        case Ev::kLogin: login(e.a); break;
        case Ev::kTick: tick(); break;
        case Ev::kCheck: check(); break;
        case Ev::kHook:
          sc_.hook(tenant_.get(), abs(now_));
          push(now_ + static_cast<int64_t>(sc_.hook_every_s * 1000), Ev::kHook);
          break;
      }
      drain();
    }
    for (auto& qv : qv_) {
      for (const auto& pend : qv.pending) {
        for (const auto& [t, c] : pend) {
          if (!calls_[c].measured) continue;
          ++stats_[calls_[c].q].virtual_unresolved;
          ++cls(calls_[c]).virtual_unresolved;
        }
      }
    }
    res_.q = stats_;
    res_.normal = nstats_;
    res_.vip = vstats_;
    res_.calls = calls_.size();
    res_.offers = counters_.offers.load();
    res_.sweep_matches = counters_.sweep_matches.load();
    res_.wall_s = std::chrono::duration<double>(std::chrono::steady_clock::now() - w0).count();
    return res_;
  }

  void timer(int64_t due_ms, TimerRef t) { push(due_ms - start_ms_, Ev::kTimer, 0, std::make_shared<TimerRef>(std::move(t))); }

 private:
  struct Call {
    int q = 0;
    int64_t arrived = 0;
    std::string ix, entry, reservation, agent;
    enum class St { kWaiting, kServed, kAbandoned, kOverflowed } st = St::kWaiting;
    bool measured = false;
    bool vip = false;
    bool bot = false;  // phases 5b+5c
  };
  struct AgentSim {
    std::string id, session;
    uint64_t gen = 0;
    std::vector<int> queues;
    std::map<int, double> speed;  // per queue index
    int64_t login_ms = 0;
  };
  QueueStats& cls(const Call& c) { return c.vip ? vstats_[c.q] : nstats_[c.q]; }
  struct QV {
    std::multiset<int64_t> waiting;          // arrival times of callers waiting now
    std::multimap<int64_t, int> pending[2];  // hung-up callers whose virtual wait is unresolved, per class (1: VIP)
  };

  int64_t start_ms_ = kStartMs;
  int64_t abs(int64_t t) const { return start_ms_ + t; }

  void error(const std::string& what, const Error& e) {
    ++res_.engine_errors;
    if (res_.violations.size() < 10) res_.violations.push_back(what + ": " + e.parity + " " + e.message);
  }

  rv1::InteractionOwner owner(const Call& c, uint64_t acked = 0) {
    rv1::InteractionOwner o;
    o.set_tenant_id(kTenant);
    o.set_interaction_id(c.ix);
    o.set_owner_epoch(1);
    o.set_replica_id(kIe);
    o.set_acked_delivery_seq(acked);
    return o;
  }
  av1::SessionRef session(const AgentSim& a) {
    av1::SessionRef s;
    s.set_tenant_id(kTenant);
    s.set_agent_id(a.id);
    s.set_session_id(a.session);
    s.set_attach_generation(a.gen);
    return s;
  }
  std::string cmd() { return "sim-" + std::to_string(++cmd_); }

  void next_arrival(int q, int64_t from) {
    // Thinning against the largest bucket rate (bots included).
    const QueueSpec& s = sc_.queues[q];
    double lmax = 0;
    for (size_t b = 0; b < s.per_s.size(); ++b) lmax = std::max(lmax, s.per_s[b] + (b < s.bot_per_s.size() ? s.bot_per_s[b] : 0));
    for (size_t b = s.per_s.size(); b < s.bot_per_s.size(); ++b) lmax = std::max(lmax, s.bot_per_s[b] + s.per_s.back());
    if (lmax <= 0) return;
    double t = from / 1000.0;
    for (;;) {
      t += std::exponential_distribution<double>(lmax)(rng_);
      if (t * 1000 > end_ms_) return;
      if (std::uniform_real_distribution<double>(0, 1)(rng_) * lmax <= s.rate(t) + s.bot_rate(t)) break;
    }
    push(static_cast<int64_t>(std::llround(t * 1000)), Ev::kArrival, q);
  }

  void arrival(int q, bool redial = false) {
    if (!redial) next_arrival(q, now_);
    Call c;
    c.q = q;
    if (redial) ++res_.redials;
    {
      const double t = now_ / 1000.0, b = sc_.queues[q].bot_rate(t);
      if (!redial && b > 0 && std::uniform_real_distribution<double>(0, 1)(rng_) * (sc_.queues[q].rate(t) + b) < b) {
        c.bot = true;
        ++res_.bots;
      }
    }
    c.arrived = now_;
    c.ix = uuid_of(1, calls_.size() + 1);
    c.measured = now_ >= m_from_ && now_ < m_to_;
    c.vip = sc_.queues[q].vip_share > 0 && std::uniform_real_distribution<double>(0, 1)(rng_) < sc_.queues[q].vip_share;
    const int idx = static_cast<int>(calls_.size());
    by_ix_[c.ix] = idx;
    calls_.push_back(c);
    if (c.measured) {
      ++stats_[q].offered;
      ++cls(c).offered;
    }
    qv_[q].waiting.insert(now_);
    rv1::EnqueueRequest r;
    *r.mutable_owner() = owner(calls_[idx]);
    r.set_enqueue_seq(1);
    r.mutable_target()->set_queue_id(sc_.queues[q].id);
    r.set_first_queued_at_ms(abs(now_));
    r.set_ordering_at_ms(abs(now_));
    r.set_channel(rv1::CHANNEL_VOICE);
    r.set_reason(rv1::ENQUEUE_REASON_NEW);
    if (calls_[idx].vip) r.set_caller_class(rv1::CALLER_CLASS_VIP);
    auto out = tenant_->enqueue(r, abs(now_));
    if (!out) {
      error("enqueue", out.error());
      return;
    }
    calls_[idx].entry = out->entry().entry_id();
    const double patience = calls_[idx].bot ? sc_.queues[q].bot_patience_s : sc_.queues[q].patience.sample(rng_);
    push(now_ + static_cast<int64_t>(std::llround(patience * 1000)), Ev::kAbandon, idx);
  }

  void abandon(int idx) {
    Call& c = calls_[idx];
    if (c.st != Call::St::kWaiting) return;
    c.st = Call::St::kAbandoned;
    auto it = qv_[c.q].waiting.find(c.arrived);
    if (it != qv_[c.q].waiting.end()) qv_[c.q].waiting.erase(it);
    if (c.measured) {
      for (QueueStats* st : {&stats_[c.q], &cls(c)}) {
        ++st->abandoned;
        st->wait_all_sum += (now_ - c.arrived) / 1000.0;
        if (now_ - c.arrived < static_cast<int64_t>(sc_.short_abandon_s * 1000)) ++st->short_abandoned;
      }
      qv_[c.q].pending[c.vip ? 1 : 0].emplace(c.arrived, idx);
    }
    rv1::DequeueRequest d;
    *d.mutable_owner() = owner(c);
    d.set_entry_id(c.entry);
    d.set_reason(rv1::DEQUEUE_REASON_ABANDONED);
    auto out = tenant_->dequeue(d, abs(now_));
    if (!out) error("dequeue", out.error());
    // Phases 5b+5c: a caller (not a bot) who gave up may call again.
    const QueueSpec& qs = sc_.queues[c.q];
    if (!c.bot && qs.redial_p > 0 && std::uniform_real_distribution<double>(0, 1)(rng_) < qs.redial_p) {
      const double delay = std::exponential_distribution<double>(1.0 / std::max(1.0, qs.redial_delay_s))(rng_);
      push(now_ + static_cast<int64_t>(std::llround(delay * 1000)), Ev::kRedial, c.q);
    }
  }

  // Someone of queue q was offered an agent (`served_arrival` arrived then, of class `vip`) or an
  // agent of q is idle (served_arrival = INT64_MAX): every hung-up caller who would have been served
  // before it had this agent. Within a class the queue is FCFS; a VIP caller goes before every normal
  // one, so a normal caller served resolves every hung-up VIP caller.
  void resolve(int q, int64_t served_arrival, bool vip = false) {
    auto take = [&](std::multimap<int64_t, int>& p, int64_t bound) {
      auto end = p.lower_bound(bound);
      for (auto it = p.begin(); it != end; ++it) {
        const Call& c = calls_[it->second];
        for (QueueStats* st : {&stats_[q], &cls(c)}) {
          st->virtual_wait_sum += (now_ - c.arrived) / 1000.0;
          ++st->virtual_n;
        }
      }
      p.erase(p.begin(), end);
    };
    if (vip) {
      take(qv_[q].pending[1], served_arrival);
    } else {
      take(qv_[q].pending[1], INT64_MAX);
      take(qv_[q].pending[0], served_arrival);
    }
  }

  void on_offer(AgentSim& a, const av1::Offer& o) {
    auto it = by_ix_.find(o.interaction_id());
    if (it == by_ix_.end()) return;
    Call& c = calls_[it->second];
    if (c.st == Call::St::kWaiting) {
      c.st = Call::St::kServed;
      auto w = qv_[c.q].waiting.find(c.arrived);
      if (w != qv_[c.q].waiting.end()) qv_[c.q].waiting.erase(w);
      resolve(c.q, c.arrived, c.vip);
      const double wait = (now_ - c.arrived) / 1000.0;
      res_.max_wait_seen_s = std::max(res_.max_wait_seen_s, wait);
      if (wait > sc_.queues[c.q].max_wait_s + 1) ++res_.past_max_wait;
      if (c.measured) {
        const QueueSpec& qs = sc_.queues[c.q];
        const double tau_c = c.vip && qs.vip_sla_s > 0 ? qs.vip_sla_s : qs.sla_s;
        for (QueueStats* s : {&stats_[c.q], &cls(c)}) {
          ++s->answered;
          // The queue's overall SL uses each class's own threshold.
          if (wait <= tau_c) ++s->answered_in_sla;
          s->wait_answered_sum += wait;
          s->wait_all_sum += wait;
          s->virtual_wait_sum += wait;
          ++s->virtual_n;
        }
      }
    }
    c.agent = a.id;
    av1::AnswerOfferRequest r;
    *r.mutable_session() = session(a);
    r.set_command_id(cmd());
    r.set_offer_id(o.offer_id());
    r.set_decision(av1::DECISION_ACCEPT);
    auto out = tenant_->answer_offer(r, abs(now_));
    if (!out) error("answer_offer", out.error());
  }

  void on_assigned(const rv1::Assigned& as) {
    auto it = by_ix_.find(as.target().interaction_id());
    if (it == by_ix_.end()) return;
    const int idx = it->second;
    Call& c = calls_[idx];
    c.reservation = as.reservation_id();
    rv1::ReportOutcomeRequest r;
    *r.mutable_owner() = owner(c);
    r.set_reservation_id(c.reservation);
    r.set_outcome(rv1::OUTCOME_CONNECTED);
    auto out = tenant_->report_outcome(r, abs(now_));
    if (!out) {
      error("report_outcome", out.error());
      return;
    }
    double talk = c.bot ? sc_.queues[c.q].bot_talk_s : sc_.queues[c.q].talk.sample(rng_);
    {
      auto ag = by_agent_.find(c.agent);
      if (ag != by_agent_.end()) {
        auto sp = agents_[ag->second].speed.find(c.q);
        if (sp != agents_[ag->second].speed.end() && sp->second > 0) talk /= sp->second;
      }
    }
    push(now_ + std::max<int64_t>(1, std::llround(talk * 1000)), Ev::kCallEnd, idx);
  }

  void call_end(int idx) {
    Call& c = calls_[idx];
    rv1::CallEndedRequest r;
    *r.mutable_owner() = owner(c);
    r.set_agent_id(c.agent);
    r.set_reservation_id(c.reservation);
    r.set_ended_at_ms(abs(now_));
    auto out = tenant_->call_ended(r, abs(now_));
    if (!out) {
      error("call_ended", out.error());
      return;
    }
    const double wrap = sc_.queues[c.q].wrap.sample(rng_);
    push(now_ + std::max<int64_t>(0, std::llround(wrap * 1000)), Ev::kWrapEnd, idx);
  }

  void wrap_end(int idx) {
    Call& c = calls_[idx];
    AgentSim& a = agents_[by_agent_.at(c.agent)];
    av1::EndWrapUpRequest r;
    *r.mutable_session() = session(a);
    r.set_command_id(cmd());
    r.set_reservation_id(c.reservation);
    r.set_disposition_code("done");
    auto out = tenant_->end_wrap_up(r, abs(now_));
    if (!out) {
      error("end_wrap_up", out.error());
      return;
    }
    if (out->agent().state() == av1::AGENT_STATE_READY) idle(a);
  }

  // The queue's max wait passed (DISCONNECT): the caller leaves; IE ends the entry (L13).
  void on_overflow(int idx) {
    Call& c = calls_[idx];
    if (c.st != Call::St::kWaiting) return;
    c.st = Call::St::kOverflowed;
    auto w = qv_[c.q].waiting.find(c.arrived);
    if (w != qv_[c.q].waiting.end()) qv_[c.q].waiting.erase(w);
    const double wait = (now_ - c.arrived) / 1000.0;
    res_.max_wait_seen_s = std::max(res_.max_wait_seen_s, wait);
    if (wait > sc_.queues[c.q].max_wait_s + 1) ++res_.past_max_wait;
    if (c.measured) ++res_.overflowed[c.q];
    rv1::DequeueRequest d;
    *d.mutable_owner() = owner(c);
    d.set_entry_id(c.entry);
    d.set_reason(rv1::DEQUEUE_REASON_OVERFLOW);
    auto out = tenant_->dequeue(d, abs(now_));
    if (!out) error("dequeue overflow", out.error());
  }

  // The agent found nothing to take: none of its queues has a caller it could serve.
  void idle(const AgentSim& a) {
    for (int q : a.queues) {
      if (qv_[q].waiting.empty()) resolve(q, INT64_MAX);
    }
  }

  void login(int i) {
    AgentSim& a = agents_[i];
    av1::AttachAgentRequest r;
    r.set_tenant_id(kTenant);
    r.set_agent_id(a.id);
    r.set_replica_id(kGw);
    r.set_session_id(a.session);
    r.set_agent_contact("sip:" + a.id + "@acme.agents.example");
    auto out = tenant_->attach(r, abs(now_));
    if (!out) {
      error("attach", out.error());
      return;
    }
    a.gen = out->attach_generation();
    av1::SetStateRequest s;
    *s.mutable_session() = session(a);
    s.set_command_id(cmd());
    s.set_target(av1::TARGET_STATE_READY);
    auto st = tenant_->set_state(s, abs(now_));
    if (!st) {
      error("set_state", st.error());
      return;
    }
    if (st->agent().state() == av1::AGENT_STATE_READY) idle(a);
  }

  void tick() {
    ++ticks_;
    tenant_->sweep(abs(now_));
    tenant_->redeliver(abs(now_));
    tenant_->reap_orphans(abs(now_));
    if (ticks_ % 60 == 0) tenant_->prune(abs(now_));
    push(now_ + 1000, Ev::kTick);
  }

  void check() {
    for (size_t q = 0; q < qv_.size(); ++q) {
      for (int64_t at : qv_[q].waiting) {
        if (now_ - at > static_cast<int64_t>((sc_.queues[q].max_wait_s + 2) * 1000)) ++res_.past_max_wait;
      }
    }
    uint64_t n = 0;
    const auto v = tenant_->check_invariants(abs(now_), &n);
    res_.invariant_checks += n;
    res_.invariant_violations += v.size();
    for (const auto& s : v) {
      if (res_.violations.size() < 10) res_.violations.push_back(s);
    }
    push(now_ + sc_.check_every_s * 1000, Ev::kCheck);
  }

  // Everything the engine sent during the last call, in order: acknowledged at once, reacted to.
  void drain() {
    for (int round = 0; round < 100000 && !sink_.out.empty(); ++round) {
      std::vector<Msg> batch;
      batch.swap(sink_.out);
      for (auto& m : batch) {
        if (auto* ie = std::get_if<rv1::WatchResponse>(&m.m)) {
          if (ie->delivery_seq() == 0) continue;  // heartbeats and positions
          std::string ix;
          if (ie->has_assigned()) ix = ie->assigned().target().interaction_id();
          else if (ie->has_wrap_up_ended()) ix = ie->wrap_up_ended().target().interaction_id();
          else if (ie->has_revoke()) ix = ie->revoke().target().interaction_id();
          else if (ie->has_overflow()) ix = ie->overflow().target().interaction_id();
          auto it = by_ix_.find(ix);
          if (it != by_ix_.end()) tenant_->ack(owner(calls_[it->second], ie->delivery_seq()), abs(now_));
          if (ie->has_assigned()) on_assigned(ie->assigned());
          if (ie->has_overflow() && it != by_ix_.end()) on_overflow(it->second);
          if (ie->has_revoke()) ++res_.engine_errors;  // not expected here
        } else {
          auto& gw = std::get<av1::WatchResponse>(m.m);
          av1::Ack k;
          k.set_tenant_id(gw.tenant_id());
          k.set_agent_id(gw.agent_id());
          k.set_attach_generation(gw.attach_generation());
          k.set_delivery_seq(gw.delivery_seq());
          tenant_->gw_ack(k);
          if (gw.has_offer()) on_offer(agents_[by_agent_.at(gw.agent_id())], gw.offer());
        }
      }
    }
  }

  const Scenario& sc_;
  std::mt19937_64 rng_;
  Epochs epochs_{1, 0};
  SimSink sink_;
  SimTimers timers_;
  Counters counters_;
  std::unique_ptr<Tenant> tenant_;
  std::priority_queue<Event, std::vector<Event>, std::greater<>> heap_;
  uint64_t seq_ = 0, cmd_ = 0, ticks_ = 0;
  int64_t now_ = 0, end_ms_ = 0, m_from_ = 0, m_to_ = 0;
  std::map<std::string, int> q_index_;
  std::vector<Call> calls_;
  std::unordered_map<std::string, int> by_ix_, by_agent_;
  std::vector<AgentSim> agents_;
  std::vector<QV> qv_;
  std::vector<QueueStats> stats_, nstats_, vstats_;
  RepResult res_;
};

void SimTimers::at(int64_t due_ms, TimerRef t) { d_->timer(due_ms, std::move(t)); }

}  // namespace

RepResult run_replication(const Scenario& sc, uint64_t seed) {
  Driver d(sc, seed);
  return d.run();
}

}  // namespace pcr::sim
