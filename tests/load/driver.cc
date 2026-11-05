// Phase 7 load driver (PLAN §7 "Load", §8 row 7): many simulated gateway replicas (agents.v1) and IE
// replicas (routing.v1) driving ONE real RE process over gRPC in real time (no test clock), the
// way the gateway and IE do: attach + SetState READY per agent (paced like the gateway's 500/s
// limiter), Enqueue per call, accept every offer, report CONNECTED, end the call after its talk
// time, end wrap-up, acknowledge every stream message (batched, as both contracts allow).
//
// It measures what the load targets name (tests/load/run.sh judges them with RE's own metrics):
//   - AttachAgent latency (send -> answer), and SetState READY latency, during a shift-start storm;
//   - time to offer: Enqueue sent -> the Offer on the agent's gateway stream (RPC -> stream),
//     split into before / during / after the storm (the storm must not move it);
//   - Enqueue latency, RPC refusals by code, offers that expired, calls never offered;
// and what it can check from outside: never two live offers for one agent or one call; every call
// connects, overflows, or is still held by RE at the end (Reattach); none is lost.
//   load_driver <config.json> <summary.json>
#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "parity/agents/v1/agents.grpc.pb.h"
#include "parity/routing/v1/routing.grpc.pb.h"

namespace rv1 = ::parity::routing::v1;
namespace av1 = ::parity::agents::v1;
using json = nlohmann::json;
using SteadyClock = std::chrono::steady_clock;

namespace {

int64_t wall_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string uuid7(std::mt19937_64& rng) {
  const uint64_t ms = static_cast<uint64_t>(wall_ms());
  const uint64_t a = rng(), b = rng();
  char buf[64];
  std::snprintf(buf, sizeof buf, "%08llx-%04llx-7%03llx-%04llx-%012llx", static_cast<unsigned long long>((ms >> 16) & 0xffffffffULL),
                static_cast<unsigned long long>(ms & 0xffff), static_cast<unsigned long long>(a & 0xfff),
                static_cast<unsigned long long>(0x8000 | ((a >> 12) & 0x3fff)),
                static_cast<unsigned long long>(b & 0xffffffffffffULL));
  return buf;
}

struct Agent {
  std::string tenant, id, session;
  int replica = 0;
  uint64_t gen = 0;
  bool attached = false, ready_once = false;
  std::string live_offer, offer_ix;
  int64_t offer_expires = 0;
};

struct Call {
  std::string tenant, queue, ix, entry, res, connected_res, agent;
  int replica = 0;
  int64_t enq_us = 0;  // steady clock, when Enqueue was sent
  bool offered = false, connected = false, ended = false, wrapped = false, overflowed = false, refused = false;
  uint64_t seen_seq = 0;
};

struct Sample {
  int64_t t_ms;  // run time when the operation started
  int64_t us;
};

struct Tenant {
  std::string id;
  std::vector<std::string> queues;
  std::vector<std::string> agents;
  double weight = 0;  // share of calls (its agent count)
};

class Driver {
 public:
  json cfg;
  std::string bin;
  std::vector<std::shared_ptr<grpc::Channel>> chans;
  std::vector<std::unique_ptr<rv1::RoutingService::Stub>> rs;
  std::vector<std::unique_ptr<av1::AgentService::Stub>> as;
  std::atomic<uint64_t> rr{0};
  std::vector<Tenant> tenants;
  std::discrete_distribution<size_t> tenant_pick;
  int gw_replicas = 4, ie_replicas = 2;
  SteadyClock::time_point t0 = SteadyClock::now();
  std::mutex rng_mu;
  std::mt19937_64 rng{std::random_device{}()};

  std::mutex mu;  // agents, calls, samples, violations
  std::unordered_map<std::string, Agent> agents;  // tenant/agent
  std::unordered_map<std::string, Call> calls;    // interaction id
  std::unordered_map<std::string, std::string> live_offer_by_ix;
  std::vector<Sample> attach_lat, ready_lat, enqueue_lat, tto, accept_lat;
  std::vector<std::string> violations;
  std::map<std::string, uint64_t> refusals;  // rpc:code -> count
  uint64_t stream_msgs = 0, replays_dropped = 0, offers_seen = 0, offers_expired_seen = 0, detached_seen = 0;
  uint64_t calls_started = 0;
  std::atomic<int64_t> connected_now{0}, waiting_now{0};
  std::atomic<bool> stop{false}, arrivals_on{true};
  int64_t storm_from_ms = -1, storm_to_ms = -1;
  // Step-up runs: named windows (storms at rising attach rates, call steps at rising call rates).
  struct Win {
    std::string name;
    int64_t from_ms, to_ms;
    double target_rate;
  };
  std::mutex wmu;
  std::vector<Win> wins;

  // ---- scheduling: a timer heap feeding a pool of workers ----
  struct Job {
    int64_t due_us;
    uint64_t seq;
    std::function<void()> fn;
    bool operator>(const Job& o) const { return due_us != o.due_us ? due_us > o.due_us : seq > o.seq; }
  };
  std::mutex jmu;
  std::condition_variable jcv;
  std::priority_queue<Job, std::vector<Job>, std::greater<>> timers;
  uint64_t jseq = 0;
  std::mutex rmu;
  std::condition_variable rcv;
  std::deque<std::function<void()>> ready;
  std::atomic<int64_t> backlog{0}, backlog_max{0};

  int64_t now_us() const { return std::chrono::duration_cast<std::chrono::microseconds>(SteadyClock::now() - t0).count(); }
  int64_t now_ms() const { return now_us() / 1000; }

  void later(int64_t delay_ms, std::function<void()> fn) {
    if (delay_ms <= 0) {
      run_now(std::move(fn));
      return;
    }
    {
      std::lock_guard lk(jmu);
      timers.push(Job{now_us() + delay_ms * 1000, ++jseq, std::move(fn)});
    }
    jcv.notify_one();
  }
  void run_now(std::function<void()> fn) {
    {
      std::lock_guard lk(rmu);
      ready.push_back(std::move(fn));
      const int64_t b = static_cast<int64_t>(ready.size());
      if (b > backlog_max.load()) backlog_max = b;
    }
    rcv.notify_one();
  }
  void timer_loop() {
    std::unique_lock lk(jmu);
    while (!stop.load()) {
      if (timers.empty()) {
        jcv.wait_for(lk, std::chrono::milliseconds(50));
        continue;
      }
      const int64_t due = timers.top().due_us, n = now_us();
      if (due > n) {
        jcv.wait_for(lk, std::chrono::microseconds(std::min<int64_t>(due - n, 50'000)));
        continue;
      }
      auto fn = timers.top().fn;
      timers.pop();
      lk.unlock();
      run_now(std::move(fn));
      lk.lock();
    }
  }
  void worker() {
    for (;;) {
      std::function<void()> fn;
      {
        std::unique_lock lk(rmu);
        rcv.wait(lk, [&] { return !ready.empty() || stop.load(); });
        if (ready.empty()) return;
        fn = std::move(ready.front());
        ready.pop_front();
      }
      fn();
    }
  }

  int64_t uniform(int64_t lo, int64_t hi) {
    std::lock_guard lk(rng_mu);
    return hi <= lo ? lo : lo + static_cast<int64_t>(rng() % static_cast<uint64_t>(hi - lo + 1));
  }
  std::string new_id() {
    std::lock_guard lk(rng_mu);
    return uuid7(rng);
  }

  static std::string trailer(const grpc::ClientContext& c, const char* key) {
    const auto& m = c.GetServerTrailingMetadata();
    auto it = m.find(key);
    return it == m.end() ? std::string() : std::string(it->second.data(), it->second.size());
  }

  // One unary RPC; UNAVAILABLE (RE's inbox limits) is retried with backoff and jitter within the budget.
  template <class F>
  grpc::Status call(const char* what, const std::string& tenant, F f, std::string* parity = nullptr, int64_t budget_ms = 20'000) {
    const int64_t until = now_ms() + budget_ms;
    for (int attempt = 0;; ++attempt) {
      grpc::ClientContext c;
      c.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(5000));
      if (!tenant.empty()) c.AddMetadata("tenant-id", tenant);
      const size_t k = rr.fetch_add(1) % chans.size();
      const grpc::Status s = f(k, &c);
      const std::string pe = trailer(c, "parity-error");
      if (parity) *parity = pe;
      if (s.ok()) return s;
      {
        std::lock_guard lk(mu);
        ++refusals[std::string(what) + ":" + (pe.empty() ? std::to_string(static_cast<int>(s.error_code())) : pe)];
      }
      const bool retry = s.error_code() == grpc::StatusCode::UNAVAILABLE || s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED;
      if (!retry || now_ms() >= until) return s;
      std::this_thread::sleep_for(std::chrono::milliseconds(std::min<int64_t>(500, (20LL << std::min(attempt, 4)) + uniform(0, 20))));
    }
  }

  // ---- acknowledgements, batched per replica every 10 ms ----
  std::mutex amu;
  std::vector<std::vector<av1::Ack>> gw_acks;
  std::vector<rv1::InteractionOwner> ie_acks;
  void ack_loop() {
    while (!stop.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      std::vector<std::vector<av1::Ack>> g;
      std::vector<rv1::InteractionOwner> ie;
      {
        std::lock_guard lk(amu);
        g.swap(gw_acks);
        gw_acks.resize(static_cast<size_t>(gw_replicas));
        ie.swap(ie_acks);
      }
      for (size_t r = 0; r < g.size(); ++r) {
        if (g[r].empty()) continue;
        auto q = std::make_shared<av1::AcknowledgeRequest>();
        q->set_replica_id("gw-load-" + std::to_string(r));
        for (auto& a : g[r]) *q->add_acks() = std::move(a);
        run_now([this, q] {
          av1::AcknowledgeResponse resp;
          (void)call("AgentsAcknowledge", "", [&](size_t k, grpc::ClientContext* c) { return as[k]->Acknowledge(c, *q, &resp); });
        });
      }
      for (size_t i = 0; i < ie.size(); i += 500) {
        auto q = std::make_shared<rv1::AcknowledgeRequest>();
        for (size_t j = i; j < std::min(ie.size(), i + 500); ++j) *q->add_acks() = ie[j];
        run_now([this, q] {
          rv1::AcknowledgeResponse resp;
          (void)call("Acknowledge", "", [&](size_t k, grpc::ClientContext* c) { return rs[k]->Acknowledge(c, *q, &resp); });
        });
      }
    }
  }

  rv1::InteractionOwner owner(const Call& c, uint64_t acked) const {
    rv1::InteractionOwner o;
    o.set_tenant_id(c.tenant);
    o.set_interaction_id(c.ix);
    o.set_owner_epoch(1);
    o.set_replica_id("ie-load-" + std::to_string(c.replica));
    o.set_acked_delivery_seq(acked);
    return o;
  }
  av1::SessionRef session(const Agent& a) const {
    av1::SessionRef s;
    s.set_tenant_id(a.tenant);
    s.set_agent_id(a.id);
    s.set_session_id(a.session);
    s.set_attach_generation(a.gen);
    return s;
  }
  static std::string key(const std::string& tenant, const std::string& agent) { return tenant + "/" + agent; }

  // ---- agents ----
  void attach(const std::string& k, bool then_ready) {
    av1::AttachAgentRequest q;
    std::string tenant;
    {
      std::lock_guard lk(mu);
      Agent& a = agents.at(k);
      tenant = a.tenant;
      q.set_tenant_id(a.tenant);
      q.set_agent_id(a.id);
      q.set_replica_id("gw-load-" + std::to_string(a.replica));
      q.set_session_id(a.session);
      q.set_agent_contact("sip:" + a.id + "@" + a.tenant + ".agents.example");
    }
    av1::AttachAgentResponse r;
    const int64_t t = now_ms(), u0 = now_us();
    const auto s = call("AttachAgent", tenant, [&](size_t i, grpc::ClientContext* c) { return as[i]->AttachAgent(c, q, &r); });
    const int64_t us = now_us() - u0;
    av1::AgentState st = av1::AGENT_STATE_UNSPECIFIED;
    {
      std::lock_guard lk(mu);
      attach_lat.push_back({t, us});
      if (!s.ok()) return;
      Agent& a = agents.at(k);
      a.gen = r.attach_generation();
      a.attached = true;
      st = r.agent().state();
    }
    for (const auto& res : r.reservations()) {
      if (res.wrap_up()) later(200, [this, k, rid = res.reservation_id()] { end_wrap(k, rid); });
    }
    if (then_ready && (st == av1::AGENT_STATE_NOT_READY || st == av1::AGENT_STATE_LOGGED_OUT)) set_ready(k);
  }

  void set_ready(const std::string& k) {
    av1::SetStateRequest q;
    std::string tenant;
    {
      std::lock_guard lk(mu);
      const Agent& a = agents.at(k);
      tenant = a.tenant;
      *q.mutable_session() = session(a);
    }
    q.set_command_id(new_id());
    q.set_target(av1::TARGET_STATE_READY);
    av1::SetStateResponse r;
    std::string pe;
    const int64_t t = now_ms(), u0 = now_us();
    const auto s = call("SetState", tenant, [&](size_t i, grpc::ClientContext* c) { return as[i]->SetState(c, q, &r); }, &pe);
    std::lock_guard lk(mu);
    ready_lat.push_back({t, now_us() - u0});
    if (s.ok()) agents.at(k).ready_once = true;
    if (!s.ok() && pe == "session_superseded") later(100, [this, k] { attach(k, true); });
  }

  // Under mu.
  void open_offer(Agent& a, const std::string& oid, const std::string& ix, int64_t expires, int64_t rx_us) {
    if (a.live_offer == oid) return;
    const int64_t w = wall_ms();
    if (!a.live_offer.empty() && a.offer_expires > w - 1000) {
      violations.push_back("double offer to agent " + a.id + ": " + a.live_offer + " still live, then " + oid);
    }
    auto other = live_offer_by_ix.find(ix);
    if (other != live_offer_by_ix.end() && other->second != key(a.tenant, a.id)) {
      auto b = agents.find(other->second);
      if (b != agents.end() && b->second.offer_ix == ix && !b->second.live_offer.empty() && b->second.offer_expires > w - 1000) {
        violations.push_back("double offer for call " + ix + ": agents " + b->second.id + " and " + a.id);
      }
    }
    a.live_offer = oid;
    a.offer_ix = ix;
    a.offer_expires = expires;
    live_offer_by_ix[ix] = key(a.tenant, a.id);
    auto c = calls.find(ix);
    if (c != calls.end() && !c->second.offered) {
      c->second.offered = true;
      waiting_now.fetch_sub(1);
      tto.push_back({c->second.enq_us / 1000, rx_us - c->second.enq_us});
    }
  }
  // Under mu.
  void close_offer(Agent& a, const std::string& oid) {
    if (a.live_offer != oid) return;
    auto it = live_offer_by_ix.find(a.offer_ix);
    if (it != live_offer_by_ix.end() && it->second == key(a.tenant, a.id)) live_offer_by_ix.erase(it);
    a.live_offer.clear();
    a.offer_ix.clear();
  }

  void answer(const std::string& k, const std::string& oid) {
    av1::AnswerOfferRequest q;
    std::string tenant;
    {
      std::lock_guard lk(mu);
      Agent& a = agents.at(k);
      if (a.live_offer != oid) return;
      tenant = a.tenant;
      *q.mutable_session() = session(a);
    }
    q.set_command_id(new_id());
    q.set_offer_id(oid);
    q.set_decision(av1::DECISION_ACCEPT);
    av1::AnswerOfferResponse r;
    std::string pe;
    const int64_t t = now_ms(), u0 = now_us();
    const auto s = call("AnswerOffer", tenant, [&](size_t i, grpc::ClientContext* c) { return as[i]->AnswerOffer(c, q, &r); }, &pe);
    std::lock_guard lk(mu);
    accept_lat.push_back({t, now_us() - u0});
    close_offer(agents.at(k), oid);
    if (!s.ok() && pe == "session_superseded") later(100, [this, k] { attach(k, true); });
  }

  void end_wrap(const std::string& k, const std::string& rid) {
    av1::EndWrapUpRequest q;
    std::string tenant;
    {
      std::lock_guard lk(mu);
      const Agent& a = agents.at(k);
      tenant = a.tenant;
      *q.mutable_session() = session(a);
    }
    q.set_command_id(new_id());
    q.set_reservation_id(rid);
    q.set_disposition_code("done");
    av1::EndWrapUpResponse r;
    (void)call("EndWrapUp", tenant, [&](size_t i, grpc::ClientContext* c) { return as[i]->EndWrapUp(c, q, &r); });
  }

  // ---- calls ----
  void enqueue() {
    Call c;
    {
      std::lock_guard lk(rng_mu);
      const Tenant& t = tenants[tenant_pick(rng)];
      c.tenant = t.id;
      c.queue = t.queues[rng() % t.queues.size()];
      c.ix = uuid7(rng);
      c.replica = static_cast<int>(rng() % static_cast<uint64_t>(ie_replicas));
    }
    rv1::EnqueueRequest q;
    *q.mutable_owner() = owner(c, 0);
    q.set_enqueue_seq(1);
    q.mutable_target()->set_queue_id(c.queue);
    const int64_t w = wall_ms();
    q.set_first_queued_at_ms(w);
    q.set_ordering_at_ms(w);
    q.set_channel(rv1::CHANNEL_VOICE);
    q.set_reason(rv1::ENQUEUE_REASON_NEW);
    const int64_t t = now_ms();
    c.enq_us = now_us();
    const std::string ix = c.ix, tenant = c.tenant;
    {
      std::lock_guard lk(mu);
      calls.emplace(ix, std::move(c));
      ++calls_started;
      waiting_now.fetch_add(1);
    }
    rv1::EnqueueResponse r;
    std::string pe;
    const int64_t u0 = now_us();
    const auto s = call("Enqueue", tenant, [&](size_t i, grpc::ClientContext* cx) { return rs[i]->Enqueue(cx, q, &r); }, &pe);
    std::lock_guard lk(mu);
    enqueue_lat.push_back({t, now_us() - u0});
    Call& x = calls.at(ix);
    if (s.ok()) {
      x.entry = r.entry().entry_id();
    } else {
      x.refused = true;  // queue_full or a refusal: IE would run its overflow action
      if (!x.offered) waiting_now.fetch_sub(1);
    }
  }

  void report_connected(const std::string& ix, const std::string& rid) {
    rv1::ReportOutcomeRequest q;
    std::string tenant;
    {
      std::lock_guard lk(mu);
      Call& c = calls.at(ix);
      if (c.res != rid || c.connected) return;
      tenant = c.tenant;
      *q.mutable_owner() = owner(c, c.seen_seq);
    }
    q.set_reservation_id(rid);
    q.set_outcome(rv1::OUTCOME_CONNECTED);
    rv1::ReportOutcomeResponse r;
    std::string pe;
    const auto s = call("ReportOutcome", tenant, [&](size_t i, grpc::ClientContext* cx) { return rs[i]->ReportOutcome(cx, q, &r); }, &pe);
    std::lock_guard lk(mu);
    Call& c = calls.at(ix);
    if (!s.ok()) {
      if (pe == "reservation_unknown") violations.push_back("lost reservation " + rid + " of " + ix);
      return;
    }
    if (r.recorded() == rv1::OUTCOME_CONNECTED) {
      if (c.connected && c.connected_res != rid) violations.push_back("call " + ix + " connected twice");
      if (!c.connected) connected_now.fetch_add(1);
      c.connected = true;
      c.connected_res = rid;
      const auto& tk = cfg.at("calls").value("talk_s", json::array({100, 140}));
      const int64_t talk = uniform(tk[0].get<int64_t>() * 1000, tk[1].get<int64_t>() * 1000);
      later(talk, [this, ix, rid] { call_ended(ix, rid); });
    } else if (c.res == rid) {
      c.res.clear();
    }
  }

  void call_ended(const std::string& ix, const std::string& rid) {
    rv1::CallEndedRequest q;
    std::string tenant;
    {
      std::lock_guard lk(mu);
      Call& c = calls.at(ix);
      if (c.ended) return;
      c.ended = true;
      connected_now.fetch_sub(1);
      tenant = c.tenant;
      *q.mutable_owner() = owner(c, c.seen_seq);
      q.set_agent_id(c.agent);
    }
    q.set_reservation_id(rid);
    q.set_ended_at_ms(wall_ms());
    rv1::CallEndedResponse r;
    std::string pe;
    const auto s = call("CallEnded", tenant, [&](size_t i, grpc::ClientContext* cx) { return rs[i]->CallEnded(cx, q, &r); }, &pe);
    if (!s.ok() && pe == "reservation_unknown") {
      std::lock_guard lk(mu);
      violations.push_back("lost reservation " + rid + " of " + ix + " (CallEnded)");
    }
  }

  // ---- streams ----
  void ie_message(const rv1::WatchResponse& m, int replica) {
    if (m.has_heartbeat() || m.has_position()) return;
    std::string ix;
    if (m.has_assigned()) ix = m.assigned().target().interaction_id();
    if (m.has_revoke()) ix = m.revoke().target().interaction_id();
    if (m.has_overflow()) ix = m.overflow().target().interaction_id();
    if (m.has_wrap_up_ended()) ix = m.wrap_up_ended().target().interaction_id();
    if (ix.empty()) return;
    const uint64_t seq = m.delivery_seq();
    rv1::InteractionOwner ack;
    {
      std::lock_guard lk(mu);
      ++stream_msgs;
      auto it = calls.find(ix);
      if (it == calls.end()) return;
      Call& c = it->second;
      if (seq != 0 && seq <= c.seen_seq) {
        ++replays_dropped;
      } else {
        if (seq != 0) c.seen_seq = seq;
        if (m.has_assigned()) {
          if (!c.connected) {
            c.res = m.assigned().reservation_id();
            c.agent = m.assigned().agent_id();
            later(cfg.at("calls").value("connect_ms", 50), [this, ix, rid = c.res] { report_connected(ix, rid); });
          } else if (m.assigned().reservation_id() != c.connected_res) {
            violations.push_back("call " + ix + " assigned again after it connected");
          }
        } else if (m.has_revoke()) {
          if (c.res == m.revoke().reservation_id() && !c.connected) c.res.clear();
        } else if (m.has_overflow()) {
          if (!c.overflowed && !c.offered) waiting_now.fetch_sub(1);
          c.overflowed = true;
        } else if (m.has_wrap_up_ended()) {
          if (m.wrap_up_ended().reservation_id() == c.connected_res) c.wrapped = true;
        }
      }
      if (seq == 0) return;
      ack = owner(c, seq);
    }
    (void)replica;
    std::lock_guard lk(amu);
    ie_acks.push_back(std::move(ack));
  }

  void gw_message(const av1::WatchResponse& m, int replica, int64_t rx_us) {
    if (m.has_heartbeat()) return;
    const std::string k = key(m.tenant_id(), m.agent_id());
    const uint64_t seq = m.delivery_seq();
    {
      std::lock_guard lk(mu);
      ++stream_msgs;
      auto ait = agents.find(k);
      if (ait == agents.end()) return;
      Agent& a = ait->second;
      if (m.attach_generation() >= a.gen) {
        if (m.has_offer()) {
          ++offers_seen;
          open_offer(a, m.offer().offer_id(), m.offer().interaction_id(), m.offer().expires_at_ms(), rx_us);
          const auto& am = cfg.at("calls").value("answer_ms", json::array({50, 250}));
          later(uniform(am[0].get<int64_t>(), am[1].get<int64_t>()), [this, k, oid = m.offer().offer_id()] { answer(k, oid); });
        } else if (m.has_offer_revoked()) {
          close_offer(a, m.offer_revoked().offer_id());
          if (m.offer_revoked().reason() == av1::REVOKE_REASON_EXPIRED) {
            ++offers_expired_seen;
            if (!stop.load()) later(200, [this, k] { set_ready(k); });
          }
        } else if (m.has_reserved()) {
          if (!a.live_offer.empty() && a.offer_ix == m.reserved().interaction_id()) close_offer(a, a.live_offer);
        } else if (m.has_wrap_up_started()) {
          later(cfg.at("calls").value("wrap_ms", 500), [this, k, rid = m.wrap_up_started().reservation_id()] { end_wrap(k, rid); });
        } else if (m.has_detached()) {
          ++detached_seen;
          a.attached = false;
          if (!stop.load()) later(200, [this, k] { attach(k, true); });
        } else if (m.has_state_changed()) {
          const auto st = m.state_changed().agent().state();
          if (st == av1::AGENT_STATE_NOT_READY && !stop.load() && a.ready_once) later(300, [this, k] { set_ready(k); });
        }
      }
    }
    if (seq == 0) return;
    av1::Ack ack;
    ack.set_tenant_id(m.tenant_id());
    ack.set_agent_id(m.agent_id());
    ack.set_attach_generation(m.attach_generation());
    ack.set_delivery_seq(seq);
    std::lock_guard lk(amu);
    gw_acks[static_cast<size_t>(replica)].push_back(std::move(ack));
  }

  template <bool kIe>
  void watch_loop(int replica) {
    auto ch = grpc::CreateChannel(cfg.at("target").get<std::string>(), grpc::InsecureChannelCredentials());
    auto rstub = rv1::RoutingService::NewStub(ch);
    auto astub = av1::AgentService::NewStub(ch);
    while (!stop.load()) {
      grpc::ClientContext c;
      if constexpr (kIe) {
        rv1::WatchRequest q;
        q.set_replica_id("ie-load-" + std::to_string(replica));
        q.set_bin(bin);
        auto rd = rstub->Watch(&c, q);
        rv1::WatchResponse m;
        while (!stop.load() && rd->Read(&m)) ie_message(m, replica);
        c.TryCancel();
        (void)rd->Finish();
      } else {
        av1::WatchRequest q;
        q.set_replica_id("gw-load-" + std::to_string(replica));
        q.set_bin(bin);
        auto rd = astub->Watch(&c, q);
        av1::WatchResponse m;
        while (!stop.load() && rd->Read(&m)) gw_message(m, replica, now_us());
        c.TryCancel();
        (void)rd->Finish();
      }
      if (stop.load()) return;
      {
        std::lock_guard lk(mu);
        ++refusals[std::string(kIe ? "Watch" : "AgentsWatch") + ":closed"];
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  // Attaches `ks` at `rate` per second (the gateway's limiter), each followed by SetState READY.
  void attach_paced(const std::vector<std::string>& ks, double rate) {
    const int64_t start = now_us();
    for (size_t i = 0; i < ks.size() && !stop.load(); ++i) {
      const int64_t due = start + static_cast<int64_t>(1e6 * static_cast<double>(i) / rate);
      const int64_t n = now_us();
      if (due > n) std::this_thread::sleep_for(std::chrono::microseconds(due - n));
      run_now([this, k = ks[i]] { attach(k, true); });
    }
  }

  static json pct(std::vector<int64_t> v) {
    if (v.empty()) return {{"n", 0}};
    std::sort(v.begin(), v.end());
    auto at = [&](double p) { return v[std::min(v.size() - 1, static_cast<size_t>(p * static_cast<double>(v.size())))] / 1000.0; };
    return {{"n", v.size()}, {"p50_ms", at(0.50)}, {"p99_ms", at(0.99)}, {"max_ms", v.back() / 1000.0}};
  }
  static json window(const std::vector<Sample>& s, int64_t from_ms, int64_t to_ms) {
    std::vector<int64_t> v;
    for (const auto& x : s) {
      if (x.t_ms >= from_ms && x.t_ms < to_ms) v.push_back(x.us);
    }
    return pct(std::move(v));
  }

  json run() {
    bin = cfg.value("bin", std::string("bin-a"));
    gw_replicas = cfg.value("gw_replicas", 4);
    ie_replicas = cfg.value("ie_replicas", 2);
    {
      std::ifstream in(cfg.at("seed").get<std::string>());
      const json seed = json::parse(in);
      std::vector<double> w;
      for (const auto& t : seed.at("tenants")) {
        if (t.value("bin", bin) != bin) continue;
        Tenant x;
        x.id = t.at("id").get<std::string>();
        for (const auto& q : t.at("queues")) x.queues.push_back(q.at("id").get<std::string>());
        for (const auto& a : t.at("agents")) x.agents.push_back(a.at("id").get<std::string>());
        x.weight = static_cast<double>(std::max<size_t>(1, x.agents.size()));
        w.push_back(x.weight);
        tenants.push_back(std::move(x));
      }
      tenant_pick = std::discrete_distribution<size_t>(w.begin(), w.end());
    }
    // Every agent, interleaved across tenants and queues so a partial login covers every queue.
    std::vector<std::string> order;
    {
      size_t most = 0;
      for (const auto& t : tenants) most = std::max(most, t.agents.size());
      for (size_t i = 0; i < most; ++i) {
        for (const auto& t : tenants) {
          if (i >= t.agents.size()) continue;
          const std::string k = key(t.id, t.agents[i]);
          Agent a;
          a.tenant = t.id;
          a.id = t.agents[i];
          a.session = uuid7(rng);
          a.replica = static_cast<int>(std::hash<std::string>{}(k) % static_cast<size_t>(gw_replicas));
          agents.emplace(k, std::move(a));
          order.push_back(k);
        }
      }
      // Within a tenant, seeds list agents queue group by group: stride through them so the first
      // logins spread over the queues.
      const size_t stride = static_cast<size_t>(cfg.value("login_stride", 97));
      std::vector<std::string> spread;
      spread.reserve(order.size());
      for (size_t s = 0; s < stride; ++s) {
        for (size_t i = s; i < order.size(); i += stride) spread.push_back(order[i]);
      }
      order.swap(spread);
    }
    const int nch = cfg.value("channels", 8);
    for (int i = 0; i < nch; ++i) {
      grpc::ChannelArguments args;
      args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);  // a connection per channel
      auto ch = grpc::CreateCustomChannel(cfg.at("target").get<std::string>(), grpc::InsecureChannelCredentials(), args);
      chans.push_back(ch);
      rs.push_back(rv1::RoutingService::NewStub(ch));
      as.push_back(av1::AgentService::NewStub(ch));
    }
    gw_acks.resize(static_cast<size_t>(gw_replicas));
    std::vector<std::thread> threads;
    threads.emplace_back([this] { timer_loop(); });
    const int nw = cfg.value("workers", 128);
    for (int i = 0; i < nw; ++i) threads.emplace_back([this] { worker(); });
    threads.emplace_back([this] { ack_loop(); });
    for (int r = 0; r < gw_replicas; ++r) threads.emplace_back([this, r] { watch_loop<false>(r); });
    for (int r = 0; r < ie_replicas; ++r) threads.emplace_back([this, r] { watch_loop<true>(r); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 1. Agents logged in before the run (fast; not measured against the storm's targets).
    const json pre = cfg.value("prelogin", json::object());
    size_t npre = pre.value("agents", -1) < 0 ? order.size() : std::min(order.size(), pre.value("agents", size_t{0}));
    std::vector<std::string> first(order.begin(), order.begin() + static_cast<long>(npre)), rest(order.begin() + static_cast<long>(npre), order.end());
    attach_paced(first, pre.value("rate", 2000.0));
    // Wait until the prelogin agents are READY (or 60 s).
    for (int i = 0; i < 600; ++i) {
      size_t done = 0;
      {
        std::lock_guard lk(mu);
        for (const auto& k : first) done += agents.at(k).ready_once;
      }
      if (done >= first.size()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    const int64_t prelogin_done_ms = now_ms();
    std::printf("prelogin: %zu agents ready at %.1f s\n", first.size(), static_cast<double>(prelogin_done_ms) / 1000);
    std::fflush(stdout);
    {
      std::lock_guard lk(mu);
      attach_lat.clear();  // the storm's numbers only
      ready_lat.clear();
    }
    const int64_t run0 = now_ms();

    // 2. The storm: the rest attach at the gateway's rate from storm.at_s.
    // `storms` (a step-up): several storms one after another, each {at_s, rate, agents}.
    std::thread storm_th;
    json storms = cfg.value("storms", json::array());
    const json storm = cfg.value("storm", json());
    if (storms.empty() && storm.is_object()) storms.push_back({{"at_s", storm.value("at_s", 10.0)}, {"rate", storm.value("rate", 500.0)},
                                                              {"agents", rest.size()}});
    if (!storms.empty() && !rest.empty()) {
      storm_th = std::thread([this, storms, rest, run0] {
        size_t next = 0;
        for (const auto& sm : storms) {
          const int64_t at = run0 + static_cast<int64_t>(sm.value("at_s", 10.0) * 1000);
          while (now_ms() < at && !stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(5));
          const size_t n = std::min(rest.size() - next, sm.value("agents", rest.size()));
          const double rate = sm.value("rate", 500.0);
          std::vector<std::string> part(rest.begin() + static_cast<long>(next), rest.begin() + static_cast<long>(next + n));
          next += n;
          const int64_t from = now_ms();
          if (storm_from_ms < 0) storm_from_ms = from;
          attach_paced(part, rate);
          storm_to_ms = now_ms();
          char name[64];
          std::snprintf(name, sizeof name, "storm %zu agents at %.0f/s", n, rate);
          std::lock_guard lk(wmu);
          wins.push_back({name, from, storm_to_ms, rate});
        }
      });
    }

    // 3. Calls: ramp at ramp_rate until target_concurrent calls are up, then hold (rate = target /
    // mean talk) with bursts; Poisson arrivals.
    const json cc = cfg.at("calls");
    // `steps` (a step-up): after start_s the call rate follows [{rate, s}, ...]; before it, `rate`.
    const json steps = cc.value("steps", json::array());
    const double steps_at = cc.value("start_s", 0.0);
    double duration_s = cc.value("duration_s", 60.0);
    if (!steps.empty()) {
      duration_s = steps_at;
      for (const auto& st : steps) duration_s += st.at("s").get<double>();
    }
    int cur_step = -1;
    int64_t step_from = 0;
    const double base = cc.value("rate", 10.0), ramp = cc.value("ramp_rate", base);
    const int64_t target = cc.value("target_concurrent", int64_t{0});
    const json burst = cc.value("burst", json());
    std::exponential_distribution<double> unit(1.0);
    int64_t next_us = now_us();
    bool reached = target <= 0;
    json timeline = json::array();
    int64_t last_tl = 0;
    while (now_ms() - run0 < static_cast<int64_t>(duration_s * 1000) && !stop.load()) {
      const double t_s = static_cast<double>(now_ms() - run0) / 1000;
      const int64_t live = connected_now.load() + waiting_now.load();
      if (!reached && live >= target) reached = true;
      double rate = reached ? base : ramp;
      if (reached && burst.is_object()) {
        const double every = burst.value("every_s", 30.0), len = burst.value("len_s", 5.0);
        if (std::fmod(t_s, every) < len) rate = burst.value("rate", 50.0);
      }
      if (!steps.empty() && t_s >= steps_at) {
        double acc = steps_at;
        int k = 0;
        for (; k < static_cast<int>(steps.size()); ++k) {
          acc += steps[static_cast<size_t>(k)].at("s").get<double>();
          if (t_s < acc) break;
        }
        k = std::min(k, static_cast<int>(steps.size()) - 1);
        if (k != cur_step) {
          if (cur_step >= 0) {
            const double r0 = steps[static_cast<size_t>(cur_step)].at("rate").get<double>();
            std::lock_guard lk(wmu);
            wins.push_back({"calls at " + std::to_string(static_cast<int>(r0)) + "/s", step_from, now_ms(), r0});
          }
          cur_step = k;
          step_from = now_ms();
        }
        rate = steps[static_cast<size_t>(k)].at("rate").get<double>();
      }
      if (now_ms() - last_tl >= 1000) {
        last_tl = now_ms();
        std::lock_guard lk(mu);
        timeline.push_back({{"t_s", std::round(t_s)}, {"connected", connected_now.load()}, {"waiting", waiting_now.load()},
                            {"calls", calls_started}, {"worker_backlog", backlog_max.exchange(0)}});
      }
      const int64_t n = now_us();
      if (n < next_us) {
        std::this_thread::sleep_for(std::chrono::microseconds(std::min<int64_t>(next_us - n, 20'000)));
        continue;
      }
      run_now([this] { enqueue(); });
      double gap;
      {
        std::lock_guard lk(rng_mu);
        gap = unit(rng) / std::max(0.01, rate);
      }
      next_us += static_cast<int64_t>(gap * 1e6);
      if (next_us < n - 1'000'000) next_us = n;  // fell behind by > 1 s: don't fire a backlog at once
    }
    arrivals_on = false;
    const int64_t arrivals_end_ms = now_ms();
    if (cur_step >= 0) {
      const double r0 = steps[static_cast<size_t>(cur_step)].at("rate").get<double>();
      std::lock_guard lk(wmu);
      wins.push_back({"calls at " + std::to_string(static_cast<int>(r0)) + "/s", step_from, arrivals_end_ms, r0});
    }
    if (storm_th.joinable()) storm_th.join();

    // 4. The end: optionally cut every connected call short, then wait for calls to finish.
    if (cfg.value("end_calls_at_end", true)) {
      std::vector<std::pair<std::string, std::string>> live;
      {
        std::lock_guard lk(mu);
        for (const auto& [ix, c] : calls) {
          if (c.connected && !c.ended) live.emplace_back(ix, c.connected_res);
        }
      }
      for (const auto& [ix, rid] : live) run_now([this, ix = ix, rid = rid] { call_ended(ix, rid); });
    }
    const int64_t drain_until = now_ms() + static_cast<int64_t>(cfg.value("drain_s", 30.0) * 1000);
    size_t open = 0;
    for (;;) {
      open = 0;
      {
        std::lock_guard lk(mu);
        for (const auto& [ix, c] : calls) {
          if (!c.wrapped && !c.overflowed && !c.refused) ++open;
        }
      }
      if (open == 0 || now_ms() > drain_until) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    // A call that didn't finish must still be held by RE (not lost): Reattach a bounded sample.
    std::vector<std::string> unfinished;
    {
      std::lock_guard lk(mu);
      for (const auto& [ix, c] : calls) {
        if (!c.wrapped && !c.overflowed && !c.refused) unfinished.push_back(ix);
      }
    }
    size_t held = 0, checked = 0, lost = 0;
    for (const auto& ix : unfinished) {
      if (checked >= 2000) break;
      ++checked;
      rv1::ReattachRequest q;
      std::string tenant;
      bool connected = false, has_entry = false;
      {
        std::lock_guard lk(mu);
        const Call& c = calls.at(ix);
        tenant = c.tenant;
        connected = c.connected;
        has_entry = !c.entry.empty();
        *q.mutable_owner() = owner(c, c.seen_seq);
      }
      rv1::ReattachResponse r;
      const auto s = call("Reattach", tenant, [&](size_t i, grpc::ClientContext* cx) { return rs[i]->Reattach(cx, q, &r); });
      if (s.ok() && (r.has_entry() || r.reservations_size() > 0)) {
        ++held;
      } else if (!connected && has_entry) {
        ++lost;
        std::lock_guard lk(mu);
        violations.push_back("lost call at the end: " + ix);
      }
    }
    stop = true;
    jcv.notify_all();
    rcv.notify_all();
    for (auto& t : threads) {
      if (t.joinable()) t.detach();  // stream readers block in Read; the process exits next
    }

    std::lock_guard lk(mu);
    json out;
    size_t offered = 0, connected = 0, wrapped = 0, overflowed = 0, refused = 0, never_offered = 0;
    for (const auto& [ix, c] : calls) {
      offered += c.offered;
      connected += c.connected;
      wrapped += c.wrapped;
      overflowed += c.overflowed;
      refused += c.refused;
      if (!c.offered && !c.overflowed && !c.refused) ++never_offered;
    }
    const int64_t end = now_ms() + 1;
    const int64_t sf = storm_from_ms < 0 ? end : storm_from_ms, st = storm_to_ms < 0 ? end : storm_to_ms + 2000;
    out["agents"] = agents.size();
    out["prelogin"] = first.size();
    out["storm_agents"] = storm.is_object() ? rest.size() : 0;
    out["storm_s"] = storm_from_ms < 0 ? 0.0 : static_cast<double>(storm_to_ms - storm_from_ms) / 1000;
    out["attach_rate_achieved"] = storm_from_ms < 0 || storm_to_ms <= storm_from_ms
                                      ? 0.0 : static_cast<double>(rest.size()) * 1000.0 / static_cast<double>(storm_to_ms - storm_from_ms);
    out["attach_ms"] = window(attach_lat, 0, end);
    out["set_ready_ms"] = window(ready_lat, 0, end);
    out["time_to_offer_ms"] = {{"all", window(tto, 0, end)}, {"before_storm", window(tto, 0, sf)}, {"during_storm", window(tto, sf, st)},
                               {"after_storm", window(tto, st, end)}};
    out["enqueue_ms"] = window(enqueue_lat, 0, end);
    out["answer_offer_ms"] = window(accept_lat, 0, end);
    out["calls"] = {{"started", calls_started}, {"offered", offered}, {"connected", connected}, {"wrapped", wrapped},
                    {"overflowed", overflowed}, {"refused", refused}, {"never_offered", never_offered},
                    {"unfinished", unfinished.size()}, {"unfinished_checked", checked}, {"unfinished_held", held}, {"lost", lost}};
    int64_t peak_connected = 0, peak_live = 0;
    for (const auto& x : timeline) {
      peak_connected = std::max(peak_connected, x.at("connected").get<int64_t>());
      peak_live = std::max(peak_live, x.at("connected").get<int64_t>() + x.at("waiting").get<int64_t>());
    }
    out["peak_connected"] = peak_connected;
    out["peak_concurrent"] = peak_live;
    out["arrivals_s"] = static_cast<double>(arrivals_end_ms - run0) / 1000;
    out["calls_rate_achieved"] = out["arrivals_s"].get<double>() > 0 ? static_cast<double>(calls_started) / out["arrivals_s"].get<double>() : 0.0;
    {
      // Each step-up window with what was achieved in it (not what was configured).
      std::lock_guard wl(wmu);
      json wj = json::array();
      for (const auto& x : wins) {
        const double secs = std::max(0.001, static_cast<double>(x.to_ms - x.from_ms) / 1000);
        auto count = [&](const std::vector<Sample>& v) {
          size_t n = 0;
          for (const auto& y : v) n += y.t_ms >= x.from_ms && y.t_ms < x.to_ms;
          return n;
        };
        int64_t peak = 0;
        for (const auto& tl : timeline) {
          const int64_t ts = static_cast<int64_t>(tl.at("t_s").get<double>() * 1000) + run0;
          if (ts >= x.from_ms && ts <= x.to_ms + 1000) peak = std::max(peak, tl.at("connected").get<int64_t>() + tl.at("waiting").get<int64_t>());
        }
        const int64_t epoch_off = wall_ms() - now_ms();
        wj.push_back({{"name", x.name}, {"from_s", (x.from_ms - run0) / 1000.0}, {"secs", secs}, {"target_rate", x.target_rate},
                      {"from_epoch_ms", x.from_ms + epoch_off}, {"to_epoch_ms", x.to_ms + epoch_off},
                      {"attach_rate_achieved", count(attach_lat) / secs}, {"calls_rate_achieved", count(enqueue_lat) / secs},
                      {"offers_rate", count(tto) / secs}, {"peak_concurrent", peak},
                      {"attach_ms", window(attach_lat, x.from_ms, x.to_ms)}, {"set_ready_ms", window(ready_lat, x.from_ms, x.to_ms)},
                      {"time_to_offer_ms", window(tto, x.from_ms, x.to_ms)}, {"enqueue_ms", window(enqueue_lat, x.from_ms, x.to_ms)},
                      {"answer_offer_ms", window(accept_lat, x.from_ms, x.to_ms)}});
      }
      out["windows"] = wj;
    }
    // Wall-clock marks for run.sh's samples (the optimizer's CPU over the calls' window).
    const int64_t epoch_now = wall_ms(), mono_now = now_ms();
    out["run0_epoch_ms"] = epoch_now - (mono_now - run0);
    out["arrivals_end_epoch_ms"] = epoch_now - (mono_now - arrivals_end_ms);
    out["offers_seen"] = offers_seen;
    out["offers_expired_seen"] = offers_expired_seen;
    out["detached_seen"] = detached_seen;
    out["stream_msgs"] = stream_msgs;
    out["replays_dropped"] = replays_dropped;
    out["refusals"] = refusals;
    out["violations"] = violations;
    out["timeline"] = timeline;
    return out;
  }
};

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::fprintf(stderr, "usage: load_driver <config.json> <summary.json>\n");
    return 2;
  }
  std::ifstream in(argv[1]);
  std::stringstream ss;
  ss << in.rdbuf();
  auto* d = new Driver();  // never destroyed: detached stream readers may still touch it
  d->cfg = json::parse(ss.str());
  const json out = d->run();
  std::ofstream(argv[2]) << out.dump(2) << "\n";
  json brief = out;
  brief.erase("timeline");
  std::printf("%s\n", brief.dump().c_str());
  std::fflush(stdout);
  std::_Exit(out.at("violations").empty() ? 0 : 1);
}
