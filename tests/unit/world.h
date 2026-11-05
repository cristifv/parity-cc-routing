// Test helpers shared by the tenant tests: a timer heap, a recording sink, a one-tenant world on a
// virtual clock (PLAN §7).
#pragma once

#include <map>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "src/core/tenant.h"

namespace pcr {
namespace testing_world {

constexpr int64_t T0 = 1'792'800'000'000;

struct Heap final : TimerSink {
  struct Item {
    int64_t due;
    uint64_t seq;
    TimerRef ref;
    bool operator>(const Item& o) const { return due != o.due ? due > o.due : seq > o.seq; }
  };
  std::priority_queue<Item, std::vector<Item>, std::greater<>> q;
  uint64_t seq = 0;
  void at(int64_t due, TimerRef t) override { q.push(Item{due, ++seq, std::move(t)}); }
};

// Records what reaches a stream. By default every message does; with `hub` set it behaves like
// the stream hub (CR-053): only a message of the replica's live stream generation is delivered.
struct Rec final : Sink {
  std::vector<std::pair<std::string, rv1::WatchResponse>> ie;
  std::vector<std::pair<std::string, av1::WatchResponse>> gw;
  bool live = true;
  bool hub = false;
  std::map<std::string, uint64_t> live_gen;
  size_t held = 0;
  bool deliver(const std::string& r, uint64_t g) {
    if (!hub) return true;
    auto it = live_gen.find(r);
    if (it != live_gen.end() && it->second == g) return true;
    ++held;
    return false;
  }
  void to_ie(const std::string& r, uint64_t g, const rv1::WatchResponse& m) override {
    if (deliver(r, g)) ie.emplace_back(r, m);
  }
  void to_gw(const std::string& r, uint64_t g, const av1::WatchResponse& m) override {
    if (deliver(r, g)) gw.emplace_back(r, m);
  }
  bool ie_live(const std::string&) const override { return live; }
  std::vector<std::string> replicas;  // phase 3: live IE replicas for RunCallback
  std::vector<std::string> ie_replicas() const override { return replicas; }
};

inline QueueConfig queue(const std::string& id, std::vector<SkillReq> req = {}) {
  QueueConfig q;
  q.id = id;
  q.required = std::move(req);
  q.max_wait_ms = 600'000;
  q.max_wait_action = MaxWaitAction::kDisconnect;
  return q;
}

inline AgentConfig agent(const std::string& id, std::vector<std::string> queues,
                  std::map<std::string, uint32_t> skills = {}) {
  AgentConfig a;
  a.id = id;
  a.contact = "sip:" + id + "@acme.agents.example";
  a.queues = std::move(queues);
  a.skills = std::move(skills);
  return a;
}

struct World {
  Heap heap;
  Rec rec;
  Counters counters;
  Epochs epochs{7, 0};
  std::unique_ptr<Tenant> t;
  int64_t now = T0;
  std::map<std::string, Tenant::SeededSession> sess;
  int cmd = 0;
  uint32_t seq_by_ix = 0;

  explicit World(TenantConfig cfg) {
    cfg.id = cfg.id.empty() ? "acme" : cfg.id;
    t = std::make_unique<Tenant>(cfg, &epochs, &rec, &heap, &counters);
  }
  void ready(const std::string& a, int64_t since) {
    sess[a] = t->seed_attached(a, "gw-1", "", since, now);
  }
  void attach_not_ready(const std::string& a) { sess[a] = t->seed_attached(a, "gw-1", "", std::nullopt, now); }
  void advance(int64_t ms) {
    const int64_t end = now + ms;
    int64_t next_tick = (now / 1000 + 1) * 1000;
    for (;;) {
      const int64_t tdue = heap.q.empty() ? INT64_MAX : heap.q.top().due;
      const int64_t due = std::min(tdue, next_tick);
      if (due > end) break;
      now = due;
      if (tdue <= next_tick) {
        auto it = heap.q.top();
        heap.q.pop();
        t->on_timer(it.ref, now);
      } else {
        next_tick += 1000;
        t->sweep(now);
        t->redeliver(now);
      }
    }
    now = end;
  }
  rv1::InteractionOwner owner(const std::string& ix, uint64_t epoch = 100) {
    rv1::InteractionOwner o;
    o.set_tenant_id(t->id());
    o.set_interaction_id(ix);
    o.set_owner_epoch(epoch);
    o.set_replica_id("ie-1");
    return o;
  }
  Result<rv1::EnqueueResponse> enqueue(const std::string& ix, const std::string& q, uint32_t prio = 0,
                                       std::vector<SkillReq> reqs = {}) {
    rv1::EnqueueRequest r;
    *r.mutable_owner() = owner(ix);
    r.set_enqueue_seq(1);
    r.mutable_target()->set_queue_id(q);
    r.set_priority(prio);
    for (auto& rq : reqs) {
      auto* x = r.add_requirements();
      x->set_skill_id(rq.skill_id);
      x->set_min_level(rq.min_level);
    }
    r.set_first_queued_at_ms(now);
    r.set_ordering_at_ms(now);
    return t->enqueue(r, now);
  }
  av1::SessionRef ref(const std::string& a) {
    av1::SessionRef s;
    s.set_tenant_id(t->id());
    s.set_agent_id(a);
    s.set_session_id(sess[a].session_id);
    s.set_attach_generation(sess[a].attach_generation);
    return s;
  }
  std::string next_cmd() { return "cmd-" + std::to_string(++cmd); }
  Result<av1::AnswerOfferResponse> answer(const std::string& a, bool accept) {
    av1::AnswerOfferRequest r;
    *r.mutable_session() = ref(a);
    r.set_command_id(next_cmd());
    r.set_offer_id(t->live_offer_of(a));
    r.set_decision(accept ? av1::DECISION_ACCEPT : av1::DECISION_REJECT);
    return t->answer_offer(r, now);
  }
  Result<av1::SetStateResponse> set(const std::string& a, av1::TargetState s, std::string reason = "") {
    av1::SetStateRequest r;
    *r.mutable_session() = ref(a);
    r.set_command_id(next_cmd());
    r.set_target(s);
    r.set_reason_code(reason);
    return t->set_state(r, now);
  }
  std::vector<std::string> broken() { return t->check_invariants(now); }
};

}  // namespace testing_world
}  // namespace pcr
