// Phase 6 chaos driver (PLAN §7 "Chaos"): one simulated IE replica (routing.v1) and one simulated
// gateway replica (agents.v1) driving real RE instances (a leader and standbys, possibly two bins)
// while tests/chaos/run.sh kills, pauses or cuts them off. It does what IE and the gateway must do
// on a leader change (the requirement this phase hands to IE-059): follow parity-leader / the next
// instance, reopen both Watch streams, re-attach every agent with its session (existing: true),
// Reattach every interaction it still owns (never Enqueue again while RE has an entry), keep
// connected calls, and drop replays by delivery_seq. On tenant_not_in_bin it moves to the other bin.
//
// Every stream message and RPC result is recorded (--log, JSON lines). At the end the checker reads
// the recording and reports: double offers (two live offers for one agent, or for one interaction),
// entries ending more than once (a second reservation connected for one call), lost entries (a
// call RE no longer holds and that never ended), lost reservations (reservation_unknown for one it
// handed out), and every failover gap (no successful RPC) with the chaos events' times.
#include <grpcpp/grpcpp.h>
#include <signal.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "parity/agents/v1/agents.grpc.pb.h"
#include "parity/routing/v1/routing.grpc.pb.h"

namespace rv1 = ::parity::routing::v1;
namespace av1 = ::parity::agents::v1;
using json = nlohmann::json;

namespace {

int64_t now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string uuid7(std::mt19937_64& rng) {
  const uint64_t ms = static_cast<uint64_t>(now_ms());
  const uint64_t a = rng(), b = rng();
  char buf[64];
  std::snprintf(buf, sizeof buf, "%08llx-%04llx-7%03llx-%04llx-%012llx", static_cast<unsigned long long>((ms >> 16) & 0xffffffffULL),
                static_cast<unsigned long long>(ms & 0xffff), static_cast<unsigned long long>(a & 0xfff),
                static_cast<unsigned long long>(0x8000 | ((a >> 12) & 0x3fff)),
                static_cast<unsigned long long>(b & 0xffffffffffffULL));
  return buf;
}

struct Target {
  std::string bin, addr;
  std::shared_ptr<grpc::Channel> ch;
  std::unique_ptr<rv1::RoutingService::Stub> rs;
  std::unique_ptr<av1::AgentService::Stub> as;
};

struct Agent {
  std::string id, session;
  uint64_t gen = 0;
  std::string live_offer;    // the offer the gateway holds open
  std::string offer_ix;      // its interaction
  int64_t offer_expires = 0;
  bool attached = false;
};

struct Call {
  std::string id, entry;
  int64_t enqueued_at = 0;
  std::string res;           // the reservation being worked
  std::string connected_res;  // the reservation that connected (the entry's one end)
  bool connected = false, ended = false, wrapped = false, overflowed = false;
  std::set<std::string> reservations;  // every reservation RE assigned for it
  int64_t first_offer_at = 0;
};

class Driver {
 public:
  json cfg;
  std::vector<Target> targets;
  std::string tenant, queue;
  std::mt19937_64 rng{std::random_device{}()};
  std::mutex mu;
  std::map<std::string, Agent> agents;
  std::map<std::string, Call> calls;
  std::map<std::string, std::string> live_offer_by_ix;  // interaction -> agent with its live offer
  std::atomic<int> cur{0};
  std::atomic<bool> stop{false}, arrivals_done{false};
  std::atomic<uint64_t> max_epoch{0};
  std::atomic<uint64_t> resyncs{0};
  std::ofstream log;
  std::mutex log_mu;
  // results
  std::vector<std::string> violations;
  std::vector<std::pair<int64_t, int64_t>> gaps;  // (from, to) of no successful RPC
  std::atomic<int64_t> last_ok{0}, fail_since{0};
  std::vector<int64_t> time_to_offer;
  uint64_t rpcs_ok = 0, rpcs_failed = 0, stream_msgs = 0, replays_dropped = 0;
  std::map<std::string, uint64_t> ie_seen_seq;  // interaction -> highest delivery_seq processed
  std::map<std::string, uint64_t> gw_seen_seq;  // agent:gen -> highest
  // a tiny timer wheel
  struct Job {
    int64_t due;
    uint64_t seq;
    std::function<void()> fn;
    bool operator>(const Job& o) const { return due != o.due ? due > o.due : seq > o.seq; }
  };
  std::mutex jmu;
  std::condition_variable jcv;
  std::priority_queue<Job, std::vector<Job>, std::greater<>> jobs;
  uint64_t jseq = 0;

  void record(json j) {
    j["t"] = now_ms();
    std::lock_guard lk(log_mu);
    log << j.dump() << "\n";
  }

  void later(int64_t delay_ms, std::function<void()> fn) {
    {
      std::lock_guard lk(jmu);
      jobs.push(Job{now_ms() + delay_ms, ++jseq, std::move(fn)});
    }
    jcv.notify_one();
  }

  void worker() {
    for (;;) {
      std::function<void()> fn;
      {
        std::unique_lock lk(jmu);
        jcv.wait_for(lk, std::chrono::milliseconds(20), [&] { return !jobs.empty() && jobs.top().due <= now_ms(); });
        if (stop.load() && jobs.empty()) return;
        if (jobs.empty() || jobs.top().due > now_ms()) continue;
        fn = jobs.top().fn;
        jobs.pop();
      }
      fn();
    }
  }

  Target& target() { return targets[static_cast<size_t>(cur.load()) % targets.size()]; }

  void rotate(const std::string& leader_hint, const std::string& want_bin) {
    const int n = static_cast<int>(targets.size());
    if (!leader_hint.empty()) {
      for (int i = 0; i < n; ++i) {
        if (targets[i].addr == leader_hint) {
          cur = i;
          return;
        }
      }
    }
    const std::string bin = want_bin.empty() ? target().bin : want_bin;
    for (int k = 1; k <= n; ++k) {
      const int i = (cur.load() + k) % n;
      if (targets[i].bin == bin) {
        cur = i;
        return;
      }
    }
  }

  static std::string trailer(const grpc::ClientContext& c, const char* key) {
    const auto& m = c.GetServerTrailingMetadata();
    auto it = m.find(key);
    return it == m.end() ? std::string() : std::string(it->second.data(), it->second.size());
  }

  void note_ok() {
    const int64_t t = now_ms();
    const int64_t fs = fail_since.exchange(0);
    if (fs != 0) {
      const int64_t lo = last_ok.load();
      std::lock_guard lk(mu);
      if (lo > 0 && t - lo > 300) gaps.emplace_back(lo, t);
    }
    last_ok = t;
    std::lock_guard lk(mu);
    ++rpcs_ok;
  }

  // One RPC with failover: retried on another instance while RE is unavailable, at most `budget_ms`.
  template <class F>
  grpc::Status call(const char* what, F f, int64_t budget_ms = 30'000, std::string* parity = nullptr) {
    const int64_t until = now_ms() + budget_ms;
    for (int attempt = 0;; ++attempt) {
      grpc::ClientContext c;
      c.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
      c.AddMetadata("tenant-id", tenant);
      Target& t = target();
      const grpc::Status s = f(t, &c);
      const std::string pe = trailer(c, "parity-error");
      if (parity) *parity = pe;
      if (s.ok()) {
        note_ok();
        return s;
      }
      const bool not_leader = pe == "not_leader";
      const bool moved = pe == "tenant_not_in_bin";
      const bool retry = not_leader || moved || s.error_code() == grpc::StatusCode::UNAVAILABLE ||
                         s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED;
      if (!retry) {
        note_ok();  // RE answered
        return s;
      }
      {
        std::lock_guard lk(mu);
        ++rpcs_failed;
      }
      int64_t zero = 0;
      fail_since.compare_exchange_strong(zero, now_ms());
      if (now_ms() >= until) return s;
      std::string other_bin;
      if (moved) {
        for (const auto& x : targets) {
          if (x.bin != t.bin) other_bin = x.bin;
        }
      }
      const std::string bin_before = target().bin;
      rotate(not_leader ? trailer(c, "parity-leader") : "", other_bin);
      if (moved && target().bin != bin_before) {
        // A tenant move: the other bin holds the tenant now (IE: Reattach there; gateway: re-attach).
        record({{"k", "bin_moved"}, {"from", bin_before}, {"to", target().bin}});
        later(200, [this] { resync(); });
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(attempt < 3 ? 30 : 100));
    }
  }

  rv1::InteractionOwner owner(const std::string& ix, uint64_t acked = 0) {
    rv1::InteractionOwner o;
    o.set_tenant_id(tenant);
    o.set_interaction_id(ix);
    o.set_owner_epoch(1);
    o.set_replica_id("ie-chaos");
    o.set_acked_delivery_seq(acked);
    return o;
  }

  av1::SessionRef session(const Agent& a) {
    av1::SessionRef s;
    s.set_tenant_id(tenant);
    s.set_agent_id(a.id);
    s.set_session_id(a.session);
    s.set_attach_generation(a.gen);
    return s;
  }

  // ---- agents ----
  void attach(const std::string& id) {
    av1::AttachAgentRequest q;
    {
      std::lock_guard lk(mu);
      Agent& a = agents[id];
      q.set_tenant_id(tenant);
      q.set_agent_id(id);
      q.set_replica_id("gw-chaos");
      q.set_session_id(a.session);
      q.set_agent_contact("sip:" + id + "@" + tenant + ".agents.example");
    }
    av1::AttachAgentResponse r;
    const auto s = call("AttachAgent", [&](Target& t, grpc::ClientContext* c) { return t.as->AttachAgent(c, q, &r); });
    if (!s.ok()) {
      record({{"k", "attach_failed"}, {"agent", id}, {"code", s.error_code()}});
      return;
    }
    av1::AgentState st;
    {
      std::lock_guard lk(mu);
      Agent& a = agents[id];
      a.gen = r.attach_generation();
      a.attached = true;
      st = r.agent().state();
      if (r.has_offer()) {
        open_offer(a, r.offer().offer_id(), r.offer().interaction_id(), r.offer().expires_at_ms());
      }
    }
    record({{"k", "attached"}, {"agent", id}, {"gen", r.attach_generation()}, {"existing", r.existing()},
            {"state", av1::AgentState_Name(st)}, {"offer", r.has_offer() ? r.offer().offer_id() : ""}});
    if (r.has_offer()) {
      const std::string oid = r.offer().offer_id();
      later(50, [this, id, oid] { answer(id, oid); });
    }
    if (st == av1::AGENT_STATE_NOT_READY || st == av1::AGENT_STATE_LOGGED_OUT) set_ready(id);
    for (const auto& res : r.reservations()) {
      if (res.wrap_up()) later(200, [this, id, rid = res.reservation_id()] { end_wrap(id, rid); });
    }
  }

  void set_ready(const std::string& id) {
    av1::SetStateRequest q;
    {
      std::lock_guard lk(mu);
      *q.mutable_session() = session(agents[id]);
    }
    q.set_command_id(uuid7(rng));
    q.set_target(av1::TARGET_STATE_READY);
    av1::SetStateResponse r;
    std::string pe;
    const auto s = call("SetState", [&](Target& t, grpc::ClientContext* c) { return t.as->SetState(c, q, &r); }, 30'000, &pe);
    if (!s.ok() && pe == "session_superseded") later(100, [this, id] { attach(id); });
  }

  // Under mu.
  void open_offer(Agent& a, const std::string& oid, const std::string& ix, int64_t expires) {
    if (a.live_offer == oid) return;  // a replay of the same offer
    const int64_t t = now_ms();
    if (!a.live_offer.empty() && a.offer_expires > t - 1000) {
      violations.push_back("double offer to agent " + a.id + ": " + a.live_offer + " still live, then " + oid);
    }
    auto other = live_offer_by_ix.find(ix);
    if (other != live_offer_by_ix.end() && other->second != a.id) {
      Agent& b = agents[other->second];
      if (b.offer_ix == ix && !b.live_offer.empty() && b.offer_expires > t - 1000) {
        // Two agents' streams aren't ordered against each other after a replay: the earlier offer's
        // revoke may still be on its way. A double offer if it doesn't close within 2 s.
        suspects[b.live_offer] = Suspect{ix, b.id, a.id, t};
      }
    }
    a.live_offer = oid;
    a.offer_ix = ix;
    a.offer_expires = expires;
    live_offer_by_ix[ix] = a.id;
    auto c = calls.find(ix);
    if (c != calls.end() && c->second.first_offer_at == 0) {
      c->second.first_offer_at = t;
      time_to_offer.push_back(t - c->second.enqueued_at);
    }
  }

  struct Suspect {
    std::string ix, holder, other;
    int64_t at = 0;
  };
  std::map<std::string, Suspect> suspects;  // offer id -> a second agent's offer for its interaction
  // Under mu.
  void judge_suspects(bool all) {
    const int64_t t = now_ms();
    for (auto it = suspects.begin(); it != suspects.end();) {
      if (all || t - it->second.at > 2000) {
        violations.push_back("double offer for interaction " + it->second.ix + ": agents " + it->second.holder + " and " +
                             it->second.other);
        it = suspects.erase(it);
      } else {
        ++it;
      }
    }
  }

  // Under mu.
  void close_offer(Agent& a, const std::string& oid) {
    suspects.erase(oid);
    if (a.live_offer != oid) return;
    auto it = live_offer_by_ix.find(a.offer_ix);
    if (it != live_offer_by_ix.end() && it->second == a.id) live_offer_by_ix.erase(it);
    a.live_offer.clear();
    a.offer_ix.clear();
  }

  void answer(const std::string& id, const std::string& oid) {
    av1::AnswerOfferRequest q;
    {
      std::lock_guard lk(mu);
      Agent& a = agents[id];
      if (a.live_offer != oid) return;
      *q.mutable_session() = session(a);
    }
    q.set_command_id(uuid7(rng));
    q.set_offer_id(oid);
    q.set_decision(av1::DECISION_ACCEPT);
    av1::AnswerOfferResponse r;
    std::string pe;
    record({{"k", "accept_sent"}, {"agent", id}, {"offer", oid}});
    // Chaos "mid-accept": with the trigger file present, the leader (its pid in the file) is killed
    // 2 ms after this accept is sent, while RE handles and commits it.
    const std::string trig = cfg.value("kill_on_accept_file", std::string());
    if (!trig.empty() && std::rename(trig.c_str(), (trig + ".taken").c_str()) == 0) {
      std::ifstream pf(trig + ".taken");
      long pid = 0;
      pf >> pid;
      std::thread([this, pid, oid] {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        if (pid > 1) ::kill(static_cast<pid_t>(pid), SIGKILL);
        record({{"k", "killed_on_accept"}, {"pid", pid}, {"offer", oid}});
      }).detach();
    }
    const auto s = call("AnswerOffer", [&](Target& t, grpc::ClientContext* c) { return t.as->AnswerOffer(c, q, &r); }, 30'000, &pe);
    record({{"k", "accept_done"}, {"agent", id}, {"offer", oid}, {"ok", s.ok()}, {"err", pe},
            {"res", s.ok() ? r.reservation().reservation_id() : ""}});
    std::lock_guard lk(mu);
    close_offer(agents[id], oid);
    if (!s.ok() && pe == "session_superseded") later(100, [this, id] { attach(id); });
  }

  void end_wrap(const std::string& id, const std::string& rid) {
    av1::EndWrapUpRequest q;
    {
      std::lock_guard lk(mu);
      *q.mutable_session() = session(agents[id]);
    }
    q.set_command_id(uuid7(rng));
    q.set_reservation_id(rid);
    q.set_disposition_code("done");
    av1::EndWrapUpResponse r;
    std::string pe;
    const auto s = call("EndWrapUp", [&](Target& t, grpc::ClientContext* c) { return t.as->EndWrapUp(c, q, &r); }, 30'000, &pe);
    if (!s.ok() && pe != "invalid_transition") record({{"k", "endwrap_failed"}, {"agent", id}, {"res", rid}, {"err", pe}});
  }

  // ---- calls ----
  void enqueue(const std::string& ix) {
    rv1::EnqueueRequest q;
    *q.mutable_owner() = owner(ix);
    q.set_enqueue_seq(1);
    q.mutable_target()->set_queue_id(queue);
    const int64_t t = now_ms();
    q.set_first_queued_at_ms(t);
    q.set_ordering_at_ms(t);
    q.set_channel(rv1::CHANNEL_VOICE);
    q.set_reason(rv1::ENQUEUE_REASON_NEW);
    {
      std::lock_guard lk(mu);
      Call& c = calls[ix];
      c.id = ix;
      c.enqueued_at = t;
    }
    rv1::EnqueueResponse r;
    std::string pe;
    const auto s = call("Enqueue", [&](Target& tg, grpc::ClientContext* c) { return tg.rs->Enqueue(c, q, &r); }, 60'000, &pe);
    record({{"k", "enqueued"}, {"ix", ix}, {"sent", t}, {"ok", s.ok()}, {"err", pe}, {"entry", s.ok() ? r.entry().entry_id() : ""},
            {"existing", s.ok() && r.existing()}});
    std::lock_guard lk(mu);
    if (s.ok()) calls[ix].entry = r.entry().entry_id();
  }

  void report_connected(const std::string& ix, const std::string& rid) {
    rv1::ReportOutcomeRequest q;
    {
      std::lock_guard lk(mu);
      Call& c = calls[ix];
      if (c.res != rid || c.connected) return;
      *q.mutable_owner() = owner(ix, ie_seen_seq[ix]);
    }
    q.set_reservation_id(rid);
    q.set_outcome(rv1::OUTCOME_CONNECTED);
    rv1::ReportOutcomeResponse r;
    std::string pe;
    const auto s = call("ReportOutcome", [&](Target& t, grpc::ClientContext* c) { return t.rs->ReportOutcome(c, q, &r); }, 30'000, &pe);
    record({{"k", "outcome"}, {"ix", ix}, {"res", rid}, {"ok", s.ok()}, {"err", pe},
            {"recorded", s.ok() ? rv1::Outcome_Name(r.recorded()) : ""}, {"existing", s.ok() && r.existing()}});
    std::lock_guard lk(mu);
    Call& c = calls[ix];
    if (!s.ok()) {
      if (pe == "reservation_unknown") violations.push_back("lost reservation " + rid + " of " + ix + " (ReportOutcome)");
      return;
    }
    if (r.recorded() == rv1::OUTCOME_CONNECTED) {
      if (c.connected && c.connected_res != rid) {
        violations.push_back("entry of " + ix + " ended twice: reservations " + c.connected_res + " and " + rid + " connected");
      }
      c.connected = true;
      c.connected_res = rid;
      const int64_t talk = cfg.value("talk_ms_min", 2000) +
                           static_cast<int64_t>(rng() % static_cast<uint64_t>(std::max(1, cfg.value("talk_ms_span", 3000))));
      later(talk, [this, ix, rid] { call_ended(ix, rid); });
    } else if (c.res == rid) {
      c.res.clear();  // expired or revoked first: the entry is back
    }
  }

  void call_ended(const std::string& ix, const std::string& rid) {
    rv1::CallEndedRequest q;
    std::string agent;
    {
      std::lock_guard lk(mu);
      *q.mutable_owner() = owner(ix, ie_seen_seq[ix]);
    }
    q.set_reservation_id(rid);
    q.set_ended_at_ms(now_ms());
    // The agent: from the recorded Assigned.
    {
      std::lock_guard lk(mu);
      auto it = assigned_agent.find(rid);
      if (it != assigned_agent.end()) agent = it->second;
    }
    q.set_agent_id(agent);
    rv1::CallEndedResponse r;
    std::string pe;
    const auto s = call("CallEnded", [&](Target& t, grpc::ClientContext* c) { return t.rs->CallEnded(c, q, &r); }, 30'000, &pe);
    record({{"k", "ended"}, {"ix", ix}, {"res", rid}, {"ok", s.ok()}, {"err", pe}});
    std::lock_guard lk(mu);
    if (!s.ok() && pe == "reservation_unknown") violations.push_back("lost reservation " + rid + " of " + ix + " (CallEnded)");
    if (s.ok()) calls[ix].ended = true;
  }
  std::map<std::string, std::string> assigned_agent;  // reservation -> agent

  // ---- streams ----
  void ie_message(const rv1::WatchResponse& m) {
    if (m.leader_epoch() > max_epoch.load()) {
      const uint64_t was = max_epoch.exchange(m.leader_epoch());
      if (was != 0 && m.leader_epoch() > was) {
        record({{"k", "epoch_rise"}, {"from", was}, {"to", m.leader_epoch()}});
        later(0, [this] { resync(); });
      }
    }
    if (m.has_heartbeat()) return;
    std::string ix;
    if (m.has_assigned()) ix = m.assigned().target().interaction_id();
    if (m.has_revoke()) ix = m.revoke().target().interaction_id();
    if (m.has_overflow()) ix = m.overflow().target().interaction_id();
    if (m.has_wrap_up_ended()) ix = m.wrap_up_ended().target().interaction_id();
    if (m.has_position()) return;
    if (ix.empty()) return;
    const uint64_t seq = m.delivery_seq();
    {
      std::lock_guard lk(mu);
      ++stream_msgs;
      uint64_t& seen = ie_seen_seq[ix];
      if (seq != 0 && seq <= seen) {
        ++replays_dropped;
        goto ack;
      }
      if (seq != 0) seen = seq;
      Call& c = calls[ix];
      if (m.has_assigned()) {
        const std::string rid = m.assigned().reservation_id();
        assigned_agent[rid] = m.assigned().agent_id();
        c.reservations.insert(rid);
        record({{"k", "assigned"}, {"ix", ix}, {"res", rid}, {"agent", m.assigned().agent_id()}, {"seq", seq},
                {"epoch", m.leader_epoch()}});
        if (c.connected && rid != c.connected_res) {
          violations.push_back("entry of " + ix + " assigned again after it connected: " + rid);
        } else if (!c.connected) {
          c.res = rid;
          later(100, [this, ix, rid] { report_connected(ix, rid); });
        }
      } else if (m.has_revoke()) {
        record({{"k", "revoke"}, {"ix", ix}, {"res", m.revoke().reservation_id()}, {"seq", seq}});
        if (c.res == m.revoke().reservation_id() && !c.connected) c.res.clear();
      } else if (m.has_overflow()) {
        record({{"k", "overflow"}, {"ix", ix}, {"seq", seq}});
        c.overflowed = true;
      } else if (m.has_wrap_up_ended()) {
        record({{"k", "wrapped"}, {"ix", ix}, {"res", m.wrap_up_ended().reservation_id()}, {"seq", seq}});
        if (m.wrap_up_ended().reservation_id() == c.connected_res) c.wrapped = true;
      }
    }
  ack:
    if (seq == 0) return;
    rv1::AcknowledgeRequest q;
    auto* a = q.add_acks();
    *a = owner(ix, seq);
    later(0, [this, q] {
      rv1::AcknowledgeResponse rr;
      (void)call("Acknowledge", [&](Target& t, grpc::ClientContext* c) { return t.rs->Acknowledge(c, q, &rr); }, 5'000);
    });
  }

  void gw_message(const av1::WatchResponse& m) {
    if (m.leader_epoch() > max_epoch.load()) {
      const uint64_t was = max_epoch.exchange(m.leader_epoch());
      if (was != 0 && m.leader_epoch() > was) {
        record({{"k", "epoch_rise"}, {"from", was}, {"to", m.leader_epoch()}});
        later(0, [this] { resync(); });
      }
    }
    if (m.has_heartbeat()) return;
    const std::string id = m.agent_id();
    const uint64_t seq = m.delivery_seq();
    bool ack = seq != 0;
    {
      std::lock_guard lk(mu);
      ++stream_msgs;
      auto ait = agents.find(id);
      if (ait == agents.end()) return;
      Agent& a = ait->second;
      if (m.attach_generation() < a.gen) return;  // an older attachment's
      uint64_t& seen = gw_seen_seq[id + ":" + std::to_string(m.attach_generation())];
      if (seq != 0 && seq <= seen) {
        ++replays_dropped;
      } else {
        if (seq != 0) seen = seq;
        if (m.has_offer()) {
          record({{"k", "offer"}, {"agent", id}, {"offer", m.offer().offer_id()}, {"ix", m.offer().interaction_id()},
                  {"seq", seq}, {"epoch", m.leader_epoch()}});
          open_offer(a, m.offer().offer_id(), m.offer().interaction_id(), m.offer().expires_at_ms());
          const std::string oid = m.offer().offer_id();
          later(50 + static_cast<int64_t>(rng() % 250), [this, id, oid] { answer(id, oid); });
        } else if (m.has_offer_revoked()) {
          record({{"k", "offer_revoked"}, {"agent", id}, {"offer", m.offer_revoked().offer_id()}, {"seq", seq},
                  {"reason", av1::RevokeReason_Name(m.offer_revoked().reason())}});
          close_offer(a, m.offer_revoked().offer_id());
          if (m.offer_revoked().reason() == av1::REVOKE_REASON_EXPIRED) later(200, [this, id] { set_ready(id); });
        } else if (m.has_reserved()) {
          record({{"k", "reserved"}, {"agent", id}, {"res", m.reserved().reservation_id()}, {"seq", seq}});
          if (!a.live_offer.empty() && a.offer_ix == m.reserved().interaction_id()) close_offer(a, a.live_offer);
        } else if (m.has_wrap_up_started()) {
          const std::string rid = m.wrap_up_started().reservation_id();
          later(300, [this, id, rid] { end_wrap(id, rid); });
        } else if (m.has_detached()) {
          record({{"k", "detached"}, {"agent", id}});
          a.attached = false;
          later(200, [this, id] { attach(id); });
        } else if (m.has_state_changed()) {
          const auto st = m.state_changed().agent().state();
          if (st == av1::AGENT_STATE_NOT_READY && !stop.load()) later(300, [this, id] { set_ready(id); });
        }
      }
    }
    if (!ack) return;
    av1::AcknowledgeRequest q;
    auto* k = q.add_acks();
    k->set_tenant_id(tenant);
    k->set_agent_id(id);
    k->set_attach_generation(m.attach_generation());
    k->set_delivery_seq(seq);
    later(0, [this, q] {
      av1::AcknowledgeResponse rr;
      (void)call("AgentsAcknowledge", [&](Target& t, grpc::ClientContext* c) { return t.as->Acknowledge(c, q, &rr); }, 5'000);
    });
  }

  template <class Stub, class Req, class Resp>
  void watch_loop(bool ie) {
    while (!stop.load()) {
      Target& t = target();
      grpc::ClientContext c;
      Req q;
      q.set_replica_id(ie ? "ie-chaos" : "gw-chaos");
      q.set_bin(t.bin);
      std::unique_ptr<grpc::ClientReader<Resp>> rd;
      if constexpr (std::is_same_v<Req, rv1::WatchRequest>) {
        rd = t.rs->Watch(&c, q);
      } else {
        rd = t.as->Watch(&c, q);
      }
      Resp m;
      bool first = true;
      // A tenant move or a failover elsewhere changed the instance: follow it.
      std::atomic<bool> done{false};
      std::thread guard([&] {
        while (!done.load() && !stop.load() && &target() == &t) std::this_thread::sleep_for(std::chrono::milliseconds(50));
        c.TryCancel();
      });
      while (!stop.load() && rd->Read(&m)) {
        if (first) {
          first = false;
          record({{"k", ie ? "ie_stream_open" : "gw_stream_open"}, {"to", t.addr}, {"epoch", m.leader_epoch()}});
        }
        if constexpr (std::is_same_v<Resp, rv1::WatchResponse>) {
          ie_message(m);
        } else {
          gw_message(m);
        }
      }
      done = true;
      guard.join();
      c.TryCancel();
      const grpc::Status s = rd->Finish();
      if (stop.load()) return;
      const std::string pe = trailer(c, "parity-error");
      std::string other_bin;
      if (pe == "tenant_not_in_bin") {
        for (const auto& x : targets) {
          if (x.bin != t.bin) other_bin = x.bin;
        }
      }
      if (&t == &target()) rotate(pe == "not_leader" ? trailer(c, "parity-leader") : "", other_bin);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  // What IE (IE-059) and the gateway do when the leader changes: every agent re-attached with its
  // session, every interaction still owned Reattached; Enqueue again only when RE has no entry.
  void resync() {
    ++resyncs;
    std::vector<std::string> ids, ixs;
    {
      std::lock_guard lk(mu);
      for (const auto& [id, a] : agents) ids.push_back(id);
      for (const auto& [ix, c] : calls) {
        if (!c.wrapped && !c.overflowed) ixs.push_back(ix);
      }
    }
    record({{"k", "resync"}, {"agents", ids.size()}, {"calls", ixs.size()}});
    for (const auto& id : ids) later(0, [this, id] { attach(id); });
    for (const auto& ix : ixs) later(0, [this, ix] { reattach(ix, /*final_check=*/false); });
  }

  // Returns whether RE still holds the call (an entry or a live reservation).
  bool reattach(const std::string& ix, bool final_check) {
    rv1::ReattachRequest q;
    {
      std::lock_guard lk(mu);
      *q.mutable_owner() = owner(ix, ie_seen_seq[ix]);
    }
    rv1::ReattachResponse r;
    std::string pe;
    const auto s = call("Reattach", [&](Target& t, grpc::ClientContext* c) { return t.rs->Reattach(c, q, &r); }, 30'000, &pe);
    if (!s.ok()) {
      record({{"k", "reattach_failed"}, {"ix", ix}, {"err", pe}});
      return false;
    }
    const bool holds = r.has_entry() || r.reservations_size() > 0;
    record({{"k", "reattached"}, {"ix", ix}, {"entry", r.has_entry() ? r.entry().entry_id() : ""},
            {"reservations", r.reservations_size()}, {"final", final_check}});
    std::lock_guard lk(mu);
    Call& c = calls[ix];
    for (const auto& res : r.reservations()) {
      if (res.state() == rv1::RESERVATION_STATE_ASSIGNED && !c.connected && c.res != res.reservation_id()) {
        // An Assigned we never processed (it replays too): work it now.
        c.res = res.reservation_id();
        assigned_agent[res.reservation_id()] = res.agent_id();
        later(100, [this, ix, rid = res.reservation_id()] { report_connected(ix, rid); });
      }
    }
    if (!final_check && !holds && !c.connected && !c.overflowed && !c.entry.empty()) {
      violations.push_back("lost entry: RE no longer holds " + ix + " (" + c.entry + ") and it never connected");
    }
    return holds;
  }

  json run() {
    tenant = cfg.at("tenant").get<std::string>();
    queue = cfg.at("queue").get<std::string>();
    for (const auto& t : cfg.at("targets")) {
      Target x;
      x.bin = t.at("bin").get<std::string>();
      x.addr = t.at("addr").get<std::string>();
      grpc::ChannelArguments args;
      args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
      args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 500);
      args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 100);
      x.ch = grpc::CreateCustomChannel(x.addr, grpc::InsecureChannelCredentials(), args);
      x.rs = rv1::RoutingService::NewStub(x.ch);
      x.as = av1::AgentService::NewStub(x.ch);
      targets.push_back(std::move(x));
    }
    log.open(cfg.at("log").get<std::string>());
    std::vector<std::thread> workers;
    for (int i = 0; i < 16; ++i) workers.emplace_back([this] { worker(); });
    std::thread ie_th([this] { watch_loop<rv1::RoutingService::Stub, rv1::WatchRequest, rv1::WatchResponse>(true); });
    std::thread gw_th([this] { watch_loop<av1::AgentService::Stub, av1::WatchRequest, av1::WatchResponse>(false); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto& a : cfg.at("agents")) {
      const std::string id = a.get<std::string>();
      {
        std::lock_guard lk(mu);
        agents[id].id = id;
        agents[id].session = uuid7(rng);
      }
      later(0, [this, id] { attach(id); });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    const double rate = cfg.value("rate", 10.0);
    const int64_t duration = cfg.value("duration_s", 60) * 1000;
    const int64_t t0 = now_ms();
    const std::string stop_file = cfg.value("stop_file", std::string());
    int n = 0;
    while (now_ms() - t0 < duration) {
      if (!stop_file.empty() && std::ifstream(stop_file).good()) break;
      const std::string ix = uuid7(rng);
      ++n;
      later(0, [this, ix] { enqueue(ix); });
      std::this_thread::sleep_for(std::chrono::microseconds(static_cast<int64_t>(1e6 / rate)));
    }
    arrivals_done = true;
    record({{"k", "arrivals_done"}, {"calls", n}});
    // Drain: every call connects, ends and wraps up (bounded).
    const int64_t drain_until = now_ms() + cfg.value("drain_s", 45) * 1000;
    for (;;) {
      size_t open = 0;
      {
        std::lock_guard lk(mu);
        for (const auto& [ix, c] : calls) {
          if (!c.wrapped && !c.overflowed) ++open;
        }
      }
      if (open == 0 || now_ms() > drain_until) break;
      {
        std::lock_guard lk(mu);
        judge_suspects(false);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    // Final check: a call that didn't finish must still be held by RE (not lost).
    std::vector<std::string> open_calls;
    {
      std::lock_guard lk(mu);
      for (const auto& [ix, c] : calls) {
        if (!c.wrapped && !c.overflowed) open_calls.push_back(ix);
      }
    }
    size_t still_held = 0;
    for (const auto& ix : open_calls) {
      bool connected = false;
      {
        std::lock_guard lk(mu);
        connected = calls[ix].connected;
      }
      if (reattach(ix, /*final_check=*/true)) {
        ++still_held;
      } else if (!connected) {
        std::lock_guard lk(mu);
        if (!calls[ix].entry.empty()) violations.push_back("lost entry at the end: " + ix);
      }
    }
    stop = true;
    jcv.notify_all();
    for (auto& t : targets) t.ch.reset();
    for (auto& w : workers) w.join();
    ie_th.detach();
    gw_th.detach();
    // The recording's checks.
    json out;
    std::lock_guard lk(mu);
    judge_suspects(true);
    size_t connected = 0, wrapped = 0, assigned_twice = 0;
    for (const auto& [ix, c] : calls) {
      connected += c.connected;
      wrapped += c.wrapped;
      if (c.reservations.size() > 1) ++assigned_twice;
    }
    std::sort(time_to_offer.begin(), time_to_offer.end());
    auto pct = [&](double p) {
      if (time_to_offer.empty()) return int64_t{0};
      return time_to_offer[std::min(time_to_offer.size() - 1, static_cast<size_t>(p * time_to_offer.size()))];
    };
    json gj = json::array();
    for (const auto& [a, b] : gaps) gj.push_back({{"from", a}, {"to", b}, {"ms", b - a}});
    out["calls"] = calls.size();
    out["connected"] = connected;
    out["wrapped"] = wrapped;
    out["open_at_end"] = open_calls.size();
    out["open_still_held"] = still_held;
    out["calls_reassigned"] = assigned_twice;  // re-offered after a revoke/expiry: legal, counted
    out["violations"] = violations;
    out["gaps"] = gj;
    out["rpcs_ok"] = rpcs_ok;
    out["rpcs_failed"] = rpcs_failed;
    out["stream_msgs"] = stream_msgs;
    out["replays_dropped"] = replays_dropped;
    out["resyncs"] = resyncs.load();
    out["max_leader_epoch"] = max_epoch.load();
    out["time_to_offer_ms"] = {{"p50", pct(0.50)}, {"p99", pct(0.99)}, {"max", time_to_offer.empty() ? 0 : time_to_offer.back()},
                               {"n", time_to_offer.size()}};
    return out;
  }
};

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::fprintf(stderr, "usage: chaos_driver <config.json> <summary.json>\n");
    return 2;
  }
  std::ifstream in(argv[1]);
  std::stringstream ss;
  ss << in.rdbuf();
  Driver d;
  d.cfg = json::parse(ss.str());
  const json out = d.run();
  std::ofstream(argv[2]) << out.dump(2) << "\n";
  std::printf("%s\n", out.dump().c_str());
  std::fflush(stdout);
  std::_Exit(out["violations"].empty() ? 0 : 1);
}
