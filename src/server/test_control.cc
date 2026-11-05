#include "src/server/test_control.h"

#include <future>
#include <map>
#include <nlohmann/json.hpp>
#include <set>

#include "src/ops/seed.h"

namespace pcr {

namespace {

using json = nlohmann::json;

int64_t num(const json& j) {
  if (j.is_string()) return std::stoll(j.get<std::string>());
  return j.get<int64_t>();
}

HttpReply ok_json(const json& j) { return HttpReply{200, "application/json", j.dump()}; }
HttpReply bad(const std::string& why) { return HttpReply{400, "text/plain", why + "\n"}; }

// Test queues: the given world names queues only; the rest are the phase-1 defaults, with a long
// max wait so a script's timers are the script's own.
QueueConfig test_queue(const std::string& id, int64_t offer_timeout_ms) {
  QueueConfig q;
  q.id = id;
  q.offer_timeout_ms = offer_timeout_ms;
  q.max_wait_ms = 3'600'000;
  q.max_wait_action = MaxWaitAction::kDisconnect;
  return q;
}

av1::AgentState state_named(const std::string& s) {
  av1::AgentState st = av1::AGENT_STATE_UNSPECIFIED;
  if (!av1::AgentState_Parse("AGENT_STATE_" + s, &st)) return av1::AGENT_STATE_UNSPECIFIED;
  return st;
}

template <class F>
void on_writer(Engine* e, const std::string& tenant, F f) {
  std::promise<void> p;
  auto fut = p.get_future();
  e->post(tenant, [&](Tenant* t, int64_t now) {
    f(t, now);
    p.set_value();
  }, /*external=*/false);
  fut.wait();
}

}  // namespace

HttpReply TestControl::handle(const std::string& method, const std::string& path, const std::string& body) {
  if (path == "/_test/v1/sync") {
    engine_->barrier();
    return ok_json({{"now_ms", clock_->now_ms()}});
  }
  if (path == "/_test/v1/invariants") {
    const auto tt = engine_->totals();
    return ok_json({{"violations", engine_->check_invariants()},
                    {"totals", {{"sweep_matches", tt.sweep_matches}, {"foreign_refused", tt.foreign_refused},
                                {"coverage_refused", tt.coverage_refused}, {"moves_refused", tt.moves_refused},
                                {"moves_applied", tt.moves_applied}, {"moves_pending", tt.moves_pending},
                                {"callbacks_dispatched", tt.callbacks_dispatched}, {"callbacks_flagged", tt.callbacks_flagged}}}});
  }
  if (method != "POST") return HttpReply{405, "text/plain", "POST\n"};
  if (path == "/_test/v1/reset") {
    current_ = boot_;
    engine_->load(current_, boot_epochs_);
    hub_->close_all();
    return ok_json({{"ok", true}});
  }
  if (path == "/_test/v1/seed") return seed(body);
  if (path == "/_test/v1/advance") {
    const json j = json::parse(body);
    const int64_t ms = num(j.at("ms"));
    if (ms < 0 || ms > 86'400'000) return bad("ms out of range");
    const int64_t to = clock_->now_ms() + ms;
    engine_->advance_to(clock_, to);
    hub_->tick(to);
    engine_->barrier();
    return ok_json({{"now_ms", to}});
  }
  if (path == "/_test/v1/inject") return inject(body);
  if (path == "/_test/v1/restart") {
    // The phase-1 restart rule, simulated: state lost, a higher incarnation, every stream closed.
    const json j = json::parse(body);
    const uint64_t inc = static_cast<uint64_t>(num(j.at("incarnation")));
    if (inc < 1 || inc <= engine_->epochs().leader_epoch) return bad("the incarnation must rise");
    hub_->close_all();
    engine_->load(current_, Epochs{inc, (inc - 1) << 32});
    return ok_json({{"leader_epoch", inc}});
  }
  return HttpReply{404, "text/plain", "unknown\n"};
}

HttpReply TestControl::seed(const std::string& body) {
  const json j = json::parse(body);
  const std::string kind = j.at("kind").get<std::string>();
  std::vector<TenantConfig> tenants;
  json sessions = json::array();
  Epochs ep;
  int64_t start = 0;
  if (kind == "seed") {
    std::string error;
    if (!parse_seed(j.at("seed").dump(), bin_, &tenants, &error)) return bad(error);
    start = num(j.at("start_ms"));
    ep = Epochs{static_cast<uint64_t>(num(j.value("leader_epoch", json(1)))), 0};
    if (j.contains("seq_base")) ep.seq_base = static_cast<uint64_t>(num(j.at("seq_base")));
    clock_->set(start);
    hub_->close_all();
    engine_->load(tenants, ep);
    current_ = tenants;
    return ok_json({{"tenants", tenants.size()}});
  }
  const json& g = j.at("given");
  start = num(g.at("start_ms"));
  ep = Epochs{static_cast<uint64_t>(num(g.at("leader_epoch"))), 0};
  const int64_t offer_ms = g.contains("offer_timeout_ms") ? num(g.at("offer_timeout_ms")) : 15'000;
  std::map<std::string, TenantConfig> by;
  auto tenant = [&](const std::string& id) -> TenantConfig& {
    TenantConfig& t = by[id];
    t.id = id;
    if (g.contains("assignment_deadline_ms")) t.assignment_deadline_ms = num(g.at("assignment_deadline_ms"));
    if (g.contains("grace_ms")) t.grace_ms = num(g.at("grace_ms"));
    // Phase 3 (0.11.0): direct offers' max wait, callbacks' deadline.
    if (g.contains("direct_max_wait_ms")) t.direct_max_wait_ms = num(g.at("direct_max_wait_ms"));
    if (g.contains("callback_deadline_ms")) t.callback_deadline_ms = num(g.at("callback_deadline_ms"));
    if (g.contains("reject_skip_ms")) t.reject_skip_ms = num(g.at("reject_skip_ms"));
    // CR-083's owner-loss cleanup is not what a script tests: beyond any script's horizon.
    t.owner_loss_ms = g.contains("owner_loss_ms") ? num(g.at("owner_loss_ms")) : 3'600'000;
    return t;
  };
  if (kind == "routing_given") {
    for (const auto& t : g.at("tenants")) tenant(t.get<std::string>());
    for (const auto& q : g.at("queues")) {
      QueueConfig qc = test_queue(q.at("queue_id").get<std::string>(), offer_ms);
      // 0.9.0: a given queue may name its max-wait action (OverflowAction names).
      if (q.contains("max_wait_ms")) qc.max_wait_ms = num(q.at("max_wait_ms"));
      if (q.contains("max_wait_action")) {
        rv1::OverflowAction a = rv1::OVERFLOW_ACTION_UNSPECIFIED;
        if (!rv1::OverflowAction_Parse(q.at("max_wait_action").get<std::string>(), &a) ||
            a == rv1::OVERFLOW_ACTION_UNSPECIFIED) {
          return bad("given queue max_wait_action: QUEUE, VOICEMAIL, CALLBACK_OFFER, DISCONNECT or FLOW_STEP");
        }
        qc.max_wait_action = static_cast<MaxWaitAction>(a);
        qc.max_wait_queue = q.value("max_wait_queue_id", std::string());
        qc.max_wait_flow_step = q.value("max_wait_flow_step", std::string());
      }
      tenant(q.at("tenant_id").get<std::string>()).queues.push_back(qc);
    }
    for (const auto& a : g.at("agents")) {
      AgentConfig ac;
      ac.id = a.at("agent_id").get<std::string>();
      ac.contact = a.value("agent_contact", std::string());
      for (const auto& q : a.at("queues")) ac.queues.push_back(q.get<std::string>());
      if (a.contains("skills")) {
        for (const auto& [k, v] : a.at("skills").items()) ac.skills[k] = v.get<uint32_t>();
      }
      tenant(a.at("tenant_id").get<std::string>()).agents.push_back(ac);
    }
  } else if (kind == "agents_given") {
    std::set<std::pair<std::string, std::string>> qs;
    const json given_queues = j.value("queues", json::array());  // never iterate a temporary
    for (const auto& q : given_queues) {
      qs.insert({q.at("tenant_id").get<std::string>(), q.at("queue_id").get<std::string>()});
    }
    const json declared = g.value("queues", json::array());  // 0.11.0: a script may declare them
    for (const auto& q : declared) qs.insert({q.at("tenant_id").get<std::string>(), q.at("queue_id").get<std::string>()});
    for (const auto& a : g.at("agents")) {
      const std::string t = a.at("tenant_id").get<std::string>();
      if (std::none_of(qs.begin(), qs.end(), [&](const auto& p) { return p.first == t; })) qs.insert({t, "sales"});
    }
    for (const auto& [t, q] : qs) tenant(t).queues.push_back(test_queue(q, offer_ms));
    for (const auto& a : g.at("agents")) {
      const std::string t = a.at("tenant_id").get<std::string>();
      AgentConfig ac;
      ac.id = a.at("agent_id").get<std::string>();
      if (a.contains("queues")) {  // 0.11.0: explicit membership
        for (const auto& q : a.at("queues")) ac.queues.push_back(q.get<std::string>());
      } else {
        for (const auto& [qt, q] : qs) {
          if (qt == t) ac.queues.push_back(q);
        }
      }
      tenant(t).agents.push_back(ac);
    }
  } else {
    return bad("kind: seed, routing_given or agents_given");
  }
  // 0.11.0: the tenants' config.routing and config.routing_pin documents at the start.
  const json routing = g.value("routing", json::array());
  for (const auto& d : routing) {
    RoutingPolicy p;
    std::string tid, error;
    if (!parse_routing_doc(d.dump(), &p, &tid, &error)) return bad("given routing: " + error);
    if (!by.contains(tid)) return bad("given routing of an unknown tenant");
    by[tid].routing = p;
  }
  const json pins = g.value("routing_pins", json::array());
  for (const auto& d : pins) {
    RoutingPin p;
    bool tomb = false;
    std::string tid, error;
    if (!parse_pin_doc(d.dump(), &p, &tomb, &tid, &error) || tomb) return bad("given routing_pins: " + error);
    if (!by.contains(tid)) return bad("given pin of an unknown tenant");
    by[tid].pins.push_back(p);
  }
  for (auto& [id, t] : by) tenants.push_back(t);
  clock_->set(start);
  hub_->close_all();
  engine_->load(tenants, ep);
  current_ = tenants;
  // Bring the agents to the given state on their writers.
  for (const auto& a : g.at("agents")) {
    const std::string t = a.at("tenant_id").get<std::string>();
    const std::string id = a.at("agent_id").get<std::string>();
    if (kind == "routing_given") {
      std::optional<int64_t> since;
      if (a.contains("ready_since_ms")) since = num(a.at("ready_since_ms"));
      Tenant::SeededSession s;
      on_writer(engine_, t, [&](Tenant* tn, int64_t now) {
        s = tn->seed_attached(id, "gw-script", a.value("agent_contact", std::string()), since, now);
      });
      sessions.push_back({{"tenant_id", t}, {"agent_id", id}, {"session_id", s.session_id},
                          {"attach_generation", s.attach_generation}, {"replica_id", "gw-script"}});
    } else {
      const std::string res = a.contains("reservation") ? a.at("reservation").at("reservation_id").get<std::string>() : "";
      const std::string ix = a.contains("reservation") ? a.at("reservation").at("interaction_id").get<std::string>() : "";
      on_writer(engine_, t, [&](Tenant* tn, int64_t now) {
        tn->seed_state(id, state_named(a.at("state").get<std::string>()), a.value("reason_code", std::string()), res, ix, now);
      });
    }
  }
  return ok_json({{"sessions", sessions}});
}

HttpReply TestControl::inject(const std::string& body) {
  const json j = json::parse(body);
  const std::string what = j.at("what").get<std::string>();
  const json& a = j.at("args");
  const std::string t = a.at("tenant_id").get<std::string>();
  json out;
  std::string error;
  if (what == "queue_call") {
    on_writer(engine_, t, [&](Tenant* tn, int64_t now) {
      if (!tn) {
        error = "unknown tenant";
        return;
      }
      auto r = tn->inject_queue_call(a.at("interaction_id").get<std::string>(), a.at("queue_id").get<std::string>(), now,
                                     a.value("priority", 0u));
      if (r) {
        out["entry_id"] = r->entry().entry_id();
      } else {
        error = r.error().parity;
      }
    });
  } else if (what == "call_connected") {
    on_writer(engine_, t, [&](Tenant* tn, int64_t now) {
      if (!tn) {
        error = "unknown tenant";
        return;
      }
      auto r = tn->inject_call_connected(a.at("reservation_id").get<std::string>(), now);
      if (!r) error = r.error().parity;
    });
  } else if (what == "config_routing" || what == "config_routing_pin") {
    // As the config reader would apply a record: `key` is the record's key (the tenant, or
    // {tenant}/{agent_id}); a document naming another tenant than its key is refused and counted.
    const json& doc = a.at("doc");
    std::string tid, err_text;
    RoutingPolicy policy;
    RoutingPin pin;
    bool tomb = false;
    const bool parsed = what == "config_routing" ? parse_routing_doc(doc.dump(), &policy, &tid, &err_text)
                                                 : parse_pin_doc(doc.dump(), &pin, &tomb, &tid, &err_text);
    if (!parsed) return bad(err_text);
    const std::string key = a.value("key", what == "config_routing" ? tid : tid + "/" + pin.agent_id);
    const std::string key_tenant = key.substr(0, key.find('/'));
    bool applied = false;
    std::string why;
    on_writer(engine_, key_tenant, [&](Tenant* tn, int64_t now) {
      if (!tn) {
        error = "unknown tenant";
        return;
      }
      if (key_tenant != tid || (what == "config_routing_pin" && key != tid + "/" + pin.agent_id)) {
        tn->count_foreign();
        why = "the key names another tenant or agent";
        return;
      }
      applied = what == "config_routing" ? tn->install_policy(policy, now, &why) : tn->install_pin(pin, tomb, now, &why);
    });
    out["applied"] = applied;
    if (!applied) out["why"] = why;
  } else if (what == "moves") {
    std::vector<Tenant::Move> moves;
    for (const auto& m : a.at("moves")) {
      Tenant::Move mv;
      mv.agent_id = m.at("agent_id").get<std::string>();
      for (const auto& q : m.at("queues")) {
        const std::string tier = q.at("tier").get<std::string>();
        mv.queues.emplace_back(q.at("queue_id").get<std::string>(),
                               tier == "MEMBERSHIP_TIER_PRIMARY" || tier == "primary" ? Tier::kPrimary
                               : tier == "MEMBERSHIP_TIER_SECONDARY" || tier == "secondary" ? Tier::kSecondary
                                                                                            : Tier::kNone);
        if (q.contains("weight")) mv.weights[mv.queues.back().first] = q.at("weight").get<double>();
      }
      moves.push_back(std::move(mv));
    }
    Tenant::MoveResult res;
    on_writer(engine_, t, [&](Tenant* tn, int64_t now) {
      if (!tn) {
        error = "unknown tenant";
        return;
      }
      res = tn->apply_moves(moves, now);
    });
    out = {{"applied", res.applied}, {"pending", res.pending}, {"silent", res.silent}, {"refused", res.refused}, {"why", res.why}};
  } else if (what == "call_ended") {
    on_writer(engine_, t, [&](Tenant* tn, int64_t now) {
      if (!tn) {
        error = "unknown tenant";
        return;
      }
      auto r = tn->inject_call_ended(a.at("reservation_id").get<std::string>(), now);
      if (!r) error = r.error().parity;
    });
  } else {
    return bad("what: queue_call, call_connected, call_ended, config_routing, config_routing_pin or moves");
  }
  if (!error.empty()) return bad(error);
  return ok_json(out);
}

}  // namespace pcr
