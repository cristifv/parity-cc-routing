#include "src/ops/seed.h"

#include <map>
#include <nlohmann/json.hpp>
#include <set>

namespace pcr {

namespace {

using json = nlohmann::json;

struct Bad {
  std::string what;
};

int64_t secs(const json& j, const char* k, int64_t dflt, int64_t lo, int64_t hi) {
  if (!j.contains(k)) return dflt * 1000;
  if (!j.at(k).is_number_integer()) throw Bad{std::string(k) + " must be an integer"};
  const int64_t v = j.at(k).get<int64_t>();
  if (v < lo || v > hi) throw Bad{std::string(k) + " out of range"};
  return v * 1000;
}

QueueConfig parse_queue(const json& q) {
  QueueConfig c;
  c.id = q.at("id").get<std::string>();
  if (!valid_queue_id(c.id)) throw Bad{"queue id " + c.id};
  if (q.contains("required_skills")) {
    for (const auto& r : q.at("required_skills")) {
      SkillReq s{r.at("skill_id").get<std::string>(), r.value("min_level", 0u)};
      if (s.skill_id.empty() || s.min_level > 5) throw Bad{"queue " + c.id + ": a required skill"};
      c.required.push_back(s);
    }
  }
  c.default_priority = q.value("priority", 50u);
  if (c.default_priority < 1 || c.default_priority > 100) throw Bad{"queue " + c.id + ": priority 1-100"};
  c.offer_timeout_ms = secs(q, "offer_timeout_s", 15, 5, 120);
  c.wrap_up_limit_ms = secs(q, "wrap_up_limit_s", 60, 0, 3600);
  c.sla_ms = secs(q, "sla_s", 20, 1, 3600);
  c.escalate_after_ms = q.contains("escalate_after_s") ? secs(q, "escalate_after_s", 0, 1, 86400) : 3 * c.sla_ms;
  if (!q.contains("max_wait_s") || !q.contains("max_wait_action")) {
    throw Bad{"queue " + c.id + ": max_wait_s and max_wait_action are required"};
  }
  c.max_wait_ms = secs(q, "max_wait_s", 0, 1, 86400);
  const json& a = q.at("max_wait_action");
  const std::string act = a.at("action").get<std::string>();
  if (act == "disconnect") {
    c.max_wait_action = MaxWaitAction::kDisconnect;
  } else if (act == "voicemail") {
    c.max_wait_action = MaxWaitAction::kVoicemail;  // L13; the task itself is phase 3
  } else if (act == "queue") {
    c.max_wait_action = MaxWaitAction::kQueue;
    c.max_wait_queue = a.at("queue_id").get<std::string>();
  } else if (act == "flow_step") {
    c.max_wait_action = MaxWaitAction::kFlowStep;
    c.max_wait_flow_step = a.at("flow_step").get<std::string>();
    if (c.max_wait_flow_step.empty()) throw Bad{"queue " + c.id + ": flow_step"};
  } else if (act == "callback_offer") {
    c.max_wait_action = MaxWaitAction::kCallbackOffer;  // phase 3 (L15)
  } else {
    throw Bad{"queue " + c.id + ": max_wait_action " + act + " (disconnect, voicemail, queue, flow_step, callback_offer)"};
  }
  c.max_entries = q.value("max_entries", 1000u);
  if (c.max_entries < 1 || c.max_entries > 100000) throw Bad{"queue " + c.id + ": max_entries"};
  c.aht_ms = secs(q, "aht_s", 180, 1, 7200);
  return c;
}

Tier tier_named(const std::string& t) {
  if (t == "primary") return Tier::kPrimary;
  if (t == "secondary") return Tier::kSecondary;
  throw Bad{"tier must be primary or secondary"};
}

uint64_t version_of(const json& j) {
  if (!j.contains("version") || !j.at("version").is_number_integer() || j.at("version").get<int64_t>() < 1) {
    throw Bad{"version must be an integer >= 1"};
  }
  return j.at("version").get<uint64_t>();
}

RoutingPolicy routing_from(const json& d) {
  RoutingPolicy p;
  p.version = version_of(d);
  const std::string mode = d.value("mode", std::string("off"));
  if (mode == "off") {
    p.mode = RoutingMode::kOff;
  } else if (mode == "advise") {
    p.mode = RoutingMode::kAdvise;
  } else if (mode == "auto") {
    p.mode = RoutingMode::kAuto;
  } else {
    throw Bad{"mode must be off, advise or auto"};
  }
  p.dwell_min_ms = secs(d, "dwell_min_s", 300, 60, 3600);
  if (d.contains("max_moves_per_hour")) {
    const int64_t m = d.at("max_moves_per_hour").get<int64_t>();
    if (m < 0 || m > 12) throw Bad{"max_moves_per_hour 0-12"};
    p.max_moves_per_hour = static_cast<int>(m);
  }
  p.reject_skip_ms = d.contains("reject_skip_s") ? secs(d, "reject_skip_s", 30, 0, 300) : -1;
  p.vip_skill = d.value("vip_skill", std::string());
  for (const auto& st : d.value("proficiency_speed", json::array())) {
    const int64_t level = st.at("min_level").get<int64_t>();
    const double speed = st.at("speed").get<double>();
    if (level < 0 || level > 100 || !(speed >= 0.25 && speed <= 4)) throw Bad{"proficiency_speed: min_level 0-100, speed 0.25-4"};
    if (!p.proficiency_speed.empty() && static_cast<uint32_t>(level) <= p.proficiency_speed.back().first) {
      throw Bad{"proficiency_speed: rising min_level"};
    }
    p.proficiency_speed.emplace_back(static_cast<uint32_t>(level), speed);
  }
  std::set<std::string> seen;
  for (const auto& q : d.value("queues", json::array())) {
    QueuePolicy qp;
    qp.queue_id = q.at("queue_id").get<std::string>();
    if (!valid_queue_id(qp.queue_id) || !seen.insert(qp.queue_id).second) throw Bad{"routing queue " + qp.queue_id};
    qp.alpha = q.value("alpha", 1.0);
    qp.beta = q.value("beta", 1.0);
    if (!(qp.alpha > 0) || !(qp.beta >= 0)) throw Bad{"routing queue " + qp.queue_id + ": alpha > 0, beta >= 0"};
    qp.min_agents = q.value("min_agents", 0);
    qp.max_agents = q.value("max_agents", 0);
    if (qp.min_agents < 0 || qp.max_agents < 0 || (qp.max_agents > 0 && qp.min_agents > qp.max_agents)) {
      throw Bad{"routing queue " + qp.queue_id + ": min_agents/max_agents"};
    }
    qp.secondary_delay_ms = secs(q, "secondary_delay_s", 10, 0, 300);
    qp.escalate_after_ms = q.contains("escalate_after_s") ? secs(q, "escalate_after_s", 0, 1, 86400) : 0;
    // Phase 5 redesign (config.routing 0.12.0): the levers' defaults and bounds, VIP.
    qp.weight = q.value("weight", 1.0);
    qp.weight_min = q.value("weight_min", 0.25);
    qp.weight_max = q.value("weight_max", 4.0);
    if (!(qp.weight_min >= 0.25) || !(qp.weight_max <= 4) || !(qp.weight_min <= qp.weight) || !(qp.weight <= qp.weight_max)) {
      throw Bad{"routing queue " + qp.queue_id + ": weight_min <= weight <= weight_max within 0.25-4"};
    }
    qp.delay_min_ms = secs(q, "delay_min_s", 0, 0, 300);
    qp.delay_max_ms = secs(q, "delay_max_s", std::max<int64_t>(60, qp.secondary_delay_ms / 1000), 0, 300);
    if (qp.delay_min_ms > qp.secondary_delay_ms || qp.secondary_delay_ms > qp.delay_max_ms) {
      throw Bad{"routing queue " + qp.queue_id + ": delay_min_s <= secondary_delay_s <= delay_max_s"};
    }
    qp.sl_target = q.value("sl_target", 0.8);
    if (!(qp.sl_target > 0 && qp.sl_target <= 1)) throw Bad{"routing queue " + qp.queue_id + ": sl_target 0-1"};
    qp.vip_sla_ms = q.contains("vip_sla_s") ? secs(q, "vip_sla_s", 0, 1, 3600) : 0;
    qp.vip_alpha = q.value("vip_alpha", 2.0);
    qp.vip_beta = q.value("vip_beta", 2.0);
    if (!(qp.vip_alpha > 0) || !(qp.vip_beta >= 0)) throw Bad{"routing queue " + qp.queue_id + ": vip_alpha > 0, vip_beta >= 0"};
    qp.vip_reserved_min = q.value("vip_reserved_min", 0);
    if (qp.vip_reserved_min < 0 || qp.vip_reserved_min > 1000) throw Bad{"routing queue " + qp.queue_id + ": vip_reserved_min 0-1000"};
    p.queues.push_back(qp);
  }
  return p;
}

RoutingPin pin_from(const json& d, bool* tombstone) {
  RoutingPin p;
  p.agent_id = d.at("agent_id").get<std::string>();
  if (!valid_agent_id(p.agent_id)) throw Bad{"pin agent_id"};
  *tombstone = d.value("tombstone", false);
  if (*tombstone) {
    p.version = d.contains("version") ? version_of(d) : 0;
    return p;
  }
  p.version = version_of(d);
  std::set<std::string> seen;
  for (const auto& q : d.value("queues", json::array())) {
    const std::string id = q.at("queue_id").get<std::string>();
    if (!valid_queue_id(id) || !seen.insert(id).second) throw Bad{"pin queue " + id};
    p.queues.emplace_back(id, tier_named(q.at("tier").get<std::string>()));
  }
  for (const auto& q : d.value("exclusions", json::array())) {
    const std::string id = q.get<std::string>();
    if (!valid_queue_id(id)) throw Bad{"pin exclusion " + id};
    p.exclusions.push_back(id);
  }
  if (p.queues.empty() && p.exclusions.empty()) throw Bad{"a pin names queues or exclusions"};
  p.until_ms = d.value("until_ms", int64_t{0});
  if (p.until_ms < 0) throw Bad{"until_ms"};
  p.reason_code = d.value("reason_code", std::string());
  return p;
}

}  // namespace

bool parse_routing_doc(const std::string& text, RoutingPolicy* out, std::string* tenant, std::string* error) {
  try {
    const json d = json::parse(text);
    *tenant = d.at("tenant_id").get<std::string>();
    if (!valid_tenant_id(*tenant)) throw Bad{"tenant_id"};
    *out = routing_from(d);
    return true;
  } catch (const Bad& b) {
    *error = b.what;
  } catch (const std::exception& e) {
    *error = std::string("config.routing: ") + e.what();
  }
  return false;
}

bool parse_pin_doc(const std::string& text, RoutingPin* out, bool* tombstone, std::string* tenant,
                   std::string* error) {
  try {
    const json d = json::parse(text);
    *tenant = d.at("tenant_id").get<std::string>();
    if (!valid_tenant_id(*tenant)) throw Bad{"tenant_id"};
    *out = pin_from(d, tombstone);
    return true;
  } catch (const Bad& b) {
    *error = b.what;
  } catch (const std::exception& e) {
    *error = std::string("config.routing_pin: ") + e.what();
  }
  return false;
}

namespace {
bool parse_seed_impl(const std::string& text, const std::string& bin, std::vector<TenantConfig>* out,
                     std::vector<std::pair<std::string, TenantConfig>>* all, std::string* error);
}  // namespace

bool parse_seed(const std::string& text, const std::string& bin, std::vector<TenantConfig>* out,
                std::string* error) {
  return parse_seed_impl(text, bin, out, nullptr, error);
}

bool parse_seed_all(const std::string& text, std::vector<std::pair<std::string, TenantConfig>>* out, std::string* error) {
  return parse_seed_impl(text, "", nullptr, out, error);
}

namespace {
bool parse_seed_impl(const std::string& text, const std::string& bin, std::vector<TenantConfig>* out,
                     std::vector<std::pair<std::string, TenantConfig>>* all, std::string* error) {
  try {
    const json doc = json::parse(text);
    if (doc.value("schema_version", 0) != 1) throw Bad{"schema_version must be 1"};
    std::set<std::string> seen;
    for (const auto& t : doc.at("tenants")) {
      TenantConfig tc;
      tc.id = t.at("id").get<std::string>();
      if (!valid_tenant_id(tc.id)) throw Bad{"tenant id " + tc.id};
      if (!seen.insert(tc.id).second) throw Bad{"tenant " + tc.id + " twice"};
      tc.reject_skip_ms = secs(t, "reject_skip_s", 30, 0, 300);
      tc.assignment_deadline_ms = secs(t, "assignment_deadline_s", 30, 5, 300);
      tc.grace_ms = secs(t, "grace_s", 60, 1, 3600);
      tc.owner_loss_ms = secs(t, "owner_loss_s", 60, 10, 3600);  // CR-083
      // Phase 3: direct offers and callbacks.
      tc.direct_max_wait_ms = secs(t, "direct_max_wait_s", 60, 5, 3600);
      if (t.contains("direct_max_wait_action")) {
        const json& a = t.at("direct_max_wait_action");
        const std::string act = a.at("action").get<std::string>();
        if (act == "voicemail") {
          tc.direct_max_wait_action = MaxWaitAction::kVoicemail;
        } else if (act == "disconnect") {
          tc.direct_max_wait_action = MaxWaitAction::kDisconnect;
        } else if (act == "flow_step") {
          tc.direct_max_wait_action = MaxWaitAction::kFlowStep;
          tc.direct_max_wait_flow_step = a.at("flow_step").get<std::string>();
        } else {
          throw Bad{"tenant " + tc.id + ": direct_max_wait_action voicemail, disconnect or flow_step"};
        }
      }
      tc.callback_deadline_ms = secs(t, "callback_deadline_s", 120, 10, 3600);
      tc.callback_retry_after_ms = secs(t, "callback_retry_after_s", 900, 10, 86400);
      tc.callback_max_attempts = t.value("callback_max_attempts", 3);
      if (tc.callback_max_attempts < 1 || tc.callback_max_attempts > 10) throw Bad{"tenant " + tc.id + ": callback_max_attempts 1-10"};
      std::set<std::string> qids;
      for (const auto& q : t.at("queues")) {
        tc.queues.push_back(parse_queue(q));
        if (!qids.insert(tc.queues.back().id).second) throw Bad{"queue " + tc.queues.back().id + " twice"};
      }
      std::map<std::string, std::string> next;
      for (const auto& q : tc.queues) {
        if (q.max_wait_action != MaxWaitAction::kQueue) continue;
        if (!qids.contains(q.max_wait_queue) || q.max_wait_queue == q.id) {
          throw Bad{"queue " + q.id + ": max_wait_action queue names an unknown queue"};
        }
        next[q.id] = q.max_wait_queue;
      }
      // CR-057: a chain of QUEUE actions must end in a terminal action (sales -> support -> sales
      // would move a caller forever: the bounded-wait rule).
      for (const auto& [start, first] : next) {
        std::set<std::string> seen{start};
        std::string path = start, at = first;
        for (;;) {
          path += " -> " + at;
          if (!seen.insert(at).second) throw Bad{"tenant " + tc.id + ": max_wait_action queue chain " + path + " never ends"};
          auto n = next.find(at);
          if (n == next.end()) break;
          at = n->second;
        }
      }
      std::set<std::string> aids;
      for (const auto& a : t.at("agents")) {
        AgentConfig ac;
        ac.id = a.at("id").get<std::string>();
        if (!valid_agent_id(ac.id) || ac.id.find('@') != std::string::npos) throw Bad{"agent id " + ac.id};
        if (!aids.insert(ac.id).second) throw Bad{"agent " + ac.id + " twice"};
        ac.contact = a.value("contact", std::string());
        if (a.contains("skills")) {
          for (const auto& [k, v] : a.at("skills").items()) {
            const uint32_t lvl = v.get<uint32_t>();
            if (lvl < 1 || lvl > 5) throw Bad{"agent " + ac.id + ": skill level 1-5"};
            ac.skills[k] = lvl;
          }
        }
        for (const auto& q : a.at("queues")) {
          const std::string qid = q.get<std::string>();
          if (!qids.contains(qid)) throw Bad{"agent " + ac.id + ": unknown queue " + qid};
          ac.queues.push_back(qid);
        }
        tc.agents.push_back(std::move(ac));
      }
      // Phase 4: config.routing and config.routing_pin, until admin-console publishes them. Tenant
      // isolation: a document of another tenant, or a pin naming an agent or queue this tenant
      // doesn't have, is refused.
      if (t.contains("routing")) {
        const json& d = t.at("routing");
        if (d.value("tenant_id", tc.id) != tc.id) throw Bad{"tenant " + tc.id + ": routing of another tenant"};
        tc.routing = routing_from(d);
        for (const auto& qp : tc.routing.queues) {
          if (!qids.contains(qp.queue_id)) throw Bad{"tenant " + tc.id + ": routing names unknown queue " + qp.queue_id};
        }
      }
      std::set<std::string> pinned;
      for (const auto& d : t.value("routing_pins", json::array())) {
        if (d.value("tenant_id", tc.id) != tc.id) throw Bad{"tenant " + tc.id + ": a pin of another tenant"};
        bool tomb = false;
        RoutingPin p = pin_from(d, &tomb);
        if (tomb) continue;
        if (!aids.contains(p.agent_id)) throw Bad{"tenant " + tc.id + ": a pin names unknown agent " + p.agent_id};
        for (const auto& [q, tier] : p.queues) {
          if (!qids.contains(q)) throw Bad{"tenant " + tc.id + ": a pin names unknown queue " + q};
        }
        for (const auto& q : p.exclusions) {
          if (!qids.contains(q)) throw Bad{"tenant " + tc.id + ": a pin names unknown queue " + q};
        }
        if (!pinned.insert(p.agent_id).second) throw Bad{"tenant " + tc.id + ": agent " + p.agent_id + " pinned twice"};
        tc.pins.push_back(std::move(p));
      }
      if (all) {
        all->emplace_back(t.value("bin", std::string()), std::move(tc));  // "": every bin (as parse_seed)
      } else if (t.value("bin", bin) == bin) {
        out->push_back(std::move(tc));
      }
    }
    return true;
  } catch (const Bad& b) {
    *error = b.what;
  } catch (const std::exception& e) {
    *error = std::string("seed: ") + e.what();
  }
  return false;
}
}  // namespace

}  // namespace pcr
