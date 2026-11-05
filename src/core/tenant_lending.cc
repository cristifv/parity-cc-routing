// Phase 8: campaign lending (IE step 13a; parity-contracts CONVENTIONS "Campaign lending"; PLAN §4.3
// "Mode"). An agent works inbound (routed here) or for one outbound campaign (dialed by IE's campaign
// leader), never both. This registry owns the mode flag:
//   - a lend or a return switches the agent at once when it is idle (no offer, no call), else when
//     its offer resolves or its wrap-up ends (pending, reported); never mid-call;
//   - a lent agent has an empty inbound active set, is never offered a call (agent_available) and is
//     out of the logged-in counts and the model (refresh_agent's `counted`);
//   - lending that would leave an open queue with no logged-in agent who can take it is refused,
//     not deferred (pending lends count as gone: effective() is their next, empty set);
//   - the campaign's leader is fenced by CampaignOwner.owner_epoch; it gets each lent agent's
//     AgentAvailability on its Watch stream (delivery_seq 0, latest wins, a snapshot at stream open);
//   - the mode, pending switches and campaign leaders are in re.state (tenant_state.cc).
#include <algorithm>
#include <cstdio>
#include <tuple>

#include "src/core/tenant.h"
#include "src/ops/log.h"

namespace pcr {

namespace {

constexpr int64_t kAssignGrace = 2'000;  // L7: deadline_ms + 2 s (as tenant.cc)
constexpr size_t kMaxAgentsPerRequest = 1000;

bool engaged(av1::AgentState s) {
  return s == av1::AGENT_STATE_OFFERED || s == av1::AGENT_STATE_RESERVED || s == av1::AGENT_STATE_ON_CALL ||
         s == av1::AGENT_STATE_WRAP_UP;
}

std::string json_str(const std::string& v) {
  std::string out = "\"";
  for (unsigned char c : v) {
    if (c == '"' || c == '\\') {
      out += '\\';
      out += static_cast<char>(c);
    } else if (c < 0x20) {
      char buf[8];
      std::snprintf(buf, sizeof buf, "\\u%04x", c);
      out += buf;
    } else {
      out += static_cast<char>(c);
    }
  }
  return out + "\"";
}

}  // namespace

// ---- the campaign and its leader ----------------------------------------------------------------

Result<Tenant::Campaign*> Tenant::campaign_owned(const rv1::CampaignOwner& o, bool create) {
  if (o.tenant_id() != id_) return err(Code::kInvalidArgument, "tenant_mismatch", "the campaign's tenant");
  auto it = campaigns_.find(o.campaign_id());
  if (it == campaigns_.end()) {
    if (!create) return err(Code::kFailedPrecondition, "agent_not_lent", "no agent is lent to this campaign");
    it = campaigns_.emplace(o.campaign_id(), Campaign{o.campaign_id(), o.replica_id(), o.owner_epoch()}).first;
    touch_camp(it->first);
    return &it->second;
  }
  Campaign& c = it->second;
  if (o.owner_epoch() < c.owner_epoch) return err(Code::kAborted, "stale_owner", "a newer campaign leader exists");
  if (o.owner_epoch() > c.owner_epoch || o.replica_id() != c.replica) {
    // A new leader (or the leader's new replica) takes the campaign over: its stream gets every
    // lent agent's availability from now on, starting with a snapshot.
    c.owner_epoch = o.owner_epoch();
    c.replica = o.replica_id();
    touch_camp(c.id);
    send_campaign_snapshot(c);
  }
  return &c;
}

// ---- LendAgents ---------------------------------------------------------------------------------

rv1::LendResult Tenant::lend_one(AgentSt& a, const std::string& campaign, int64_t now) {
  if (!a.campaign.empty()) {
    if (a.campaign != campaign) return rv1::LEND_RESULT_OTHER_CAMPAIGN;
    if (a.mode_pending) {
      // Lent with a return pending (on a campaign call): the return is cancelled, it stays lent.
      touch_agent(a.cfg.id);
      a.mode_pending = false;
      a.to_campaign.clear();
      retarget(a, av1::MEMBERSHIP_REASON_MODE_CHANGE, now, /*check_coverage=*/false);
      note_availability(a);
    }
    return rv1::LEND_RESULT_LENT;  // already lent here: a repeat answers the same
  }
  if (a.mode_pending) return a.to_campaign == campaign ? rv1::LEND_RESULT_PENDING : rv1::LEND_RESULT_OTHER_CAMPAIGN;
  if (!a.attached || a.state == av1::AGENT_STATE_LOGGED_OUT) return rv1::LEND_RESULT_NOT_LOGGED_IN;
  // PLAN §4.3 coverage: refused, not deferred. A deferred lend would keep the agent in limbo and
  // take it from inbound later, at a moment nobody chose; the leader asks for another instead.
  if (uncovers(a, effective(a), Set{})) {
    ++counters_->coverage_refused;
    LogRec r("info", "agent.lend_refused", "lending refused: it would leave an open queue uncovered");
    if (r.on()) r.tenant(id_).str("agent_id", a.cfg.id).attr("campaign_id", campaign).attr("reason", "coverage");
    return rv1::LEND_RESULT_COVERAGE;
  }
  touch_agent(a.cfg.id);
  if (!engaged(a.state) && a.offer_id.empty()) {
    // Idle (READY or not ready, no offer, no call): the switch is at once, atomically in this task.
    a.campaign = campaign;
    retarget(a, av1::MEMBERSHIP_REASON_MODE_CHANGE, now, /*check_coverage=*/false);  // active = {}
    refresh_agent(a);  // out of the longest-idle index and the logged-in counts
    event_mode(a, campaign, "lent", now);
    note_availability(a);
    return rv1::LEND_RESULT_LENT;
  }
  // An offer or a call: the switch waits (apply_mode at the next free state). Its next inbound set
  // is empty from now on (effective()), so coverage checks already count it as gone.
  a.mode_pending = true;
  a.to_campaign = campaign;
  retarget(a, av1::MEMBERSHIP_REASON_MODE_CHANGE, now, /*check_coverage=*/false);  // pending_set = {}
  note_availability(a);
  return rv1::LEND_RESULT_PENDING;
}

Result<rv1::LendAgentsResponse> Tenant::lend_agents(const rv1::LendAgentsRequest& r, int64_t now) {
  const bool by_count = r.count() > 0;
  if (by_count && r.agent_ids_size() > 0) return err(Code::kInvalidArgument, "invalid_request", "agent_ids or count, not both");
  if (!by_count && (r.requirements_size() > 0 || !r.request_id().empty())) {
    return err(Code::kInvalidArgument, "invalid_request", "requirements and request_id go with count");
  }
  if (static_cast<size_t>(r.agent_ids_size()) > kMaxAgentsPerRequest || r.count() > kMaxAgentsPerRequest) {
    return err(Code::kInvalidArgument, "invalid_request", "at most 1000 agents per request");
  }
  for (const auto& id : r.agent_ids()) {
    if (!valid_agent_id(id)) return err(Code::kInvalidArgument, "invalid_request", "agent_id");
  }
  std::vector<SkillReq> reqs;
  std::string fp = "count=" + std::to_string(r.count());
  for (const auto& q : r.requirements()) {
    if (q.skill_id().empty() || q.skill_id().size() > 64 || q.min_level() > 5) {
      return err(Code::kInvalidArgument, "invalid_request", "requirement");
    }
    reqs.push_back(SkillReq{q.skill_id(), q.min_level()});
    fp += ";" + q.skill_id() + ":" + std::to_string(q.min_level());
  }
  if (by_count && !valid_command_id(r.request_id())) return err(Code::kInvalidArgument, "invalid_request", "request_id");
  auto c = campaign_owned(r.owner(), /*create=*/true);
  if (!c) return std::unexpected(c.error());
  const std::string campaign = (*c)->id;
  rv1::LendAgentsResponse out;
  std::string key;
  if (by_count) {
    // Idempotent by request_id (kept 10 min with the commands): a repeat, a new leader's included,
    // returns the first answer's agents and lends nothing more.
    key = "lend:" + campaign + ":" + r.request_id();
    if (auto it = commands_.find(key); it != commands_.end()) {
      if (it->second.fingerprint != fp) return err(Code::kInvalidArgument, "invalid_request", "request_id reused for another request");
      rv1::LendAgentsResponse first;
      if (!first.ParseFromString(it->second.response)) return err(Code::kInternal, "internal", "stored answer unreadable");
      *out.mutable_agents() = first.agents();
      out.set_existing(true);
      lent_list(campaign, out.mutable_lent());
      return out;
    }
    // RE picks: inbound agents logged in with the skills; idle READY ones first (longest idle), then
    // busy ones (their switch pends), then not-ready ones; never the last one covering a queue.
    std::vector<std::tuple<int, int64_t, std::string>> cand;
    for (const auto& [id, a] : agents_) {
      if (!a.campaign.empty() || a.mode_pending || !a.attached || a.state == av1::AGENT_STATE_LOGGED_OUT) continue;
      if (!meets(a, reqs)) continue;
      const int rank = agent_available(a) ? 0 : (engaged(a.state) || !a.offer_id.empty()) ? 1 : 2;
      cand.emplace_back(rank, rank == 0 ? a.idle_since : 0, id);
    }
    std::sort(cand.begin(), cand.end());
    for (const auto& [rank, idle, id] : cand) {
      if (out.agents_size() >= static_cast<int>(r.count())) break;
      const rv1::LendResult res = lend_one(agents_.at(id), campaign, now);
      if (res != rv1::LEND_RESULT_LENT && res != rv1::LEND_RESULT_PENDING) continue;  // coverage: the next one
      auto* x = out.add_agents();
      x->set_agent_id(id);
      x->set_result(res);
    }
    Cmd cmd;
    cmd.fingerprint = fp;
    cmd.at = now;
    rv1::LendAgentsResponse stored;
    *stored.mutable_agents() = out.agents();
    if (stored.SerializeToString(&cmd.response)) {
      commands_[key] = std::move(cmd);
      touch_cmd(key);
    }
  } else {
    for (const auto& id : r.agent_ids()) {
      auto* x = out.add_agents();
      x->set_agent_id(id);
      // Tenant isolation: only this tenant's registry is searched; another tenant's agent is unknown.
      auto it = agents_.find(id);
      x->set_result(it == agents_.end() ? rv1::LEND_RESULT_UNKNOWN_AGENT : lend_one(it->second, campaign, now));
    }
  }
  lent_list(campaign, out.mutable_lent());
  settle(now);
  return out;
}

// ---- ReturnAgents -------------------------------------------------------------------------------

rv1::ReturnResult Tenant::return_one(AgentSt& a, const std::string& campaign, int64_t now) {
  if (a.campaign.empty()) {
    if (!a.mode_pending || a.to_campaign != campaign) return rv1::RETURN_RESULT_NOT_LENT;
    // A pending lend is cancelled: it never left inbound (no mode change to publish).
    touch_agent(a.cfg.id);
    a.mode_pending = false;
    a.to_campaign.clear();
    retarget(a, av1::MEMBERSHIP_REASON_MODE_CHANGE, now, /*check_coverage=*/false);
    send_availability(a, campaign, rv1::AVAILABILITY_STATE_RETURNED, 0);
    a.avail_sent = -1;
    return rv1::RETURN_RESULT_RETURNED;
  }
  if (a.campaign != campaign) return rv1::RETURN_RESULT_NOT_LENT;
  if (a.mode_pending) return rv1::RETURN_RESULT_PENDING;  // already going back after this call
  touch_agent(a.cfg.id);
  if (!engaged(a.state) && a.offer_id.empty()) {
    a.campaign.clear();
    a.primary_since = 0;  // no dwell owed (PLAN §4.3 mode)
    a.secondary_since = 0;
    retarget(a, av1::MEMBERSHIP_REASON_MODE_CHANGE, now, /*check_coverage=*/false);  // its inbound set
    refresh_agent(a);
    if (agent_available(a)) todo_agents_.push_back(a.cfg.id);
    send_availability(a, campaign, rv1::AVAILABILITY_STATE_RETURNED, 0);
    a.avail_sent = -1;
    event_mode(a, campaign, "returned", now);
    return rv1::RETURN_RESULT_RETURNED;
  }
  // On a campaign call: back once its wrap-up ends.
  a.mode_pending = true;
  a.to_campaign.clear();
  retarget(a, av1::MEMBERSHIP_REASON_MODE_CHANGE, now, /*check_coverage=*/false);  // pending_set = inbound
  return rv1::RETURN_RESULT_PENDING;
}

Result<rv1::ReturnAgentsResponse> Tenant::return_agents(const rv1::ReturnAgentsRequest& r, int64_t now) {
  if (static_cast<size_t>(r.agent_ids_size()) > kMaxAgentsPerRequest) {
    return err(Code::kInvalidArgument, "invalid_request", "at most 1000 agents per request");
  }
  for (const auto& id : r.agent_ids()) {
    if (!valid_agent_id(id)) return err(Code::kInvalidArgument, "invalid_request", "agent_id");
  }
  auto c = campaign_owned(r.owner(), /*create=*/true);
  if (!c) return std::unexpected(c.error());
  const std::string campaign = (*c)->id;
  rv1::ReturnAgentsResponse out;
  if (r.agent_ids_size() == 0) {
    // The campaign ended: every agent lent to it (pending lends included).
    for (auto& [id, a] : agents_) {
      if (mode_target(a) != campaign && a.campaign != campaign) continue;
      auto* x = out.add_agents();
      x->set_agent_id(id);
      x->set_result(return_one(a, campaign, now));
    }
  } else {
    for (const auto& id : r.agent_ids()) {
      auto* x = out.add_agents();
      x->set_agent_id(id);
      auto it = agents_.find(id);
      x->set_result(it == agents_.end() ? rv1::RETURN_RESULT_NOT_LENT : return_one(it->second, campaign, now));
    }
  }
  settle(now);  // returned agents may take waiting callers at once
  return out;
}

// ---- ReserveAgent -------------------------------------------------------------------------------

Result<rv1::ReserveAgentResponse> Tenant::reserve_agent(const rv1::ReserveAgentRequest& r, int64_t now) {
  if (!valid_agent_id(r.agent_id()) || !r.has_campaign()) {
    return err(Code::kInvalidArgument, "invalid_request", "agent_id or campaign");
  }
  const rv1::CampaignOwner& co = r.campaign();
  if (co.tenant_id() != id_) return err(Code::kInvalidArgument, "tenant_mismatch", "the campaign's tenant");
  if (!valid_agent_id(co.campaign_id()) || co.owner_epoch() < 1 || co.replica_id().empty()) {
    return err(Code::kInvalidArgument, "invalid_request", "campaign");
  }
  auto c = campaign_owned(co, /*create=*/false);
  if (!c) return std::unexpected(c.error());
  const std::string campaign = (*c)->id;
  auto own = owned(r.owner(), /*create=*/true, now);
  if (!own) return std::unexpected(own.error());
  Interaction& ix = **own;
  auto as_view = [](const Reservation& res) {
    rv1::Reservation v;
    v.set_reservation_id(res.id);
    v.set_agent_id(res.agent);
    v.set_agent_contact(res.contact);
    if (res.st == ResSt::kAssigned) {
      v.set_deadline_ms(res.deadline_ms);
      v.set_state(rv1::RESERVATION_STATE_ASSIGNED);
    } else if (res.st != ResSt::kEnded) {
      v.set_state(rv1::RESERVATION_STATE_CONNECTED);
    }
    return v;
  };
  // A repeat (same interaction and agent, for this campaign): the reservation made first.
  for (const auto& rid : ix.reservations) {
    auto res = reservations_.find(rid);
    if (res != reservations_.end() && res->second.agent == r.agent_id() && res->second.campaign == campaign &&
        res->second.st != ResSt::kEnded) {
      rv1::ReserveAgentResponse out;
      *out.mutable_reservation() = as_view(res->second);
      out.set_existing(true);
      return out;
    }
  }
  // Only an idle READY agent lent to this campaign (tenant isolation: this registry only).
  auto ait = agents_.find(r.agent_id());
  if (ait == agents_.end()) return err(Code::kFailedPrecondition, "agent_not_lent", "not lent to this campaign");
  AgentSt& a = ait->second;
  if (a.campaign != campaign || a.mode_pending || a.state != av1::AGENT_STATE_READY || !a.attached ||
      !a.offer_id.empty() || !a.reservations.empty()) {
    return err(Code::kFailedPrecondition, "agent_not_lent", "not lent to this campaign, or not idle and ready");
  }
  Reservation res;
  res.id = new_uuid_v7(now);
  res.interaction = ix.id;
  res.agent = a.cfg.id;
  res.contact = a.contact;
  res.deadline_ms = now + cfg_.assignment_deadline_ms;
  res.st = ResSt::kAssigned;
  res.campaign = campaign;
  ix.reservations.insert(res.id);
  hold_ix(ix.id);  // CR-187: it holds a live reservation
  touch_ix(ix.id);
  a.reservations.insert(res.id);
  reservations_.emplace(res.id, res);
  touch_res(res.id);
  touch_agent(a.cfg.id);
  set_agent_state(a, av1::AGENT_STATE_RESERVED, "", now, /*echo=*/false);
  av1::WatchResponse reserved;
  *reserved.mutable_reserved() = res_view(res);
  send_gw(a, std::move(reserved), now);
  av1::WatchResponse sc;
  *sc.mutable_state_changed()->mutable_agent() = view(a);
  send_gw(a, std::move(sc), now);
  timers_->at(res.deadline_ms + kAssignGrace, TimerRef{id_, TimerKind::kAssignDeadline, res.id});
  {
    LogRec lr("info", "campaign.reserved", "a lent agent reserved for a campaign call");
    if (lr.on()) lc(lr, ix.id).str("reservation_id", res.id).str("agent_id", a.cfg.id).attr("campaign_id", campaign)
                     .attr("deadline_in_ms", res.deadline_ms - now);
  }
  rv1::ReserveAgentResponse out;
  *out.mutable_reservation() = as_view(res);
  return out;
}

// ---- switches at idle points --------------------------------------------------------------------

void Tenant::apply_mode(AgentSt& a, int64_t now) {
  if (!a.mode_pending) return;
  touch_agent(a.cfg.id);
  const std::string from = a.campaign;
  a.campaign = a.to_campaign;
  a.mode_pending = false;
  a.to_campaign.clear();
  // The pending set the switch made ({} for a lend, the inbound set for a return) applies next
  // (set_agent_state: apply_pending), then refresh_agent and note_availability.
  if (a.campaign.empty()) {
    a.primary_since = 0;  // no dwell owed
    a.secondary_since = 0;
    send_availability(a, from, rv1::AVAILABILITY_STATE_RETURNED, 0);
    a.avail_sent = -1;
    event_mode(a, from, "returned", now);
  } else {
    event_mode(a, a.campaign, "lent", now);
  }
}

void Tenant::end_lending(AgentSt& a, int64_t now) {
  const std::string campaign = !a.campaign.empty() ? a.campaign : (a.mode_pending ? a.to_campaign : std::string());
  if (campaign.empty()) return;
  touch_agent(a.cfg.id);
  const bool was_lent = !a.campaign.empty();
  a.campaign.clear();
  a.mode_pending = false;
  a.to_campaign.clear();
  a.pending_set.reset();
  a.primary_since = 0;
  a.secondary_since = 0;
  // Logged out: inbound again, with its inbound set at once (it holds no offer and no call now).
  retarget(a, av1::MEMBERSHIP_REASON_MODE_CHANGE, now, /*check_coverage=*/false);
  send_availability(a, campaign, rv1::AVAILABILITY_STATE_RETURNED, 0);
  a.avail_sent = -1;
  if (was_lent) event_mode(a, campaign, "logged_out", now);
}

// ---- availability for the campaign's leader -----------------------------------------------------

rv1::AvailabilityState Tenant::availability(const AgentSt& a, int64_t* wrap_ends) const {
  *wrap_ends = 0;
  if (a.campaign.empty()) return a.mode_pending ? rv1::AVAILABILITY_STATE_PENDING : rv1::AVAILABILITY_STATE_RETURNED;
  switch (a.state) {
    case av1::AGENT_STATE_READY:
      return a.attached && a.offer_id.empty() && a.reservations.empty() && !a.mode_pending
                 ? rv1::AVAILABILITY_STATE_READY
                 : rv1::AVAILABILITY_STATE_NOT_READY;
    case av1::AGENT_STATE_RESERVED:
      return rv1::AVAILABILITY_STATE_RESERVED;
    case av1::AGENT_STATE_ON_CALL:
      return rv1::AVAILABILITY_STATE_ON_CALL;
    case av1::AGENT_STATE_WRAP_UP:
      for (const auto& rid : a.reservations) {
        auto r = reservations_.find(rid);
        if (r != reservations_.end() && r->second.st == ResSt::kWrapUp) *wrap_ends = std::max(*wrap_ends, r->second.wrap_ends);
      }
      return rv1::AVAILABILITY_STATE_WRAP_UP;
    default:
      return rv1::AVAILABILITY_STATE_NOT_READY;
  }
}

void Tenant::note_availability(AgentSt& a) {
  const std::string& campaign = !a.campaign.empty() ? a.campaign : a.mode_pending ? a.to_campaign : a.campaign;
  if (campaign.empty()) return;  // inbound with nothing pending: nobody to tell
  int64_t wrap_ends = 0;
  const rv1::AvailabilityState st = availability(a, &wrap_ends);
  if (static_cast<int>(st) == a.avail_sent && wrap_ends == a.avail_wrap_sent) return;
  send_availability(a, campaign, st, wrap_ends);
}

void Tenant::send_availability(AgentSt& a, const std::string& campaign, rv1::AvailabilityState st, int64_t wrap_ends) {
  a.avail_sent = static_cast<int>(st);
  a.avail_wrap_sent = wrap_ends;
  auto c = campaigns_.find(campaign);
  if (c == campaigns_.end() || c->second.replica.empty()) return;
  rv1::WatchResponse m;
  m.set_leader_epoch(epochs_->leader_epoch);
  m.set_delivery_seq(0);  // never replayed: the latest wins, a stream open resends all
  auto* v = m.mutable_agent_availability();
  v->set_tenant_id(id_);
  v->set_agent_id(a.cfg.id);
  v->set_campaign_id(campaign);
  v->set_state(st);
  if (st == rv1::AVAILABILITY_STATE_WRAP_UP) v->set_wrapup_ends_at_ms(wrap_ends);
  v->set_owner_epoch(c->second.owner_epoch);
  sink_->to_ie(c->second.replica, ie_gen(c->second.replica), m);
}

void Tenant::send_campaign_snapshot(const Campaign& c) {
  for (auto& [id, a] : agents_) {
    if (a.campaign != c.id && !(a.campaign.empty() && a.mode_pending && a.to_campaign == c.id)) continue;
    int64_t wrap_ends = 0;
    const rv1::AvailabilityState st = availability(a, &wrap_ends);
    send_availability(a, c.id, st, wrap_ends);
  }
}

void Tenant::lent_list(const std::string& campaign, google::protobuf::RepeatedPtrField<rv1::AgentAvailability>* out) const {
  auto c = campaigns_.find(campaign);
  const uint64_t epoch = c == campaigns_.end() ? 0 : c->second.owner_epoch;
  for (const auto& [id, a] : agents_) {
    if (a.campaign != campaign && !(a.campaign.empty() && a.mode_pending && a.to_campaign == campaign)) continue;
    int64_t wrap_ends = 0;
    auto* v = out->Add();
    v->set_tenant_id(id_);
    v->set_agent_id(id);
    v->set_campaign_id(campaign);
    v->set_state(availability(a, &wrap_ends));
    if (v->state() == rv1::AVAILABILITY_STATE_WRAP_UP) v->set_wrapup_ends_at_ms(wrap_ends);
    v->set_owner_epoch(epoch);
  }
}

// agent.events mode_changed (parity-contracts 0.14.0): reports and desktops see every switch.
void Tenant::event_mode(const AgentSt& a, const std::string& campaign, const char* reason, int64_t now) {
  {
    LogRec r("info", "agent.mode_changed", "agent mode changed (campaign lending)");
    if (r.on()) {
      r.tenant(id_).str("agent_id", a.cfg.id).transition(a.campaign.empty() ? "campaign" : "inbound",
                                                           a.campaign.empty() ? "inbound" : "campaign");
      r.attr("campaign_id", campaign).attr("reason", reason);
    }
  }
  if (!events_) return;
  std::string body = std::string("{\"mode\":\"") + (a.campaign.empty() ? "inbound" : "campaign") +
                     "\",\"campaign_id\":" + json_str(campaign) + ",\"reason\":\"" + reason + "\"}";
  events_->publish(AgentEvent{"mode_changed", id_, a.cfg.id, now, std::move(body), ""});
}

// ---- tests --------------------------------------------------------------------------------------

std::string Tenant::campaign_of(const std::string& agent) const {
  auto it = agents_.find(agent);
  return it == agents_.end() ? std::string() : it->second.campaign;
}

bool Tenant::mode_pending_of(const std::string& agent) const {
  auto it = agents_.find(agent);
  return it != agents_.end() && it->second.mode_pending;
}

}  // namespace pcr
