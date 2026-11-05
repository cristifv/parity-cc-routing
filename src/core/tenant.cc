#include "src/core/tenant.h"

#include "src/ops/log.h"

#include <algorithm>
#include <cmath>
#include <cstdio>

namespace pcr {

namespace {

constexpr int64_t kDay = 86'400'000;
constexpr int64_t kFutureClamp = 5'000;
constexpr int64_t kAssignGrace = 2'000;      // L7: deadline_ms + 2 s
constexpr int64_t kAdoptionBudget = 10'000;  // L7 pause, at most 10 s
constexpr int64_t kRedeliverAfter = 2'000;
constexpr size_t kGwOutboxCap = 256;     // phase 5 (CR-137): an agent's unacknowledged gateway messages
constexpr int64_t kCommandTtl = 600'000;     // (tenant, command_id) kept 10 min
constexpr int64_t kInteractionTtl = 600'000;
// CR-056: every head removal changes the position of every entry behind it. Positions 1..10 hear
// each change at once; entries further back at most once per 5 s (the 1 s sweep sends what a
// throttle held back), so a 1,000-entry queue no longer emits 1,000 messages per removal.
constexpr uint32_t kPositionHead = 10;
constexpr int64_t kPositionEveryMs = 5'000;

std::string sig_of(const std::vector<SkillReq>& reqs) {
  std::string s;
  for (const auto& r : reqs) {
    s += r.skill_id;
    s += ':';
    s += std::to_string(r.min_level);
    s += ';';
  }
  return s;
}

bool busy(av1::AgentState s) {
  return s == av1::AGENT_STATE_RESERVED || s == av1::AGENT_STATE_ON_CALL ||
         s == av1::AGENT_STATE_WRAP_UP;
}

// Holds an offer or a call: a move waits (PLAN §4.3 "next offer, never mid-call").
bool engaged(av1::AgentState s) { return s == av1::AGENT_STATE_OFFERED || busy(s); }

constexpr int64_t kHourMs = 3'600'000;
constexpr int64_t kCallbackRedeliverMs = 5'000;
constexpr int64_t kCallbackGraceMs = 10'000;
constexpr int64_t kBucketMs = 900'000;
// The synthetic queue of direct and consult-to-agent entries: "" is never a queue id.
const std::string kDirect;
constexpr int64_t kVipRank = int64_t{1} << 32;  // a VIP entry's rank: above every priority (uint32)

// routing.v1 0.12.0: Enqueue's caller class (additive; an older IE sends none: normal).
bool vip_of(const rv1::EnqueueRequest& r) { return r.caller_class() == rv1::CALLER_CLASS_VIP; }
std::atomic<uint64_t> g_instances{0};

av1::MembershipTier tier_pb(Tier t) {
  return t == Tier::kPrimary   ? av1::MEMBERSHIP_TIER_PRIMARY
         : t == Tier::kSecondary ? av1::MEMBERSHIP_TIER_SECONDARY
                                 : av1::MEMBERSHIP_TIER_UNSPECIFIED;
}

template <class S>
bool same_tiers(const S& a, const S& b) {
  if (a.size() != b.size()) return false;
  for (auto ia = a.begin(), ib = b.begin(); ia != a.end(); ++ia, ++ib) {
    if (ia->first != ib->first || ia->second.tier != ib->second.tier || ia->second.pinned != ib->second.pinned) return false;
  }
  return true;
}

template <class S>
std::set<std::string> primaries_of(const S& s) {
  std::set<std::string> out;
  for (const auto& [q, m] : s) {
    if (m.tier == Tier::kPrimary) out.insert(q);
  }
  return out;
}

template <class S>
std::set<std::string> secondaries_of(const S& s) {
  std::set<std::string> out;
  for (const auto& [q, m] : s) {
    if (m.tier == Tier::kSecondary) out.insert(q);
  }
  return out;
}

// A JSON string literal (agent.events bodies).
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

// log-record.v1 names (from_state/to_state, attrs): lowercase, as agent.events writes them.
const char* state_name(av1::AgentState s) {
  static const char* const names[] = {"unspecified", "logged_out", "not_ready", "ready", "offered", "reserved", "on_call", "wrap_up"};
  const int i = static_cast<int>(s);
  return i >= 0 && i < 8 ? names[i] : "unspecified";
}
const char* outcome_name(rv1::Outcome o) {
  static const char* const names[] = {"unspecified", "connected", "agent_no_answer", "agent_leg_failed", "caller_gone",
                                      "already_assigned", "customer_no_answer", "expired", "revoked"};
  const int i = static_cast<int>(o);
  return i >= 0 && i < 9 ? names[i] : "unspecified";
}
const char* action_name(MaxWaitAction a) {
  switch (a) {
    case MaxWaitAction::kQueue: return "queue";
    case MaxWaitAction::kVoicemail: return "voicemail";
    case MaxWaitAction::kCallbackOffer: return "callback_offer";
    case MaxWaitAction::kDisconnect: return "disconnect";
    case MaxWaitAction::kFlowStep: return "flow_step";
  }
  return "unspecified";
}
const char* dequeue_name(rv1::DequeueReason r) {
  static const char* const names[] = {"unspecified", "abandoned", "transferred", "callback", "voicemail",
                                      "ended", "consult_canceled", "overflow", "moved"};
  const int i = static_cast<int>(r);
  return i >= 0 && i < 9 ? names[i] : "unspecified";
}

const char* reason_name(av1::MembershipReason r) {
  switch (r) {
    case av1::MEMBERSHIP_REASON_LOGIN: return "login";
    case av1::MEMBERSHIP_REASON_OPTIMIZER: return "optimizer";
    case av1::MEMBERSHIP_REASON_SUPERVISOR_PIN: return "supervisor_pin";
    case av1::MEMBERSHIP_REASON_SUPERVISOR_UNPIN: return "supervisor_unpin";
    case av1::MEMBERSHIP_REASON_CONFIG: return "config";
    case av1::MEMBERSHIP_REASON_MODE_CHANGE: return "mode_change";
    case av1::MEMBERSHIP_REASON_FALLBACK: return "fallback";
    default: return "unspecified";
  }
}

}  // namespace

Tenant::Tenant(TenantConfig cfg, const Epochs* epochs, Sink* sink, TimerSink* timers,
               Counters* counters)
    : id_(cfg.id), cfg_(std::move(cfg)), epochs_(epochs), sink_(sink), timers_(timers),
      counters_(counters) {
  for (const auto& q : cfg_.queues) {
    QueueSt st;
    st.cfg = q;
    st.aht_ms = static_cast<double>(q.aht_ms);
    queues_.emplace(q.id, std::move(st));
  }
  for (const auto& a : cfg_.agents) {
    AgentSt st;
    st.cfg = a;
    st.member = std::set<std::string>(a.queues.begin(), a.queues.end());
    st.contact = a.contact;
    st.next_seq = epochs_->seq_base;
    st.version = epochs_->seq_base;  // state_version, like every counter, rises across incarnations
    st.queues_version = epochs_->seq_base;
    agents_.emplace(a.id, std::move(st));
  }
  instance_ = ++g_instances;
  {
    // Direct and consult-to-agent entries (phase 3) live in a synthetic queue that no agent is a
    // member of: only their target agent serves them.
    QueueSt d;
    d.cfg.id = kDirect;
    d.cfg.max_wait_ms = cfg_.direct_max_wait_ms;
    d.cfg.max_wait_action = cfg_.direct_max_wait_action;
    d.cfg.max_wait_flow_step = cfg_.direct_max_wait_flow_step;
    d.cfg.escalate_after_ms = kDay;
    d.aht_ms = static_cast<double>(d.cfg.aht_ms);
    d.secondary_delay_ms = 0;
    d.escalate_after_ms = kDay;
    queues_.emplace(kDirect, std::move(d));
  }
  // Phase 4: the tenant's policy and pins (the seed's config.routing / config.routing_pin; parse_seed
  // refused any naming another tenant's agent or queue), then every agent's first active set.
  policy_ = cfg_.routing;
  apply_policy_to_queues();
  for (const auto& p : cfg_.pins) {
    if (!agents_.contains(p.agent_id)) continue;
    pins_[p.agent_id] = p;
    pin_versions_[p.agent_id] = p.version;
  }
  for (auto& [id, a] : agents_) a.active = desired(a, 0);
}

// ======================================================================================
// Owners, fences, idempotency
// ======================================================================================

Result<Tenant::Interaction*> Tenant::owned(const rv1::InteractionOwner& o, bool create,
                                          int64_t now, bool* rebound) {
  if (rebound) *rebound = false;
  auto it = interactions_.find(o.interaction_id());
  if (it == interactions_.end()) {
    if (!create) return err(Code::kNotFound, "", "no such interaction");
    Interaction ix;
    ix.id = o.interaction_id();
    ix.owner_epoch = o.owner_epoch();
    ix.replica = o.replica_id();
    ix.next_seq = epochs_->seq_base;
    ix.acked = epochs_->seq_base;
    it = interactions_.emplace(ix.id, std::move(ix)).first;
  }
  Interaction& ix = it->second;
  if (o.owner_epoch() < ix.owner_epoch) {
    return err(Code::kAborted, "stale_owner", "a higher owner_epoch exists");
  }
  touch_ix(ix.id);
  hold_ix(ix.id);
  const bool raised = o.owner_epoch() > ix.owner_epoch;
  if (raised) {
    ix.owner_epoch = o.owner_epoch();
    ix.replica = o.replica_id();
    ix.owner_lost = false;  // a new owner (CR-083)
    if (!ix.callback_id.empty()) claim_callback(ix, now);
  }
  ix.last_activity = now;
  replica_rpc_[o.replica_id()] = now;
  apply_ack(ix, o.acked_delivery_seq());
  if (raised) {
    // CR-054 (contract ruling 2026-10-28): replays carry the CURRENT owner fence. Re-stamp at
    // every epoch raise, whether or not a stream is live, and replay to the new owner at once,
    // before this RPC sends anything newer (CR-053's order).
    restamp(ix);
    replay_ie(ix, now, /*new_owner=*/true);
    if (rebound) *rebound = true;
  }
  return &ix;
}

void Tenant::restamp(Interaction& ix) {
  for (auto& m : ix.unacked) {
    rv1::WatchResponse& w = m.msg;
    rv1::EventTarget* t = w.has_assigned()        ? w.mutable_assigned()->mutable_target()
                          : w.has_overflow()      ? w.mutable_overflow()->mutable_target()
                          : w.has_revoke()        ? w.mutable_revoke()->mutable_target()
                          : w.has_wrap_up_ended() ? w.mutable_wrap_up_ended()->mutable_target()
                          : w.has_position()      ? w.mutable_position()->mutable_target()
                                                  : nullptr;
    if (t) *t = target_of(ix);
  }
}

uint64_t Tenant::ie_gen(const std::string& replica) const {
  auto it = ie_gen_.find(replica);
  return it == ie_gen_.end() ? 0 : it->second;
}

uint64_t Tenant::gw_gen(const std::string& replica) const {
  auto it = gw_gen_.find(replica);
  return it == gw_gen_.end() ? 0 : it->second;
}

// Every unacknowledged message, in delivery_seq order, to the interaction's replica (its live
// stream if this tenant has replayed to it; else that stream's replay sends them).
void Tenant::replay_ie(Interaction& ix, int64_t now, bool new_owner) {
  if (ix.replica.empty()) return;
  const uint64_t gen = ie_gen(ix.replica);
  for (auto& m : ix.unacked) {
    m.sent_at = now;
    if (new_owner) m.first_at = now;  // a new owner gets its own CR-083 window
    if (sendable(m.msg)) sink_->to_ie(ix.replica, gen, m.msg);
  }
}

void Tenant::replay_gw(AgentSt& a, int64_t now) {
  if (a.replica.empty()) return;
  const uint64_t gen = gw_gen(a.replica);
  for (auto& m : a.unacked) {
    m.sent_at = now;
    sink_->to_gw(a.replica, gen, m.msg);
  }
}

void Tenant::apply_ack(Interaction& ix, uint64_t acked) {
  if (acked <= ix.acked) return;
  touch_ix(ix.id);
  ix.acked = acked;
  while (!ix.unacked.empty() && ix.unacked.front().seq <= acked) ix.unacked.pop_front();
}

bool Tenant::ack(const rv1::InteractionOwner& a, int64_t now) {
  auto it = interactions_.find(a.interaction_id());
  if (it == interactions_.end()) return false;
  Interaction& ix = it->second;
  touch_ix(ix.id);
  if (!ix.callback_id.empty() && ix.owner_epoch == 0 && a.owner_epoch() > 0) {
    // CONVENTIONS "Callbacks": IE acknowledges a RunCallback right after it claimed the interaction:
    // the ack's fence makes that replica the owner.
    ix.owner_epoch = a.owner_epoch();
    ix.replica = a.replica_id();
    claim_callback(ix, now);
  }
  if (a.owner_epoch() != ix.owner_epoch || a.replica_id() != ix.replica) return false;
  ix.last_activity = now;
  replica_rpc_[a.replica_id()] = now;
  apply_ack(ix, a.acked_delivery_seq());
  return true;
}

Result<Tenant::AgentSt*> Tenant::session(const av1::SessionRef& s) {
  if (!valid_agent_id(s.agent_id()) || !valid_uuid(s.session_id()) || s.attach_generation() == 0) {
    return err(Code::kInvalidArgument, "invalid_request", "bad session");
  }
  auto it = agents_.find(s.agent_id());
  if (it == agents_.end()) return err(Code::kNotFound, "agent_unknown", "no such agent");
  AgentSt& a = it->second;
  if (s.attach_generation() != a.generation || s.session_id() != a.session_id) {
    return err(Code::kAborted, "session_superseded", "a newer attachment exists");
  }
  return &a;
}

template <class Resp>
std::optional<Result<Resp>> Tenant::repeat(const std::string& cmd_id, const std::string& fp) {
  auto it = commands_.find(cmd_id);
  if (it == commands_.end()) return std::nullopt;
  if (it->second.fingerprint != fp) {
    return Result<Resp>(err(Code::kAlreadyExists, "conflict", "command_id reused"));
  }
  if (it->second.error) return Result<Resp>(std::unexpected(*it->second.error));
  Resp r;
  if (!r.ParseFromString(it->second.response)) {
    return Result<Resp>(err(Code::kInternal, "internal", "stored answer unreadable"));
  }
  return Result<Resp>(std::move(r));
}

template <class Resp>
Result<Resp> Tenant::remember(const std::string& cmd_id, const std::string& fp, Result<Resp> r,
                              int64_t now) {
  if (!r && r.error().parity == "session_superseded") return r;
  Cmd c;
  c.fingerprint = fp;
  c.at = now;
  if (r) {
    if (!r->SerializeToString(&c.response)) return r;
  } else {
    c.error = r.error();
  }
  commands_[cmd_id] = std::move(c);
  touch_cmd(cmd_id);
  return r;
}

// ======================================================================================
// Messages
// ======================================================================================

// log-record.v1: a call-scoped record's correlation: the tenant, the xsid (the interaction's id),
// the owner's fence and the leader's epoch.
LogRec& Tenant::lc(LogRec& r, const std::string& interaction) const {
  if (!r.on()) return r;
  r.tenant(id_).xsid(interaction).num("leader_epoch", static_cast<int64_t>(epochs_->leader_epoch));
  if (auto it = interactions_.find(interaction); it != interactions_.end()) {
    r.num("owner_epoch", static_cast<int64_t>(it->second.owner_epoch));
  }
  return r;
}

// agent.selected's decision (CONVENTIONS "Logging": at most 16 candidates, 12 parts, 4096 bytes):
// the READY agents that could take the entry, longest idle first, with their tier and idle time.
std::string Tenant::decision_of(const Entry& e, const AgentSt& chosen, bool sampled, int64_t now) const {
  const Tier ct = e.direct_agent.empty() ? tier_of(chosen, e.queue) : Tier::kNone;
  const char* why = !e.direct_agent.empty() ? "direct"
                    : e.escalated           ? "escalated"
                    : ct == Tier::kPrimary   ? "longest_idle_primary"
                    : ct == Tier::kSecondary ? "secondary_after_delay"
                                             : "eligible";
  auto cand = [&](const AgentSt& a, bool eligible) {
    const Tier t = e.direct_agent.empty() ? tier_of(a, e.queue) : Tier::kNone;
    char buf[200];
    std::snprintf(buf, sizeof buf, "{\"id\":%s,\"parts\":{\"tier\":%d,\"idle_s\":%.1f},\"eligible\":%s}",
                  json_str(a.cfg.id).c_str(), static_cast<int>(t),
                  static_cast<double>(std::max<int64_t>(0, now - a.idle_since)) / 1000.0, eligible ? "true" : "false");
    return std::string(buf);
  };
  std::string list = cand(chosen, true);
  int listed = 1, total = 1, scanned = 0;
  for (const auto& [idle, id] : ready_idle_) {
    if (++scanned > 2048) break;  // bounded: total counts what was looked at
    if (id == chosen.cfg.id) continue;
    const AgentSt& a = agents_.at(id);
    if (!can_serve(a, e)) continue;
    ++total;
    if (listed < 16) {
      list += "," + cand(a, serves_now(a, e));
      ++listed;
    }
  }
  return std::string("{\"kind\":\"agent.select\",\"sampled\":") + (sampled ? "true" : "false") + ",\"total\":" +
         std::to_string(total) + ",\"chosen\":" + json_str(chosen.cfg.id) + ",\"reason\":\"" + why +
         "\",\"candidates\":[" + list + "]}";
}

rv1::EventTarget Tenant::target_of(const Interaction& ix) const {
  rv1::EventTarget t;
  t.set_tenant_id(id_);
  t.set_interaction_id(ix.id);
  t.set_owner_epoch(ix.owner_epoch);
  return t;
}

void Tenant::send_ie(Interaction& ix, rv1::WatchResponse m, int64_t now, bool replayable) {
  if (ix.owner_lost) return;  // CR-083: nobody to tell until a new owner
  m.set_leader_epoch(epochs_->leader_epoch);
  if (replayable) {
    touch_ix(ix.id);
    hold_ix(ix.id);
    m.set_delivery_seq(++ix.next_seq);
    ix.unacked.push_back(OutIe{m.delivery_seq(), now, m, now});
  } else {
    m.set_delivery_seq(0);
  }
  if (!ix.replica.empty()) sink_->to_ie(ix.replica, ie_gen(ix.replica), m);
}

void Tenant::send_gw(AgentSt& a, av1::WatchResponse m, int64_t now) {
  if (a.generation == 0) return;  // never attached: nobody to tell
  touch_agent(a.cfg.id);
  m.set_leader_epoch(epochs_->leader_epoch);
  m.set_tenant_id(id_);
  m.set_agent_id(a.cfg.id);
  m.set_attach_generation(a.generation);
  m.set_delivery_seq(++a.next_seq);
  // Phase 5 (p6 review finding 6, CR-137): the outbox is bounded, so an agent's record stays small
  // whatever a detached (or slow) gateway acknowledges. Only the latest QueuesChanged counts: an
  // older one still unacknowledged is superseded (the desktop shows the newest active set).
  if (m.has_queues_changed()) {
    auto& q = a.unacked;
    const size_t before = q.size();
    q.erase(std::remove_if(q.begin(), q.end(), [](const OutGw& o) { return o.msg.has_queues_changed(); }), q.end());
    counters_->gw_outbox_coalesced += before - q.size();
  }
  a.unacked.push_back(OutGw{m.delivery_seq(), now, m});
  if (a.unacked.size() > kGwOutboxCap) compact_outbox(a);
  if (!a.replica.empty()) sink_->to_gw(a.replica, gw_gen(a.replica), m);
}

// Over the cap: keep what still describes the agent now (its latest StateChanged and QueuesChanged,
// messages about its live offer and live reservations, Detached) and drop the rest, oldest first;
// the gateway drops repeats by delivery_seq and a new attachment gets a snapshot anyway (CR-137).
void Tenant::compact_outbox(AgentSt& a) {
  auto& q = a.unacked;
  const size_t before = q.size();
  int64_t last_state = -1;
  for (size_t i = 0; i < q.size(); ++i) {
    if (q[i].msg.has_state_changed()) last_state = static_cast<int64_t>(i);
  }
  std::deque<OutGw> keep;
  for (size_t i = 0; i < q.size(); ++i) {
    const av1::WatchResponse& m = q[i].msg;
    bool k = m.has_queues_changed() || m.has_detached() || static_cast<int64_t>(i) == last_state;
    if (m.has_offer()) k = m.offer().offer_id() == a.offer_id;
    if (m.has_reserved()) k = a.reservations.contains(m.reserved().reservation_id());
    if (m.has_wrap_up_started()) k = a.reservations.contains(m.wrap_up_started().reservation_id());
    if (k) keep.push_back(std::move(q[i]));
  }
  // Still over (cannot happen with one live offer and a few reservations): the oldest go.
  while (keep.size() > kGwOutboxCap) keep.pop_front();
  q.swap(keep);
  counters_->gw_outbox_dropped += before - q.size();
}

av1::AgentView Tenant::view(const AgentSt& a) const {
  av1::AgentView v;
  v.set_state(a.state);
  if (a.state == av1::AGENT_STATE_NOT_READY) v.set_reason_code(a.reason);
  v.set_pending_target(a.pending);
  v.set_pending_reason_code(a.pending_reason);
  v.set_since_ms(a.since);
  v.set_state_version(a.version);
  // Phase 8: the mode flag (a pending lend is still inbound until it switches).
  if (a.campaign.empty()) {
    v.set_mode(av1::MODE_INBOUND);
  } else {
    v.set_mode(av1::MODE_CAMPAIGN);
    v.set_campaign_id(a.campaign);
  }
  auto fill = [](google::protobuf::RepeatedPtrField<av1::QueueMembership>* out, const Set& set) {
    for (const auto& [q, m] : set) {
      auto* x = out->Add();
      x->set_queue_id(q);
      x->set_tier(tier_pb(m.tier));
      x->set_pinned(m.pinned);
      x->set_since_ms(m.since);
    }
  };
  fill(v.mutable_queues(), a.active);
  if (a.pending_set) fill(v.mutable_pending_queues(), *a.pending_set);
  v.set_queues_version(a.queues_version);
  return v;
}

av1::Reservation Tenant::res_view(const Reservation& r) const {
  av1::Reservation v;
  v.set_reservation_id(r.id);
  v.set_interaction_id(r.interaction);
  if (!r.campaign.empty()) {
    v.set_origin(av1::RESERVATION_ORIGIN_CAMPAIGN);  // phase 8: ReserveAgent
    v.set_campaign_id(r.campaign);
  } else if (!r.callback_id.empty()) {
    v.set_origin(av1::RESERVATION_ORIGIN_CALLBACK);
  } else if (r.direct) {
    v.set_origin(av1::RESERVATION_ORIGIN_DIRECT);
  } else if (!r.queue.empty() || r.st == ResSt::kAssigned) {
    v.set_origin(av1::RESERVATION_ORIGIN_INBOUND);
  }
  if (r.st == ResSt::kAssigned) v.set_deadline_ms(r.deadline_ms);
  if (r.st == ResSt::kWrapUp) {
    v.set_wrap_up(true);
    v.set_wrap_up_ends_at_ms(r.wrap_ends);
  }
  return v;
}

rv1::QueueEntry Tenant::entry_view(const Entry& e, int64_t now) const {
  rv1::QueueEntry v;
  v.set_entry_id(e.id);
  v.set_enqueue_seq(e.enqueue_seq);
  *v.mutable_target() = e.target;
  v.set_first_queued_at_ms(e.first_queued_at);
  v.set_ordering_at_ms(e.ordering_at);
  v.set_state(e.state);
  if (e.state == rv1::ENTRY_STATE_WAITING) {
    const uint32_t p = position_of(e);
    v.set_position(p);
    rv1::ExpectedWaitQuality quality = rv1::EXPECTED_WAIT_QUALITY_UNSPECIFIED;
    v.set_expected_wait_s(expected_wait_s(e, p, now, &quality));
    v.set_expected_wait_quality(quality);
  }
  return v;
}

// ======================================================================================
// Agent state
// ======================================================================================

// Inbound only (phase 8): a lent agent is never offered an inbound, direct, consult or callback call.
bool Tenant::agent_available(const AgentSt& a) const {
  return a.state == av1::AGENT_STATE_READY && a.attached && a.offer_id.empty() && a.campaign.empty();
}

bool Tenant::entry_available(const Entry& e) const {
  return e.state == rv1::ENTRY_STATE_WAITING && e.offer_id.empty() && !e.converting;
}

void Tenant::refresh_agent(AgentSt& a) {
  touch_agent(a.cfg.id);
  // CR-056: each queue keeps its count of eligible agents logged in, so the wait estimate never
  // scans the agents.
  // Phase 8: a lent agent is out of the queues' logged-in counts (and so of the model, PLAN §4.3 mode).
  const bool counted = a.attached && a.state != av1::AGENT_STATE_LOGGED_OUT && a.campaign.empty();
  if (counted != a.counted) {
    a.counted = counted;
    ++agents_version_;
    for (const auto& qid : a.member) {
      auto q = queues_.find(qid);
      if (q != queues_.end() && eligible_queue(a, q->second, false)) q->second.logged_in += counted ? 1 : -1;
    }
  }
  if (a.avail_key) {
    ready_idle_.erase(*a.avail_key);
    a.avail_key.reset();
  }
  if (agent_available(a)) {
    a.avail_key = std::make_pair(a.idle_since, a.cfg.id);
    ready_idle_.insert(*a.avail_key);
  }
  // Lever (b): the queue the agent is busy on (its offer's, else a live reservation's), kept as a
  // per-queue count so the dispatch never scans agents.
  std::string bq;
  if (!a.offer_id.empty()) {
    auto o = offers_.find(a.offer_id);
    if (o != offers_.end()) {
      auto e = entries_.find(o->second.entry);
      if (e != entries_.end()) bq = e->second.queue;
    }
  } else {
    for (const auto& rid : a.reservations) {
      auto r = reservations_.find(rid);
      if (r != reservations_.end() && r->second.st != ResSt::kEnded) {
        bq = r->second.queue;
        break;
      }
    }
  }
  if (bq != a.busy_queue) {
    if (auto q = queues_.find(a.busy_queue); !a.busy_queue.empty() && q != queues_.end()) --q->second.busy;
    if (auto q = queues_.find(bq); !bq.empty() && q != queues_.end()) ++q->second.busy;
    a.busy_queue = bq;
  }
}

void Tenant::refresh_entry(Entry& e) {
  touch_entry(e.id);
  auto qit = queues_.find(e.queue);
  if (qit == queues_.end()) return;
  QueueSt& q = qit->second;
  if (e.avail_key) {
    auto s = q.avail.find(e.sig);
    if (s != q.avail.end()) s->second.erase(*e.avail_key);
    e.avail_key.reset();
  }
  if (e.esc_key) {
    escalated_.erase(*e.esc_key);
    e.esc_key.reset();
  }
  if (e.delayed_key) {
    auto s = q.delayed.find(e.sig);
    if (s != q.delayed.end()) s->second.erase(*e.delayed_key);
    e.delayed_key.reset();
  }
  if (!e.direct_agent.empty()) {
    auto da = agents_.find(e.direct_agent);
    if (da != agents_.end()) da->second.directs.erase(e.id);
  }
  std::optional<EKey> was_waiting;
  if (e.wait_key) {
    q.waiting.erase(*e.wait_key);
    was_waiting = std::move(e.wait_key);
    e.wait_key.reset();
  }
  // PLACEMENT_HEAD (the agent was lost): ahead of every priority; then the VIP class, then priority.
  const EKey k{e.head ? INT64_MIN : -(static_cast<int64_t>(e.priority) + (e.vip ? kVipRank : 0)), e.ordering_at,
               e.order_seq, e.id};
  if (e.state == rv1::ENTRY_STATE_WAITING) {
    e.wait_key = k;
    const auto it = q.waiting.insert(k).first;
    // CR-104: an entry that lands ahead of others (a higher priority, an overflow move, a call back
    // from an offer keeping its ordering time) moves everyone behind it back one place; the 1 s
    // sweep tells them (positions(), throttled beyond the head as CR-056 says). Re-inserting the
    // same key in the same place moves nobody.
    if (std::next(it) != q.waiting.end() && was_waiting != k) q.positions_pending = true;
  }
  if (entry_available(e)) {
    e.avail_key = k;
    q.avail[e.sig].insert(k);
    if (e.delay_passed) {
      e.delayed_key = k;
      q.delayed[e.sig].insert(k);
    }
    if (e.escalated) {
      e.esc_key = XKey{e.ordering_at, e.order_seq, e.id};
      escalated_.insert(*e.esc_key);
    }
    if (!e.direct_agent.empty()) {
      auto da = agents_.find(e.direct_agent);
      if (da != agents_.end()) da->second.directs.insert(e.id);
    }
  }
}

void Tenant::set_agent_state(AgentSt& a, av1::AgentState s, const std::string& reason,
                             int64_t now, bool echo) {
  const std::string r = s == av1::AGENT_STATE_NOT_READY ? reason : std::string();
  if (a.state == s && a.reason == r) {
    refresh_agent(a);
    return;
  }
  const bool was_engaged = engaged(a.state);
  {
    LogRec lr("info", "agent.state_changed", "agent state changed");
    if (lr.on()) {
      lr.tenant(id_).str("agent_id", a.cfg.id).transition(state_name(a.state), state_name(s));
      if (!r.empty()) lr.attr("reason_code", r);
      if (!a.campaign.empty()) lr.attr("campaign_id", a.campaign);
    }
  }
  a.state = s;
  a.reason = r;
  a.since = now;
  ++a.version;
  // PLAN §4.3: a pending active set applies once the agent holds no offer and no call; while it
  // does, its set must not change (check_invariants compares the versions).
  if (engaged(s) && !was_engaged) a.active_version_busy = a.active_version;
  if (!engaged(s)) {
    // Phase 8: idle (or after wrap-up): a pending mode switch applies first, then the pending set
    // it made; a logout ends a lending altogether.
    if (s == av1::AGENT_STATE_LOGGED_OUT) {
      end_lending(a, now);
    } else {
      apply_mode(a, now);
    }
    apply_pending(a, now);
  }
  if (s == av1::AGENT_STATE_READY) a.idle_since = now;
  refresh_agent(a);
  if (s == av1::AGENT_STATE_READY) todo_agents_.push_back(a.cfg.id);
  if (echo) {
    av1::WatchResponse m;
    *m.mutable_state_changed()->mutable_agent() = view(a);
    send_gw(a, std::move(m), now);
  }
  event_state(a, now);
  note_availability(a);
}

// The agent's last reservation ended: the pending state applies, else READY.
void Tenant::become_free(AgentSt& a, int64_t now, bool echo) {
  if (!a.reservations.empty()) return;
  const auto target = a.pending;
  const std::string reason = a.pending_reason;
  a.pending = av1::TARGET_STATE_UNSPECIFIED;
  a.pending_reason.clear();
  switch (target) {
    case av1::TARGET_STATE_NOT_READY:
      set_agent_state(a, av1::AGENT_STATE_NOT_READY, reason, now, echo);
      break;
    case av1::TARGET_STATE_LOGGED_OUT:
      set_agent_state(a, av1::AGENT_STATE_LOGGED_OUT, "", now, echo);
      break;
    default:
      set_agent_state(a, av1::AGENT_STATE_READY, "", now, echo);
      break;
  }
}

// A state the agent (or RE for it) asks for: now when idle, pending while busy.
void Tenant::ask(AgentSt& a, av1::TargetState target, const std::string& reason, int64_t now,
                 bool echo) {
  touch_agent(a.cfg.id);
  if (busy(a.state)) {
    a.pending = target;
    a.pending_reason = target == av1::TARGET_STATE_NOT_READY ? reason : std::string();
    return;
  }
  if (a.state == av1::AGENT_STATE_OFFERED && target != av1::TARGET_STATE_READY) {
    // Not ready or logout during OFFERED revokes the offer; it counts as a reject.
    reject(a.offer_id, now, /*to_ready=*/false);
  }
  switch (target) {
    case av1::TARGET_STATE_READY:
      if (a.state != av1::AGENT_STATE_OFFERED) {
        set_agent_state(a, av1::AGENT_STATE_READY, "", now, echo);
      }
      break;
    case av1::TARGET_STATE_NOT_READY:
      set_agent_state(a, av1::AGENT_STATE_NOT_READY, reason, now, echo);
      break;
    case av1::TARGET_STATE_LOGGED_OUT:
      set_agent_state(a, av1::AGENT_STATE_LOGGED_OUT, "", now, echo);
      break;
    default:
      break;
  }
}

// ======================================================================================
// Dispatch (PLAN §4.2)
// ======================================================================================

bool Tenant::meets(const AgentSt& a, const std::vector<SkillReq>& reqs) const {
  for (const auto& r : reqs) {
    auto it = a.cfg.skills.find(r.skill_id);
    if (it == a.cfg.skills.end()) return false;
    if (r.min_level != 0 && it->second < r.min_level) return false;
  }
  return true;
}

bool Tenant::eligible_queue(const AgentSt& a, const QueueSt& q, bool escalated) const {
  if (!escalated && !a.member.contains(q.cfg.id)) return false;
  return meets(a, q.cfg.required);
}

// Whether the agent may ever take this entry: a direct entry only its agent; a consult never the
// agent who asked; a queue entry an eligible member (any eligible agent once escalated) that the
// entry's requirements allow and no exclusion keeps out.
bool Tenant::can_serve(const AgentSt& a, const Entry& e) const {
  if (!e.consult_from.empty() && a.cfg.id == e.consult_from) return false;
  if (!e.direct_agent.empty()) return a.cfg.id == e.direct_agent;
  if (e.queue.empty()) return false;
  auto q = queues_.find(e.queue);
  if (q == queues_.end()) return false;
  if (excluded(a, e.queue)) return false;
  return eligible_queue(a, q->second, e.escalated) && meets(a, e.reqs);
}

// Whether the agent may take it NOW (PLAN §4.2): a primary at once, a secondary once the entry
// waited the queue's delay, anyone eligible once it is past escalate_after.
bool Tenant::serves_now(const AgentSt& a, const Entry& e) const {
  if (!can_serve(a, e)) return false;
  if (!e.direct_agent.empty() || e.escalated) return true;
  const Tier t = tier_of(a, e.queue);
  return t == Tier::kPrimary || (t == Tier::kSecondary && e.delay_passed);
}

// A reject skips the agent for the entry only while it is fresh (reject_skip) AND another
// eligible, non-skipped agent is READY (PLAN §4.2): a one-agent queue never strands a caller.
// A direct offer's reject holds for its whole time: nobody else can take it, and max wait bounds it.
bool Tenant::skip_active(const Entry& e, const std::string& agent, int64_t now) const {
  auto s = e.skips.find(agent);
  if (s == e.skips.end() || s->second <= now) return false;
  if (!e.direct_agent.empty()) return true;
  for (const auto& [idle, other] : ready_idle_) {
    if (other == agent) continue;
    const AgentSt& b = agents_.at(other);
    if (!serves_now(b, e)) continue;
    auto bs = e.skips.find(other);
    if (bs != e.skips.end() && bs->second > now) continue;
    return true;
  }
  return false;
}

// Candidates for an offerable entry: same tenant (structural), READY, attached, inbound, no
// offer, meeting the queue's and the entry's requirements, not skipped; longest idle first, then
// agent_id (the set's order).
// Phase 4 order: tier first (primaries, then secondaries once the delay passed, then any eligible
// agent once escalated), then longest idle, then agent_id (the set's order).
Tenant::AgentSt* Tenant::pick_agent(const Entry& e, int64_t now) {
  if (!e.direct_agent.empty()) {
    auto it = agents_.find(e.direct_agent);
    if (it == agents_.end() || !agent_available(it->second) || skip_active(e, e.direct_agent, now)) return nullptr;
    return &it->second;
  }
  AgentSt* best = nullptr;
  AgentSt* skipped = nullptr;
  int best_rank = 9, skipped_rank = 9;
  // A VIP reserve (phase 5 redesign) keeps VIP-eligible agents from a normal entry before its delay.
  bool reserve = false;
  if (!e.vip && !e.head && !e.escalated && !e.delay_passed) {
    auto q = queues_.find(e.queue);
    reserve = q != queues_.end() && q->second.pol.vip_reserved_min > 0 && !policy_.vip_skill.empty() &&
              ready_vip(q->second) <= q->second.pol.vip_reserved_min;
  }
  for (const auto& [idle, id] : ready_idle_) {
    AgentSt& a = agents_.at(id);
    if (!can_serve(a, e)) continue;
    if (reserve && vip_agent(a)) {
      ++counters_->vip_reserve_holds;
      continue;
    }
    const Tier t = tier_of(a, e.queue);
    int rank = 9;
    if (t == Tier::kPrimary) {
      rank = 0;
    } else if (t == Tier::kSecondary && e.delay_passed) {
      rank = 1;
    } else if (e.escalated) {
      rank = 2;
    } else {
      continue;
    }
    auto s = e.skips.find(id);
    if (s != e.skips.end() && s->second > now) {
      if (rank < skipped_rank) {
        skipped = &a;
        skipped_rank = rank;
      }
      continue;
    }
    if (rank < best_rank) {
      best = &a;
      best_rank = rank;
      if (rank == 0) break;
    }
  }
  // Every eligible READY agent rejected it recently: the skip no longer holds.
  return best ? best : skipped;
}

// An agent becomes ready: escalated entries first (oldest), then priority, then the
// SLA-normalized age, then the older ordering time, then queue_id.
// An agent becomes ready (PLAN §4.2): entries past escalate_after first (oldest, any queue it is
// eligible for), then its direct offers, then over its active set: priority (secondaries only past
// the queue's delay), the weighted SLA-normalized age w x age / tau, the older ordering time, the
// queue id.
Tenant::Entry* Tenant::pick_entry(AgentSt& a, int64_t now) {
  for (const auto& k : escalated_) {
    Entry& e = entries_.at(std::get<2>(k));
    if (!can_serve(a, e) || skip_active(e, a.cfg.id, now)) continue;
    return &e;
  }
  Entry* best = nullptr;
  for (const auto& id : a.directs) {
    Entry& e = entries_.at(id);
    if (!entry_available(e) || skip_active(e, a.cfg.id, now)) continue;
    if (!best || *e.avail_key < *best->avail_key) best = &e;
  }
  if (best) return best;
  // Over the active set (PLAN §4.2; phase 5 redesign): the class (a head entry, then VIP, then
  // normal), lever (b)'s bound rank (auto: below min first, at max last), priority, the weighted
  // SLA-normalized age w x age / tau (w = the agent's own weight x the queue's lever weight), the
  // older ordering time, the queue id.
  double best_u = 0;
  int best_bounds = 0;
  bool saw_max = false;
  auto cls = [](const Entry& e) { return e.head ? 2 : e.vip ? 1 : 0; };
  for (const auto& [qid, m] : a.active) {
    auto qit = queues_.find(qid);
    if (qit == queues_.end() || qid.empty()) continue;
    QueueSt& q = qit->second;
    if (!eligible_queue(a, q, false) || excluded(a, qid)) continue;
    auto w = a.weights.find(qid);
    const double weight = (w == a.weights.end() ? 1.0 : w->second) * q.weight;
    const int brank = bounds_rank(q);
    // A VIP reserve: this agent takes the queue's normal entries only past their delay.
    const bool reserved = m.tier == Tier::kPrimary && q.pol.vip_reserved_min > 0 && !policy_.vip_skill.empty() &&
                          vip_agent(a) && ready_vip(q) <= q.pol.vip_reserved_min;
    auto consider = [&](Entry& e) {
      saw_max = saw_max || brank == 0;
      const double u = weight * static_cast<double>(now - e.ordering_at) /
                       static_cast<double>(std::max<int64_t>(1, q.cfg.sla_ms));
      bool better = false;
      if (!best) {
        better = true;
      } else if (cls(e) != cls(*best)) {
        better = cls(e) > cls(*best);
      } else if (brank != best_bounds) {
        better = brank > best_bounds;
      } else if (e.priority != best->priority) {
        better = e.priority > best->priority;
      } else if (u != best_u) {
        better = u > best_u;
      } else if (e.ordering_at != best->ordering_at) {
        better = e.ordering_at < best->ordering_at;
      } else {
        better = e.queue < best->queue;
      }
      if (better) {
        best = &e;
        best_u = u;
        best_bounds = brank;
      }
    };
    auto& index = m.tier == Tier::kPrimary && !reserved ? q.avail : q.delayed;
    for (auto& [sig, list] : index) {
      for (const auto& k : list) {
        Entry& e = entries_.at(std::get<3>(k));
        if (!meets(a, e.reqs)) break;  // one signature, one set of requirements
        if (skip_active(e, a.cfg.id, now)) continue;
        consider(e);
        break;
      }
    }
    if (reserved) {
      // Reserved: the queue's head entries of the VIP class (or head placement) still come first.
      bool held = false;
      for (auto& [sig, list] : q.avail) {
        for (const auto& k : list) {
          Entry& e = entries_.at(std::get<3>(k));
          if (!meets(a, e.reqs)) break;
          if (skip_active(e, a.cfg.id, now)) continue;
          if (e.vip || e.head) consider(e); else held = !e.delay_passed;
          break;
        }
      }
      if (held) ++counters_->vip_reserve_holds;
    }
  }
  if (best && levers_on()) {
    if (best_bounds == 2 && !best->vip && !best->head) ++counters_->bounds_min_picks;
    if (best_bounds > 0 && saw_max) ++counters_->bounds_max_defers;
  }
  return best;
}

// Runs the dispatch work this operation produced until nothing changes: entries first, then
// agents, then entries whose skip may have lapsed because the READY set shrank.
void Tenant::settle(int64_t now) {
  if (draining_) {
    todo_entries_.clear();
    todo_agents_.clear();
    return;
  }
  for (int round = 0; round < 1000; ++round) {
    bool did = false;
    while (!todo_entries_.empty() || !todo_agents_.empty()) {
      if (!todo_entries_.empty()) {
        const std::string id = todo_entries_.front();
        todo_entries_.pop_front();
        auto it = entries_.find(id);
        if (it == entries_.end() || !entry_available(it->second)) continue;
        if (AgentSt* a = pick_agent(it->second, now)) {
          make_offer(it->second, *a, now);
          did = true;
        }
        continue;
      }
      const std::string id = todo_agents_.front();
      todo_agents_.pop_front();
      auto it = agents_.find(id);
      if (it == agents_.end() || !agent_available(it->second)) continue;
      if (Entry* e = pick_entry(it->second, now)) {
        make_offer(*e, it->second, now);
        did = true;
      }
    }
    if (!did) return;
    for (const auto& id : skip_entries_) todo_entries_.push_back(id);
  }
}

void Tenant::make_offer(Entry& e, AgentSt& a, int64_t now) {
  const QueueSt& q = queues_.at(e.queue);
  {
    // log-record.v1: who was chosen, and (one offer in log_decision_every(), or at debug) why.
    LogRec r("info", "agent.selected", "agent selected");
    if (r.on()) {
      lc(r, e.interaction).str("queue_id", e.queue).str("entry_id", e.id).str("agent_id", a.cfg.id);
      const bool sampled = ++decisions_ % log_decision_every() == 0;
      if (sampled) r.decision(decision_of(e, a, true, now));
    }
  }
  Offer o{new_uuid_v7(now), a.cfg.id, e.id, now + q.cfg.offer_timeout_ms,
          e.direct_agent.empty() ? tier_of(a, e.queue) : Tier::kNone};
  e.offer_id = o.id;
  a.offer_id = o.id;
  touch_offer(o.id);
  offers_.emplace(o.id, o);  // before the state change: refresh_agent counts the offer's queue (lever b)
  refresh_entry(e);
  set_agent_state(a, av1::AGENT_STATE_OFFERED, "", now, /*echo=*/false);
  av1::WatchResponse m;
  auto* of = m.mutable_offer();
  of->set_offer_id(o.id);
  of->set_interaction_id(e.interaction);
  of->set_channel(av1::CHANNEL_VOICE);
  of->set_queue_id(e.queue);
  of->set_expires_at_ms(o.expires_at);
  of->set_tier(tier_pb(o.tier));
  of->set_consult(!e.consult_from.empty());
  of->set_direct(!e.direct_agent.empty() && e.consult_from.empty());
  of->set_callback(!e.callback_id.empty());
  send_gw(a, std::move(m), now);
  timers_->at(o.expires_at, TimerRef{id_, TimerKind::kOfferExpiry, o.id});
  {
    LogRec r("info", "offer.sent", "offer sent");
    if (r.on()) lc(r, e.interaction).str("agent_id", a.cfg.id).str("entry_id", e.id).str("queue_id", e.queue)
                    .attr("offer_id", o.id).attr("timeout_ms", o.expires_at - now).attr("tier", static_cast<int>(o.tier));
  }
  ++counters_->offers;
  if (task_start_ != std::chrono::steady_clock::time_point{}) {
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - task_start_).count();
    const uint64_t u = static_cast<uint64_t>(std::max<int64_t>(0, us));
    const int b = std::min(31, u == 0 ? 0 : 64 - __builtin_clzll(u));
    counters_->decision_us_buckets[b].fetch_add(1, std::memory_order_relaxed);
    counters_->decision_us_sum.fetch_add(u, std::memory_order_relaxed);
    uint64_t prev = counters_->decision_us_max.load(std::memory_order_relaxed);
    while (u > prev && !counters_->decision_us_max.compare_exchange_weak(prev, u, std::memory_order_relaxed)) {
    }
  }
  counters_->time_to_offer_ms_sum += static_cast<uint64_t>(std::max<int64_t>(0, now - e.queue_entered_at));
}

void Tenant::expire_offer(const std::string& offer_id, int64_t now) {
  auto it = offers_.find(offer_id);
  if (it == offers_.end()) return;
  const Offer o = it->second;
  offers_.erase(it);
  touch_offer(o.id);
  AgentSt& a = agents_.at(o.agent);
  touch_agent(o.agent);
  a.offer_id.clear();
  a.past_offers.insert(o.id);
  av1::WatchResponse m;
  m.mutable_offer_revoked()->set_offer_id(o.id);
  m.mutable_offer_revoked()->set_reason(av1::REVOKE_REASON_EXPIRED);
  send_gw(a, std::move(m), now);
  set_agent_state(a, av1::AGENT_STATE_NOT_READY, "missed_offer", now, /*echo=*/true);
  auto e = entries_.find(o.entry);
  if (e != entries_.end()) {
    {
      LogRec r("info", "offer.expired", "offer expired: the caller keeps its place");
      if (r.on()) lc(r, e->second.interaction).str("agent_id", o.agent).str("entry_id", o.entry).attr("offer_id", o.id);
    }
    e->second.offer_id.clear();
    refresh_entry(e->second);
    todo_entries_.push_back(e->first);
  }
  ++counters_->offers_expired;
}

// RE withdraws an offer (caller gone, detached): the agent is READY again, no penalty.
void Tenant::revoke_offer(const std::string& offer_id, av1::RevokeReason why, int64_t now) {
  auto it = offers_.find(offer_id);
  if (it == offers_.end()) return;
  const Offer o = it->second;
  offers_.erase(it);
  touch_offer(o.id);
  AgentSt& a = agents_.at(o.agent);
  touch_agent(o.agent);
  a.offer_id.clear();
  a.past_offers.insert(o.id);
  av1::WatchResponse m;
  m.mutable_offer_revoked()->set_offer_id(o.id);
  m.mutable_offer_revoked()->set_reason(why);
  send_gw(a, std::move(m), now);
  set_agent_state(a, av1::AGENT_STATE_READY, "", now, /*echo=*/true);
  auto e = entries_.find(o.entry);
  if (e != entries_.end()) {
    {
      LogRec r("info", "offer.revoked", "offer withdrawn");
      if (r.on()) lc(r, e->second.interaction).str("agent_id", o.agent).str("entry_id", o.entry).attr("offer_id", o.id)
                      .attr("reason", av1::RevokeReason_Name(why));
    }
    e->second.offer_id.clear();
    refresh_entry(e->second);
    todo_entries_.push_back(e->first);
  }
  ++counters_->offers_revoked;
}

// A reject (or not-ready/logout while OFFERED): skip this agent for the entry (temporary), reset
// its idle-since to now (CR-032), offer the entry on.
void Tenant::reject(const std::string& offer_id, int64_t now, bool to_ready) {
  auto it = offers_.find(offer_id);
  if (it == offers_.end()) return;
  const Offer o = it->second;
  offers_.erase(it);
  touch_offer(o.id);
  AgentSt& a = agents_.at(o.agent);
  touch_agent(o.agent);
  a.offer_id.clear();
  a.past_offers.insert(o.id);
  auto e = entries_.find(o.entry);
  if (e != entries_.end()) {
    {
      LogRec r("info", "offer.rejected", "offer rejected");
      if (r.on()) lc(r, e->second.interaction).str("agent_id", o.agent).str("entry_id", o.entry).attr("offer_id", o.id)
                      .attr("answered", to_ready);
    }
    e->second.offer_id.clear();
    e->second.skips[a.cfg.id] = now + reject_skip_ms();
    skip_entries_.insert(e->first);
    timers_->at(now + reject_skip_ms(), TimerRef{id_, TimerKind::kSkipExpiry, e->first});
    refresh_entry(e->second);
    todo_entries_.push_back(e->first);
  }
  if (to_ready) {
    // The offer is answered: the agent is READY at the back of the longest-idle order.
    set_agent_state(a, av1::AGENT_STATE_READY, "", now, /*echo=*/true);
  } else {
    av1::WatchResponse m;
    m.mutable_offer_revoked()->set_offer_id(o.id);
    m.mutable_offer_revoked()->set_reason(av1::REVOKE_REASON_STATE_CHANGED);
    send_gw(a, std::move(m), now);
    refresh_agent(a);
  }
  ++counters_->offers_rejected;
}

void Tenant::accept(const std::string& offer_id, int64_t now, av1::Reservation* out) {
  const Offer o = offers_.at(offer_id);
  offers_.erase(offer_id);
  touch_offer(offer_id);
  AgentSt& a = agents_.at(o.agent);
  Entry& e = entries_.at(o.entry);
  Interaction& ix = interactions_.at(e.interaction);
  touch_agent(a.cfg.id);
  touch_ix(ix.id);
  touch_queue(e.queue);
  a.offer_id.clear();
  a.past_offers.insert(o.id);
  e.offer_id.clear();

  left_waiting(e, now, /*abandoned=*/false);
  Reservation r;
  r.id = new_uuid_v7(now);
  r.interaction = ix.id;
  r.entry_id = e.id;
  r.agent = a.cfg.id;
  r.contact = a.contact;
  r.queue = e.queue;
  r.deadline_ms = now + cfg_.assignment_deadline_ms;
  r.st = ResSt::kAssigned;
  r.consult = !e.consult_from.empty();
  r.direct = !e.direct_agent.empty() && e.consult_from.empty();
  r.callback_id = e.callback_id;
  if (!r.callback_id.empty()) r.deadline_ms = now + cfg_.callback_deadline_ms;
  {
    QueueSt& bq = queues_.at(e.queue);
    const int64_t waited = std::max<int64_t>(0, now - e.queue_entered_at);
    ++bq.bucket.answered;
    if (waited <= bq.cfg.sla_ms) ++bq.bucket.answered_in_sla;
    bq.bucket.wait_answered_ms += static_cast<double>(waited);
  }
  e.state = rv1::ENTRY_STATE_RESERVED;
  e.reservation_id = r.id;
  refresh_entry(e);
  ix.reservations.insert(r.id);
  a.reservations.insert(r.id);
  reservations_.emplace(r.id, r);
  touch_res(r.id);

  *out = res_view(r);
  {
    LogRec lr("info", "offer.accepted", "offer accepted: agent reserved");
    if (lr.on()) lc(lr, ix.id).str("agent_id", a.cfg.id).str("reservation_id", r.id).str("entry_id", e.id).str("queue_id", e.queue)
                     .duration_ms(static_cast<double>(std::max<int64_t>(0, now - e.queue_entered_at)));
  }
  set_agent_state(a, av1::AGENT_STATE_RESERVED, "", now, /*echo=*/false);
  av1::WatchResponse reserved;
  *reserved.mutable_reserved() = res_view(r);
  send_gw(a, std::move(reserved), now);
  av1::WatchResponse sc;
  *sc.mutable_state_changed()->mutable_agent() = view(a);
  send_gw(a, std::move(sc), now);

  ++counters_->offers_accepted;
  if (!r.callback_id.empty()) {
    // A callback: RunCallback (not Assigned) once the attempt is durable (dispatch_callback).
    timers_->at(r.deadline_ms + kCallbackGraceMs, TimerRef{id_, TimerKind::kAssignDeadline, r.id});
    const std::string q = e.queue;
    dispatch_callback(e, reservations_.at(r.id), now);
    positions(q, now);
    return;
  }
  rv1::WatchResponse m;
  auto* as = m.mutable_assigned();
  *as->mutable_target() = target_of(ix);
  as->set_reservation_id(r.id);
  as->set_entry_id(e.id);
  as->set_agent_id(a.cfg.id);
  as->set_agent_contact(r.contact);
  as->set_deadline_ms(r.deadline_ms);
  as->set_consult(r.consult);
  send_ie(ix, std::move(m), now);
  timers_->at(r.deadline_ms + kAssignGrace, TimerRef{id_, TimerKind::kAssignDeadline, r.id});
  positions(e.queue, now);
}

// ======================================================================================
// Entries and reservations
// ======================================================================================

uint32_t Tenant::position_of(const Entry& e) const {
  if (!e.wait_key) return 0;
  const QueueSt& q = queues_.at(e.queue);
  if (!e.direct_agent.empty()) {  // a direct offer's place is among its agent's
    uint32_t p = 1;
    for (const auto& k : q.waiting) {
      if (k == *e.wait_key) return p;
      if (entries_.at(std::get<3>(k)).direct_agent == e.direct_agent) ++p;
    }
    return 0;
  }
  auto it = q.waiting.find(*e.wait_key);
  if (it == q.waiting.end()) return 0;
  return static_cast<uint32_t>(std::distance(q.waiting.begin(), it)) + 1;
}

int Tenant::eligible_logged_in(const QueueSt& q, const Entry& e) const {
  if (e.reqs.empty()) return q.logged_in;
  auto& c = sig_count_[{q.cfg.id, e.sig}];
  if (c.first != agents_version_ + 1) {  // recount once per change of the logged-in set
    int n = 0;
    for (const auto& [id, a] : agents_) {
      if (a.counted && eligible_queue(a, q, false) && meets(a, e.reqs)) ++n;
    }
    c = {agents_version_ + 1, n};
  }
  return c.second;
}

void Tenant::left_waiting(Entry& e, int64_t now, bool abandoned) {
  auto q = queues_.find(e.queue);
  if (q == queues_.end()) return;
  q->second.waited_ms += static_cast<double>(std::max<int64_t>(0, now - e.queue_entered_at));
  if (abandoned) {
    touch_queue(e.queue);
    ++q->second.abandons;
    ++q->second.bucket.abandoned;
  }
}

// Phase 1's estimate (PLAN §8 1.3): position x mean handle time / eligible agents logged in. With
// nobody eligible logged in, the time left to max_wait_s as a lower bound, never 0 (CR-013).
// Phase 2 (--model-ewt): the model's EWT by position from the optimizer's plan while it is valid
// (PLAN §3.4: V_k, the wait of a caller who stays; CR-033: per requirement signature); phase 1's
// estimate whenever there is no valid plan or table for the entry.
uint32_t Tenant::expected_wait_s(const Entry& e, uint32_t position, int64_t now,
                                 rv1::ExpectedWaitQuality* quality) const {
  rv1::ExpectedWaitQuality dummy;
  rv1::ExpectedWaitQuality& qual = quality ? *quality : dummy;
  const QueueSt& q = queues_.at(e.queue);
  const int64_t max_wait = e.direct_agent.empty() ? q.cfg.max_wait_ms : cfg_.direct_max_wait_ms;
  const int64_t left = e.queue_entered_at + max_wait - now;
  const uint32_t lower_bound = static_cast<uint32_t>(std::max<int64_t>(1, (left + 999) / 1000));
  if (!e.direct_agent.empty()) {  // one agent: the time to its max wait, a lower bound
    qual = rv1::EXPECTED_WAIT_QUALITY_LONG;
    return lower_bound;
  }
  const int logged_in = eligible_logged_in(q, e);
  if (logged_in == 0) {
    qual = rv1::EXPECTED_WAIT_QUALITY_LONG;  // CR-013: a lower bound, never 0
    return lower_bound;
  }
  if (model_ewt_ && ewt_plan_ && now <= ewt_plan_->valid_until_ms && position > 0) {
    // A VIP entry (phase 5 redesign) uses its class's table: its position counts VIP entries only.
    auto t = ewt_plan_->tables.find({e.queue, e.vip && e.sig.empty() ? std::string(kVipSig) : e.sig});
    if (t != ewt_plan_->tables.end() && !t->second.by_position.empty()) {
      ++counters_->ewt_model;
      const EwtTable& tb = t->second;
      if (tb.long_wait) {
        qual = rv1::EXPECTED_WAIT_QUALITY_LONG;
        return lower_bound;
      }
      const size_t n = tb.by_position.size();
      const double v = position <= n ? tb.by_position[position - 1]
                                     : tb.by_position.back() + static_cast<double>(position - n) * tb.step_s;
      if (std::isfinite(v) && v >= 0 && v < 4.0e9) {
        // model::Quality: 0 exact, 1 approx, 2 overloaded, 3 long (CR-096's mapping).
        qual = tb.quality == 0 ? rv1::EXPECTED_WAIT_QUALITY_MODEL
               : tb.quality == 1 ? rv1::EXPECTED_WAIT_QUALITY_APPROX
                                 : rv1::EXPECTED_WAIT_QUALITY_LONG;
        if (qual == rv1::EXPECTED_WAIT_QUALITY_LONG) return std::max(lower_bound, static_cast<uint32_t>(std::max<double>(1, std::ceil(v))));
        return static_cast<uint32_t>(std::max<double>(1, std::ceil(v)));
      }
    }
  }
  ++counters_->ewt_heuristic;
  qual = rv1::EXPECTED_WAIT_QUALITY_HEURISTIC;
  const double s = position * q.aht_ms / 1000.0 / logged_in;
  return static_cast<uint32_t>(std::llround(std::ceil(s)));
}

// Sends PositionUpdate (never replayed, delivery_seq 0) to every waiting entry of the queue
// whose position changed. An entry that (re-)enters WAITING (L3, L4, L7, L8, an L14 move) has
// last_position 0 and gets one too (CR-062); only a new entry's is in the Enqueue answer
// (create_entry presets it).
void Tenant::positions(const std::string& queue, int64_t now) {
  auto qit = queues_.find(queue);
  if (qit == queues_.end()) return;
  qit->second.positions_pending = false;
  uint32_t p = 0;
  std::map<std::string, uint32_t> direct_rank;  // the direct queue: a place per agent
  for (const auto& k : qit->second.waiting) {
    Entry& e = entries_.at(std::get<3>(k));
    p = e.direct_agent.empty() ? p + 1 : ++direct_rank[e.direct_agent];
    if (e.last_position == p) continue;
    if (position_throttle_ && p > kPositionHead && e.last_position != 0 && now - e.position_sent_at < kPositionEveryMs) {
      qit->second.positions_pending = true;
      continue;
    }
    e.last_position = p;
    e.position_sent_at = now;
    auto ix = interactions_.find(e.interaction);
    if (ix == interactions_.end()) continue;
    rv1::WatchResponse m;
    auto* pu = m.mutable_position();
    *pu->mutable_target() = target_of(ix->second);
    pu->set_entry_id(e.id);
    pu->set_position(p);
    rv1::ExpectedWaitQuality quality = rv1::EXPECTED_WAIT_QUALITY_UNSPECIFIED;
    pu->set_expected_wait_s(expected_wait_s(e, p, now, &quality));
    pu->set_expected_wait_quality(quality);
    send_ie(ix->second, std::move(m), now, /*replayable=*/false);
  }
}

Tenant::Entry& Tenant::create_entry(Interaction& ix, const rv1::EnqueueRequest& r,
                                    const QueueSt& q, int64_t now) {
  Entry e;
  e.id = new_uuid_v7(now);
  e.interaction = ix.id;
  e.queue = q.cfg.id;
  e.first_queue = q.cfg.id;
  e.enqueue_seq = r.enqueue_seq();
  e.target = r.target();
  if (r.target().kind_case() == rv1::Target::kAgentId) e.direct_agent = r.target().agent_id();
  if (r.target().kind_case() == rv1::Target::kConsult) {
    e.consult_from = r.target().consult().from_agent_id();
    if (r.target().consult().to_case() == rv1::ConsultTarget::kAgentId) e.direct_agent = r.target().consult().agent_id();
  }
  e.head = r.placement() == rv1::PLACEMENT_HEAD;
  e.vip = vip_of(r);
  e.delay_passed = !e.direct_agent.empty() || q.secondary_delay_ms <= 0;
  e.priority = r.priority() == 0 ? q.cfg.default_priority : r.priority();
  for (const auto& rq : r.requirements()) e.reqs.push_back(SkillReq{rq.skill_id(), rq.min_level()});
  std::sort(e.reqs.begin(), e.reqs.end());
  e.sig = sig_of(e.reqs);
  e.first_queued_at = std::min(r.first_queued_at_ms(), now);
  e.ordering_at = std::min(r.ordering_at_ms(), now);
  e.original_ordering_at = e.ordering_at;
  e.queue_entered_at = now;
  e.created_at = now;
  e.order_seq = ++order_seq_;
  e.state = rv1::ENTRY_STATE_WAITING;
  {
    QueueSt& qs = queues_.at(q.cfg.id);
    touch_queue(q.cfg.id);
    ++qs.arrivals;
    ++qs.bucket.offered;
    count_offered_10s(&qs.bucket, now);
    qs.prio_lo = std::min(qs.prio_lo, e.priority);
    qs.prio_hi = std::max(qs.prio_hi, e.priority);
    if (e.vip) {
      ++qs.vip_arrivals;
      ++counters_->vip_enqueued;
    }
  }
  auto [it, ok] = entries_.emplace(e.id, std::move(e));
  Entry& ref = it->second;
  ix.entry_id = ref.id;
  touch_ix(ix.id);
  hold_ix(ix.id);
  if (ref.direct_agent.empty()) {
    timers_->at(now + q.escalate_after_ms, TimerRef{id_, TimerKind::kEscalate, ref.id});
    if (!ref.delay_passed) timers_->at(now + q.secondary_delay_ms, TimerRef{id_, TimerKind::kSecondaryDelay, ref.id});
  }
  timers_->at(now + q.cfg.max_wait_ms, TimerRef{id_, TimerKind::kMaxWait, ref.id});
  refresh_entry(ref);
  ref.last_position = position_of(ref);
  ref.position_sent_at = now;  // the Enqueue answer carries it
  {
    LogRec lr("info", "entry.enqueued", "caller queued");
    if (lr.on()) lc(lr, ix.id).str("queue_id", ref.queue.empty() ? std::string() : ref.queue).str("entry_id", ref.id)
                     .str("agent_id", ref.direct_agent).attr("enqueue_seq", static_cast<int64_t>(ref.enqueue_seq))
                     .attr("priority", static_cast<int64_t>(ref.priority)).attr("reason", rv1::EnqueueReason_Name(r.reason()))
                     .attr("position", static_cast<int64_t>(ref.last_position)).attr("vip", ref.vip);
  }
  todo_entries_.push_back(ref.id);
  return ref;
}

// The entry leaves RE for good (consumed, dequeued, caller gone, replaced).
void Tenant::remove_entry(Entry& e, int64_t now) {
  if (!e.offer_id.empty()) revoke_offer(e.offer_id, av1::REVOKE_REASON_CALLER_GONE, now);
  const std::string id = e.id, queue = e.queue;
  e.state = rv1::ENTRY_STATE_UNSPECIFIED;
  refresh_entry(e);
  auto ix = interactions_.find(e.interaction);
  if (ix != interactions_.end()) {
    touch_ix(ix->first);
    if (ix->second.entry_id == id) ix->second.entry_id.clear();
    ix->second.past_entries.insert(id);
  }
  skip_entries_.erase(id);
  entries_.erase(id);
  positions(queue, now);
}

// Back to WAITING at its ordering time (L3, L4, L7, L8): offered again, max wait still counts.
void Tenant::entry_back(Entry& e, int64_t now) {
  e.state = rv1::ENTRY_STATE_WAITING;
  e.reservation_id.clear();
  e.last_position = 0;  // positions() tells IE where it is again (CR-062)
  refresh_entry(e);
  if (e.overflow_due) {
    e.overflow_due = false;
    overflow(e, now);  // its own PositionUpdates
    return;
  }
  positions(e.queue, now);
  todo_entries_.push_back(e.id);
}

// max_wait_s fired (PLAN §4.2): every entry leaves its queue by this action at the latest.
void Tenant::overflow(Entry& e, int64_t now) {
  touch_entry(e.id);
  touch_queue(e.queue);
  if (e.state == rv1::ENTRY_STATE_RESERVED) {
    e.overflow_due = true;
    return;
  }
  if (e.state != rv1::ENTRY_STATE_WAITING || !e.callback_id.empty() || e.callback_offered) return;
  const QueueSt& q = queues_.at(e.queue);
  {
    LogRec lr("info", "entry.overflowed", "max wait reached: the queue's overflow action");
    if (lr.on()) lc(lr, e.interaction).str("queue_id", e.queue).str("entry_id", e.id).attr("action", action_name(q.cfg.max_wait_action))
                     .duration_ms(static_cast<double>(std::max<int64_t>(0, now - e.queue_entered_at)));
  }
  if (q.cfg.max_wait_action == MaxWaitAction::kCallbackOffer) {
    // L15: the caller is offered a callback; the entry keeps its place (and its offer, if any).
    e.callback_offered = true;
    auto ix = interactions_.find(e.interaction);
    if (ix != interactions_.end() && !test_drop_overflow_) {
      rv1::WatchResponse m;
      auto* ov = m.mutable_overflow();
      *ov->mutable_target() = target_of(ix->second);
      ov->set_entry_id(e.id);
      ov->set_action(rv1::OVERFLOW_ACTION_CALLBACK_OFFER);
      send_ie(ix->second, std::move(m), now);
    }
    ++queues_.at(e.queue).bucket.overflowed;
    ++counters_->overflows;
    return;
  }
  if (!e.offer_id.empty()) revoke_offer(e.offer_id, av1::REVOKE_REASON_CALLER_GONE, now);
  left_waiting(e, now, /*abandoned=*/false);
  auto ix = interactions_.find(e.interaction);
  rv1::WatchResponse m;
  auto* ov = m.mutable_overflow();
  if (ix != interactions_.end()) *ov->mutable_target() = target_of(ix->second);
  ov->set_entry_id(e.id);
  ov->set_action(static_cast<rv1::OverflowAction>(q.cfg.max_wait_action));
  const std::string old_queue = e.queue;
  if (q.cfg.max_wait_action == MaxWaitAction::kQueue && queues_.contains(q.cfg.max_wait_queue)) {
    // L14: same entry, same times, the new queue's timers.
    ov->set_queue_id(q.cfg.max_wait_queue);
    e.state = rv1::ENTRY_STATE_UNSPECIFIED;
    refresh_entry(e);
    e.queue = q.cfg.max_wait_queue;
    e.target.set_queue_id(e.queue);
    e.state = rv1::ENTRY_STATE_WAITING;
    e.escalated = false;
    e.queue_entered_at = now;
    e.last_position = 0;  // its place in the new queue goes out after the Overflow (CR-062)
    {
      QueueSt& qs = queues_.at(e.queue);
      touch_queue(e.queue);
      ++qs.arrivals;
      ++qs.bucket.offered;
      count_offered_10s(&qs.bucket, now);
      qs.prio_lo = std::min(qs.prio_lo, e.priority);
      qs.prio_hi = std::max(qs.prio_hi, e.priority);
    }
    const QueueSt& nq = queues_.at(e.queue);
    e.delay_passed = nq.secondary_delay_ms <= 0;
    refresh_entry(e);
    timers_->at(now + nq.escalate_after_ms, TimerRef{id_, TimerKind::kEscalate, e.id});
    timers_->at(now + nq.cfg.max_wait_ms, TimerRef{id_, TimerKind::kMaxWait, e.id});
    if (!e.delay_passed) timers_->at(now + nq.secondary_delay_ms, TimerRef{id_, TimerKind::kSecondaryDelay, e.id});
    todo_entries_.push_back(e.id);
  } else {
    // L13: OVERFLOWED, offered to no one, kept until Dequeue(OVERFLOW).
    if (q.cfg.max_wait_action == MaxWaitAction::kFlowStep) ov->set_flow_step(q.cfg.max_wait_flow_step);
    e.state = rv1::ENTRY_STATE_OVERFLOWED;
    refresh_entry(e);
  }
  if (ix != interactions_.end() && !test_drop_overflow_) send_ie(ix->second, std::move(m), now);
  positions(old_queue, now);
  if (e.queue != old_queue) positions(e.queue, now);
  ++queues_.at(old_queue).bucket.overflowed;
  ++counters_->overflows;
}

void Tenant::end_reservation(Reservation& r, rv1::Outcome o, int64_t now) {
  touch_res(r.id);
  touch_agent(r.agent);
  r.st = ResSt::kEnded;
  if (r.outcome == rv1::OUTCOME_UNSPECIFIED) r.outcome = o;
  r.ended_at = now;
  auto a = agents_.find(r.agent);
  if (a != agents_.end()) {
    a->second.reservations.erase(r.id);
    refresh_agent(a->second);
  }
}

void Tenant::start_wrap_up(Reservation& r, int64_t now) {
  // Phase 8: a campaign call's wrap-up has the default limit (no queue; "" is the direct entries').
  auto qit = r.campaign.empty() ? queues_.find(r.queue) : queues_.end();
  const int64_t limit = qit != queues_.end() ? qit->second.cfg.wrap_up_limit_ms : 60'000;
  touch_res(r.id);
  r.st = ResSt::kWrapUp;
  r.wrap_ends = now + limit;
  AgentSt& a = agents_.at(r.agent);
  av1::WatchResponse m;
  auto* w = m.mutable_wrap_up_started();
  w->set_reservation_id(r.id);
  w->set_interaction_id(r.interaction);
  w->set_ends_at_ms(r.wrap_ends);
  send_gw(a, std::move(m), now);
  {
    LogRec lr("info", "wrap_up.started", "the call ended: wrap-up");
    if (lr.on()) lc(lr, r.interaction).str("reservation_id", r.id).str("agent_id", r.agent)
                     .attr("ends_in_ms", r.wrap_ends - now).attr("campaign_id", r.campaign);
  }
  set_agent_state(a, av1::AGENT_STATE_WRAP_UP, "", now, /*echo=*/true);
  timers_->at(r.wrap_ends, TimerRef{id_, TimerKind::kWrapUpLimit, r.id});
  if (qit != queues_.end() && r.connected_at > 0) {
    // Talk time now; wrap-up is added when it ends.
    r.ended_at = now;
  }
}

void Tenant::finish_wrap_up(Reservation& r, int64_t now, bool echo, const std::string& disposition) {
  {
    LogRec lr("info", "wrap_up.ended", "wrap-up ended");
    if (lr.on()) lc(lr, r.interaction).str("reservation_id", r.id).str("agent_id", r.agent)
                     .attr("ended_by", disposition.empty() && echo ? "limit" : "agent");
  }
  const int64_t talk = r.connected_at > 0 && r.ended_at >= r.connected_at ? r.ended_at - r.connected_at : 0;
  end_reservation(r, r.outcome, now);
  auto qit = r.campaign.empty() ? queues_.find(r.queue) : queues_.end();  // phase 8: no queue statistics
  if (qit != queues_.end() && r.connected_at > 0) {
    touch_queue(r.queue);
    const double handle = static_cast<double>(talk + (now - (r.wrap_ends - qit->second.cfg.wrap_up_limit_ms)));
    qit->second.aht_ms = 0.9 * qit->second.aht_ms + 0.1 * std::max(1000.0, handle);
    const double wrap = static_cast<double>(std::max<int64_t>(0, now - (r.wrap_ends - qit->second.cfg.wrap_up_limit_ms)));
    qit->second.wrap_ms = 0.9 * qit->second.wrap_ms + 0.1 * wrap;
    qit->second.bucket.handle_ms += std::max(0.0, handle);
    ++qit->second.bucket.handled;
    ++qit->second.win_handled;
    qit->second.win_handle_ms += std::max(0.0, handle);
  }
  if (events_) {
    // C12: the disposition reaches reports on agent.events (WrapUpEnded carries none).
    events_->publish(AgentEvent{"wrap_up_ended", id_, r.agent, now,
                                "{\"reservation_id\":" + json_str(r.id) + ",\"interaction_id\":" + json_str(r.interaction) +
                                    ",\"queue_id\":" + json_str(r.queue) + ",\"disposition_code\":" + json_str(disposition) +
                                    ",\"ended_by\":\"" + (disposition.empty() && echo ? "limit" : "agent") + "\"}", ""});
  }
  auto ix = interactions_.find(r.interaction);
  if (ix != interactions_.end()) {
    rv1::WatchResponse m;
    auto* w = m.mutable_wrap_up_ended();
    *w->mutable_target() = target_of(ix->second);
    w->set_reservation_id(r.id);
    w->set_agent_id(r.agent);
    w->set_ended_at_ms(now);
    send_ie(ix->second, std::move(m), now);
  }
  AgentSt& a = agents_.at(r.agent);
  become_free(a, now, echo);
}

// ======================================================================================
// routing.v1
// ======================================================================================

Result<rv1::EnqueueResponse> Tenant::enqueue(const rv1::EnqueueRequest& r, int64_t now) {
  if (r.enqueue_seq() == 0) return err(Code::kInvalidArgument, "invalid_request", "enqueue_seq");
  if (r.priority() > 100) return err(Code::kInvalidArgument, "invalid_request", "priority");
  // Phase 3: a queue, one agent (a direct offer or a transfer to it) or a consult from the
  // connected agent to a queue or another agent. The target's key keeps repeats exact.
  std::string qid, tkey;
  std::string direct;
  switch (r.target().kind_case()) {
    case rv1::Target::kQueueId:
      qid = r.target().queue_id();
      tkey = "q:" + qid;
      break;
    case rv1::Target::kAgentId:
      direct = r.target().agent_id();
      if (!valid_agent_id(direct)) return err(Code::kInvalidArgument, "invalid_request", "target agent");
      qid = kDirect;
      tkey = "a:" + direct;
      break;
    case rv1::Target::kConsult: {
      const auto& c = r.target().consult();
      if (!valid_agent_id(c.from_agent_id())) return err(Code::kInvalidArgument, "invalid_request", "consult from");
      if (c.to_case() == rv1::ConsultTarget::kQueueId) {
        qid = c.queue_id();
        tkey = "c:" + c.from_agent_id() + ":q:" + qid;
      } else if (c.to_case() == rv1::ConsultTarget::kAgentId) {
        direct = c.agent_id();
        if (!valid_agent_id(direct) || direct == c.from_agent_id()) {
          return err(Code::kInvalidArgument, "invalid_request", "consult target agent");
        }
        qid = kDirect;
        tkey = "c:" + c.from_agent_id() + ":a:" + direct;
      } else {
        return err(Code::kInvalidArgument, "invalid_request", "consult target");
      }
      break;
    }
    default:
      return err(Code::kInvalidArgument, "invalid_request", "target");
  }
  if (!direct.empty()) {
    // Tenant isolation: only an agent of this tenant, and only one logged in, takes a direct offer.
    auto da = agents_.find(direct);
    // Phase 8: nor a lent one (campaign agents take no transfers, consults or direct calls in v1).
    if (da == agents_.end() || !da->second.attached || da->second.state == av1::AGENT_STATE_LOGGED_OUT ||
        !da->second.campaign.empty()) {
      return err(Code::kFailedPrecondition, "agent_unavailable", "the agent can't take a direct offer");
    }
  }
  for (const auto& rq : r.requirements()) {
    if (rq.skill_id().empty() || rq.min_level() > 5) {
      return err(Code::kInvalidArgument, "invalid_request", "requirement");
    }
  }
  for (int64_t t : {r.first_queued_at_ms(), r.ordering_at_ms()}) {
    if (t <= 0 || now - t > kDay) return err(Code::kInvalidArgument, "invalid_request", "time");
  }
  auto qit = queues_.find(qid);
  if (qit == queues_.end() || (qid.empty() && direct.empty())) {
    return err(Code::kFailedPrecondition, "queue_unknown", "no such queue");
  }

  auto own = owned(r.owner(), /*create=*/true, now);
  if (!own) return std::unexpected(own.error());
  Interaction& ix = **own;
  if (r.target().kind_case() == rv1::Target::kConsult) {
    // A consult comes from an agent connected to this interaction.
    bool connected = false;
    for (const auto& rid : ix.reservations) {
      auto res = reservations_.find(rid);
      if (res != reservations_.end() && res->second.agent == r.target().consult().from_agent_id() &&
          res->second.st == ResSt::kConnected) {
        connected = true;
      }
    }
    if (!connected) return err(Code::kInvalidArgument, "invalid_request", "the consulting agent isn't connected");
  }

  if (r.enqueue_seq() < ix.max_enqueue_seq) {
    return err(Code::kFailedPrecondition, "stale_enqueue", "a higher enqueue_seq exists");
  }
  if (r.enqueue_seq() == ix.max_enqueue_seq) {
    // A repeat: the entry made first, wherever RE has moved it since.
    if (ix.seq_queue[r.enqueue_seq()] != tkey) {
      return err(Code::kAlreadyExists, "enqueue_conflict", "same enqueue_seq, another target");
    }
    rv1::EnqueueResponse out;
    auto live = entries_.find(ix.entry_id);
    if (live != entries_.end() && live->second.enqueue_seq == r.enqueue_seq()) {
      *out.mutable_entry() = entry_view(live->second, now);
    } else {
      *out.mutable_entry() = ix.snaps[r.enqueue_seq()];
    }
    out.set_existing(true);
    return out;
  }
  // A higher enqueue_seq replaces the live entry.
  if (auto live = entries_.find(ix.entry_id); live != entries_.end()) {
    Entry& old = live->second;
    if (old.state == rv1::ENTRY_STATE_RESERVED) {
      auto res = reservations_.find(old.reservation_id);
      if (res != reservations_.end() && res->second.st == ResSt::kAssigned) {
        end_reservation(res->second, rv1::OUTCOME_REVOKED, now);
        become_free(agents_.at(res->second.agent), now, /*echo=*/true);
      }
    }
    remove_entry(old, now);
  }
  QueueSt& q = qit->second;
  if (q.waiting.size() >= q.cfg.max_entries) {
    settle(now);
    return err(Code::kResourceExhausted, "queue_full", "the queue is full");
  }
  Entry& e = create_entry(ix, r, q, now);
  const std::string eid = e.id;
  ix.max_enqueue_seq = r.enqueue_seq();
  ix.seq_queue[r.enqueue_seq()] = tkey;
  if (!e.direct_agent.empty()) ++counters_->direct_offers;
  if (!e.consult_from.empty()) ++counters_->consults;
  settle(now);
  rv1::EnqueueResponse out;
  auto it = entries_.find(eid);
  if (it != entries_.end()) {
    *out.mutable_entry() = entry_view(it->second, now);
    ix.snaps[r.enqueue_seq()] = out.entry();
  }
  out.set_existing(false);
  return out;
}

Result<rv1::DequeueResponse> Tenant::dequeue(const rv1::DequeueRequest& r, int64_t now) {
  if (!valid_uuid(r.entry_id()) || r.reason() == rv1::DEQUEUE_REASON_UNSPECIFIED) {
    return err(Code::kInvalidArgument, "invalid_request", "entry_id or reason");
  }
  auto own = owned(r.owner(), /*create=*/false, now);
  if (!own) {
    if (own.error().code == Code::kNotFound) return err(Code::kNotFound, "entry_unknown");
    return std::unexpected(own.error());
  }
  Interaction& ix = **own;
  rv1::DequeueResponse out;
  if (ix.entry_id == r.entry_id()) {
    Entry& e = entries_.at(r.entry_id());
    if (e.state == rv1::ENTRY_STATE_RESERVED) {
      // L17: the caller left while Assigned is on its way.
      auto res = reservations_.find(e.reservation_id);
      if (res != reservations_.end() && res->second.st == ResSt::kAssigned) {
        const bool gone = r.reason() == rv1::DEQUEUE_REASON_ABANDONED ||
                          r.reason() == rv1::DEQUEUE_REASON_ENDED;
        end_reservation(res->second, gone ? rv1::OUTCOME_CALLER_GONE : rv1::OUTCOME_REVOKED, now);
        become_free(agents_.at(res->second.agent), now, /*echo=*/true);
      }
    }
    if (e.state == rv1::ENTRY_STATE_WAITING) left_waiting(e, now, r.reason() == rv1::DEQUEUE_REASON_ABANDONED);
    {
      LogRec lr("info", "entry.dequeued", "caller left the queue");
      if (lr.on()) lc(lr, ix.id).str("queue_id", e.queue).str("entry_id", e.id).attr("reason", dequeue_name(r.reason()))
                       .duration_ms(static_cast<double>(std::max<int64_t>(0, now - e.queue_entered_at)));
    }
    remove_entry(e, now);
    out.set_already_gone(false);
  } else if (ix.past_entries.contains(r.entry_id())) {
    out.set_already_gone(true);
  } else {
    return err(Code::kNotFound, "entry_unknown", "no such entry");
  }
  settle(now);
  return out;
}

Result<rv1::ReportOutcomeResponse> Tenant::report_outcome(const rv1::ReportOutcomeRequest& r,
                                                          int64_t now) {
  if (!valid_uuid(r.reservation_id()) || r.outcome() < rv1::OUTCOME_CONNECTED ||
      r.outcome() > rv1::OUTCOME_CUSTOMER_NO_ANSWER) {
    return err(Code::kInvalidArgument, "invalid_request", "reservation_id or outcome");
  }
  auto own = owned(r.owner(), /*create=*/false, now);
  if (!own) {
    if (own.error().code == Code::kNotFound) return err(Code::kNotFound, "reservation_unknown");
    return std::unexpected(own.error());
  }
  Interaction& ix = **own;
  auto rit = reservations_.find(r.reservation_id());
  if (rit == reservations_.end() || rit->second.interaction != ix.id) {
    return err(Code::kNotFound, "reservation_unknown", "no such reservation");
  }
  Reservation& res = rit->second;
  rv1::ReportOutcomeResponse out;
  if (res.outcome != rv1::OUTCOME_UNSPECIFIED) {
    // The first outcome wins (L9: EXPIRED/REVOKED after a race). `existing` is true only for a
    // repeat of the recorded outcome (L17's CALLER_GONE); a late CONNECTED after RE's own EXPIRED
    // is IE's first report: existing false, recorded EXPIRED (CR-071).
    out.set_existing(r.outcome() == res.outcome);
    out.set_recorded(res.outcome);
    return out;
  }
  if (res.st != ResSt::kAssigned ||
      (r.outcome() == rv1::OUTCOME_CUSTOMER_NO_ANSWER && res.callback_id.empty() && res.campaign.empty())) {
    return err(Code::kInvalidArgument, "invalid_request", "outcome not allowed now");
  }
  {
    LogRec lr("info", "reservation.reported", "IE reported the assignment's outcome");
    if (lr.on()) lc(lr, ix.id).str("reservation_id", res.id).str("agent_id", res.agent).str("entry_id", res.entry_id)
                     .attr("outcome", outcome_name(r.outcome())).attr("campaign_id", res.campaign);
  }
  AgentSt& a = agents_.at(res.agent);
  auto eit = entries_.find(res.entry_id);
  if (!res.callback_id.empty()) {
    // A callback's attempt (RunCallback): the agent as for any reservation; the callback decides
    // what comes next (done, another attempt, flagged).
    const std::string cb = res.callback_id;
    if (r.outcome() == rv1::OUTCOME_CONNECTED) {
      touch_res(res.id);
      res.outcome = rv1::OUTCOME_CONNECTED;
      res.st = ResSt::kConnected;
      res.connected_once = true;
      res.connected_at = now;
      if (eit != entries_.end()) remove_entry(eit->second, now);
      set_agent_state(a, av1::AGENT_STATE_ON_CALL, "", now, /*echo=*/true);
    } else {
      end_reservation(res, r.outcome(), now);
      if (r.outcome() == rv1::OUTCOME_AGENT_NO_ANSWER) {
        a.pending = av1::TARGET_STATE_UNSPECIFIED;
        a.pending_reason.clear();
        set_agent_state(a, av1::AGENT_STATE_NOT_READY, "missed_offer", now, /*echo=*/true);
      } else {
        become_free(a, now, /*echo=*/true);
      }
    }
    callback_ended(cb, r.outcome(), now);
    settle(now);
    out.set_existing(false);
    out.set_recorded(r.outcome());
    return out;
  }
  switch (r.outcome()) {
    case rv1::OUTCOME_CONNECTED: {  // L2
      touch_res(res.id);
      res.outcome = rv1::OUTCOME_CONNECTED;
      res.st = ResSt::kConnected;
      res.connected_once = true;
      res.connected_at = now;
      if (eit != entries_.end()) {
        ix.snaps[eit->second.enqueue_seq] = entry_view(eit->second, now);
        remove_entry(eit->second, now);
      }
      set_agent_state(a, av1::AGENT_STATE_ON_CALL, "", now, /*echo=*/true);
      break;
    }
    case rv1::OUTCOME_AGENT_NO_ANSWER:  // L3: the missed-offer rule
      end_reservation(res, rv1::OUTCOME_AGENT_NO_ANSWER, now);
      if (eit != entries_.end()) entry_back(eit->second, now);
      a.pending = av1::TARGET_STATE_UNSPECIFIED;
      a.pending_reason.clear();
      set_agent_state(a, av1::AGENT_STATE_NOT_READY, "missed_offer", now, /*echo=*/true);
      break;
    case rv1::OUTCOME_AGENT_LEG_FAILED:  // L4: as L3, no penalty
      end_reservation(res, rv1::OUTCOME_AGENT_LEG_FAILED, now);
      if (eit != entries_.end()) entry_back(eit->second, now);
      become_free(a, now, /*echo=*/true);
      break;
    case rv1::OUTCOME_CALLER_GONE:  // L5
      end_reservation(res, rv1::OUTCOME_CALLER_GONE, now);
      if (eit != entries_.end()) remove_entry(eit->second, now);
      become_free(a, now, /*echo=*/true);
      break;
    case rv1::OUTCOME_ALREADY_ASSIGNED:  // L6
      end_reservation(res, rv1::OUTCOME_ALREADY_ASSIGNED, now);
      if (eit != entries_.end() && eit->second.reservation_id == res.id) remove_entry(eit->second, now);
      become_free(a, now, /*echo=*/true);
      break;
    case rv1::OUTCOME_CUSTOMER_NO_ANSWER:  // phase 8: a campaign call nobody answered (checked above)
      end_reservation(res, rv1::OUTCOME_CUSTOMER_NO_ANSWER, now);
      become_free(a, now, /*echo=*/true);
      break;
    default:
      break;
  }
  settle(now);
  out.set_existing(false);
  out.set_recorded(r.outcome());
  return out;
}

Result<rv1::CallEndedResponse> Tenant::call_ended(const rv1::CallEndedRequest& r, int64_t now) {
  if (!valid_uuid(r.reservation_id()) || !valid_agent_id(r.agent_id())) {
    return err(Code::kInvalidArgument, "invalid_request", "reservation_id or agent_id");
  }
  auto own = owned(r.owner(), /*create=*/false, now);
  if (!own) {
    if (own.error().code == Code::kNotFound) return err(Code::kNotFound, "reservation_unknown");
    return std::unexpected(own.error());
  }
  Interaction& ix = **own;
  auto rit = reservations_.find(r.reservation_id());
  if (rit == reservations_.end() || rit->second.interaction != ix.id ||
      rit->second.agent != r.agent_id()) {
    return err(Code::kNotFound, "reservation_unknown", "no such reservation");
  }
  Reservation& res = rit->second;
  rv1::CallEndedResponse out;
  if (res.st == ResSt::kWrapUp || (res.st == ResSt::kEnded && res.connected_once)) {
    out.set_existing(true);
    return out;
  }
  if (res.st != ResSt::kConnected) {
    return err(Code::kInvalidArgument, "invalid_request", "the reservation never connected");
  }
  start_wrap_up(res, now);  // L10
  settle(now);
  out.set_existing(false);
  return out;
}

Result<rv1::ReattachResponse> Tenant::reattach(const rv1::ReattachRequest& r, int64_t now) {
  bool rebound = false;
  auto own = owned(r.owner(), /*create=*/true, now, &rebound);
  if (!own) return std::unexpected(own.error());
  Interaction& ix = **own;
  // Rebinds the interaction's messages to this replica and replays them (owned() already did
  // when the epoch rose).
  if (!rebound) {
    ix.replica = r.owner().replica_id();
    replay_ie(ix, now);
  }
  rv1::ReattachResponse out;
  if (auto e = entries_.find(ix.entry_id); e != entries_.end()) {
    *out.mutable_entry() = entry_view(e->second, now);
  }
  for (const auto& rid : ix.reservations) {
    const Reservation& res = reservations_.at(rid);
    if (res.st == ResSt::kEnded) continue;
    auto* v = out.add_reservations();
    v->set_reservation_id(res.id);
    v->set_agent_id(res.agent);
    v->set_agent_contact(res.contact);
    if (res.st == ResSt::kAssigned) {
      v->set_deadline_ms(res.deadline_ms);
      v->set_state(rv1::RESERVATION_STATE_ASSIGNED);
    } else {
      v->set_state(rv1::RESERVATION_STATE_CONNECTED);
    }
  }
  out.set_last_delivery_seq(ix.next_seq);
  settle(now);
  return out;
}

void Tenant::on_ie_stream_open(const std::string& replica, uint64_t gen, int64_t now) {
  uint64_t& have = ie_gen_[replica];
  if (gen <= have) return;  // an older stream's replay, run after a newer one's
  have = gen;
  for (auto& [id, ix] : interactions_) {
    if (ix.replica == replica) replay_ie(ix, now);
  }
  // Phase 8: the campaigns this replica leads get every lent agent's availability (never replayed:
  // the snapshot makes good what a dropped stream lost).
  for (const auto& [cid, c] : campaigns_) {
    if (c.replica == replica) send_campaign_snapshot(c);
  }
}

// ======================================================================================
// agents.v1
// ======================================================================================

Result<av1::AttachAgentResponse> Tenant::attach(const av1::AttachAgentRequest& r, int64_t now) {
  if (!valid_agent_id(r.agent_id()) || !valid_uuid(r.session_id()) || r.replica_id().empty()) {
    return err(Code::kInvalidArgument, "invalid_request", "agent_id, session_id or replica_id");
  }
  auto it = agents_.find(r.agent_id());
  if (it == agents_.end()) return err(Code::kNotFound, "agent_unknown", "no such agent");
  AgentSt& a = it->second;
  av1::AttachAgentResponse out;
  const bool existing = a.generation > 0 && a.session_id == r.session_id();
  if (!existing) {
    if (a.generation > 0 && !a.session_id.empty()) {
      av1::WatchResponse m;
      m.mutable_detached()->set_reason(av1::DETACHED_REASON_SUPERSEDED);
      send_gw(a, std::move(m), now);
    }
    // Generations only rise, also across incarnations (PLAN §8 restart rule).
    a.generation = std::max(a.generation, epochs_->seq_base) + 1;
    a.next_seq = epochs_->seq_base;
    a.unacked.clear();  // the snapshot replaces the backlog
    a.session_id = r.session_id();
  }
  if (a.replica != r.replica_id()) {
    // Another gateway replica serves the agent now: what it hasn't acknowledged goes there
    // first, in order (CR-053), before anything this attach sends.
    a.replica = r.replica_id();
    replay_gw(a, now);
  }
  if (!r.agent_contact().empty()) a.contact = r.agent_contact();
  a.attached = true;
  a.detached_at = 0;
  if (a.state == av1::AGENT_STATE_LOGGED_OUT) {
    a.primary_since = 0;  // PLAN §4.4: no dwell owed at login
    a.secondary_since = 0;
    a.move_times.clear();
    set_agent_state(a, av1::AGENT_STATE_NOT_READY, "login", now, /*echo=*/false);
    refresh_agent(a);
  }
  refresh_agent(a);
  note_availability(a);  // phase 8: a lent agent's desktop is back
  if (agent_available(a)) todo_agents_.push_back(a.cfg.id);
  settle(now);
  out.set_attach_generation(a.generation);
  out.set_existing(existing);
  *out.mutable_agent() = view(a);
  if (auto o = offers_.find(a.offer_id); o != offers_.end()) {
    const Entry& e = entries_.at(o->second.entry);
    auto* of = out.mutable_offer();
    of->set_offer_id(o->first);
    of->set_interaction_id(e.interaction);
    of->set_channel(av1::CHANNEL_VOICE);
    of->set_queue_id(e.queue);
    of->set_expires_at_ms(o->second.expires_at);
    of->set_tier(tier_pb(o->second.tier));
    of->set_consult(!e.consult_from.empty());
    of->set_direct(!e.direct_agent.empty() && e.consult_from.empty());
    of->set_callback(!e.callback_id.empty());
  }
  for (const auto& rid : a.reservations) *out.add_reservations() = res_view(reservations_.at(rid));
  out.set_leader_epoch(epochs_->leader_epoch);
  return out;
}

Result<av1::DetachAgentResponse> Tenant::detach(const av1::DetachAgentRequest& r, int64_t now) {
  av1::DetachAgentResponse out;
  auto it = agents_.find(r.session().agent_id());
  if (it == agents_.end()) return err(Code::kNotFound, "agent_unknown", "no such agent");
  AgentSt& a = it->second;
  if (r.session().attach_generation() != a.generation || r.session().session_id() != a.session_id ||
      !a.attached) {
    out.set_applied(false);
    return out;
  }
  a.attached = false;
  a.detached_at = now;
  refresh_agent(a);
  note_availability(a);  // phase 8: a lent agent without its desktop can't be reserved
  if (!a.offer_id.empty()) {
    // Re-offered elsewhere with its ordering time kept, no penalty.
    revoke_offer(a.offer_id, av1::REVOKE_REASON_DETACHED, now);
    refresh_agent(a);
  }
  if (r.reason() == av1::DETACH_REASON_LOGOUT) {
    ask(a, av1::TARGET_STATE_LOGGED_OUT, "", now, /*echo=*/true);
  } else {
    timers_->at(now + cfg_.grace_ms, TimerRef{id_, TimerKind::kDetachGrace, a.cfg.id});
  }
  settle(now);
  out.set_applied(true);
  return out;
}

Result<av1::SetStateResponse> Tenant::set_state(const av1::SetStateRequest& r, int64_t now) {
  auto s = session(r.session());
  if (!s) return std::unexpected(s.error());
  if (!valid_command_id(r.command_id())) return err(Code::kInvalidArgument, "invalid_request", "command_id");
  const std::string fp = "set|" + std::to_string(r.target()) + "|" + r.reason_code();
  if (auto rep = repeat<av1::SetStateResponse>(r.command_id(), fp)) return *rep;
  AgentSt& a = **s;
  Result<av1::SetStateResponse> res = [&]() -> Result<av1::SetStateResponse> {
    if (r.target() == av1::TARGET_STATE_UNSPECIFIED ||
        (r.target() == av1::TARGET_STATE_NOT_READY && r.reason_code().empty())) {
      return err(Code::kInvalidArgument, "invalid_request", "target or reason_code");
    }
    if (r.target() == av1::TARGET_STATE_READY && a.state == av1::AGENT_STATE_LOGGED_OUT) {
      return err(Code::kFailedPrecondition, "invalid_transition", "ready while logged out");
    }
    ask(a, r.target(), r.reason_code(), now, /*echo=*/false);
    settle(now);
    av1::SetStateResponse out;
    *out.mutable_agent() = view(a);
    return out;
  }();
  return remember(r.command_id(), fp, std::move(res), now);
}

Result<av1::AnswerOfferResponse> Tenant::answer_offer(const av1::AnswerOfferRequest& r, int64_t now) {
  auto s = session(r.session());
  if (!s) return std::unexpected(s.error());
  if (!valid_command_id(r.command_id())) return err(Code::kInvalidArgument, "invalid_request", "command_id");
  const std::string fp = "answer|" + r.offer_id() + "|" + std::to_string(r.decision());
  if (auto rep = repeat<av1::AnswerOfferResponse>(r.command_id(), fp)) return *rep;
  AgentSt& a = **s;
  Result<av1::AnswerOfferResponse> res = [&]() -> Result<av1::AnswerOfferResponse> {
    if (r.decision() != av1::DECISION_ACCEPT && r.decision() != av1::DECISION_REJECT) {
      return err(Code::kInvalidArgument, "invalid_request", "decision");
    }
    if (r.offer_id().empty() || a.offer_id != r.offer_id()) {
      return err(Code::kFailedPrecondition, "offer_gone", "the offer is gone");
    }
    const Offer& o = offers_.at(r.offer_id());
    if (now >= o.expires_at) {
      // An answer at or after expires_at_ms loses, even before the timer ran.
      expire_offer(r.offer_id(), now);
      settle(now);
      return err(Code::kFailedPrecondition, "offer_gone", "the offer expired");
    }
    av1::AnswerOfferResponse out;
    out.set_decision(r.decision());
    if (r.decision() == av1::DECISION_ACCEPT) {
      accept(r.offer_id(), now, out.mutable_reservation());
    } else {
      reject(r.offer_id(), now, /*to_ready=*/true);
    }
    settle(now);
    return out;
  }();
  return remember(r.command_id(), fp, std::move(res), now);
}

Result<av1::EndWrapUpResponse> Tenant::end_wrap_up(const av1::EndWrapUpRequest& r, int64_t now) {
  auto s = session(r.session());
  if (!s) return std::unexpected(s.error());
  if (!valid_command_id(r.command_id())) return err(Code::kInvalidArgument, "invalid_request", "command_id");
  const std::string fp = "wrap|" + r.reservation_id() + "|" + r.disposition_code();
  if (auto rep = repeat<av1::EndWrapUpResponse>(r.command_id(), fp)) return *rep;
  AgentSt& a = **s;
  Result<av1::EndWrapUpResponse> res = [&]() -> Result<av1::EndWrapUpResponse> {
    auto rit = reservations_.find(r.reservation_id());
    if (rit == reservations_.end() || rit->second.agent != a.cfg.id) {
      return err(Code::kNotFound, "reservation_unknown", "no such reservation");
    }
    Reservation& res = rit->second;
    if (res.st == ResSt::kWrapUp) {
      finish_wrap_up(res, now, /*echo=*/false, r.disposition_code());
    } else if (res.st != ResSt::kEnded) {
      return err(Code::kFailedPrecondition, "invalid_transition", "not in wrap-up");
    }
    settle(now);
    av1::EndWrapUpResponse out;
    *out.mutable_agent() = view(a);
    return out;
  }();
  return remember(r.command_id(), fp, std::move(res), now);
}

void Tenant::gw_ack(const av1::Ack& k) {
  auto it = agents_.find(k.agent_id());
  if (it == agents_.end()) return;
  AgentSt& a = it->second;
  if (k.attach_generation() != a.generation) return;
  touch_agent(a.cfg.id);
  auto& q = a.unacked;
  q.erase(std::remove_if(q.begin(), q.end(), [&](const OutGw& m) { return m.seq == k.delivery_seq(); }),
          q.end());
}

void Tenant::on_gw_stream_open(const std::string& replica, uint64_t gen, int64_t now) {
  uint64_t& have = gw_gen_[replica];
  if (gen <= have) return;
  have = gen;
  for (auto& [id, a] : agents_) {
    if (a.replica == replica) replay_gw(a, now);
  }
}

// ======================================================================================
// Timers and periodic work
// ======================================================================================

void Tenant::on_timer(const TimerRef& t, int64_t now) {
  switch (t.kind) {
    case TimerKind::kOfferExpiry: {
      auto o = offers_.find(t.key);
      if (o != offers_.end() && now >= o->second.expires_at) expire_offer(t.key, now);
      break;
    }
    case TimerKind::kAssignDeadline: {  // L7
      auto rit = reservations_.find(t.key);
      if (rit == reservations_.end() || rit->second.st != ResSt::kAssigned) break;
      Reservation& res = rit->second;
      if (!res.callback_id.empty()) {
        // A RunCallback nobody reported on by deadline + 10 s: flagged, never redialled by itself.
        if (now < res.deadline_ms + kCallbackGraceMs) break;
        end_reservation(res, rv1::OUTCOME_EXPIRED, now);
        callback_ended(res.callback_id, rv1::OUTCOME_EXPIRED, now);
        become_free(agents_.at(res.agent), now, /*echo=*/true);
        ++counters_->assign_expired;
        break;
      }
      if (now < res.deadline_ms + kAssignGrace) break;
      auto ix = interactions_.find(res.interaction);
      if (ix == interactions_.end()) break;
      if (!sink_->ie_live(ix->second.replica)) {
        // Paused while the bound replica has no stream, at most 10 s (adoption budget).
        if (res.pause_since == 0) {
          res.pause_since = now;
          touch_res(res.id);
        }
        if (now - res.pause_since < kAdoptionBudget) {
          timers_->at(std::min(now + 1000, res.pause_since + kAdoptionBudget),
                      TimerRef{id_, TimerKind::kAssignDeadline, res.id});
          break;
        }
      }
      {
        LogRec lr("warn", "reservation.expired", "no outcome by the assignment deadline: revoked (L7)");
        if (lr.on()) lc(lr, res.interaction).str("reservation_id", res.id).str("agent_id", res.agent).str("entry_id", res.entry_id);
      }
      end_reservation(res, rv1::OUTCOME_EXPIRED, now);
      rv1::WatchResponse m;
      auto* rv = m.mutable_revoke();
      *rv->mutable_target() = target_of(ix->second);
      rv->set_reservation_id(res.id);
      rv->set_reason("expired");
      send_ie(ix->second, std::move(m), now);
      if (auto e = entries_.find(res.entry_id); e != entries_.end()) entry_back(e->second, now);
      become_free(agents_.at(res.agent), now, /*echo=*/true);
      ++counters_->assign_expired;
      break;
    }
    case TimerKind::kWrapUpLimit: {
      auto rit = reservations_.find(t.key);
      if (rit != reservations_.end() && rit->second.st == ResSt::kWrapUp && now >= rit->second.wrap_ends) {
        finish_wrap_up(rit->second, now, /*echo=*/true);
      }
      break;
    }
    case TimerKind::kEscalate: {
      auto e = entries_.find(t.key);
      if (e == entries_.end() || e->second.escalated || !e->second.direct_agent.empty()) break;
      const QueueSt& q = queues_.at(e->second.queue);
      if (now < e->second.queue_entered_at + q.escalate_after_ms) break;
      e->second.escalated = true;
      refresh_entry(e->second);
      todo_entries_.push_back(e->first);
      break;
    }
    case TimerKind::kMaxWait: {
      auto e = entries_.find(t.key);
      if (e == entries_.end()) break;
      const QueueSt& q = queues_.at(e->second.queue);
      if (now < e->second.queue_entered_at + q.cfg.max_wait_ms) break;  // an older queue's timer
      overflow(e->second, now);
      break;
    }
    case TimerKind::kSkipExpiry: {
      auto e = entries_.find(t.key);
      if (e == entries_.end()) break;
      touch_entry(t.key);
      std::erase_if(e->second.skips, [&](const auto& kv) { return kv.second <= now; });
      if (e->second.skips.empty()) skip_entries_.erase(t.key);
      todo_entries_.push_back(t.key);
      break;
    }
    case TimerKind::kSecondaryDelay: {
      auto e = entries_.find(t.key);
      if (e == entries_.end() || e->second.delay_passed) break;
      const QueueSt& q = queues_.at(e->second.queue);
      if (now < e->second.queue_entered_at + q.secondary_delay_ms) break;  // an older queue's timer
      e->second.delay_passed = true;
      refresh_entry(e->second);
      todo_entries_.push_back(e->first);
      break;
    }
    case TimerKind::kPinExpiry: {
      auto p = pins_.find(t.key);
      auto a = agents_.find(t.key);
      if (p == pins_.end() || a == agents_.end() || p->second.until_ms == 0 || now < p->second.until_ms) break;
      pins_.erase(p);
      touch_pin(t.key);
      a->second.primary_since = now;  // unpinned: back to the plan, dwell restarts
      retarget(a->second, av1::MEMBERSHIP_REASON_SUPERVISOR_UNPIN, now, /*check_coverage=*/true);
      break;
    }
    case TimerKind::kCallbackRedeliver:
    case TimerKind::kCallbackDue:
      on_callback_timer(t, now);
      break;
    case TimerKind::kDetachGrace: {
      auto a = agents_.find(t.key);
      if (a == agents_.end() || a->second.attached || a->second.detached_at == 0) break;
      if (now < a->second.detached_at + cfg_.grace_ms) break;
      a->second.detached_at = 0;
      ask(a->second, av1::TARGET_STATE_NOT_READY, "disconnected", now, /*echo=*/true);
      break;
    }
  }
  settle(now);
}

void Tenant::redeliver(int64_t now) {
  // Phase 7 (CR-187): only the interactions that may hold something (held_ix_), dropping the ones
  // that no longer do; the hub's liveness asked once per replica, not once per interaction.
  std::unordered_map<std::string, bool> live;
  for (auto hit = held_ix_.begin(); hit != held_ix_.end();) {
    auto it = interactions_.find(*hit);
    if (it == interactions_.end() || (it->second.unacked.empty() && !holds(it->second))) {
      hit = held_ix_.erase(hit);
      continue;
    }
    ++hit;
    Interaction& ix = it->second;
    if (ix.unacked.empty() || ix.replica.empty()) continue;
    auto lv = live.find(ix.replica);
    if (lv == live.end()) lv = live.emplace(ix.replica, sink_->ie_live(ix.replica)).first;
    if (!lv->second) continue;
    const uint64_t gen = ie_gen(ix.replica);
    for (auto& m : ix.unacked) {
      if (now - m.sent_at < kRedeliverAfter || !sendable(m.msg)) continue;
      m.sent_at = now;
      sink_->to_ie(ix.replica, gen, m.msg);
      ++counters_->redeliveries;
    }
  }
  for (auto& [id, a] : agents_) {
    if (a.replica.empty() || a.unacked.empty()) continue;
    const uint64_t gen = gw_gen(a.replica);
    for (auto& m : a.unacked) {
      if (now - m.sent_at < kRedeliverAfter) continue;
      m.sent_at = now;
      sink_->to_gw(a.replica, gen, m.msg);
      ++counters_->redeliveries;
    }
  }
}

// The safety net: any READY agent that could serve a waiting entry is a missed dispatch. Not
// while draining: no new offers then (CR-061).
void Tenant::sweep(int64_t now) {
  for (auto& [id, q] : queues_) {
    if (q.positions_pending) positions(id, now);  // CR-056: what the throttle held back
  }
  if (draining_) return;
  // Phase 4: changes that waited for coverage, and (auto mode) config.routing's min/max agents.
  for (auto& [id, a] : agents_) {
    if (a.retarget_due) {
      retarget(a, pinned(a, now) ? av1::MEMBERSHIP_REASON_SUPERVISOR_PIN : av1::MEMBERSHIP_REASON_CONFIG, now,
               /*check_coverage=*/true);
    }
  }
  settle(now);  // CR-175: agents a retry just freed are dispatched here, not judged as misses below
  stall_check(now);
  // Phase 7 (CR-186): the backstop below is a scan of READY agents, and with 40,000 of them it held
  // the writer ~40 ms every second (time to offer p99 62 ms at 1,000 calls/s). Nothing can match
  // while no entry waits; otherwise at most kSweepSlice agents per second, round robin, so a large
  // READY pool is covered over a few seconds (a match here is a bug either way).
  bool waiting = false;
  for (const auto& [id, q] : queues_) {
    if (!q.waiting.empty()) {
      waiting = true;
      break;
    }
  }
  if (!waiting) {
    settle(now);
    return;
  }
  std::vector<std::string> ready;
  if (ready_idle_.size() <= kSweepSlice) {
    for (const auto& [idle, id] : ready_idle_) ready.push_back(id);
    sweep_cursor_ = {};
  } else {
    ready.reserve(kSweepSlice);
    auto it = ready_idle_.lower_bound(sweep_cursor_);
    while (ready.size() < kSweepSlice) {  // fewer than the index holds: no agent twice
      if (it == ready_idle_.end()) it = ready_idle_.begin();
      ready.push_back(it->second);
      sweep_cursor_ = *it;
      ++it;
    }
    sweep_cursor_.second.push_back('\0');  // the next slice starts just after the last agent taken
  }
  for (const auto& id : ready) {
    AgentSt& a = agents_.at(id);
    if (!agent_available(a)) continue;
    if (Entry* e = pick_entry(a, now)) {
      ++counters_->sweep_matches;
      make_offer(*e, a, now);
    }
  }
  settle(now);
}

bool Tenant::holds(const Interaction& ix) const {
  if (!ix.entry_id.empty() || !ix.unacked.empty()) return true;
  for (const auto& rid : ix.reservations) {
    auto r = reservations_.find(rid);
    if (r != reservations_.end() && r->second.st != ResSt::kEnded) return true;
  }
  return false;
}

void Tenant::reap_orphans(int64_t now) {
  if (draining_ || test_no_owner_loss_) return;
  const int64_t limit = cfg_.owner_loss_ms;
  std::unordered_map<std::string, bool> live, gone;  // per replica with a stake here
  // Phase 7 (CR-187): an interaction that holds nothing is never an orphan: walk held_ix_ only.
  for (const auto& hid : held_ix_) {
    auto hit = interactions_.find(hid);
    if (hit == interactions_.end()) continue;
    const Interaction& ix = hit->second;
    if (ix.owner_lost || ix.replica.empty() || ix.owner_epoch == 0 || live.contains(ix.replica) || !holds(ix)) continue;
    const bool l = sink_->ie_live(ix.replica);
    live[ix.replica] = l;
    if (l) {
      replica_gone_since_.erase(ix.replica);
      replica_live_since_.try_emplace(ix.replica, now);
      gone[ix.replica] = false;
    } else {
      replica_live_since_.erase(ix.replica);
      const int64_t since = replica_gone_since_.try_emplace(ix.replica, now).first->second;
      auto rpc = replica_rpc_.find(ix.replica);
      const int64_t last_rpc = rpc == replica_rpc_.end() ? 0 : rpc->second;
      gone[ix.replica] = now - since >= limit && now - last_rpc >= limit;  // (a)
    }
  }
  std::vector<std::string> orphans;
  for (const auto& id : held_ix_) {
    auto hit = interactions_.find(id);
    if (hit == interactions_.end()) continue;
    const Interaction& ix = hit->second;
    auto g = gone.find(ix.replica);
    if (ix.owner_lost || ix.owner_epoch == 0 || g == gone.end() || !holds(ix)) continue;
    bool lost = g->second;
    if (!lost && live[ix.replica] && !ix.unacked.empty()) {  // (b)
      const int64_t from = std::max(ix.unacked.front().first_at, replica_live_since_.at(ix.replica));
      lost = now - from >= limit;
    }
    if (lost) orphans.push_back(id);
  }
  for (const auto& id : orphans) lose_owner(interactions_.at(id), now);
  std::erase_if(replica_gone_since_, [&](const auto& kv) { return !gone.contains(kv.first); });
  std::erase_if(replica_live_since_, [&](const auto& kv) { return !gone.contains(kv.first); });
  if (!orphans.empty()) settle(now);
}

void Tenant::lose_owner(Interaction& ix, int64_t now) {
  {
    LogRec lr("warn", "owner.lost", "the call's IE owner is gone: its entry and reservations are cleaned up (L18)");
    if (lr.on()) lc(lr, ix.id).attr("replica", ix.replica).attr("unacked", static_cast<int64_t>(ix.unacked.size()));
  }
  touch_ix(ix.id);
  ix.owner_lost = true;
  ix.unacked.clear();  // orphan messages: nobody will acknowledge them
  if (auto e = entries_.find(ix.entry_id); e != entries_.end()) {
    Entry& en = e->second;
    if (en.state == rv1::ENTRY_STATE_RESERVED) {
      auto res = reservations_.find(en.reservation_id);
      if (res != reservations_.end() && res->second.st == ResSt::kAssigned) {
        end_reservation(res->second, rv1::OUTCOME_REVOKED, now);
        become_free(agents_.at(res->second.agent), now, /*echo=*/true);
      }
    }
    remove_entry(en, now);  // a live offer is revoked (CALLER_GONE): the agent is READY again
  }
  for (const auto& rid : std::vector<std::string>(ix.reservations.begin(), ix.reservations.end())) {
    auto r = reservations_.find(rid);
    if (r == reservations_.end()) continue;
    if (r->second.st == ResSt::kAssigned) {
      end_reservation(r->second, rv1::OUTCOME_REVOKED, now);
      if (!r->second.callback_id.empty()) callback_ended(r->second.callback_id, rv1::OUTCOME_REVOKED, now);
      become_free(agents_.at(r->second.agent), now, /*echo=*/true);
    } else if (r->second.st == ResSt::kConnected) {
      start_wrap_up(r->second, now);  // the call is gone as far as RE can know
    }
  }
  // A new owner's Enqueue with the same enqueue_seq makes a new entry (CONVENTIONS: "Enqueue again
  // only if RE has no entry").
  ix.max_enqueue_seq = 0;
  ix.snaps.clear();
  ix.seq_queue.clear();
  ++counters_->owner_lost;
}

std::vector<std::string> Tenant::held_interactions() const {
  std::vector<std::string> out;
  for (const auto& [id, ix] : interactions_) {
    if (holds(ix)) out.push_back(id);
  }
  return out;
}

void Tenant::prune(int64_t now) {
  std::erase_if(commands_, [&](const auto& kv) {
    if (now - kv.second.at <= kCommandTtl) return false;
    touch_cmd(kv.first);
    return true;
  });
  std::vector<std::string> gone;
  for (const auto& [id, ix] : interactions_) {
    if (!ix.entry_id.empty() || !ix.unacked.empty() || now - ix.last_activity < kInteractionTtl) continue;
    bool live = false;
    for (const auto& rid : ix.reservations) {
      if (reservations_.at(rid).st != ResSt::kEnded) live = true;
    }
    if (!live) gone.push_back(id);
  }
  for (const auto& id : gone) {
    for (const auto& rid : interactions_.at(id).reservations) {
      reservations_.erase(rid);
      touch_res(rid);
    }
    interactions_.erase(id);
    touch_ix(id);
  }
  for (auto& [id, a] : agents_) {
    if (a.past_offers.size() > 64) a.past_offers.clear();
  }
}

// ======================================================================================
// Seeding and test stimuli
// ======================================================================================

Tenant::SeededSession Tenant::seed_attached(const std::string& agent, const std::string& replica,
                                            const std::string& contact,
                                            std::optional<int64_t> ready_since, int64_t now) {
  AgentSt& a = agents_.at(agent);
  a.generation = std::max(a.generation, epochs_->seq_base) + 1;
  a.next_seq = epochs_->seq_base;
  a.session_id = new_uuid_v7(now);
  a.replica = replica;
  if (!contact.empty()) a.contact = contact;
  a.attached = true;
  a.version = 1;
  if (ready_since) {
    a.state = av1::AGENT_STATE_READY;
    a.version = 2;
    a.since = *ready_since;
    a.idle_since = *ready_since;
  } else {
    a.state = av1::AGENT_STATE_NOT_READY;
    a.reason = "login";
    a.since = now;
  }
  refresh_agent(a);
  return SeededSession{agent, a.session_id, a.generation};
}

void Tenant::seed_state(const std::string& agent, av1::AgentState s, const std::string& reason,
                        const std::string& reservation_id, const std::string& interaction_id,
                        int64_t now) {
  AgentSt& a = agents_.at(agent);
  a.state = s;
  a.since = now;
  a.idle_since = now;
  a.reason = s == av1::AGENT_STATE_NOT_READY ? reason : "";
  // The versions a real history would have reached: NOT_READY 1, READY 2, ON_CALL 5.
  a.version = s == av1::AGENT_STATE_READY ? 2 : s == av1::AGENT_STATE_ON_CALL ? 5 : 1;
  if (s == av1::AGENT_STATE_ON_CALL && !reservation_id.empty()) {
    Interaction ix;
    ix.id = interaction_id;
    ix.owner_epoch = 1;
    ix.next_seq = epochs_->seq_base;
    ix.acked = epochs_->seq_base;
    ix.last_activity = now;
    ix.reservations.insert(reservation_id);
    interactions_.emplace(interaction_id, std::move(ix));
    touch_ix(interaction_id);
    hold_ix(interaction_id);
    touch_res(reservation_id);
    Reservation r;
    r.id = reservation_id;
    r.interaction = interaction_id;
    r.agent = agent;
    r.contact = a.contact;
    r.st = ResSt::kConnected;
    r.outcome = rv1::OUTCOME_CONNECTED;
    r.connected_once = true;
    r.connected_at = now;
    reservations_.emplace(reservation_id, r);
    a.reservations.insert(reservation_id);
  }
  refresh_agent(a);
}

Result<rv1::EnqueueResponse> Tenant::inject_queue_call(const std::string& interaction,
                                                       const std::string& queue, int64_t now,
                                                       uint32_t priority) {
  rv1::EnqueueRequest r;
  auto* o = r.mutable_owner();
  o->set_tenant_id(id_);
  o->set_interaction_id(interaction);
  o->set_owner_epoch(1);
  o->set_replica_id("ie-test-control");
  r.set_enqueue_seq(1);
  r.mutable_target()->set_queue_id(queue);
  r.set_priority(priority);
  r.set_first_queued_at_ms(now);
  r.set_ordering_at_ms(now);
  r.set_channel(rv1::CHANNEL_VOICE);
  r.set_reason(rv1::ENQUEUE_REASON_NEW);
  return enqueue(r, now);
}

Result<bool> Tenant::inject_call_connected(const std::string& reservation_id, int64_t now) {
  auto rit = reservations_.find(reservation_id);
  if (rit == reservations_.end() || rit->second.st != ResSt::kAssigned) {
    return err(Code::kNotFound, "reservation_unknown", "no assigned reservation");
  }
  auto ix = interactions_.find(rit->second.interaction);
  if (ix == interactions_.end()) return err(Code::kNotFound, "reservation_unknown", "no interaction");
  rv1::ReportOutcomeRequest r;
  auto* o = r.mutable_owner();
  o->set_tenant_id(id_);
  o->set_interaction_id(ix->second.id);
  o->set_owner_epoch(ix->second.owner_epoch);
  o->set_replica_id(ix->second.replica);
  o->set_acked_delivery_seq(ix->second.next_seq);
  r.set_reservation_id(reservation_id);
  r.set_outcome(rv1::OUTCOME_CONNECTED);
  auto res = report_outcome(r, now);
  if (!res) return std::unexpected(res.error());
  return true;
}

Result<bool> Tenant::inject_call_ended(const std::string& reservation_id, int64_t now) {
  auto rit = reservations_.find(reservation_id);
  if (rit == reservations_.end() || rit->second.st != ResSt::kConnected) {
    return err(Code::kNotFound, "reservation_unknown", "no connected reservation");
  }
  start_wrap_up(rit->second, now);
  settle(now);
  return true;
}

// ======================================================================================
// Checks
// ======================================================================================

std::vector<std::string> Tenant::check_invariants(int64_t now, uint64_t* checked) const {
  std::vector<std::string> v;
  uint64_t n = 0;
  auto bad = [&](std::string s) { v.push_back(id_ + ": " + std::move(s)); };
  // Phase 7 (CR-187): the periodic scans see every interaction that holds something.
  for (const auto& [id, ix] : interactions_) {
    ++n;
    if ((holds(ix) || !ix.unacked.empty()) && !held_ix_.contains(id)) bad("interaction " + id + " holds something but isn't in held_ix_");
  }
  for (const auto& [oid, o] : offers_) {
    auto a = agents_.find(o.agent);
    auto e = entries_.find(o.entry);
    ++n;
    if (a == agents_.end() || e == entries_.end()) {
      bad("offer " + oid + " names an unknown agent or entry (another tenant's?)");
      continue;
    }
    if (a->second.offer_id != oid) bad("agent " + o.agent + " does not hold its offer");
    if (e->second.offer_id != oid) bad("entry " + o.entry + " does not hold its offer");
    if (a->second.state != av1::AGENT_STATE_OFFERED) bad("offered agent " + o.agent + " not OFFERED");
    if (!a->second.attached) bad("offer to detached agent " + o.agent);
    if (!a->second.campaign.empty()) bad("offer to lent agent " + o.agent);  // phase 8
    // Phase 4: the agent held the queue in its active set (a primary, or a secondary past the
    // delay) unless the entry was past escalate_after (PLAN §4.2).
    if (!serves_now(a->second, e->second)) bad("offer outside the agent's active set (or to one who can't serve) " + o.entry);
  }
  // Availability recomputed from the agents themselves, then compared with the index.
  std::set<std::pair<int64_t, std::string>> avail_agents;
  for (const auto& [aid, a] : agents_) {
    if (!a.offer_id.empty() && !offers_.contains(a.offer_id)) bad("agent " + aid + " holds a dead offer");
    if (a.state == av1::AGENT_STATE_OFFERED && a.offer_id.empty()) bad("agent " + aid + " OFFERED without an offer");
    if (agent_available(a)) avail_agents.insert({a.idle_since, aid});
    // PLAN §4.3: a move never applies during an offer or a call, and applies as soon as there is
    // neither.
    if (engaged(a.state) && a.active_version != a.active_version_busy) bad("agent " + aid + " moved during an offer or a call");
    if (!engaged(a.state) && a.pending_set) bad("agent " + aid + " holds a pending set while free");
    // Phase 8: a lent agent has no inbound queues; a mode switch is pending only while engaged; a
    // pending lend's next set is empty; a lending names a campaign this tenant holds.
    if (!a.campaign.empty() && !a.active.empty()) bad("lent agent " + aid + " has inbound queues");
    if (a.mode_pending && !engaged(a.state) && a.offer_id.empty()) bad("agent " + aid + " holds a pending mode switch while idle");
    if (a.mode_pending && !a.to_campaign.empty() && !effective(a).empty()) bad("agent " + aid + " is being lent but keeps inbound queues");
    if (a.mode_pending && !a.campaign.empty() && !a.to_campaign.empty()) bad("agent " + aid + " lent to two campaigns");
    if (!mode_target(a).empty() && !campaigns_.contains(mode_target(a))) bad("agent " + aid + " lent to an unknown campaign");
    if (!mode_target(a).empty() && a.state == av1::AGENT_STATE_LOGGED_OUT) bad("logged-out agent " + aid + " still lent");
    for (const auto& [q, m] : a.active) {
      if (!eligible(a, q) || excluded(a, q)) bad("agent " + aid + " has " + q + " active without being eligible");
    }
    ++n;
  }
  if (avail_agents != ready_idle_) bad("the longest-idle index differs from the available agents");
  std::map<std::string, int> busy_recount;
  for (const auto& [aid, a] : agents_) {
    std::string bq;
    if (!a.offer_id.empty()) {
      auto o = offers_.find(a.offer_id);
      if (o != offers_.end() && entries_.contains(o->second.entry)) bq = entries_.at(o->second.entry).queue;
    } else {
      for (const auto& rid : a.reservations) {
        auto r = reservations_.find(rid);
        if (r != reservations_.end() && r->second.st != ResSt::kEnded) {
          bq = r->second.queue;
          break;
        }
      }
    }
    if (!bq.empty()) ++busy_recount[bq];
  }
  // CR-056: the kept per-queue count of eligible agents logged in equals a recount.
  for (const auto& [qid, q] : queues_) {
    int cnt = 0;
    for (const auto& [aid, a] : agents_) {
      if (a.attached && a.state != av1::AGENT_STATE_LOGGED_OUT && a.campaign.empty() && eligible_queue(a, q, false)) ++cnt;
    }
    ++n;
    if (cnt != q.logged_in) bad("queue " + qid + ": logged-in count " + std::to_string(q.logged_in) + " != " + std::to_string(cnt));
    // Lever (b): the kept count of agents busy on the queue equals a recount from offers and
    // live reservations.
    const int busy_n = qid.empty() ? 0 : busy_recount[qid];
    if (!qid.empty() && busy_n != q.busy) bad("queue " + qid + ": busy count " + std::to_string(q.busy) + " != " + std::to_string(busy_n));
  }
  std::set<XKey> esc;
  std::map<std::string, std::map<std::string, std::set<EKey>>> avail_entries;  // queue -> sig -> keys
  std::map<std::string, std::set<EKey>> waiting;
  std::map<std::string, std::map<std::string, std::set<EKey>>> delayed_entries;
  for (const auto& [eid, e] : entries_) {
    const EKey k{e.head ? INT64_MIN : -(static_cast<int64_t>(e.priority) + (e.vip ? kVipRank : 0)), e.ordering_at,
                 e.order_seq, e.id};
    if (e.state == rv1::ENTRY_STATE_WAITING) waiting[e.queue].insert(k);
    if (entry_available(e)) {
      avail_entries[e.queue][e.sig].insert(k);
      if (e.delay_passed) delayed_entries[e.queue][e.sig].insert(k);
      if (e.escalated) esc.insert(XKey{e.ordering_at, e.order_seq, e.id});
    }
    ++n;
  }
  if (esc != escalated_) bad("the escalated index differs from the escalated available entries");
  for (const auto& [qid, q] : queues_) {
    auto w = waiting.find(qid);
    if ((w == waiting.end() ? std::set<EKey>{} : w->second) != q.waiting) bad("queue " + qid + ": the position index differs from its waiting entries");
    for (const auto& [sig, keys] : q.avail) {
      const auto& want = avail_entries[qid][sig];
      if (keys != want) bad("queue " + qid + ": the signature index differs from its available entries");
    }
    for (const auto& [sig, keys] : avail_entries[qid]) {
      if (!keys.empty() && !q.avail.contains(sig)) bad("queue " + qid + ": an available entry is missing from its signature index");
    }
    for (const auto& [sig, keys] : q.delayed) {
      if (keys != delayed_entries[qid][sig]) bad("queue " + qid + ": the secondary-delay index differs from its entries past the delay");
    }
    ++n;
  }
  for (const auto& [eid, e] : entries_) {
    if (e.ordering_at != e.original_ordering_at) bad("entry " + eid + " lost its ordering time");
    if (!e.offer_id.empty() && !offers_.contains(e.offer_id)) bad("entry " + eid + " holds a dead offer");
    const QueueSt& q = queues_.at(e.queue);
    // A scheduled callback has no caller on hold; a CALLBACK_OFFER's entry did its max-wait duty (L15).
    if (e.state == rv1::ENTRY_STATE_WAITING && e.callback_id.empty() && !e.callback_offered &&
        now >= e.queue_entered_at + q.cfg.max_wait_ms) {
      bad("entry " + eid + " waited past max_wait_s without its action");
    }
    if (!draining_ && entry_available(e)) {
      // No eligible READY agent idles while a servable entry waits (d = 0 in phase 1), judged
      // against the recomputed availability, not the dispatcher's index.
      bool any_eligible = false, any_unskipped = false;
      for (const auto& [idle, aid] : avail_agents) {
        const AgentSt& a = agents_.at(aid);
        ++n;
        if (!serves_now(a, e) || reserve_holds(a, e)) continue;
        any_eligible = true;
        auto s = e.skips.find(aid);
        if (s != e.skips.end() && s->second > now) continue;
        any_unskipped = true;
        bad("agent " + aid + " idles while entry " + eid + " waits");
      }
      // ... and a skip never holds when no un-skipped eligible agent is READY (a direct offer's
      // reject holds: its one agent, its max wait).
      if (any_eligible && !any_unskipped && e.direct_agent.empty()) bad("entry " + eid + " stranded by rejects");
    }
  }
  for (const auto& [rid, r] : reservations_) {
    if (r.st == ResSt::kEnded) continue;
    auto a = agents_.find(r.agent);
    if (a == agents_.end() || !a->second.reservations.contains(rid)) bad("reservation " + rid + " lost its agent");
  }
  if (checked) *checked += n;
  return v;
}

std::vector<Tenant::EntrySnap> Tenant::entries_snapshot() const {
  std::vector<EntrySnap> out;
  for (const auto& [id, e] : entries_) out.push_back(EntrySnap{e.interaction, e.id, e.queue, e.state});
  return out;
}

bool Tenant::test_fault(const std::string& kind) {
  if (kind == "drop_entry") {
    for (auto& [id, e] : entries_) {
      if (e.state != rv1::ENTRY_STATE_WAITING || !e.offer_id.empty()) continue;
      e.state = rv1::ENTRY_STATE_UNSPECIFIED;
      refresh_entry(e);  // out of every index, no message, no Dequeue: a lost caller
      auto ix = interactions_.find(e.interaction);
      if (ix != interactions_.end() && ix->second.entry_id == id) {
        ix->second.entry_id.clear();
        touch_ix(ix->first);
      }
      entries_.erase(id);
      return true;
    }
    return false;
  }
  if (kind == "no_owner_loss") {
    test_no_owner_loss_ = true;
    return true;
  }
  if (kind == "drop_overflow") {  // CR-100: the max-wait action runs, its Overflow never reaches IE
    test_drop_overflow_ = true;
    return true;
  }
  if (kind == "unindex_agent") {
    for (auto& [id, a] : agents_) {
      if (!a.avail_key) continue;
      ready_idle_.erase(*a.avail_key);  // the agent stays READY and attached: invisible to dispatch
      return true;
    }
    return false;
  }
  return false;
}

size_t Tenant::waiting_entries() const {
  size_t n = 0;
  for (const auto& [id, q] : queues_) n += q.waiting.size();
  return n;
}

std::string Tenant::live_offer_of(const std::string& agent) const {
  auto a = agents_.find(agent);
  return a == agents_.end() ? std::string() : a->second.offer_id;
}

av1::AgentState Tenant::state_of(const std::string& agent) const {
  auto a = agents_.find(agent);
  return a == agents_.end() ? av1::AGENT_STATE_UNSPECIFIED : a->second.state;
}

std::vector<std::string> Tenant::live_reservations() const {
  std::vector<std::string> out;
  for (const auto& [id, r] : reservations_) {
    if (r.st != ResSt::kEnded) out.push_back(id);
  }
  return out;
}

std::vector<std::string> Tenant::entry_ids() const {
  std::vector<std::string> out;
  for (const auto& [id, e] : entries_) out.push_back(id);
  return out;
}

// ======================================================================================
// The model's feed (phase 2)
// ======================================================================================

std::shared_ptr<const ModelSnapshot> Tenant::model_snapshot(int64_t now) {
  auto snap = std::make_shared<ModelSnapshot>();
  snap->tenant = id_;
  snap->at_ms = now;
  snap->window_ms = last_snapshot_ > 0 ? now - last_snapshot_ : 0;
  last_snapshot_ = now;
  std::map<std::string, int> qi;
  for (auto& [id, q] : queues_) {
    if (id.empty()) continue;  // the direct queue isn't modelled
    qi[id] = static_cast<int>(snap->queues.size());
    ModelSnapshot::Queue s;
    s.id = id;
    s.arrivals = q.arrivals;
    s.abandons = q.abandons;
    s.waited_s = q.waited_ms / 1000.0;
    s.aht_s = q.aht_ms / 1000.0;
    s.sla_ms = q.cfg.sla_ms;
    s.max_wait_ms = q.cfg.max_wait_ms;
    s.waiting = static_cast<uint32_t>(q.waiting.size());
    s.logged_in = q.logged_in;
    // CR-098: numeric priorities differ among the window's arrivals or the entries now waiting. The
    // VIP class is modelled (two classes, phase 5 redesign) and isn't a mix by itself.
    {
      int64_t lo = INT64_MAX, hi = INT64_MIN;
      for (const auto& k : q.waiting) {
        if (std::get<0>(k) == INT64_MIN) continue;  // head placement
        const int64_t p = (-std::get<0>(k)) % kVipRank;
        lo = std::min(lo, p);
        hi = std::max(hi, p);
        if (std::get<0>(k) <= -kVipRank) ++s.vip_waiting;
      }
      s.priority_mix = q.prio_hi > q.prio_lo || (lo != INT64_MAX && hi > lo);
    }
    q.prio_lo = UINT32_MAX;
    q.prio_hi = 0;
    // Phase 5.
    s.wrap_s = q.wrap_ms / 1000.0;
    int64_t oldest = 0;
    for (const auto& k : q.waiting) {
      const int64_t at = std::get<1>(k);
      if (oldest == 0 || at < oldest) oldest = at;
    }
    s.head_age_s = oldest > 0 ? std::max<int64_t>(0, now - oldest) / 1000.0 : 0;
    s.min_agents = q.min_agents;
    s.max_agents = q.max_agents;
    s.alpha = q.pol.alpha;
    s.beta = q.pol.beta;
    s.vip_arrivals = q.vip_arrivals;
    s.busy_agents = q.busy;
    s.weight = q.weight;
    s.weight_default = q.pol.weight;
    s.weight_min = q.pol.weight_min;
    s.weight_max = q.pol.weight_max;
    s.secondary_delay_ms = q.secondary_delay_ms;
    s.delay_default_ms = q.pol.secondary_delay_ms;
    s.delay_min_ms = q.pol.delay_min_ms;
    s.delay_max_ms = q.pol.delay_max_ms;
    s.sl_target = q.pol.sl_target;
    s.vip_sla_ms = q.pol.vip_sla_ms;
    s.vip_alpha = q.pol.vip_alpha;
    s.vip_beta = q.pol.vip_beta;
    s.vip_reserved_min = q.pol.vip_reserved_min;
    s.bucket_offered = q.bucket.offered;
    s.bucket_answered = q.bucket.answered;
    s.bucket_in_sla = q.bucket.answered_in_sla;
    s.bucket_abandoned = q.bucket.abandoned;
    s.bucket_start_ms = bucket_start_;
    s.handled = q.win_handled;
    s.handle_s = q.win_handle_ms / 1000.0;
    snap->queues.push_back(std::move(s));
    q.arrivals = q.abandons = q.vip_arrivals = q.win_handled = 0;
    q.waited_ms = q.win_handle_ms = 0;
  }
  // Phase 5 redesign: every agent individually (no groups): what it takes now, its configured speed
  // per queue, its VIP eligibility; no performance number of any kind.
  snap->mode = static_cast<uint8_t>(policy_.mode);
  snap->plan_version = opt_.version;
  snap->plan_stage = opt_.stage;
  auto speed_of = [&](const AgentSt& a, const QueueSt& q) {
    if (policy_.proficiency_speed.empty()) return 1.0;
    uint32_t level = 0;
    for (const auto& r : q.cfg.required) {
      auto sk = a.cfg.skills.find(r.skill_id);
      if (sk != a.cfg.skills.end()) level = std::max(level, sk->second);
    }
    double sp = 1;
    for (const auto& [min_level, speed] : policy_.proficiency_speed) {
      if (level >= min_level) sp = speed;
    }
    return sp;
  };
  for (const auto& [id, a] : agents_) {
    if (!a.counted || !a.attached) continue;
    ModelSnapshot::Agent x;
    x.id = id;
    x.counts = a.state != av1::AGENT_STATE_NOT_READY && a.state != av1::AGENT_STATE_LOGGED_OUT;
    switch (a.state) {
      case av1::AGENT_STATE_READY: x.activity = a.offer_id.empty() ? 0 : 1; break;
      case av1::AGENT_STATE_OFFERED:
      case av1::AGENT_STATE_RESERVED: x.activity = 1; break;
      case av1::AGENT_STATE_ON_CALL: x.activity = 2; break;
      case av1::AGENT_STATE_WRAP_UP: x.activity = 3; break;
      default: x.activity = 4; break;
    }
    for (const auto& qid : a.member) {
      auto q = queues_.find(qid);
      if (q == queues_.end() || !eligible_queue(a, q->second, false) || excluded(a, qid)) continue;
      x.eligible.push_back(qi.at(qid));
    }
    for (const auto& [qid, m] : effective(a)) {
      auto it = qi.find(qid);
      auto q = queues_.find(qid);
      if (it == qi.end() || q == queues_.end() || !eligible_queue(a, q->second, false) || excluded(a, qid)) continue;
      x.set.emplace_back(it->second, static_cast<uint8_t>(m.tier));
      x.speed.push_back(speed_of(a, q->second));
      snap->tiers = snap->tiers || m.tier == Tier::kSecondary;
    }
    x.vip = vip_agent(a);
    x.pinned = pinned(a, now);
    snap->agents.push_back(std::move(x));
  }
  // Requirement signatures present among waiting entries (CR-033).
  for (const auto& [id, q] : queues_) {
    if (id.empty()) continue;
    for (const auto& [sig, keys] : q.avail) {
      if (sig.empty() || keys.empty()) continue;
      const Entry& e = entries_.at(std::get<3>(*keys.begin()));
      const int n = eligible_logged_in(q, e);
      snap->sigs.push_back(ModelSnapshot::Sig{qi.at(id), sig, q.logged_in > 0 ? static_cast<double>(n) / q.logged_in : 0});
    }
  }
  return snap;
}

bool Tenant::install_ewt(std::shared_ptr<const EwtPlan> plan, int64_t now) {
  // Tenant isolation: a plan names its tenant; one computed for another is never used here.
  if (!plan || plan->tenant != id_ || (ewt_plan_ && plan->version <= ewt_plan_->version) ||
      plan->valid_until_ms < now) {
    ++counters_->plans_refused;
    return false;
  }
  ewt_plan_ = std::move(plan);
  ++counters_->plans_installed;
  return true;
}


// ======================================================================================
// Phase 4: agents moving between queues (PLAN §4)
// ======================================================================================

bool Tenant::eligible(const AgentSt& a, const std::string& queue) const {
  if (queue.empty() || !a.member.contains(queue)) return false;
  auto q = queues_.find(queue);
  return q != queues_.end() && eligible_queue(a, q->second, false);
}

bool Tenant::excluded(const AgentSt& a, const std::string& queue) const {
  auto p = pins_.find(a.cfg.id);
  if (p == pins_.end()) return false;
  return std::find(p->second.exclusions.begin(), p->second.exclusions.end(), queue) != p->second.exclusions.end();
}

Tier Tenant::tier_of(const AgentSt& a, const std::string& queue) const {
  auto it = a.active.find(queue);
  return it == a.active.end() ? Tier::kNone : it->second.tier;
}

bool Tenant::pinned(const AgentSt& a, int64_t now) const {
  auto p = pins_.find(a.cfg.id);
  return p != pins_.end() && !p->second.queues.empty() && (p->second.until_ms == 0 || now < p->second.until_ms);
}

// The set the agent should have now: a pin's queues; else (auto mode) the optimizer's; else every
// queue it is a member of and eligible for, all primary (PLAN §4.1). Exclusions apply to all three.
Tenant::Set Tenant::desired(const AgentSt& a, int64_t now) const {
  Set out;
  if (!mode_target(a).empty()) return out;  // phase 8: lent (or being lent): no inbound queues
  auto put = [&](const std::string& q, Tier t, bool pin) {
    if (!eligible(a, q) || excluded(a, q)) return;
    AgentSt::Member m{t, pin, now};
    auto c = a.active.find(q);
    if (c != a.active.end() && c->second.tier == t) m.since = c->second.since;
    out[q] = m;
  };
  if (pinned(a, now)) {
    for (const auto& [q, t] : pins_.at(a.cfg.id).queues) put(q, t, true);
  } else if (policy_.mode == RoutingMode::kAuto && a.plan) {
    for (const auto& [q, t] : *a.plan) put(q, t, false);
  } else {
    for (const auto& q : a.member) put(q, Tier::kPrimary, false);
  }
  return out;
}

// PLAN §4.3 coverage: every queue keeps at least one logged-in eligible agent with it in some
// active set (pending sets count as what they will be).
bool Tenant::uncovers(const AgentSt& a, const Set& from, const Set& to) const {
  if (!a.counted) return false;
  for (const auto& [qid, m] : from) {
    if (to.contains(qid) || !eligible(a, qid)) continue;
    bool other = false;
    for (const auto& [bid, b] : agents_) {
      if (bid == a.cfg.id || !b.counted) continue;
      if (effective(b).contains(qid) && eligible(b, qid)) {
        other = true;
        break;
      }
    }
    if (!other) return true;
  }
  return false;
}

void Tenant::send_queues(AgentSt& a, av1::MembershipReason why, int64_t now) {
  av1::WatchResponse m;
  auto* q = m.mutable_queues_changed();
  const av1::AgentView v = view(a);
  *q->mutable_queues() = v.queues();
  *q->mutable_pending_queues() = v.pending_queues();
  q->set_reason(why);
  q->set_queues_version(a.queues_version);
  send_gw(a, std::move(m), now);
}

bool Tenant::retarget(AgentSt& a, av1::MembershipReason why, int64_t now, bool check_coverage) {
  touch_agent(a.cfg.id);
  Set want = desired(a, now);
  const Set& eff = effective(a);
  if (same_tiers(want, eff)) {
    a.retarget_due = false;
    // CR-175: the set is unchanged, but what lifted (an exclusion, a pin) may let a READY agent take
    // a waiting call now: dispatch it, never leave it to the 1 s sweep.
    if (agent_available(a)) todo_agents_.push_back(a.cfg.id);
    return true;
  }
  if (check_coverage && uncovers(a, eff, want)) {
    ++counters_->coverage_refused;
    a.retarget_due = true;  // the 1 s sweep tries again (another agent may log in)
    return false;
  }
  a.retarget_due = false;
  const Set before = a.active;
  if (engaged(a.state) || !a.offer_id.empty()) {
    // Next offer, never mid-call: shown as pending until the offer resolves or the call ends.
    if (same_tiers(want, a.active)) {
      a.pending_set.reset();
    } else {
      a.pending_set = std::move(want);
      a.pending_set_reason = why;
    }
    ++a.queues_version;
    ++counters_->moves_pending;
    send_queues(a, why, now);
  } else {
    a.active = std::move(want);
    a.pending_set.reset();
    ++a.active_version;
    ++a.queues_version;
    ++counters_->moves_applied;
    send_queues(a, why, now);
    if (agent_available(a)) todo_agents_.push_back(a.cfg.id);
  }
  event_queues(a, before, why, now);
  return true;
}

void Tenant::apply_pending(AgentSt& a, int64_t now) {
  if (!a.pending_set) return;
  touch_agent(a.cfg.id);
  const Set before = a.active;
  const av1::MembershipReason why = a.pending_set_reason;
  a.active = std::move(*a.pending_set);
  a.pending_set.reset();
  for (auto& [q, m] : a.active) {
    auto b = before.find(q);
    if (b == before.end() || b->second.tier != m.tier) m.since = now;
  }
  ++a.active_version;
  ++a.queues_version;
  ++counters_->moves_applied;
  send_queues(a, why, now);
  event_queues(a, before, why, now);
}

// One optimizer or config move, re-checked against the state held now (CR-021). *outcome: 1
// applied at once, 2 pending (next offer), 3 silent (weights only).
bool Tenant::try_move(AgentSt& a, const std::map<std::string, Tier>& to, const std::map<std::string, double>* w,
                      av1::MembershipReason why, int64_t now, std::string* why_not, int* outcome) {
  auto refuse = [&](const char* r, std::atomic<uint64_t>& counter) {
    *why_not = r;
    ++counter;
    return false;
  };
  if (policy_.mode != RoutingMode::kAuto) return refuse("mode", counters_->moves_refused);
  if (pinned(a, now)) return refuse("pinned", counters_->moves_refused);
  for (const auto& [q, t] : to) {
    if (q.empty() || !queues_.contains(q)) return refuse("unknown queue", counters_->foreign_refused);
    if (t != Tier::kPrimary && t != Tier::kSecondary) return refuse("tier", counters_->moves_refused);
    if (excluded(a, q)) return refuse("excluded", counters_->moves_refused);
    if (!eligible(a, q)) return refuse("not eligible", counters_->moves_stale);
  }
  if (w) {
    for (const auto& [q, x] : *w) {
      if (!to.contains(q) || !(x >= 0.25 && x <= 4.0)) return refuse("weight", counters_->moves_refused);
    }
  }
  if (!a.counted) return refuse("logged out", counters_->moves_stale);
  touch_agent(a.cfg.id);
  Set want;
  for (const auto& [q, t] : to) want[q] = AgentSt::Member{t, false, now};
  const Set& eff = effective(a);
  if (same_tiers(want, eff)) {
    if (w) a.weights = *w;  // CR-018: weight-only changes say nothing to the desktop
    *outcome = 3;
    return true;
  }
  while (!a.move_times.empty() && now - a.move_times.front() >= kHourMs) a.move_times.pop_front();
  const bool primary_change = primaries_of(want) != primaries_of(eff);
  if (primary_change) {
    if (a.primary_since > 0 && now - a.primary_since < policy_.dwell_min_ms) return refuse("dwell", counters_->moves_refused);
    if (static_cast<int>(a.move_times.size()) >= policy_.max_moves_per_hour) return refuse("hourly cap", counters_->moves_refused);
  } else if (secondaries_of(want) != secondaries_of(eff) && a.secondary_since > 0 &&
             now - a.secondary_since < policy_.dwell_min_ms / 2) {
    return refuse("change rate", counters_->moves_refused);  // CR-018
  }
  if (uncovers(a, eff, want)) return refuse("coverage", counters_->coverage_refused);
  a.plan = to;
  if (w) a.weights = *w;
  if (primary_change) {
    a.primary_since = now;
    a.move_times.push_back(now);
  }
  a.secondary_since = now;
  retarget(a, why, now, /*check_coverage=*/false);
  *outcome = a.pending_set ? 2 : 1;
  return true;
}

Tenant::MoveResult Tenant::apply_moves(const std::vector<Move>& moves, int64_t now) {
  MoveResult res;
  for (const auto& mv : moves) {
    auto it = agents_.find(mv.agent_id);
    if (it == agents_.end()) {
      // Tenant isolation: a plan naming an agent this tenant doesn't have (another tenant's)
      // changes nothing and is counted.
      ++counters_->foreign_refused;
      ++res.refused;
      res.why.push_back(mv.agent_id + ": not an agent of " + id_);
      continue;
    }
    std::map<std::string, Tier> to;
    bool dup = false;
    for (const auto& [q, t] : mv.queues) dup |= !to.emplace(q, t).second;
    std::string why;
    int outcome = 0;
    if (dup) {
      why = "a queue twice";
      ++counters_->moves_refused;
    } else if (try_move(it->second, to, mv.weights.empty() ? nullptr : &mv.weights, av1::MEMBERSHIP_REASON_OPTIMIZER, now, &why,
                        &outcome)) {
      if (outcome == 1) ++res.applied;
      if (outcome == 2) ++res.pending;
      if (outcome == 3) ++res.silent;
      continue;
    }
    ++res.refused;
    res.why.push_back(mv.agent_id + ": " + why);
  }
  settle(now);
  return res;
}

// Auto mode without an optimizer plan (phase 4): config.routing's min/max agents per queue move
// agents by the fairness order (fewest moves in the last hour, longest on the current primary,
// higher proficiency on the queue, agent_id; never performance scores), within dwell, the hourly cap
// and coverage. Phase 5's planner takes over the targets.
void Tenant::apply_policy_to_queues() {
  for (auto& [id, q] : queues_) {
    if (id.empty()) continue;
    q.pol = QueuePolicy{};
    q.pol.queue_id = id;
    q.escalate_after_ms = q.cfg.escalate_after_ms;
  }
  for (const auto& qp : policy_.queues) {
    auto it = queues_.find(qp.queue_id);
    if (it == queues_.end() || qp.queue_id.empty()) continue;
    it->second.pol = qp;
    if (qp.escalate_after_ms > 0) it->second.escalate_after_ms = qp.escalate_after_ms;
  }
  for (auto& [id, q] : queues_) {
    if (id.empty()) continue;
    q.min_agents = q.pol.min_agents;
    q.max_agents = q.pol.max_agents;
  }
  revert_levers();
  // A fresh lever plan stays in force under the new bounds (auto only; PLAN §6's stale rule aside).
  if (levers_on() && opt_.version > 0 && opt_.stage == 0) {
    for (const auto& [qid, wd] : opt_.applied) {
      auto it = queues_.find(qid);
      if (it == queues_.end()) continue;
      QueueSt& q = it->second;
      q.weight = std::clamp(wd.first, q.pol.weight_min, q.pol.weight_max);
      q.secondary_delay_ms = std::clamp(wd.second, q.pol.delay_min_ms, q.pol.delay_max_ms);
    }
  }
}

void Tenant::revert_levers() {
  for (auto& [id, q] : queues_) {
    if (id.empty()) continue;
    q.weight = q.pol.weight;
    q.secondary_delay_ms = q.pol.secondary_delay_ms;
  }
}

void Tenant::rearm_delays(int64_t now) {
  // Entries not yet past their queue's delay get a timer for the delay in force (an older timer that
  // fires early is ignored); one already past it is offerable to secondaries at once.
  for (auto& [eid, e] : entries_) {
    if (e.delay_passed || e.state != rv1::ENTRY_STATE_WAITING) continue;
    const QueueSt& q = queues_.at(e.queue);
    if (now >= e.queue_entered_at + q.secondary_delay_ms) {
      e.delay_passed = true;
      refresh_entry(e);
      todo_entries_.push_back(eid);
    } else {
      timers_->at(e.queue_entered_at + q.secondary_delay_ms, TimerRef{id_, TimerKind::kSecondaryDelay, eid});
    }
  }
}

int Tenant::bounds_rank(const QueueSt& q) const {
  if (!levers_on()) return 1;
  if (q.min_agents > 0 && q.busy < q.min_agents) return 2;
  if (q.max_agents > 0 && q.busy >= q.max_agents) return 0;
  return 1;
}

bool Tenant::vip_agent(const AgentSt& a) const {
  return !policy_.vip_skill.empty() && a.cfg.skills.contains(policy_.vip_skill);
}

int Tenant::ready_vip(const QueueSt& q) const {
  int n = 0;
  for (const auto& [idle, id] : ready_idle_) {
    const AgentSt& a = agents_.at(id);
    if (vip_agent(a) && eligible_queue(a, q, false) && !excluded(a, q.cfg.id)) ++n;
  }
  return n;
}

bool Tenant::reserve_holds(const AgentSt& a, const Entry& e) const {
  if (e.vip || e.head || e.escalated || e.delay_passed || !e.direct_agent.empty() || !vip_agent(a)) return false;
  auto q = queues_.find(e.queue);
  if (q == queues_.end() || q->second.pol.vip_reserved_min <= 0) return false;
  return ready_vip(q->second) <= q->second.pol.vip_reserved_min;
}

bool Tenant::install_policy(const RoutingPolicy& p, int64_t now, std::string* why) {
  auto refuse = [&](const std::string& r, std::atomic<uint64_t>& c) {
    ++c;
    if (why) *why = r;
    return false;
  };
  if (p.version <= policy_.version) return refuse("not newer than the version held", counters_->config_refused);
  if (p.dwell_min_ms < 60'000 || p.dwell_min_ms > 3'600'000) return refuse("dwell_min_s 60-3600", counters_->config_refused);
  if (p.max_moves_per_hour < 0 || p.max_moves_per_hour > 12) return refuse("max_moves_per_hour 0-12", counters_->config_refused);
  if (p.reject_skip_ms > 300'000) return refuse("reject_skip_s 0-300", counters_->config_refused);
  double last_level = -1;
  for (const auto& [level, speed] : p.proficiency_speed) {
    if (static_cast<double>(level) <= last_level || !(speed >= 0.25 && speed <= 4)) {
      return refuse("proficiency_speed: rising levels, speed 0.25-4", counters_->config_refused);
    }
    last_level = level;
  }
  std::set<std::string> seen;
  for (const auto& qp : p.queues) {
    // Tenant isolation: a policy naming a queue this tenant doesn't have is refused whole.
    if (qp.queue_id.empty() || !queues_.contains(qp.queue_id)) return refuse("unknown queue " + qp.queue_id, counters_->foreign_refused);
    if (!seen.insert(qp.queue_id).second) return refuse("queue twice", counters_->config_refused);
    if (qp.secondary_delay_ms < 0 || qp.secondary_delay_ms > 300'000) return refuse("secondary_delay_s 0-300", counters_->config_refused);
    if (qp.min_agents < 0 || qp.max_agents < 0 || (qp.max_agents > 0 && qp.min_agents > qp.max_agents)) {
      return refuse("min_agents/max_agents", counters_->config_refused);
    }
    if (!(qp.alpha > 0) || !(qp.beta >= 0) || !(qp.vip_alpha > 0) || !(qp.vip_beta >= 0)) {
      return refuse("alpha/beta", counters_->config_refused);
    }
    if (!(qp.weight_min >= 0.25) || !(qp.weight_max <= 4) || !(qp.weight_min <= qp.weight) || !(qp.weight <= qp.weight_max)) {
      return refuse("weight_min <= weight <= weight_max within 0.25-4", counters_->config_refused);
    }
    if (qp.delay_min_ms < 0 || qp.delay_max_ms > 300'000 || qp.delay_min_ms > qp.secondary_delay_ms ||
        qp.secondary_delay_ms > qp.delay_max_ms) {
      return refuse("delay_min_s <= secondary_delay_s <= delay_max_s within 0-300", counters_->config_refused);
    }
    if (!(qp.sl_target > 0 && qp.sl_target <= 1)) return refuse("sl_target 0-1", counters_->config_refused);
    if (qp.vip_sla_ms < 0 || qp.vip_reserved_min < 0) return refuse("vip_sla_s/vip_reserved_min", counters_->config_refused);
  }
  const RoutingMode old = policy_.mode;
  policy_ = p;
  touch_plan();
  apply_policy_to_queues();
  if (old != p.mode && p.mode != RoutingMode::kAuto) {
    for (auto& [id, a] : agents_) {
      if (a.plan) touch_agent(id);
      a.plan.reset();
    }
  }
  const av1::MembershipReason why_change = old != p.mode ? av1::MEMBERSHIP_REASON_MODE_CHANGE : av1::MEMBERSHIP_REASON_CONFIG;
  for (auto& [id, a] : agents_) retarget(a, why_change, now, /*check_coverage=*/false);
  rearm_delays(now);
  // Lever (b) and the VIP reserve may let a READY agent take something now.
  for (const auto& [idle, id] : ready_idle_) todo_agents_.push_back(id);
  settle(now);
  return true;
}

bool Tenant::install_pin(const RoutingPin& p, bool tombstone, int64_t now, std::string* why) {
  auto refuse = [&](const std::string& r, std::atomic<uint64_t>& c) {
    ++c;
    if (why) *why = r;
    return false;
  };
  auto ait = agents_.find(p.agent_id);
  // Tenant isolation: a pin naming an agent or queue this tenant doesn't have changes nothing.
  if (ait == agents_.end()) return refuse("agent " + p.agent_id + " is not an agent of " + id_, counters_->foreign_refused);
  for (const auto& [q, t] : p.queues) {
    if (q.empty() || !queues_.contains(q)) return refuse("unknown queue " + q, counters_->foreign_refused);
    if (t != Tier::kPrimary && t != Tier::kSecondary) return refuse("tier", counters_->config_refused);
  }
  for (const auto& q : p.exclusions) {
    if (q.empty() || !queues_.contains(q)) return refuse("unknown queue " + q, counters_->foreign_refused);
  }
  auto held = pin_versions_.find(p.agent_id);
  // A Kafka tombstone carries no version (0): it always unpins; a document must be newer.
  if (held != pin_versions_.end() && !(tombstone && p.version == 0) && p.version <= held->second) {
    return refuse("not newer than the version held", counters_->config_refused);
  }
  AgentSt& a = ait->second;
  const bool was = pinned(a, now);
  touch_pin(p.agent_id);
  if (p.version > 0) pin_versions_[p.agent_id] = p.version;
  if (tombstone) {
    pins_.erase(p.agent_id);
  } else {
    pins_[p.agent_id] = p;
    if (p.until_ms > 0) timers_->at(p.until_ms, TimerRef{id_, TimerKind::kPinExpiry, p.agent_id});
  }
  const bool is = pinned(a, now);
  if (was && !is) a.primary_since = now;  // unpinned: dwell restarts
  const av1::MembershipReason r = is ? av1::MEMBERSHIP_REASON_SUPERVISOR_PIN
                                  : was ? av1::MEMBERSHIP_REASON_SUPERVISOR_UNPIN
                                        : av1::MEMBERSHIP_REASON_CONFIG;
  // Pins ignore dwell (a person decided) but never uncover a queue: one that would waits.
  retarget(a, r, now, /*check_coverage=*/true);
  settle(now);
  return true;
}

std::vector<std::pair<std::string, Tier>> Tenant::active_of(const std::string& agent) const {
  std::vector<std::pair<std::string, Tier>> out;
  auto a = agents_.find(agent);
  if (a == agents_.end()) return out;
  for (const auto& [q, m] : a->second.active) out.emplace_back(q, m.tier);
  return out;
}

std::vector<std::pair<std::string, Tier>> Tenant::pending_of(const std::string& agent) const {
  std::vector<std::pair<std::string, Tier>> out;
  auto a = agents_.find(agent);
  if (a == agents_.end() || !a->second.pending_set) return out;
  for (const auto& [q, m] : *a->second.pending_set) out.emplace_back(q, m.tier);
  return out;
}

// ======================================================================================
// Phase 5: the optimization loop's writer side (PLAN §5.3, §4.4, §6)
// ======================================================================================

std::map<std::string, double> Tenant::weights_of(const std::string& agent) const {
  auto a = agents_.find(agent);
  return a == agents_.end() ? std::map<std::string, double>{} : a->second.weights;
}

double Tenant::queue_weight(const std::string& queue) const {
  auto q = queues_.find(queue);
  return q == queues_.end() ? 0 : q->second.weight;
}

int64_t Tenant::queue_delay(const std::string& queue) const {
  auto q = queues_.find(queue);
  return q == queues_.end() ? -1 : q->second.secondary_delay_ms;
}

int Tenant::busy_on(const std::string& queue) const {
  auto q = queues_.find(queue);
  return q == queues_.end() ? -1 : q->second.busy;
}

bool Tenant::install_levers(std::shared_ptr<const LeverPlan> p, int64_t now, LeverResult* out) {
  LeverResult r;
  // Tenant isolation: a plan names its tenant; one computed for another is never used here.
  if (!p || p->tenant != id_) {
    ++counters_->levers_refused;
    ++counters_->foreign_refused;
    return false;
  }
  if (p->version <= opt_.version || p->valid_until_ms < now) {
    ++counters_->levers_refused;
    return false;
  }
  for (const auto& pq : p->queues) {
    if (!pq.id.empty() && queues_.contains(pq.id)) continue;
    ++counters_->levers_refused;  // a queue this tenant doesn't have: refused whole
    ++counters_->foreign_refused;
    return false;
  }
  touch_opt();
  ++counters_->levers_installed;
  const bool was_stale = opt_.stage != 0;
  opt_.version = p->version;
  opt_.computed_at = p->computed_at_ms;
  opt_.valid_until = p->valid_until_ms;
  opt_.quality = p->quality;
  opt_.stage = 0;
  if (!levers_on()) {
    r.why = "mode";
    r.held = static_cast<int>(p->queues.size());
    counters_->levers_held += p->queues.size();
    opt_.applied.clear();
    if (out) *out = r;
    return true;
  }
  bool delays = false;
  if (was_stale) {
    // Back from config defaults: what the earlier plan applied is forgotten; this plan decides.
    opt_.applied.clear();
  }
  for (const auto& pq : p->queues) {
    QueueSt& q = queues_.at(pq.id);
    if (pq.quality != 0 || !std::isfinite(pq.weight)) {
      ++r.held;  // PLAN §4.3: approx never changes weights or delays by itself
      ++counters_->levers_held;
      continue;
    }
    const double w = std::clamp(pq.weight, q.pol.weight_min, q.pol.weight_max);
    const int64_t d = std::clamp(pq.delay_ms, q.pol.delay_min_ms, q.pol.delay_max_ms);
    if (w != pq.weight || d != pq.delay_ms) ++r.clamped;
    opt_.applied[pq.id] = {w, d};
    if (w == q.weight && d == q.secondary_delay_ms) continue;
    q.weight = w;  // silent: weights never reach the desktop (CR-018)
    if (d != q.secondary_delay_ms) {
      q.secondary_delay_ms = d;
      delays = true;
    }
    ++r.applied;
    ++counters_->levers_applied;
  }
  if (r.applied == 0 && r.held > 0) r.why = "approx";
  if (delays) rearm_delays(now);
  settle(now);
  if (out) *out = r;
  return true;
}

// PLAN §6 (phase 5 redesign): until valid_until the last plan's levers; then config's weights and
// delays (nothing narrows: no lever ever narrowed a set); a fresh plan puts the levers back. Only in
// auto mode with an optimizer feeding the tenant: elsewhere the levers are config's anyway.
void Tenant::stall_check(int64_t now) {
  if (!optimizer_on_ || !levers_on() || opt_.version == 0 || opt_.stage != 0 || now <= opt_.valid_until) return;
  touch_opt();
  ++counters_->stall_stale;
  opt_.stage = 1;
  opt_.applied.clear();
  revert_levers();
  rearm_delays(now);
  settle(now);
}

// A supervisor's forced state (agents.v1 0.11.0, CR-072). RESERVED: L8 (the reservation revoked,
// the entry back at its ordering time); OFFERED: the offer revoked (STATE_CHANGED); on a call or in
// wrap-up: pending, calls are never touched. A forced logout also ends the attachment.
Result<av1::ForceStateResponse> Tenant::force_state(const av1::ForceStateRequest& r, int64_t now) {
  if (!valid_agent_id(r.agent_id())) return err(Code::kInvalidArgument, "invalid_request", "agent_id");
  if (!valid_command_id(r.command_id())) return err(Code::kInvalidArgument, "invalid_request", "command_id");
  const std::string fp = "force|" + r.agent_id() + "|" + std::to_string(r.target()) + "|" + r.reason_code();
  if (auto rep = repeat<av1::ForceStateResponse>(r.command_id(), fp)) return *rep;
  Result<av1::ForceStateResponse> res = [&]() -> Result<av1::ForceStateResponse> {
    if ((r.target() != av1::TARGET_STATE_NOT_READY && r.target() != av1::TARGET_STATE_LOGGED_OUT) ||
        (r.target() == av1::TARGET_STATE_NOT_READY && r.reason_code().empty())) {
      return err(Code::kInvalidArgument, "invalid_request", "target or reason_code");
    }
    auto it = agents_.find(r.agent_id());
    if (it == agents_.end()) return err(Code::kNotFound, "agent_unknown", "no such agent");
    AgentSt& a = it->second;
    const bool logout = r.target() == av1::TARGET_STATE_LOGGED_OUT;
    if (a.state == av1::AGENT_STATE_RESERVED) {
      for (const auto& rid : std::vector<std::string>(a.reservations.begin(), a.reservations.end())) {
        auto rit = reservations_.find(rid);
        if (rit == reservations_.end() || rit->second.st != ResSt::kAssigned) continue;
        Reservation& res = rit->second;
        end_reservation(res, rv1::OUTCOME_REVOKED, now);  // L8
        auto ix = interactions_.find(res.interaction);
        if (ix != interactions_.end()) {
          rv1::WatchResponse m;
          auto* rv = m.mutable_revoke();
          *rv->mutable_target() = target_of(ix->second);
          rv->set_reservation_id(res.id);
          rv->set_reason(logout ? "agent_logged_out" : "forced_state");
          send_ie(ix->second, std::move(m), now);
        }
        if (!res.callback_id.empty()) {
          callback_ended(res.callback_id, rv1::OUTCOME_REVOKED, now);
        } else if (auto e = entries_.find(res.entry_id); e != entries_.end()) {
          entry_back(e->second, now);
        }
      }
    }
    if (a.reservations.empty() && a.state == av1::AGENT_STATE_RESERVED) {
      a.pending = av1::TARGET_STATE_UNSPECIFIED;  // the supervisor's state wins over a pending one
      a.pending_reason.clear();
      if (logout) {
        set_agent_state(a, av1::AGENT_STATE_LOGGED_OUT, "", now, /*echo=*/true);
      } else {
        set_agent_state(a, av1::AGENT_STATE_NOT_READY, r.reason_code(), now, /*echo=*/true);
      }
    } else {
      ask(a, r.target(), r.reason_code(), now, /*echo=*/true);
    }
    if (logout && a.state == av1::AGENT_STATE_LOGGED_OUT && a.attached) {
      av1::WatchResponse m;
      m.mutable_detached()->set_reason(av1::DETACHED_REASON_FORCED_LOGOUT);
      send_gw(a, std::move(m), now);
      a.attached = false;
      a.detached_at = 0;
      refresh_agent(a);  // (marks the agent)
    }
    ++counters_->forced_states;
    if (events_) {
      events_->publish(AgentEvent{"forced_state", id_, a.cfg.id, now,
                                  "{\"supervisor_id\":" + json_str(r.supervisor_id()) + ",\"target\":\"" +
                                      (logout ? "logged_out" : "not_ready") + "\",\"reason_code\":" + json_str(r.reason_code()) + "}", ""});
    }
    settle(now);
    av1::ForceStateResponse out;
    *out.mutable_agent() = view(a);
    return out;
  }();
  return remember(r.command_id(), fp, std::move(res), now);
}

// ---- agent.events (phase 3, PLAN §5.4): the writer builds the body; the publisher frames it ----

void Tenant::event_state(const AgentSt& a, int64_t now) {
  if (!events_) return;
  static const char* names[] = {"unspecified", "logged_out", "not_ready", "ready", "offered", "reserved", "on_call", "wrap_up"};
  const int s = static_cast<int>(a.state);
  std::string body = "{\"state\":\"" + std::string(s >= 0 && s < 8 ? names[s] : "unspecified") + "\",\"state_version\":" +
                     std::to_string(a.version);
  if (a.state == av1::AGENT_STATE_NOT_READY) body += ",\"reason_code\":" + json_str(a.reason);
  body += "}";
  events_->publish(AgentEvent{"state_changed", id_, a.cfg.id, now, std::move(body), ""});
}

void Tenant::event_queues(const AgentSt& a, const Set& before, av1::MembershipReason why, int64_t now) {
  if (!events_) return;
  auto list = [](const Set& s) {
    std::string out = "[";
    for (const auto& [q, m] : s) {
      if (out.size() > 1) out += ",";
      out += "{\"queue_id\":" + json_str(q) + ",\"tier\":\"" + (m.tier == Tier::kPrimary ? "primary" : "secondary") + "\"" +
             (m.pinned ? ",\"pinned\":true" : "") + "}";
    }
    return out + "]";
  };
  std::string body = "{\"before\":" + list(before) + ",\"after\":" + list(a.active) + ",\"pending\":" +
                     (a.pending_set ? list(*a.pending_set) : std::string("[]")) + ",\"reason\":\"" + reason_name(why) +
                     "\",\"queues_version\":" + std::to_string(a.queues_version) + "}";
  events_->publish(AgentEvent{"queues_changed", id_, a.cfg.id, now, std::move(body), ""});
}

// ======================================================================================
// Phase 3 (callbacks, voicemail tasks, buckets): see below
// ======================================================================================

namespace {
bool valid_e164(const std::string& s) {
  if (s.size() < 8 || s.size() > 16 || s[0] != '+' || s[1] < '1' || s[1] > '9') return false;
  return std::all_of(s.begin() + 1, s.end(), [](char c) { return c >= '0' && c <= '9'; });
}
}  // namespace

// ConvertToCallback (L15; CONVENTIONS "Callbacks"): the record is durable in Postgres before the
// answer, the entry's removal or anything else. While it is being written the entry is offered to no
// one; if Postgres can't take it the entry goes back as it was and IE gets UNAVAILABLE.
void Tenant::convert_to_callback(const rv1::ConvertToCallbackRequest& r, int64_t now, CallbackDone done) {
  if (!valid_uuid(r.entry_id()) || !valid_e164(r.callback_number())) {
    done(err(Code::kInvalidArgument, "invalid_request", "entry_id or callback_number"));
    return;
  }
  auto own = owned(r.owner(), /*create=*/false, now);
  if (!own) {
    done(own.error().code == Code::kNotFound ? err(Code::kNotFound, "entry_unknown") : std::unexpected(own.error()));
    return;
  }
  Interaction& ix = **own;
  if (auto c = callback_by_entry_.find(r.entry_id()); c != callback_by_entry_.end()) {
    rv1::ConvertToCallbackResponse out;  // a repeat
    out.set_callback_id(c->second);
    out.set_existing(true);
    done(out);
    return;
  }
  if (ix.entry_id != r.entry_id()) {
    done(err(Code::kNotFound, "entry_unknown", "no such entry"));
    return;
  }
  Entry& e = entries_.at(r.entry_id());
  if (e.converting) {
    done(err(Code::kUnavailable, "store_unavailable", "the conversion is being written; retry"));
    return;
  }
  if (e.state != rv1::ENTRY_STATE_WAITING || e.queue.empty() || !e.callback_id.empty()) {
    done(err(Code::kFailedPrecondition, "entry_not_waiting", "the entry isn't waiting in a queue"));
    return;
  }
  CallbackRecord c;
  c.tenant = id_;
  c.callback_id = new_uuid_v7(now);
  c.original_interaction_id = ix.id;
  c.entry_id = e.id;
  c.queue_id = e.queue;
  c.number = r.callback_number();
  c.priority = e.priority;
  c.reqs = e.reqs;
  c.first_queued_at = e.first_queued_at;
  c.ordering_at = r.keep_position() ? e.ordering_at : now;
  c.state = "scheduled";
  c.attempt_interaction_id = new_uuid_v7(now);
  c.due_at = now;
  c.created_at = c.updated_at = now;
  // Offered to no one while it is written (an agent can't take a caller who is becoming a callback).
  if (!e.offer_id.empty()) revoke_offer(e.offer_id, av1::REVOKE_REASON_CALLER_GONE, now);
  e.converting = true;
  refresh_entry(e);
  const std::string cb = c.callback_id;
  callbacks_[cb] = c;
  touch_cb(cb);
  const uint64_t inst = instance_;
  auto shared = std::make_shared<CallbackDone>(std::move(done));
  const bool queued = durable_ && durable_->put_callback(c, [cb, inst, shared](Tenant* t, bool ok, int64_t at) {
    if (!t || t->instance() != inst) {
      (*shared)(err(Code::kUnavailable, "store_unavailable", "RE restarted the tenant"));
      return;
    }
    t->on_convert_persisted(cb, ok, at, *shared);
  });
  if (!queued) on_convert_persisted(cb, false, now, *shared);
}

void Tenant::on_convert_persisted(const std::string& cb, bool ok, int64_t now, const CallbackDone& done) {
  auto it = callbacks_.find(cb);
  if (it == callbacks_.end()) {
    done(err(Code::kUnavailable, "store_unavailable", "callback lost"));
    return;
  }
  CallbackRecord& c = it->second;
  touch_cb(cb);
  auto e = entries_.find(c.entry_id);
  if (!ok) {
    // Postgres down or busy: nothing changed for the caller; IE may retry or take another path.
    ++counters_->callbacks_refused;
    callbacks_.erase(it);
    if (e != entries_.end()) {
      e->second.converting = false;
      refresh_entry(e->second);
      todo_entries_.push_back(e->first);
      if (e->second.overflow_due) {
        e->second.overflow_due = false;
        overflow(e->second, now);
      }
    }
    settle(now);
    done(err(Code::kUnavailable, "store_unavailable", "callbacks can't be stored now"));
    return;
  }
  callback_by_entry_[c.entry_id] = cb;
  // The entry becomes the callback (DequeueReason CALLBACK): its caller may hang up now.
  if (e != entries_.end()) {
    e->second.converting = false;
    left_waiting(e->second, now, /*abandoned=*/false);
    remove_entry(e->second, now);
  }
  ++counters_->callbacks_created;
  requeue_callback(cb, now);
  settle(now);
  rv1::ConvertToCallbackResponse out;
  out.set_callback_id(cb);
  out.set_existing(false);
  done(out);
}

void Tenant::requeue_callback(const std::string& cb, int64_t now) {
  auto it = callbacks_.find(cb);
  if (it == callbacks_.end() || it->second.state != "scheduled") return;
  if (it->second.due_at > now) {
    timers_->at(it->second.due_at, TimerRef{id_, TimerKind::kCallbackDue, cb});
    return;
  }
  create_callback_entry(it->second, now);
}

// The callback waits in its queue like a caller (at its ordering time) under an interaction id RE
// made for this attempt; it has no caller on hold, so no max-wait action (escalation still applies).
void Tenant::create_callback_entry(CallbackRecord& c, int64_t now) {
  auto qit = queues_.find(c.queue_id);
  if (qit == queues_.end() || c.queue_id.empty()) {
    flag_callback(c, "queue_gone", now);
    return;
  }
  auto [ixit, fresh] = interactions_.try_emplace(c.attempt_interaction_id);
  Interaction& ix = ixit->second;
  touch_ix(c.attempt_interaction_id);
  hold_ix(c.attempt_interaction_id);
  if (fresh) {
    ix.id = c.attempt_interaction_id;
    ix.next_seq = epochs_->seq_base;
    ix.acked = epochs_->seq_base;
  }
  if (!ix.entry_id.empty() && entries_.contains(ix.entry_id)) return;  // already queued
  ix.callback_id = c.callback_id;
  ix.unacked.clear();  // an earlier dispatch of this attempt is void (IE drops a repeat by its id)
  ix.last_activity = now;
  QueueSt& q = qit->second;
  Entry e;
  e.id = new_uuid_v7(now);
  e.interaction = ix.id;
  e.queue = e.first_queue = q.cfg.id;
  e.target.set_queue_id(q.cfg.id);
  e.enqueue_seq = 1;
  e.priority = c.priority == 0 ? q.cfg.default_priority : c.priority;
  e.reqs = c.reqs;
  e.sig = [&] {
    std::string s;
    for (const auto& r : e.reqs) s += r.skill_id + ":" + std::to_string(r.min_level) + ";";
    return s;
  }();
  e.first_queued_at = c.first_queued_at;
  e.ordering_at = e.original_ordering_at = std::min(c.ordering_at, now);
  e.queue_entered_at = e.created_at = now;
  e.order_seq = ++order_seq_;
  e.state = rv1::ENTRY_STATE_WAITING;
  e.callback_id = c.callback_id;
  e.delay_passed = q.secondary_delay_ms <= 0;
  auto [eit, ok] = entries_.emplace(e.id, std::move(e));
  Entry& ref = eit->second;
  ix.entry_id = ref.id;
  timers_->at(now + q.escalate_after_ms, TimerRef{id_, TimerKind::kEscalate, ref.id});
  if (!ref.delay_passed) timers_->at(now + q.secondary_delay_ms, TimerRef{id_, TimerKind::kSecondaryDelay, ref.id});
  refresh_entry(ref);
  todo_entries_.push_back(ref.id);
}

// An agent accepted a callback: the attempt (its interaction id, the agent, the reservation) is
// durable BEFORE RunCallback leaves RE, so after a restart RE knows it may have been dialled and
// re-sends only the same interaction id, which IE drops if it already placed it (TCPA).
void Tenant::dispatch_callback(Entry& e, Reservation& r, int64_t now) {
  auto it = callbacks_.find(e.callback_id);
  if (it == callbacks_.end()) return;
  CallbackRecord& c = it->second;
  touch_cb(c.callback_id);
  c.state = "dispatched";
  c.reservation_id = r.id;
  c.agent_id = r.agent;
  c.updated_at = now;
  const std::string cb = c.callback_id, rid = r.id;
  const uint64_t inst = instance_;
  const bool queued = durable_ && durable_->put_callback(c, [cb, rid, inst](Tenant* t, bool ok, int64_t at) {
    if (t && t->instance() == inst) t->on_dispatch_persisted(cb, rid, ok, at);
  });
  if (!queued) on_dispatch_persisted(cb, rid, false, now);
}

void Tenant::on_dispatch_persisted(const std::string& cb, const std::string& rid, bool ok, int64_t now) {
  auto it = callbacks_.find(cb);
  auto rit = reservations_.find(rid);
  if (it == callbacks_.end()) return;
  CallbackRecord& c = it->second;
  touch_cb(cb);
  const bool live = rit != reservations_.end() && rit->second.st == ResSt::kAssigned;
  if (ok && live) {
    send_run_callback(cb, now);
    ++counters_->callbacks_dispatched;
    timers_->at(now + kCallbackRedeliverMs, TimerRef{id_, TimerKind::kCallbackRedeliver, cb});
    settle(now);
    return;
  }
  if (!ok) ++counters_->callbacks_refused;
  if (live) {
    // Not durable: no RunCallback. The agent is freed (no penalty); the callback tries again in 30 s.
    Reservation& res = rit->second;
    end_reservation(res, rv1::OUTCOME_REVOKED, now);
    auto e = entries_.find(res.entry_id);
    if (e != entries_.end()) remove_entry(e->second, now);
    become_free(agents_.at(res.agent), now, /*echo=*/true);
  }
  if (c.state == "dispatched" && !ok) {
    c.state = "scheduled";
    c.reservation_id.clear();
    c.agent_id.clear();
    c.due_at = now + 30'000;
    requeue_callback(cb, now);
  }
  settle(now);
}

void Tenant::send_run_callback(const std::string& cb, int64_t now) {
  auto it = callbacks_.find(cb);
  if (it == callbacks_.end()) return;
  const CallbackRecord& c = it->second;
  auto ixit = interactions_.find(c.attempt_interaction_id);
  auto rit = reservations_.find(c.reservation_id);
  if (ixit == interactions_.end() || rit == reservations_.end()) return;
  Interaction& ix = ixit->second;
  if (ix.replica.empty() || !sink_->ie_live(ix.replica)) {
    // The replica the caller was on, if live; else any live one (the redelivery moves it on).
    auto orig = interactions_.find(c.original_interaction_id);
    if (orig != interactions_.end() && !orig->second.replica.empty() && sink_->ie_live(orig->second.replica)) {
      ix.replica = orig->second.replica;
    } else {
      auto reps = sink_->ie_replicas();
      if (!reps.empty()) ix.replica = reps.front();
    }
  }
  const Reservation& res = rit->second;
  {
    LogRec lr("info", "callback.dispatched", "RunCallback sent: the callback's call is placed by IE");
    if (lr.on()) lc(lr, ix.id).parent_xsid(c.original_interaction_id).str("reservation_id", res.id).str("agent_id", res.agent)
                     .str("queue_id", c.queue_id).attr("callback_id", c.callback_id).attr("attempt", static_cast<int64_t>(c.attempt));
  }
  rv1::WatchResponse m;
  auto* rc = m.mutable_run_callback();
  rc->set_tenant_id(id_);
  rc->set_interaction_id(ix.id);
  rc->set_original_interaction_id(c.original_interaction_id);
  rc->set_callback_id(c.callback_id);
  rc->set_reservation_id(res.id);
  rc->set_agent_id(res.agent);
  rc->set_agent_contact(res.contact);
  rc->set_number(c.number);
  rc->set_queue_id(c.queue_id);
  rc->set_first_queued_at_ms(c.first_queued_at);
  rc->set_deadline_ms(res.deadline_ms);
  send_ie(ix, std::move(m), now);
}

// IE acknowledged (or sent an RPC under) its claim of the attempt: from now on that replica owns it.
void Tenant::claim_callback(Interaction& ix, int64_t now) {
  auto it = callbacks_.find(ix.callback_id);
  if (it == callbacks_.end() || it->second.state != "dispatched" || it->second.attempt_interaction_id != ix.id) return;
  touch_cb(it->first);
  it->second.state = "claimed";
  it->second.updated_at = now;
  persist_callback(it->first);
}

void Tenant::persist_callback(const std::string& cb) {
  touch_cb(cb);
  auto it = callbacks_.find(cb);
  if (it == callbacks_.end() || !durable_) return;
  if (!durable_->put_callback(it->second, [](Tenant*, bool, int64_t) {})) ++counters_->callbacks_refused;
}

void Tenant::flag_callback(CallbackRecord& c, const std::string& why, int64_t now) {
  // CONVENTIONS: an expired callback is not redialled automatically; flagged, counted, alerted.
  c.state = "flagged";
  c.outcome = why;
  c.updated_at = now;
  ++counters_->callbacks_flagged;
  persist_callback(c.callback_id);
}

// The attempt's reservation ended (an outcome, RE's expiry or revoke): what happens to the callback.
void Tenant::callback_ended(const std::string& cb, rv1::Outcome o, int64_t now) {
  auto it = callbacks_.find(cb);
  if (it == callbacks_.end()) return;
  CallbackRecord& c = it->second;
  if (c.state == "done" || c.state == "flagged") return;
  touch_cb(cb);
  const bool claimed = c.state == "claimed";
  auto ix = interactions_.find(c.attempt_interaction_id);
  if (ix != interactions_.end()) {
    touch_ix(ix->first);
    if (auto e = entries_.find(ix->second.entry_id); e != entries_.end()) remove_entry(e->second, now);
    // The attempt is over: its RunCallback must never be sent again (an unclaimed one would be
    // dialled; TCPA). Other messages (a Revoke) still reach the owner.
    std::erase_if(ix->second.unacked, [](const OutIe& m) { return m.msg.has_run_callback(); });
  }
  c.updated_at = now;
  c.reservation_id.clear();
  c.agent_id.clear();
  auto again = [&](bool new_attempt, int64_t due) {
    if (new_attempt) c.attempt_interaction_id = new_uuid_v7(now);
    c.state = "scheduled";
    c.due_at = due;
    persist_callback(cb);
    requeue_callback(cb, now);
  };
  switch (o) {
    case rv1::OUTCOME_CONNECTED:
      c.state = "done";
      c.outcome = "connected";
      persist_callback(cb);
      break;
    case rv1::OUTCOME_CALLER_GONE:
    case rv1::OUTCOME_ALREADY_ASSIGNED:
      c.state = "done";
      c.outcome = o == rv1::OUTCOME_CALLER_GONE ? "customer_gone" : "already_assigned";
      persist_callback(cb);
      break;
    case rv1::OUTCOME_CUSTOMER_NO_ANSWER:
      if (++c.attempt >= cfg_.callback_max_attempts) {
        c.state = "done";
        c.outcome = "no_answer";
        persist_callback(cb);
      } else {
        again(/*new_attempt=*/true, now + cfg_.callback_retry_after_ms);
      }
      break;
    case rv1::OUTCOME_AGENT_NO_ANSWER:
    case rv1::OUTCOME_AGENT_LEG_FAILED:
      again(/*new_attempt=*/true, now);  // the agent's side failed: another agent, a new attempt
      break;
    case rv1::OUTCOME_REVOKED:
      if (claimed) {
        flag_callback(c, "revoked_after_claim", now);  // IE may have dialled: never again by itself
      } else {
        again(/*new_attempt=*/false, now);  // the same id: IE drops it if it already has it
      }
      break;
    default:  // EXPIRED and anything else: flagged
      flag_callback(c, "expired", now);
      break;
  }
}

void Tenant::on_callback_timer(const TimerRef& t, int64_t now) {
  auto it = callbacks_.find(t.key);
  if (it == callbacks_.end()) return;
  CallbackRecord& c = it->second;
  if (t.kind == TimerKind::kCallbackDue) {
    if (c.state == "scheduled" && now >= c.due_at) create_callback_entry(c, now);
    return;
  }
  // kCallbackRedeliver: unacknowledged for 5 s: another live stream, never past deadline + 10 s.
  if (c.state != "dispatched") return;
  auto ix = interactions_.find(c.attempt_interaction_id);
  auto rit = reservations_.find(c.reservation_id);
  if (ix == interactions_.end() || ix->second.owner_epoch > 0 || ix->second.unacked.empty()) return;
  if (rit == reservations_.end() || rit->second.st != ResSt::kAssigned) return;
  if (now >= rit->second.deadline_ms + kCallbackGraceMs) return;
  auto reps = sink_->ie_replicas();
  if (!reps.empty()) {
    auto cur = std::find(reps.begin(), reps.end(), ix->second.replica);
    const std::string next = cur == reps.end() ? reps.front() : *(std::next(cur) == reps.end() ? reps.begin() : std::next(cur));
    if (next != ix->second.replica) {
      touch_ix(ix->first);
      ix->second.replica = next;
      replay_ie(ix->second, now);
      ++counters_->redeliveries;
    }
  }
  timers_->at(now + kCallbackRedeliverMs, TimerRef{id_, TimerKind::kCallbackRedeliver, t.key});
}

// After a restart (phase 1's rule: RE's memory is gone, Postgres isn't). Scheduled callbacks queue
// again. A dispatched one goes back with the SAME interaction id while IE still remembers it (its
// ie.calls ENDED marker lasts 1 h), so IE drops it if it already placed it; older, or claimed by IE,
// RE can't know what happened: flagged, never redialled automatically (TCPA).
void Tenant::restore_callbacks(const std::vector<CallbackRecord>& rows, int64_t now) {
  for (const auto& row : rows) {
    if (row.tenant != id_ || callbacks_.contains(row.callback_id)) continue;
    CallbackRecord c = row;
    callbacks_[c.callback_id] = c;
    touch_cb(c.callback_id);
    CallbackRecord& ref = callbacks_.at(c.callback_id);
    callback_by_entry_[c.entry_id] = c.callback_id;
    if (c.state == "scheduled") {
      requeue_callback(c.callback_id, now);
    } else if (c.state == "dispatched" && now - c.updated_at < kHourMs) {
      ref.state = "scheduled";
      ref.reservation_id.clear();
      ref.agent_id.clear();
      ref.due_at = now;
      persist_callback(c.callback_id);
      requeue_callback(c.callback_id, now);
    } else if (c.state == "dispatched" || c.state == "claimed") {
      flag_callback(ref, "unknown_after_restart", now);
    }
  }
  settle(now);
}

std::map<std::string, std::string> Tenant::callback_states() const {
  std::map<std::string, std::string> out;
  for (const auto& [id, c] : callbacks_) out[id] = c.state;
  return out;
}

// CreateVoicemailTask: durable before the answer; a repeat (the same recording) answers the task
// made first, also across restarts (Postgres' unique (tenant, recording_id)).
void Tenant::create_voicemail_task(const rv1::CreateVoicemailTaskRequest& r, int64_t now, TaskDone done) {
  if (!valid_uuid(r.recording_id()) || r.recorded_at_ms() <= 0 || now - r.recorded_at_ms() > kDay ||
      r.duration_s() == 0 || r.duration_s() > 86'400) {
    done(err(Code::kInvalidArgument, "invalid_request", "recording_id, recorded_at_ms or duration_s"));
    return;
  }
  if (r.queue_id().empty() || !queues_.contains(r.queue_id())) {
    done(err(Code::kFailedPrecondition, "queue_unknown", "no such queue"));
    return;
  }
  auto own = owned(r.owner(), /*create=*/true, now);
  if (!own) {
    done(std::unexpected(own.error()));
    return;
  }
  if (auto t = tasks_by_recording_.find(r.recording_id()); t != tasks_by_recording_.end()) {
    if (t->second.interaction_id != r.owner().interaction_id() || t->second.queue_id != r.queue_id()) {
      done(err(Code::kInvalidArgument, "invalid_request", "the recording belongs to another task"));
      return;
    }
    rv1::CreateVoicemailTaskResponse out;
    out.set_task_id(t->second.task_id);
    out.set_existing(true);
    done(out);
    return;
  }
  TaskRecord tr;
  tr.tenant = id_;
  tr.task_id = new_uuid_v7(now);
  tr.interaction_id = r.owner().interaction_id();
  tr.queue_id = r.queue_id();
  tr.recording_id = r.recording_id();
  tr.recorded_at = std::min(r.recorded_at_ms(), now);
  tr.duration_s = r.duration_s();
  tr.created_at = now;
  const uint64_t inst = instance_;
  auto shared = std::make_shared<TaskDone>(std::move(done));
  const bool queued = durable_ && durable_->put_task(tr, [tr, inst, shared](Tenant* t, bool ok, const std::string& task_id, bool existing, int64_t) {
    if (!ok || !t || t->instance() != inst) {
      if (t && t->instance() == inst) ++t->counters_->callbacks_refused;
      (*shared)(err(Code::kUnavailable, "store_unavailable", "voicemail tasks can't be stored now"));
      return;
    }
    TaskRecord kept = tr;
    kept.task_id = task_id;
    t->tasks_by_recording_[tr.recording_id] = kept;
    if (!existing) ++t->counters_->voicemail_tasks;
    rv1::CreateVoicemailTaskResponse out;
    out.set_task_id(task_id);
    out.set_existing(existing);
    (*shared)(out);
  });
  if (!queued) {
    ++counters_->callbacks_refused;
    (*shared)(err(Code::kUnavailable, "store_unavailable", "voicemail tasks can't be stored now"));
  }
}

// routing_bucket_actuals (PLAN §5.3): every 15 minutes, one row per queue (zeros included: the
// forecasters need them).
void Tenant::count_offered_10s(BucketRow* b, int64_t now) {
  // The bucket running now (before the first take_buckets: the one `now` is in).
  const int64_t start = bucket_start_ > 0 ? bucket_start_ : now / kBucketMs * kBucketMs;
  const int64_t k = (now - start) / 10'000;
  if (k < 0 || k >= kBucketMs / 10'000) return;  // a late arrival of the previous bucket: 15-min count only
  if (b->offered_10s.empty()) b->offered_10s.assign(static_cast<size_t>(kBucketMs / 10'000), 0);
  uint16_t& c = b->offered_10s[static_cast<size_t>(k)];
  if (c < UINT16_MAX) ++c;
}

bool Tenant::install_guard(std::shared_ptr<const ArrivalGuard> g, int64_t now) {
  if (!g || g->tenant != id_) return false;
  if (guard_ && guard_->computed_at_ms > g->computed_at_ms) return false;  // an older plan's
  guard_ = std::move(g);
  return true;
}

bool Tenant::guard_breached(int64_t now) {
  if (!guard_ || now > guard_->valid_until_ms || last_snapshot_ <= 0 || now - last_early_ms_ < 2'000) return false;
  const double win_s = static_cast<double>(now - last_snapshot_) / 1000.0;
  if (win_s <= 0) return false;
  for (const auto& [qid, rate] : guard_->per_s) {
    auto it = queues_.find(qid);
    if (it == queues_.end() || !(rate >= 0) || !std::isfinite(rate)) continue;
    const double e = rate * win_s, n = static_cast<double>(it->second.arrivals);
    const double band = 4 * std::sqrt(e) + 3;
    if (n >= e + band || (e - band > 0 && n <= e - band)) {
      last_early_ms_ = now;
      return true;
    }
  }
  return false;
}

std::vector<BucketRow> Tenant::take_buckets(int64_t now) {
  std::vector<BucketRow> out;
  const int64_t start = now / kBucketMs * kBucketMs;
  if (bucket_start_ == 0) {
    bucket_start_ = start;
    return out;
  }
  if (start <= bucket_start_) return out;
  for (auto& [id, q] : queues_) {
    touch_queue(id);
    if (id.empty()) continue;
    BucketRow b = q.bucket;
    b.tenant = id_;
    b.queue_id = id;
    b.bucket_start_ms = bucket_start_;
    out.push_back(std::move(b));
    q.bucket = BucketRow{};
  }
  bucket_start_ = start;
  return out;
}

}  // namespace pcr
