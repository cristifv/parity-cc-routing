// Phase 6 (PLAN §5.5): a tenant's state as re.state records, and its rebuild from them. The writer
// marks what each operation changes (touch_*); collect() encodes those objects at the end of the
// task; restore() is the takeover and restart path (and a tenant move's arrival).
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <algorithm>

#include "src/core/tenant.h"
#include "src/state/state.pb.h"

namespace pcr {

namespace st = ::parity::cc_routing::state::v1;

namespace {

constexpr int64_t kAssignGraceMs = 2'000;     // tenant.cc: L7's deadline + 2 s
constexpr int64_t kCallbackGraceMs = 10'000;  // tenant.cc: a RunCallback's deadline + 10 s
constexpr int64_t kCallbackRedeliverMs = 5'000;

// Deterministic bytes (maps sorted), so equal state hashes equal.
std::string det(const google::protobuf::MessageLite& m) {
  std::string out;
  {
    google::protobuf::io::StringOutputStream so(&out);
    google::protobuf::io::CodedOutputStream co(&so);
    co.SetSerializationDeterministic(true);
    if (!m.SerializeToCodedStream(&co)) return {};
  }
  return out;
}

uint64_t hash_of(const std::string& s) { return std::hash<std::string>{}(s); }

const char* const kKinds[] = {"plan", "pin", "agent", "queue", "out", "entry", "offer", "res", "cmd", "cb", "opt", "camp"};

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

template <class Set, class Rep>
void put_members(const Set& set, Rep* out) {
  for (const auto& [q, m] : set) {
    auto* x = out->Add();
    x->set_queue_id(q);
    x->set_tier(static_cast<int32_t>(m.tier));
    x->set_pinned(m.pinned);
    x->set_since_ms(m.since);
  }
}

template <class Set, class Rep>
void get_members(const Rep& in, Set* out) {
  out->clear();
  for (const auto& x : in) {
    typename Set::mapped_type m;
    m.tier = static_cast<Tier>(x.tier());
    m.pinned = x.pinned();
    m.since = x.since_ms();
    (*out)[x.queue_id()] = m;
  }
}

}  // namespace

std::string Tenant::state_key(const char* kind, const std::string& id) const {
  std::string k;
  k.reserve(id_.size() + id.size() + 8);
  k += id_;
  k += '/';
  k += kind;
  k += '/';
  k += id;
  return k;
}

void Tenant::touch_all() {
  if (!track_) return;
  for (const auto& [id, a] : agents_) dirty_.agent.insert(id);
  for (const auto& [id, e] : entries_) dirty_.entry.insert(id);
  for (const auto& [id, o] : offers_) dirty_.offer.insert(id);
  for (const auto& [id, r] : reservations_) dirty_.res.insert(id);
  for (const auto& [id, ix] : interactions_) dirty_.out.insert(id);
  for (const auto& [id, c] : commands_) dirty_.cmd.insert(id);
  for (const auto& [id, v] : pin_versions_) dirty_.pin.insert(id);
  for (const auto& [id, p] : pins_) dirty_.pin.insert(id);
  for (const auto& [id, c] : callbacks_) dirty_.cb.insert(id);
  for (const auto& [id, q] : queues_) dirty_.queue.insert(id);
  for (const auto& [id, c] : campaigns_) dirty_.camp.insert(id);
  dirty_.plan = true;
  dirty_.opt = true;
}

bool Tenant::encode(const std::string& kind, const std::string& id, st::Record* rec) const {
  if (kind == "agent") {
    auto it = agents_.find(id);
    if (it == agents_.end()) return false;
    const AgentSt& a = it->second;
    st::Agent* p = rec->mutable_agent();
    p->set_id(a.cfg.id);
    p->set_state(a.state);
    p->set_reason(a.reason);
    p->set_pending(a.pending);
    p->set_pending_reason(a.pending_reason);
    p->set_since_ms(a.since);
    p->set_version(a.version);
    p->set_idle_since_ms(a.idle_since);
    p->set_attached(a.attached);
    p->set_session_id(a.session_id);
    p->set_replica(a.replica);
    p->set_contact(a.contact);
    p->set_generation(a.generation);
    p->set_detached_at_ms(a.detached_at);
    p->set_offer_id(a.offer_id);
    for (const auto& r : a.reservations) p->add_reservations(r);
    p->set_next_seq(a.next_seq);
    for (const auto& m : a.unacked) {
      auto* o = p->add_unacked();
      o->set_seq(m.seq);
      o->set_msg(det(m.msg));
    }
    put_members(a.active, p->mutable_active());
    if (a.pending_set) {
      p->set_has_pending_set(true);
      put_members(*a.pending_set, p->mutable_pending_set());
    }
    p->set_pending_set_reason(a.pending_set_reason);
    p->set_queues_version(a.queues_version);
    p->set_active_version(a.active_version);
    p->set_active_version_busy(a.active_version_busy);
    if (a.plan) {
      p->set_has_plan(true);
      for (const auto& [q, t] : *a.plan) {
        auto* x = p->add_plan();
        x->set_queue_id(q);
        x->set_tier(static_cast<int32_t>(t));
      }
    }
    for (const auto& [q, w] : a.weights) (*p->mutable_weights())[q] = w;
    for (int64_t t : a.move_times) p->add_move_times_ms(t);
    p->set_primary_since_ms(a.primary_since);
    p->set_secondary_since_ms(a.secondary_since);
    p->set_retarget_due(a.retarget_due);
    p->set_campaign(a.campaign);  // phase 8: the mode flag and a pending switch
    p->set_mode_pending(a.mode_pending);
    p->set_to_campaign(a.to_campaign);
    return true;
  }
  if (kind == "camp") {
    auto it = campaigns_.find(id);
    if (it == campaigns_.end()) return false;
    st::Campaign* p = rec->mutable_camp();
    p->set_id(it->second.id);
    p->set_owner_epoch(it->second.owner_epoch);
    p->set_replica(it->second.replica);
    return true;
  }
  if (kind == "entry") {
    auto it = entries_.find(id);
    if (it == entries_.end()) return false;
    const Entry& e = it->second;
    st::Entry* p = rec->mutable_entry();
    p->set_id(e.id);
    p->set_interaction_id(e.interaction);
    p->set_queue_id(e.queue);
    p->set_first_queue_id(e.first_queue);
    p->set_enqueue_seq(e.enqueue_seq);
    p->set_priority(e.priority);
    for (const auto& r : e.reqs) {
      auto* x = p->add_reqs();
      x->set_skill_id(r.skill_id);
      x->set_min_level(r.min_level);
    }
    p->set_first_queued_at_ms(e.first_queued_at);
    p->set_ordering_at_ms(e.ordering_at);
    p->set_queue_entered_at_ms(e.queue_entered_at);
    p->set_created_at_ms(e.created_at);
    p->set_original_ordering_at_ms(e.original_ordering_at);
    p->set_order_seq(e.order_seq);
    p->set_state(e.state);
    p->set_escalated(e.escalated);
    p->set_overflow_due(e.overflow_due);
    p->set_offer_id(e.offer_id);
    p->set_reservation_id(e.reservation_id);
    for (const auto& [a, until] : e.skips) (*p->mutable_skips())[a] = until;
    p->set_delay_passed(e.delay_passed);
    p->set_target(det(e.target));
    p->set_direct_agent(e.direct_agent);
    p->set_consult_from(e.consult_from);
    p->set_head(e.head);
    p->set_callback_id(e.callback_id);
    p->set_converting(e.converting);
    p->set_callback_offered(e.callback_offered);
    p->set_vip(e.vip);
    return true;
  }
  if (kind == "offer") {
    auto it = offers_.find(id);
    if (it == offers_.end()) return false;
    st::Offer* p = rec->mutable_offer();
    p->set_id(it->second.id);
    p->set_agent_id(it->second.agent);
    p->set_entry_id(it->second.entry);
    p->set_expires_at_ms(it->second.expires_at);
    p->set_tier(static_cast<int32_t>(it->second.tier));
    return true;
  }
  if (kind == "res") {
    auto it = reservations_.find(id);
    if (it == reservations_.end()) return false;
    const Reservation& r = it->second;
    st::Reservation* p = rec->mutable_res();
    p->set_id(r.id);
    p->set_interaction_id(r.interaction);
    p->set_entry_id(r.entry_id);
    p->set_agent_id(r.agent);
    p->set_contact(r.contact);
    p->set_queue_id(r.queue);
    p->set_deadline_ms(r.deadline_ms);
    p->set_st(static_cast<int32_t>(r.st));
    p->set_outcome(r.outcome);
    p->set_connected_once(r.connected_once);
    p->set_connected_at_ms(r.connected_at);
    p->set_wrap_ends_ms(r.wrap_ends);
    p->set_ended_at_ms(r.ended_at);
    p->set_pause_since_ms(r.pause_since);
    p->set_consult(r.consult);
    p->set_direct(r.direct);
    p->set_callback_id(r.callback_id);
    p->set_campaign(r.campaign);
    return true;
  }
  if (kind == "out") {
    auto it = interactions_.find(id);
    if (it == interactions_.end()) return false;
    const Interaction& ix = it->second;
    st::Interaction* p = rec->mutable_out();
    p->set_id(ix.id);
    p->set_owner_epoch(ix.owner_epoch);
    p->set_replica(ix.replica);
    p->set_entry_id(ix.entry_id);
    for (const auto& e : ix.past_entries) p->add_past_entries(e);
    for (const auto& [seq, v] : ix.snaps) (*p->mutable_snaps())[seq] = det(v);
    for (const auto& [seq, q] : ix.seq_queue) (*p->mutable_seq_queue())[seq] = q;
    p->set_max_enqueue_seq(ix.max_enqueue_seq);
    p->set_next_seq(ix.next_seq);
    p->set_acked(ix.acked);
    for (const auto& m : ix.unacked) {
      auto* o = p->add_unacked();
      o->set_seq(m.seq);
      if (m.msg.has_run_callback() && !m.msg.run_callback().number().empty()) {
        // PII: a callback's number never goes to re.state (CR-107); reconcile_callbacks puts it back.
        rv1::WatchResponse copy = m.msg;
        copy.mutable_run_callback()->clear_number();
        o->set_msg(det(copy));
      } else {
        o->set_msg(det(m.msg));
      }
    }
    for (const auto& r : ix.reservations) p->add_reservations(r);
    p->set_owner_lost(ix.owner_lost);
    p->set_callback_id(ix.callback_id);
    return true;
  }
  if (kind == "cmd") {
    auto it = commands_.find(id);
    if (it == commands_.end()) return false;
    st::Command* p = rec->mutable_cmd();
    p->set_command_id(id);
    p->set_fingerprint(it->second.fingerprint);
    p->set_at_ms(it->second.at);
    if (it->second.error) {
      p->set_has_error(true);
      p->set_error_code(static_cast<int32_t>(it->second.error->code));
      p->set_error_parity(it->second.error->parity);
      p->set_error_message(it->second.error->message);
    }
    p->set_response(it->second.response);
    return true;
  }
  if (kind == "plan") {
    st::Plan* p = rec->mutable_plan();
    p->set_version(policy_.version);
    p->set_mode(static_cast<int32_t>(policy_.mode));
    p->set_dwell_min_ms(policy_.dwell_min_ms);
    p->set_max_moves_per_hour(policy_.max_moves_per_hour);
    p->set_reject_skip_ms(policy_.reject_skip_ms);
    p->set_vip_skill(policy_.vip_skill);
    for (const auto& [level, speed] : policy_.proficiency_speed) {
      auto* x = p->add_proficiency_speed();
      x->set_min_level(level);
      x->set_speed(speed);
    }
    for (const auto& q : policy_.queues) {
      auto* x = p->add_queues();
      x->set_queue_id(q.queue_id);
      x->set_alpha(q.alpha);
      x->set_beta(q.beta);
      x->set_min_agents(q.min_agents);
      x->set_max_agents(q.max_agents);
      x->set_secondary_delay_ms(q.secondary_delay_ms);
      x->set_escalate_after_ms(q.escalate_after_ms);
      x->set_weight(q.weight);
      x->set_weight_min(q.weight_min);
      x->set_weight_max(q.weight_max);
      x->set_delay_min_ms(q.delay_min_ms);
      x->set_delay_max_ms(q.delay_max_ms);
      x->set_sl_target(q.sl_target);
      x->set_vip_sla_ms(q.vip_sla_ms);
      x->set_vip_alpha(q.vip_alpha);
      x->set_vip_beta(q.vip_beta);
      x->set_vip_reserved_min(q.vip_reserved_min);
    }
    return true;
  }
  if (kind == "opt") {
    // Phase 5 redesign: the lever plan held, its validity and the levers in force (a takeover keeps
    // them: no stall revert after a replay).
    if (opt_.version == 0 && opt_.stage == 0) return false;
    st::OptPlan* p = rec->mutable_opt();
    p->set_version(opt_.version);
    p->set_computed_at_ms(opt_.computed_at);
    p->set_valid_until_ms(opt_.valid_until);
    p->set_quality(opt_.quality);
    p->set_stage(opt_.stage);
    for (const auto& [q, wd] : opt_.applied) {
      auto* x = p->add_applied();
      x->set_queue_id(q);
      x->set_weight(wd.first);
      x->set_delay_ms(wd.second);
    }
    return true;
  }
  if (kind == "pin") {
    auto v = pin_versions_.find(id);
    auto p = pins_.find(id);
    if (v == pin_versions_.end() && p == pins_.end()) return false;
    st::Pin* x = rec->mutable_pin();
    x->set_agent_id(id);
    if (v != pin_versions_.end()) x->set_version_seen(v->second);
    if (p != pins_.end()) {
      x->set_present(true);
      x->set_version(p->second.version);
      for (const auto& [q, t] : p->second.queues) {
        auto* y = x->add_queues();
        y->set_queue_id(q);
        y->set_tier(static_cast<int32_t>(t));
      }
      for (const auto& q : p->second.exclusions) x->add_exclusions(q);
      x->set_until_ms(p->second.until_ms);
      x->set_reason_code(p->second.reason_code);
    }
    return true;
  }
  if (kind == "cb") {
    auto it = callbacks_.find(id);
    if (it == callbacks_.end()) return false;
    const CallbackRecord& c = it->second;
    st::Callback* p = rec->mutable_cb();  // never the number (PII stays in Postgres)
    p->set_callback_id(c.callback_id);
    p->set_original_interaction_id(c.original_interaction_id);
    p->set_entry_id(c.entry_id);
    p->set_queue_id(c.queue_id);
    p->set_priority(c.priority);
    for (const auto& r : c.reqs) {
      auto* x = p->add_reqs();
      x->set_skill_id(r.skill_id);
      x->set_min_level(r.min_level);
    }
    p->set_first_queued_at_ms(c.first_queued_at);
    p->set_ordering_at_ms(c.ordering_at);
    p->set_state(c.state);
    p->set_attempt(c.attempt);
    p->set_attempt_interaction_id(c.attempt_interaction_id);
    p->set_reservation_id(c.reservation_id);
    p->set_agent_id(c.agent_id);
    p->set_outcome(c.outcome);
    p->set_due_at_ms(c.due_at);
    p->set_created_at_ms(c.created_at);
    p->set_updated_at_ms(c.updated_at);
    return true;
  }
  if (kind == "queue") {
    auto it = queues_.find(id);
    if (it == queues_.end()) return false;
    const QueueSt& q = it->second;
    st::QueueStats* p = rec->mutable_queue();
    p->set_queue_id(id);
    p->set_aht_ms(q.aht_ms);
    p->set_bucket_start_ms(bucket_start_);
    p->set_offered(q.bucket.offered);
    p->set_answered(q.bucket.answered);
    p->set_answered_in_sla(q.bucket.answered_in_sla);
    p->set_abandoned(q.bucket.abandoned);
    p->set_overflowed(q.bucket.overflowed);
    p->set_wait_answered_ms(q.bucket.wait_answered_ms);
    p->set_handle_ms(q.bucket.handle_ms);
    p->set_handled(q.bucket.handled);
    p->set_wrap_ms(q.wrap_ms);
    return true;
  }
  return false;
}

void Tenant::emit(const std::string& kind, const std::string& id, std::vector<StateRec>* out, int64_t now) {
  st::Record rec;
  const std::string key = state_key(kind.c_str(), id);
  if (!encode(kind, id, &rec)) {
    if (written_.erase(key) > 0) out->push_back(StateRec{id_, key, std::nullopt});
    return;
  }
  const uint64_t h = hash_of(det(rec));
  auto [it, fresh] = written_.try_emplace(key, h);
  if (!fresh) {
    if (it->second == h) return;
    it->second = h;
  }
  rec.set_schema_version(1);
  rec.set_tenant_epoch(tenant_epoch_);
  rec.set_leader_epoch(epochs_->leader_epoch);
  rec.set_written_at_ms(now);
  out->push_back(StateRec{id_, key, det(rec)});
}

void Tenant::collect(std::vector<StateRec>* out, int64_t now) {
  if (!track_) return;
  Dirty d = std::move(dirty_);
  dirty_ = Dirty{};
  if (d.plan) emit("plan", "routing", out, now);
  if (d.opt) emit("opt", "staffing", out, now);
  auto each = [&](const char* kind, const std::set<std::string>& ids) {
    for (const auto& id : ids) emit(kind, id, out, now);
  };
  each("pin", d.pin);
  each("agent", d.agent);
  each("queue", d.queue);
  each("out", d.out);
  each("entry", d.entry);
  each("offer", d.offer);
  each("res", d.res);
  each("cmd", d.cmd);
  each("cb", d.cb);
  each("camp", d.camp);
  if (!paranoid_) return;
  // Tests: anything whose encoding differs from what was written is a change nobody marked.
  auto check = [&](const char* kind, const std::string& id) {
    st::Record rec;
    const std::string key = state_key(kind, id);
    auto w = written_.find(key);
    if (!encode(kind, id, &rec)) {
      if (w != written_.end()) unmarked_.push_back(key + " (removed)");
      return;
    }
    if (w == written_.end() || w->second != hash_of(det(rec))) unmarked_.push_back(key);
  };
  check("plan", "routing");
  check("opt", "staffing");
  for (const auto& [id, a] : agents_) check("agent", id);
  for (const auto& [id, e] : entries_) check("entry", id);
  for (const auto& [id, o] : offers_) check("offer", id);
  for (const auto& [id, r] : reservations_) check("res", id);
  for (const auto& [id, ix] : interactions_) check("out", id);
  for (const auto& [id, c] : commands_) check("cmd", id);
  for (const auto& [id, c] : callbacks_) check("cb", id);
  for (const auto& [id, q] : queues_) check("queue", id);
  for (const auto& [id, c] : campaigns_) check("camp", id);
  std::set<std::string> pins;
  for (const auto& [id, v] : pin_versions_) pins.insert(id);
  for (const auto& [id, p] : pins_) pins.insert(id);
  for (const auto& id : pins) check("pin", id);
  for (const auto& [key, h] : written_) {
    // A key written once must still exist (or have been tombstoned by a collect).
    const size_t a = key.find('/'), b = key.find('/', a + 1);
    const std::string kind = key.substr(a + 1, b - a - 1), id = key.substr(b + 1);
    st::Record rec;
    if (!encode(kind, id, &rec)) unmarked_.push_back(key + " (gone, no tombstone)");
  }
}

std::string Tenant::durable_dump() const {
  std::vector<std::string> lines;
  auto add = [&](const char* kind, const std::string& id) {
    st::Record rec;
    if (encode(kind, id, &rec)) lines.push_back(state_key(kind, id) + "=" + rec.ShortDebugString());
  };
  add("plan", "routing");
  add("opt", "staffing");
  for (const auto& [id, a] : agents_) add("agent", id);
  for (const auto& [id, e] : entries_) add("entry", id);
  for (const auto& [id, o] : offers_) add("offer", id);
  for (const auto& [id, r] : reservations_) add("res", id);
  for (const auto& [id, ix] : interactions_) add("out", id);
  for (const auto& [id, c] : commands_) add("cmd", id);
  for (const auto& [id, c] : callbacks_) add("cb", id);
  for (const auto& [id, q] : queues_) add("queue", id);
  for (const auto& [id, c] : campaigns_) add("camp", id);
  std::set<std::string> pins;
  for (const auto& [id, v] : pin_versions_) pins.insert(id);
  for (const auto& [id, p] : pins_) pins.insert(id);
  for (const auto& id : pins) add("pin", id);
  std::sort(lines.begin(), lines.end());
  std::string out;
  for (const auto& l : lines) {
    out += l;
    out += '\n';
  }
  return out;
}

Tenant::RestoreStats Tenant::restore(const std::vector<std::pair<std::string, std::string>>& kvs, int64_t now,
                                     bool restamp) {
  RestoreStats stats;
  const bool tracking = track_;
  track_ = false;  // nothing below is a change: it is what re.state already holds
  std::map<std::string, std::vector<st::Record>> by_kind;
  for (const auto& [key, value] : kvs) {
    const size_t a = key.find('/');
    const size_t b = a == std::string::npos ? std::string::npos : key.find('/', a + 1);
    if (b == std::string::npos || key.substr(0, a) != id_) {
      ++stats.skipped;  // another tenant's key never reaches this tenant
      continue;
    }
    st::Record rec;
    if (!rec.ParseFromString(value)) {
      ++stats.skipped;
      continue;
    }
    by_kind[key.substr(a + 1, b - a - 1)].push_back(std::move(rec));
    ++stats.records;
  }
  // plan, then pins, then agents (their sets name queues and pins), then the rest.
  for (const auto& rec : by_kind["plan"]) {
    const st::Plan& p = rec.plan();
    RoutingPolicy pol;
    pol.version = p.version();
    pol.mode = static_cast<RoutingMode>(p.mode());
    pol.dwell_min_ms = p.dwell_min_ms();
    pol.max_moves_per_hour = p.max_moves_per_hour();
    pol.reject_skip_ms = p.reject_skip_ms();
    pol.vip_skill = p.vip_skill();
    for (const auto& x : p.proficiency_speed()) pol.proficiency_speed.emplace_back(x.min_level(), x.speed());
    for (const auto& q : p.queues()) {
      QueuePolicy qp;
      qp.queue_id = q.queue_id();
      qp.alpha = q.alpha();
      qp.beta = q.beta();
      qp.min_agents = q.min_agents();
      qp.max_agents = q.max_agents();
      qp.secondary_delay_ms = q.secondary_delay_ms();
      qp.escalate_after_ms = q.escalate_after_ms();
      qp.weight = q.weight();
      qp.weight_min = q.weight_min();
      qp.weight_max = q.weight_max();
      qp.delay_min_ms = q.delay_min_ms();
      qp.delay_max_ms = q.delay_max_ms();
      qp.sl_target = q.sl_target();
      qp.vip_sla_ms = q.vip_sla_ms();
      qp.vip_alpha = q.vip_alpha();
      qp.vip_beta = q.vip_beta();
      qp.vip_reserved_min = q.vip_reserved_min();
      pol.queues.push_back(qp);
    }
    policy_ = pol;
    apply_policy_to_queues();
  }
  for (const auto& rec : by_kind["opt"]) {
    const st::OptPlan& p = rec.opt();
    opt_ = Opt{};
    opt_.version = p.version();
    opt_.computed_at = p.computed_at_ms();
    opt_.valid_until = p.valid_until_ms();
    opt_.quality = static_cast<uint8_t>(p.quality());
    opt_.stage = p.stage();
    for (const auto& x : p.applied()) opt_.applied[x.queue_id()] = {x.weight(), x.delay_ms()};
    apply_policy_to_queues();  // the levers the plan put in force (auto, fresh)
  }
  for (const auto& rec : by_kind["pin"]) {
    const st::Pin& p = rec.pin();
    if (!agents_.contains(p.agent_id())) {
      ++stats.skipped;
      continue;
    }
    pins_.erase(p.agent_id());
    pin_versions_.erase(p.agent_id());
    if (p.version_seen() > 0) pin_versions_[p.agent_id()] = p.version_seen();
    if (!p.present()) continue;
    RoutingPin pin;
    pin.agent_id = p.agent_id();
    pin.version = p.version();
    for (const auto& q : p.queues()) pin.queues.emplace_back(q.queue_id(), static_cast<Tier>(q.tier()));
    for (const auto& q : p.exclusions()) pin.exclusions.push_back(q);
    pin.until_ms = p.until_ms();
    pin.reason_code = p.reason_code();
    pins_[pin.agent_id] = std::move(pin);
  }
  for (const auto& rec : by_kind["agent"]) {
    const st::Agent& p = rec.agent();
    auto it = agents_.find(p.id());
    if (it == agents_.end()) {
      ++stats.skipped;  // no longer in the tenant's config
      continue;
    }
    AgentSt& a = it->second;
    a.state = static_cast<av1::AgentState>(p.state());
    a.reason = p.reason();
    a.pending = static_cast<av1::TargetState>(p.pending());
    a.pending_reason = p.pending_reason();
    a.since = p.since_ms();
    a.version = p.version();
    a.idle_since = p.idle_since_ms();
    a.attached = p.attached();
    a.session_id = p.session_id();
    a.replica = p.replica();
    a.contact = p.contact();
    a.generation = p.generation();
    a.detached_at = p.detached_at_ms();
    a.offer_id = p.offer_id();
    a.reservations = std::set<std::string>(p.reservations().begin(), p.reservations().end());
    a.next_seq = p.next_seq();
    a.unacked.clear();
    for (const auto& m : p.unacked()) {
      OutGw o{m.seq(), 0, {}};
      if (!o.msg.ParseFromString(m.msg())) continue;
      o.msg.set_leader_epoch(epochs_->leader_epoch);  // replays carry the new leader's epoch
      a.unacked.push_back(std::move(o));
    }
    get_members(p.active(), &a.active);
    if (p.has_pending_set()) {
      Set s;
      get_members(p.pending_set(), &s);
      a.pending_set = std::move(s);
    } else {
      a.pending_set.reset();
    }
    a.pending_set_reason = static_cast<av1::MembershipReason>(p.pending_set_reason());
    a.queues_version = p.queues_version();
    a.active_version = p.active_version();
    a.active_version_busy = p.active_version_busy();
    if (p.has_plan()) {
      std::map<std::string, Tier> plan;
      for (const auto& x : p.plan()) plan[x.queue_id()] = static_cast<Tier>(x.tier());
      a.plan = std::move(plan);
    } else {
      a.plan.reset();
    }
    a.weights.clear();
    for (const auto& [q, w] : p.weights()) a.weights[q] = w;
    a.move_times = std::deque<int64_t>(p.move_times_ms().begin(), p.move_times_ms().end());
    a.primary_since = p.primary_since_ms();
    a.secondary_since = p.secondary_since_ms();
    a.retarget_due = p.retarget_due();
    a.campaign = p.campaign();
    a.mode_pending = p.mode_pending();
    a.to_campaign = p.to_campaign();
    a.avail_sent = -1;  // the leader's stream gets a snapshot when it opens
  }
  for (const auto& rec : by_kind["camp"]) {
    const st::Campaign& p = rec.camp();
    campaigns_[p.id()] = Campaign{p.id(), p.replica(), p.owner_epoch()};
  }
  for (const auto& rec : by_kind["queue"]) {
    const st::QueueStats& p = rec.queue();
    auto it = queues_.find(p.queue_id());
    if (it == queues_.end()) {
      ++stats.skipped;
      continue;
    }
    QueueSt& q = it->second;
    q.aht_ms = p.aht_ms();
    q.wrap_ms = p.wrap_ms();
    bucket_start_ = std::max(bucket_start_, p.bucket_start_ms());
    q.bucket.offered = p.offered();
    q.bucket.answered = p.answered();
    q.bucket.answered_in_sla = p.answered_in_sla();
    q.bucket.abandoned = p.abandoned();
    q.bucket.overflowed = p.overflowed();
    q.bucket.wait_answered_ms = p.wait_answered_ms();
    q.bucket.handle_ms = p.handle_ms();
    q.bucket.handled = p.handled();
  }
  for (const auto& rec : by_kind["out"]) {
    const st::Interaction& p = rec.out();
    Interaction ix;
    ix.id = p.id();
    ix.owner_epoch = p.owner_epoch();
    ix.replica = p.replica();
    ix.entry_id = p.entry_id();
    ix.past_entries = std::set<std::string>(p.past_entries().begin(), p.past_entries().end());
    for (const auto& [seq, v] : p.snaps()) {
      if (!ix.snaps[seq].ParseFromString(v)) ix.snaps.erase(seq);
    }
    for (const auto& [seq, q] : p.seq_queue()) ix.seq_queue[seq] = q;
    ix.max_enqueue_seq = p.max_enqueue_seq();
    ix.next_seq = p.next_seq();
    ix.acked = p.acked();
    for (const auto& m : p.unacked()) {
      OutIe o{m.seq(), 0, {}, now};
      if (!o.msg.ParseFromString(m.msg())) continue;
      o.msg.set_leader_epoch(epochs_->leader_epoch);
      ix.unacked.push_back(std::move(o));
    }
    ix.reservations = std::set<std::string>(p.reservations().begin(), p.reservations().end());
    ix.last_activity = now;  // a new leader's prune clock starts now
    ix.owner_lost = p.owner_lost();
    ix.callback_id = p.callback_id();
    hold_ix(ix.id);
    interactions_[ix.id] = std::move(ix);
  }
  for (const auto& rec : by_kind["entry"]) {
    const st::Entry& p = rec.entry();
    if (!queues_.contains(p.queue_id()) || !interactions_.contains(p.interaction_id())) {
      ++stats.skipped;
      continue;
    }
    Entry e;
    e.id = p.id();
    e.interaction = p.interaction_id();
    e.queue = p.queue_id();
    e.first_queue = p.first_queue_id();
    e.enqueue_seq = p.enqueue_seq();
    e.priority = p.priority();
    for (const auto& r : p.reqs()) e.reqs.push_back(SkillReq{r.skill_id(), r.min_level()});
    e.sig = sig_of(e.reqs);
    e.first_queued_at = p.first_queued_at_ms();
    e.ordering_at = p.ordering_at_ms();
    e.queue_entered_at = p.queue_entered_at_ms();
    e.created_at = p.created_at_ms();
    e.original_ordering_at = p.original_ordering_at_ms();
    e.order_seq = p.order_seq();
    e.state = static_cast<rv1::EntryState>(p.state());
    e.escalated = p.escalated();
    e.overflow_due = p.overflow_due();
    e.offer_id = p.offer_id();
    e.reservation_id = p.reservation_id();
    for (const auto& [a, until] : p.skips()) e.skips[a] = until;
    e.delay_passed = p.delay_passed();
    if (!e.target.ParseFromString(p.target())) {
      ++stats.skipped;
      continue;
    }
    e.direct_agent = p.direct_agent();
    e.consult_from = p.consult_from();
    e.head = p.head();
    e.callback_id = p.callback_id();
    e.converting = p.converting();
    e.callback_offered = p.callback_offered();
    e.vip = p.vip();
    order_seq_ = std::max(order_seq_, e.order_seq);
    entries_[e.id] = std::move(e);
  }
  for (const auto& rec : by_kind["offer"]) {
    const st::Offer& p = rec.offer();
    if (!agents_.contains(p.agent_id()) || !entries_.contains(p.entry_id())) {
      ++stats.skipped;
      continue;
    }
    offers_[p.id()] = Offer{p.id(), p.agent_id(), p.entry_id(), p.expires_at_ms(), static_cast<Tier>(p.tier())};
  }
  for (const auto& rec : by_kind["res"]) {
    const st::Reservation& p = rec.res();
    if (!agents_.contains(p.agent_id())) {
      ++stats.skipped;
      continue;
    }
    Reservation r;
    r.id = p.id();
    r.interaction = p.interaction_id();
    r.entry_id = p.entry_id();
    r.agent = p.agent_id();
    r.contact = p.contact();
    r.queue = p.queue_id();
    r.deadline_ms = p.deadline_ms();
    r.st = static_cast<ResSt>(p.st());
    r.outcome = static_cast<rv1::Outcome>(p.outcome());
    r.connected_once = p.connected_once();
    r.connected_at = p.connected_at_ms();
    r.wrap_ends = p.wrap_ends_ms();
    r.ended_at = p.ended_at_ms();
    r.pause_since = p.pause_since_ms();
    r.consult = p.consult();
    r.direct = p.direct();
    r.callback_id = p.callback_id();
    r.campaign = p.campaign();
    reservations_[r.id] = std::move(r);
  }
  for (const auto& rec : by_kind["cmd"]) {
    const st::Command& p = rec.cmd();
    Cmd c;
    c.fingerprint = p.fingerprint();
    c.at = p.at_ms();
    if (p.has_error()) c.error = Error{static_cast<Code>(p.error_code()), p.error_parity(), p.error_message()};
    c.response = p.response();
    commands_[p.command_id()] = std::move(c);
  }
  for (const auto& rec : by_kind["cb"]) {
    const st::Callback& p = rec.cb();
    CallbackRecord c;
    c.tenant = id_;
    c.callback_id = p.callback_id();
    c.original_interaction_id = p.original_interaction_id();
    c.entry_id = p.entry_id();
    c.queue_id = p.queue_id();
    c.priority = p.priority();
    for (const auto& r : p.reqs()) c.reqs.push_back(SkillReq{r.skill_id(), r.min_level()});
    c.first_queued_at = p.first_queued_at_ms();
    c.ordering_at = p.ordering_at_ms();
    c.state = p.state();
    c.attempt = p.attempt();
    c.attempt_interaction_id = p.attempt_interaction_id();
    c.reservation_id = p.reservation_id();
    c.agent_id = p.agent_id();
    c.outcome = p.outcome();
    c.due_at = p.due_at_ms();
    c.created_at = p.created_at_ms();
    c.updated_at = p.updated_at_ms();
    callbacks_[c.callback_id] = std::move(c);
  }
  // Derived state: indices, counts, the callback map, then timers.
  for (auto& [qid, q] : queues_) {
    q.avail.clear();
    q.waiting.clear();
    q.delayed.clear();
    q.logged_in = 0;
  }
  ready_idle_.clear();
  escalated_.clear();
  skip_entries_.clear();
  for (auto& [id, a] : agents_) {
    a.counted = false;
    a.avail_key.reset();
    a.directs.clear();
  }
  ++agents_version_;
  for (auto& [id, a] : agents_) refresh_agent(a);
  for (auto& [id, e] : entries_) {
    refresh_entry(e);
    if (!e.skips.empty()) skip_entries_.insert(id);
  }
  std::set<std::string> converting_entries;
  for (const auto& [id, e] : entries_) {
    if (e.converting) converting_entries.insert(id);
  }
  callback_by_entry_.clear();
  for (const auto& [id, c] : callbacks_) {
    if (!converting_entries.contains(c.entry_id)) callback_by_entry_[c.entry_id] = id;
  }
  reconciled_ = callbacks_.empty() && converting_entries.empty();
  rearm(now);
  for (const auto& [id, e] : entries_) {
    if (entry_available(e)) todo_entries_.push_back(id);
  }
  for (const auto& [idle, id] : ready_idle_) todo_agents_.push_back(id);
  track_ = tracking;
  if (restamp) {
    written_.clear();
    touch_all();  // a tenant move: everything again, under the new epoch
  } else {
    // What re.state holds is what was just restored: the next collect writes only new changes.
    written_.clear();
    dirty_ = Dirty{};
    const bool t = track_;
    track_ = true;
    touch_all();
    Dirty all = std::move(dirty_);
    dirty_ = Dirty{};
    track_ = t;
    auto seed = [&](const char* kind, const std::set<std::string>& ids) {
      for (const auto& id : ids) {
        st::Record rec;
        if (encode(kind, id, &rec)) written_[state_key(kind, id)] = hash_of(det(rec));
      }
    };
    if (all.plan) seed("plan", {"routing"});
    if (all.opt) seed("opt", {"staffing"});
    seed("pin", all.pin);
    seed("agent", all.agent);
    seed("queue", all.queue);
    seed("out", all.out);
    seed("entry", all.entry);
    seed("offer", all.offer);
    seed("res", all.res);
    seed("cmd", all.cmd);
    seed("cb", all.cb);
    seed("camp", all.camp);
  }
  settle(now);
  return stats;
}

// Every timer from its absolute deadline. Each timer's handler checks its condition again, so one
// armed twice or for something already done changes nothing; one already due fires at once.
void Tenant::rearm(int64_t now) {
  for (const auto& [id, o] : offers_) timers_->at(o.expires_at, TimerRef{id_, TimerKind::kOfferExpiry, id});
  for (const auto& [id, r] : reservations_) {
    if (r.st == ResSt::kAssigned) {
      const int64_t due = r.deadline_ms + (r.callback_id.empty() ? kAssignGraceMs : kCallbackGraceMs);
      timers_->at(due, TimerRef{id_, TimerKind::kAssignDeadline, id});
    } else if (r.st == ResSt::kWrapUp) {
      timers_->at(r.wrap_ends, TimerRef{id_, TimerKind::kWrapUpLimit, id});
    }
  }
  for (const auto& [id, e] : entries_) {
    auto qit = queues_.find(e.queue);
    if (qit == queues_.end()) continue;
    const QueueSt& q = qit->second;
    if (e.direct_agent.empty()) {
      if (!e.escalated) timers_->at(e.queue_entered_at + q.escalate_after_ms, TimerRef{id_, TimerKind::kEscalate, id});
      if (!e.delay_passed) {
        timers_->at(e.queue_entered_at + q.secondary_delay_ms, TimerRef{id_, TimerKind::kSecondaryDelay, id});
      }
    }
    if (e.callback_id.empty()) timers_->at(e.queue_entered_at + q.cfg.max_wait_ms, TimerRef{id_, TimerKind::kMaxWait, id});
    for (const auto& [agent, until] : e.skips) timers_->at(until, TimerRef{id_, TimerKind::kSkipExpiry, id});
  }
  for (const auto& [id, a] : agents_) {
    if (!a.attached && a.detached_at > 0) timers_->at(a.detached_at + cfg_.grace_ms, TimerRef{id_, TimerKind::kDetachGrace, id});
  }
  for (const auto& [id, p] : pins_) {
    if (p.until_ms > 0) timers_->at(p.until_ms, TimerRef{id_, TimerKind::kPinExpiry, id});
  }
  for (const auto& [id, c] : callbacks_) {
    if (c.state == "scheduled") {
      timers_->at(std::max(c.due_at, now), TimerRef{id_, TimerKind::kCallbackDue, id});
    } else if (c.state == "dispatched") {
      timers_->at(now + kCallbackRedeliverMs, TimerRef{id_, TimerKind::kCallbackRedeliver, id});
    }
  }
}

void Tenant::reconcile_callbacks(const std::vector<CallbackRecord>& rows, int64_t now) {
  std::map<std::string, const CallbackRecord*> by_id;
  for (const auto& r : rows) {
    if (r.tenant == id_) by_id[r.callback_id] = &r;
  }
  std::vector<CallbackRecord> unknown;
  for (const auto& r : rows) {
    if (r.tenant == id_ && !callbacks_.contains(r.callback_id)) unknown.push_back(r);
  }
  // Conversions in flight at the old leader: the row decides (the fence means none lands later).
  std::vector<std::string> converting;
  for (const auto& [id, e] : entries_) {
    if (e.converting) converting.push_back(id);
  }
  for (const auto& eid : converting) {
    std::string cb;
    for (const auto& [id, c] : callbacks_) {
      if (c.entry_id == eid) cb = id;
    }
    auto row = by_id.find(cb);
    if (!cb.empty() && row != by_id.end()) {
      callbacks_[cb].number = row->second->number;
      on_convert_persisted(cb, true, now, [](Result<rv1::ConvertToCallbackResponse>) {});
    } else if (!cb.empty()) {
      on_convert_persisted(cb, false, now, [](Result<rv1::ConvertToCallbackResponse>) {});
    } else {
      Entry& e = entries_.at(eid);
      e.converting = false;
      refresh_entry(e);
      todo_entries_.push_back(eid);
    }
  }
  for (auto& [id, c] : callbacks_) {
    auto row = by_id.find(id);
    if (row != by_id.end()) {
      c.number = row->second->number;
      if (row->second->state != c.state) persist_callback(id);  // Postgres catches up (best effort)
    } else if (c.number.empty() && (c.state == "scheduled" || c.state == "dispatched" || c.state == "claimed")) {
      // re.state knows an open callback Postgres doesn't (or no longer) hold: without its number it
      // is never dialled; flagged for a person.
      flag_callback(c, "number_unknown_after_takeover", now);
      touch_cb(id);
    }
  }
  // Numbers into the RunCallbacks waiting to be (re)sent.
  for (auto& [id, ix] : interactions_) {
    if (ix.callback_id.empty()) continue;
    auto c = callbacks_.find(ix.callback_id);
    for (auto& m : ix.unacked) {
      if (!m.msg.has_run_callback() || !m.msg.run_callback().number().empty()) continue;
      if (c != callbacks_.end() && !c->second.number.empty()) m.msg.mutable_run_callback()->set_number(c->second.number);
    }
  }
  // A dispatch whose RunCallback never left the old leader (the completion wasn't committed): the
  // attempt is written again, then RunCallback goes (the same attempt id: IE drops a repeat).
  for (auto& [id, c] : callbacks_) {
    if (c.state != "dispatched" || c.number.empty()) continue;
    auto ix = interactions_.find(c.attempt_interaction_id);
    auto res = reservations_.find(c.reservation_id);
    if (ix == interactions_.end() || res == reservations_.end() || res->second.st != ResSt::kAssigned) continue;
    bool sent = false;
    for (const auto& m : ix->second.unacked) sent |= m.msg.has_run_callback();
    if (sent || ix->second.acked > 0) continue;
    auto e = entries_.find(res->second.entry_id);
    if (e != entries_.end()) dispatch_callback(e->second, res->second, now);
  }
  if (!unknown.empty()) restore_callbacks(unknown, now);
  reconciled_ = true;
  settle(now);
}

void Tenant::leave(int64_t now) {
  draining_ = true;  // no new offers here: the arriving bin offers
  std::vector<std::string> live;
  for (const auto& [id, o] : offers_) live.push_back(id);
  for (const auto& id : live) revoke_offer(id, av1::REVOKE_REASON_DETACHED, now);
  settle(now);
}

}  // namespace pcr
