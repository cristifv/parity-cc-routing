#include "src/ha/node.h"

#include <chrono>
#include <cstdlib>

#include "src/ops/log.h"
#include "src/state/state.pb.h"

namespace pcr {

namespace st = ::parity::cc_routing::state::v1;

namespace {
int64_t wall_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace

HaNode::HaNode(Options o, Engine* engine, PgStore* store, std::vector<std::pair<std::string, TenantConfig>> tenants,
               Metrics* metrics)
    : opts_(std::move(o)), engine_(engine), store_(store), tenants_(std::move(tenants)), metrics_(metrics) {
  state_ = std::make_unique<KafkaState>(
      KafkaState::Options{opts_.brokers, opts_.topic, opts_.bin},
      [this] { return coord_ && coord_->may_lead(); }, [this](const std::string& why) { step_down(why); });
  Coordinator::Hooks hooks;
  hooks.won = [this](uint64_t epoch) { takeover(epoch); };
  hooks.lost = [this](const std::string& why) { step_down(why); };
  hooks.leader_changed = [this](const std::string& who) {
    LogRec("info", "leader.seen", "the bin's leader").str("bin", opts_.bin).attr("leader", who.empty() ? std::string("(none)") : who);
  };
  hooks.tenant_changed = [this](const std::string& tenant, const Coordinator::TenantAt& at, bool deleted) {
    {
      std::lock_guard lk(mu_);
      moves_.push_back(MoveEvent{tenant, at, deleted});
    }
    cv_.notify_one();
  };
  coord_ = std::make_unique<Coordinator>(
      Coordinator::Options{opts_.etcd, opts_.bin, opts_.advertise, opts_.ttl_s, 1000, 1500}, std::move(hooks));
  if (metrics_) {
    metrics_->computed("parity_cc_routing_leader", "gauge", "1 while this instance leads its bin and serves",
                       [this] { return serving_.load() ? 1.0 : 0.0; });
    metrics_->computed("parity_cc_routing_leader_epoch", "gauge", "The leader key's create revision (0 on a standby)",
                       [this] { return serving_.load() ? static_cast<double>(coord_->leader_epoch()) : 0.0; });
    metrics_->computed("parity_cc_routing_takeover_seconds", "gauge", "The last takeover: elected to serving",
                       [this] { return takeover_ms_.load() / 1000.0; });
    metrics_->computed("parity_cc_routing_lease_left_seconds", "gauge", "Time left before the step-down deadline",
                       [this] { return coord_->lease_left_ms() / 1000.0; });
    metrics_->computed("parity_cc_routing_state_commits_total", "counter", "re.state transactions committed",
                       [this] { return static_cast<double>(state_->stats().commits); });
    metrics_->computed("parity_cc_routing_state_records_total", "counter", "re.state records committed",
                       [this] { return static_cast<double>(state_->stats().records); });
    metrics_->computed("parity_cc_routing_state_commit_max_seconds", "gauge", "Longest re.state commit",
                       [this] { return state_->stats().commit_ms_max / 1000.0; });
    metrics_->computed("parity_cc_routing_state_backlog", "gauge", "re.state records submitted, not yet committed",
                       [this] { return static_cast<double>(state_->backlog()); });
    metrics_->computed("parity_cc_routing_state_consumed_total", "counter", "re.state records the follower read",
                       [this] { return static_cast<double>(state_->consumed()); });
    metrics_->computed("parity_cc_routing_tenant_moves_total", "counter", "Tenants that arrived or left",
                       [this] { return static_cast<double>(moves_in_.load() + moves_out_.load()); });
    metrics_->computed("parity_cc_routing_state_commit_p99_seconds", "gauge",
                       "re.state commit p99 (upper bound of its log2 bucket)", [this] {
                         const auto s = state_->stats();
                         uint64_t total = 0;
                         for (uint64_t c : s.commit_ms_buckets) total += c;
                         uint64_t seen = 0;
                         for (int b = 0; b < 16; ++b) {
                           seen += s.commit_ms_buckets[b];
                           if (total > 0 && seen * 100 >= total * 99) return static_cast<double>(1ull << b) / 1000.0;
                         }
                         return 0.0;
                       });
  }
}

HaNode::~HaNode() {
  {
    std::lock_guard lk(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  if (mover_.joinable()) mover_.join();
}

bool HaNode::start(std::string* err) {
  if (!state_->start_follower(err)) return false;
  mover_ = std::thread([this] { mover_loop(); });
  return coord_->start(err);
}

void HaNode::step_down(const std::string& why) {
  serving_ = false;
  LogRec("error", "leader.lost", "leadership lost: exiting so this instance restarts as a standby").str("bin", opts_.bin).error("not_leader", why);
  log_flush();
  std::_Exit(3);
}

const TenantConfig* HaNode::config_of(const std::string& tenant) const {
  for (const auto& [bin, cfg] : tenants_) {
    if (cfg.id == tenant) return &cfg;
  }
  return nullptr;
}

std::string HaNode::static_bin(const std::string& tenant) const {
  for (const auto& [bin, cfg] : tenants_) {
    if (cfg.id == tenant) return bin.empty() ? opts_.bin : bin;
  }
  return {};
}

void HaNode::takeover(uint64_t epoch) {
  const auto t0 = std::chrono::steady_clock::now();
  std::string err;
  if (!state_->become_leader(epoch, 15'000, &err)) {
    step_down("takeover: " + err);
    return;
  }
  const int64_t fenced_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  uint64_t inc = 1;
  const std::string meta_key = "~bin/" + opts_.bin + "/meta";
  if (auto m = state_->get(meta_key)) {
    st::Record r;
    if (r.ParseFromString(*m) && r.has_meta()) inc = r.meta().incarnation() + 1;
  } else if (!opts_.first_start) {
    LogRec("error", "startup.refused", "re.state holds no incarnation for this bin: start with --first-start=yes only for a new deployment")
        .str("bin", opts_.bin).error("invalid_request");
    log_flush();
    std::_Exit(2);
  }
  engine_->load({}, Epochs{epoch, (inc - 1) << 32});
  {
    st::Record r;
    r.set_schema_version(1);
    r.set_leader_epoch(epoch);
    r.set_written_at_ms(wall_ms());
    r.mutable_meta()->set_bin(opts_.bin);
    r.mutable_meta()->set_incarnation(inc);
    if (!state_->commit_now({StateRec{"", meta_key, r.SerializeAsString()}}, &err)) {
      step_down("takeover: the incarnation: " + err);
      return;
    }
  }
  incarnation_ = inc;
  const auto tp = coord_->tenants();
  size_t records = 0, n = 0;
  for (const auto& [bin, cfg] : tenants_) {
    std::string at = bin.empty() ? opts_.bin : bin;
    uint64_t te = 0;
    if (auto it = tp.find(cfg.id); it != tp.end()) {
      at = it->second.bin;
      te = it->second.epoch;
    }
    if (at != opts_.bin) continue;
    auto recs = state_->records_of(cfg.id);
    records += recs.size();
    // Records of an older epoch: the tenant moved here while this bin had no leader.
    const bool restamp = state_->tenant_epoch_seen(cfg.id) < te;
    engine_->add_tenant(cfg, te, [recs = std::move(recs), restamp](Tenant* t, int64_t now) { t->restore(recs, now, restamp); });
    {
      std::lock_guard lk(mu_);
      owned_[cfg.id] = te;
    }
    if (store_) store_->load_tenant(cfg.id, te, epoch);
    ++n;
  }
  state_->start_commits();
  leading_ = true;
  serving_ = true;
  const int64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  takeover_ms_ = ms;
  LogRec("info", "takeover.done", "takeover done: serving").str("bin", opts_.bin).num("leader_epoch", static_cast<int64_t>(epoch))
      .duration_ms(static_cast<double>(ms)).attr("incarnation", static_cast<int64_t>(inc)).attr("tenants", static_cast<int64_t>(n))
      .attr("records", static_cast<int64_t>(records)).attr("takeover_ms", static_cast<int64_t>(ms))
      .attr("fenced_and_read_ms", static_cast<int64_t>(fenced_ms));
  cv_.notify_all();  // tenant moves that waited for the leadership
}

void HaNode::drain(int64_t timeout_ms) {
  if (!leading_.load()) {
    coord_->stop(false);
    state_->stop();
    return;
  }
  engine_->set_draining(true);  // no new offers (and that is committed like any change)
  serving_ = false;             // new RPCs: not_leader (the standby is about to lead)
  if (!state_->flush(timeout_ms)) LogRec("warn", "state.flush_incomplete", "re.state: not everything was committed before the drain's end").str("bin", opts_.bin);
  coord_->stop(/*resign=*/true);
  state_->stop();
}

bool HaNode::move_tenant(const std::string& tenant, const std::string& bin, uint64_t epoch, std::string* err) {
  return coord_->put("tp/tenants/" + tenant, "{\"bin\":\"" + bin + "\",\"epoch\":" + std::to_string(epoch) + "}", err);
}

void HaNode::mover_loop() {
  for (;;) {
    MoveEvent ev;
    {
      std::unique_lock lk(mu_);
      cv_.wait(lk, [&] { return stop_ || (leading_.load() && !moves_.empty()); });
      if (stop_) return;
      ev = std::move(moves_.front());
      moves_.pop_front();
    }
    if (ev.deleted) {
      LogRec("warn", "tenant.assignment_ignored", "tp/tenants: a tenant's key was deleted (ignored: the last assignment stands)").tenant(ev.tenant);
      continue;
    }
    if (!config_of(ev.tenant)) continue;  // not a tenant this deployment knows
    bool have = false;
    uint64_t held = 0;
    {
      std::lock_guard lk(mu_);
      auto it = owned_.find(ev.tenant);
      have = it != owned_.end();
      if (have) held = it->second;
    }
    if (ev.at.bin == opts_.bin && !have) {
      arrive(ev.tenant, ev.at.epoch);
    } else if (ev.at.bin != opts_.bin && have && ev.at.epoch > held) {
      leave(ev.tenant, ev.at.bin, ev.at.epoch);
    }
  }
}

void HaNode::leave(const std::string& tenant, const std::string& to_bin, uint64_t epoch) {
  const auto t0 = std::chrono::steady_clock::now();
  engine_->remove_tenant(tenant, epoch, opts_.bin, to_bin, [](Tenant* t, int64_t now) { t->leave(now); });
  if (store_) store_->forget_tenant(tenant);
  {
    std::lock_guard lk(mu_);
    owned_.erase(tenant);
  }
  ++moves_out_;
  LogRec("info", "tenant.moved_away", "tenant moved away: handover written").tenant(tenant).str("bin", opts_.bin)
      .duration_ms(static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count()))
      .attr("target_bin", to_bin).attr("tenant_epoch", static_cast<int64_t>(epoch));
}

void HaNode::arrive(const std::string& tenant, uint64_t epoch) {
  const auto t0 = std::chrono::steady_clock::now();
  const std::string marker = tenant + "/handover/" + std::to_string(epoch);
  bool seen = false;
  while (std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(opts_.handover_wait_ms)) {
    if (state_->get(marker)) {
      seen = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  if (!seen) LogRec("warn", "tenant.handover_missing", "tenant arriving without the leaving bin's handover (waited 5 s)").tenant(tenant).str("bin", opts_.bin);
  const TenantConfig* cfg = config_of(tenant);
  auto recs = state_->records_of(tenant);
  const size_t n = recs.size();
  engine_->add_tenant(*cfg, epoch, [recs = std::move(recs)](Tenant* t, int64_t now) { t->restore(recs, now, /*restamp=*/true); });
  {
    std::lock_guard lk(mu_);
    owned_[tenant] = epoch;
  }
  if (store_) store_->load_tenant(tenant, epoch, coord_->leader_epoch());
  ++moves_in_;
  LogRec("info", "tenant.arrived", "tenant arrived").tenant(tenant).str("bin", opts_.bin)
      .duration_ms(static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count()))
      .attr("tenant_epoch", static_cast<int64_t>(epoch)).attr("records", static_cast<int64_t>(n)).attr("handover", seen ? "seen" : "missing");
}

}  // namespace pcr
