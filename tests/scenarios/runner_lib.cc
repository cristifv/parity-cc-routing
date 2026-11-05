// The scenario runner library (PLAN §7.1, round 4): plays parity-contracts' routing-scenario.v1 files
// against the real engine. Every request goes through the real gRPC services (the binary's Server,
// on an ephemeral port) and the test control (manual clock, accelerated time); simulated IE and
// agent clients react to what the engine sends. Reactions read an in-process tap of the stream hub
// (every message the engine handed to a stream, in order, so the run is deterministic); the real
// Watch streams are opened and consumed too. The invariant checker reads the event log (I1-I11).
// CR-055: every invariant counts the checks that actually compared something ("exercised"), I3 is
// reconciled two-sided against the engine's own entries, `already_gone` on an abandon is a
// violation, `expectations.must_occur` names outcomes a variant must provoke, and `--fault=` injects
// a known bug so a run can show which invariant catches it.
#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <future>
#include <cstdio>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>
#include <set>
#include <thread>

#include "parity/agents/v1/agents.grpc.pb.h"
#include "parity/routing/v1/routing.grpc.pb.h"
#include "src/core/engine.h"
#include "src/ops/metrics.h"
#include "src/server/hub.h"
#include "src/server/services.h"
#include "src/server/test_control.h"
#include "src/opt/optimizer.h"
#include "tests/scenarios/runner.h"

using json = nlohmann::json;
using namespace pcr;
using pcr::scenario::Fault;
using pcr::scenario::RunOptions;

namespace {

constexpr int64_t kStart = 1'792'800'000'000;

// ---- distributions and arrivals ----

struct Dist {
  std::string kind = "fixed";
  double mean = 0, cv = 0, s = 0;
  static Dist from(const json& j) {
    Dist d;
    d.kind = j.at("kind");
    if (d.kind == "fixed") d.s = j.at("s");
    if (d.kind != "fixed") d.mean = j.at("mean_s");
    if (d.kind == "lognormal") d.cv = j.at("cv");
    return d;
  }
  double sample(std::mt19937_64& r) const {
    if (kind == "fixed") return s;
    if (kind == "exponential") return std::exponential_distribution<double>(1.0 / mean)(r);
    const double sigma2 = std::log(1 + cv * cv);
    return std::lognormal_distribution<double>(std::log(mean) - sigma2 / 2, std::sqrt(sigma2))(r);
  }
};

struct Arrivals {
  json j;
  double rate(double t) const {  // calls per second
    const std::string k = j.at("kind");
    double per_hour = 0;
    if (k == "piecewise") {
      const auto& v = j.at("per_hour");
      const size_t i = std::min(v.size() - 1, static_cast<size_t>(t / j.at("bucket_s").get<double>()));
      per_hour = v[i];
    } else if (k == "profile") {
      const auto& v = j.at("shape");
      double peak = 0;
      for (const auto& x : v) peak = std::max(peak, x.get<double>());
      const size_t i = std::min(v.size() - 1, static_cast<size_t>(t / j.at("bucket_s").get<double>()));
      per_hour = peak > 0 ? v[i].get<double>() / peak * j.at("scale_per_hour").get<double>() : 0;
    } else if (k == "poisson") {
      const auto& p = j.at("points");
      per_hour = p.back().at("per_hour");
      for (size_t i = 0; i + 1 < p.size(); ++i) {
        const double a = p[i].at("t_s"), b = p[i + 1].at("t_s");
        if (t >= a && t < b) {
          const double x = (t - a) / std::max(1e-9, b - a);
          per_hour = p[i].at("per_hour").get<double>() * (1 - x) + p[i + 1].at("per_hour").get<double>() * x;
          break;
        }
      }
      if (t < p.front().at("t_s").get<double>()) per_hour = p.front().at("per_hour");
    }
    double m = 1;
    const json spikes = j.value("spikes", json::array());
    for (const auto& s : spikes) {
      const double a = s.at("start_s"), d = s.at("duration_s"), mult = s.at("multiplier"), ramp = s.value("ramp_s", 0.0);
      if (t < a || t >= a + d) continue;
      double f = 1;
      if (ramp > 0) f = std::min({1.0, (t - a) / ramp, (a + d - t) / ramp});
      m *= 1 + (mult - 1) * std::max(0.0, f);
    }
    return per_hour * m / 3600.0;
  }
  std::vector<double> times(double duration, std::mt19937_64& r) const {
    std::vector<double> out;
    if (j.at("kind") == "list") {
      for (const auto& x : j.at("at_s")) out.push_back(x);
      return out;
    }
    double lmax = 0;
    for (double t = 0; t < duration; t += 1) lmax = std::max(lmax, rate(t));
    if (lmax <= 0) return out;
    std::exponential_distribution<double> gap(lmax);
    std::uniform_real_distribution<double> u(0, 1);
    for (double t = gap(r); t < duration; t += gap(r)) {
      if (u(r) * lmax <= rate(t)) out.push_back(t);  // thinning
    }
    return out;
  }
};

// ---- the run ----

struct Call {
  std::string tenant, queue, ix, entry, reservation;
  uint32_t priority = 0;
  int64_t arrived = 0, queue_since = 0, connected_at = 0;
  enum St { kWaiting, kAssigned, kConnected, kDone } st = kWaiting;
  std::string end;  // answered, abandoned, overflowed, refused, lost_restart
  std::string first_queue;  // the queue it arrived at (KPIs per queue)
  uint64_t seen_seq = 0;
  bool answered = false;
  // ie_restart (CR-083): the call belonged to an IE process that restarted alone and forgot it.
  bool forgotten = false, cleaned = false, late = false;
  int64_t forgotten_at = 0;
  // CR-100: when IE's view last went back to waiting (a failed or revoked assignment), and whether
  // the IE-side I5 bound has already been reported for this call.
  int64_t back_waiting_at = 0;
  bool i5_ie_reported = false;
};

struct Agent {
  std::string tenant, id, site, session;
  uint64_t gen = 0, last_seq = 0;
  bool logged_in = false, attached = false, away = false;
  std::string offer;
};

struct Tee final : Sink {
  Hub* hub = nullptr;
  std::mutex mu;
  std::vector<std::pair<std::string, rv1::WatchResponse>> ie;
  std::vector<std::pair<std::string, av1::WatchResponse>> gw;
  // Only what reached a stream (a message held for a newer stream's replay comes again).
  void to_ie(const std::string& r, uint64_t g, const rv1::WatchResponse& m) override {
    std::lock_guard lk(mu);
    if (hub->push_ie(r, g, m)) ie.emplace_back(r, m);
  }
  void to_gw(const std::string& r, uint64_t g, const av1::WatchResponse& m) override {
    std::lock_guard lk(mu);
    if (hub->push_gw(r, g, m)) gw.emplace_back(r, m);
  }
  bool ie_live(const std::string& r) const override { return hub->ie_live(r); }
};

template <class Msg>
struct Drain {
  grpc::ClientContext ctx;
  std::unique_ptr<grpc::ClientReader<Msg>> reader;
  std::thread th;
  void start() {
    th = std::thread([this] {
      Msg m;
      while (reader->Read(&m)) {
      }
    });
  }
  ~Drain() {
    ctx.TryCancel();
    if (th.joinable()) th.join();
  }
};

class Run {
 public:
  Run(const json& sc, const RunOptions& opt) : sc_(sc), rng_(sc.at("seed").get<uint64_t>()), fault_(opt.fault), opt_(opt) {
    // CR-049: a deterministic run (the invariant search, regression replays) has one shard and
    // counter-based ids, so the same scenario gives the same message order, ids and draws.
    if (opt_.deterministic) set_deterministic_ids(sc.at("seed").get<uint64_t>() * 2 + 1);
    Engine::Limits lim;
    lim.model_ewt = opt_.model_ewt;
    lim.position_throttle = opt_.position_throttle;
    Engine::ModelFeed feed;
    if (opt_.model_ewt) {
      optimizer_ = std::make_unique<Optimizer>(Optimizer::Options{});
      feed = [o = optimizer_.get()](std::shared_ptr<const ModelSnapshot> s) { o->submit(std::move(s)); };
    }
    engine_ = std::make_unique<Engine>(&clock_, &tee_, opt_.deterministic ? 1 : opt_.shards, lim, feed);
    if (optimizer_) optimizer_->attach(engine_.get());
    hub_ = std::make_unique<Hub>(&engine_->epochs());
    tee_.hub = hub_.get();
    server_ = std::make_unique<Server>(ServerOptions{"127.0.0.1:0", sc.at("bin"), nullptr, nullptr}, &clock_, engine_.get(), hub_.get(), &metrics_);
    if (!server_->start()) throw std::runtime_error("server");
    control_ = std::make_unique<TestControl>(engine_.get(), hub_.get(), &clock_, std::vector<TenantConfig>{}, Epochs{1, 0}, sc.at("bin"));
    auto ch = grpc::CreateChannel("127.0.0.1:" + std::to_string(server_->port()), grpc::InsecureChannelCredentials());
    routing_ = rv1::RoutingService::NewStub(ch);
    agent_stub_ = av1::AgentService::NewStub(ch);
  }

  ~Run() {
    // CR-099: the optimizer goes before the engine (main.cc's order).
    if (optimizer_ && engine_) {
      engine_->stop_model_feed();
      optimizer_->attach(nullptr);
      optimizer_.reset();
    }
    if (opt_.deterministic) set_deterministic_ids(0);
  }

  json play() {
    seed();
    const int64_t end = sc_.at("duration_s").get<int64_t>() * 1000;
    plan();
    if (fault_.at_ms >= 0) {
      at(fault_.at_ms, [this] {
        bool any = false;
        for (const auto& t : sc_.at("tenants")) {
          std::promise<bool> done;
          auto f = done.get_future();
          engine_->post(t.at("id").get<std::string>(), [&](Tenant* tn, int64_t) { done.set_value(tn && tn->test_fault(fault_.kind)); }, false);
          any |= f.get();
        }
        fault_applied_ = any;
      });
    }
    open_ie();
    open_gw();
    int64_t t = 0;
    for (;;) {
      int64_t next = actions_.empty() ? end : std::min(end, actions_.begin()->first);
      next = std::min(next, t + 1000);
      if (next <= t && !actions_.empty() && actions_.begin()->first <= t) next = t;
      advance(next);
      t = next;
      while (!actions_.empty() && actions_.begin()->first <= t) {
        auto f = std::move(actions_.begin()->second);
        actions_.erase(actions_.begin());
        f();
        react();
      }
      check_step();
      if (t >= end) break;
    }
    return report();
  }

 private:
  int64_t now() const { return clock_.now_ms() - kStart; }
  void violate(const std::string& inv, const std::string& what) {
    if (violations_.size() < 50) violations_.push_back(inv + ": " + what);
    ++violation_count_;
    ++violated_[inv];
  }
  // A check that compared something (CR-055: per-invariant evidence that it isn't vacuous).
  void ex(const char* inv, uint64_t n = 1) { exercised_[inv] += n; }
  // An outcome a variant may require (expectations.must_occur).
  void occur(const std::string& what) { ++occurred_[what]; }
  void at(int64_t t_ms, std::function<void()> f) { actions_.emplace(std::max(t_ms, now()), std::move(f)); }
  double uniform() { return std::uniform_real_distribution<double>(0, 1)(rng_); }

  void post(const std::string& path, const json& body) {
    HttpReply r = control_->handle("POST", path, body.dump());
    if (r.status != 200) throw std::runtime_error(path + ": " + r.body);
  }
  void advance(int64_t t) {
    if (t > now()) post("/_test/v1/advance", {{"ms", t - now()}});
    react();
  }

  // The seed.v1 document from the scenario's tenants (agents of mass_login groups included).
  void seed() {
    json tenants = json::array();
    for (const auto& t : sc_.at("tenants")) {
      json tj = {{"id", t.at("id")}, {"queues", json::array()}, {"agents", json::array()}};
      if (t.contains("reject_skip_s")) tj["reject_skip_s"] = t.at("reject_skip_s");
      if (t.contains("owner_loss_s")) tj["owner_loss_s"] = t.at("owner_loss_s");
      owner_loss_ms_[t.at("id").get<std::string>()] = t.value("owner_loss_s", 60) * 1000;
      for (const auto& q : t.at("queues")) {
        json qj = q;
        for (const char* k : {"arrivals", "handle_time", "patience", "priority_mix"}) qj.erase(k);
        tj["queues"].push_back(qj);
        QueueSim qs;
        qs.max_wait_ms = q.at("max_wait_s").get<int64_t>() * 1000;
        qs.sla_ms = q.value("sla_s", 20) * 1000;
        qs.handle = Dist::from(q.at("handle_time"));
        qs.patience = Dist::from(q.at("patience"));
        qs.wrap_limit_ms = q.value("wrap_up_limit_s", 60) * 1000;
        qs.max_entries = q.value("max_entries", 1000);
        qs.mix = q.value("priority_mix", json::array());
        queues_[t.at("id").get<std::string>() + "/" + q.at("id").get<std::string>()] = qs;
      }
      auto add = [&](const json& g) {
        for (int i = 0; i < g.at("count").get<int>(); ++i) {
          std::string id;
          const auto& ids = g.value("ids", json::array());
          if (i < static_cast<int>(ids.size())) {
            id = ids[i];
          } else {
            id = g.value("id_prefix", std::string("agent")) + "-" + std::to_string(i + 1);
          }
          json aj = {{"id", id}, {"queues", g.at("queues")}};
          if (g.contains("skills")) aj["skills"] = g.at("skills");
          tj["agents"].push_back(aj);
          Agent a;
          a.tenant = t.at("id");
          a.id = id;
          a.site = g.value("site", "");
          agents_[a.tenant + "/" + id] = a;
          groups_[&g].push_back(a.tenant + "/" + id);
        }
      };
      for (const auto& g : t.at("agents")) add(g);
      if (sc_.contains("events")) {
        for (const auto& e : sc_.at("events")) {
          if (e.at("kind") == "mass_login" && e.at("tenant") == t.at("id")) add(e.at("group"));
        }
      }
      tenants.push_back(tj);
      behaviour_[t.at("id")] = t.value("behaviour", json::object());
    }
    post("/_test/v1/seed", {{"kind", "seed"}, {"start_ms", kStart}, {"leader_epoch", 1},
                             {"seed", {{"schema_version", 1}, {"tenants", tenants}}}});
  }

  struct QueueSim {
    int64_t max_wait_ms = 0, sla_ms = 0, wrap_limit_ms = 0;
    int max_entries = 0;
    Dist handle, patience;
    json mix;
  };

  void plan() {
    const double dur = sc_.at("duration_s");
    int n = 0;
    for (const auto& t : sc_.at("tenants")) {
      for (const auto& q : t.at("queues")) {
        Arrivals a{q.at("arrivals")};
        for (double s : a.times(dur, rng_)) {
          const std::string tenant = t.at("id"), queue = q.at("id");
          at(static_cast<int64_t>(s * 1000), [this, tenant, queue] { arrive(tenant, queue); });
          ++n;
        }
      }
      for (const auto& g : t.at("agents")) {
        const auto shifts = g.value("shifts", json::array({{{"login_s", 0}, {"logout_s", 1e9}}}));
        for (const auto& key : groups_[&g]) {
          for (const auto& s : shifts) {
            at(static_cast<int64_t>(s.at("login_s").get<double>() * 1000), [this, key] { login(key, true); });
            const double out = s.at("logout_s");
            if (out < dur) at(static_cast<int64_t>(out * 1000), [this, key] { logout(key); });
          }
        }
      }
    }
    const json events = sc_.value("events", json::array());
    for (const auto& e : events) {
      const int64_t t = static_cast<int64_t>(e.at("at_s").get<double>() * 1000);
      at(t, [this, e] { event(e); });
    }
    planned_ = n;
  }

  // ---- IE side ----

  rv1::InteractionOwner owner(const Call& c) {
    rv1::InteractionOwner o;
    o.set_tenant_id(c.tenant);
    o.set_interaction_id(c.ix);
    o.set_owner_epoch(owner_epoch_);
    o.set_replica_id(replica_);
    o.set_acked_delivery_seq(c.seen_seq);
    return o;
  }
  template <class F>
  grpc::Status rpc(const std::string& tenant, F f, grpc::ClientContext* ctx = nullptr, bool timed = false) {
    grpc::ClientContext local;
    grpc::ClientContext& c = ctx ? *ctx : local;
    c.AddMetadata("tenant-id", tenant);
    c.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    const auto a = std::chrono::steady_clock::now();
    grpc::Status st = f(c);
    if (timed) {
      lat_.push_back(std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - a).count());
    }
    return st;
  }

  std::string arrive(const std::string& tenant, const std::string& queue, bool send = true) {
    Call c;
    c.tenant = tenant;
    c.queue = queue;
    c.ix = new_uuid_v7(clock_.now_ms());
    c.arrived = now();
    c.queue_since = now();
    const QueueSim& qs = queues_.at(tenant + "/" + queue);
    double u = uniform();
    for (const auto& m : qs.mix) {
      if (u < m.at("share").get<double>()) {
        c.priority = m.at("priority");
        break;
      }
      u -= m.at("share").get<double>();
    }
    ++arrived_[tenant];
    ++q_arrived_[tenant + "/" + queue];
    c.first_queue = queue;
    open_.insert(c.ix);
    calls_[c.ix] = c;
    if (send) enqueue(c.ix, 0);
    const int64_t patience = static_cast<int64_t>(qs.patience.sample(rng_) * 1000);
    const std::string ix = c.ix;
    at(now() + patience, [this, ix] { abandon(ix); });
    return ix;
  }

  rv1::EnqueueRequest enqueue_request(const Call& c) {
    rv1::EnqueueRequest r;
    *r.mutable_owner() = owner(c);
    r.set_enqueue_seq(1);
    r.mutable_target()->set_queue_id(c.queue);
    r.set_priority(c.priority);
    r.set_first_queued_at_ms(kStart + c.arrived);
    r.set_ordering_at_ms(kStart + c.arrived);
    r.set_channel(rv1::CHANNEL_VOICE);
    r.set_reason(rv1::ENQUEUE_REASON_NEW);
    return r;
  }

  // An Enqueue's answer, wherever it came from (a plain call or a flood).
  void enqueued(const std::string& ix, int tries, const grpc::Status& st, const grpc::ClientContext& ctx,
                const rv1::EnqueueRequest& r, const rv1::EnqueueResponse& out) {
    Call& c = calls_.at(ix);
    if (st.ok()) {
      c.entry = out.entry().entry_id();
      ex("I11");
      if (out.entry().ordering_at_ms() != r.ordering_at_ms()) violate("I11", "ordering_at_ms changed by Enqueue");
      if (c.st == Call::kDone) {
        // The caller hung up while the request was in flight: IE dequeues what it got.
        dequeue(c, rv1::DEQUEUE_REASON_ABANDONED);
      }
      return;
    }
    if (st.error_code() == grpc::StatusCode::UNAVAILABLE && tries < 600) {
      ++unavailable_;
      occur("request_refused_unavailable");
      if (c.st != Call::kDone) at(now() + 100, [this, ix, tries] { enqueue(ix, tries + 1); });
      return;
    }
    if (st.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED) {
      auto t = ctx.GetServerTrailingMetadata();
      auto it = t.find("parity-error");
      if (it != t.end() && std::string(it->second.data(), it->second.size()) == "queue_full") occur("queue_full");
      finish(c, "refused");
      return;
    }
    violate("I3", "Enqueue failed: " + std::to_string(st.error_code()) + " " + st.error_message());
    finish(c, "refused");
  }

  void dequeue(Call& c, rv1::DequeueReason why) {
    rv1::DequeueRequest d;
    *d.mutable_owner() = owner(c);
    d.set_entry_id(c.entry);
    d.set_reason(why);
    rv1::DequeueResponse out;
    auto st = rpc(c.tenant, [&](grpc::ClientContext& cc) { return routing_->Dequeue(&cc, d, &out); });
    ex("I3");
    if (!st.ok()) {
      violate("I3", "Dequeue failed: " + st.error_message());
    } else if (out.already_gone()) {
      // Nothing IE did removed this entry (no CALLER_GONE, no CONNECTED, no overflow
      // handled): the engine dropped a caller.
      violate("I3", c.tenant + ": Dequeue answered already_gone for an entry IE still held");
    }
  }

  void enqueue(const std::string& ix, int tries) {
    Call& c = calls_.at(ix);
    if (c.st == Call::kDone) return;  // hung up before the retry
    const rv1::EnqueueRequest r = enqueue_request(c);
    rv1::EnqueueResponse out;
    grpc::ClientContext ctx;
    auto st = rpc(c.tenant, [&](grpc::ClientContext& cc) { return routing_->Enqueue(&cc, r, &out); }, &ctx, true);
    enqueued(ix, tries, st, ctx, r, out);
  }

  void finish(Call& c, const std::string& how) {
    if (c.st == Call::kDone) return;
    c.st = Call::kDone;
    c.end = how;
    ++ends_[c.tenant][how];
    ++q_ends_[c.tenant + "/" + c.first_queue][how];
    open_.erase(c.ix);
  }

  void abandon(const std::string& ix) {
    Call& c = calls_.at(ix);
    if (c.st == Call::kDone) return;
    if (c.st == Call::kConnected) {
      call_ended(ix);  // the caller hangs up on the agent
      return;
    }
    if (!c.entry.empty()) dequeue(c, rv1::DEQUEUE_REASON_ABANDONED);
    finish(c, "abandoned");
  }

  void outcome(const std::string& ix, rv1::Outcome o) {
    Call& c = calls_.at(ix);
    if (c.reservation.empty() || c.forgotten) return;
    rv1::ReportOutcomeRequest r;
    *r.mutable_owner() = owner(c);
    r.set_reservation_id(c.reservation);
    r.set_outcome(o);
    rv1::ReportOutcomeResponse out;
    auto st = rpc(c.tenant, [&](grpc::ClientContext& cc) { return routing_->ReportOutcome(&cc, r, &out); });
    if (!st.ok()) {
      if (c.st != Call::kDone) violate("I11", "ReportOutcome failed: " + st.error_message());
      return;
    }
    auto& rec = recorded_[c.reservation];
    if (rec != rv1::OUTCOME_UNSPECIFIED) ex("I11");
    if (rec != rv1::OUTCOME_UNSPECIFIED && rec != out.recorded()) violate("I11", "the first outcome didn't win");
    rec = out.recorded();
    if (c.st == Call::kDone) return;
    if (out.recorded() == rv1::OUTCOME_CONNECTED && o == rv1::OUTCOME_CONNECTED) {
      c.st = Call::kConnected;
      c.connected_at = now();
      c.answered = true;
      ++answered_by_[c.tenant];
      const QueueSim& qs = queues_.at(c.tenant + "/" + c.queue);
      wait_ms_.push_back({c.tenant, now() - c.arrived, now() - c.arrived <= qs.sla_ms, c.first_queue});
      at(now() + static_cast<int64_t>(qs.handle.sample(rng_) * 1000), [this, ix] { call_ended(ix); });
    } else if (c.st == Call::kAssigned) {
      c.st = Call::kWaiting;
      c.back_waiting_at = now();
      c.reservation.clear();
    }
  }

  void call_ended(const std::string& ix) {
    Call& c = calls_.at(ix);
    if (c.st != Call::kConnected) return;
    rv1::CallEndedRequest r;
    *r.mutable_owner() = owner(c);
    r.set_agent_id(agent_of_[c.reservation]);
    r.set_reservation_id(c.reservation);
    r.set_ended_at_ms(clock_.now_ms());
    rv1::CallEndedResponse out;
    auto st = rpc(c.tenant, [&](grpc::ClientContext& cc) { return routing_->CallEnded(&cc, r, &out); });
    if (!st.ok()) violate("I3", "CallEnded failed: " + st.error_message());
    finish(c, "answered");
  }

  void on_ie(const rv1::WatchResponse& m) {
    if (m.leader_epoch() < ie_epoch_) violate("I7", "a routing message from an older leader_epoch");
    if (m.has_position()) {
      ex("I7");
      ++position_updates_;
    }
    ie_epoch_ = std::max(ie_epoch_, m.leader_epoch());
    if (m.has_heartbeat() || m.has_position()) return;
    std::string ix;
    if (m.has_assigned()) ix = m.assigned().target().interaction_id();
    if (m.has_overflow()) ix = m.overflow().target().interaction_id();
    if (m.has_revoke()) ix = m.revoke().target().interaction_id();
    if (m.has_wrap_up_ended()) ix = m.wrap_up_ended().target().interaction_id();
    auto it = calls_.find(ix);
    if (it == calls_.end()) return;
    Call& c = it->second;
    if (c.forgotten) return;  // the restarted IE doesn't know it: no reaction, no ack (CR-083)
    const std::string tenant = m.has_assigned() ? m.assigned().target().tenant_id()
                               : m.has_overflow() ? m.overflow().target().tenant_id()
                               : m.has_revoke() ? m.revoke().target().tenant_id()
                                                : m.wrap_up_ended().target().tenant_id();
    ex("I2");
    if (tenant != c.tenant) violate("I2", "a message about " + c.tenant + "'s call names " + tenant);
    auto& seen = ie_seq_[ix];
    ex("I7");
    if (m.delivery_seq() <= seen.first) {
      if (m.delivery_seq() == seen.first && m.SerializeAsString() != seen.second) {
        violate("I7", "delivery_seq reused for another message");
      }
      return;  // a replay
    }
    if (m.delivery_seq() != seen.first + 1 && seen.first != 0 && m.leader_epoch() == last_epoch_of_[ix]) {
      violate("I7", "delivery_seq skipped");
    }
    seen = {m.delivery_seq(), m.SerializeAsString()};
    last_epoch_of_[ix] = m.leader_epoch();
    c.seen_seq = m.delivery_seq();
    acks_[ix] = c.tenant;
    if (m.has_assigned()) {
      const auto& a = m.assigned();
      if (!agents_.contains(c.tenant + "/" + a.agent_id())) violate("I2", "Assigned names an agent outside " + c.tenant);
      agent_of_[a.reservation_id()] = a.agent_id();
      if (c.st == Call::kDone) {
        // abandon vs assign: IE answers a late Assigned with CALLER_GONE.
        c.reservation = a.reservation_id();
        outcome(ix, rv1::OUTCOME_CALLER_GONE);
        return;
      }
      c.st = Call::kAssigned;
      c.reservation = a.reservation_id();
      const json& b = behaviour_[c.tenant];
      const double u = uniform();
      rv1::Outcome o = rv1::OUTCOME_CONNECTED;
      int64_t delay = 1000;
      if (u < b.value("p_leg_failed", 0.0)) {
        o = rv1::OUTCOME_AGENT_LEG_FAILED;
      } else if (u < b.value("p_leg_failed", 0.0) + b.value("p_no_answer", 0.0)) {
        o = rv1::OUTCOME_AGENT_NO_ANSWER;
        delay = 10'000;
      }
      at(now() + delay, [this, ix, o] { outcome(ix, o); });
    } else if (m.has_overflow()) {
      ex("I5");
      const auto act = m.overflow().action();
      occur(act == rv1::OVERFLOW_ACTION_QUEUE ? "overflow_queue" : act == rv1::OVERFLOW_ACTION_FLOW_STEP ? "overflow_flow_step" : "overflow_disconnect");
      if (c.st == Call::kDone) return;
      if (act == rv1::OVERFLOW_ACTION_QUEUE) {
        c.queue = m.overflow().queue_id();
        c.queue_since = now();
        if (c.st == Call::kAssigned) {
          c.st = Call::kWaiting;
          c.back_waiting_at = now();
        }
      } else {
        c.entry = m.overflow().entry_id();
        dequeue(c, rv1::DEQUEUE_REASON_OVERFLOW);
        finish(c, "overflowed");
      }
    } else if (m.has_revoke()) {
      if (m.revoke().reason() == "expired") occur("assignment_expired");
      if (c.st == Call::kAssigned) {
        c.st = Call::kWaiting;
        c.back_waiting_at = now();
        c.reservation.clear();
      }
    }
  }

  // ---- agent side ----

  av1::SessionRef ref(const Agent& a) {
    av1::SessionRef s;
    s.set_tenant_id(a.tenant);
    s.set_agent_id(a.id);
    s.set_session_id(a.session);
    s.set_attach_generation(a.gen);
    return s;
  }
  std::string cmd() { return "sim:" + std::to_string(++cmd_); }

  void attach(Agent& a, bool new_session) {
    if (new_session || a.session.empty()) a.session = new_uuid_v7(clock_.now_ms());
    av1::AttachAgentRequest r;
    r.set_tenant_id(a.tenant);
    r.set_agent_id(a.id);
    r.set_replica_id("gw-sim-1");
    r.set_session_id(a.session);
    r.set_agent_contact("sip:" + a.id + "@" + a.tenant + ".agents.example");
    av1::AttachAgentResponse out;
    auto st = rpc(a.tenant, [&](grpc::ClientContext& cc) { return agent_stub_->AttachAgent(&cc, r, &out); });
    if (!st.ok()) {
      violate("I6", "AttachAgent failed for " + a.id + ": " + st.error_message());
      return;
    }
    if (out.attach_generation() < a.gen) violate("I7", "attach_generation fell for " + a.id);
    if (out.attach_generation() != a.gen) a.last_seq = 0;
    a.gen = out.attach_generation();
    a.attached = true;
    view(a, out.agent());
  }
  void set_state(Agent& a, av1::TargetState s, const std::string& reason = "") {
    av1::SetStateRequest r;
    *r.mutable_session() = ref(a);
    r.set_command_id(cmd());
    r.set_target(s);
    r.set_reason_code(reason);
    av1::SetStateResponse out;
    auto st = rpc(a.tenant, [&](grpc::ClientContext& cc) { return agent_stub_->SetState(&cc, r, &out); });
    if (st.ok()) view(a, out.agent());
  }
  void login(const std::string& key, bool ready) {
    Agent& a = agents_.at(key);
    if (a.away) return;
    attach(a, false);
    a.logged_in = true;
    if (ready) set_state(a, av1::TARGET_STATE_READY);
  }
  void logout(const std::string& key) {
    Agent& a = agents_.at(key);
    if (!a.attached) return;
    set_state(a, av1::TARGET_STATE_LOGGED_OUT);
    a.logged_in = false;
  }

  // I6: views in version order; consecutive versions must be a legal transition.
  void view(Agent& a, const av1::AgentView& v) {
    auto& last = views_[a.tenant + "/" + a.id];
    if (v.state_version() < last.state_version()) return;  // an older view
    if (v.state_version() == last.state_version()) {
      if (last.state_version() != 0 && v.state() != last.state()) violate("I6", "two states with one state_version for " + a.id);
      last = v;
      return;
    }
    if (v.state_version() == last.state_version() + 1 && last.state_version() != 0) {
      ex("I6");
      static const std::set<std::pair<int, int>> ok = {
          {1, 2}, {2, 3}, {2, 2}, {2, 1}, {3, 4}, {3, 2}, {3, 1}, {4, 5}, {4, 3}, {4, 2}, {4, 1},
          {5, 6}, {5, 3}, {5, 2}, {5, 1}, {6, 7}, {7, 3}, {7, 2}, {7, 1}, {1, 1}};
      if (!ok.contains({last.state(), v.state()})) {
        violate("I6", "illegal transition " + std::to_string(last.state()) + "->" + std::to_string(v.state()) + " for " + a.id);
      }
    }
    last = v;
  }

  void on_gw(const av1::WatchResponse& m) {
    if (m.leader_epoch() < gw_epoch_) violate("I7", "an agents message from an older leader_epoch");
    gw_epoch_ = std::max(gw_epoch_, m.leader_epoch());
    if (m.has_heartbeat()) return;
    auto it = agents_.find(m.tenant_id() + "/" + m.agent_id());
    if (it == agents_.end()) {
      violate("I2", "a message for an unknown agent");
      return;
    }
    Agent& a = it->second;
    if (m.attach_generation() != a.gen) return;  // an older attachment's message
    ex("I7");
    if (m.delivery_seq() <= a.last_seq) return;  // a replay
    if (a.last_seq != 0 && m.delivery_seq() != a.last_seq + 1) violate("I7", "agent delivery_seq skipped for " + a.id);
    a.last_seq = m.delivery_seq();
    gw_acks_.push_back({a.tenant, a.id, a.gen, m.delivery_seq()});
    if (m.has_offer()) {
      const auto& o = m.offer();
      // An offer follows READY. One that arrives after a newer view of the agent (a re-attach's
      // snapshot came first, then the replay of what the outage held back) is long gone: no live
      // offer, no answer (found by the invariant search: regression-i6-seed-40).
      {
        const av1::AgentView& cur = views_[it->first];
        if (cur.state_version() != 0 && cur.state() != av1::AGENT_STATE_READY && cur.state() != av1::AGENT_STATE_OFFERED) return;
      }
      auto c = calls_.find(o.interaction_id());
      ex("I2");
      ex("I1");
      if (c != calls_.end() && c->second.tenant != a.tenant) violate("I2", "an offer of " + c->second.tenant + "'s call to " + a.tenant + "'s agent");
      if (!a.offer.empty()) violate("I1", "a second live offer for " + a.id);
      // One live offer per call: indexed by interaction (an O(agents) scan per offer does not
      // scale to the full-scale runs' 10k agents).
      if (auto lo = ix_offer_.find(o.interaction_id()); lo != ix_offer_.end() && lo->second != it->first) {
        const Agent& other = agents_.at(lo->second);
        if (!other.offer.empty() && live_offer_ix_[other.offer] == o.interaction_id()) violate("I1", "one call offered to two agents");
      }
      a.offer = o.offer_id();
      live_offer_ix_[o.offer_id()] = o.interaction_id();
      ix_offer_[o.interaction_id()] = it->first;
      av1::AgentView v = views_[it->first];
      if (v.state() != av1::AGENT_STATE_OFFERED) {  // a SetState answer may already show OFFERED
        v.set_state(av1::AGENT_STATE_OFFERED);
        v.set_state_version(v.state_version() + 1);
        view(a, v);
      }
      const json& b = behaviour_[a.tenant];
      const double u = uniform();
      if (u < b.value("p_ignore", 0.0)) return;  // it expires
      const bool accept = u >= b.value("p_ignore", 0.0) + b.value("p_reject", 0.0);
      const Dist d = b.contains("answer_delay") ? Dist::from(b.at("answer_delay")) : Dist{"fixed", 0, 0, 2};
      const std::string key = it->first, offer = o.offer_id();
      at(now() + static_cast<int64_t>(d.sample(rng_) * 1000), [this, key, offer, accept] { answer(key, offer, accept); });
    } else if (m.has_offer_revoked()) {
      if (m.offer_revoked().reason() == av1::REVOKE_REASON_EXPIRED) occur("offer_expired");
      if (a.offer == m.offer_revoked().offer_id()) a.offer.clear();
    } else if (m.has_reserved()) {
      a.offer.clear();
    } else if (m.has_state_changed()) {
      view(a, m.state_changed().agent());
      // A person who missed an offer (or was disconnected) gets back to work after a while.
      const auto& v = m.state_changed().agent();
      if (v.state() == av1::AGENT_STATE_NOT_READY && (v.reason_code() == "missed_offer" || v.reason_code() == "disconnected") && a.logged_in) {
        const std::string key = it->first;
        at(now() + 10'000, [this, key] {
          Agent& b = agents_.at(key);
          if (b.attached && !b.away && b.logged_in) set_state(b, av1::TARGET_STATE_READY);
        });
      }
    } else if (m.has_wrap_up_started()) {
      const json& b = behaviour_[a.tenant];
      if (b.contains("wrap_up")) {
        const double s = Dist::from(b.at("wrap_up")).sample(rng_);
        const std::string key = it->first, res = m.wrap_up_started().reservation_id();
        at(now() + static_cast<int64_t>(s * 1000), [this, key, res] { end_wrap(key, res); });
      }
    } else if (m.has_detached()) {
      a.attached = false;
    }
  }

  void answer(const std::string& key, const std::string& offer, bool accept) {
    Agent& a = agents_.at(key);
    if (a.away || !a.attached) return;
    av1::AnswerOfferRequest r;
    *r.mutable_session() = ref(a);
    r.set_command_id(cmd());
    r.set_offer_id(offer);
    r.set_decision(accept ? av1::DECISION_ACCEPT : av1::DECISION_REJECT);
    av1::AnswerOfferResponse out;
    grpc::ClientContext ctx;
    auto st = rpc(a.tenant, [&](grpc::ClientContext& cc) { return agent_stub_->AnswerOffer(&cc, r, &out); }, &ctx, true);
    if (st.ok() && !accept) occur("offer_rejected");
    if (st.ok() || a.offer == offer) a.offer.clear();
  }

  void end_wrap(const std::string& key, const std::string& res) {
    Agent& a = agents_.at(key);
    if (a.away || !a.attached) return;
    av1::EndWrapUpRequest r;
    *r.mutable_session() = ref(a);
    r.set_command_id(cmd());
    r.set_reservation_id(res);
    r.set_disposition_code("done");
    av1::EndWrapUpResponse out;
    auto st = rpc(a.tenant, [&](grpc::ClientContext& cc) { return agent_stub_->EndWrapUp(&cc, r, &out); });
    if (st.ok()) view(a, out.agent());
  }

  // ---- scenario events ----

  void event(const json& e) {
    const std::string k = e.at("kind");
    if (k == "agent_outage") {
      std::vector<std::string> hit;
      for (auto& [key, a] : agents_) {
        if (a.tenant != e.value("tenant", a.tenant) || (e.contains("site") && a.site != e.at("site").get<std::string>()) || !a.attached) continue;
        if (uniform() * 100 < e.value("percent", 100.0)) hit.push_back(key);
      }
      for (const auto& key : hit) {
        Agent& a = agents_.at(key);
        av1::DetachAgentRequest d;
        *d.mutable_session() = ref(a);
        d.set_reason(av1::DETACH_REASON_SOCKET_CLOSED);
        av1::DetachAgentResponse out;
        rpc(a.tenant, [&](grpc::ClientContext& cc) { return agent_stub_->DetachAgent(&cc, d, &out); });
        a.away = true;
        a.attached = false;
        a.offer.clear();
      }
      if (e.contains("down_s")) {
        at(now() + static_cast<int64_t>(e.at("down_s").get<double>() * 1000), [this, hit] {
          for (const auto& key : hit) {
            Agent& a = agents_.at(key);
            a.away = false;
            if (a.logged_in) login(key, true);
          }
        });
      }
    } else if (k == "mass_logout") {
      for (auto& [key, a] : agents_) {
        if (a.tenant == e.at("tenant").get<std::string>() && a.logged_in && uniform() * 100 < e.value("percent", 100.0)) logout(key);
      }
    } else if (k == "mass_login") {
      const auto& g = e.at("group");
      const double over = e.value("over_s", 0.0) * 1000;
      int i = 0;
      const int n = g.at("count");
      for (auto& [gp, keys] : groups_) {
        if (*gp != g) continue;
        for (const auto& key : keys) {
          at(now() + static_cast<int64_t>(over * i++ / std::max(1, n)), [this, key] { login(key, true); });
        }
      }
    } else if (k == "ie_stream_lost") {
      ie_.reset();
      ie_down_ = true;
      at(now() + static_cast<int64_t>(e.value("down_s", 10.0) * 1000), [this] { open_ie(); });
    } else if (k == "gateway_disconnect") {
      gw_.reset();
      gw_down_ = true;
      at(now() + static_cast<int64_t>(e.value("down_s", 10.0) * 1000), [this] {
        open_gw();
        for (auto& [key, a] : agents_) {
          if (a.attached && !a.away) attach(a, false);  // the gateway re-attaches its live sessions
        }
      });
    } else if (k == "ie_restart") {
      // IE restarts alone (RE and the gateway stay up): the process forgets every call it owned;
      // it comes back after down_s as a new replica (a new pod) and claims new calls with new
      // epochs. RE must drop the old replica's calls (CR-083) within owner_loss_s.
      ie_.reset();
      ie_down_ = true;
      for (auto& [ix, c] : calls_) {
        if (c.st == Call::kDone) continue;
        c.forgotten = true;
        c.forgotten_at = now();
        any_forgotten_ = true;
        finish(c, "lost_ie_restart");
      }
      replica_ = "ie-sim-" + std::to_string(++replica_n_);
      owner_epoch_ += 1000;
      at(now() + static_cast<int64_t>(e.value("down_s", 5.0) * 1000), [this] { open_ie(); });
    } else if (k == "request_flood") {
      flood(e);
    } else if (k == "re_restart") {
      // The phase-1 restart rule: RE, IE and the gateway restart together.
      ie_.reset();
      gw_.reset();
      post("/_test/v1/restart", {{"incarnation", engine_->epochs().leader_epoch + 1}});
      for (auto& [ix, c] : calls_) finish(c, "lost_restart");
      owner_epoch_ += 1000;
      open_ie();
      open_gw();
      for (auto& [key, a] : agents_) {
        a.offer.clear();
        views_.erase(key);
        if (a.logged_in && !a.away) {
          attach(a, true);
          set_state(a, av1::TARGET_STATE_READY);
        }
      }
    } else {
      skipped_events_.push_back(k + " (phase 4)");
    }
  }

  // request_flood: `count` callers at once, their Enqueues sent concurrently. With `stall` the
  // tenant's writer is held (a GC pause, CPU starvation) until every request is either waiting in
  // its inbox or refused, so the per-tenant inbox share (PLAN §6.1, CR-034) must refuse the rest
  // UNAVAILABLE (IE retries those with backoff).
  void flood(const json& e) {
    const std::string tenant = e.at("tenant"), queue = e.at("queue");
    const int n = e.value("count", 1);
    std::vector<std::string> ixs;
    for (int i = 0; i < n; ++i) ixs.push_back(arrive(tenant, queue, /*send=*/false));
    const bool stall = e.value("stall", false);
    std::promise<void> release, stalled;
    std::shared_future<void> held = release.get_future().share();
    if (stall) {
      auto s = stalled.get_future();
      engine_->post(tenant, [held, &stalled](Tenant*, int64_t) {
        stalled.set_value();
        held.wait();
      }, /*external=*/false);
      s.wait();
    }
    struct Req {
      grpc::ClientContext ctx;
      rv1::EnqueueRequest req;
      rv1::EnqueueResponse resp;
      grpc::Status st;
    };
    std::vector<std::unique_ptr<Req>> reqs;
    std::mutex mu;
    std::condition_variable cv;
    int done = 0;
    for (const auto& ix : ixs) {
      auto r = std::make_unique<Req>();
      r->req = enqueue_request(calls_.at(ix));
      r->ctx.AddMetadata("tenant-id", tenant);
      r->ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
      Req* rp = r.get();
      reqs.push_back(std::move(r));
      routing_->async()->Enqueue(&rp->ctx, &rp->req, &rp->resp, [rp, &mu, &cv, &done](grpc::Status st) {
        std::lock_guard lk(mu);
        rp->st = std::move(st);
        ++done;
        cv.notify_all();
      });
    }
    if (stall) {
      const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30);
      for (;;) {
        int d;
        {
          std::lock_guard lk(mu);
          d = done;
        }
        if (static_cast<int>(engine_->queued(tenant)) + d >= n) break;
        if (std::chrono::steady_clock::now() > until) {
          violate("flood", "the flood's requests never all reached the engine");
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
      release.set_value();
    }
    {
      std::unique_lock lk(mu);
      cv.wait(lk, [&] { return done == n; });
    }
    for (size_t i = 0; i < reqs.size(); ++i) enqueued(ixs[i], 0, reqs[i]->st, reqs[i]->ctx, reqs[i]->req, reqs[i]->resp);
    react();
  }

  void open_ie() {
    ie_ = std::make_unique<Drain<rv1::WatchResponse>>();
    rv1::WatchRequest w;
    w.set_replica_id(replica_);
    w.set_bin(sc_.at("bin"));
    ie_->reader = routing_->Watch(&ie_->ctx, w);
    ie_->start();
    control_->handle("POST", "/_test/v1/sync", "{}");
    for (int i = 0; i < 200 && !hub_->ie_live(replica_); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(5));
    ie_down_ = false;
    ie_up_at_ = now();
    control_->handle("POST", "/_test/v1/sync", "{}");
  }
  void open_gw() {
    gw_ = std::make_unique<Drain<av1::WatchResponse>>();
    av1::WatchRequest w;
    w.set_replica_id("gw-sim-1");
    w.set_bin(sc_.at("bin"));
    gw_->reader = agent_stub_->Watch(&gw_->ctx, w);
    gw_->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    gw_down_ = false;
    control_->handle("POST", "/_test/v1/sync", "{}");
  }

  // Everything the engine sent since the last look, in order; then the acks.
  void react() {
    for (int round = 0; round < 1000; ++round) {
      control_->handle("POST", "/_test/v1/sync", "{}");
      std::vector<std::pair<std::string, rv1::WatchResponse>> ie;
      std::vector<std::pair<std::string, av1::WatchResponse>> gw;
      {
        std::lock_guard lk(tee_.mu);
        ie.swap(tee_.ie);
        gw.swap(tee_.gw);
      }
      if (ie.empty() && gw.empty()) break;
      for (auto& [r, m] : ie) {
        if (!ie_down_) on_ie(m);
      }
      for (auto& [r, m] : gw) {
        if (!gw_down_) on_gw(m);
      }
      flush_acks();
    }
  }

  void flush_acks() {
    if (!acks_.empty() && !ie_down_) {
      rv1::AcknowledgeRequest r;
      for (const auto& [ix, tenant] : acks_) *r.add_acks() = owner(calls_.at(ix));
      rv1::AcknowledgeResponse out;
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
      routing_->Acknowledge(&ctx, r, &out);
      acks_.clear();
    }
    if (!gw_acks_.empty() && !gw_down_) {
      av1::AcknowledgeRequest r;
      r.set_replica_id("gw-sim-1");
      for (const auto& [t, id, gen, seq] : gw_acks_) {
        auto* a = r.add_acks();
        a->set_tenant_id(t);
        a->set_agent_id(id);
        a->set_attach_generation(gen);
        a->set_delivery_seq(seq);
      }
      av1::AcknowledgeResponse out;
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
      agent_stub_->Acknowledge(&ctx, r, &out);
      gw_acks_.clear();
    }
  }

  // I3, two-sided: the engine holds an entry for exactly the calls IE still has queued (waiting
  // with an entry, or assigned). A dropped entry, or one the engine kept after IE let it go, shows.
  void reconcile(const char* when) {
    std::set<std::string> engine, ie;
    engine_state_.clear();
    for (const auto& [tenant, e] : engine_->entries_snapshot()) {
      engine_state_[e.interaction] = e.state;
      if (!engine.insert(e.interaction).second) violate("I3", tenant + ": two live entries for one interaction");
      auto c = calls_.find(e.interaction);
      if (c != calls_.end() && c->second.tenant != tenant) violate("I2", "an entry of " + c->second.tenant + "'s call in " + tenant);
    }
    for (const auto& ixk : open_) {
      const Call& c = calls_.at(ixk);
      if ((c.st == Call::kWaiting && !c.entry.empty()) || c.st == Call::kAssigned) ie.insert(ixk);
    }
    ex("I3", engine.size() + ie.size());
    for (const auto& ix : ie) {
      if (!engine.contains(ix)) violate("I3", std::string(when) + ": " + calls_.at(ix).tenant + " call queued in IE has no entry in the engine");
    }
    for (const auto& ix : engine) {
      auto c = calls_.find(ix);
      if (c != calls_.end() && c->second.forgotten) continue;  // an orphan: judged by orphans() below
      if (!ie.contains(ix)) violate("I3", std::string(when) + ": the engine keeps an entry IE no longer has");
    }
    orphans(when);
  }

  // CR-083: whatever RE holds for a forgotten call (an entry, a message, a reservation) is gone
  // by the bound: owner_loss_s (+ the wrap-up limit for a call that was connected, + slack).
  void orphans(const char* when) {
    if (!any_forgotten_) return;
    bool any = false;
    for (const auto& [ix, c] : calls_) any |= c.forgotten && !c.cleaned;
    if (!any) {
      any_forgotten_ = false;
      return;
    }
    std::set<std::string> held;
    for (auto& ix : engine_->held_interactions()) held.insert(std::move(ix));
    for (auto& [ix, c] : calls_) {
      if (!c.forgotten || c.cleaned) continue;
      ex("I3");
      if (!held.contains(ix)) {
        c.cleaned = true;
        occur("owner_lost");
        continue;
      }
      const QueueSim& qs = queues_.at(c.tenant + "/" + c.queue);
      const int64_t bound = c.forgotten_at + owner_loss_ms_[c.tenant] + qs.wrap_limit_ms + 10'000;
      if (now() > bound && !c.late) {
        c.late = true;
        violate("I3", std::string(when) + ": " + c.tenant + " still holds a call of the restarted IE " +
                          std::to_string(now() - c.forgotten_at) + " ms later (CR-083)");
      }
    }
  }

  void check_step() {
    // Every step by default; the full-scale runs check every few seconds of virtual time.
    if (opt_.check_every_ms > 1000 && now() - last_check_ < opt_.check_every_ms) return;
    last_check_ = now();
    reconcile("step");
    // I5: no entry waits past its max-wait action (plus one timer tick).
    for (const auto& ixk : open_) {
      Call& c = calls_.at(ixk);
      if (c.st != Call::kWaiting || c.entry.empty()) continue;
      const QueueSim& qs = queues_.at(c.tenant + "/" + c.queue);
      ex("I5");
      // CR-100: IE's side as well. A caller IE still holds as waiting must hear its max-wait action
      // by max_wait + one tick, counted from the latest of: its max wait, its return to waiting (a
      // failed assignment past max wait overflows at once, `overflow_due`), and IE's stream coming
      // back (an outage holds the Overflow until the replay); plus the replay's grace (Watch
      // redelivers unacknowledged messages every 2 s).
      constexpr int64_t kReplayGraceMs = 5'000;
      const int64_t ie_due = std::max({c.queue_since + qs.max_wait_ms, c.back_waiting_at, ie_up_at_}) + 1500 + kReplayGraceMs;
      if (!ie_down_ && now() > ie_due && !c.i5_ie_reported) {
        c.i5_ie_reported = true;
        ++i5_ie_side_;
        violate("I5", c.tenant + " call still waiting on IE's side " + std::to_string(now() - c.queue_since) + " ms after entering " +
                          c.queue + " (max wait " + std::to_string(qs.max_wait_ms) + " ms; no Overflow heard)");
      }
      // The engine's own entry decides the strict bound: one it already overflowed (the Overflow
      // waiting in an IE stream outage, replayed on reconnect) has left by its action
      // (regression-i5-seed-24); the IE-side bound above covers its delivery.
      auto es = engine_state_.find(ixk);
      if (es == engine_state_.end() || es->second != rv1::ENTRY_STATE_WAITING) continue;
      if (now() - c.queue_since > qs.max_wait_ms + 1500) {
        violate("I5", c.tenant + " call waited " + std::to_string(now() - c.queue_since) + " ms in " + c.queue);
        c.queue_since = now();  // report once per max-wait window
      }
    }
    // I1, I4, I11 (ordering kept), the engine's own structural checks.
    uint64_t checked = 0;
    for (const auto& v : engine_->check_invariants(&checked)) violate("I4", v);
    ex("I4", checked);
    if (engine_->totals().sweep_matches != 0 && !sweep_reported_) {
      sweep_reported_ = true;
      violate("I4", "the 1 s sweep found a missed dispatch");
    }
  }

  json report() {
    react();
    json out = {{"scenario", sc_.at("name")}, {"planned_arrivals", planned_}};
    json per = json::object();
    uint64_t waiting_sim = 0;
    for (const auto& [ix, c] : calls_) {
      if (c.st != Call::kDone) ++waiting_sim;
    }
    for (const auto& t : sc_.at("tenants")) {
      const std::string id = t.at("id");
      json e = ends_[id];
      uint64_t ended = 0;
      for (const auto& [k, v] : ends_[id]) ended += v;
      uint64_t open = 0;
      for (const auto& [ix, c] : calls_) {
        if (c.tenant == id && c.st != Call::kDone) ++open;
      }
      // I3: every arrival is answered, abandoned, overflowed, refused, lost by the restart or still open.
      if (arrived_[id] != ended + open) violate("I3", id + ": arrived " + std::to_string(arrived_[id]) + " != ended + open");
      uint64_t in_sla = 0, ans = 0;
      int64_t max_wait = 0;
      for (const auto& w : wait_ms_) {
        if (w.tenant != id) continue;
        ++ans;
        in_sla += w.in_sla;
        max_wait = std::max(max_wait, w.ms);
      }
      const double sl = ans ? static_cast<double>(in_sla) / std::max<uint64_t>(1, arrived_[id] - ends_[id]["refused"]) : 0;
      const double ab = arrived_[id] ? static_cast<double>(ends_[id]["abandoned"]) / arrived_[id] : 0;
      per[id] = {{"arrived", arrived_[id]}, {"ends", e}, {"open", open}, {"service_level", sl}, {"abandon_rate", ab},
                 {"max_wait_s", max_wait / 1000.0}};
      const json kpis = sc_.at("expectations").value("kpi", json::array());
      for (const auto& k : kpis) {
        if (k.value("tenant", id) != id) continue;
        if (k.contains("queue")) continue;  // judged per queue below
        const std::string m = k.at("metric");
        const double v = m == "service_level" ? sl : m == "abandon_rate" ? ab : m == "max_wait_s" ? max_wait / 1000.0
                         : m == "answered" ? static_cast<double>(ends_[id]["answered"]) : static_cast<double>(ends_[id]["overflowed"]);
        const double tol = k.value("tolerance", 0.0);
        if ((k.contains("min") && v < k.at("min").get<double>() - tol) || (k.contains("max") && v > k.at("max").get<double>() + tol)) {
          violate("KPI", id + " " + m + " = " + std::to_string(v));
        }
        ++kpi_checked_;
      }
    }
    // KPIs per (tenant, queue), the arrival queue's: service level, abandonment, max wait, answered,
    // overflowed (expectations.kpi with `queue`).
    json perq = json::object();
    for (const auto& t : sc_.at("tenants")) {
      const std::string id = t.at("id");
      for (const auto& q : t.at("queues")) {
        const std::string key = id + "/" + q.at("id").get<std::string>();
        uint64_t in_sla = 0, ans = 0;
        int64_t max_wait = 0;
        for (const auto& w : wait_ms_) {
          if (w.tenant != id || w.queue != q.at("id").get<std::string>()) continue;
          ++ans;
          in_sla += w.in_sla;
          max_wait = std::max(max_wait, w.ms);
        }
        auto& e = q_ends_[key];
        const uint64_t arr = q_arrived_[key];
        const double sl = arr > e["refused"] ? static_cast<double>(in_sla) / (arr - e["refused"]) : 0;
        const double ab = arr ? static_cast<double>(e["abandoned"]) / arr : 0;
        perq[key] = {{"arrived", arr}, {"answered", e["answered"]}, {"abandoned", e["abandoned"]},
                     {"overflowed", e["overflowed"]}, {"refused", e["refused"]}, {"service_level", sl},
                     {"abandon_rate", ab}, {"max_wait_s", max_wait / 1000.0}};
        for (const auto& k : sc_.at("expectations").value("kpi", json::array())) {
          if (!k.contains("queue") || k.value("tenant", id) != id || k.at("queue") != q.at("id")) continue;
          const std::string m = k.at("metric");
          const double v = m == "service_level" ? sl : m == "abandon_rate" ? ab : m == "max_wait_s" ? max_wait / 1000.0
                           : m == "answered" ? static_cast<double>(e["answered"]) : static_cast<double>(e["overflowed"]);
          const double tol = k.value("tolerance", 0.0);
          ++kpi_checked_;
          if ((k.contains("min") && v < k.at("min").get<double>() - tol) || (k.contains("max") && v > k.at("max").get<double>() + tol)) {
            violate("KPI", key + " " + m + " = " + std::to_string(v));
          }
        }
      }
    }
    out["queues"] = perq;
    // I3 against the engine at the end, two-sided (CR-055; the M1 check was one-sided).
    post("/_test/v1/advance", {{"ms", 1000}});
    react();
    reconcile("end");
    const auto tot = engine_->totals();
    const uint64_t re_waiting = engine_->totals().waiting;
    (void)waiting_sim;
    // I9 latency, I10 memory.
    std::sort(lat_.begin(), lat_.end());
    const double p99 = lat_.empty() ? 0 : lat_[std::min(lat_.size() - 1, static_cast<size_t>(lat_.size() * 0.99))];
    const double mx = lat_.empty() ? 0 : lat_.back();
    const json& lj = sc_.at("expectations").value("latency_ms", json::object());
    if (lj.contains("p99") && p99 > lj.at("p99").get<double>()) violate("I9", "p99 " + std::to_string(p99) + " ms");
    if (lj.contains("max") && mx > lj.at("max").get<double>()) violate("I9", "max " + std::to_string(mx) + " ms");
    const double rss = rss_mib();
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
    constexpr bool kSanitized = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
    constexpr bool kSanitized = true;
#else
    constexpr bool kSanitized = false;
#endif
#else
    constexpr bool kSanitized = false;
#endif
    // Sanitizer shadow memory inflates RSS several times: reported, judged only in normal builds.
    if (!kSanitized && sc_.at("expectations").contains("rss_mib_max") &&
        rss > sc_.at("expectations").at("rss_mib_max").get<double>()) {
      violate("I10", "rss " + std::to_string(rss) + " MiB");
    }
    uint64_t cap = 0;
    for (const auto& [k, q] : queues_) cap += q.max_entries;
    ex("I10");
    if (re_waiting > cap) violate("I10", "entries above the queues' max_entries");
    ex("I9", lat_.size());
    const json must = sc_.at("expectations").value("must_occur", json::array());
    for (const auto& w : must) {
      if (occurred_[w.get<std::string>()] == 0) violate("expectations", "must_occur " + w.get<std::string>() + " never happened");
    }
    if (fault_.at_ms >= 0 && !fault_applied_) violate("fault", "the fault " + fault_.kind + " found nothing to break");
    out["tenants"] = per;
    out["engine"] = {{"offers", tot.offers}, {"accepted", tot.accepted}, {"rejected", tot.rejected}, {"expired", tot.expired},
                     {"overflows", tot.overflows}, {"sweep_matches", tot.sweep_matches}, {"assign_expired", tot.assign_expired},
                     {"redeliveries", tot.redeliveries}, {"inbox_refused", tot.inbox_refused}};
    out["latency_ms"] = {{"p99", p99}, {"max", mx}, {"requests", lat_.size()}};
    // I9 as PLAN §6.1 states it: the offer decision inside the writer (from the start of the task
    // that made the entry offerable or the agent ready), per shard; p99 as a log2 bucket bound.
    uint64_t decisions = 0;
    for (uint64_t c : tot.decision_us_buckets) decisions += c;
    double dp99 = 0;
    for (int b = 0, acc = 0; b < 32; ++b) {
      acc += static_cast<int>(tot.decision_us_buckets[b]);
      if (decisions && acc >= 0.99 * decisions) {
        dp99 = std::ldexp(1.0, b) / 1000.0;  // bucket b holds values < 2^b us
        break;
      }
    }
    const double dmax = tot.decision_max_us / 1000.0;
    out["decision_latency_ms"] = {{"p99_le", dp99}, {"max", dmax}, {"decisions", decisions}};
    if (opt_.decision_p99_ms > 0) {
      ex("I9", decisions);
      if (dp99 > opt_.decision_p99_ms) violate("I9", "offer decision p99 <= " + std::to_string(dp99) + " ms > " + std::to_string(opt_.decision_p99_ms));
      if (dmax > opt_.decision_max_ms) violate("I9", "offer decision max " + std::to_string(dmax) + " ms > " + std::to_string(opt_.decision_max_ms));
    }
    out["ewt"] = {{"model", tot.ewt_model}, {"heuristic", tot.ewt_heuristic}, {"plans_installed", tot.plans_installed},
                  {"plans_refused", tot.plans_refused}};
    out["kpi_checked"] = kpi_checked_;
    out["rss_mib"] = rss;
    // CR-056: the PositionUpdates IE received and the streams the hub closed as slow consumers.
    out["position_updates"] = position_updates_;
    out["i5_ie_side"] = i5_ie_side_;  // CR-100: I5 reports only IE's side could make
    out["streams_closed_slow"] = hub_->closed_slow();
    out["unavailable_retries"] = unavailable_;
    out["prediction_error"] = "not available in phase 1 (no predictor; phase 2 CTMC, 5c NNMPC)";
    if (!skipped_events_.empty()) out["skipped_events"] = skipped_events_;
    out["violations"] = violations_;
    out["violation_count"] = violation_count_;
    out["violated"] = violated_;
    out["exercised"] = exercised_;
    out["occurred"] = occurred_;
    return out;
  }

  static double rss_mib() {
    std::ifstream in("/proc/self/status");
    std::string line;
    while (std::getline(in, line)) {
      if (line.rfind("VmRSS:", 0) == 0) return std::stod(line.substr(6)) / 1024.0;
    }
    return 0;
  }

  struct Wait {
    std::string tenant;
    int64_t ms;
    bool in_sla;
    std::string queue;
  };

  json sc_;
  std::mt19937_64 rng_;
  ManualClock clock_{kStart};
  Tee tee_;
  Metrics metrics_;
  std::unique_ptr<Engine> engine_;
  std::unique_ptr<Hub> hub_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<TestControl> control_;
  std::unique_ptr<rv1::RoutingService::Stub> routing_;
  std::unique_ptr<av1::AgentService::Stub> agent_stub_;
  std::unique_ptr<Drain<rv1::WatchResponse>> ie_;
  std::unique_ptr<Drain<av1::WatchResponse>> gw_;
  bool ie_down_ = true, gw_down_ = true, sweep_reported_ = false;
  int64_t ie_up_at_ = 0;  // when IE's stream last came back (CR-100)
  std::multimap<int64_t, std::function<void()>> actions_;
  std::map<std::string, QueueSim> queues_;
  std::map<std::string, Call> calls_;
  std::map<std::string, Agent> agents_;
  std::map<const json*, std::vector<std::string>> groups_;
  std::map<std::string, json> behaviour_;
  std::map<std::string, av1::AgentView> views_;
  std::map<std::string, std::pair<uint64_t, std::string>> ie_seq_;
  std::map<std::string, uint64_t> last_epoch_of_;
  std::map<std::string, std::string> acks_;
  std::vector<std::tuple<std::string, std::string, uint64_t, uint64_t>> gw_acks_;
  std::map<std::string, std::string> agent_of_, live_offer_ix_;
  std::map<std::string, rv1::Outcome> recorded_;
  std::map<std::string, uint64_t> arrived_, answered_by_;
  std::map<std::string, std::map<std::string, uint64_t>> ends_;
  std::vector<Wait> wait_ms_;
  std::vector<double> lat_;
  std::vector<std::string> violations_, skipped_events_;
  std::map<std::string, uint64_t> exercised_, occurred_, violated_;
  Fault fault_;
  RunOptions opt_;
  std::unique_ptr<Optimizer> optimizer_;
  std::set<std::string> open_;
  std::map<std::string, rv1::EntryState> engine_state_;  // the last reconcile's engine entries
  std::map<std::string, std::string> ix_offer_;  // interaction -> the agent holding its live offer
  std::map<std::string, uint64_t> q_arrived_;
  std::map<std::string, std::map<std::string, uint64_t>> q_ends_;
  int64_t last_check_ = -1'000'000;
  bool any_forgotten_ = false;
  uint64_t kpi_checked_ = 0;
  uint64_t position_updates_ = 0;
  uint64_t i5_ie_side_ = 0;
  bool fault_applied_ = false;
  std::string replica_ = "ie-sim-1";
  int replica_n_ = 1;
  std::map<std::string, int64_t> owner_loss_ms_;
  uint64_t violation_count_ = 0, unavailable_ = 0, ie_epoch_ = 0, gw_epoch_ = 0, cmd_ = 0, owner_epoch_ = 100;
  int planned_ = 0;
};

}  // namespace

namespace pcr::scenario {

json run(const json& sc, const RunOptions& opt) {
  Run r(sc, opt);
  return r.play();
}

}  // namespace pcr::scenario
