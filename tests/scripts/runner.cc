// The G5 script runner (PLAN §8 1.4, CR-011): plays parity-contracts' executable routing.v1 scripts
// and the agents.v1 scripts against the real engine, through real gRPC (the same Server as the
// binary, on an ephemeral port) and the test control. CONVENTIONS "Executable routing scripts":
// RE-made ids are placeholders bound at first sight; expected_wait_s is not compared; everything
// else is exact. agents.v1 fixtures also leave since_ms and deadline_ms loose (CR-047).
#include <google/protobuf/util/json_util.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <set>
#include <sstream>
#include <thread>

#include "parity/agents/v1/agents.grpc.pb.h"
#include "parity/routing/v1/routing.grpc.pb.h"
#include "src/core/engine.h"
#include "src/ops/metrics.h"
#include "src/server/hub.h"
#include "src/server/services.h"
#include "src/server/test_control.h"

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace pcr;

namespace {

struct Fail {
  std::string why;
};

std::string to_json(const google::protobuf::Message& m) {
  std::string s;
  google::protobuf::util::JsonPrintOptions o;
  o.preserve_proto_field_names = true;
  if (!google::protobuf::util::MessageToJsonString(m, &s, o).ok()) throw Fail{"print"};
  return s;
}

void from_json(const std::string& text, google::protobuf::Message* m) {
  auto st = google::protobuf::util::JsonStringToMessage(text, m);
  if (!st.ok()) throw Fail{"parse " + std::string(m->GetTypeName()) + ": " + std::string(st.message())};
}

// A message as a JSON tree with protobuf's defaults dropped (both sides go through the proto).
json norm(const google::protobuf::Message& m) { return json::parse(to_json(m)); }

template <class Msg>
struct StreamReader {
  grpc::ClientContext ctx;
  std::unique_ptr<grpc::ClientReader<Msg>> reader;
  std::mutex mu;
  std::condition_variable cv;
  std::deque<Msg> q;
  bool ended = false;
  std::thread th;
  std::vector<json> matched;  // for replay detection

  void start() {
    th = std::thread([this] {
      Msg m;
      while (reader->Read(&m)) {
        std::lock_guard lk(mu);
        q.push_back(m);
        cv.notify_all();
      }
      std::lock_guard lk(mu);
      ended = true;
      cv.notify_all();
    });
  }
  std::optional<Msg> next(int ms) {
    std::unique_lock lk(mu);
    if (!cv.wait_for(lk, std::chrono::milliseconds(ms), [&] { return !q.empty() || ended; })) return std::nullopt;
    if (q.empty()) return std::nullopt;
    Msg m = q.front();
    q.pop_front();
    return m;
  }
  void stop() {
    ctx.TryCancel();
    if (th.joinable()) th.join();
  }
  ~StreamReader() { stop(); }
};

// Phase 3: the scripts' Postgres stand-in. Every write succeeds; completions arrive later on the
// tenant's writer, as the real store's do. A voicemail task's recording maps to the task made first.
struct MemoryStore final : Durable {
  Engine* engine = nullptr;
  std::mutex mu;
  std::map<std::pair<std::string, std::string>, std::string> tasks;
  bool put_callback(const CallbackRecord& r, Done done) override {
    engine->post(r.tenant, [done](Tenant* t, int64_t now) { done(t, true, now); }, false);
    return true;
  }
  bool put_task(const TaskRecord& r, TaskDone done) override {
    std::string id = r.task_id;
    bool existing = false;
    {
      std::lock_guard lk(mu);
      auto [it, fresh] = tasks.try_emplace({r.tenant, r.recording_id}, r.task_id);
      id = it->second;
      existing = !fresh;
    }
    engine->post(r.tenant, [done, id, existing](Tenant* t, int64_t now) { done(t, true, id, existing, now); }, false);
    return true;
  }
  void put_buckets(std::vector<BucketRow>) override {}
};

class Runner {
 public:
  Runner(const std::string& contracts) : contracts_(contracts) {
    engine_ = std::make_unique<Engine>(&clock_, &late_, 2);
    store_.engine = engine_.get();
    engine_->set_durable(&store_, nullptr);
    hub_ = std::make_unique<Hub>(&engine_->epochs());
    late_.hub = hub_.get();
    server_ = std::make_unique<Server>(ServerOptions{"127.0.0.1:0", "bin-a", nullptr, nullptr}, &clock_, engine_.get(), hub_.get(), &metrics_);
    if (!server_->start()) throw Fail{"cannot start the server"};
    control_ = std::make_unique<TestControl>(engine_.get(), hub_.get(), &clock_, std::vector<TenantConfig>{}, Epochs{1, 0}, "bin-a");
    channel_ = grpc::CreateChannel("127.0.0.1:" + std::to_string(server_->port()), grpc::InsecureChannelCredentials());
    routing_ = rv1::RoutingService::NewStub(channel_);
    agents_ = av1::AgentService::NewStub(channel_);
  }

  // "" on pass, the reason on failure.
  std::string play(const fs::path& file) {
    std::ifstream in(file);
    script_ = json::parse(in);
    binds_.clear();
    ie_.clear();
    gw_.reset();
    harness_gw_.reset();
    sessions_.clear();
    latest_offer_.clear();
    ever_offered_.clear();
    const bool agents = script_.at("package") == "parity.agents.v1";
    try {
      seed(agents);
      int i = 0;
      for (const auto& step : script_.at("steps")) {
        step_ = i++;
        run(step, agents);
      }
    } catch (const Fail& f) {
      for (auto& [k, s] : ie_) s->stop();
      return "step " + std::to_string(step_) + ": " + f.why;
    }
    for (auto& [k, s] : ie_) s->stop();
    if (gw_) gw_->stop();
    if (harness_gw_) harness_gw_->stop();
    return "";
  }

 private:
  void control(const std::string& path, const json& body, json* out = nullptr) {
    HttpReply r = control_->handle("POST", path, body.dump());
    if (r.status != 200) throw Fail{"test control " + path + ": " + r.body};
    if (out) *out = json::parse(r.body);
  }

  void seed(bool agents) {
    const json& g = script_.at("given");
    json req = {{"kind", agents ? "agents_given" : "routing_given"}, {"given", g}};
    if (agents) {
      json qs = json::array();
      for (const auto& s : script_.at("steps")) {
        if (s.value("op", "") == "inject" && s.value("what", "") == "queue_call") {
          qs.push_back({{"tenant_id", s.at("args").at("tenant_id")}, {"queue_id", s.at("args").at("queue_id")}});
        }
      }
      req["queues"] = qs;
    }
    json out;
    control("/_test/v1/seed", req, &out);
    if (!agents) {
      for (const auto& s : out.at("sessions")) sessions_[s.at("tenant_id").get<std::string>() + "/" + s.at("agent_id").get<std::string>()] = s;
      // The harness's own agents.v1 stream: where the given agents' offers arrive.
      harness_gw_ = std::make_unique<StreamReader<av1::WatchResponse>>();
      av1::WatchRequest w;
      w.set_replica_id("gw-script");
      w.set_bin("bin-a");
      harness_gw_->reader = agents_->Watch(&harness_gw_->ctx, w);
      harness_gw_->start();
      if (!harness_gw_->next(2000)) throw Fail{"the harness's agents stream didn't open"};
    }
  }

  std::string subst(std::string body) const {
    for (const auto& [want, have] : binds_) {
      for (size_t p = body.find(want); p != std::string::npos; p = body.find(want, p + have.size())) {
        body.replace(p, want.size(), have);
      }
    }
    return body;
  }

  void bind(const std::string& want, const std::string& have, const std::string& where) {
    auto it = binds_.find(want);
    if (it != binds_.end()) {
      if (it->second != have) throw Fail{where + ": placeholder " + want + " is " + it->second + ", got " + have};
      return;
    }
    for (const auto& [w, h] : binds_) {
      if (h == have) throw Fail{where + ": " + have + " already bound to " + w};
    }
    binds_[want] = have;
  }

  void compare(const json& want, const json& have, const std::string& path, bool agents) {
    // Ids RE makes (0.11.0 adds callback_id, task_id and RunCallback's interaction_id).
    static const std::set<std::string> kIds = {"entry_id", "reservation_id", "offer_id", "callback_id", "task_id"};
    // 0.11.0's fields, compared only where a script names them (scripts before 0.11.0 don't).
    static const std::set<std::string> kNamedOnly = {"queues", "pending_queues", "queues_version", "tier",
                                                     "expected_wait_quality", "callback"};
    std::set<std::string> ignore = {"expected_wait_s"};
    if (agents) ignore.insert({"since_ms", "deadline_ms"});
    if (want.is_object()) {
      if (!have.is_object()) throw Fail{path + ": expected an object, got " + have.dump()};
      std::set<std::string> keys;
      for (const auto& [k, v] : want.items()) keys.insert(k);
      for (const auto& [k, v] : have.items()) keys.insert(k);
      for (const auto& k : keys) {
        if (ignore.contains(k)) continue;
        if (kNamedOnly.contains(k) && !want.contains(k)) continue;
        if (k == "interaction_id" && path.ends_with(".run_callback") && want.contains(k) && have.contains(k)) {
          bind(want.at(k).get<std::string>(), have.at(k).get<std::string>(), path + "." + k);
          continue;
        }
        if (!want.contains(k) || !have.contains(k)) {
          throw Fail{path + "." + k + ": expected " + (want.contains(k) ? want.at(k).dump() : "absent") + ", got " +
                     (have.contains(k) ? have.at(k).dump() : "absent")};
        }
        if (kIds.contains(k) && want.at(k).is_string()) {
          bind(want.at(k).get<std::string>(), have.at(k).get<std::string>(), path + "." + k);
          continue;
        }
        compare(want.at(k), have.at(k), path + "." + k, agents);
      }
      return;
    }
    if (want.is_array()) {
      if (!have.is_array() || want.size() != have.size()) throw Fail{path + ": expected " + want.dump() + ", got " + have.dump()};
      for (size_t i = 0; i < want.size(); ++i) compare(want[i], have[i], path + "[" + std::to_string(i) + "]", agents);
      return;
    }
    if (want != have) throw Fail{path + ": expected " + want.dump() + ", got " + have.dump()};
  }

  // Compares an answer (or a status) with the step's expect.
  template <class Resp>
  void expect(const json& step, const grpc::Status& st, const grpc::ClientContext& ctx, const Resp& resp, bool agents) {
    const json& e = step.at("expect");
    if (e.contains("status")) {
      std::string perr;
      auto t = ctx.GetServerTrailingMetadata();
      if (auto it = t.find("parity-error"); it != t.end()) perr = std::string(it->second.data(), it->second.size());
      static const char* names[] = {"OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND",
                                    "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION",
                                    "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS",
                                    "UNAUTHENTICATED"};
      const std::string got = names[st.error_code()];
      if (got != e.at("status").get<std::string>() || perr != e.at("parity_error").get<std::string>()) {
        throw Fail{"expected " + e.at("status").get<std::string>() + "/" + e.at("parity_error").get<std::string>() +
                   ", got " + got + "/" + perr + " (" + st.error_message() + ")"};
      }
      return;
    }
    if (!st.ok()) throw Fail{"expected an answer, got " + std::to_string(st.error_code()) + " " + st.error_message()};
    Resp want;
    from_json(e.at("body").dump(), &want);
    compare(norm(want), norm(resp), "answer", agents);
  }

  template <class Req>
  void prepare(const json& step, grpc::ClientContext* ctx, Req* req) {
    const json md = step.value("metadata", json::object());  // a named copy: never iterate a temporary
    for (const auto& [k, v] : md.items()) ctx->AddMetadata(k, v.get<std::string>());
    ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    from_json(subst(step.at("body").dump()), req);
  }

#define UNARY(STUB, NS, METHOD)                                         \
  if (method == #METHOD) {                                              \
    grpc::ClientContext ctx;                                            \
    NS::METHOD##Request req;                                            \
    NS::METHOD##Response resp;                                          \
    prepare(step, &ctx, &req);                                          \
    grpc::Status st = STUB->METHOD(&ctx, req, &resp);                   \
    expect(step, st, ctx, resp, agents);                                \
    return;                                                             \
  }

  void call(const json& step, bool agents) {
    const std::string full = step.at("method").get<std::string>();
    const std::string method = full.substr(full.rfind('/') + 1);
    const bool is_agents = full.find("parity.agents.v1") == 0;
    if (method == "Watch") {
      json first;
      if (is_agents) {
        gw_ = std::make_unique<StreamReader<av1::WatchResponse>>();
        av1::WatchRequest req;
        prepare(step, &gw_->ctx, &req);
        gw_->ctx.set_deadline(std::chrono::system_clock::time_point::max());
        gw_->reader = agents_->Watch(&gw_->ctx, req);
        gw_->start();
        auto m = gw_->next(2000);
        if (!m) throw Fail{"no first message on the agents stream"};
        av1::WatchResponse want;
        from_json(step.at("expect").at("body").dump(), &want);
        compare(norm(want), norm(*m), "watch", agents);
      } else {
        auto s = std::make_unique<StreamReader<rv1::WatchResponse>>();
        rv1::WatchRequest req;
        prepare(step, &s->ctx, &req);
        s->ctx.set_deadline(std::chrono::system_clock::time_point::max());
        s->reader = routing_->Watch(&s->ctx, req);
        s->start();
        auto m = s->next(2000);
        if (!m) throw Fail{"no first message on stream " + step.value("stream", "")};
        rv1::WatchResponse want;
        from_json(step.at("expect").at("body").dump(), &want);
        compare(norm(want), norm(*m), "watch", agents);
        ie_[step.value("stream", "")] = std::move(s);
      }
      return;
    }
    if (is_agents) {
      UNARY(agents_, av1, AttachAgent)
      UNARY(agents_, av1, DetachAgent)
      UNARY(agents_, av1, SetState)
      UNARY(agents_, av1, AnswerOffer)
      UNARY(agents_, av1, EndWrapUp)
      UNARY(agents_, av1, Acknowledge)
      UNARY(agents_, av1, ForceState)
    } else {
      UNARY(routing_, rv1, Enqueue)
      UNARY(routing_, rv1, Dequeue)
      UNARY(routing_, rv1, ReportOutcome)
      UNARY(routing_, rv1, CallEnded)
      UNARY(routing_, rv1, Reattach)
      UNARY(routing_, rv1, Acknowledge)
      UNARY(routing_, rv1, ConvertToCallback)
      UNARY(routing_, rv1, CreateVoicemailTask)
      UNARY(routing_, rv1, LendAgents)  // phase 8 (0.14.0): campaign lending
      UNARY(routing_, rv1, ReturnAgents)
      UNARY(routing_, rv1, ReserveAgent)
    }
    throw Fail{"unknown method " + full};
  }

  // The next non-heartbeat message, skipping exact replays of messages already matched.
  template <class Msg>
  void deliver(StreamReader<Msg>& s, const json& step, bool agents) {
    Msg want;
    from_json(step.at("body").dump(), &want);
    const json w = norm(want);
    for (;;) {
      auto m = s.next(2000);
      if (!m) throw Fail{"no message delivered (expected " + w.dump() + ")"};
      const json h = norm(*m);
      // Unsequenced messages (Heartbeat, PositionUpdate) match only a step that asks for that kind.
      if (m->delivery_seq() == 0 && (want.delivery_seq() != 0 || m->event_case() != want.event_case())) continue;
      if (std::find(s.matched.begin(), s.matched.end(), h) != s.matched.end()) {
        // An exact replay of a message already matched: skipped, unless this step expects that
        // very repeat (a redelivery after 2 s without an ack).
        try {
          compare(w, h, "deliver", agents);
          return;
        } catch (const Fail&) {
          continue;
        }
      }
      compare(w, h, "deliver", agents);
      s.matched.push_back(h);
      return;
    }
  }

  // Reads the harness's agents stream until it is quiet, noting each agent's latest offer.
  void drain_offers() {
    control_->handle("POST", "/_test/v1/sync", "{}");
    if (!harness_gw_) return;
    while (auto m = harness_gw_->next(60)) {
      const std::string key = m->tenant_id() + "/" + m->agent_id();
      if (m->has_offer()) {
        latest_offer_[key] = m->offer().offer_id();
        ever_offered_.insert(key);
      }
    }
  }

  av1::SessionRef ref(const std::string& tenant, const std::string& agent) {
    auto it = sessions_.find(tenant + "/" + agent);
    if (it == sessions_.end()) throw Fail{"no session for " + agent};
    av1::SessionRef s;
    s.set_tenant_id(tenant);
    s.set_agent_id(agent);
    s.set_session_id(it->second.at("session_id").get<std::string>());
    s.set_attach_generation(it->second.at("attach_generation").get<uint64_t>());
    return s;
  }

  void inject(const json& step, bool agents) {
    const std::string what = step.at("what");
    const json& a = step.at("args");
    if (what == "queue_call" || what == "call_ended" || what == "call_connected" || what == "config_routing" ||
        what == "config_routing_pin" || what == "moves") {
      json args = json::parse(subst(a.dump()));
      if (!args.contains("tenant_id") && args.contains("doc")) args["tenant_id"] = args.at("doc").at("tenant_id");
      json out;
      control("/_test/v1/inject", {{"what", what}, {"args", args}}, &out);
      // 0.11.0: a config or moves stimulus may pin what RE made of it (args.expect: a subset of
      // the answer).
      if (a.contains("expect")) {
        for (const auto& [k, v] : a.at("expect").items()) {
          if (!out.contains(k) || out.at(k) != v) {
            throw Fail{what + ": expected " + k + "=" + v.dump() + ", got " + out.dump()};
          }
        }
      }
      return;
    }
    if (what == "supervisor_force") {
      // 0.11.0: a supervisor forces the agent's state, through agents.v1 ForceState.
      grpc::ClientContext ctx;
      ctx.AddMetadata("tenant-id", a.at("tenant_id").get<std::string>());
      av1::ForceStateRequest r;
      r.set_tenant_id(a.at("tenant_id").get<std::string>());
      r.set_agent_id(a.at("agent_id").get<std::string>());
      r.set_command_id("script-force:" + std::to_string(step_));
      av1::TargetState t = av1::TARGET_STATE_UNSPECIFIED;
      if (!av1::TargetState_Parse(a.at("target").get<std::string>(), &t)) throw Fail{"supervisor_force: target"};
      r.set_target(t);
      r.set_reason_code(a.value("reason_code", std::string()));
      r.set_supervisor_id(a.value("supervisor_id", std::string("supervisor-1")));
      av1::ForceStateResponse out;
      auto st = agents_->ForceState(&ctx, r, &out);
      if (!st.ok()) throw Fail{"supervisor_force: " + st.error_message()};
      return;
    }
    const std::string tenant = a.value("tenant_id", ""), agent = a.value("agent_id", "");
    if (what == "agent_ready") {
      grpc::ClientContext ctx;
      ctx.AddMetadata("tenant-id", tenant);
      av1::SetStateRequest r;
      *r.mutable_session() = ref(tenant, agent);
      r.set_command_id("script:" + std::to_string(step_));
      r.set_target(av1::TARGET_STATE_READY);
      av1::SetStateResponse out;
      auto st = agents_->SetState(&ctx, r, &out);
      if (!st.ok()) throw Fail{"agent_ready: " + st.error_message()};
      return;
    }
    if (what == "stream_drop") {
      const std::string label = a.value("stream", "");
      auto it = ie_.find(label);
      if (it == ie_.end()) throw Fail{"stream_drop: unknown stream"};
      it->second->stop();
      // The server sees the cancel asynchronously: wait until RE no longer counts the replica's
      // stream as live (L7's pause depends on it).
      const std::string replica = script_.at("streams").at(label).at("replica_id");
      for (int i = 0; i < 400 && hub_->ie_live(replica); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(5));
      if (hub_->ie_live(replica)) throw Fail{"stream_drop: the stream stayed live"};
      control_->handle("POST", "/_test/v1/sync", "{}");
      return;
    }
    if (what == "agent_answers") {
      drain_offers();
      const std::string key = tenant + "/" + agent;
      const std::string result = a.at("result");
      if (!ever_offered_.contains(key)) {
        if (result != "no_offer") throw Fail{agent + " was never offered a call (expected " + result + ")"};
        return;
      }
      if (result == "no_offer") throw Fail{agent + " was offered a call (expected no_offer)"};
      grpc::ClientContext ctx;
      ctx.AddMetadata("tenant-id", tenant);
      av1::AnswerOfferRequest r;
      *r.mutable_session() = ref(tenant, agent);
      r.set_command_id("script:" + std::to_string(step_));
      r.set_offer_id(latest_offer_[key]);
      av1::Decision d = av1::DECISION_UNSPECIFIED;
      if (!av1::Decision_Parse(a.at("decision").get<std::string>(), &d)) throw Fail{"decision"};
      r.set_decision(d);
      av1::AnswerOfferResponse out;
      auto st = agents_->AnswerOffer(&ctx, r, &out);
      std::string got;
      if (st.ok()) {
        got = out.decision() == av1::DECISION_ACCEPT ? "accepted" : "rejected";
      } else {
        auto t = ctx.GetServerTrailingMetadata();
        auto it = t.find("parity-error");
        got = it == t.end() ? st.error_message() : std::string(it->second.data(), it->second.size());
      }
      if (got != result) throw Fail{agent + " answered: expected " + result + ", got " + got};
      return;
    }
    throw Fail{"unknown inject " + what};
  }

  void run(const json& step, bool agents) {
    const std::string op = step.at("op");
    if (op == "call") return call(step, agents);
    if (op == "advance") {
      control("/_test/v1/advance", {{"ms", step.at("ms")}});
      return;
    }
    if (op == "inject") return inject(step, agents);
    if (op == "deliver") {
      if (agents) {
        if (!gw_) throw Fail{"deliver before Watch"};
        return deliver(*gw_, step, agents);
      }
      auto it = ie_.find(step.value("stream", ""));
      if (it == ie_.end()) throw Fail{"deliver on an unknown stream"};
      return deliver(*it->second, step, agents);
    }
    throw Fail{"unknown op " + op};
  }

  struct Late final : Sink {
    Hub* hub = nullptr;
    void to_ie(const std::string& r, uint64_t g, const rv1::WatchResponse& m) override { hub->to_ie(r, g, m); }
    void to_gw(const std::string& r, uint64_t g, const av1::WatchResponse& m) override { hub->to_gw(r, g, m); }
    bool ie_live(const std::string& r) const override { return hub->ie_live(r); }
    std::vector<std::string> ie_replicas() const override { return hub->ie_replicas(); }
  };

  std::string contracts_;
  ManualClock clock_{0};
  Late late_;
  MemoryStore store_;
  Metrics metrics_;
  std::unique_ptr<Engine> engine_;
  std::unique_ptr<Hub> hub_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<TestControl> control_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<rv1::RoutingService::Stub> routing_;
  std::unique_ptr<av1::AgentService::Stub> agents_;
  json script_;
  int step_ = 0;
  std::map<std::string, std::string> binds_;
  std::map<std::string, std::unique_ptr<StreamReader<rv1::WatchResponse>>> ie_;
  std::unique_ptr<StreamReader<av1::WatchResponse>> gw_, harness_gw_;
  std::map<std::string, json> sessions_;
  std::map<std::string, std::string> latest_offer_;
  std::set<std::string> ever_offered_;
};

// skip.txt: "<path relative to fixtures/>  <phase>  <reason>"; a skip without a phase fails.
std::map<std::string, std::string> read_skips(const std::string& path, bool* bad) {
  std::map<std::string, std::string> out;
  std::ifstream in(path);
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') continue;
    std::istringstream ss(line);
    std::string file, phase;
    ss >> file >> phase;
    if (phase.rfind("phase-", 0) != 0) {
      std::fprintf(stderr, "skip.txt: %s has no phase\n", file.c_str());
      *bad = true;
    }
    out[file] = line;
  }
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  std::string contracts = "external/+_repo_rules+parity_contracts";
  std::string skips = "tests/scripts/skip.txt";
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--contracts=", 0) == 0) contracts = a.substr(12);
    if (contracts.ends_with("/VERSION")) contracts = contracts.substr(0, contracts.size() - 8);
    if (a.rfind("--skips=", 0) == 0) skips = a.substr(8);
  }
  bool bad = false;
  auto skip = read_skips(skips, &bad);
  Runner runner(contracts);
  int pass = 0, fail = 0, skipped = 0;
  std::vector<fs::path> files;
  for (const char* pkg : {"routing.v1", "agents.v1"}) {
    for (const auto& e : fs::recursive_directory_iterator(fs::path(contracts) / "fixtures" / pkg)) {
      if (e.path().extension() == ".json") files.push_back(e.path());
    }
  }
  std::sort(files.begin(), files.end());
  for (const auto& f : files) {
    const std::string full = f.string();
    const std::string rel = full.substr(full.rfind("/fixtures/") + 10);
    std::ifstream in(f);
    const json s = json::parse(in);
    const bool executable = s.contains("given");
    if (skip.contains(rel)) {
      if (executable && rel.find("agents.v1") == std::string::npos) {
        std::printf("STALE-SKIP %s (it has a given block now)\n", rel.c_str());
        bad = true;
      }
      std::printf("SKIP %s\n", skip[rel].c_str());
      ++skipped;
      continue;
    }
    if (!executable) {
      std::printf("FAIL %s: no given block and not in skip.txt\n", rel.c_str());
      ++fail;
      continue;
    }
    const std::string why = runner.play(f);
    if (why.empty()) {
      std::printf("PASS %s\n", rel.c_str());
      ++pass;
    } else {
      std::printf("FAIL %s: %s\n", rel.c_str(), why.c_str());
      ++fail;
    }
  }
  std::printf("scripts: %d passed, %d failed, %d skipped\n", pass, fail, skipped);
  return (fail == 0 && !bad && pass > 0) ? 0 : 1;
}
