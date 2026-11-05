#include "src/server/services.h"

#include <atomic>
#include <map>

#include "src/ops/log.h"

namespace pcr {

namespace {

constexpr const char* kRequests = "parity_cc_routing_requests_total";

std::string md(const grpc::CallbackServerContext* ctx, const char* key) {
  const auto& m = ctx->client_metadata();
  auto it = m.find(key);
  return it == m.end() ? std::string() : std::string(it->second.data(), it->second.size());
}

std::string labels(const char* rpc, const std::string& code) {
  return std::string("rpc=\"") + rpc + "\",code=\"" + code + "\"";
}

// Who a request was about, for the refusal log (ids only, CR-069). Only ids that validate are
// logged; anything else is "(invalid)", never the caller's raw string (CR-082).
struct Ids {
  std::string tenant, interaction, agent;
  static Ids of(const std::string& t, const std::string& ix, const std::string& a) {
    auto keep = [](bool ok, const std::string& v) { return v.empty() ? v : ok ? v : std::string("(invalid)"); };
    return Ids{keep(valid_tenant_id(t), t), keep(valid_uuid(ix), ix), keep(valid_agent_id(a) && a.size() <= 128, a)};
  }
};

// CR-069: a refusal is logged with its ids (rate-limited per rpc and code), so on-call can match
// IE's "routing-engine refused" errors. Ordinary races (an offer already gone, a superseded
// session) are only counted.
void log_refusal(const char* rpc, const std::string& code, const Ids& ids) {
  if (code == "offer_gone" || code == "session_superseded") return;
  static RateLimit limit(1000);
  uint64_t held = 0;
  int64_t since = 0;
  if (!limit.allow(std::string(rpc) + "|" + code, &held, &since)) return;
  // log-record.v1 request.refused: dedup per (op, error.code) a second; the held-back ones are its
  // repeat_count (CONVENTIONS "Rate limits and dedup").
  const bool contract_code = !code.empty() && code.front() >= 'a' && code.front() <= 'z';
  LogRec("warn", "request.refused", "request refused")
      .op(rpc)
      .error(contract_code ? code : "internal", contract_code ? std::string() : "grpc status " + code)
      .tenant(ids.tenant == "(invalid)" ? std::string() : ids.tenant)
      .xsid(ids.interaction == "(invalid)" ? std::string() : ids.interaction)
      .str("agent_id", ids.agent == "(invalid)" ? std::string() : ids.agent)
      .repeat(held, since);
}

// Answers a unary call with a Result: the canonical status plus the parity-error trailer.
// Phase 6: called on a shard writer, the answer waits for the commit of what the call changed
// (gate::after_commit); everywhere else it goes at once.
template <class Resp>
void reply_now(grpc::CallbackServerContext* ctx, grpc::ServerUnaryReactor* r, Resp* out,
               Result<Resp>&& res, Metrics* m, const char* rpc, const Ids& ids) {
  if (res) {
    *out = std::move(*res);
    m->inc(kRequests, labels(rpc, "ok"));
    r->Finish(grpc::Status::OK);
    return;
  }
  const Error& e = res.error();
  if (!e.parity.empty()) ctx->AddTrailingMetadata("parity-error", e.parity);
  const std::string code = e.parity.empty() ? std::to_string(static_cast<int>(e.code)) : e.parity;
  m->inc(kRequests, labels(rpc, code));
  log_refusal(rpc, code, ids);
  r->Finish(grpc::Status(static_cast<grpc::StatusCode>(e.code), e.message));
}

template <class Resp>
void reply(grpc::CallbackServerContext* ctx, grpc::ServerUnaryReactor* r, Resp* out,
           Result<Resp>&& res, Metrics* m, const char* rpc, const Ids& ids) {
  if (!gate::current()) {
    reply_now<Resp>(ctx, r, out, std::move(res), m, rpc, ids);
    return;
  }
  auto held = std::make_shared<Result<Resp>>(std::move(res));
  gate::after_commit([=] { reply_now<Resp>(ctx, r, out, std::move(*held), m, rpc, ids); });
}

void fail_now(grpc::CallbackServerContext* ctx, grpc::ServerUnaryReactor* r, Code c,
              const std::string& parity, Metrics* m, const char* rpc, const Ids& ids, const std::string& leader) {
  if (!parity.empty()) ctx->AddTrailingMetadata("parity-error", parity);
  if (parity == "not_leader" && !leader.empty()) ctx->AddTrailingMetadata("parity-leader", leader);
  const std::string code = parity.empty() ? std::to_string(static_cast<int>(c)) : parity;
  m->inc(kRequests, labels(rpc, code));
  if (c != Code::kUnimplemented && parity != "not_leader") log_refusal(rpc, code, ids);
  r->Finish(grpc::Status(static_cast<grpc::StatusCode>(c), parity));
}

void fail(grpc::CallbackServerContext* ctx, grpc::ServerUnaryReactor* r, Code c,
          const std::string& parity, Metrics* m, const char* rpc, const Ids& ids = {}) {
  if (!gate::current()) {
    fail_now(ctx, r, c, parity, m, rpc, ids, "");
    return;
  }
  gate::after_commit([=] { fail_now(ctx, r, c, parity, m, rpc, ids, ""); });
}

// A stream refused before it opens.
template <class Msg>
class Refused final : public grpc::ServerWriteReactor<Msg> {
 public:
  Refused(grpc::CallbackServerContext* ctx, Code c, const std::string& parity, const std::string& leader = "") {
    if (!parity.empty()) ctx->AddTrailingMetadata("parity-error", parity);
    if (!leader.empty()) ctx->AddTrailingMetadata("parity-leader", leader);
    this->Finish(grpc::Status(static_cast<grpc::StatusCode>(c), parity));
  }
  void OnDone() override { delete this; }
};

// routing.v1's writer rules (CONVENTIONS): an owner with a tenant equal to the tenant-id
// metadata, an interaction UUID, an epoch of at least 1, a replica.
std::optional<std::pair<Code, std::string>> check_owner(const grpc::CallbackServerContext* ctx,
                                                        bool has_owner,
                                                        const rv1::InteractionOwner& o) {
  if (!has_owner) return std::make_pair(Code::kInvalidArgument, std::string("invalid_request"));
  if (md(ctx, "tenant-id") != o.tenant_id()) {
    return std::make_pair(Code::kInvalidArgument, std::string("tenant_mismatch"));
  }
  if (!valid_tenant_id(o.tenant_id()) || !valid_uuid(o.interaction_id()) || o.owner_epoch() < 1 ||
      o.replica_id().empty()) {
    return std::make_pair(Code::kInvalidArgument, std::string("invalid_request"));
  }
  return std::nullopt;
}

// Phase 8: a campaign request's owner (routing.v1 CampaignOwner): the tenant equal to the tenant-id
// metadata, a campaign id (opaque), a leader fence of at least 1, a replica.
std::optional<std::pair<Code, std::string>> check_campaign(const grpc::CallbackServerContext* ctx, bool has_owner,
                                                           const rv1::CampaignOwner& o) {
  if (!has_owner) return std::make_pair(Code::kInvalidArgument, std::string("invalid_request"));
  if (md(ctx, "tenant-id") != o.tenant_id()) {
    return std::make_pair(Code::kInvalidArgument, std::string("tenant_mismatch"));
  }
  if (!valid_tenant_id(o.tenant_id()) || !valid_agent_id(o.campaign_id()) || o.owner_epoch() < 1 ||
      o.replica_id().empty() || o.replica_id().size() > 128) {
    return std::make_pair(Code::kInvalidArgument, std::string("invalid_request"));
  }
  return std::nullopt;
}

}  // namespace

// ======================================================================================

class Server::Routing final : public rv1::RoutingService::CallbackService {
 public:
  Routing(Server* s) : s_(s) {}

  template <class Req, class Resp, class Body>
  grpc::ServerUnaryReactor* unary(grpc::CallbackServerContext* ctx, const Req* req, Resp* out,
                                  const char* rpc, Body body) {
    auto* r = ctx->DefaultReactor();
    const Ids ids = Ids::of(req->owner().tenant_id(), req->owner().interaction_id(), "");
    if (s_->not_leader(ctx, r, rpc)) return r;
    if (auto bad = check_owner(ctx, req->has_owner(), req->owner())) {
      fail(ctx, r, bad->first, bad->second, s_->metrics_, rpc, ids);
      return r;
    }
    Metrics* m = s_->metrics_;
    const bool ok = s_->engine_->post(req->owner().tenant_id(), [=](Tenant* t, int64_t now) {
      if (!t) {
        fail(ctx, r, Code::kFailedPrecondition, "tenant_not_in_bin", m, rpc, ids);
        return;
      }
      reply<Resp>(ctx, r, out, body(t, now), m, rpc, ids);
    });
    if (!ok) fail(ctx, r, Code::kUnavailable, "", m, rpc, ids);
    return r;
  }

  grpc::ServerUnaryReactor* Enqueue(grpc::CallbackServerContext* ctx, const rv1::EnqueueRequest* req,
                                    rv1::EnqueueResponse* out) override {
    return unary(ctx, req, out, "Enqueue", [req](Tenant* t, int64_t now) { return t->enqueue(*req, now); });
  }
  grpc::ServerUnaryReactor* Dequeue(grpc::CallbackServerContext* ctx, const rv1::DequeueRequest* req,
                                    rv1::DequeueResponse* out) override {
    return unary(ctx, req, out, "Dequeue", [req](Tenant* t, int64_t now) { return t->dequeue(*req, now); });
  }
  grpc::ServerUnaryReactor* ReportOutcome(grpc::CallbackServerContext* ctx,
                                          const rv1::ReportOutcomeRequest* req,
                                          rv1::ReportOutcomeResponse* out) override {
    return unary(ctx, req, out, "ReportOutcome",
                 [req](Tenant* t, int64_t now) { return t->report_outcome(*req, now); });
  }
  grpc::ServerUnaryReactor* CallEnded(grpc::CallbackServerContext* ctx, const rv1::CallEndedRequest* req,
                                      rv1::CallEndedResponse* out) override {
    return unary(ctx, req, out, "CallEnded", [req](Tenant* t, int64_t now) { return t->call_ended(*req, now); });
  }
  grpc::ServerUnaryReactor* Reattach(grpc::CallbackServerContext* ctx, const rv1::ReattachRequest* req,
                                     rv1::ReattachResponse* out) override {
    return unary(ctx, req, out, "Reattach", [req](Tenant* t, int64_t now) { return t->reattach(*req, now); });
  }
  // Phase 8: campaign lending. LendAgents and ReturnAgents are about a campaign (CampaignOwner),
  // ReserveAgent about the campaign call's interaction (and its campaign).
  template <class Req, class Resp, class Body>
  grpc::ServerUnaryReactor* campaign_unary(grpc::CallbackServerContext* ctx, const Req* req, Resp* out,
                                           const char* rpc, Body body) {
    auto* r = ctx->DefaultReactor();
    const Ids ids = Ids::of(req->owner().tenant_id(), "", "");
    if (s_->not_leader(ctx, r, rpc)) return r;
    if (auto bad = check_campaign(ctx, req->has_owner(), req->owner())) {
      fail(ctx, r, bad->first, bad->second, s_->metrics_, rpc, ids);
      return r;
    }
    Metrics* m = s_->metrics_;
    const bool ok = s_->engine_->post(req->owner().tenant_id(), [=](Tenant* t, int64_t now) {
      if (!t) {
        fail(ctx, r, Code::kFailedPrecondition, "tenant_not_in_bin", m, rpc, ids);
        return;
      }
      reply<Resp>(ctx, r, out, body(t, now), m, rpc, ids);
    });
    if (!ok) fail(ctx, r, Code::kUnavailable, "", m, rpc, ids);
    return r;
  }
  grpc::ServerUnaryReactor* LendAgents(grpc::CallbackServerContext* ctx, const rv1::LendAgentsRequest* req,
                                       rv1::LendAgentsResponse* out) override {
    return campaign_unary(ctx, req, out, "LendAgents", [req](Tenant* t, int64_t now) { return t->lend_agents(*req, now); });
  }
  grpc::ServerUnaryReactor* ReturnAgents(grpc::CallbackServerContext* ctx, const rv1::ReturnAgentsRequest* req,
                                         rv1::ReturnAgentsResponse* out) override {
    return campaign_unary(ctx, req, out, "ReturnAgents", [req](Tenant* t, int64_t now) { return t->return_agents(*req, now); });
  }
  grpc::ServerUnaryReactor* ReserveAgent(grpc::CallbackServerContext* ctx, const rv1::ReserveAgentRequest* req,
                                         rv1::ReserveAgentResponse* out) override {
    return unary(ctx, req, out, "ReserveAgent", [req](Tenant* t, int64_t now) { return t->reserve_agent(*req, now); });
  }

  // Phase 3: callbacks and voicemail tasks are durable (Postgres) before the answer, which comes
  // later from the tenant's writer (the store's completion). Postgres down: UNAVAILABLE /
  // store_unavailable, nothing changed.
  template <class Req, class Resp, class Body>
  grpc::ServerUnaryReactor* deferred(grpc::CallbackServerContext* ctx, const Req* req, Resp* out,
                                     const char* rpc, Body body) {
    auto* r = ctx->DefaultReactor();
    const Ids ids = Ids::of(req->owner().tenant_id(), req->owner().interaction_id(), "");
    if (s_->not_leader(ctx, r, rpc)) return r;
    if (auto bad = check_owner(ctx, req->has_owner(), req->owner())) {
      fail(ctx, r, bad->first, bad->second, s_->metrics_, rpc, ids);
      return r;
    }
    Metrics* m = s_->metrics_;
    const bool ok = s_->engine_->post(req->owner().tenant_id(), [=](Tenant* t, int64_t now) {
      if (!t) {
        fail(ctx, r, Code::kFailedPrecondition, "tenant_not_in_bin", m, rpc, ids);
        return;
      }
      body(t, now, [=](Result<Resp> res) { reply<Resp>(ctx, r, out, std::move(res), m, rpc, ids); });
    });
    if (!ok) fail(ctx, r, Code::kUnavailable, "", m, rpc, ids);
    return r;
  }
  grpc::ServerUnaryReactor* ConvertToCallback(grpc::CallbackServerContext* ctx,
                                              const rv1::ConvertToCallbackRequest* req,
                                              rv1::ConvertToCallbackResponse* out) override {
    return deferred(ctx, req, out, "ConvertToCallback", [req](Tenant* t, int64_t now, Tenant::CallbackDone done) {
      t->convert_to_callback(*req, now, std::move(done));
    });
  }
  grpc::ServerUnaryReactor* CreateVoicemailTask(grpc::CallbackServerContext* ctx,
                                                const rv1::CreateVoicemailTaskRequest* req,
                                                rv1::CreateVoicemailTaskResponse* out) override {
    return deferred(ctx, req, out, "CreateVoicemailTask", [req](Tenant* t, int64_t now, Tenant::TaskDone done) {
      t->create_voicemail_task(*req, now, std::move(done));
    });
  }

  // Batched acks span tenants (no tenant-id metadata): each goes to its tenant's writer; stale or
  // foreign ones are ignored and counted.
  grpc::ServerUnaryReactor* Acknowledge(grpc::CallbackServerContext* ctx, const rv1::AcknowledgeRequest* req,
                                        rv1::AcknowledgeResponse* out) override {
    auto* r = ctx->DefaultReactor();
    if (s_->not_leader(ctx, r, "Acknowledge")) return r;
    std::map<std::string, std::vector<int>> by_tenant;
    uint32_t bad = 0;
    for (int i = 0; i < req->acks_size(); ++i) {
      const auto& a = req->acks(i);
      if (!valid_tenant_id(a.tenant_id()) || !s_->engine_->has_tenant(a.tenant_id())) {
        ++bad;
        continue;
      }
      by_tenant[a.tenant_id()].push_back(i);
    }
    if (by_tenant.empty()) {
      out->set_ignored(bad);
      s_->metrics_->inc(kRequests, labels("Acknowledge", "ok"));
      r->Finish(grpc::Status::OK);
      return r;
    }
    struct Gather {
      std::atomic<int> left;
      std::atomic<uint32_t> ignored;
    };
    auto g = std::make_shared<Gather>();
    g->left = static_cast<int>(by_tenant.size());
    g->ignored = bad;
    Metrics* m = s_->metrics_;
    for (auto& [tenant, idx] : by_tenant) {
      s_->engine_->post(tenant, [=, idx = idx](Tenant* t, int64_t now) {
        uint32_t ign = 0;
        for (int i : idx) {
          if (!t || !t->ack(req->acks(i), now)) ++ign;
        }
        g->ignored += ign;
        if (--g->left == 0) {
          gate::after_commit([=] {
            out->set_ignored(g->ignored.load());
            m->inc(kRequests, labels("Acknowledge", "ok"));
            r->Finish(grpc::Status::OK);
          });
        }
      }, /*external=*/false);
    }
    return r;
  }

  grpc::ServerWriteReactor<rv1::WatchResponse>* Watch(grpc::CallbackServerContext* ctx,
                                                      const rv1::WatchRequest* req) override {
    if (s_->opts_.serving && !s_->opts_.serving()) {
      s_->metrics_->inc(kRequests, labels("Watch", "not_leader"));
      return new Refused<rv1::WatchResponse>(ctx, Code::kUnavailable, "not_leader", s_->opts_.leader ? s_->opts_.leader() : "");
    }
    if (req->replica_id().empty() || req->bin() != s_->opts_.bin) {
      s_->metrics_->inc(kRequests, labels("Watch", "invalid_request"));
      return new Refused<rv1::WatchResponse>(ctx, Code::kInvalidArgument, "invalid_request");
    }
    uint64_t gen = 0;
    auto* w = s_->hub_->open_ie(ctx, req->replica_id(), s_->clock_->now_ms(), &gen);
    const std::string replica = req->replica_id();
    // After the stream is registered: a writer that runs first holds its messages back (they
    // are unacknowledged) until this replay sends them in order (CR-053).
    s_->engine_->post_all_tenants([replica, gen](Tenant* t, int64_t now) {
      if (t) t->on_ie_stream_open(replica, gen, now);
    });
    s_->metrics_->inc(kRequests, labels("Watch", "ok"));
    return w;
  }

 private:
  Server* s_;
};

// ======================================================================================

class Server::Agents final : public av1::AgentService::CallbackService {
 public:
  Agents(Server* s) : s_(s) {}

  template <class Resp, class Body>
  grpc::ServerUnaryReactor* unary(grpc::CallbackServerContext* ctx, const std::string& tenant,
                                  const std::string& agent, Resp* out, const char* rpc, Body body) {
    auto* r = ctx->DefaultReactor();
    const Ids ids = Ids::of(tenant, "", agent);
    if (s_->not_leader(ctx, r, rpc)) return r;
    if (md(ctx, "tenant-id") != tenant) {
      fail(ctx, r, Code::kInvalidArgument, "tenant_mismatch", s_->metrics_, rpc, ids);
      return r;
    }
    if (!valid_tenant_id(tenant)) {
      fail(ctx, r, Code::kInvalidArgument, "invalid_request", s_->metrics_, rpc, Ids{});
      return r;
    }
    Metrics* m = s_->metrics_;
    const bool ok = s_->engine_->post(tenant, [=](Tenant* t, int64_t now) {
      if (!t) {
        fail(ctx, r, Code::kFailedPrecondition, "tenant_not_in_bin", m, rpc, ids);  // 0.11.0 (CR-014)
        return;
      }
      reply<Resp>(ctx, r, out, body(t, now), m, rpc, ids);
    });
    if (!ok) fail(ctx, r, Code::kUnavailable, "", m, rpc, ids);
    return r;
  }

  grpc::ServerUnaryReactor* AttachAgent(grpc::CallbackServerContext* ctx, const av1::AttachAgentRequest* req,
                                        av1::AttachAgentResponse* out) override {
    return unary(ctx, req->tenant_id(), req->agent_id(), out, "AttachAgent",
                 [req](Tenant* t, int64_t now) { return t->attach(*req, now); });
  }
  grpc::ServerUnaryReactor* DetachAgent(grpc::CallbackServerContext* ctx, const av1::DetachAgentRequest* req,
                                        av1::DetachAgentResponse* out) override {
    return unary(ctx, req->session().tenant_id(), req->session().agent_id(), out, "DetachAgent",
                 [req](Tenant* t, int64_t now) { return t->detach(*req, now); });
  }
  grpc::ServerUnaryReactor* SetState(grpc::CallbackServerContext* ctx, const av1::SetStateRequest* req,
                                     av1::SetStateResponse* out) override {
    return unary(ctx, req->session().tenant_id(), req->session().agent_id(), out, "SetState",
                 [req](Tenant* t, int64_t now) { return t->set_state(*req, now); });
  }
  grpc::ServerUnaryReactor* AnswerOffer(grpc::CallbackServerContext* ctx, const av1::AnswerOfferRequest* req,
                                        av1::AnswerOfferResponse* out) override {
    return unary(ctx, req->session().tenant_id(), req->session().agent_id(), out, "AnswerOffer",
                 [req](Tenant* t, int64_t now) { return t->answer_offer(*req, now); });
  }
  grpc::ServerUnaryReactor* EndWrapUp(grpc::CallbackServerContext* ctx, const av1::EndWrapUpRequest* req,
                                      av1::EndWrapUpResponse* out) override {
    return unary(ctx, req->session().tenant_id(), req->session().agent_id(), out, "EndWrapUp",
                 [req](Tenant* t, int64_t now) { return t->end_wrap_up(*req, now); });
  }
  // 0.11.0: a supervisor's forced state (the gateway authorizes the supervisor; RE checks the tenant).
  grpc::ServerUnaryReactor* ForceState(grpc::CallbackServerContext* ctx, const av1::ForceStateRequest* req,
                                       av1::ForceStateResponse* out) override {
    return unary(ctx, req->tenant_id(), req->agent_id(), out, "ForceState",
                 [req](Tenant* t, int64_t now) { return t->force_state(*req, now); });
  }
  grpc::ServerUnaryReactor* Acknowledge(grpc::CallbackServerContext* ctx, const av1::AcknowledgeRequest* req,
                                        av1::AcknowledgeResponse*) override {
    auto* r = ctx->DefaultReactor();
    if (s_->not_leader(ctx, r, "AgentsAcknowledge")) return r;
    std::map<std::string, std::vector<av1::Ack>> by_tenant;
    for (const auto& a : req->acks()) {
      if (valid_tenant_id(a.tenant_id())) by_tenant[a.tenant_id()].push_back(a);
    }
    for (auto& [tenant, acks] : by_tenant) {
      s_->engine_->post(tenant, [acks = std::move(acks)](Tenant* t, int64_t) {
        if (!t) return;
        for (const auto& a : acks) t->gw_ack(a);
      }, /*external=*/false);
    }
    s_->metrics_->inc(kRequests, labels("AgentsAcknowledge", "ok"));
    r->Finish(grpc::Status::OK);
    return r;
  }
  grpc::ServerWriteReactor<av1::WatchResponse>* Watch(grpc::CallbackServerContext* ctx,
                                                      const av1::WatchRequest* req) override {
    if (s_->opts_.serving && !s_->opts_.serving()) {
      s_->metrics_->inc(kRequests, labels("AgentsWatch", "not_leader"));
      return new Refused<av1::WatchResponse>(ctx, Code::kUnavailable, "not_leader", s_->opts_.leader ? s_->opts_.leader() : "");
    }
    if (req->replica_id().empty() || req->bin() != s_->opts_.bin) {
      s_->metrics_->inc(kRequests, labels("AgentsWatch", "invalid_request"));
      return new Refused<av1::WatchResponse>(ctx, Code::kInvalidArgument, "invalid_request");
    }
    uint64_t gen = 0;
    auto* w = s_->hub_->open_gw(ctx, req->replica_id(), s_->clock_->now_ms(), &gen);
    const std::string replica = req->replica_id();
    s_->engine_->post_all_tenants([replica, gen](Tenant* t, int64_t now) {
      if (t) t->on_gw_stream_open(replica, gen, now);
    });
    s_->metrics_->inc(kRequests, labels("AgentsWatch", "ok"));
    return w;
  }

 private:
  Server* s_;
};

// ======================================================================================

Server::Server(ServerOptions opts, const Clock* clock, Engine* engine, Hub* hub, Metrics* metrics)
    : opts_(std::move(opts)), clock_(clock), engine_(engine), hub_(hub), metrics_(metrics),
      routing_(std::make_unique<Routing>(this)), agents_(std::make_unique<Agents>(this)) {}

Server::~Server() { shutdown(0); }

bool Server::not_leader(grpc::CallbackServerContext* ctx, grpc::ServerUnaryReactor* r, const char* rpc) {
  if (!opts_.serving || opts_.serving()) return false;
  fail_now(ctx, r, Code::kUnavailable, "not_leader", metrics_, rpc, Ids{}, opts_.leader ? opts_.leader() : "");
  return true;
}

bool Server::start() {
  grpc::ServerBuilder b;
  b.AddListeningPort(opts_.listen, grpc::InsecureServerCredentials(), &port_);  // mTLS: phase 6
  b.RegisterService(routing_.get());
  b.RegisterService(agents_.get());
  b.SetMaxReceiveMessageSize(1 << 20);
  b.SetMaxSendMessageSize(1 << 20);
  server_ = b.BuildAndStart();
  return server_ != nullptr && port_ != 0;
}

void Server::shutdown(int grace_ms) {
  if (!server_) return;
  hub_->close_all();
  server_->Shutdown(std::chrono::system_clock::now() + std::chrono::milliseconds(grace_ms));
  server_->Wait();
  server_.reset();
}

}  // namespace pcr
