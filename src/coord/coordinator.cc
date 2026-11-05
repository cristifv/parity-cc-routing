#include "src/coord/coordinator.h"

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

#include <chrono>

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "src/ops/log.h"

namespace pcr {

namespace ep = ::etcdserverpb;

namespace {

int64_t mono_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

void deadline(grpc::ClientContext* c, int ms) {
  c->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(ms));
}

std::string prefix_end(const std::string& p) {
  std::string e = p;
  e.back() = static_cast<char>(e.back() + 1);
  return e;
}

const char* kTenants = "tp/tenants/";

}  // namespace

Coordinator::Coordinator(Options o, Hooks h) : opts_(std::move(o)), hooks_(std::move(h)) {}

Coordinator::~Coordinator() { stop(false); }

bool Coordinator::parse_tenant(const std::string& value, TenantAt* out) {
  try {
    const auto j = nlohmann::json::parse(value);
    out->bin = j.at("bin").get<std::string>();
    out->epoch = j.at("epoch").get<uint64_t>();
    return !out->bin.empty();
  } catch (const std::exception&) {
    return false;
  }
}

bool Coordinator::start(std::string* err) {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 2000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 1000);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
  ch_ = grpc::CreateCustomChannel(opts_.endpoint, grpc::InsecureChannelCredentials(), args);
  // tp/tenants/ first: the caller decides which tenants this bin serves before it campaigns.
  {
    auto kv = ep::KV::NewStub(ch_);
    grpc::ClientContext c;
    deadline(&c, 5000);
    ep::RangeRequest q;
    q.set_key(kTenants);
    q.set_range_end(prefix_end(kTenants));
    ep::RangeResponse r;
    const grpc::Status s = kv->Range(&c, q, &r);
    if (!s.ok()) {
      *err = "etcd " + opts_.endpoint + ": " + s.error_message();
      return false;
    }
    std::lock_guard lk(mu_);
    for (const auto& x : r.kvs()) {
      TenantAt at;
      if (parse_tenant(x.value(), &at)) tenants_[x.key().substr(std::string(kTenants).size())] = at;
    }
    tenants_rev_ = r.header().revision();
  }
  {
    auto lease = ep::Lease::NewStub(ch_);
    grpc::ClientContext c;
    deadline(&c, 5000);
    ep::LeaseGrantRequest q;
    q.set_ttl(opts_.ttl_s);
    ep::LeaseGrantResponse r;
    const int64_t sent = mono_ms();
    const grpc::Status s = lease->LeaseGrant(&c, q, &r);
    if (!s.ok() || !r.error().empty() || r.id() == 0) {
      *err = "etcd lease: " + (s.ok() ? r.error() : s.error_message());
      return false;
    }
    lease_ = r.id();
    deadline_ = sent + r.ttl() * 1000 - opts_.margin_ms;
  }
  ka_th_ = std::thread([this] { keepalive_loop(); });
  el_th_ = std::thread([this] { election_loop(); });
  tw_th_ = std::thread([this] { tenants_loop(); });
  return true;
}

void Coordinator::stop(bool resign) {
  if (stop_.exchange(true)) return;
  if (resign && leading_.load() && ch_) {
    auto kv = ep::KV::NewStub(ch_);
    grpc::ClientContext c;
    deadline(&c, 2000);
    ep::TxnRequest q;
    auto* cmp = q.add_compare();
    cmp->set_key("re/" + opts_.bin + "/leader");
    cmp->set_target(ep::Compare::CREATE);
    cmp->set_result(ep::Compare::EQUAL);
    cmp->set_create_revision(static_cast<int64_t>(epoch_.load()));
    q.add_success()->mutable_request_delete_range()->set_key("re/" + opts_.bin + "/leader");
    ep::TxnResponse r;
    (void)kv->Txn(&c, q, &r);
  }
  leading_ = false;
  if (ch_ && lease_.load() != 0) {
    auto lease = ep::Lease::NewStub(ch_);
    grpc::ClientContext c;
    deadline(&c, 1000);
    ep::LeaseRevokeRequest q;
    q.set_id(lease_.load());
    ep::LeaseRevokeResponse r;
    (void)lease->LeaseRevoke(&c, q, &r);
  }
  {
    std::lock_guard lk(ctx_mu_);
    for (auto* c : {ka_ctx_, el_ctx_, tw_ctx_}) {
      if (c) c->TryCancel();
    }
  }
  for (auto* t : {&ka_th_, &el_th_, &tw_th_}) {
    if (t->joinable()) t->join();
  }
}

bool Coordinator::may_lead() const {
  return leading_.load(std::memory_order_acquire) && !lost_.load(std::memory_order_acquire) &&
         mono_ms() < deadline_.load(std::memory_order_acquire);
}

int64_t Coordinator::lease_left_ms() const { return deadline_.load() - mono_ms(); }

std::string Coordinator::leader() const {
  std::lock_guard lk(mu_);
  return leader_;
}

std::map<std::string, Coordinator::TenantAt> Coordinator::tenants() const {
  std::lock_guard lk(mu_);
  return tenants_;
}

void Coordinator::set_leader(const std::string& who) {
  {
    std::lock_guard lk(mu_);
    if (leader_ == who) return;
    leader_ = who;
  }
  if (hooks_.leader_changed) hooks_.leader_changed(who);
}

void Coordinator::lose(const std::string& why) {
  if (!leading_.load() || lost_.exchange(true)) return;
  leading_ = false;
  ++stepdowns_;
  LogRec("warn", "leader.stepped_down", "stepping down").str("bin", opts_.bin).num("leader_epoch", static_cast<int64_t>(epoch_.load())).error("not_leader", why);
  if (hooks_.lost) hooks_.lost(why);
}

bool Coordinator::put(const std::string& key, const std::string& value, std::string* err) {
  auto kv = ep::KV::NewStub(ch_);
  grpc::ClientContext c;
  deadline(&c, 3000);
  ep::PutRequest q;
  q.set_key(key);
  q.set_value(value);
  ep::PutResponse r;
  const grpc::Status s = kv->Put(&c, q, &r);
  if (!s.ok() && err) *err = s.error_message();
  return s.ok();
}

void Coordinator::keepalive_loop() {
  auto stub = ep::Lease::NewStub(ch_);
  while (!stop_.load()) {
    grpc::ClientContext ctx;
    {
      std::lock_guard lk(ctx_mu_);
      if (stop_.load()) break;
      ka_ctx_ = &ctx;
    }
    auto stream = stub->LeaseKeepAlive(&ctx);
    std::mutex smu;
    std::deque<int64_t> sent;  // send times of the requests not yet answered (answers come in order)
    std::atomic<bool> broken{false}, expired{false};
    std::thread reader([&] {
      ep::LeaseKeepAliveResponse r;
      while (stream->Read(&r)) {
        int64_t t0 = 0;
        {
          std::lock_guard lk(smu);
          if (sent.empty()) continue;
          t0 = sent.front();
          sent.pop_front();
        }
        if (r.ttl() <= 0) {
          expired = true;
          break;
        }
        const int64_t d = t0 + r.ttl() * 1000 - opts_.margin_ms;
        int64_t cur = deadline_.load();
        while (d > cur && !deadline_.compare_exchange_weak(cur, d)) {
        }
        ++keepalives_;
      }
      broken = true;
    });
    int64_t next = 0;
    while (!stop_.load() && !broken.load()) {
      const int64_t now = mono_ms();
      if (leading_.load() && now >= deadline_.load()) lose("no keep-alive answer within TTL - 1.5 s");
      if (now >= next) {
        ep::LeaseKeepAliveRequest q;
        q.set_id(lease_.load());
        {
          std::lock_guard lk(smu);
          sent.push_back(now);
        }
        if (!stream->Write(q)) break;
        next = now + opts_.keepalive_ms;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    stream->WritesDone();
    ctx.TryCancel();
    reader.join();
    (void)stream->Finish();
    {
      std::lock_guard lk(ctx_mu_);
      ka_ctx_ = nullptr;
    }
    if (stop_.load()) break;
    if (expired.load()) {
      if (leading_.load()) {
        lose("the lease expired");
      } else {
        // A standby's lease ran out (it was paused): a new one, then it may campaign again.
        auto lease = ep::Lease::NewStub(ch_);
        grpc::ClientContext c;
        deadline(&c, 3000);
        ep::LeaseGrantRequest q;
        q.set_ttl(opts_.ttl_s);
        ep::LeaseGrantResponse r;
        const int64_t t0 = mono_ms();
        if (lease->LeaseGrant(&c, q, &r).ok() && r.id() != 0) {
          lease_ = r.id();
          deadline_ = t0 + r.ttl() * 1000 - opts_.margin_ms;
        }
      }
    }
    // Lost contact with etcd: the deadline is not extended meanwhile (a leader steps down by it).
    for (int i = 0; i < 4 && !stop_.load(); ++i) {
      if (leading_.load() && mono_ms() >= deadline_.load()) lose("no keep-alive answer within TTL - 1.5 s");
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
}

bool Coordinator::campaign(std::string* holder, int64_t* rev) {
  auto kv = ep::KV::NewStub(ch_);
  grpc::ClientContext c;
  deadline(&c, 3000);
  const std::string key = "re/" + opts_.bin + "/leader";
  ep::TxnRequest q;
  auto* cmp = q.add_compare();
  cmp->set_key(key);
  cmp->set_target(ep::Compare::CREATE);
  cmp->set_result(ep::Compare::EQUAL);
  cmp->set_create_revision(0);
  auto* put = q.add_success()->mutable_request_put();
  put->set_key(key);
  put->set_value(opts_.advertise);
  put->set_lease(lease_.load());
  q.add_failure()->mutable_request_range()->set_key(key);
  ep::TxnResponse r;
  const int64_t sent = mono_ms();
  const grpc::Status s = kv->Txn(&c, q, &r);
  if (!s.ok()) return false;
  *rev = r.header().revision();
  if (r.succeeded()) {
    // The put happened at this revision: the key's create revision is the leader_epoch.
    epoch_ = static_cast<uint64_t>(r.header().revision());
    // The lease was alive when the txn was sent (etcd refuses an unknown lease).
    int64_t cur = deadline_.load();
    const int64_t d = sent + static_cast<int64_t>(opts_.ttl_s) * 1000 - opts_.margin_ms;
    if (d < cur) deadline_ = d;
    leading_ = true;
    return true;
  }
  holder->clear();
  if (r.responses_size() > 0 && r.responses(0).response_range().kvs_size() > 0) {
    *holder = r.responses(0).response_range().kvs(0).value();
  }
  return true;
}

// Returns when the leader key is deleted or replaced (or on stop).
void Coordinator::watch_leader(int64_t from_rev) {
  auto stub = ep::Watch::NewStub(ch_);
  const std::string key = "re/" + opts_.bin + "/leader";
  while (!stop_.load()) {
    grpc::ClientContext ctx;
    {
      std::lock_guard lk(ctx_mu_);
      if (stop_.load()) return;
      el_ctx_ = &ctx;
    }
    auto stream = stub->Watch(&ctx);
    ep::WatchRequest q;
    q.mutable_create_request()->set_key(key);
    q.mutable_create_request()->set_start_revision(from_rev);
    bool gone = false;
    if (stream->Write(q)) {
      ep::WatchResponse r;
      while (!gone && stream->Read(&r)) {
        if (r.canceled()) break;  // compacted: look again
        for (const auto& e : r.events()) {
          from_rev = e.kv().mod_revision() + 1;
          if (e.type() == mvccpb::Event::DELETE) {
            gone = true;
          } else if (leading_.load() && static_cast<uint64_t>(e.kv().create_revision()) != epoch_.load()) {
            gone = true;
          } else if (!leading_.load()) {
            set_leader(e.kv().value());
          }
        }
      }
    }
    stream->WritesDone();
    ctx.TryCancel();
    (void)stream->Finish();
    {
      std::lock_guard lk(ctx_mu_);
      el_ctx_ = nullptr;
    }
    if (gone) return;
    if (stop_.load()) return;
    // The stream broke (etcd unreachable) or was compacted: is the key still there?
    auto kv = ep::KV::NewStub(ch_);
    grpc::ClientContext c;
    deadline(&c, 2000);
    ep::RangeRequest rq;
    rq.set_key(key);
    ep::RangeResponse rr;
    if (kv->Range(&c, rq, &rr).ok()) {
      if (rr.kvs_size() == 0) return;
      if (leading_.load() && static_cast<uint64_t>(rr.kvs(0).create_revision()) != epoch_.load()) return;
      from_rev = rr.header().revision() + 1;
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }
}

void Coordinator::election_loop() {
  while (!stop_.load()) {
    std::string holder;
    int64_t rev = 0;
    if (!campaign(&holder, &rev)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      continue;
    }
    if (leading_.load()) {
      set_leader(opts_.advertise);
      LogRec("info", "leader.elected", "elected").str("bin", opts_.bin).num("leader_epoch", static_cast<int64_t>(epoch_.load()));
      if (hooks_.won) hooks_.won(epoch_.load());
      watch_leader(rev + 1);
      if (!stop_.load()) lose("the leader key is gone or replaced");
      return;  // a leader that lost never leads again in this process (it exits; PLAN §5.5)
    }
    set_leader(holder);
    watch_leader(rev + 1);
    set_leader("");
  }
}

void Coordinator::tenants_loop() {
  auto stub = ep::Watch::NewStub(ch_);
  int64_t from = 0;
  {
    std::lock_guard lk(mu_);
    from = tenants_rev_ + 1;
  }
  const size_t plen = std::string(kTenants).size();
  while (!stop_.load()) {
    grpc::ClientContext ctx;
    {
      std::lock_guard lk(ctx_mu_);
      if (stop_.load()) return;
      tw_ctx_ = &ctx;
    }
    auto stream = stub->Watch(&ctx);
    ep::WatchRequest q;
    q.mutable_create_request()->set_key(kTenants);
    q.mutable_create_request()->set_range_end(prefix_end(kTenants));
    q.mutable_create_request()->set_start_revision(from);
    bool resync = false;
    if (stream->Write(q)) {
      ep::WatchResponse r;
      while (stream->Read(&r)) {
        if (r.canceled()) {
          resync = true;
          break;
        }
        for (const auto& e : r.events()) {
          from = e.kv().mod_revision() + 1;
          const std::string tenant = e.kv().key().substr(plen);
          TenantAt at;
          const bool del = e.type() == mvccpb::Event::DELETE;
          if (!del && !parse_tenant(e.kv().value(), &at)) {
            LogRec("warn", "tenant.assignment_ignored", "tp/tenants: unreadable value (ignored)").tenant(tenant);
            continue;
          }
          {
            std::lock_guard lk(mu_);
            if (del) {
              tenants_.erase(tenant);
            } else {
              tenants_[tenant] = at;
            }
          }
          if (hooks_.tenant_changed) hooks_.tenant_changed(tenant, at, del);
        }
      }
    }
    stream->WritesDone();
    ctx.TryCancel();
    (void)stream->Finish();
    {
      std::lock_guard lk(ctx_mu_);
      tw_ctx_ = nullptr;
    }
    if (stop_.load()) return;
    if (resync) {
      // Compacted past `from`: read the prefix again and report what differs.
      auto kv = ep::KV::NewStub(ch_);
      grpc::ClientContext c;
      deadline(&c, 3000);
      ep::RangeRequest rq;
      rq.set_key(kTenants);
      rq.set_range_end(prefix_end(kTenants));
      ep::RangeResponse rr;
      if (kv->Range(&c, rq, &rr).ok()) {
        std::map<std::string, TenantAt> now;
        for (const auto& x : rr.kvs()) {
          TenantAt at;
          if (parse_tenant(x.value(), &at)) now[x.key().substr(plen)] = at;
        }
        std::map<std::string, TenantAt> before;
        {
          std::lock_guard lk(mu_);
          before = tenants_;
          tenants_ = now;
        }
        for (const auto& [t, at] : now) {
          auto b = before.find(t);
          if (b == before.end() || b->second.bin != at.bin || b->second.epoch != at.epoch) {
            if (hooks_.tenant_changed) hooks_.tenant_changed(t, at, false);
          }
        }
        for (const auto& [t, at] : before) {
          if (!now.contains(t) && hooks_.tenant_changed) hooks_.tenant_changed(t, at, true);
        }
        from = rr.header().revision() + 1;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}

}  // namespace pcr
