#include "src/server/hub.h"

#include <vector>

namespace pcr {

namespace {
constexpr int64_t kHeartbeatMs = 5'000;
}

template <class Msg>
bool StreamState<Msg>::push(const Msg& m, size_t max_buffered) {
  WatchStream<Msg>* start = nullptr;
  bool slow = false;
  {
    std::lock_guard lk(mu);
    if (finish_called || close_pending) return false;
    if (q.size() >= max_buffered) {
      slow = true;
    } else {
      q.push_back(m);
      if (!writing) {
        writing = true;
        cur = std::move(q.front());
        q.pop_front();
        start = r;
      }
    }
  }
  if (slow) {
    close(grpc::Status(grpc::StatusCode::UNAVAILABLE, "slow consumer"), "");
    return false;
  }
  if (start) start->StartWrite(&cur);
  return true;
}

template <class Msg>
void StreamState<Msg>::close(grpc::Status s, const std::string& parity_error) {
  WatchStream<Msg>* fin = nullptr;
  {
    std::lock_guard lk(mu);
    if (finish_called || close_pending) return;
    closing.store(true);
    if (!parity_error.empty() && ctx) ctx->AddTrailingMetadata("parity-error", parity_error);
    if (writing) {
      close_pending = true;
      close_status = s;
      return;
    }
    finish_called = true;
    fin = r;
  }
  if (fin) fin->Finish(s);
}

template <class Msg>
void StreamState<Msg>::on_write_done(bool ok) {
  WatchStream<Msg>* start = nullptr;
  WatchStream<Msg>* fin = nullptr;
  grpc::Status st;
  {
    std::lock_guard lk(mu);
    if (!ok) {
      writing = false;
      q.clear();
      if (!finish_called) {
        finish_called = true;
        closing.store(true);
        fin = r;
        st = grpc::Status::CANCELLED;
      }
    } else if (close_pending) {
      writing = false;
      finish_called = true;
      fin = r;
      st = close_status;
    } else if (q.empty()) {
      writing = false;
    } else {
      cur = std::move(q.front());
      q.pop_front();
      start = r;
    }
  }
  if (start) start->StartWrite(&cur);
  if (fin) fin->Finish(st);
}

template <class Msg>
void WatchStream<Msg>::OnDone() {
  hub_->done(st_);
  {
    std::lock_guard lk(st_->mu);
    st_->r = nullptr;
  }
  delete this;
}

template struct StreamState<rv1::WatchResponse>;
template struct StreamState<av1::WatchResponse>;
template class WatchStream<rv1::WatchResponse>;
template class WatchStream<av1::WatchResponse>;

grpc::ServerWriteReactor<rv1::WatchResponse>* Hub::open_ie(grpc::CallbackServerContext* ctx,
                                                           const std::string& replica, int64_t now,
                                                           uint64_t* gen) {
  auto st = std::make_shared<IeState>();
  st->ctx = ctx;
  st->replica = replica;
  st->gen = *gen = next_gen_.fetch_add(1) + 1;
  st->last_heartbeat = now;
  auto* r = new WatchStream<rv1::WatchResponse>(this, st);
  st->r = r;
  rv1::WatchResponse hb;
  hb.set_leader_epoch(epochs_->leader_epoch);
  hb.mutable_heartbeat()->set_at_ms(now);
  st->push(hb, kMaxBuffered);  // a stream's first message is a Heartbeat
  std::shared_ptr<IeState> old;
  {
    std::lock_guard lk(mu_);
    auto& slot = ie_[replica];
    old = slot;
    slot = st;
  }
  if (old) old->close(grpc::Status(grpc::StatusCode::ABORTED, "a newer stream of this replica"), "stream_superseded");
  return r;
}

grpc::ServerWriteReactor<av1::WatchResponse>* Hub::open_gw(grpc::CallbackServerContext* ctx,
                                                           const std::string& replica, int64_t now,
                                                           uint64_t* gen) {
  auto st = std::make_shared<GwState>();
  st->ctx = ctx;
  st->replica = replica;
  st->gen = *gen = next_gen_.fetch_add(1) + 1;
  st->last_heartbeat = now;
  auto* r = new WatchStream<av1::WatchResponse>(this, st);
  st->r = r;
  av1::WatchResponse hb;
  hb.set_leader_epoch(epochs_->leader_epoch);
  hb.mutable_heartbeat()->set_at_ms(now);
  st->push(hb, kMaxBuffered);
  std::shared_ptr<GwState> old;
  {
    std::lock_guard lk(mu_);
    auto& slot = gw_[replica];
    old = slot;
    slot = st;
  }
  if (old) old->close(grpc::Status(grpc::StatusCode::ABORTED, "a newer stream of this replica"), "stream_superseded");
  return r;
}

void Hub::done(const std::shared_ptr<IeState>& s) {
  std::lock_guard lk(mu_);
  auto it = ie_.find(s->replica);
  if (it != ie_.end() && it->second == s) ie_.erase(it);
}

void Hub::done(const std::shared_ptr<GwState>& s) {
  std::lock_guard lk(mu_);
  auto it = gw_.find(s->replica);
  if (it != gw_.end() && it->second == s) gw_.erase(it);
}

bool Hub::push_ie(const std::string& replica, uint64_t gen, const rv1::WatchResponse& m) {
  std::shared_ptr<IeState> s;
  {
    std::lock_guard lk(mu_);
    auto it = ie_.find(replica);
    if (it == ie_.end()) return false;
    if (it->second->gen != gen) {
      held_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    s = it->second;
  }
  const bool was_closing = s->closing.load();
  const bool ok = s->push(m, kMaxBuffered);
  if (!ok && !was_closing) closed_slow_.fetch_add(1);
  return ok;
}

bool Hub::push_gw(const std::string& replica, uint64_t gen, const av1::WatchResponse& m) {
  std::shared_ptr<GwState> s;
  {
    std::lock_guard lk(mu_);
    auto it = gw_.find(replica);
    if (it == gw_.end()) return false;
    if (it->second->gen != gen) {
      held_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    s = it->second;
  }
  return s->push(m, kMaxBuffered);
}

uint64_t Hub::ie_stream_gen(const std::string& replica) const {
  std::lock_guard lk(mu_);
  auto it = ie_.find(replica);
  return it == ie_.end() ? 0 : it->second->gen;
}

bool Hub::ie_live(const std::string& replica) const {
  std::lock_guard lk(mu_);
  auto it = ie_.find(replica);
  return it != ie_.end() && !it->second->closing.load();
}

std::vector<std::string> Hub::ie_replicas() const {
  std::vector<std::string> out;
  std::lock_guard lk(mu_);
  for (const auto& [r, s] : ie_) {
    if (!s->closing.load()) out.push_back(r);
  }
  return out;  // std::map: sorted
}

void Hub::tick(int64_t now) {
  std::vector<std::shared_ptr<IeState>> ies;
  std::vector<std::shared_ptr<GwState>> gws;
  {
    std::lock_guard lk(mu_);
    for (auto& [r, s] : ie_) {
      if (now - s->last_heartbeat >= kHeartbeatMs) {
        s->last_heartbeat = now;
        ies.push_back(s);
      }
    }
    for (auto& [r, s] : gw_) {
      if (now - s->last_heartbeat >= kHeartbeatMs) {
        s->last_heartbeat = now;
        gws.push_back(s);
      }
    }
  }
  rv1::WatchResponse hb;
  hb.set_leader_epoch(epochs_->leader_epoch);
  hb.mutable_heartbeat()->set_at_ms(now);
  for (auto& s : ies) s->push(hb, kMaxBuffered);
  av1::WatchResponse gb;
  gb.set_leader_epoch(epochs_->leader_epoch);
  gb.mutable_heartbeat()->set_at_ms(now);
  for (auto& s : gws) s->push(gb, kMaxBuffered);
}

void Hub::close_all() {
  std::vector<std::shared_ptr<IeState>> ies;
  std::vector<std::shared_ptr<GwState>> gws;
  {
    std::lock_guard lk(mu_);
    for (auto& [r, s] : ie_) ies.push_back(s);
    for (auto& [r, s] : gw_) gws.push_back(s);
  }
  for (auto& s : ies) s->close(grpc::Status(grpc::StatusCode::UNAVAILABLE, "shutting down"), "");
  for (auto& s : gws) s->close(grpc::Status(grpc::StatusCode::UNAVAILABLE, "shutting down"), "");
}

size_t Hub::streams() const {
  std::lock_guard lk(mu_);
  return ie_.size() + gw_.size();
}

}  // namespace pcr
