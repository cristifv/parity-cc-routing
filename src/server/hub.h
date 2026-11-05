// The Watch streams (PLAN §5.2, §8 1.3): one stream per IE replica (routing.v1) and per gateway
// replica (agents.v1); the newest stream of a replica_id wins (stream_superseded). Shard writers
// hand messages to the hub, which only queues them. No gRPC operation is ever started while one of
// our locks is held, and a stream's reactor is touched only by the thread that won the transition
// (to writing, or to finishing) under the stream's lock, so a reactor is never used after Finish.
// A slow consumer's stream is closed; its unacknowledged messages replay when it reconnects.
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "parity/agents/v1/agents.pb.h"
#include "parity/routing/v1/routing.pb.h"
#include "src/core/tenant.h"

namespace pcr {

class Hub;

template <class Msg>
class WatchStream;

// A stream's state; shared by the hub (lookups) and the reactor (lifetime).
template <class Msg>
struct StreamState {
  std::mutex mu;
  std::deque<Msg> q;
  Msg cur;
  bool writing = false;
  bool close_pending = false;
  bool finish_called = false;
  grpc::Status close_status;
  WatchStream<Msg>* r = nullptr;
  grpc::CallbackServerContext* ctx = nullptr;
  std::string replica;
  uint64_t gen = 0;            // set before the stream is registered; never changes
  int64_t last_heartbeat = 0;  // hub lock
  std::atomic<bool> closing{false};

  // Thread-safe. False when the stream is closing.
  bool push(const Msg& m, size_t max_buffered);
  // Thread-safe, idempotent.
  void close(grpc::Status s, const std::string& parity_error);
  // Called by the reactor only.
  void on_write_done(bool ok);
};

template <class Msg>
class WatchStream final : public grpc::ServerWriteReactor<Msg> {
 public:
  WatchStream(Hub* hub, std::shared_ptr<StreamState<Msg>> st) : hub_(hub), st_(std::move(st)) {}
  void OnWriteDone(bool ok) override { st_->on_write_done(ok); }
  void OnCancel() override { st_->close(grpc::Status::CANCELLED, ""); }
  void OnDone() override;

 private:
  Hub* hub_;
  std::shared_ptr<StreamState<Msg>> st_;
};

using IeState = StreamState<rv1::WatchResponse>;
using GwState = StreamState<av1::WatchResponse>;

class Hub final : public Sink {
 public:
  static constexpr size_t kMaxBuffered = 50'000;  // per stream (PLAN §6.1)

  explicit Hub(const Epochs* epochs) : epochs_(epochs) {}

  // Creates the reactor for a Watch call: queues the opening Heartbeat, registers the stream with
  // a new generation (*gen) and closes the replica's older one (ABORTED / stream_superseded). The
  // caller then posts the replay for *gen to every tenant (CR-053).
  grpc::ServerWriteReactor<rv1::WatchResponse>* open_ie(grpc::CallbackServerContext* ctx,
                                                        const std::string& replica, int64_t now,
                                                        uint64_t* gen);
  grpc::ServerWriteReactor<av1::WatchResponse>* open_gw(grpc::CallbackServerContext* ctx,
                                                        const std::string& replica, int64_t now,
                                                        uint64_t* gen);
  void done(const std::shared_ptr<IeState>& s);
  void done(const std::shared_ptr<GwState>& s);

  // Delivers only to the replica's live stream of generation `gen` (else drops: CR-053).
  void to_ie(const std::string& replica, uint64_t gen, const rv1::WatchResponse& m) override {
    push_ie(replica, gen, m);
  }
  void to_gw(const std::string& replica, uint64_t gen, const av1::WatchResponse& m) override {
    push_gw(replica, gen, m);
  }
  // The live routing stream's generation for `replica` (0: none). Tests only.
  uint64_t ie_stream_gen(const std::string& replica) const;
  // As to_ie/to_gw; true when the message was queued on the stream (tests' taps).
  bool push_ie(const std::string& replica, uint64_t gen, const rv1::WatchResponse& m);
  bool push_gw(const std::string& replica, uint64_t gen, const av1::WatchResponse& m);
  bool ie_live(const std::string& replica) const override;
  std::vector<std::string> ie_replicas() const override;

  // Heartbeats every 5 s on every stream (`now` on the engine's clock).
  void tick(int64_t now);
  void close_all();
  size_t streams() const;
  uint64_t closed_slow() const { return closed_slow_.load(); }
  // Messages a writer sent for an older (or no) stream generation: they wait for the replay.
  uint64_t held_for_replay() const { return held_.load(); }

 private:
  const Epochs* epochs_;
  mutable std::mutex mu_;
  std::map<std::string, std::shared_ptr<IeState>> ie_;
  std::map<std::string, std::shared_ptr<GwState>> gw_;
  std::atomic<uint64_t> closed_slow_{0};
  std::atomic<uint64_t> next_gen_{0};
  std::atomic<uint64_t> held_{0};
};

}  // namespace pcr
