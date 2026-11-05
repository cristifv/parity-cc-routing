// CR-053's race test: IE reconnects its routing Watch stream again and again while the shard
// writers keep producing messages for interactions bound to that replica. On every stream the
// messages of one interaction must arrive as 1, 2, 3, ... (a redelivery may repeat an older one);
// a jump forward means a newer message overtook the replay, and IE (which drops
// delivery_seq <= the highest it forwarded) would lose the older ones.
//
// The world: two queues whose max-wait actions move an entry back and forth (sales -> support ->
// sales), so every virtual second each waiting entry gets an Overflow (replayable). Nobody acks,
// so each new stream replays the whole history first. A control run sends with the stream's live
// generation instead of the replayed one (the M1 behaviour) and reports how often it inverts.
#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <map>
#include <thread>

#include "parity/routing/v1/routing.grpc.pb.h"
#include "src/core/engine.h"
#include "src/ops/metrics.h"
#include "src/server/hub.h"
#include "src/server/services.h"

namespace pcr {
namespace {

constexpr int64_t kStart = 1'792'800'000'000;
constexpr int kCalls = 16;

struct ToHub final : Sink {
  Hub* hub = nullptr;
  bool bypass = false;  // the control: ignore the replayed generation (no CR-053 fix)
  void to_ie(const std::string& r, uint64_t g, const rv1::WatchResponse& m) override {
    hub->push_ie(r, bypass ? hub->ie_stream_gen(r) : g, m);
  }
  void to_gw(const std::string& r, uint64_t g, const av1::WatchResponse& m) override { hub->push_gw(r, g, m); }
  bool ie_live(const std::string& r) const override { return hub->ie_live(r); }
};

QueueConfig q(const std::string& id, const std::string& next) {
  QueueConfig c;
  c.id = id;
  c.max_wait_ms = 1'000;
  c.max_wait_action = MaxWaitAction::kQueue;
  c.max_wait_queue = next;
  return c;
}

struct Outcome {
  int streams = 0, messages = 0, jumps = 0;
  bool budget_out = false;  // the wall-clock budget ended the run before min_messages
};

constexpr int kWantMessages = 2000;  // enough traffic to race
constexpr int kFloorMessages = 300;  // below this, a run proves nothing: the host is too slow
constexpr auto kBudget = std::chrono::seconds(120);

// Reconnects at least `rounds` times and until `min_messages` were seen, or until the wall-clock
// budget runs out (a sanitizer or a busy host slows the writer: the run lasts longer instead of
// racing less; it is never cut short by a stream count).
Outcome run(bool bypass, int rounds, int min_messages) {
  const auto deadline = std::chrono::steady_clock::now() + kBudget;
  ManualClock clock(kStart);
  ToHub sink;
  sink.bypass = bypass;
  Engine engine(&clock, &sink, 2);
  Hub hub(&engine.epochs());
  sink.hub = &hub;
  TenantConfig t;
  t.id = "acme";
  t.queues = {q("sales", "support"), q("support", "sales")};  // a cycle: only the engine sees it
  engine.load({t}, Epochs{1, 0});
  Metrics metrics;
  Server server(ServerOptions{"127.0.0.1:0", "bin-a", nullptr, nullptr}, &clock, &engine, &hub, &metrics);
  EXPECT_TRUE(server.start());
  auto ch = grpc::CreateChannel("127.0.0.1:" + std::to_string(server.port()), grpc::InsecureChannelCredentials());
  auto stub = rv1::RoutingService::NewStub(ch);
  for (int i = 0; i < kCalls; ++i) {
    rv1::EnqueueRequest r;
    auto* o = r.mutable_owner();
    o->set_tenant_id("acme");
    o->set_interaction_id(new_uuid_v7(clock.now_ms()));
    o->set_owner_epoch(1);
    o->set_replica_id("ie-race");
    r.set_enqueue_seq(1);
    r.mutable_target()->set_queue_id("sales");
    r.set_first_queued_at_ms(clock.now_ms());
    r.set_ordering_at_ms(clock.now_ms());
    r.set_channel(rv1::CHANNEL_VOICE);
    r.set_reason(rv1::ENQUEUE_REASON_NEW);
    grpc::ClientContext ctx;
    ctx.AddMetadata("tenant-id", "acme");
    rv1::EnqueueResponse out;
    EXPECT_TRUE(stub->Enqueue(&ctx, r, &out).ok());
  }
  std::atomic<bool> stop{false};
  std::thread writer([&] {
    // One virtual second per step: every entry overflows once (a new delivery_seq each). CR-191: at
    // most one step per 5 ms of wall time. Unpaced, virtual time raced ahead, and since this test never
    // acknowledges, the backlog every new stream's replay sends grew past the hub's 50,000-message
    // stream buffer: from then on each stream was closed as a slow consumer at its replay and the run
    // crawled to the 120 s budget (about half the runs, the phase-6 source included). Paced, the
    // backlog stays far below the cap while ~3,200 new messages a second still race every reconnect.
    while (!stop) {
      engine.advance_to(&clock, clock.now_ms() + 1'000);
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });
  Outcome res;
  for (int round = 0; round < rounds || res.messages < min_messages; ++round) {
    if (round >= rounds && std::chrono::steady_clock::now() >= deadline) {
      res.budget_out = true;
      break;
    }
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(40));
    rv1::WatchRequest w;
    w.set_replica_id("ie-race");
    w.set_bin("bin-a");
    auto reader = stub->Watch(&ctx, w);
    std::map<std::string, uint64_t> last;  // per interaction, on this stream
    rv1::WatchResponse m;
    bool first = true;
    while (reader->Read(&m)) {
      if (first) {
        EXPECT_TRUE(m.has_heartbeat()) << "a stream's first message is a Heartbeat";
        first = false;
      }
      if (m.delivery_seq() == 0) continue;
      ++res.messages;
      EXPECT_TRUE(m.has_overflow());
      uint64_t& l = last[m.overflow().target().interaction_id()];
      if (m.delivery_seq() > l + 1) ++res.jumps;  // an older message would come after it
      if (m.delivery_seq() > l) l = m.delivery_seq();
    }
    reader->Finish();
    ++res.streams;
  }
  stop = true;
  writer.join();
  server.shutdown(0);
  return res;
}

TEST(Race, ReplayIsNeverOvertakenAcrossReconnects) {
  const Outcome r = run(/*bypass=*/false, 60, kWantMessages);
  std::printf("race: generations on: %d streams, %d messages, %d jumps%s\n", r.streams, r.messages, r.jumps,
              r.budget_out ? " (the 120 s budget ran out first)" : "");
  EXPECT_GE(r.streams, 60);
  // The invariant, on whatever was produced; too little traffic is a slow host, not a pass.
  EXPECT_EQ(r.jumps, 0);
  EXPECT_GE(r.messages, kFloorMessages) << "host too slow: " << r.messages << " messages in 120 s (the race needs "
                                        << kFloorMessages << " at least, " << kWantMessages << " normally)";
}

// The control run shows the race test isn't vacuous: without the generations a newer message
// overtakes the replay. Reported, not asserted (a scheduler may hide the window in one run).
TEST(Race, ControlWithoutGenerationsShowsTheInversion) {
  const Outcome r = run(/*bypass=*/true, 60, kWantMessages);
  std::printf("race: control (M1 behaviour): %d streams, %d messages, %d jumps (varies by run)\n", r.streams, r.messages, r.jumps);
  EXPECT_GE(r.streams, 60);
}

}  // namespace
}  // namespace pcr
