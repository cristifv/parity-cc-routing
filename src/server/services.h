// parity.routing.v1.RoutingService and parity.agents.v1.AgentService on one gRPC server (9800),
// the grpc++ callback API (PLAN §5.2). Handlers check the request and its tenant-id metadata, then
// hand the work to the tenant's shard writer; the writer answers.
#pragma once

#include <grpcpp/grpcpp.h>

#include <functional>
#include <memory>
#include <string>

#include "parity/agents/v1/agents.grpc.pb.h"
#include "parity/routing/v1/routing.grpc.pb.h"
#include "src/core/engine.h"
#include "src/ops/metrics.h"
#include "src/server/hub.h"

namespace pcr {

struct ServerOptions {
  std::string listen = "127.0.0.1:9800";
  std::string bin = "bin-a";
  // Phase 6: false while this instance is a standby (or stepping down): every RPC and stream is
  // refused UNAVAILABLE / not_leader with `parity-leader` = leader() (PLAN §5.2). Unset: always serving.
  std::function<bool()> serving;
  std::function<std::string()> leader;
};

class Server {
 public:
  Server(ServerOptions opts, const Clock* clock, Engine* engine, Hub* hub, Metrics* metrics);
  ~Server();
  // Binds and starts serving; false when the port can't be bound.
  bool start();
  int port() const { return port_; }
  // Closes the streams and stops (grace for in-flight RPCs).
  void shutdown(int grace_ms);

 private:
  class Routing;
  class Agents;
  // Phase 6: answers UNAVAILABLE / not_leader (+ parity-leader) while a standby; true if it did.
  bool not_leader(grpc::CallbackServerContext* ctx, grpc::ServerUnaryReactor* r, const char* rpc);
  ServerOptions opts_;
  const Clock* clock_;
  Engine* engine_;
  Hub* hub_;
  Metrics* metrics_;
  std::unique_ptr<Routing> routing_;
  std::unique_ptr<Agents> agents_;
  std::unique_ptr<grpc::Server> server_;
  int port_ = 0;
};

}  // namespace pcr
