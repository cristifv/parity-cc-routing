// agent.events (PLAN §5.4; parity-contracts schemas/events/agent-events.v1.json): the tenant writers
// hand events to a bounded buffer (never block; full: dropped and counted, events are not state);
// one thread frames each as JSON (event_id UUIDv7, schema_version, type, tenant_id, agent_id, at_ms)
// and produces it with librdkafka (idempotent producer), key = tenant_id, headers tenant_id and
// schema_version (Q12). No personal data: agent ids are opaque, reason and disposition codes are
// the tenant's codes.
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#include "src/core/tenant.h"

namespace pcr {

// The event's JSON line (also used by tests without a broker).
std::string frame_agent_event(const AgentEvent& e, const std::string& event_id);

class KafkaEvents final : public EventSink {
 public:
  struct Options {
    std::string brokers;  // bootstrap.servers
    std::string topic = "agent.events";
    size_t max_buffered = 50'000;
  };
  explicit KafkaEvents(Options o);
  ~KafkaEvents() override;
  // False when librdkafka refused the configuration (the reason in *error).
  bool start(std::string* error);
  void stop();
  void publish(AgentEvent e) override;
  uint64_t dropped() const { return dropped_.load(); }
  uint64_t produced() const { return produced_.load(); }
  uint64_t failed() const { return failed_.load(); }

 private:
  void loop();
  Options opts_;
  void* rk_ = nullptr;  // rd_kafka_t*
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<AgentEvent> q_;
  bool stop_ = false;
  std::thread th_;
  std::atomic<uint64_t> dropped_{0}, produced_{0};
 public:
  std::atomic<uint64_t> failed_{0};  // delivery reports with an error (librdkafka's callback)
};

}  // namespace pcr
