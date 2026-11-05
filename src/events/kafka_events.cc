#include "src/events/kafka_events.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop

#include <chrono>
#include <cstring>

#include "src/ops/log.h"

namespace pcr {

namespace {
constexpr const char* kSchemaVersion = "1";

void on_delivery(rd_kafka_t*, const rd_kafka_message_t* m, void* opaque) {
  if (m->err) static_cast<KafkaEvents*>(opaque)->failed_.fetch_add(1, std::memory_order_relaxed);
}
}  // namespace

std::string frame_agent_event(const AgentEvent& e, const std::string& event_id) {
  // The writer's body is an object; the frame's fields go first, then the body's (no overlap).
  std::string out = "{\"schema_version\":1,\"event_id\":\"" + event_id + "\",\"type\":\"" + e.kind +
                    "\",\"tenant_id\":\"" + e.tenant + "\",\"agent_id\":\"" + e.agent_id + "\",\"at_ms\":" +
                    std::to_string(e.at_ms);
  if (e.json.size() > 2) {
    out += ",";
    out.append(e.json, 1, e.json.size() - 1);  // the body without its opening brace
  } else {
    out += "}";
  }
  return out;
}

KafkaEvents::KafkaEvents(Options o) : opts_(std::move(o)) {}

KafkaEvents::~KafkaEvents() { stop(); }

bool KafkaEvents::start(std::string* error) {
  char err[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  auto set = [&](const char* k, const std::string& v) {
    if (rd_kafka_conf_set(conf, k, v.c_str(), err, sizeof err) != RD_KAFKA_CONF_OK) {
      *error = std::string(k) + ": " + err;
      return false;
    }
    return true;
  };
  if (!set("bootstrap.servers", opts_.brokers) || !set("enable.idempotence", "true") ||
      !set("linger.ms", "20") || !set("message.timeout.ms", "30000") ||
      !set("queue.buffering.max.messages", std::to_string(opts_.max_buffered))) {
    rd_kafka_conf_destroy(conf);
    return false;
  }
  rd_kafka_conf_set_dr_msg_cb(conf, on_delivery);
  rd_kafka_conf_set_opaque(conf, this);
  rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, err, sizeof err);
  if (!rk) {
    *error = err;
    return false;
  }
  rk_ = rk;
  th_ = std::thread([this] { loop(); });
  return true;
}

void KafkaEvents::stop() {
  {
    std::lock_guard lk(mu_);
    if (stop_) return;
    stop_ = true;
  }
  cv_.notify_all();
  if (th_.joinable()) th_.join();
  if (rk_) {
    rd_kafka_flush(static_cast<rd_kafka_t*>(rk_), 2000);
    rd_kafka_destroy(static_cast<rd_kafka_t*>(rk_));
    rk_ = nullptr;
  }
}

void KafkaEvents::publish(AgentEvent e) {
  {
    std::lock_guard lk(mu_);
    if (stop_ || q_.size() >= opts_.max_buffered) {
      dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    q_.push_back(std::move(e));
  }
  cv_.notify_one();
}

void KafkaEvents::loop() {
  auto* rk = static_cast<rd_kafka_t*>(rk_);
  for (;;) {
    std::deque<AgentEvent> batch;
    {
      std::unique_lock lk(mu_);
      cv_.wait_for(lk, std::chrono::milliseconds(100), [&] { return stop_ || !q_.empty(); });
      if (stop_ && q_.empty()) return;
      batch.swap(q_);
    }
    for (const auto& e : batch) {
      const std::string id = e.topic.empty() ? new_uuid_v7(e.at_ms) : std::string();
      const std::string body = e.topic.empty() ? frame_agent_event(e, id) : e.json;
      const std::string& topic = e.topic.empty() ? opts_.topic : e.topic;
      rd_kafka_headers_t* h = rd_kafka_headers_new(2);
      rd_kafka_header_add(h, "tenant_id", -1, e.tenant.data(), static_cast<ssize_t>(e.tenant.size()));
      rd_kafka_header_add(h, "schema_version", -1, kSchemaVersion, 1);
      const rd_kafka_resp_err_t r = rd_kafka_producev(
          rk, RD_KAFKA_V_TOPIC(topic.c_str()), RD_KAFKA_V_KEY(e.tenant.data(), e.tenant.size()),
          RD_KAFKA_V_VALUE(const_cast<char*>(body.data()), body.size()), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
          RD_KAFKA_V_HEADERS(h), RD_KAFKA_V_END);
      if (r != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_headers_destroy(h);  // not taken over on error
        dropped_.fetch_add(1, std::memory_order_relaxed);
      } else {
        produced_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    rd_kafka_poll(rk, 0);
  }
}

}  // namespace pcr
