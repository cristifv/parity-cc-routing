#include "src/state/kafka_state.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <pthread.h>
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop

#include <chrono>
#include <random>

#include "src/ops/log.h"
#include "src/state/state.pb.h"

namespace pcr {

namespace st = ::parity::cc_routing::state::v1;

namespace {

uint32_t fnv1a(const std::string& s) {
  uint32_t h = 2166136261u;
  for (unsigned char c : s) {
    h ^= c;
    h *= 16777619u;
  }
  return h;
}

std::string tenant_of(const std::string& key) {
  if (key.empty() || key[0] == '~') return {};
  const size_t a = key.find('/');
  return a == std::string::npos ? std::string() : key.substr(0, a);
}

int64_t mono_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Takes ownership of e; true when e was null (no error).
bool ok(rd_kafka_error_t* e, std::string* err, bool* fatal = nullptr, bool* abortable = nullptr, bool* retriable = nullptr) {
  if (!e) return true;
  if (err) *err = rd_kafka_error_string(e);
  if (fatal) *fatal = rd_kafka_error_is_fatal(e);
  if (abortable) *abortable = rd_kafka_error_txn_requires_abort(e);
  if (retriable) *retriable = rd_kafka_error_is_retriable(e);
  rd_kafka_error_destroy(e);
  return false;
}

}  // namespace

KafkaState::KafkaState(Options o, std::function<bool()> may_lead, std::function<void(const std::string&)> step_down)
    : opts_(std::move(o)), may_lead_(std::move(may_lead)), step_down_(std::move(step_down)) {}

KafkaState::~KafkaState() { stop(); }

int KafkaState::partition_of(const std::string& tenant_or_key) const {
  return partitions_ <= 0 ? 0 : static_cast<int>(fnv1a(tenant_or_key) % static_cast<uint32_t>(partitions_));
}

bool KafkaState::start_follower(std::string* err) {
  char e[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  std::mt19937_64 rng(std::random_device{}());
  const std::string group = "parity-cc-routing-follower-" + opts_.bin + "-" + std::to_string(rng());
  for (const auto& [k, v] : std::vector<std::pair<std::string, std::string>>{
           {"bootstrap.servers", opts_.brokers}, {"group.id", group}, {"enable.auto.commit", "false"},
           {"isolation.level", "read_committed"}, {"auto.offset.reset", "earliest"},
           {"enable.partition.eof", "false"}, {"fetch.wait.max.ms", "10"}}) {
    if (rd_kafka_conf_set(conf, k.c_str(), v.c_str(), e, sizeof e) != RD_KAFKA_CONF_OK) {
      *err = k + ": " + e;
      rd_kafka_conf_destroy(conf);
      return false;
    }
  }
  rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, e, sizeof e);
  if (!rk) {
    *err = e;
    return false;
  }
  rd_kafka_poll_set_consumer(rk);
  rd_kafka_topic_t* t = rd_kafka_topic_new(rk, opts_.topic.c_str(), nullptr);
  const rd_kafka_metadata_t* md = nullptr;
  rd_kafka_resp_err_t r = RD_KAFKA_RESP_ERR_NO_ERROR;
  for (int i = 0; i < 20; ++i) {
    r = rd_kafka_metadata(rk, 0, t, &md, 3000);
    if (r == RD_KAFKA_RESP_ERR_NO_ERROR && md->topic_cnt == 1 && md->topics[0].err == RD_KAFKA_RESP_ERR_NO_ERROR &&
        md->topics[0].partition_cnt > 0) {
      break;
    }
    if (md) rd_kafka_metadata_destroy(md);
    md = nullptr;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  rd_kafka_topic_destroy(t);
  if (!md) {
    *err = "topic " + opts_.topic + " not readable: " + rd_kafka_err2str(r);
    rd_kafka_destroy(rk);
    return false;
  }
  partitions_ = md->topics[0].partition_cnt;
  rd_kafka_metadata_destroy(md);
  rd_kafka_topic_partition_list_t* list = rd_kafka_topic_partition_list_new(partitions_);
  for (int p = 0; p < partitions_; ++p) {
    rd_kafka_topic_partition_list_add(list, opts_.topic.c_str(), p)->offset = RD_KAFKA_OFFSET_BEGINNING;
  }
  r = rd_kafka_assign(rk, list);
  rd_kafka_topic_partition_list_destroy(list);
  if (r != RD_KAFKA_RESP_ERR_NO_ERROR) {
    *err = std::string("assign: ") + rd_kafka_err2str(r);
    rd_kafka_destroy(rk);
    return false;
  }
  consumer_ = rk;
  follow_th_ = std::thread([this] {
    pthread_setname_np(pthread_self(), "pcr-follow");
    follow_loop();
  });
  return true;
}

void KafkaState::follow_loop() {
  auto* rk = static_cast<rd_kafka_t*>(consumer_);
  int64_t last_err_log = 0;
  while (!follow_stop_.load()) {
    rd_kafka_message_t* m = rd_kafka_consumer_poll(rk, 100);
    if (!m) continue;
    if (m->err) {
      if (m->err != RD_KAFKA_RESP_ERR__PARTITION_EOF && mono_ms() - last_err_log > 5000) {
        last_err_log = mono_ms();
        LogRec("warn", "dependency.unavailable", "re.state follower error").error("unavailable", rd_kafka_message_errstr(m)).attr("dependency", "kafka");
      }
    } else if (m->key) {
      apply(m->partition, std::string(static_cast<const char*>(m->key), m->key_len),
            static_cast<const char*>(m->payload), m->payload ? m->len : 0);
    }
    rd_kafka_message_destroy(m);
  }
}

void KafkaState::apply(int partition, const std::string& key, const char* value, size_t len) {
  consumed_.fetch_add(1, std::memory_order_relaxed);
  const std::string tenant = tenant_of(key);
  std::lock_guard lk(cache_mu_);
  if (!value) {
    cache_.erase(key);
    return;
  }
  st::Record rec;
  if (!rec.ParseFromArray(value, static_cast<int>(len))) return;
  if (rec.has_fence()) {
    if (rec.fence().bin() == opts_.bin) {
      fence_seen_[partition] = rec.fence().nonce();
      fence_cv_.notify_all();
    }
    return;
  }
  if (!tenant.empty()) {
    uint64_t& newest = epoch_of_[tenant];
    if (rec.tenant_epoch() < newest) return;  // an older owner's record (a tenant move)
    newest = rec.tenant_epoch();
  }
  Entry& e = cache_[key];
  e.epoch = rec.tenant_epoch();
  e.value.assign(value, len);
}

std::vector<std::pair<std::string, std::string>> KafkaState::records_of(const std::string& tenant) const {
  std::vector<std::pair<std::string, std::string>> out;
  const std::string prefix = tenant + "/";
  std::lock_guard lk(cache_mu_);
  auto ep = epoch_of_.find(tenant);
  const uint64_t newest = ep == epoch_of_.end() ? 0 : ep->second;
  for (const auto& [k, e] : cache_) {
    if (k.size() > prefix.size() && k.compare(0, prefix.size(), prefix) == 0 && e.epoch == newest) {
      if (k.find("/handover/") != std::string::npos) continue;  // a marker, not state
      out.emplace_back(k, e.value);
    }
  }
  return out;
}

std::optional<std::string> KafkaState::get(const std::string& key) const {
  std::lock_guard lk(cache_mu_);
  auto it = cache_.find(key);
  if (it == cache_.end()) return std::nullopt;
  return it->second.value;
}

uint64_t KafkaState::tenant_epoch_seen(const std::string& tenant) const {
  std::lock_guard lk(cache_mu_);
  auto it = epoch_of_.find(tenant);
  return it == epoch_of_.end() ? 0 : it->second;
}

bool KafkaState::caught_up() {
  auto* rk = static_cast<rd_kafka_t*>(consumer_);
  if (!rk) return false;
  rd_kafka_topic_partition_list_t* list = rd_kafka_topic_partition_list_new(partitions_);
  for (int p = 0; p < partitions_; ++p) rd_kafka_topic_partition_list_add(list, opts_.topic.c_str(), p);
  bool done = rd_kafka_position(rk, list) == RD_KAFKA_RESP_ERR_NO_ERROR;
  for (int p = 0; done && p < partitions_; ++p) {
    int64_t lo = 0, hi = 0;
    if (rd_kafka_query_watermark_offsets(rk, opts_.topic.c_str(), p, &lo, &hi, 1000) != RD_KAFKA_RESP_ERR_NO_ERROR) {
      done = false;
      break;
    }
    const int64_t pos = list->elems[p].offset;
    if (hi > lo && (pos < 0 || pos < hi)) done = false;
  }
  rd_kafka_topic_partition_list_destroy(list);
  return done;
}

bool KafkaState::become_leader(uint64_t leader_epoch, int64_t timeout_ms, std::string* err) {
  char e[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  for (const auto& [k, v] : std::vector<std::pair<std::string, std::string>>{
           {"bootstrap.servers", opts_.brokers}, {"transactional.id", "parity-cc-routing." + opts_.bin},
           {"enable.idempotence", "true"}, {"acks", "all"}, {"linger.ms", "1"},
           {"transaction.timeout.ms", "10000"}, {"message.timeout.ms", "10000"},
           {"queue.buffering.max.messages", "1000000"}}) {
    if (rd_kafka_conf_set(conf, k.c_str(), v.c_str(), e, sizeof e) != RD_KAFKA_CONF_OK) {
      *err = k + ": " + e;
      rd_kafka_conf_destroy(conf);
      return false;
    }
  }
  rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, e, sizeof e);
  if (!rk) {
    *err = e;
    return false;
  }
  producer_ = rk;
  // Fences every earlier producer with this transactional.id (the previous leader) and aborts its
  // open transaction.
  const auto t0 = std::chrono::steady_clock::now();
  if (!ok(rd_kafka_init_transactions(rk, static_cast<int>(timeout_ms)), err)) {
    *err = "init_transactions: " + *err;
    return false;
  }
  const auto t1 = std::chrono::steady_clock::now();
  std::mt19937_64 rng(std::random_device{}());
  const std::string nonce = std::to_string(leader_epoch) + "-" + std::to_string(rng());
  std::vector<StateRec> fences;
  for (int p = 0; p < partitions_; ++p) {
    st::Record rec;
    rec.set_schema_version(1);
    rec.set_leader_epoch(leader_epoch);
    rec.set_written_at_ms(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    auto* f = rec.mutable_fence();
    f->set_bin(opts_.bin);
    f->set_leader_epoch(leader_epoch);
    f->set_nonce(nonce);
    fences.push_back(StateRec{"#" + std::to_string(p), "~fence/" + opts_.bin + "/" + std::to_string(p), rec.SerializeAsString()});
  }
  if (!commit_now(fences, err)) return false;
  std::unique_lock lk(cache_mu_);
  const bool seen = fence_cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
    if (static_cast<int>(fence_seen_.size()) < partitions_) return false;
    for (const auto& [p, n] : fence_seen_) {
      if (n != nonce) return false;
    }
    return true;
  });
  lk.unlock();
  if (!seen) {
    *err = "the follower did not read the takeover fence in time";
    return false;
  }
  const auto t2 = std::chrono::steady_clock::now();
  LogRec("info", "state.fenced", "re.state: producer fenced, topic read to the end")
      .attr("init_transactions_ms", static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count()))
      .attr("fence_ms", static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()));
  return true;
}

void KafkaState::start_commits() {
  {
    std::lock_guard qlk(mu_);
    if (leading_) return;
    leading_ = true;
  }
  commit_th_ = std::thread([this] {
    pthread_setname_np(pthread_self(), "pcr-commit");
    commit_loop();
  });
}

bool KafkaState::produce(void* rkp, const StateRec& r, std::string* err) {
  auto* rk = static_cast<rd_kafka_t*>(rkp);
  int partition = 0;
  if (!r.tenant.empty() && r.tenant[0] == '#') {
    partition = std::stoi(r.tenant.substr(1));  // an explicit partition (the fence records)
  } else {
    partition = partition_of(r.tenant.empty() ? r.key : r.tenant);
  }
  void* vptr = r.value ? const_cast<char*>(r.value->data()) : nullptr;
  const size_t vlen = r.value ? r.value->size() : 0;
  for (int i = 0; i < 1000; ++i) {
    rd_kafka_headers_t* h = rd_kafka_headers_new(1);
    if (!r.tenant.empty() && r.tenant[0] != '#') {
      rd_kafka_header_add(h, "tenant_id", -1, r.tenant.data(), static_cast<ssize_t>(r.tenant.size()));
    }
    const rd_kafka_resp_err_t e = rd_kafka_producev(
        rk, RD_KAFKA_V_TOPIC(opts_.topic.c_str()), RD_KAFKA_V_PARTITION(partition),
        RD_KAFKA_V_KEY(r.key.data(), r.key.size()),
        RD_KAFKA_V_VALUE(vptr, vlen),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_HEADERS(h), RD_KAFKA_V_END);
    if (e == RD_KAFKA_RESP_ERR_NO_ERROR) return true;
    rd_kafka_headers_destroy(h);
    if (e != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
      *err = rd_kafka_err2str(e);
      return false;
    }
    rd_kafka_poll(rk, 10);
  }
  *err = "producer queue full";
  return false;
}

// One transaction; up to 3 attempts for abortable or retriable errors. False: step down.
bool KafkaState::commit_records(const std::vector<const StateRec*>& recs, std::string* err) {
  auto* rk = static_cast<rd_kafka_t*>(producer_);
  for (int attempt = 0; attempt < 3; ++attempt) {
    bool fatal = false, abortable = false, retriable = false;
    if (attempt > 0) {
      std::lock_guard lk(stats_mu_);
      ++stats_.retries;
    }
    if (!ok(rd_kafka_begin_transaction(rk), err, &fatal)) {
      if (fatal) return false;
      continue;
    }
    bool produced = true;
    for (const StateRec* r : recs) {
      if (!produce(rk, *r, err)) {
        produced = false;
        break;
      }
    }
    if (produced) {
      rd_kafka_error_t* ce = rd_kafka_commit_transaction(rk, 10'000);
      if (ok(ce, err, &fatal, &abortable, &retriable)) return true;
      if (fatal) return false;
      if (retriable && !abortable) {
        // The outcome is unknown; retrying the commit is what librdkafka asks for.
        if (ok(rd_kafka_commit_transaction(rk, 10'000), err, &fatal, &abortable)) return true;
        if (fatal) return false;
      }
    }
    {
      std::lock_guard lk(stats_mu_);
      ++stats_.aborts;
    }
    if (!ok(rd_kafka_abort_transaction(rk, 10'000), err, &fatal) && fatal) return false;
  }
  return false;
}

bool KafkaState::commit_now(const std::vector<StateRec>& recs, std::string* err) {
  std::vector<const StateRec*> ptrs;
  for (const auto& r : recs) ptrs.push_back(&r);
  return commit_records(ptrs, err);
}

void KafkaState::submit(Batch&& b) {
  const size_t n = b.records.size();
  {
    std::lock_guard lk(mu_);
    if (stop_ || failed_.load()) return;  // stepping down: nothing more becomes visible
    q_.push_back(std::move(b));
  }
  backlog_.fetch_add(n, std::memory_order_relaxed);
  cv_.notify_one();
}

void KafkaState::fail(const std::string& why) {
  if (failed_.exchange(true)) return;
  {
    std::lock_guard lk(mu_);
    q_.clear();
  }
  idle_cv_.notify_all();
  if (step_down_) step_down_(why);
}

void KafkaState::commit_loop() {
  for (;;) {
    std::vector<Batch> group;
    size_t n = 0;
    {
      std::unique_lock lk(mu_);
      cv_.wait(lk, [&] { return stop_ || !q_.empty(); });
      if (q_.empty()) return;  // stopped and drained
      while (!q_.empty() && (group.empty() || n + q_.front().records.size() <= opts_.max_records_per_txn)) {
        n += q_.front().records.size();
        group.push_back(std::move(q_.front()));
        q_.pop_front();
      }
      busy_ = true;
    }
    if (failed_.load()) return;
    if (!may_lead_()) {
      fail("lease lost before a commit");
      return;
    }
    if (n > 0) {
      std::vector<const StateRec*> recs;
      recs.reserve(n);
      for (const auto& b : group) {
        for (const auto& r : b.records) recs.push_back(&r);
      }
      const auto t0 = std::chrono::steady_clock::now();
      std::string err;
      if (!commit_records(recs, &err)) {
        fail("re.state commit failed or fenced: " + err);
        return;
      }
      const auto ms = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
      std::lock_guard lk(stats_mu_);
      ++stats_.commits;
      stats_.records += n;
      const int b = std::min(15, ms == 0 ? 0 : 64 - __builtin_clzll(ms));
      ++stats_.commit_ms_buckets[b];
      stats_.commit_ms_max = std::max(stats_.commit_ms_max, ms);
    }
    // A commit that finished after the lease's deadline stays committed (the next leader reads it)
    // but releases nothing: its messages replay from the new leader.
    if (!may_lead_()) {
      fail("lease lost before releasing a commit's effects");
      return;
    }
    for (auto& b : group) {
      for (auto& fx : b.effects) fx();
    }
    backlog_.fetch_sub(n, std::memory_order_relaxed);
    {
      std::lock_guard lk(mu_);
      busy_ = false;
    }
    idle_cv_.notify_all();
  }
}

bool KafkaState::flush(int64_t timeout_ms) {
  std::unique_lock lk(mu_);
  return idle_cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] { return failed_.load() || (q_.empty() && !busy_); }) &&
         !failed_.load();
}

void KafkaState::stop() {
  {
    std::lock_guard lk(mu_);
    if (stop_) return;
    stop_ = true;
  }
  cv_.notify_all();
  if (commit_th_.joinable()) commit_th_.join();
  follow_stop_ = true;
  if (follow_th_.joinable()) follow_th_.join();
  if (consumer_) {
    rd_kafka_consumer_close(static_cast<rd_kafka_t*>(consumer_));
    rd_kafka_destroy(static_cast<rd_kafka_t*>(consumer_));
    consumer_ = nullptr;
  }
  if (producer_) {
    rd_kafka_destroy(static_cast<rd_kafka_t*>(producer_));
    producer_ = nullptr;
  }
}

KafkaState::Stats KafkaState::stats() const {
  std::lock_guard lk(stats_mu_);
  return stats_;
}

}  // namespace pcr
