
// event_producer.h
// Kafka producer pentru NNMPC – publică evenimente relevante din ciclul WEM
//
// Dependență: librdkafka C++ (rdkafka++)
//   Fedora:  sudo dnf install librdkafka-devel
//   Ubuntu:  sudo apt install librdkafka-dev
//
// Build (adaugă la comanda existentă):
//   ... -lrdkafka++  -lrdkafka
//
// Topics Kafka publicate:
//   nnmpc.staffing       – decizia de staffing per ciclu (15 min)
//   nnmpc.forecast       – predicțiile Transformer (arr + AHT pe horizon)
//   nnmpc.shrinkage      – shrinkage planned/unplanned curent
//   nnmpc.gamification   – scoruri și badge-uri agenți (după fiecare update)
//   nnmpc.shift_bidding  – alocări overtime
//   nnmpc.fine_tune      – loss online fine-tuning (monitorizare model)
//   nnmpc.queue          – starea cozii în timp real
// ─────────────────────────────────────────────────────────────────────────────

#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <chrono>
#include <format>
#include <functional>
#include <iostream>
#include <memory>
#include <print>
#include <string>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
//  JSON helpers minimali (fără dependențe externe)
//  Producem JSON manual pentru a evita nlohmann/rapidjson ca dependență.
// ─────────────────────────────────────────────────────────────────────────────

namespace json {

inline std::string str(const std::string& s) {
    return "\"" + s + "\"";
}
inline std::string kv(const std::string& k, const std::string& v) {
    return str(k) + ":" + str(v);
}
inline std::string kv(const std::string& k, double v) {
    return str(k) + ":" + std::format("{:.4f}", v);
}
inline std::string kv(const std::string& k, int v) {
    return str(k) + ":" + std::to_string(v);
}
inline std::string kv(const std::string& k, bool v) {
    return str(k) + ":" + (v ? "true" : "false");
}
inline std::string obj(std::initializer_list<std::string> fields) {
    std::string s = "{";
    bool first = true;
    for (const auto& f : fields) {
        if (!first) s += ",";
        s += f;
        first = false;
    }
    return s + "}";
}
inline std::string arr(const std::vector<std::string>& items) {
    std::string s = "[";
    for (int i = 0; i < (int)items.size(); ++i) {
        if (i > 0) s += ",";
        s += items[i];
    }
    return s + "]";
}
inline std::string arr(const std::vector<double>& vals) {
    std::vector<std::string> items;
    for (double v : vals) items.push_back(std::format("{:.4f}", v));
    return arr(items);
}
inline int64_t epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

} // namespace json

// ─────────────────────────────────────────────────────────────────────────────
//  Event structures (moved outside main class)
// ─────────────────────────────────────────────────────────────────────────────

struct config {
    std::string brokers          = "localhost:9092";
    std::string client_id        = "nnmpc-producer";
    // Topics
    std::string topic_staffing   = "nnmpc.staffing";
    std::string topic_forecast   = "nnmpc.forecast";
    std::string topic_shrinkage  = "nnmpc.shrinkage";
    std::string topic_gamif      = "nnmpc.gamification";
    std::string topic_shift      = "nnmpc.shift_bidding";
    std::string topic_finetune   = "nnmpc.fine_tune";
    std::string topic_queue      = "nnmpc.queue";
    // Kafka producer settings
    int         linger_ms        = 5;      // micro-batching
    int         batch_num_msgs   = 1000;
    std::string compression      = "snappy";
};

struct staffing_event {
    int64_t timestamp_ms;
    double  arrivals;
    int     optimal_staff;
    int     effective_staff;
    double  queue_length;
    double  shrinkage_planned;
    double  shrinkage_unplanned;
    bool    optimizer_parallel;
    int     n_islands;
    int     cycle;
};

struct forecast_event {
    int64_t             timestamp_ms;
    std::vector<double> arrivals_forecast;  // horizon pași
    std::vector<double> aht_forecast;
    int                 horizon_steps;
    double              interval_sec;
};

struct shrinkage_event {
    int64_t timestamp_ms;
    double  planned_pct;
    double  unplanned_pct;
    double  total_pct;
    int     rolling_window_days;
};

struct agent_gamification_event {
    int64_t     timestamp_ms;
    int         agent_id;
    double      total_points;
    int         badge_level;
    std::string badge_name;
    double      adherence_rate;
    double      avg_alert_time_sec;
};

struct shift_bidding_event {
    int64_t     timestamp_ms;
    int         agent_id;
    int         slot;
    bool        voluntary;          // true = a cerut OT, false = obligatoriu
    std::string badge_name;
    double      gamification_points;
    int         seniority_weeks;
};

struct fine_tune_event {
    int64_t timestamp_ms;
    float   huber_loss;
    int     step;
    int     seq_len_used;
};

struct queue_event {
    int64_t timestamp_ms;
    double  queue_length;
    double  lambda;          // apeluri/sec
    double  mu_eff;          // capacitate servire efectivă/sec
    double  net_rate;        // λ - μ_eff
    int     effective_agents;
};

// ─────────────────────────────────────────────────────────────────────────────
//  event_producer (formerly NNMPCKafkaProducer)
// ─────────────────────────────────────────────────────────────────────────────

class event_producer {
public:
    using config_t = config;  // alias for convenience

    // ── Constructor / Destructor ──────────────────────────────────────────

    explicit event_producer(const config& cfg = {}) : cfg_(cfg) {
        std::string err;

        auto conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

        auto set = [&](const std::string& k, const std::string& v) {
            if (conf->set(k, v, err) != RdKafka::Conf::CONF_OK)
                std::print(stderr, "[Kafka] Conf error {}: {}\n", k, err);
        };

        set("bootstrap.servers",  cfg_.brokers);
        set("client.id",          cfg_.client_id);
        set("linger.ms",          std::to_string(cfg_.linger_ms));
        set("batch.num.messages", std::to_string(cfg_.batch_num_msgs));
        set("compression.codec",  cfg_.compression);
        set("acks",               "1");          // leader ack, echilibru perf/durabilitate
        set("retries",            "3");
        set("retry.backoff.ms",   "200");

        // Delivery report callback – loghează erorile de livrare
        conf->set("dr_cb", &dr_cb_, err);

        producer_.reset(RdKafka::Producer::create(conf.get(), err));
        if (!producer_) {
            std::print(stderr, "[Kafka] Failed to create producer: {}\n", err);
            enabled_ = false;
            return;
        }

        enabled_ = true;
        std::print("[Kafka] Producer inițializat → {}\n", cfg_.brokers);
    }

    ~event_producer() {
        if (producer_) {
            producer_->flush(5000);  // flush pending messages la shutdown
        }
    }

    bool is_enabled() const { return enabled_; }

    // ── Publish metode ────────────────────────────────────────────────────

    void publish_staffing(const staffing_event& e) {
        std::string payload = json::obj({
            json::kv("event_type",           "staffing_decision"),
            json::kv("timestamp_ms",         (int)e.timestamp_ms),
            json::kv("arrivals",             e.arrivals),
            json::kv("optimal_staff",        e.optimal_staff),
            json::kv("effective_staff",      e.effective_staff),
            json::kv("queue_length",         e.queue_length),
            json::kv("shrinkage_planned_pct",  e.shrinkage_planned * 100.0),
            json::kv("shrinkage_unplanned_pct",e.shrinkage_unplanned * 100.0),
            json::kv("shrinkage_total_pct",
                     (e.shrinkage_planned + e.shrinkage_unplanned) * 100.0),
            json::kv("optimizer_parallel",   e.optimizer_parallel),
            json::kv("n_islands",            e.n_islands),
            json::kv("cycle",                e.cycle)
        });
        produce(cfg_.topic_staffing, payload,
                std::to_string(e.cycle));  // key = cycle pentru partitionare
    }

    void publish_forecast(const forecast_event& e) {
        std::string payload = json::obj({
            json::kv("event_type",       "forecast"),
            json::kv("timestamp_ms",     (int)e.timestamp_ms),
            json::kv("horizon_steps",    e.horizon_steps),
            json::kv("interval_sec",     e.interval_sec),
            "\"arrivals_forecast\":"  + json::arr(e.arrivals_forecast),
            "\"aht_forecast\":"       + json::arr(e.aht_forecast)
        });
        produce(cfg_.topic_forecast, payload);
    }

    void publish_shrinkage(const shrinkage_event& e) {
        std::string payload = json::obj({
            json::kv("event_type",           "shrinkage_update"),
            json::kv("timestamp_ms",         (int)e.timestamp_ms),
            json::kv("planned_pct",          e.planned_pct),
            json::kv("unplanned_pct",        e.unplanned_pct),
            json::kv("total_pct",            e.total_pct),
            json::kv("rolling_window_days",  e.rolling_window_days)
        });
        produce(cfg_.topic_shrinkage, payload);
    }

    void publish_agent_gamification(const agent_gamification_event& e) {
        std::string payload = json::obj({
            json::kv("event_type",        "gamification_update"),
            json::kv("timestamp_ms",      (int)e.timestamp_ms),
            json::kv("agent_id",          e.agent_id),
            json::kv("total_points",      e.total_points),
            json::kv("badge_level",       e.badge_level),
            json::kv("badge_name",        e.badge_name),
            json::kv("adherence_rate",    e.adherence_rate),
            json::kv("avg_alert_time_sec",e.avg_alert_time_sec)
        });
        // Key = agent_id → stickiness pe partiție per agent
        produce(cfg_.topic_gamif, payload, std::to_string(e.agent_id));
    }

    void publish_shift_bidding(const shift_bidding_event& e) {
        std::string payload = json::obj({
            json::kv("event_type",          "shift_bidding"),
            json::kv("timestamp_ms",        (int)e.timestamp_ms),
            json::kv("agent_id",            e.agent_id),
            json::kv("slot",                e.slot),
            json::kv("voluntary",           e.voluntary),
            json::kv("badge_name",          e.badge_name),
            json::kv("gamification_points", e.gamification_points),
            json::kv("seniority_weeks",     e.seniority_weeks)
        });
        produce(cfg_.topic_shift, payload);
    }

    void publish_fine_tune(const fine_tune_event& e) {
        std::string payload = json::obj({
            json::kv("event_type",    "fine_tune"),
            json::kv("timestamp_ms",  (int)e.timestamp_ms),
            json::kv("huber_loss",    (double)e.huber_loss),
            json::kv("step",          e.step),
            json::kv("seq_len_used",  e.seq_len_used)
        });
        produce(cfg_.topic_finetune, payload);
    }

    void publish_queue(const queue_event& e) {
        std::string payload = json::obj({
            json::kv("event_type",       "queue_state"),
            json::kv("timestamp_ms",     (int)e.timestamp_ms),
            json::kv("queue_length",     e.queue_length),
            json::kv("lambda",           e.lambda),
            json::kv("mu_eff",           e.mu_eff),
            json::kv("net_rate",         e.net_rate),
            json::kv("effective_agents", e.effective_agents)
        });
        produce(cfg_.topic_queue, payload);
    }

    // Poll non-blocant – apelat periodic pentru delivery reports
    void poll() {
        if (producer_) producer_->poll(0);
    }

private:
    config                              cfg_;
    bool                                enabled_ = false;
    std::unique_ptr<RdKafka::Producer>  producer_;

    // Delivery report callback – loghează erorile de livrare
    struct delivery_report_cb : public RdKafka::DeliveryReportCb {
        void dr_cb(RdKafka::Message& msg) override {
            if (msg.err() != RdKafka::ERR_NO_ERROR) {
                std::print(stderr, "[Kafka] Delivery error topic={} err={}\n",
                           msg.topic_name(), msg.errstr());
            }
        }
    } dr_cb_;

    void produce(const std::string& topic,
                 const std::string& payload,
                 const std::string& key = "") {
        if (!enabled_ || !producer_) return;

        RdKafka::ErrorCode err = producer_->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,           // auto partition
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(payload.data()), payload.size(),
            key.empty() ? nullptr : key.data(),
            key.empty() ? 0       : key.size(),
            json::epoch_ms(),                       // timestamp
            nullptr);                               // msg_opaque

        if (err != RdKafka::ERR_NO_ERROR) {
            if (err == RdKafka::ERR__QUEUE_FULL) {
                producer_->poll(100);               // flush și retry
                produce(topic, payload, key);
            } else {
                std::print(stderr, "[Kafka] Produce error topic={} err={}\n",
                           topic, RdKafka::err2str(err));
            }
        }

        producer_->poll(0);  // trigger delivery callbacks
    }
};

// ─────────────────────────────────────────────────────────────────────────────
//  event_publisher  –  wrapper de nivel înalt
//  Integrat în NNMPCController: apelat din run_step(), run_gamification_update()
//  etc. Dacă Kafka nu e disponibil, toate publish_* sunt no-op.
// ─────────────────────────────────────────────────────────────────────────────

class event_publisher {
public:
    explicit event_publisher(const config& cfg = {})
        : producer_(cfg), cycle_(0) {}

    bool is_enabled() const { return producer_.is_enabled(); }

    // Apelat din run_step() după calculul optimal staff
    void on_staffing_decision(double arrivals, int optimal, int effective,
                              double queue, double sh_planned, double sh_unplanned,
                              bool parallel, int n_islands,
                              const std::vector<double>& arr_pred,
                              const std::vector<double>& aht_pred,
                              double interval_sec,
                              double aht_sec)
    {
        int64_t ts = json::epoch_ms();
        ++cycle_;

        producer_.publish_staffing({
            ts, arrivals, optimal, effective,
            queue, sh_planned, sh_unplanned,
            parallel, n_islands, cycle_
        });

        producer_.publish_forecast({
            ts, arr_pred, aht_pred,
            (int)arr_pred.size(), interval_sec
        });

        double mu       = aht_sec > 0.0 ? 1.0 / aht_sec : 1.0 / 180.0;
        double lambda   = arrivals / interval_sec;

        producer_.publish_queue({
            ts, queue,
            lambda,
            static_cast<double>(effective) * mu,
            lambda - static_cast<double>(effective) * mu,
//            arrivals / interval_sec,
//            static_cast<double>(effective) *
//                (1.0 / 180.0),   // μ_eff estimat; înlocuiește cu AHT real
//            (arrivals / interval_sec) -
//                static_cast<double>(effective) * (1.0 / 180.0),
            effective
        });

        producer_.poll();
    }

    // Apelat din update_shrinkage()
    void on_shrinkage_update(double planned, double unplanned) {
        producer_.publish_shrinkage({
            json::epoch_ms(),
            planned * 100.0,
            unplanned * 100.0,
            (planned + unplanned) * 100.0,
            28  // Shrinkage::WINDOW
        });
        producer_.poll();
    }

    // Apelat din run_gamification_update() per agent
    void on_agent_gamification(int agent_id, double points, int badge_level,
                                const std::string& badge_name,
                                double adherence, double alert_time)
    {
        producer_.publish_agent_gamification({
            json::epoch_ms(), agent_id, points,
            badge_level, badge_name, adherence, alert_time
        });
        producer_.poll();
    }

    // Apelat din run_shift_bidding() per slot alocat
    void on_shift_bidding(int agent_id, int slot, bool voluntary,
                          const std::string& badge_name,
                          double points, int seniority_weeks)
    {
        producer_.publish_shift_bidding({
            json::epoch_ms(), agent_id, slot,
            voluntary, badge_name, points, seniority_weeks
        });
        producer_.poll();
    }

    // Apelat din online_fine_tune()
    void on_fine_tune(float loss, int step, int seq_len) {
        producer_.publish_fine_tune({json::epoch_ms(), loss, step, seq_len});
        producer_.poll();
    }

private:
    event_producer producer_;
    int            cycle_;
};



