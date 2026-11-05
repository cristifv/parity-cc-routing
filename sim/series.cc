#include "sim/series.h"

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>
#include <queue>
#include <random>

#include "sim/arrivals.h"

namespace pcr::sim {

namespace {

constexpr double kStep = 10;
constexpr int kStepsPerBucket = 90;
constexpr int64_t kDefaultStartMs = 1'767'571'200'000;  // 2026-01-05 00:00 UTC, a Monday

struct D {
  std::string kind = "exponential";
  double mean = 60, cv = 1, fixed = 0;
  static D of(const nlohmann::json& j) {
    D d;
    d.kind = j.value("kind", std::string("exponential"));
    d.mean = j.value("mean_s", 60.0);
    d.cv = j.value("cv", 1.0);
    d.fixed = j.value("s", 0.0);
    return d;
  }
  double mean_s() const { return kind == "fixed" ? fixed : mean; }
  double sample(std::mt19937_64& r, double scale = 1) const {
    if (kind == "fixed") return fixed * scale;
    if (kind == "lognormal") {
      const double s2 = std::log(1 + cv * cv);
      return std::lognormal_distribution<double>(std::log(mean * scale) - s2 / 2, std::sqrt(s2))(r);
    }
    return std::exponential_distribution<double>(1.0 / (mean * scale))(r);
  }
};

struct Caller {
  double arrive = 0, deadline = 0, talk = 0;
  int st = 0;  // 0 waiting, 1 served, 2 abandoned
};

struct Ev {
  double t;
  int type;  // 0 arrival, 1 departure, 2 deadline
  int64_t id;
  bool operator>(const Ev& o) const { return t != o.t ? t > o.t : type > o.type; }
};

uint64_t poisson(double lam, std::mt19937_64& r) {
  if (!(lam > 0)) return 0;
  return std::poisson_distribution<uint64_t>(lam)(r);
}

void simulate_queue(const nlohmann::json& q, const SeriesTenant& T, double dur, double occupancy, double factor,
                    double aht_drift_week, bool spikes, std::mt19937_64& rng, SeriesQueue* out) {
  RateModel rm = RateModel::from_json(q.at("arrivals"));
  if (!spikes) {
    rm.spikes.clear();
    rm.bots.clear();
  }
  const D talk = D::of(q.at("handle_time")), pat = D::of(q.at("patience"));
  const double redial_p = q.contains("redial") ? q["redial"].value("p", 0.0) : 0.0;
  const double redial_delay = q.contains("redial") ? q["redial"].value("mean_delay_s", 120.0) : 120.0;
  const int64_t steps = T.steps;
  const int64_t nb = (steps + kStepsPerBucket - 1) / kStepsPerBucket;
  out->id = q.at("id").get<std::string>();
  out->c10.assign(static_cast<size_t>(steps), 0);
  out->miss.assign(static_cast<size_t>(steps), 0);
  out->b_off.assign(static_cast<size_t>(nb), 0);
  out->b_ab.assign(static_cast<size_t>(nb), 0);
  out->b_hand.assign(static_cast<size_t>(nb), 0);
  out->b_handle_s.assign(static_cast<size_t>(nb), 0);
  const std::vector<double> gam = gamma_factors(rm.overdispersion, static_cast<size_t>(dur / 60) + 2, rng);
  double peak = 0;
  for (double t = 0; t < dur; t += 900) peak = std::max(peak, rm.planned(t) * 3600);
  out->peak_per_hour = peak;

  const double week = 7 * 86400.0;
  auto aht_scale = [&](double t) { return std::pow(1 + aht_drift_week, t / week); };
  auto cap_at = [&](double t) {
    const double slot = std::floor(t / 900) * 900 + 450;
    const double erl = rm.planned(slot) * talk.mean_s() * aht_scale(slot);
    if (erl <= 0) return 1;
    return std::max(1, static_cast<int>(std::ceil(erl / occupancy * factor)));
  };

  std::vector<Caller> calls;
  std::priority_queue<Ev, std::vector<Ev>, std::greater<Ev>> pq;
  std::deque<int64_t> waiting;
  int busy = 0;
  int cap = cap_at(0);
  std::uniform_real_distribution<double> u01(0, 1);
  auto add_call = [&](double t, double patience, double talk_s) {
    calls.push_back(Caller{t, t + patience, talk_s, 0});
    pq.push(Ev{t, 0, static_cast<int64_t>(calls.size() - 1)});
  };
  auto start = [&](int64_t id, double now) {
    calls[static_cast<size_t>(id)].st = 1;
    ++busy;
    pq.push(Ev{now + calls[static_cast<size_t>(id)].talk, 1, id});
  };
  auto serve = [&](double now) {
    while (busy < cap && !waiting.empty()) {
      const int64_t id = waiting.front();
      waiting.pop_front();
      if (calls[static_cast<size_t>(id)].st == 0) start(id, now);
    }
  };
  auto step_of = [&](double t) { return std::clamp<int64_t>(static_cast<int64_t>(t / kStep), 0, steps - 1); };
  for (int64_t k = 0; k < steps; ++k) {
    const double t0 = k * kStep;
    // This step's new arrivals (callers, bursts, bots).
    const double lam = rm.rate(t0 + kStep / 2) * kStep * gam[static_cast<size_t>(t0 / 60)];
    const double sc = aht_scale(t0);
    for (uint64_t n = poisson(lam, rng); n > 0; --n) add_call(t0 + u01(rng) * kStep, pat.sample(rng), talk.sample(rng, sc));
    if (rm.burst_per_hour > 0) {
      for (uint64_t b = poisson(rm.burst_per_hour / 3600 * kStep, rng); b > 0; --b) {
        const double at = t0 + u01(rng) * kStep;
        const uint64_t size = 1 + std::geometric_distribution<uint64_t>(1.0 / rm.burst_mean)(rng);
        for (uint64_t i = 0; i < size; ++i) add_call(at + u01(rng) * 3, pat.sample(rng), talk.sample(rng, sc));
      }
    }
    if (const double br = rm.bot_rate(t0 + kStep / 2); br > 0) {
      double bp = 3, bt = 5;
      for (const auto& b : rm.bots) {
        if (t0 >= b.start && t0 < b.start + b.duration) {
          bp = b.patience_s;
          bt = b.talk_s;
        }
      }
      for (uint64_t n = poisson(br * kStep, rng); n > 0; --n) add_call(t0 + u01(rng) * kStep, bp, bt);
    }
    if (static_cast<int64_t>(t0) % 900 == 0) {
      cap = cap_at(t0);
      serve(t0);
    }
    while (!pq.empty() && pq.top().t < t0 + kStep) {
      const Ev e = pq.top();
      pq.pop();
      Caller& c = calls[static_cast<size_t>(e.id)];
      const int64_t s = step_of(e.t);
      const size_t b = static_cast<size_t>(s / kStepsPerBucket);
      if (e.type == 0) {
        out->c10[static_cast<size_t>(s)] = static_cast<uint16_t>(std::min<int>(65535, out->c10[static_cast<size_t>(s)] + 1));
        out->b_off[b] += 1;
        if (busy < cap && waiting.empty()) {
          start(e.id, e.t);
        } else {
          waiting.push_back(e.id);
          pq.push(Ev{c.deadline, 2, e.id});
        }
      } else if (e.type == 1) {
        --busy;
        out->b_hand[b] += 1;
        out->b_handle_s[b] += static_cast<float>(c.talk);
        serve(e.t);
      } else if (c.st == 0) {
        c.st = 2;
        out->b_ab[b] += 1;
        if (redial_p > 0 && u01(rng) < redial_p) {
          const double at = e.t + std::exponential_distribution<double>(1.0 / std::max(1.0, redial_delay))(rng);
          if (at < dur) add_call(at, pat.sample(rng), talk.sample(rng, aht_scale(at)));
        }
      }
    }
  }
}

}  // namespace

SeriesTenant simulate_series(const nlohmann::json& sc, uint64_t seed, const SeriesOptions& o) {
  SeriesTenant T;
  T.name = sc.value("name", std::string());
  T.seed = seed;
  const nlohmann::json f = sc.value("forecast", nlohmann::json::object());
  T.start_ms = o.start_ms >= 0 ? o.start_ms : f.value("start_ms", kDefaultStartMs);
  const double dur = o.duration_s > 0 ? o.duration_s : sc.at("duration_s").get<double>();
  T.steps = static_cast<int64_t>(dur / kStep);
  const int64_t nb = (T.steps + kStepsPerBucket - 1) / kStepsPerBucket;
  const nlohmann::json st = f.value("staffing", nlohmann::json::object());
  const double occupancy = std::clamp(st.value("occupancy", 0.85), 0.3, 1.0);
  const double factor = std::clamp(st.value("factor", 1.0), 0.2, 3.0);
  const double aht_drift = f.contains("sample") ? f["sample"].value("aht_drift_per_week", 0.0) : 0.0;
  std::mt19937_64 rng(seed);
  for (const auto& q : sc.at("tenants").at(0).at("queues")) {
    SeriesQueue sq;
    simulate_queue(q, T, dur, occupancy, factor, aht_drift, o.spikes, rng, &sq);
    T.q.push_back(std::move(sq));
  }
  // Known calendar windows (the forecaster's flags), per bucket.
  T.holiday.assign(static_cast<size_t>(nb), 0);
  T.event.assign(static_cast<size_t>(nb), 0);
  const nlohmann::json cal = f.value("calendar", nlohmann::json::object());
  for (const char* key : {"holidays", "events"}) {
    auto& v = std::string(key) == "holidays" ? T.holiday : T.event;
    for (const auto& w : cal.value(key, nlohmann::json::array())) {
      if (!w.value("known", false)) continue;
      const int64_t a = static_cast<int64_t>(w.value("start_s", 0.0) / 900), b = static_cast<int64_t>(w.value("end_s", 0.0) / 900);
      for (int64_t i = std::max<int64_t>(0, a); i <= b && i < nb; ++i) v[static_cast<size_t>(i)] = 1;
    }
  }
  if (!o.apply_faults) return T;
  auto find = [&](const std::string& id) -> SeriesQueue* {
    for (auto& q : T.q) {
      if (q.id == id) return &q;
    }
    return nullptr;
  };
  auto mark_buckets = [&](SeriesQueue* q) {  // a bucket with half its steps missing is missing
    for (int64_t b = 0; b < nb; ++b) {
      int m = 0;
      for (int64_t s = b * kStepsPerBucket; s < std::min(T.steps, (b + 1) * kStepsPerBucket); ++s) m += q->miss[static_cast<size_t>(s)];
      if (m * 2 >= kStepsPerBucket) {
        const float nan = std::numeric_limits<float>::quiet_NaN();
        q->b_off[static_cast<size_t>(b)] = q->b_ab[static_cast<size_t>(b)] = q->b_hand[static_cast<size_t>(b)] = nan;
        q->b_handle_s[static_cast<size_t>(b)] = nan;
      }
    }
  };
  for (const auto& d : f.value("data_faults", nlohmann::json::array())) {
    const std::string kind = d.value("kind", std::string());
    const int64_t a = std::clamp<int64_t>(static_cast<int64_t>(d.value("at_s", 0.0) / kStep), 0, T.steps);
    const int64_t e = std::clamp<int64_t>(a + static_cast<int64_t>(d.value("duration_s", 0.0) / kStep), a, T.steps);
    std::vector<SeriesQueue*> qs;
    if (d.contains("queue")) {
      if (auto* q = find(d["queue"].get<std::string>())) qs.push_back(q);
    } else if (kind == "gap" || kind == "duplicate") {
      for (auto& q : T.q) qs.push_back(&q);
    }
    if (kind == "gap") {
      for (auto* q : qs) {
        for (int64_t s = a; s < e; ++s) {
          q->miss[static_cast<size_t>(s)] = 1;
          q->c10[static_cast<size_t>(s)] = 0;
        }
        mark_buckets(q);
      }
    } else if (kind == "duplicate") {
      for (auto* q : qs) {
        for (int64_t s = a; s < e; ++s) {
          const uint16_t c = q->c10[static_cast<size_t>(s)];
          q->c10[static_cast<size_t>(s)] = static_cast<uint16_t>(std::min<int>(65535, 2 * c));
          q->b_off[static_cast<size_t>(s / kStepsPerBucket)] += c;
        }
      }
    } else if (kind == "rename" && !qs.empty()) {
      SeriesQueue n = *qs.front();
      n.id = d.value("to_queue", qs.front()->id + "-renamed");
      n.renamed = true;
      n.first_step = a;
      for (int64_t s = 0; s < a; ++s) {
        n.miss[static_cast<size_t>(s)] = 1;
        n.c10[static_cast<size_t>(s)] = 0;
      }
      for (int64_t s = a; s < T.steps; ++s) {
        qs.front()->miss[static_cast<size_t>(s)] = 1;
        qs.front()->c10[static_cast<size_t>(s)] = 0;
      }
      mark_buckets(qs.front());
      mark_buckets(&n);
      T.q.push_back(std::move(n));
      T.notes.push_back("rename");
    } else if (kind == "merge") {
      std::vector<std::string> ids;
      for (const auto& x : d.value("queues", nlohmann::json::array())) ids.push_back(x.get<std::string>());
      SeriesQueue* to = find(d.value("to_queue", ids.empty() ? std::string() : ids.front()));
      if (!to) continue;
      for (const auto& id : ids) {
        SeriesQueue* from = find(id);
        if (!from || from == to) continue;
        for (int64_t s = a; s < T.steps; ++s) {
          to->c10[static_cast<size_t>(s)] =
              static_cast<uint16_t>(std::min<int>(65535, to->c10[static_cast<size_t>(s)] + from->c10[static_cast<size_t>(s)]));
          from->miss[static_cast<size_t>(s)] = 1;
          from->c10[static_cast<size_t>(s)] = 0;
        }
        for (int64_t b = a / kStepsPerBucket + 1; b < nb; ++b) {
          to->b_off[static_cast<size_t>(b)] += from->b_off[static_cast<size_t>(b)];
          to->b_ab[static_cast<size_t>(b)] += from->b_ab[static_cast<size_t>(b)];
          to->b_hand[static_cast<size_t>(b)] += from->b_hand[static_cast<size_t>(b)];
          to->b_handle_s[static_cast<size_t>(b)] += from->b_handle_s[static_cast<size_t>(b)];
        }
        mark_buckets(from);
      }
      T.notes.push_back("merge");
    }
  }
  return T;
}

}  // namespace pcr::sim
