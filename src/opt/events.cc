#include "src/opt/events.h"

#include <algorithm>
#include <cmath>
#include <nlohmann/json.hpp>

#include "src/core/types.h"

namespace pcr::opt {

namespace {

using json = nlohmann::json;

double num(double x) { return std::isfinite(x) ? x : 0.0; }

json frame(const std::string& type, const ModelSnapshot& s) {
  return json{{"schema_version", 1}, {"event_id", new_uuid_v7(s.at_ms)}, {"type", type}, {"tenant_id", s.tenant},
              {"at_ms", s.at_ms}};
}

json metrics(const model::QueueMetrics& m) {
  return json{{"sl", num(m.sl)}, {"abandon", num(m.abandon)}, {"ewt_s", num(m.ewt_mean)},
              {"quality", m.ok ? model::quality_name(m.quality) : "refused"}};
}

json metrics(const model::AgentQueue& q) {
  json j = metrics(q.m);
  if (q.vip.ok) j["vip"] = {{"sl", num(q.vip.sl)}, {"abandon", num(q.vip.abandon)}, {"ewt_s", num(q.vip.ewt_mean)}};
  return j;
}

AgentEvent wrap(const char* topic, const ModelSnapshot& s, const std::string& type, const json& body) {
  AgentEvent e;
  e.kind = type;
  e.tenant = s.tenant;
  e.at_ms = s.at_ms;
  e.topic = topic;
  e.json = body.dump();
  return e;
}

}  // namespace

AgentEvent staffing_plan_event(const ModelSnapshot& s, const std::vector<LeverQueue>& q, const LeverResult& r,
                               const PlanInfo& p) {
  json b = frame("staffing_plan", s);
  b["plan_version"] = p.version;
  b["valid_until_ms"] = p.valid_until_ms;
  b["mode"] = p.mode;
  b["applies"] = p.applies;
  b["forecast_source"] = p.source;
  b["quality"] = model::quality_name(r.quality);
  b["dispatch"] = r.dispatch == model::Dispatch::kTiers ? "tiers" : "longest_idle";
  b["objective"] = {{"current", num(r.j_current)}, {"planned", num(r.j_planned)}};
  b["agents"] = {{"counted", r.counted}, {"pools", r.pools}};
  json qs = json::array();
  for (size_t i = 0; i < q.size() && i < r.current.size() && i < r.planned.size(); ++i) {
    json x = {{"queue_id", q[i].id},
              {"arrivals_per_s", num(q[i].lambda)},
              {"vip_arrivals_per_s", num(q[i].vip_lambda)},
              {"agents", {{"reach", num(r.current[i].R)}, {"primary", num(r.current[i].P)}, {"busy", num(r.current[i].busy)}}},
              {"weight", {{"current", num(q[i].weight)}, {"planned", num(r.w_planned[i])}, {"default", num(q[i].weight_default)}}},
              {"delay_s", {{"current", num(q[i].delay_s)}, {"planned", num(r.d_planned[i])}, {"default", num(q[i].delay_default_s)}}},
              {"min_agents", q[i].min_agents},
              {"max_agents", q[i].max_agents},
              {"quality", model::quality_name(r.q_quality[i])},
              {"current", metrics(r.current[i])},
              {"planned", metrics(r.planned[i])}};
    const std::string& why = !r.planned[i].why.empty() ? r.planned[i].why : r.current[i].why;
    if (!why.empty()) x["why"] = why.substr(0, 64);
    qs.push_back(std::move(x));
  }
  b["queues"] = qs;
  return wrap(kStaffingTopic, s, "staffing_plan", b);
}

AgentEvent recommendation_event(const ModelSnapshot& s, const LeverResult& r, const PlanInfo& p, double horizon_s) {
  json b = frame("staffing_recommendation", s);
  b["plan_version"] = p.version;
  b["mode"] = p.mode;
  b["forecast_source"] = p.source;
  b["horizon_s"] = static_cast<int64_t>(horizon_s);
  json recs = json::array();
  for (const auto& rc : r.recs) {
    json x = {{"kind", rc.kind},
              {"queue_id", rc.queue},
              {"current", std::max(0, rc.current)},
              {"predicted", {{"before", metrics(rc.before)}, {"after", metrics(rc.after)}}},
              {"quality", model::quality_name(rc.quality)},
              {"reason", rc.reason.substr(0, 128)}};
    if (rc.kind == "ready_agents") {
      x["delta"] = rc.delta;
      x["within_s"] = static_cast<int64_t>(horizon_s);
    } else {
      x["value"] = std::max(0, rc.value);
    }
    recs.push_back(std::move(x));
  }
  b["recommendations"] = recs;
  return wrap(kStaffingTopic, s, "staffing_recommendation", b);
}

AgentEvent queue_snapshot_event(const ModelSnapshot& s, size_t i, const model::QueueMetrics& m) {
  const ModelSnapshot::Queue& q = s.queues[i];
  json b = frame("queue_snapshot", s);
  b["queue_id"] = q.id;
  b["waiting"] = q.waiting;
  b["head_age_s"] = num(q.head_age_s);
  b["logged_in"] = q.logged_in;
  b["vip_waiting"] = q.vip_waiting;
  b["busy_agents"] = std::max(0, q.busy_agents);
  b["weight"] = num(q.weight);
  b["delay_s"] = num(q.secondary_delay_ms / 1000.0);
  b["ewt_s"] = num(m.ewt_mean);
  b["ewt_quality"] = m.ok ? model::quality_name(m.quality) : "refused";
  const uint64_t done = q.bucket_answered + q.bucket_abandoned;
  b["bucket"] = {{"start_ms", q.bucket_start_ms}, {"offered", q.bucket_offered}, {"answered", q.bucket_answered},
                 {"answered_in_sla", q.bucket_in_sla}, {"abandoned", q.bucket_abandoned},
                 {"sl_so_far", done > 0 ? static_cast<double>(q.bucket_in_sla) / static_cast<double>(done) : 1.0}};
  return wrap(kQueueTopic, s, "queue_snapshot", b);
}

AgentEvent forecast_event(const ModelSnapshot& s, const QueueForecast& f) {
  json b = frame("forecast", s);
  b["queue_id"] = f.queue;
  b["source"] = f.source;
  b["aht_s"] = num(f.aht_s);
  b["wrap_s"] = num(f.wrap_s);
  b["patience_s"] = num(f.patience_s);
  b["vip_share"] = std::clamp(num(f.vip_share), 0.0, 1.0);
  json bs = json::array();
  const bool model = f.source == "model";
  for (size_t k = 0; k < f.per_s.size(); ++k) {
    json x = {{"start_ms", f.first_bucket_ms + static_cast<int64_t>(k) * 900'000}, {"arrivals_per_s", num(f.per_s[k])},
              {"offered", num(f.per_s[k] * 900)}};
    // Phases 5b+5c (contracts 0.15.0, additive): the model's band.
    if (model && k < f.p10_per_s.size() && k < f.p90_per_s.size()) {
      x["p10_per_s"] = num(f.p10_per_s[k]);
      x["p90_per_s"] = num(f.p90_per_s[k]);
    }
    bs.push_back(std::move(x));
  }
  b["buckets"] = bs;
  // Only with the learned forecaster on (a phase-5 run's events stay as they were).
  if (model || !f.fallback.empty()) b["aht_source"] = f.aht_from_model ? "model" : "baseline";
  if (model) b["model_version"] = f.model_version;
  static const char* kReasons[] = {"no_model", "stale", "above_range", "drift", "ratio", "width", "realized_error", "nan",
                                   "negative", "slow"};
  if (!model && !f.fallback.empty()) {
    for (const char* r : kReasons) {
      if (f.fallback == r) b["fallback"] = f.fallback;
    }
  }
  return wrap(kForecastTopic, s, "forecast", b);
}

}  // namespace pcr::opt
