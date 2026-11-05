// Phase 5's Kafka events (PLAN §5.4; parity-contracts 0.12.0 schemas/events/{staffing,queue,forecast}-events.v1.json):
// `parity.staffing` (staffing_plan every tick, staffing_recommendation in advise and auto), `queue.events`
// (queue_snapshot per queue per tick) and `parity.forecast` (forecast per queue, its 8 buckets).
// Key tenant_id, headers tenant_id + schema_version, event_id UUIDv7, no personal data (agent ids are
// opaque; no numbers, no customer keys). Built on the optimization threads; events are not state.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "model/ctmc.h"
#include "src/core/model_feed.h"
#include "src/core/tenant.h"
#include "src/opt/forecast.h"
#include "src/opt/levers.h"

namespace pcr::opt {

inline constexpr char kStaffingTopic[] = "parity.staffing";
inline constexpr char kQueueTopic[] = "queue.events";
inline constexpr char kForecastTopic[] = "parity.forecast";

struct PlanInfo {
  uint64_t version = 0;
  int64_t valid_until_ms = 0;
  std::string mode;     // off | advise | auto
  std::string applies;  // levers | none | held: <why>
  std::string source;   // the forecast's worst source this tick
};

AgentEvent staffing_plan_event(const ModelSnapshot& s, const std::vector<LeverQueue>& q, const LeverResult& r,
                               const PlanInfo& p);
// Lever (b) and (c)'s advice with predicted effects; nothing is applied by it.
AgentEvent recommendation_event(const ModelSnapshot& s, const LeverResult& r, const PlanInfo& p, double horizon_s);
AgentEvent queue_snapshot_event(const ModelSnapshot& s, size_t queue, const model::QueueMetrics& m);
AgentEvent forecast_event(const ModelSnapshot& s, const QueueForecast& f);

}  // namespace pcr::opt
