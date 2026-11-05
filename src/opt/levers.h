// The levers (phase 5 redesign, the user 2026-11-01; PLAN §5.3, §3.4): the CTMC drives the routing
// decisions, and only three levers act. Nothing here moves an agent between queues, logs anyone in
// or sends anyone on a break: supervisor pins stay manual (phase 4).
//   (a) Each queue's dispatch weight w_i and secondary delay d_i, tuned every tick from the per-agent
//       model's predicted SL and abandonment against J = sum_i lambda_i [alpha_i (1 - SL_i) + beta_i
//       Pab_i] (both classes, VIP with its own threshold and importance): a bounded coordinate search
//       from the weights and delays in force (multiplicative steps for w, `delay_step_s` for d, within
//       config.routing's bounds), over queues whose current result is exact; the plan keeps a change
//       only if J drops by >= 2 % (or 0.5 missed-SL calls per hour) and the previous tick proposed the
//       same value (hysteresis in value and in time). Applied by the writer in auto, exact results only.
//   (b) Per-queue min/max agents (config.routing): the writer's dispatch applies them in auto; the
//       optimizer recommends values (advice).
//   (c) Staffing recommendations: per queue, the READY agents it needs within the horizon so that its
//       predicted SL at the forecast meets its sl_target, with the predicted effect (advice only).
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "model/agents.h"
#include "src/core/model_feed.h"

namespace pcr::opt {

struct LeverQueue {
  std::string id;
  double lambda = 0, vip_lambda = 0, mu = 0, theta = 0;  // per second (the forecast's now)
  double sla_s = 20, vip_sla_s = 0;
  double alpha = 1, beta = 1, vip_alpha = 2, vip_beta = 2;
  double weight = 1, weight_default = 1, weight_min = 0.25, weight_max = 4;  // lever (a): in force, default, bounds
  double delay_s = 10, delay_default_s = 10, delay_min_s = 0, delay_max_s = 60;
  int min_agents = 0, max_agents = 0, vip_reserved = 0;
  double sl_target = 0.8;
  double horizon_lambda = 0, horizon_vip_lambda = 0;  // lever (c): the forecast over the horizon
};

struct LeverOptions {
  int max_candidates = 24;            // model evaluations per tenant per tick (CR-028)
  int64_t time_cap_us = 100'000;      // the tenant's cap (enterprise 100 ms; PLAN §3.5)
  double hysteresis_rel = 0.02;
  double hysteresis_abs = 0.5 / 3600;  // 0.5 missed-SL calls per hour, per second
  double weight_step = 1.41421356237;  // a candidate weight: x or / this
  double delay_step_s = 5;
  int max_recommend_queues = 8;       // lever (c): queues looked at per recommending tick
  int max_extra_agents = 64;
  double horizon_s = 900;
  model::AgentOptions model;
};

struct Recommendation {
  std::string kind;  // ready_agents | min_agents | max_agents
  std::string queue;
  int delta = 0, value = 0, current = 0;
  model::QueueMetrics before, after;
  model::Quality quality = model::Quality::kExact;
  std::string reason;
};

struct LeverResult {
  bool ok = false;
  std::string why;
  model::Dispatch dispatch = model::Dispatch::kLongestIdle;
  model::Quality quality = model::Quality::kExact;  // worst over the queues (current and planned)
  std::vector<model::AgentQueue> current, planned;
  std::vector<double> w_planned, d_planned;          // per queue (seconds)
  std::vector<model::Quality> q_quality;             // per queue: worst of current and planned
  double j_current = 0, j_planned = 0;
  int evaluated = 0, pools = 0, counted = 0;
  bool capped = false;
  std::string held;  // why the search's best wasn't kept (hysteresis), empty if kept or none found
  std::vector<Recommendation> recs;
};

// The last tick's proposal per queue (hysteresis in time), kept per tenant by the optimizer.
struct LeverMemory {
  std::map<std::string, std::pair<double, double>> proposed;  // queue -> (weight, delay_s)
};

// The per-agent model's inputs from a snapshot (every counted agent individually; the queue order
// is the snapshot's). Tiers when some agent's set has a SECONDARY queue.
void model_inputs(const ModelSnapshot& s, const std::vector<LeverQueue>& q, std::vector<model::AQueue>* aq,
                  std::vector<model::AAgent>* ag, model::Dispatch* d);

// One tenant's tick: the model at the levers in force; with `search` lever (a)'s search and its
// hysteresis; with `recommend` levers (b) and (c)'s advice.
LeverResult plan_levers(const ModelSnapshot& s, const std::vector<LeverQueue>& q, LeverMemory* mem, bool search,
                        bool recommend, const LeverOptions& o, model::Stats* st);

}  // namespace pcr::opt
