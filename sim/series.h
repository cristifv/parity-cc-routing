// The series DES (PLAN §5.9; phase 5b+5c): turns a routing-scenario.v1 file (a training sample from
// parity-contracts' catalogue, or an OOD scenario's normal history) into the forecaster's series: per
// queue, arrivals per 10 s step and per 15-min bucket the offered, abandoned and handled calls and
// their handle time. Each queue is a c-server FCFS queue with patience, staffed for its PLANNED load
// (no spikes: ceil(planned offered load in busy agents / occupancy * factor)), so spikes, surges and outages produce
// abandons, and abandons produce redials (arrivals that depend on service). Overdispersion (a
// per-minute gamma factor), clustered bursts and bots are on top. The data faults (gaps, duplicate
// minutes, a rename or merge) are applied to the observed series afterwards, never to the traffic.
//
// It is not the routing DES (sim/des: the real dispatch, agents one by one): counts don't depend on
// which agent takes which call, and this one is ~100x faster, which the base model's data needs.
#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace pcr::sim {

struct SeriesQueue {
  std::string id;
  std::vector<uint16_t> c10;   // arrivals per 10 s step (observed)
  std::vector<uint8_t> miss;   // 1: the step is missing from the observed data (a gap, before a rename)
  // Per 15-min bucket (observed).
  std::vector<float> b_off, b_ab, b_hand, b_handle_s;
  double peak_per_hour = 0;    // the planned peak (a new queue's expected volume)
  bool renamed = false;        // a queue that appeared by a rename (history starts at the rename)
  int64_t first_step = 0;      // its first observed step
};

struct SeriesTenant {
  std::string name;
  uint64_t seed = 0;
  int64_t start_ms = 0;         // epoch ms of step 0
  int64_t steps = 0;            // 10 s steps
  std::vector<SeriesQueue> q;
  std::vector<uint8_t> holiday, event;  // per bucket: known holiday / known event (the forecaster's flags)
  std::vector<std::string> notes;
};

struct SeriesOptions {
  bool apply_faults = true;
  bool spikes = true;           // false: the normal traffic only (an OOD scenario's history)
  double duration_s = 0;        // 0: the scenario's
  int64_t start_ms = -1;        // -1: forecast.start_ms (or 2026-01-05)
};

SeriesTenant simulate_series(const nlohmann::json& scenario, uint64_t seed, const SeriesOptions& o = {});

}  // namespace pcr::sim
