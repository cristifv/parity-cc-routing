#pragma once

#include <chrono>
#include <ctime>

using namespace std::chrono;

struct performance_record {
  int             agent_id = 0;
  double          fcr_rate = 0.0;
  double          csat_score = 0.0;
  double          adherence_rate = 0.0;
  double          avg_handle_time_s = 0.0;
  double          alert_response_s = 0.0;
  int             calls_handled = 0;
  double          quality_score = 0.0;
  system_clock::time_point   period_start;
};
