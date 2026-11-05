#pragma once

#include <atomic>
#include <chrono>
#include <ctime>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include <app_config.h>
#include <gamification_score.h>
#include <performance_record.h>

struct agent {
  int id;
  double rating = 0.8;
  bool prefers_overtime = false;
  double adherence_score = 1.0;
  double avg_alert_time_sec = 30.0;
  int seniority_weeks = 0;
  gamification_score gamification;
  std::vector<performance_record> perf_history;
};

struct agent_rta {
  int id;
  std::string site_id;
  std::string state;
  std::string prev_state;
  system_clock::time_point last_change;
  std::vector<std::string> skills;
};

struct rta_manager {
public:
  rta_manager(const app_config &cfg);

  int count_available() const;

  void start_simulation();

  ~rta_manager();

  mutable std::shared_mutex mtx_;

private:
  const app_config &cfg_;
  std::vector<agent_rta> agents_;
  std::thread sim_thread_;
};
