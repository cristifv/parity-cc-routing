#pragma once

#include <string>

struct gamification_score {
  int           agent_id = 0;
  double        total_points = 0.0;
  int           badge_level = 0;
  std::string   badge_name = "Bronze";
};

