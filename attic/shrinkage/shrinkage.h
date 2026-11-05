#pragma once

#include <vector>


struct shrinkage {
  double                          planned = 0.20;
  double                          unplanned = 0.10;
  std::vector<double>             planned_history;
  std::vector<double>             unplanned_history;
  static constexpr int            WINDOW = 28;

};
