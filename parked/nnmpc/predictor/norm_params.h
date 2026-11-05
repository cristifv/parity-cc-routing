#pragma once

#include <string>

struct norm_params {
  float           arr_lo = 0.0f, arr_hi = 100.0f;
  float           aht_lo = 60.0f, aht_hi = 300.0f;

  float norm_arr(double v) const;
  float norm_aht(double v) const;
  double denorm_arr(float n) const;
  double denorm_aht(float n) const;
  void save(const std::string&) const;
  bool load(const std::string&);
};
