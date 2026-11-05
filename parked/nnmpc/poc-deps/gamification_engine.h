#pragma once

#include <vector>

#include <kpi_thresholds.h>
#include <gamification_score.h>
#include <performance_record.h>

struct gamification_engine {
  explicit gamification_engine(kpi_thresholds thr = {});

  gamification_score compute(const performance_record& r) const; 

  gamification_score compute_aggregate(const std::vector<performance_record>&) const;

  private:
  kpi_thresholds      thr_;

  static double hib(double v, double tgt, double ceil_v) {
    if (v >= ceil_v) return 100.0;
    if (v <= 0.0) return 0.0;
    return v < tgt ? 50.0*v/tgt : 50.0 + 50.0 *(v-tgt)/(ceil_v - tgt);
  }

  static double lib(double v, double tgt, double mn, double mx) {
    if (v < mn) return 60.0;
    if (v>= mx) return 0.0;
    return v <= tgt ? 100.0 - 40.0 *(v - mn)/std::max(1.0, tgt-mn)
      : 50.0 * (mx - v)/std::max(1.0, mx - tgt);
  }
};
