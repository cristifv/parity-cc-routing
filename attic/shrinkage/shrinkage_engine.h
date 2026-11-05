#pragma once

#include <algorithm>
#include <cmath>
#include <numeric>

#include <metric.h>
#include <shrinkage.h>
#include <shrinkage_record.h>

struct shrinkage_engine {

  static std::pair<double, double> from_metric(const metric &m) {
    if (m.current_agents <= 0)
      return {0.20, 0.10};
    double s = m.current_agents;
    return {
        std::clamp(static_cast<double>(m.agents_absent_planned) / s, 0.0, 0.60),
        std::clamp(static_cast<double>(m.agents_absent_unplanned) / s, 0.0,
                   0.30)};
  }

  static std::pair<double, double>
  from_records(const std::vector<shrinkage_record> &recs) {
    if (recs.empty())
      return {0.20, 0.10};

    double sp = 0.0, su = 0.0;
    int cnt = 0;
    for (const auto &r : recs) {
      if (r.total_scheduled <= 0)
        continue;

      double s = r.total_scheduled;
      sp += std::clamp((r.absent_vacation + r.absent_training + r.on_break +
                        r.on_coaching + r.on_aux) /
                           s,
                       0.0, 0.60);
      su += std::clamp((r.absent_sick + r.absent_noshow) / s, 0.0, 0.30);
      ++cnt;
    }

    return cnt ? std::pair{sp / cnt, su / cnt} : std::pair{0.20, 0.10};
  }

  static void update_rolling(shrinkage &sh, const metric &m) {
    auto [pl, unpl] = from_metric(m);
    sh.planned_history.push_back(pl);
    sh.unplanned_history.push_back(unpl);
    while ((int)sh.planned_history.size() > shrinkage::WINDOW)
      sh.planned_history.erase(sh.planned_history.begin());
    while ((int)sh.unplanned_history.size() > shrinkage::WINDOW)
      sh.unplanned_history.erase(sh.unplanned_history.begin());
    sh.planned = std::accumulate(sh.planned_history.begin(),
                                 sh.planned_history.end(), 0.0) /
                 sh.planned_history.size();
    sh.unplanned = std::accumulate(sh.unplanned_history.begin(),
                                   sh.unplanned_history.end(), 0.0) /
                   sh.unplanned_history.size();
  }

  static void update_rolling(shrinkage &sh,
                             const std::vector<shrinkage_record> &recs) {
    auto [pl, unpl] = from_records(recs);
    sh.planned_history.push_back(pl);
    sh.unplanned_history.push_back(unpl);
    while ((int)sh.planned_history.size() > shrinkage::WINDOW)
      sh.planned_history.erase(sh.planned_history.begin());
    while ((int)sh.unplanned_history.size() > shrinkage::WINDOW)
      sh.unplanned_history.erase(sh.unplanned_history.begin());
    auto mean = [](const std::vector<double> &v) {
      return v.empty() ? 0.0
                       : std::accumulate(v.begin(), v.end(), 0.0) / v.size();
    };
    sh.planned = mean(sh.planned_history);
    sh.unplanned = mean(sh.unplanned_history);
  }
};
