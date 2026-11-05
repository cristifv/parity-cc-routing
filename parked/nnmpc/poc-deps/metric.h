#pragma once

#include <vector>
#include <string>

using namespace std;

struct metric {
  std::string     site_id;
  std::string     queue_id;
  double          inbound_calls = 0.0;
  double          aht_sec = 180.0;
  int             current_agents = 25;
  int             agents_absent_planned = 0;
  int             agents_absent_unplanned = 0;
  int             agents_on_break = 0;
  int             hour = 0;
  int             dayofweek = 0;
  int             month = 1;
  double          timestamp_epoch = 0.0;
};

struct metric_feature_extractor {
  vector<double> operator() (const metric& m) const {
    return {m.inbound_calls, m.aht_sec};
  }
};
