#pragma once

#include <vector>

#include <acd.h>

#define SALES     "sales"
#define SUPPORT   "support"
#define BILLING   "billing"



struct site_config {
  std::string   site_id;
  std::string   display_name;
  ACDType       acd_type           = ACDType::CloudMock;
  std::string   acd_endpoint;
  std::string   acd_api_key;
  std::string   acd_region;
  std::string   timezone;
  double        sla_target_sec     = 20.0;
  int           max_retry          = 3;
  int           timeout_ms         = 5000;
  std::vector<std::string> site_queues = { SALES, SUPPORT, BILLING };
};

static std::vector<site_config> all_sites = {
  {"site_us", "New York US",  ACDType::CloudMock, "", "", "",
         "America/New_York", 15.0, 3, 5000, {SALES, SUPPORT}},
  {"site_ro", "București RO", ACDType::CloudMock, "", "", "",
         "Europe/Bucharest", 20.0, 3, 5000, {SALES, SUPPORT, BILLING}},
  {"site_de", "Berlin DE",    ACDType::CloudMock, "", "", "",
         "Europe/Berlin",    25.0, 3, 5000, {SALES, BILLING}},
};
