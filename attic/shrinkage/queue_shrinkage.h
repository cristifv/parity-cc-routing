#pragma once

#include <string>
#include <unordered_map>

#include <site_config.h>

static std::unordered_map<std::string, double> all_queue_shrinkage = {
  {SALES,   0.22},
  {SUPPORT, 0.27},
  {BILLING, 0.25}
};
