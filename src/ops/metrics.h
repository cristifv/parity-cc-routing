// A small Prometheus text registry (PLAN §5.6): counters by (name, labels), rendered on 9810.
// Labels never carry tenant, interaction or agent ids.
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace pcr {

class Metrics {
 public:
  void inc(const std::string& name, const std::string& labels = "", uint64_t by = 1);
  // A gauge or counter computed when scraped.
  void computed(const std::string& name, const std::string& type, const std::string& help,
                std::function<double()> fn);
  void help(const std::string& name, const std::string& type, const std::string& text);
  // A histogram computed when scraped (phase 7): cumulative (upper bound, count) pairs in rising
  // order, without +Inf (added from `count`), and the sum.
  struct Hist {
    std::vector<std::pair<double, uint64_t>> buckets;
    uint64_t count = 0;
    double sum = 0;
  };
  void histogram(const std::string& name, const std::string& help, std::function<Hist()> fn);
  std::string render() const;

 private:
  mutable std::mutex mu_;
  std::map<std::string, std::map<std::string, std::unique_ptr<std::atomic<uint64_t>>>> counters_;
  std::map<std::string, std::pair<std::string, std::string>> help_;  // name -> (type, text)
  std::map<std::string, std::function<double()>> computed_;
  std::map<std::string, std::function<Hist()>> histograms_;
};

}  // namespace pcr
