#include "src/ops/metrics.h"

#include <cstdio>

namespace pcr {

void Metrics::inc(const std::string& name, const std::string& labels, uint64_t by) {
  std::lock_guard lk(mu_);
  auto& slot = counters_[name][labels];
  if (!slot) slot = std::make_unique<std::atomic<uint64_t>>(0);
  slot->fetch_add(by, std::memory_order_relaxed);
}

void Metrics::computed(const std::string& name, const std::string& type, const std::string& text,
                       std::function<double()> fn) {
  std::lock_guard lk(mu_);
  help_[name] = {type, text};
  computed_[name] = std::move(fn);
}

void Metrics::histogram(const std::string& name, const std::string& text, std::function<Hist()> fn) {
  std::lock_guard lk(mu_);
  help_[name] = {"histogram", text};
  histograms_[name] = std::move(fn);
}

void Metrics::help(const std::string& name, const std::string& type, const std::string& text) {
  std::lock_guard lk(mu_);
  help_[name] = {type, text};
}

std::string Metrics::render() const {
  std::lock_guard lk(mu_);
  std::string out;
  auto head = [&](const std::string& name, const char* dflt) {
    auto h = help_.find(name);
    if (h != help_.end()) {
      out += "# HELP " + name + " " + h->second.second + "\n# TYPE " + name + " " + h->second.first + "\n";
    } else {
      out += std::string("# TYPE ") + name + " " + dflt + "\n";
    }
  };
  for (const auto& [name, series] : counters_) {
    head(name, "counter");
    for (const auto& [labels, v] : series) {
      out += name + (labels.empty() ? "" : "{" + labels + "}") + " " +
             std::to_string(v->load(std::memory_order_relaxed)) + "\n";
    }
  }
  for (const auto& [name, fn] : computed_) {
    head(name, "gauge");
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.17g", fn());
    out += name + " " + buf + "\n";
  }
  for (const auto& [name, fn] : histograms_) {
    head(name, "histogram");
    const Hist h = fn();
    char buf[96];
    for (const auto& [le, n] : h.buckets) {
      std::snprintf(buf, sizeof buf, "{le=\"%.9g\"} %llu\n", le, static_cast<unsigned long long>(n));
      out += name + "_bucket" + buf;
    }
    std::snprintf(buf, sizeof buf, "{le=\"+Inf\"} %llu\n", static_cast<unsigned long long>(h.count));
    out += name + "_bucket" + buf;
    std::snprintf(buf, sizeof buf, " %.17g\n", h.sum);
    out += name + "_sum" + buf;
    out += name + "_count " + std::to_string(h.count) + "\n";
  }
  return out;
}

}  // namespace pcr
