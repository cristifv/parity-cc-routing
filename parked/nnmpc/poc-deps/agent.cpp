#include <agent.h>
#include <mutex>
#include <shared_mutex>

extern std::atomic<bool> g_shutdown;

rta_manager::rta_manager(const app_config &cfg) : cfg_(cfg) {
  int next_id = 0;
  for (const auto &site : cfg_.sites) {
    for (int i = 0; i < cfg_.n_agents_per_site; ++i) {
      agent_rta a;
      a.id = next_id++;
      a.site_id = site.site_id;
      a.state = "Available";
      a.prev_state = "Available";
      a.last_change = system_clock::now();
      agents_.push_back(a);
    }
  }
}

int rta_manager::count_available() const {
  std::shared_lock lock(mtx_);
  int cnt = 0;
  for (const auto &a : agents_)
    if (a.state == "Available")
      cnt++;
  return cnt;
}

void rta_manager::start_simulation() {
  sim_thread_ = std::thread([this]() {
    std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<double> ur(0.0, 1.0);
    while (!g_shutdown.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(2s);
      std::unique_lock lock(mtx_);
      for (auto &a : agents_) {
        if (ur(rng) < 0.01) {
          if (a.state == "Available") {
            double r = ur(rng);
            if (r < 0.6)
              a.state = "Busy";
            else if (r < 0.9)
              a.state = "Break";
            else
              a.state = "Absent";
          } else if (a.state == "Busy") {
            a.state = "Available";
          } else if (a.state == "Break" || a.state == "Absent") {
            a.state = "Available";
          }
          a.last_change = system_clock::now();
        }
      }
    }
  });
}

rta_manager::~rta_manager() {
  if (sim_thread_.joinable())
    sim_thread_.join();
}
