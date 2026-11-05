#pragma once

#include <memory>
#include <unordered_map>

#include <agent.h>
#include <app_config.h>
#include <data_generator.h>
#include <data_manager.h>
#include <event_producer.h>
#include <functional>
#include <gamification_engine.h>
#include <jde_optimizer.h>
#include <metric.h>
#include <metrics_stream.h>
#include <norm_params.h>
#include <shrinkage.h>
#include <transformer_predictor.h>

struct nnmpc_controller {
  nnmpc_controller(const app_config &cfg, nnmpc_transformer model,
                   const norm_params &params, data_manager &dm);

  void set_callbacks(std::function<metric()> fetch,
                     std::function<void(int)> apply,
                     std::function<void(const std::string &)> log_fn);

  void run_full_wem_cycle();

  void run_step();

  void save_state();

private:
  std::pair<std::vector<double>, std::vector<double>> predict();
  void online_fine_tune();
  void update_shrinkage();
  void run_gamification_update();
  void run_shift_bidding();
  void generate_long_term_forecast(int weeks_ahead);
  std::vector<agent> create_agent_pool(int n);
  void log(const std::string &msg);

  const app_config &cfg_;
  nnmpc_transformer model_;
  norm_params norm_;
  data_manager &dm_;
  shrinkage shrinkage_;
  std::vector<agent> agent_pool_;
  std::vector<metric> history_;
  gamification_engine gamif_;
  std::unique_ptr<torch::optim::Adam> ft_optimizer_;
  de_optimizer_handle jde_; // JDE sau ParallelJDE după flag
  double queue_ = 5.0;
  int step_ctr_ = 0;
  std::mt19937 rng_;
  std::function<metric()> fetch_cb_;
  std::function<void(int)> apply_cb_;
  std::function<void(const std::string &)> log_cb_;
  event_publisher event_pub_;
};

// controller for multi-site/queue

struct nnmpc_controller_multi {
  nnmpc_controller_multi(const app_config &cfg, const norm_params &norm,
                         data_generator_multi &, rta_manager &);

  void run_step();

  void save_all_models();

  void set_callbacks(std::function<metric()> fetch,
                     std::function<void(int)> apply,
                     std::function<void(const std::string &)> log_fn);

  void run_full_wem_cycle() {
    run_step();
    update_shrinkage();
    run_gamification_update();
    run_shift_bidding();
    generate_long_term_forecast(104);
  }

  std::vector<double> get_current_queue_lengths() const;
  std::vector<double> get_forecast_arrivals() const;
  std::vector<double> get_forecast_aht() const;
  std::vector<std::string> get_queue_keys() const;
  void apply_mpc_allocation(const std::vector<double> &allocation);

private:
  struct queue_state {
    nnmpc_transformer model;
    std::unique_ptr<metrics_stream> stream; // <-- capital M, capital S
    std::vector<metric> history;
    shrinkage shrink;
    double queue_len = 5.0;
    int step_count = 0;
    int fine_tune_count = 0;
    de_optimizer_handle optimizer;
    int optimal_staff = 0;
    int effective_staff = 0;
    std::vector<double> last_arr, last_aht;

    // Constructor – primește modelul, stream‑ul și configul
    queue_state(nnmpc_transformer &&m, std::unique_ptr<metrics_stream> s,
                const app_config &cfg)
        : model(std::move(m)), stream(std::move(s)),
          optimizer(cfg) // <-- folosește constructorul explicit al
                         // de_optimizer_handle
    {}

    // Move constructor implicit – asigură-te că poate fi generat
    // (nu e necesar, dar dacă ai erori poți forța cu = default)
    queue_state(queue_state &&) = default;
    // Nu permitem copierea
    queue_state(const queue_state &) = delete;
  };
  /*
    struct queue_state {
      nnmpc_transformer model;
      std::unique_ptr<metrics_stream> stream;
      std::vector<metric> history;
      shrinkage shrink;
      double queue_len = 5.0;
      int step_count = 0;
      int fine_tune_count = 0;
      de_optimizer_handle optimizer;
      int optimal_staff = 0;
      int effective_staff = 0;
      std::vector<double> last_arr, last_aht;

      queue_state(nnmpc_transformer &&model_, std::unique_ptr<metrics_stream> s,
                  const app_config &cfg)
          : model(std::move(model_)), stream(std::move(s)), optimizer{cfg} {}
    };
  */
  app_config cfg_;
  norm_params norm_;
  rta_manager &rta_;
  std::unordered_map<std::string, queue_state> queue_states_;
  std::vector<agent> agent_pool_;
  gamification_engine gamif_;
  std::unique_ptr<torch::optim::Adam> ft_optimizer_;
  de_optimizer_handle jde_;
  std::mt19937 rng_{std::random_device{}()};
  std::function<metric()> fetch_cb_;
  std::function<void(int)> apply_cb_;
  std::function<void(const std::string &)> log_cb_;
  event_publisher event_pub_;

  nnmpc_transformer build_model();

  void save_model_for(const std::string &key);

  std::pair<std::vector<double>, std::vector<double>> predict(queue_state &qs);

  void online_fine_tune(queue_state &qs);

  void update_shrinkage();

  void run_gamification_update();

  void run_shift_bidding();

  void generate_long_term_forecast(int weeks_ahead);

  std::vector<agent> create_agent_pool(int n);

  void log(const std::string &msg) {
    if (log_cb_) {
      log_cb_(msg);
      return;
    }
    std::print("[{}] {}\n", system_clock::now(), msg);
  }
};
