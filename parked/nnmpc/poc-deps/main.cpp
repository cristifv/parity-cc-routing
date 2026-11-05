#include <app_config.h>
#include <atomic>
#include <csignal>
#include <data_generator.h>
#include <data_manager.h>
#include <event_producer.h>
#include <filesystem>
#include <markov_allocator.h>
#include <nnmpc_controller.h>
#include <norm_params.h>
#include <pretrain.h>
#include <print>

#define SLEEP_INTERVAL_SEC 5

std::atomic<bool> g_shutdown{false};

static std::atomic<bool> g_running{true};

void signal_handler(int signum) {
  std::print("\n[SISTEM] Semnal de oprire ({}). Se asteapta finalizarea "
             "ciclului curent...\n",
             signum);
  g_running = false;
  g_shutdown.store(true, std::memory_order_relaxed);
}

int run_multi() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  app_config cfg;
  cfg.use_multi_site = true;
  cfg.use_markov_mpc = true;

  std::filesystem::create_directories(cfg.data_dir);
  std::filesystem::create_directories(cfg.model_dir);

  // 1. Normalizare (globală)
  norm_params norm;
  if (!norm.load(cfg.norm_path)) {
    norm.arr_lo = 0.0f;
    norm.arr_hi = 200.0f;
    norm.aht_lo = 60.0f;
    norm.aht_hi = 500.0f;
    norm.save(cfg.norm_path);
  }

  // 2. Generare date + pre‑training per coadă
  data_generator_multi gen(cfg);
  pretrain_multi pt(cfg, norm);
  //

  for (const auto &site : cfg.sites) {
    for (const auto &queue : site.site_queues) {
      std::string csv_path = cfg.metrics_csv_for(site.site_id, queue);
      if (!std::filesystem::exists(csv_path))
        gen.generate_queue_csv(site.site_id, queue, norm);
      std::string model_path = cfg.model_path_for(site.site_id, queue);
      if (!std::filesystem::exists(model_path)) {
        std::print("[Main] Pre‑training pentru {}/{}...\n", site.site_id,
                   queue);
        auto model = pt.run(csv_path);
        torch::save(model, model_path);
      }
    }
  }

  rta_manager rta(cfg);
  rta.start_simulation();

  // 3. Pornire controller
  nnmpc_controller_multi controller(cfg, norm, gen, rta);
  markov_mpc_allocator mpc_alloc(cfg);

  std::print(
      "╔══════════════════════════════════════════════════════════════╗\n");
  std::print(
      "║ NNMPC v27 – Multi‑site/queue Enterprise                    ║\n");
  std::print(
      "╚══════════════════════════════════════════════════════════════╝\n\n");

  while (!g_shutdown.load()) {
    controller.run_full_wem_cycle();

    if (cfg.use_markov_mpc) {
      auto queues = controller.get_current_queue_lengths();
      auto arr_f = controller.get_forecast_arrivals();
      auto aht_f = controller.get_forecast_aht();
      auto keys = controller.get_queue_keys();

      int available_agents = rta.count_available();
      auto allocation =
          mpc_alloc.allocate(queues, arr_f, aht_f, keys, available_agents);
      controller.apply_mpc_allocation(allocation);

      std::print("[MPC] Alocare ({} agenți disponibili): ", available_agents);
      for (double v : allocation)
        std::print("{:.1f} ", v);
      std::print("\n");
    }

    std::this_thread::sleep_for(15min);
  }

  controller.save_all_models();
  std::print("[Main] Oprire curată.\n");
  return 0;
}

int main() {
  app_config cfg;

  if (cfg.use_multi_site) {
    return run_multi();
  }

  std::print("╔══════════════════════════════════════════════════════╗\n"
             "║   NNMPC v5  –  C++ ONLY  │  jDE Island Model       ║\n"
             "║   Δt corect + threads auto │ Generate→Train→Run     ║\n"
             "╚══════════════════════════════════════════════════════╝\n\n");

  // ── Selector optimizer ─────────────────────────────────────────────────
  //   cfg.use_parallel_jde = true;   // producție (default)
  //   cfg.use_parallel_jde = false;  // debugging single-thread
  //   cfg.debug_seed = 42ULL;        // seed fix pentru reproducibilitate
  // ──────────────────────────────────────────────────────────────────────
  // Exemplu: decomentează linia de mai jos pentru debugging
  cfg.use_parallel_jde = false;

  std::print("Optimizer: {}\n\n",
             cfg.use_parallel_jde
                 ? std::format("ParallelJDE (island model, ≤8 threads)")
                 : std::format("JDE single-thread (seed={})", cfg.debug_seed));
  std::filesystem::create_directories(cfg.data_dir);
  std::filesystem::create_directories(cfg.model_dir);

  // ── Faza 1: Generare date ─────────────────────────────────────────
  norm_params norm;
  bool data_ok = std::filesystem::exists(cfg.metrics_csv) &&
                 std::filesystem::exists(cfg.perf_csv) &&
                 std::filesystem::exists(cfg.shrink_csv);

  if (!data_ok) {
    std::print("── Faza 1: Generare date ({} zile) ──\n", cfg.gen_days);
    data_generator gen(cfg);
    norm = gen.generate_all();
    norm.save(cfg.norm_path);
    std::print("── Faza 1 completă ──\n\n");
  } else {
    std::print("── Date existente ──\n");
    if (!norm.load(cfg.norm_path)) {
      // Recalculăm NormParams din CSV existent
      std::vector<float> arrs, ahts;
      std::ifstream f(cfg.metrics_csv);
      std::string line;
      std::getline(f, line);
      while (std::getline(f, line)) {
        std::istringstream ss(line);
        std::string tok;
        try {
          std::getline(ss, tok, ',');
          std::getline(ss, tok, ',');
          arrs.push_back(std::stof(tok));
          std::getline(ss, tok, ',');
          ahts.push_back(std::stof(tok));
        } catch (...) {
        }
      }
      if (!arrs.empty()) {
        std::sort(arrs.begin(), arrs.end());
        std::sort(ahts.begin(), ahts.end());
        int n = (int)arrs.size();
        norm.arr_lo = arrs[n / 100];
        norm.arr_hi = arrs[99 * n / 100];
        norm.aht_lo = ahts[n / 100];
        norm.aht_hi = ahts[99 * n / 100];
        norm.save(cfg.norm_path);
      }
    }
    std::print("   NormParams: arr=[{:.1f},{:.1f}]  aht=[{:.1f},{:.1f}]\n\n",
               norm.arr_lo, norm.arr_hi, norm.aht_lo, norm.aht_hi);
  }

  // ── Faza 2: Pre-training ──────────────────────────────────────────
  nnmpc_transformer model(nullptr);
  if (!std::filesystem::exists(cfg.model_path)) {
    std::print("── Faza 2: Pre-training Transformer ──\n");
    pretrain pt(cfg, norm);
    model = pt.run();
    std::print("── Faza 2 completă ──\n\n");
  } else {
    std::print("── Model existent: {} ──\n", cfg.model_path);
    model = nnmpc_transformer(cfg.n_input_features, cfg.d_model, cfg.nhead,
                              cfg.num_layers, cfg.horizon);
    torch::load(model, cfg.model_path);
    model->eval();
    std::print("── Model încărcat ──\n\n");
  }

  // ── Faza 3: Loop producție ────────────────────────────────────────
  std::print("── Faza 3: Loop producție (interval 15 min) ──\n\n");

  data_manager dm(cfg);
  nnmpc_controller controller(cfg, std::move(model), norm, dm);

  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  int sim_t = 0;
  controller.set_callbacks(
      [&]() -> metric {
        ++sim_t;
        double hour = std::fmod(sim_t * 0.25, 24.0);
        int dow = (sim_t / 96) % 7;
        constexpr double dow_w[] = {1.15, 1.05, 1.00, 1.00, 0.95, 0.60, 0.35};
        auto gauss = [](double x, double mu, double sig) {
          return std::exp(-0.5 * ((x - mu) / sig) * ((x - mu) / sig));
        };
        double base = (40.0 + 35.0 * gauss(hour, 10.5, 1.8) +
                       25.0 * gauss(hour, 14.5, 2.0)) *
                      dow_w[dow];
        metric m;
        m.inbound_calls = std::max(0.0, base + 5.0 * std::sin(sim_t * 0.3));
        m.aht_sec = std::max(60.0, 185.0 + 15.0 * std::cos(sim_t * 0.1));
        m.current_agents = 30;
        m.agents_absent_planned = 4;
        m.agents_absent_unplanned = 2;
        m.agents_on_break = 3;
        m.hour = static_cast<int>(hour);
        m.dayofweek = dow;
        m.month = 1;
        m.timestamp_epoch =
            static_cast<double>(system_clock::to_time_t(system_clock::now()));
        return m;
      },
      [](int staff) { std::print(">>> WFM APPLY: {} agenți\n", staff); },
      [](const std::string &msg) {
        auto now = system_clock::to_time_t(system_clock::now());
        std::tm tm{};
#ifdef _WIN32
        localtime_s(&tm, &now);
#else
        localtime_r(&now, &tm);
#endif
        std::print("[{:02d}:{:02d}:{:02d}] {}\n", tm.tm_hour, tm.tm_min,
                   tm.tm_sec, msg);
      });

  while (g_running) {
    controller.run_full_wem_cycle();
    // std::this_thread::sleep_for(15min);
    //
    for (int i = 0; i < SLEEP_INTERVAL_SEC && g_running; ++i)
      std::this_thread::sleep_for(1s);
  }

  std::print("Salvare stare...");

  controller.save_state();

  return 0;
}
