#include <data_generator.h>
#include <differential_evolution.h>
#include <metrics_stream.h>
#include <nnmpc_controller.h>
#include <shrinkage_engine.h>

#include <filesystem>
#include <format>
#include <print>

nnmpc_controller::nnmpc_controller(const app_config &cfg,
                                   nnmpc_transformer model,
                                   const norm_params &params, data_manager &dm)
    : cfg_(cfg), model_(std::move(model)), norm_(params), dm_(dm),
      rng_(std::random_device{}()), jde_(cfg), event_pub_(event_publisher{}) {
  ft_optimizer_ = std::make_unique<torch::optim::Adam>(
      model_->parameters(),
      torch::optim::AdamOptions(cfg_.finetune_lr).weight_decay(1e-4));
  agent_pool_ = create_agent_pool(cfg_.n_agents);
  model_->eval();
}

void nnmpc_controller::set_callbacks(
    std::function<metric()> fetch, std::function<void(int)> apply,
    std::function<void(const std::string &)> log_fn) {
  fetch_cb_ = std::move(fetch);
  apply_cb_ = std::move(apply);
  log_cb_ = std::move(log_fn);
}

void nnmpc_controller::run_full_wem_cycle() {
  generate_long_term_forecast(104);
  run_step();
  update_shrinkage();
  run_gamification_update();
  run_shift_bidding();
  log("──── Ciclu WEM complet ────");
}

void nnmpc_controller::run_step() {
  if (!fetch_cb_)
    return;
  metric cur = fetch_cb_();
  history_.push_back(cur);
  if ((int)history_.size() > 200)
    history_.erase(history_.begin());

  shrinkage_engine::update_rolling(
      shrinkage_, {{.total_scheduled = cur.current_agents,
                    .absent_vacation = cur.agents_absent_planned / 2,
                    .absent_training = cur.agents_absent_planned -
                                       cur.agents_absent_planned / 2,
                    .absent_sick = cur.agents_absent_unplanned,
                    .on_break = cur.agents_on_break}});

  auto [arr_p, aht_p] = predict();
  double sh = shrinkage_.planned + shrinkage_.unplanned;

  // JDEOptimizer: island model paralel, RNG per thread
  int optimal = jde_.optimize(queue_, arr_p, aht_p, sh);

  // [FIX Δt] Actualizare coadă membră cu unități corecte
  // λ = apeluri/sec (volum interval / Δt)
  // net_rate = λ - N_eff × μ  [apeluri/sec]
  // Δq = net_rate × Δt        [apeluri pe interval]
  const double Dt = cfg_.interval_sec;
  double lam = cur.inbound_calls / Dt; // arr total / 900 s
  double mu = cur.aht_sec > 0.0 ? 1.0 / cur.aht_sec : 1.0 / 180.0;
  int eff = std::max(1, (int)(optimal * (1.0 - sh)));
  double net_rate = lam - static_cast<double>(eff) * mu;
  queue_ = std::max(0.0, queue_ + net_rate * Dt);

  if (apply_cb_)
    apply_cb_(optimal);
  log(std::format("PROD | Arr:{:.0f} | Staff:{} (eff:{}) | Q:{:.1f} | "
                  "Shrink:{:.0f}%+{:.0f}% | jDE[{}] islands:{}",
                  cur.inbound_calls, optimal, eff, queue_,
                  shrinkage_.planned * 100, shrinkage_.unplanned * 100,
                  jde_.is_parallel() ? "parallel" : "single",
                  jde_.n_islands()));

  // ── Kafka: staffing + forecast + queue ───────────────────────────
  event_pub_.on_staffing_decision(cur.inbound_calls, optimal, eff, queue_,
                                  shrinkage_.planned, shrinkage_.unplanned,
                                  jde_.is_parallel(), jde_.n_islands(), arr_p,
                                  aht_p, cfg_.interval_sec, cur.aht_sec);

  // queue_event cu μ real din AHT curent (nu estimat 1/180)
  // on_staffing_decision publică deja queue_event intern,
  // dar acesta are mu real — îl publicăm separat pe topic dedicat
  // prin publish direct (event_publisher nu expune publish_queue direct,
  // deci îl lăsăm pe cel din on_staffing_decision care e suficient)

  if (++step_ctr_ % cfg_.finetune_every == 0)
    online_fine_tune();
}

void nnmpc_controller::save_state() {
  try {
    torch::save(model_, cfg_.model_path);
    log(std::format(
        "SISTEM | Modelul actualizat a fost salvat cu succes in : {}",
        cfg_.model_path));
  } catch (const std::exception &e) {
    log(std::format("EROARE | Salvarea modelului a esuat: {}", e.what()));
  }
}

std::pair<std::vector<double>, std::vector<double>>
nnmpc_controller::predict() {
  std::vector<double> ap(cfg_.horizon), hp(cfg_.horizon);
  double la = history_.empty() ? 55.0 : history_.back().inbound_calls;
  double lh = history_.empty() ? 180.0 : history_.back().aht_sec;

  if ((int)history_.size() < 4) {
    std::fill(ap.begin(), ap.end(), la);
    std::fill(hp.begin(), hp.end(), lh);
    return {ap, hp};
  }

  int seq = std::min((int)history_.size(), cfg_.seq_len);
  auto input = torch::zeros({seq, 1, 2});
  for (int i = 0; i < seq; ++i) {
    const metric &m = history_[history_.size() - seq + i];
    input[i][0][0] = norm_.norm_arr(m.inbound_calls);
    input[i][0][1] = norm_.norm_aht(m.aht_sec);
  }

  try {
    torch::NoGradGuard ng;
    model_->eval();
    auto out = model_->forward(input); // [horizon, 1, 2]
    double ar = norm_.arr_hi - norm_.arr_lo;
    double hr = norm_.aht_hi - norm_.aht_lo;
    for (int h = 0; h < cfg_.horizon; ++h) {
      ap[h] = std::max(0.0, la + out[h][0][0].item<float>() * ar);
      hp[h] = std::max(60.0, lh + out[h][0][1].item<float>() * hr);
    }
  } catch (const c10::Error &e) {
    log(std::format("Predict error: {}", e.what()));
    std::fill(ap.begin(), ap.end(), la);
    std::fill(hp.begin(), hp.end(), lh);
  }
  return {ap, hp};
}

void nnmpc_controller::online_fine_tune() {
  int need = cfg_.seq_len + cfg_.horizon + 2;
  if ((int)history_.size() < need)
    return;
  int seq = std::min((int)history_.size() - cfg_.horizon - 1, cfg_.seq_len);
  int base = (int)history_.size() - seq - cfg_.horizon - 1;

  auto input = torch::zeros({seq, 1, 2});
  auto target = torch::zeros({cfg_.horizon, 1, 2});
  for (int i = 0; i < seq; ++i) {
    const metric &m = history_[base + i];
    input[i][0][0] = norm_.norm_arr(m.inbound_calls);
    input[i][0][1] = norm_.norm_aht(m.aht_sec);
  }
  double la = history_[base + seq - 1].inbound_calls;
  double lh = history_[base + seq - 1].aht_sec;
  double ar = norm_.arr_hi - norm_.arr_lo, hr = norm_.aht_hi - norm_.aht_lo;
  for (int h = 0; h < cfg_.horizon; ++h) {
    const metric &mn = history_[base + seq + h];
    target[h][0][0] = (float)((mn.inbound_calls - la) / ar);
    target[h][0][1] = (float)((mn.aht_sec - lh) / hr);
  }

  try {
    model_->train();
    ft_optimizer_->zero_grad();
    auto pred = model_->forward(input);
    auto loss = torch::huber_loss(pred, target, torch::Reduction::Mean, 0.5f);
    loss.backward();
    torch::nn::utils::clip_grad_norm_(model_->parameters(),
                                      cfg_.finetune_grad_clip);
    ft_optimizer_->step();
    model_->eval();
    log(std::format("Fine-tune | loss: {:.5f}", loss.item<float>()));
    event_pub_.on_fine_tune(loss.item<float>(), step_ctr_,
                            static_cast<int>(seq));
  } catch (const c10::Error &e) {
    log(std::format("Fine-tune error: {}", e.what()));
  }
}

void nnmpc_controller::update_shrinkage() {
  auto recs = dm_.load_shrinkage();
  if (recs.empty())
    return;
  shrinkage_engine::update_rolling(shrinkage_, recs);
  log(std::format("shrinkage | Planned:{:.1f}%  Unplanned:{:.1f}%",
                  shrinkage_.planned * 100, shrinkage_.unplanned * 100));
  event_pub_.on_shrinkage_update(shrinkage_.planned, shrinkage_.unplanned);
}

void nnmpc_controller::run_gamification_update() {
  auto all = dm_.load_performance();
  std::unordered_map<int, std::vector<performance_record>> by_ag;
  for (auto &r : all)
    by_ag[r.agent_id].push_back(r);

  int upd = 0;
  for (auto &ag : agent_pool_) {
    auto it = by_ag.find(ag.id);
    if (it == by_ag.end())
      continue;
    auto &recs = it->second;
    std::sort(recs.begin(), recs.end(),
              [](const performance_record &a, const performance_record &b) {
                return a.period_start < b.period_start;
              });
    ag.gamification = gamif_.compute_aggregate(recs);
    ag.adherence_score = recs.back().adherence_rate;
    ag.avg_alert_time_sec = recs.back().alert_response_s;
    event_pub_.on_agent_gamification(
        ag.id, ag.gamification.total_points, ag.gamification.badge_level,
        ag.gamification.badge_name, ag.adherence_score, ag.avg_alert_time_sec);
    ++upd;
  }
  std::sort(agent_pool_.begin(), agent_pool_.end(),
            [](const agent &a, const agent &b) {
              return a.gamification.total_points > b.gamification.total_points;
            });

  std::string top = "Top 5: ";
  for (int i = 0; i < std::min(5, (int)agent_pool_.size()); ++i) {
    const auto &a = agent_pool_[i];
    top += std::format("[#{} {} {:.0f}pts] ", a.id, a.gamification.badge_name,
                       a.gamification.total_points);
  }
  log(top);

  int n = (int)agent_pool_.size();
  std::string bot = "Coaching: ";
  for (int i = n - 1; i >= std::max(0, n - 3); --i)
    bot += std::format("[#{} {:.0f}pts] ", agent_pool_[i].id,
                       agent_pool_[i].gamification.total_points);
  log(bot);
  log(std::format("Gamification: {} agenți actualizați", upd));
}

void nnmpc_controller::run_shift_bidding() {
  const int slots = 8;
  std::vector<agent *> cands;
  for (auto &a : agent_pool_)
    if (a.prefers_overtime)
      cands.push_back(&a);
  std::stable_sort(
      cands.begin(), cands.end(), [](const agent *a, const agent *b) {
        if (a->gamification.total_points != b->gamification.total_points)
          return a->gamification.total_points > b->gamification.total_points;
        return a->seniority_weeks > b->seniority_weeks;
      });

  int assigned = 0;
  for (auto *a : cands) {
    if (assigned >= slots)
      break;
    log(std::format("OT slot {}: #{} ({}, {:.0f}pts, {}w)", ++assigned, a->id,
                    a->gamification.badge_name, a->gamification.total_points,
                    a->seniority_weeks));

    event_pub_.on_shift_bidding(
        a->id, assigned, /*voluntary=*/true, a->gamification.badge_name,
        a->gamification.total_points, a->seniority_weeks);
  }
  if (assigned < slots) {
    for (auto &a : agent_pool_) {
      if (assigned >= slots)
        break;
      if (a.prefers_overtime || a.adherence_score < 0.80)
        continue;
      log(std::format("OT slot {} (obligatoriu): #{} (adh {:.0f}%)", ++assigned,
                      a.id, a.adherence_score * 100));

      event_pub_.on_shift_bidding(
          a.id, assigned, /*voluntary=*/false, a.gamification.badge_name,
          a.gamification.total_points, a.seniority_weeks);
    }
  }
  log(std::format("Shift bidding: {}/{} slots", assigned, slots));
}

void nnmpc_controller::generate_long_term_forecast(int weeks_ahead) {
  if (history_.empty())
    return;
  double avg = 0.0;
  for (const auto &m : history_)
    avg += m.inbound_calls;
  avg /= history_.size();
  double proj = avg, g = 1.0 + 0.08 / 52.0;
  for (int w = 1; w <= std::min(4, weeks_ahead); ++w) {
    proj *= g;
    log(std::format("LT Forecast W+{}: {:.0f} apeluri/interval", w, proj));
  }
}

std::vector<agent> nnmpc_controller::create_agent_pool(int n) {
  std::vector<agent> pool;
  pool.reserve(n);
  std::uniform_int_distribution<int> sen(0, 260);
  std::bernoulli_distribution ot(0.35);
  for (int i = 0; i < n; ++i) {
    agent a;
    a.id = i;
    a.prefers_overtime = ot(rng_);
    a.seniority_weeks = sen(rng_);
    a.adherence_score = 0.80 + 0.20 * ((i * 7 + 3) % 11) / 10.0;
    a.avg_alert_time_sec = 20.0 + 20.0 * (i % 5) / 4.0;
    a.gamification.agent_id = i;
    pool.push_back(a);
  }
  return pool;
}

void nnmpc_controller::log(const std::string &msg) {
  if (log_cb_) {
    log_cb_(msg);
    return;
  }
  auto now = system_clock::to_time_t(system_clock::now());
  std::tm tm{};
#ifdef _WIN32
  localtime_s(&tm, &now);
#else
  localtime_r(&now, &tm);
#endif
  std::print("[{:02d}:{:02d}:{:02d}] {}\n", tm.tm_hour, tm.tm_min, tm.tm_sec,
             msg);
}

/// nnmpc_controller_multi

nnmpc_controller_multi::nnmpc_controller_multi(const app_config &cfg,
                                               const norm_params &norm,
                                               data_generator_multi &gen,
                                               rta_manager &rta)
    : cfg_(cfg), norm_(norm), rta_(rta), jde_(cfg) {
  // Inițializăm fiecare coadă cu modelul său pre‑antrenat
  for (const auto &site : cfg_.sites) {
    for (const auto &q : site.site_queues) {
      std::string key = site.site_id + "/" + q;

      std::string csv_path = cfg_.metrics_csv_for(site.site_id, q);

      if (!std::filesystem::exists(csv_path)) {
        gen.generate_queue_csv(site.site_id, q, norm);
      }

      std::string model_path = cfg_.model_path_for(site.site_id, q);
      if (!std::filesystem::exists(model_path)) {
        throw std::runtime_error("Model " + model_path +
                                 " lipsă! Rulați pre‑training mai întâi.");
      }

      nnmpc_transformer model = build_model();
      torch::load(model, model_path);
      model->eval();

      auto stream = std::make_unique<metrics_stream>(csv_path, site.site_id, q);
      /*
      queue_state qs;
      qs.model = std::move(model);
      qs.stream = std::move(stream);
      qs.shrink.planned = cfg_.global_shrinkage;
      qs.shrink.unplanned = 0.10;
      qs.queue_len = 5.0;
      qs.optimizer = de_optimizer_handle(cfg_);
      queue_states_[key] = std::move(qs);
      */

      auto [it, ok] = queue_states_.try_emplace(
          key,
          std::move(model), // nnmpc_transformer&&
          std::make_unique<metrics_stream>(csv_path, site.site_id,
                                           q), //
          cfg_                                 // const app_config&
      );
      if (!ok)
        throw std::runtime_error("Cheie duplicat: " + key);
    }
  }

  agent_pool_ =
      create_agent_pool(cfg_.n_agents_per_site * (int)cfg_.sites.size());
  gamif_ = gamification_engine(kpi_thresholds{});

  ft_optimizer_ = std::make_unique<torch::optim::Adam>(
      queue_states_.begin()->second.model->parameters(),
      torch::optim::AdamOptions(cfg_.finetune_lr).weight_decay(1e-4));
}

nnmpc_transformer nnmpc_controller_multi::build_model() {
  return nnmpc_transformer{cfg_.n_input_features, cfg_.d_model, cfg_.nhead,
                           cfg_.num_layers, cfg_.horizon};
}

void nnmpc_controller_multi::set_callbacks(
    std::function<metric()> fetch, std::function<void(int)> apply,
    std::function<void(const std::string &)> log_fn) {
  fetch_cb_ = std::move(fetch);
  apply_cb_ = std::move(apply);
  log_cb_ = std::move(log_fn);
}

void nnmpc_controller_multi::run_step() {
  for (auto &[qkey, qs] : queue_states_) {
    metric m = qs.stream->next();
    qs.history.push_back(m);
    if (qs.history.size() > 200)
      qs.history.erase(qs.history.begin());

    shrinkage_engine::update_rolling(qs.shrink, m);

    auto [arr_p, aht_p] = predict(qs);
    qs.last_arr = arr_p;
    qs.last_aht = aht_p;

    double sla_target = 20.0;
    for (const auto &s : cfg_.sites)
      if (s.site_id == m.site_id) {
        sla_target = s.sla_target_sec;
        break;
      }
    queue_param qp = cfg_.get_queue_params(m.queue_id);
    double shrinkage_total = qs.shrink.planned + qs.shrink.unplanned;

    int staff =
        qs.optimizer.optimize(qs.queue_len, arr_p, aht_p, shrinkage_total,
                              sla_target, qp, cfg_.min_staff, cfg_.max_staff);
    qs.optimal_staff = staff;

    const double Dt = cfg_.interval_sec;
    double lam = m.inbound_calls / Dt;
    double mu = m.aht_sec > 0 ? 1.0 / m.aht_sec : 1.0 / qp.fallback_aht_sec;
    int eff = std::max(1, static_cast<int>(staff * (1.0 - shrinkage_total)));
    ErlangAResult ea = compute_erlang_a(eff, lam, mu, qp);
    double net = lam - eff * mu - ea.abandon_rate * lam;
    qs.queue_len = std::max(0.0, qs.queue_len + net * Dt);

    if (apply_cb_)
      apply_cb_(staff);
    log(std::format("[{}/{}] optimal={} (eff={}) q={:.1f}", m.site_id,
                    m.queue_id, staff, eff, qs.queue_len));

    if (++qs.step_count % cfg_.finetune_every == 0) {
      online_fine_tune(qs);
      if (++qs.fine_tune_count % cfg_.model_save_every == 0)
        save_model_for(qkey);
    }
  }
}

std::vector<double> nnmpc_controller_multi::get_current_queue_lengths() const {
  std::vector<double> lengths;
  for (const auto &[key, qs] : queue_states_)
    lengths.push_back(qs.queue_len);
  return lengths;
}

std::vector<double> nnmpc_controller_multi::get_forecast_arrivals() const {
  std::vector<double> arr;
  for (const auto &[key, qs] : queue_states_)
    arr.push_back(qs.last_arr.empty() ? 50.0 : qs.last_arr[0]);
  return arr;
}

std::vector<double> nnmpc_controller_multi::get_forecast_aht() const {
  std::vector<double> aht;
  for (const auto &[key, qs] : queue_states_)
    aht.push_back(qs.last_aht.empty() ? 180.0 : qs.last_aht[0]);
  return aht;
}

std::vector<std::string> nnmpc_controller_multi::get_queue_keys() const {
  std::vector<std::string> keys;
  for (const auto &[key, _] : queue_states_)
    keys.push_back(key);
  return keys;
}

void nnmpc_controller_multi::apply_mpc_allocation(
    const std::vector<double> &allocation) {
  size_t i = 0;
  for (auto &[key, qs] : queue_states_) {
    if (i >= allocation.size())
      break;
    qs.effective_staff = static_cast<int>(std::round(allocation[i]));
    ++i;
  }
}

void nnmpc_controller_multi::save_all_models() {
  for (auto &[key, qs] : queue_states_) {
    auto pos = key.find('/');
    std::string path =
        cfg_.model_path_for(key.substr(0, pos), key.substr(pos + 1));
    std::filesystem::create_directories(cfg_.model_dir);
    torch::save(qs.model, path);
  }
}

void nnmpc_controller_multi::save_model_for(const std::string &key) {
  auto it = queue_states_.find(key);
  if (it != queue_states_.end()) {
    auto pos = key.find('/');
    std::string path =
        cfg_.model_path_for(key.substr(0, pos), key.substr(pos + 1));
    torch::save(it->second.model, path);
  }
}

std::pair<std::vector<double>, std::vector<double>>
nnmpc_controller_multi::predict(queue_state &qs) {
  int H = cfg_.horizon;
  std::vector<double> ap(H, 50.0), hp(H, 180.0);
  if (qs.history.size() < 4)
    return {ap, hp};
  size_t seq = std::min(qs.history.size(), (size_t)cfg_.seq_len);
  auto input = torch::zeros({(long)seq, 1, cfg_.n_input_features});
  for (size_t i = 0; i < seq; ++i) {
    const auto &m = qs.history[qs.history.size() - seq + i];
    input[i][0][0] = norm_.norm_arr(m.inbound_calls);
    input[i][0][1] = norm_.norm_aht(m.aht_sec);
  }
  torch::NoGradGuard ng;
  qs.model->eval();
  auto out = qs.model->forward(input);
  double la = qs.history.back().inbound_calls;
  double lh = qs.history.back().aht_sec;
  double ar = norm_.arr_hi - norm_.arr_lo;
  double hr = norm_.aht_hi - norm_.aht_lo;
  for (int h = 0; h < H; ++h) {
    ap[h] = std::max(0.0, la + out[h][0][0].item<float>() * ar);
    hp[h] = std::max(60.0, lh + out[h][0][1].item<float>() * hr);
  }
  return {ap, hp};
}

void nnmpc_controller_multi::online_fine_tune(queue_state &qs) {
  if (qs.history.size() < static_cast<size_t>(cfg_.seq_len + cfg_.horizon))
    return;
  size_t base = qs.history.size() - cfg_.seq_len - cfg_.horizon;
  auto input = torch::zeros({cfg_.seq_len, 1, cfg_.n_input_features});
  auto target = torch::zeros({cfg_.horizon, 1, 2});
  double la = qs.history[base + cfg_.seq_len - 1].inbound_calls;
  double lh = qs.history[base + cfg_.seq_len - 1].aht_sec;
  double ar = norm_.arr_hi - norm_.arr_lo;
  double hr = norm_.aht_hi - norm_.aht_lo;
  for (int i = 0; i < cfg_.seq_len; ++i) {
    const metric &m = qs.history[base + i];
    input[i][0][0] = norm_.norm_arr(m.inbound_calls);
    input[i][0][1] = norm_.norm_aht(m.aht_sec);
  }
  for (int h = 0; h < cfg_.horizon; ++h) {
    const metric &mn = qs.history[base + cfg_.seq_len + h];
    target[h][0][0] = static_cast<float>((mn.inbound_calls - la) / ar);
    target[h][0][1] = static_cast<float>((mn.aht_sec - lh) / hr);
  }
  torch::optim::Adam opt(
      qs.model->parameters(),
      torch::optim::AdamOptions(cfg_.finetune_lr).weight_decay(1e-4f));
  qs.model->train();
  opt.zero_grad();
  auto pred = qs.model->forward(input);
  auto loss = torch::huber_loss(pred, target, torch::Reduction::Mean, 0.5f);
  loss.backward();
  torch::nn::utils::clip_grad_norm_(qs.model->parameters(),
                                    cfg_.finetune_grad_clip);
  opt.step();
  qs.model->eval();
}

void nnmpc_controller_multi::update_shrinkage() {}

void nnmpc_controller_multi::run_gamification_update() {
  std::vector<performance_record> all_perf;
  // În mod normal acestea ar fi citite din fișierul de performanță
  // Aici generăm câteva înregistrări fictive pentru demo
  for (auto &agent : agent_pool_) {
    performance_record rec;
    rec.agent_id = agent.id;
    rec.fcr_rate = 0.8;
    rec.csat_score = 4.0;
    rec.adherence_rate = 0.9;
    rec.avg_handle_time_s = 180.0;
    rec.alert_response_s = 25.0;
    rec.calls_handled = 120;
    rec.quality_score = 0.85;
    rec.period_start = system_clock::now();
    agent.perf_history.push_back(rec);
    agent.gamification = gamif_.compute_aggregate(agent.perf_history);
  }
  std::sort(agent_pool_.begin(), agent_pool_.end(),
            [](const agent &a, const agent &b) {
              return a.gamification.total_points > b.gamification.total_points;
            });
  log("Gamification updated.");
}

void nnmpc_controller_multi::run_shift_bidding() {
  const int slots = 8;
  int assigned = 0;
  for (auto &a : agent_pool_) {
    if (a.prefers_overtime) {
      log(std::format("OT slot {}: agent #{}", ++assigned, a.id));
      if (assigned >= slots)
        break;
    }
  }
  if (assigned < slots) {
    for (auto &a : agent_pool_) {
      if (!a.prefers_overtime && a.adherence_score > 0.8) {
        log(std::format("OT slot {} (obligatoriu): agent #{}", ++assigned,
                        a.id));
        if (assigned >= slots)
          break;
      }
    }
  }
}

void nnmpc_controller_multi::generate_long_term_forecast(int weeks_ahead) {
  double avg = 0.0;
  for (const auto &[key, qs] : queue_states_) {
    for (const auto &m : qs.history)
      avg += m.inbound_calls;
  }
  if (!queue_states_.empty())
    avg /= (queue_states_.size() *
            std::max(1, (int)queue_states_.begin()->second.history.size()));
  double proj = avg;
  double g = 1.0 + 0.08 / 52.0;
  for (int w = 1; w <= std::min(4, weeks_ahead); ++w) {
    proj *= g;
    log(std::format("LT Forecast W+{}: {:.0f} apeluri/interval", w, proj));
  }
}

std::vector<agent> nnmpc_controller_multi::create_agent_pool(int n) {

  std::vector<agent> pool;
  pool.reserve(n);
  std::uniform_int_distribution<int> sen(0, 260);
  std::bernoulli_distribution ot(0.35);
  for (int i = 0; i < n; ++i) {
    agent a;
    a.id = i;
    a.prefers_overtime = ot(rng_);
    a.seniority_weeks = sen(rng_);
    a.adherence_score = 0.80 + 0.20 * ((i * 7 + 3) % 11) / 10.0;
    a.avg_alert_time_sec = 20.0 + 20.0 * (i % 5) / 4.0;
    pool.push_back(a);
  }
  return pool;
}
