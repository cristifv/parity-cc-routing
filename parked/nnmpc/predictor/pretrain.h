#pragma once

#include <norm_params.h>
#include <app_config.h>
#include <transformer_predictor.h>


class pretrain {
public:
    pretrain(const app_config& cfg, const norm_params& norm)
        : cfg_(cfg), norm_(norm) {}

    nnmpc_transformer run() {
        std::print("\n[pretrain] Încarc secvențe din {}...\n", cfg_.metrics_csv);
        auto [X, y] = load_sequences();

        if (X.empty()) {
            std::print(stderr, "[pretrain] Nicio secvență! Verif. CSV.\n");
            return build_model();
        }
        std::print("[pretrain] {} secvențe  (seq={}, h={})\n",
                   X.size(), cfg_.seq_len, cfg_.horizon);

        int N     = (int)X.size();
        int n_val = std::max(1, (int)(N * 0.15));
        int n_tr  = N - n_val;

        auto Xtr = stack(X, 0, n_tr);     // [n_tr, seq, 2]
        auto ytr = stack(y, 0, n_tr);
        auto Xvl = stack(X, n_tr, N);
        auto yvl = stack(y, n_tr, N);
        std::print("[pretrain] Train:{} Val:{}\n", n_tr, n_val);

        auto model = build_model();
        model->train();

        torch::optim::AdamW opt(
            model->parameters(),
            torch::optim::AdamWOptions(cfg_.pretrain_lr)
                .weight_decay(cfg_.pretrain_wd));

        float best_val = std::numeric_limits<float>::max();
        int   patience = 0;
        std::string ckpt = cfg_.model_dir + "/pretrain_best.pt";
        std::filesystem::create_directories(cfg_.model_dir);

        std::ofstream log_f(cfg_.model_dir + "/training_log.csv");
        log_f << "epoch,train_loss,val_loss,lr\n";

        for (int ep = 1; ep <= cfg_.pretrain_epochs; ++ep) {
            // Cosine annealing LR
            float lr = cfg_.pretrain_lr_min
                + 0.5f * (cfg_.pretrain_lr - cfg_.pretrain_lr_min)
                * (1.0f + std::cos((float)(ep-1) / cfg_.pretrain_epochs * M_PIf32));
            for (auto& pg : opt.param_groups()) pg.options().set_lr(lr);

            float tr = train_epoch(model, opt, Xtr, ytr);
            float vl = eval_epoch(model, Xvl, yvl);

            log_f << std::format("{},{:.6f},{:.6f},{:.7f}\n", ep, tr, vl, lr);
            std::print("  Ep {:3d}/{} | train={:.5f}  val={:.5f}  lr={:.2e}\n",
                       ep, cfg_.pretrain_epochs, tr, vl, lr);

            if (vl < best_val) {
                best_val = vl; patience = 0;
                torch::save(model, ckpt);
            } else if (++patience >= cfg_.early_stop_patience) {
                std::print("  Early stop ep {}\n", ep);
                break;
            }
        }

        torch::load(model, ckpt);
        model->eval();
        std::print("[pretrain] Best val loss: {:.5f}\n", best_val);
        torch::save(model, cfg_.model_path);
        std::print("[pretrain] Salvat: {}\n", cfg_.model_path);
        return model;
    }
private:
    const app_config&  cfg_;
    const norm_params& norm_;

    nnmpc_transformer build_model() {
        return nnmpc_transformer(cfg_.n_input_features, cfg_.d_model,
                                cfg_.nhead, cfg_.num_layers, cfg_.horizon);
    }

    std::pair<std::vector<torch::Tensor>, std::vector<torch::Tensor>>
    load_sequences() {
        std::ifstream f(cfg_.metrics_csv);
        std::vector<torch::Tensor> Xv, yv;
        if (!f) return {};

        std::vector<float> arrs, ahts;
        std::string line;
        std::getline(f, line);
        while (std::getline(f, line)) {
            std::istringstream ss(line); std::string tok;
            try {
                std::getline(ss,tok,','); // ts
                std::getline(ss,tok,','); arrs.push_back(norm_.norm_arr(std::stod(tok)));
                std::getline(ss,tok,','); ahts.push_back(norm_.norm_aht(std::stod(tok)));
            } catch(...) {}
        }

        int T = (int)arrs.size();
        int total = cfg_.seq_len + cfg_.horizon;
        if (T < total) return {};

        for (int i = 0; i <= T - total; ++i) {
            auto x = torch::zeros({cfg_.seq_len, 2});
            for (int s = 0; s < cfg_.seq_len; ++s) {
                x[s][0] = arrs[i+s]; x[s][1] = ahts[i+s];
            }
            float la = arrs[i+cfg_.seq_len-1], lh = ahts[i+cfg_.seq_len-1];
            auto yy = torch::zeros({cfg_.horizon, 2});
            for (int h = 0; h < cfg_.horizon; ++h) {
                yy[h][0] = arrs[i+cfg_.seq_len+h] - la;
                yy[h][1] = ahts[i+cfg_.seq_len+h] - lh;
            }
            Xv.push_back(x); yv.push_back(yy);
        }
        return {Xv, yv};
    }

    torch::Tensor stack(const std::vector<torch::Tensor>& v, int from, int to) {
        return torch::stack(
            std::vector<torch::Tensor>(v.begin()+from, v.begin()+to), 0);
    }

  
    float train_epoch(nnmpc_transformer& m, torch::optim::AdamW& opt,
                      const torch::Tensor& X, const torch::Tensor& y) {
        int N = X.size(0), B = cfg_.pretrain_batch;
        float total = 0.0f; int nb = 0;
        auto idx = torch::randperm(N);
        for (int s = 0; s < N; s += B) {
            int e = std::min(s+B, N);
            auto bi = idx.narrow(0, s, e-s);
            auto Xb = X.index_select(0,bi).permute({1,0,2});
            auto yb = y.index_select(0,bi).permute({1,0,2});
            opt.zero_grad();
            auto pred = m->forward(Xb);
            auto loss = torch::huber_loss(pred, yb, torch::Reduction::Mean, 0.5f);
            loss.backward();
            torch::nn::utils::clip_grad_norm_(m->parameters(), cfg_.pretrain_grad_clip);
            opt.step();
            total += loss.item<float>(); ++nb;
        }
        return nb > 0 ? total/nb : 0.0f;
    }

    float eval_epoch(nnmpc_transformer& m,
                     const torch::Tensor& X, const torch::Tensor& y) {
        torch::NoGradGuard ng;
        m->eval();
        auto loss = torch::huber_loss(
            m->forward(X.permute({1,0,2})),
            y.permute({1,0,2}),
            torch::Reduction::Mean, 0.5f).item<float>();
        m->train();
        return loss;
    }
};


struct pretrain_multi {
  
  pretrain_multi(const app_config& cfg, const norm_params& norm)
        : cfg_(cfg), norm_(norm) {}

  nnmpc_transformer run(const std::string& csv_path) {
    std::print("\n[PretrainMulti] Încarc secvențe din {}...\n", csv_path);
    auto [X, y] = load_sequences(csv_path);
        if (X.empty()) {
            std::print(stderr, "[PretrainMulti] Nicio secvență!\n");
            return build_model();
        }
        std::print("[PretrainMulti] {} secvențe\n", X.size());

        int N = static_cast<int>(X.size());
        int n_val = std::max(1, static_cast<int>(N * 0.15));
        int n_tr  = N - n_val;

        auto Xtr = stack(X, 0, n_tr);
        auto ytr = stack(y, 0, n_tr);
        auto Xvl = stack(X, n_tr, N);
        auto yvl = stack(y, n_tr, N);

        auto model = build_model();
        model->train();
        torch::optim::AdamW opt(model->parameters(),
            torch::optim::AdamWOptions(cfg_.pretrain_lr).weight_decay(cfg_.pretrain_wd));

        float best_val = std::numeric_limits<float>::max();
        int   patience = 0;
        std::string ckpt = cfg_.model_dir + "/pretrain_best.pt";
        std::filesystem::create_directories(cfg_.model_dir);

        for (int ep = 1; ep <= cfg_.pretrain_epochs; ++ep) {
            float lr = cfg_.pretrain_lr_min + 0.5f *
                (cfg_.pretrain_lr - cfg_.pretrain_lr_min) *
                (1.0f + std::cos(static_cast<float>(ep - 1) / cfg_.pretrain_epochs * M_PIf32));
            for (auto& pg : opt.param_groups()) pg.options().set_lr(lr);

            float tr = train_epoch(model, opt, Xtr, ytr);
            float vl = eval_epoch(model, Xvl, yvl);
            std::print("  Ep {}/{} | train={:.5f}  val={:.5f}\n", ep, cfg_.pretrain_epochs, tr, vl);
            if (vl < best_val) {
                best_val = vl; patience = 0;
                torch::save(model, ckpt);
            } else if (++patience >= cfg_.early_stop_patience) {
                std::print("  Early stop ep {}\n", ep);
                break;
            }
        }

        torch::load(model, ckpt);
        model->eval();
        return model;
  }

  nnmpc_transformer build_model() {
    return nnmpc_transformer(cfg_.n_input_features, cfg_.d_model,
                              cfg_.nhead, cfg_.num_layers, cfg_.horizon);
  }

  std::pair<std::vector<torch::Tensor>, std::vector<torch::Tensor>>
    load_sequences(const std::string& csv_path) {
        std::ifstream f(csv_path);
        if (!f) return {};
        std::string line; std::getline(f, line);
        std::vector<float> arrs, ahts;
        while (std::getline(f, line)) {
            std::istringstream ss(line); std::string tok;
            try {
                std::getline(ss, tok, ','); // ts
                std::getline(ss, tok, ','); arrs.push_back(norm_.norm_arr(std::stod(tok)));
                std::getline(ss, tok, ','); ahts.push_back(norm_.norm_aht(std::stod(tok)));
            } catch(...) {}
        }
        int T = static_cast<int>(arrs.size());
        int total = cfg_.seq_len + cfg_.horizon;
        if (T < total) return {};
        std::vector<torch::Tensor> Xv, yv;
        for (int i = 0; i <= T - total; ++i) {
            auto x = torch::zeros({cfg_.seq_len, cfg_.n_input_features});
            for (int s = 0; s < cfg_.seq_len; ++s) {
                x[s][0] = arrs[i+s]; x[s][1] = ahts[i+s];
            }
            auto yy = torch::zeros({cfg_.horizon, 2});
            float la = arrs[i+cfg_.seq_len-1], lh = ahts[i+cfg_.seq_len-1];
            for (int h = 0; h < cfg_.horizon; ++h) {
                yy[h][0] = arrs[i+cfg_.seq_len+h] - la;
                yy[h][1] = ahts[i+cfg_.seq_len+h] - lh;
            }
            Xv.push_back(x); yv.push_back(yy);
        }
        return {Xv, yv};
  }

  torch::Tensor stack(const std::vector<torch::Tensor>& v, int from, int to) {
    return torch::stack(
      std::vector<torch::Tensor>(v.begin()+from, v.begin()+to), 0);
  }

  float train_epoch(nnmpc_transformer& m, torch::optim::AdamW& opt,
      const torch::Tensor& X, const torch::Tensor& y) {
    int N = X.size(0), B = cfg_.pretrain_batch;
    float total = 0.0f; int nb = 0;
    auto idx = torch::randperm(N);
    for (int s = 0; s < N; s += B) {
      int e = std::min(s+B, N);
      auto bi = idx.narrow(0, s, e-s);
      auto Xb = X.index_select(0,bi).permute({1,0,2});
      auto yb = y.index_select(0,bi).permute({1,0,2});
      opt.zero_grad();
      auto pred = m->forward(Xb);
      auto loss = torch::huber_loss(pred, yb, torch::Reduction::Mean, 0.5f);
      loss.backward();
      torch::nn::utils::clip_grad_norm_(m->parameters(), cfg_.pretrain_grad_clip);
      opt.step();
      total += loss.item<float>(); ++nb;
    }

    return nb > 0 ? total/nb : 0.0f;
  }

  float eval_epoch(nnmpc_transformer& m,
      const torch::Tensor& X, const torch::Tensor& y) {
    torch::NoGradGuard ng;
    m->eval();
    auto loss = torch::huber_loss(
    m->forward(X.permute({1,0,2})),
    y.permute({1,0,2}),
    torch::Reduction::Mean, 0.5f).item<float>();
    m->train();
    return loss;
  }

  private:
  
  app_config      cfg_;
  norm_params     norm_;
};


