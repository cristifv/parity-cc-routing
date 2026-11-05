#pragma once

#include <torch/torch.h>
#include <torch/nn/modules/transformer.h>
#include <torch/nn/modules/normalization.h>
#include <torch/csrc/api/include/torch/nn.h>


// torch version
struct nnmpc_transformerImpl : torch::nn::Module {
  
  public:

  nnmpc_transformerImpl(int n_features, int d_model, int nhead,
      int num_layers, int horizon, float dropout = 0.1f);
/*
  static torch::Tensor make_pos_enc(int max_len,  int d_model) {
    auto pe  = torch::zeros({max_len, 1, d_model});
    auto pos = torch::arange(0, max_len, torch::kFloat).unsqueeze(1);
    auto div = torch::exp(
        torch::arange(0, d_model, 2, torch::kFloat)
            * (-std::log(10000.0) / d_model));
        pe.index({"...", 0, torch::indexing::Slice(0, torch::indexing::None, 2)})
            = torch::sin(pos * div);
        pe.index({"...", 0, torch::indexing::Slice(1, torch::indexing::None, 2)})
            = torch::cos(pos * div);
    return pe;
  }
*/

  // Positional encoding fără torch::indexing — compatibil cu orice libtorch 2.x
  // Layout output: [max_len, 1, d_model]  (seq, batch=1, features)
  static torch::Tensor make_pos_enc(int max_len, int d_model) {
    auto pe      = torch::zeros({max_len, 1, d_model});
    auto pos     = torch::arange(0, max_len, torch::kFloat).unsqueeze(1); // [L,1]
    auto div     = torch::exp(
            torch::arange(0, d_model, 2, torch::kFloat)
            * (-std::log(10000.0) / d_model));               // [d/2]
    auto sin_v   = torch::sin(pos * div);                // [L, d/2]
    auto cos_v   = torch::cos(pos * div);                // [L, d/2]

        // Scriem coloanele fără torch::indexing (API stabil în toate versiunile)
    int half = d_model / 2;
    for (int i = 0; i < half; ++i) {
            // pe[:, 0, 2i]   = sin_v[:, i]
            // pe[:, 0, 2i+1] = cos_v[:, i]
            pe.select(2, 2*i    ).copy_(sin_v.select(1, i).unsqueeze(1));
            pe.select(2, 2*i + 1).copy_(cos_v.select(1, i).unsqueeze(1));
    }
    return pe;
  }

  torch::Tensor forward(torch::Tensor);

  private:
    torch::nn::Linear               input_proj{nullptr};
    torch::nn::TransformerEncoder   transformer{nullptr};
    torch::nn::LayerNorm            layer_norm{nullptr};
    torch::nn::Linear               output_proj{nullptr};
    torch::Tensor                   pos_enc;
    int                             horizon_;
};

TORCH_MODULE(nnmpc_transformer);

