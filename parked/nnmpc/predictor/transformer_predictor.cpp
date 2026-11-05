#include <transformer_predictor.h>

nnmpc_transformerImpl::nnmpc_transformerImpl(int n_features,
    int d_model, int nhead, int num_layers, int horizon, float dropout)
  :horizon_(horizon){
    input_proj  = register_module("input_proj",
                        torch::nn::Linear(n_features, d_model));
    output_proj = register_module("output_proj",
                        torch::nn::Linear(d_model, 2));
    layer_norm  = register_module("layer_norm",
                        torch::nn::LayerNorm(
                            torch::nn::LayerNormOptions({d_model})));

        auto enc_opts = torch::nn::TransformerEncoderLayerOptions(d_model, nhead)
            .dim_feedforward(d_model * 4)
            .dropout(dropout);
            //.batch_first(false)
            //.norm_first(true);   // Pre-LN: mai stabilă la fine-tuning
        transformer = register_module("transformer",
            torch::nn::TransformerEncoder(
                torch::nn::TransformerEncoderOptions(
                    torch::nn::TransformerEncoderLayer(enc_opts), num_layers)));

        pos_enc = make_pos_enc(512, d_model);
        register_buffer("pos_enc", pos_enc);

        torch::nn::init::xavier_uniform_(input_proj->weight);
        torch::nn::init::xavier_uniform_(output_proj->weight);
        torch::nn::init::zeros_(input_proj->bias);
        torch::nn::init::zeros_(output_proj->bias);
}

torch::Tensor nnmpc_transformerImpl::forward(torch::Tensor x) {
  int seq = x.size(0);
  x = input_proj(x);
  x = x + pos_enc.narrow(0, 0, seq);
  x = transformer(x);
  x = layer_norm(x);
  x = x.narrow(0, std::max(0, seq - horizon_), horizon_);
  return output_proj(x);  // [horizon, batch, 2]
}


