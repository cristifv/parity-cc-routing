#include "src/forecast/torch_net.h"

#include <torch/torch.h>

#include <cmath>
#include <mutex>
#include <sstream>

#include "src/forecast/sha256.h"

namespace pcr::fc {

namespace {

using torch::Tensor;

struct RMSNormImpl : torch::nn::Module {
  explicit RMSNormImpl(int64_t d) { w = register_parameter("w", torch::ones({d})); }
  Tensor forward(const Tensor& x) { return x * torch::rsqrt(x.pow(2).mean(-1, /*keepdim=*/true) + 1e-6) * w; }
  Tensor w;
};
TORCH_MODULE(RMSNorm);

// Pre-norm block: x + Attn(RMSNorm(x)), then x + SwiGLU(RMSNorm(x)).
struct BlockImpl : torch::nn::Module {
  BlockImpl(int64_t d, int64_t h, int64_t ffn) : heads(h), dh(d / h) {
    n1 = register_module("n1", RMSNorm(d));
    n2 = register_module("n2", RMSNorm(d));
    qkv = register_module("qkv", torch::nn::Linear(d, 3 * d));
    o = register_module("o", torch::nn::Linear(d, d));
    w12 = register_module("w12", torch::nn::Linear(torch::nn::LinearOptions(d, 2 * ffn).bias(false)));
    w3 = register_module("w3", torch::nn::Linear(torch::nn::LinearOptions(ffn, d).bias(false)));
  }
  Tensor forward(Tensor x) {
    const int64_t B = x.size(0), T = x.size(1), D = x.size(2);
    Tensor y = qkv(n1(x)).view({B, T, 3, heads, dh}).permute({2, 0, 3, 1, 4});  // [3, B, H, T, dh]
    Tensor q = y[0], k = y[1], v = y[2];
    Tensor a = torch::softmax(torch::matmul(q, k.transpose(-2, -1)) / std::sqrt(static_cast<double>(dh)), -1);
    Tensor att = torch::matmul(a, v);  // [B, H, T, dh]
    x = x + o(att.transpose(1, 2).reshape({B, T, D}));
    std::vector<Tensor> gu = w12(n2(x)).chunk(2, -1);
    return x + w3(torch::silu(gu[0]) * gu[1]);
  }
  int64_t heads, dh;
  RMSNorm n1{nullptr}, n2{nullptr};
  torch::nn::Linear qkv{nullptr}, o{nullptr}, w12{nullptr}, w3{nullptr};
};
TORCH_MODULE(Block);

constexpr int64_t kFlat = kH1Inputs + kCtxTokens * kCtxFeats;

struct ForecastNetImpl : torch::nn::Module {
  explicit ForecastNetImpl(const NetConfig& c) {
    const int64_t d = c.d_model;
    in_h1 = register_module("in_h1", torch::nn::Linear(kH1Patch, d));
    in_ctx = register_module("in_ctx", torch::nn::Linear(kCtxFeats, d));
    in_glob = register_module("in_glob", torch::nn::Linear(kGlobFeats, d));
    pos = register_parameter("pos", torch::randn({kH1Tokens + kCtxTokens, d}) * 0.02);
    type = register_parameter("type", torch::randn({2, d}) * 0.02);
    blocks = register_module("blocks", torch::nn::ModuleList());
    for (int i = 0; i < c.layers; ++i) blocks->push_back(Block(d, c.heads, c.ffn));
    norm = register_module("norm", RMSNorm(d));
    head1 = register_module("head1", torch::nn::Linear(2 * d, kH1Steps * kQuantiles));
    head2 = register_module("head2", torch::nn::Linear(2 * d, kH2Buckets * 2 * kQuantiles));
    // A linear autoregressive skip onto the medians (the level the encoder corrects).
    skip1 = register_module("skip1", torch::nn::Linear(kFlat, kH1Steps));
    skip2 = register_module("skip2", torch::nn::Linear(kFlat, kH2Buckets * 2));
  }
  static Tensor quant(const Tensor& r, const Tensor& med_skip) {  // [..., 3] -> p10 <= p50 <= p90
    Tensor m = r.select(-1, 1) + med_skip;
    return torch::stack({m - torch::softplus(r.select(-1, 0)), m, m + torch::softplus(r.select(-1, 2))}, -1);
  }
  // h1 [B, 72], ctx [B, 24], glob [B, 8] -> H1 [B, 60, 3], H2 arrivals [B, 8, 3], H2 AHT [B, 8, 3].
  std::tuple<Tensor, Tensor, Tensor> forward(const Tensor& h1, const Tensor& ctx, const Tensor& glob) {
    const int64_t B = h1.size(0);
    Tensor a = in_h1(h1.view({B, kH1Tokens, kH1Patch})) + type[0];
    Tensor b = in_ctx(ctx.view({B, kCtxTokens, kCtxFeats})) + type[1];
    Tensor x = torch::cat({a, b}, 1) + pos + in_glob(glob).unsqueeze(1);
    for (const auto& m : *blocks) x = m->as<Block>()->forward(x);
    x = norm(x);
    Tensor pooled = torch::cat({x.select(1, kH1Tokens - 1), x.mean(1)}, -1);
    Tensor flat = torch::cat({h1, ctx}, -1);
    Tensor r1 = head1(pooled).view({B, kH1Steps, kQuantiles});
    Tensor r2 = head2(pooled).view({B, kH2Buckets, 2 * kQuantiles});
    Tensor s2 = skip2(flat).view({B, kH2Buckets, 2});
    return {quant(r1, skip1(flat)), quant(r2.slice(2, 0, kQuantiles), s2.select(-1, 0)),
            quant(r2.slice(2, kQuantiles, 2 * kQuantiles), s2.select(-1, 1))};
  }
  torch::nn::Linear in_h1{nullptr}, in_ctx{nullptr}, in_glob{nullptr}, head1{nullptr}, head2{nullptr}, skip1{nullptr},
      skip2{nullptr};
  Tensor pos, type;
  torch::nn::ModuleList blocks{nullptr};
  RMSNorm norm{nullptr};
};
TORCH_MODULE(ForecastNet);

void pack(const std::vector<NetInput>& in, Tensor* h1, Tensor* ctx, Tensor* glob) {
  const int64_t B = static_cast<int64_t>(in.size());
  *h1 = torch::empty({B, kH1Inputs});
  *ctx = torch::empty({B, kCtxTokens * kCtxFeats});
  *glob = torch::empty({B, kGlobFeats});
  float* p1 = h1->data_ptr<float>();
  float* p2 = ctx->data_ptr<float>();
  float* p3 = glob->data_ptr<float>();
  for (int64_t i = 0; i < B; ++i) {
    const NetInput& x = in[static_cast<size_t>(i)];
    for (int j = 0; j < kH1Inputs; ++j) p1[i * kH1Inputs + j] = j < static_cast<int>(x.h1.size()) ? x.h1[j] : 0;
    for (int j = 0; j < kCtxTokens * kCtxFeats; ++j) {
      p2[i * kCtxTokens * kCtxFeats + j] = j < static_cast<int>(x.ctx.size()) ? x.ctx[j] : 0;
    }
    for (int j = 0; j < kGlobFeats; ++j) p3[i * kGlobFeats + j] = j < static_cast<int>(x.glob.size()) ? x.glob[j] : 0;
  }
}

std::vector<float> rows(const Tensor& t, int64_t i) {
  Tensor r = t[i].contiguous().view({-1});
  return std::vector<float>(r.data_ptr<float>(), r.data_ptr<float>() + r.numel());
}

class TorchNet final : public Net {
 public:
  TorchNet(const NetConfig& c, ForecastNet m) : c_(c), m_(std::move(m)) { m_->eval(); }
  const NetConfig& config() const override { return c_; }
  bool run(const std::vector<NetInput>& in, std::vector<NetOutput>* out) const override {
    out->clear();
    if (in.empty()) return true;
    try {
      c10::InferenceMode guard;
      Tensor h1, ctx, glob;
      pack(in, &h1, &ctx, &glob);
      auto [a, b, c] = m_->forward(h1, ctx, glob);
      for (int64_t i = 0; i < static_cast<int64_t>(in.size()); ++i) {
        NetOutput o;
        o.h1 = rows(a, i);
        o.h2a = rows(b, i);
        o.h2h = rows(c, i);
        out->push_back(std::move(o));
      }
      return true;
    } catch (const std::exception&) {
      out->clear();
      return false;
    }
  }
  size_t parameters() const override {
    size_t n = 0;
    for (const auto& p : m_->parameters()) n += static_cast<size_t>(p.numel());
    return n;
  }
  ForecastNet& module() { return m_; }

 private:
  NetConfig c_;
  mutable ForecastNet m_;  // forward writes no state (no dropout, no batch statistics)
};

Tensor targets(const std::vector<std::vector<float>>& t, int64_t width) {
  const int64_t B = static_cast<int64_t>(t.size());
  Tensor x = torch::full({B, width}, std::nanf(""));
  float* p = x.data_ptr<float>();
  for (int64_t i = 0; i < B; ++i) {
    for (int64_t j = 0; j < width && j < static_cast<int64_t>(t[static_cast<size_t>(i)].size()); ++j) {
      p[i * width + j] = t[static_cast<size_t>(i)][static_cast<size_t>(j)];
    }
  }
  return x;
}

// Pinball loss over the quantiles, NaN targets masked out.
Tensor pinball(const Tensor& pred, const Tensor& y) {
  Tensor mask = ~torch::isnan(y);
  Tensor yy = torch::where(mask, y, torch::zeros_like(y)).unsqueeze(-1);
  Tensor tau = torch::tensor({static_cast<float>(kQ[0]), static_cast<float>(kQ[1]), static_cast<float>(kQ[2])});
  Tensor e = yy - pred;
  Tensor l = torch::max(tau * e, (tau - 1) * e) * mask.unsqueeze(-1).to(torch::kFloat);
  return l.sum() / (mask.sum().to(torch::kFloat) * kQuantiles + 1e-6);
}

class TorchTrainer final : public Trainer {
 public:
  TorchTrainer(std::unique_ptr<TorchNet> n, double wd)
      : n_(std::move(n)), opt_(n_->module()->parameters(), torch::optim::AdamWOptions(1e-3).weight_decay(wd)) {}
  Tensor loss_t(const Batch& b) {
    Tensor h1, ctx, glob;
    pack(b.in, &h1, &ctx, &glob);
    auto [a, c, d] = n_->module()->forward(h1, ctx, glob);
    return pinball(a, targets(b.t1, kH1Steps)) + pinball(c, targets(b.t2a, kH2Buckets)) +
           0.5 * pinball(d, targets(b.t2h, kH2Buckets));
  }
  double step(const Batch& b, double lr) override {
    try {
      n_->module()->train();
      for (auto& g : opt_.param_groups()) static_cast<torch::optim::AdamWOptions&>(g.options()).lr(lr);
      opt_.zero_grad();
      Tensor l = loss_t(b);
      l.backward();
      torch::nn::utils::clip_grad_norm_(n_->module()->parameters(), 1.0);
      opt_.step();
      n_->module()->eval();
      return l.item<double>();
    } catch (const std::exception&) {
      return std::nan("");
    }
  }
  double loss(const Batch& b) override {
    try {
      torch::NoGradGuard ng;
      n_->module()->eval();
      return loss_t(b).item<double>();
    } catch (const std::exception&) {
      return std::nan("");
    }
  }
  Net& net() override { return *n_; }

 private:
  std::unique_ptr<TorchNet> n_;
  torch::optim::AdamW opt_;
};

}  // namespace

std::unique_ptr<Net> make_net(const NetConfig& c, uint64_t seed) {
  torch::manual_seed(seed);
  return std::make_unique<TorchNet>(c, ForecastNet(c));
}

std::unique_ptr<Net> load_net(const NetConfig& c, const std::string& bytes, const std::string& sha256, std::string* why) {
  // Integrity first: nothing parses the bytes unless their digest is the one the row names.
  if (sha256.size() != 64 || sha256_hex(bytes) != sha256) {
    if (why) *why = "sha256";
    return nullptr;
  }
  if (c.d_model <= 0 || c.heads <= 0 || c.d_model % c.heads != 0 || c.layers <= 0 || c.layers > 8 || c.ffn <= 0 ||
      c.arch != NetConfig{}.arch) {
    if (why) *why = "config";
    return nullptr;
  }
  try {
    ForecastNet m(c);
    std::istringstream is(bytes);
    torch::load(m, is);  // torch::jit::load underneath
    for (const auto& p : m->parameters()) {
      if (!torch::isfinite(p).all().item<bool>()) {
        if (why) *why = "nonfinite";
        return nullptr;
      }
    }
    return std::make_unique<TorchNet>(c, std::move(m));
  } catch (const std::exception&) {
    if (why) *why = "archive";
    return nullptr;
  }
}

bool save_net(const Net& n, std::string* bytes, std::string* sha256) {
  const auto* t = dynamic_cast<const TorchNet*>(&n);
  if (!t) return false;
  try {
    std::ostringstream os;
    torch::save(const_cast<TorchNet*>(t)->module(), os);
    *bytes = os.str();
    *sha256 = sha256_hex(*bytes);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

void torch_threads(int intra, int inter) {
  static std::once_flag once;
  std::call_once(once, [&] {
    try {
      at::set_num_interop_threads(std::max(1, inter));
    } catch (const std::exception&) {
    }
  });
  at::set_num_threads(std::max(1, intra));
}

std::unique_ptr<Trainer> make_trainer(std::unique_ptr<Net> start, double weight_decay) {
  auto* t = dynamic_cast<TorchNet*>(start.get());
  if (!t) return nullptr;
  start.release();
  return std::make_unique<TorchTrainer>(std::unique_ptr<TorchNet>(t), weight_decay);
}

}  // namespace pcr::fc
