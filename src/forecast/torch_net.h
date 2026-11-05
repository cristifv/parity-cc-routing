// The NNMPC forecaster network in libtorch's C++ frontend (PLAN §3.8, §5.9), rewritten from the
// PoC's nnmpc_transformer (parked/nnmpc/predictor): per queue, 12 one-minute tokens (six 10 s
// counts each) and 8 fifteen-minute context tokens, calendar/level features added to every token,
// learned positions and token types, a pre-norm transformer encoder (RMSNorm, multi-head
// attention, SwiGLU feed-forward), a final RMSNorm, and quantile heads (p50 plus softplus offsets:
// p10 <= p50 <= p90 by construction) for H1 (60 x 1 min) and H2 (8 x 15 min arrivals and AHT).
//
// Serialization: torch::save of the parameters only (a TorchScript-format archive with no code;
// the code is this file). Loading checks the sha256 of the bytes BEFORE torch::load (which reads
// the archive with torch::jit::load), and the config must match.
#pragma once

#include <memory>
#include <string>

#include "src/forecast/net.h"

namespace pcr::fc {

// Builds a fresh (randomly initialized, seeded) net.
std::unique_ptr<Net> make_net(const NetConfig& c, uint64_t seed);
// Loads a net from its serialized bytes. Refused (null, *why set) when the sha256 of `bytes` isn't
// `sha256` (checked before any parsing) or the archive doesn't fit the config.
std::unique_ptr<Net> load_net(const NetConfig& c, const std::string& bytes, const std::string& sha256, std::string* why);
// Serializes a net (torch::save): the bytes and their sha256.
bool save_net(const Net& n, std::string* bytes, std::string* sha256);
// One intra-op and one inter-op thread for inference (PLAN §5.9); the trainer sets its own.
void torch_threads(int intra, int inter);

// Training (the trainer only): a batch of inputs and targets, one optimizer step.
struct Batch {
  std::vector<NetInput> in;
  std::vector<std::vector<float>> t1;   // kH1Steps normalized targets (NaN = missing)
  std::vector<std::vector<float>> t2a;  // kH2Buckets
  std::vector<std::vector<float>> t2h;  // kH2Buckets (NaN = no call handled)
};
class Trainer {
 public:
  virtual ~Trainer() = default;
  // One AdamW step on the pinball loss; returns the loss (NaN on failure).
  virtual double step(const Batch& b, double lr) = 0;
  // The loss without a step (eval).
  virtual double loss(const Batch& b) = 0;
  virtual Net& net() = 0;
};
std::unique_ptr<Trainer> make_trainer(std::unique_ptr<Net> start, double weight_decay);

}  // namespace pcr::fc
