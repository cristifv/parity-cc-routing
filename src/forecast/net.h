// The learned forecaster's network behind a torch-free interface (PLAN §5.9): //src/forecast's
// policy code (health, scoring, the ForecastSource) never includes libtorch, so it builds and is
// tested in every lane (sanitizers included); only torch_net.cc links libtorch.
//
// Thread contract (p5 review finding 7, CR-181): a Net is immutable once loaded and run() is const:
// it may be called from several optimization threads at once (no autograd, no module state written
// in forward). The base net is shared by every tenant; a tenant's fine-tuned net is its own.
#pragma once

#include <cstdint>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "src/forecast/features.h"

namespace pcr::fc {

// The architecture (stored with every model; a mismatch is refused at load). The user's design
// (2026-11-01): d_model 32-64, 2-3 layers, pre-norm RMSNorm, SwiGLU with hidden ~ 2/3 * 4d,
// quantile heads, < 1 MB.
struct NetConfig {
  std::string arch = "nnmpc-forecaster-v1";
  int d_model = 48;
  int heads = 4;
  int layers = 2;
  int ffn = 128;  // SwiGLU hidden (2/3 * 4 * 48)
  nlohmann::json to_json() const;
  static NetConfig from_json(const nlohmann::json& j);
  bool operator==(const NetConfig& o) const;
};

class Net {
 public:
  virtual ~Net() = default;
  virtual const NetConfig& config() const = 0;
  // One forward pass over a tenant's queues (the batch). False on a failure (no output then).
  virtual bool run(const std::vector<NetInput>& in, std::vector<NetOutput>* out) const = 0;
  virtual size_t parameters() const = 0;
};

}  // namespace pcr::fc
