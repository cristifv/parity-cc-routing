// The forecaster's inference time (PLAN §5.9: <= 5 ms per tenant, cap 20 ms): the shipped base model
// on one intra-op thread, a tenant's queues as the batch (1, 3, 6, 12 queues), `--runs` runs each
// with fresh inputs; p50/p99/max in ms. Evidence only (not a test):
//   bazel run -c opt //tests/forecast:inference_bench -- --model=$PWD/model/forecast [--runs=2000]
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "src/forecast/model_file.h"
#include "src/forecast/torch_net.h"

int main(int argc, char** argv) {
  using namespace pcr::fc;
  std::string dir = "model/forecast";
  int runs = 2000;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a.rfind("--model=", 0) == 0) dir = a.substr(8);
    if (a.rfind("--runs=", 0) == 0) runs = std::stoi(a.substr(7));
  }
  torch_threads(1, 1);
  std::string why;
  auto base = load_base_spec(dir, "", &why);
  if (!base) {
    std::fprintf(stderr, "base model refused: %s\n", why.c_str());
    return 1;
  }
  std::mt19937_64 rng(1);
  std::uniform_real_distribution<float> u(0, 1.2f);
  std::printf("{\"parameters\": %zu, \"runs\": %d, \"batches\": [", base->net->parameters(), runs);
  bool first = true;
  for (int q : {1, 3, 6, 12}) {
    std::vector<double> ms;
    for (int r = 0; r < runs + 50; ++r) {
      std::vector<NetInput> in(static_cast<size_t>(q));
      for (auto& x : in) {
        x.h1.resize(kH1Inputs);
        x.ctx.resize(kCtxTokens * kCtxFeats);
        x.glob.resize(kGlobFeats);
        for (auto& v : x.h1) v = u(rng);
        for (auto& v : x.ctx) v = u(rng);
        for (auto& v : x.glob) v = u(rng);
      }
      std::vector<NetOutput> out;
      const auto t0 = std::chrono::steady_clock::now();
      if (!base->net->run(in, &out)) return 1;
      const double d = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
      if (r >= 50) ms.push_back(d);  // after a warm-up
    }
    std::sort(ms.begin(), ms.end());
    auto at = [&](double p) { return ms[static_cast<size_t>(p * static_cast<double>(ms.size() - 1))]; };
    std::printf("%s{\"queues\": %d, \"p50_ms\": %.3f, \"p99_ms\": %.3f, \"max_ms\": %.3f}", first ? "" : ", ", q, at(0.5),
                at(0.99), ms.back());
    first = false;
  }
  std::printf("]}\n");
  return 0;
}
