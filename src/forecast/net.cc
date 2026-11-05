#include "src/forecast/net.h"

namespace pcr::fc {

nlohmann::json NetConfig::to_json() const {
  return {{"arch", arch}, {"d_model", d_model}, {"heads", heads}, {"layers", layers}, {"ffn", ffn},
          {"h1_tokens", kH1Tokens}, {"h1_patch", kH1Patch}, {"ctx_tokens", kCtxTokens}, {"ctx_feats", kCtxFeats},
          {"glob_feats", kGlobFeats}, {"h1_steps", kH1Steps}, {"h2_buckets", kH2Buckets}, {"quantiles", {0.1, 0.5, 0.9}}};
}

NetConfig NetConfig::from_json(const nlohmann::json& j) {
  NetConfig c;
  c.arch = j.value("arch", std::string());
  c.d_model = j.value("d_model", 0);
  c.heads = j.value("heads", 0);
  c.layers = j.value("layers", 0);
  c.ffn = j.value("ffn", 0);
  // The fixed layout must be this build's: a model made for another layout is refused (arch "").
  if (j.value("h1_tokens", 0) != kH1Tokens || j.value("h1_patch", 0) != kH1Patch || j.value("ctx_tokens", 0) != kCtxTokens ||
      j.value("ctx_feats", 0) != kCtxFeats || j.value("glob_feats", 0) != kGlobFeats || j.value("h1_steps", 0) != kH1Steps ||
      j.value("h2_buckets", 0) != kH2Buckets) {
    c.arch.clear();
  }
  return c;
}

bool NetConfig::operator==(const NetConfig& o) const {
  return arch == o.arch && d_model == o.d_model && heads == o.heads && layers == o.layers && ffn == o.ffn;
}

}  // namespace pcr::fc
