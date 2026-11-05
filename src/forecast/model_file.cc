#include "src/forecast/model_file.h"

#include <fstream>
#include <iterator>

#include "src/forecast/torch_net.h"

namespace pcr::fc {

namespace {
bool read_file(const std::string& path, std::string* out) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  out->assign(std::istreambuf_iterator<char>(in), {});
  return true;
}
}  // namespace

std::shared_ptr<const ModelSpec> load_base_spec(const std::string& dir, const std::string& pinned_sha256,
                                                std::string* why) {
  std::string meta_text, bytes;
  if (!read_file(dir + "/base.json", &meta_text) || !read_file(dir + "/base.pt", &bytes)) {
    *why = "missing";
    return nullptr;
  }
  const nlohmann::json meta = nlohmann::json::parse(meta_text, nullptr, false);
  if (meta.is_discarded() || !meta.contains("config") || !meta.contains("sha256")) {
    *why = "bad_metadata";
    return nullptr;
  }
  const std::string sha = meta.value("sha256", std::string());
  if (!pinned_sha256.empty() && pinned_sha256 != sha) {
    *why = "sha256_pin";
    return nullptr;
  }
  NetConfig c;
  try {
    c = NetConfig::from_json(meta.at("config"));
  } catch (const std::exception&) {
    *why = "bad_config";
    return nullptr;
  }
  std::unique_ptr<Net> net = load_net(c, bytes, sha, why);
  if (!net) return nullptr;
  auto spec = std::make_shared<ModelSpec>();
  spec->version = 0;
  spec->sha256 = sha;
  spec->created_at_ms = meta.value("created_at_ms", int64_t{0});
  spec->net = std::shared_ptr<const Net>(std::move(net));
  const nlohmann::json n = meta.value("normalization", nlohmann::json::object());
  spec->envelope = n.value("envelope", 2.0);
  if (n.contains("psi_ref") && n["psi_ref"].is_array()) spec->psi_ref = n["psi_ref"].get<std::vector<double>>();
  return spec;
}

std::shared_ptr<const ModelSpec> spec_from_row(uint64_t version, const std::string& sha256, const std::string& bytes,
                                               const nlohmann::json& config, const nlohmann::json& normalization,
                                               int64_t created_at_ms, std::string* why) {
  NetConfig c;
  try {
    c = NetConfig::from_json(config);
  } catch (const std::exception&) {
    *why = "bad_config";
    return nullptr;
  }
  std::unique_ptr<Net> net = load_net(c, bytes, sha256, why);
  if (!net) return nullptr;
  auto spec = std::make_shared<ModelSpec>();
  spec->version = version;
  spec->sha256 = sha256;
  spec->created_at_ms = created_at_ms;
  spec->net = std::shared_ptr<const Net>(std::move(net));
  if (normalization.is_object()) {
    for (const auto& [q, j] : normalization.items()) {
      if (q == "_envelope") continue;
      spec->norm[q] = QueueNorm::from_json(j);
    }
    if (!spec->norm.empty()) spec->envelope = spec->norm.begin()->second.envelope;
  }
  return spec;
}

}  // namespace pcr::fc
