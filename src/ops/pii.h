// The shared masking rules (ARCHITECTURE.md §5.2; parity-contracts masking/cases.v1.json), ported
// from interaction-engine's ie-obs (crates/ie-obs/src/pii.rs) so every service masks alike
// (CR-052). A field name is normalized, then classified: phone (masked to its last 3 digits),
// secret ("[redacted]") or plain (as is; structured values are masked key by key, at every depth).
#pragma once

#include <nlohmann/json.hpp>
#include <string>

namespace pcr::pii {

enum class Class { kPhone, kSecret, kPlain };

std::string normalize(const std::string& name);
Class classify(const std::string& name);
std::string mask_phone(const std::string& raw);
// A value logged under `name`.
nlohmann::json mask_field(const std::string& name, const nlohmann::json& value);
std::string mask_field(const std::string& name, const std::string& value);
// A value logged under a plain name: the policy applied to every key at every depth.
nlohmann::json mask_nested(const nlohmann::json& value);

}  // namespace pcr::pii
