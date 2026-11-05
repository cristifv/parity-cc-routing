#include "src/ops/pii.h"

#include <array>
#include <string_view>

namespace pcr::pii {

namespace {

constexpr std::string_view kRedacted = "[redacted]";
constexpr std::array<std::string_view, 33> kPhoneNames = {
    "from", "to", "number", "phone", "caller", "callee", "caller_id", "callback_number", "destination",
    "ani", "dnis", "msisdn", "e164", "contact", "uri", "pai", "p_asserted_identity", "rpid",
    "remote_party_id", "diversion", "history_info", "user", "channel_name", "caller_channel_name",
    "caller_username", "rdnis", "dialed_extension", "originate_string", "dial_string", "sip_from_display",
    "caller_caller_id_name", "sip_destination_url", "sip_call_id"};
constexpr std::array<std::string_view, 14> kSecretNames = {
    "digits", "dtmf", "pin", "password", "secret", "token", "authorization", "api_key", "dek",
    "wrapped_dek", "sdp", "sip_body", "passwd", "credentials"};
constexpr std::array<std::string_view, 3> kSecretTokens = {"dtmf", "digit", "digits"};
constexpr std::array<std::string_view, 2> kExempt = {"reply_to", "num_digits"};

bool upper(char c) { return c >= 'A' && c <= 'Z'; }
bool lower(char c) { return c >= 'a' && c <= 'z'; }
bool digit(char c) { return c >= '0' && c <= '9'; }

bool ends_with(std::string_view s, std::string_view t) { return s.size() >= t.size() && s.substr(s.size() - t.size()) == t; }

// `name` is `listed`, or ends with "_" + `listed`.
bool matches(std::string_view name, std::string_view listed) {
  return name == listed ||
         (name.size() > listed.size() && ends_with(name, listed) && name[name.size() - listed.size() - 1] == '_');
}

template <size_t N>
bool any_of(const std::array<std::string_view, N>& a, std::string_view n) {
  for (auto x : a) {
    if (x == n) return true;
  }
  return false;
}

nlohmann::json mask_all(const nlohmann::json& v) {
  if (v.is_string()) return mask_phone(v.get<std::string>());
  if (v.is_number()) return mask_phone(v.dump());
  if (v.is_array()) {
    nlohmann::json out = nlohmann::json::array();
    for (const auto& x : v) out.push_back(mask_all(x));
    return out;
  }
  if (v.is_object()) {
    nlohmann::json out = nlohmann::json::object();
    for (const auto& [k, x] : v.items()) out[k] = mask_all(x);
    return out;
  }
  return v;
}

}  // namespace

std::string normalize(const std::string& name) {
  std::string out;
  out.reserve(name.size() + 4);
  for (size_t i = 0; i < name.size(); ++i) {
    const char c = name[i];
    if (c == '-') {
      out += '_';
      continue;
    }
    if (upper(c) && i > 0) {
      const char prev = name[i - 1];
      const bool next_lower = i + 1 < name.size() && lower(name[i + 1]);
      if (lower(prev) || digit(prev) || (upper(prev) && next_lower)) out += '_';
    }
    out += upper(c) ? static_cast<char>(c - 'A' + 'a') : c;
  }
  constexpr std::string_view kVar = "variable_";
  if (out.rfind(kVar, 0) == 0) out.erase(0, kVar.size());
  return out;
}

Class classify(const std::string& name) {
  const std::string n = normalize(name);
  if (any_of(kExempt, n)) return Class::kPlain;
  size_t start = 0;
  for (;;) {
    const size_t us = n.find('_', start);
    if (any_of(kSecretTokens, std::string_view(n).substr(start, us == std::string::npos ? std::string::npos : us - start))) {
      return Class::kSecret;
    }
    if (us == std::string::npos) break;
    start = us + 1;
  }
  for (auto s : kSecretNames) {
    if (matches(n, s)) return Class::kSecret;
  }
  for (auto s : {"_secret", "_token", "_password"}) {
    if (ends_with(n, s)) return Class::kSecret;
  }
  for (auto p : kPhoneNames) {
    if (matches(n, p)) return Class::kPhone;
  }
  for (auto s : {"_number", "_phone", "_user"}) {
    if (ends_with(n, s)) return Class::kPhone;
  }
  return Class::kPlain;
}

std::string mask_phone(const std::string& raw) {
  std::string digits;
  for (char c : raw) {
    if (digit(c)) digits += c;
  }
  if (const size_t at = raw.find("***"); at != std::string::npos) {
    std::string tail;
    for (size_t i = at + 3; i < raw.size() && digit(raw[i]); ++i) tail += raw[i];
    if (!tail.empty() && tail.size() <= 3 && tail.size() == digits.size()) return "***" + tail;
  }
  if (digits.size() >= 4) return "***" + digits.substr(digits.size() - 3);
  return "***";
}

nlohmann::json mask_nested(const nlohmann::json& v) {
  if (v.is_object()) {
    nlohmann::json out = nlohmann::json::object();
    for (const auto& [k, x] : v.items()) out[k] = mask_field(k, x);
    return out;
  }
  if (v.is_array()) {
    nlohmann::json out = nlohmann::json::array();
    for (const auto& x : v) out.push_back(mask_nested(x));
    return out;
  }
  return v;
}

nlohmann::json mask_field(const std::string& name, const nlohmann::json& value) {
  switch (classify(name)) {
    case Class::kSecret:
      return std::string(kRedacted);
    case Class::kPhone:
      return mask_all(value);
    case Class::kPlain:
    default:
      return mask_nested(value);
  }
}

std::string mask_field(const std::string& name, const std::string& value) {
  switch (classify(name)) {
    case Class::kSecret:
      return std::string(kRedacted);
    case Class::kPhone:
      return mask_phone(value);
    case Class::kPlain:
    default:
      return value;
  }
}

}  // namespace pcr::pii
