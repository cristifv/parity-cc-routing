// SHA-256 (FIPS 180-4) for the model loader's integrity check (PLAN §5.9: the bytes' sha256 is
// checked before torch::jit::load). Plain C++, no dependency; known-answer tested.
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace pcr::fc {

// Lowercase hex digest of `data`.
std::string sha256_hex(std::string_view data);

}  // namespace pcr::fc
