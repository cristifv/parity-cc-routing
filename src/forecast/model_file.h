// Loading the forecaster's models into fc::ModelSpec (PLAN §5.9 "Loading safety"): the base model's
// file pair (model/forecast/base.pt + base.json) and a tenant's routing_forecast_model row. The
// sha256 of the bytes is checked BEFORE they are parsed (torch::load reads the archive with
// torch::jit::load; the archive holds parameters only, the code is RE's own), and the stored config
// must equal the one the bytes load into. Every refusal says why (model.refused).
#pragma once

#include <memory>
#include <nlohmann/json.hpp>
#include <string>

#include "src/forecast/source.h"

namespace pcr::fc {

// `pinned_sha256` (the --forecast-model-sha256 flag), when set, must also equal base.json's.
std::shared_ptr<const ModelSpec> load_base_spec(const std::string& dir, const std::string& pinned_sha256,
                                                std::string* why);
// A tenant's model row (already checked: written_by is the trainer's role).
std::shared_ptr<const ModelSpec> spec_from_row(uint64_t version, const std::string& sha256, const std::string& bytes,
                                               const nlohmann::json& config, const nlohmann::json& normalization,
                                               int64_t created_at_ms, std::string* why);

}  // namespace pcr::fc
