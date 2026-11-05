// The phase-1 configuration (PLAN §8 "Config in phase 1"): a seed file, `seed.v1` (schema in
// seed/seed.v1.schema.json), with each tenant's queues, skills and agents. A queue without
// max_wait_s and max_wait_action is refused (review M1). The config.* topics replace it in phase 4.
#pragma once

#include <string>
#include <vector>

#include "src/core/types.h"

namespace pcr {

// Parses and validates a seed document; returns the tenants of `bin`. On error returns false with
// *error naming the first problem (no personal data in seeds).
bool parse_seed(const std::string& json, const std::string& bin, std::vector<TenantConfig>* out,
                std::string* error);
// Phase 6 (tenant moves): every tenant of the seed with the bin it names (the static assignment;
// tp/tenants/{tenant} in etcd overrides it).
bool parse_seed_all(const std::string& json, std::vector<std::pair<std::string, TenantConfig>>* out, std::string* error);

// Phase 4: a config.routing document (parity-contracts schemas/config/config-routing.v1.json).
// *tenant gets its tenant_id. Shapes only; the tenant checks its queues (install_policy).
bool parse_routing_doc(const std::string& json, RoutingPolicy* out, std::string* tenant, std::string* error);
// A config.routing_pin document, or (`"tombstone": true`, as the config reader maps a Kafka
// tombstone) an unpin. *tenant gets its tenant_id.
bool parse_pin_doc(const std::string& json, RoutingPin* out, bool* tombstone, std::string* tenant,
                   std::string* error);

}  // namespace pcr
