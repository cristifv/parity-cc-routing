// The test control (PLAN §8 1.4): /_test/v1/{reset,seed,advance,inject,sync,restart,invariants}
// on 127.0.0.1:9811, only with --test-control and only in local-* environments (main refuses it
// elsewhere). The engine runs on a manual clock then; nothing here exists in a production config.
#pragma once

#include <string>
#include <vector>

#include "src/core/engine.h"
#include "src/ops/http.h"
#include "src/server/hub.h"

namespace pcr {

class TestControl {
 public:
  TestControl(Engine* engine, Hub* hub, ManualClock* clock, std::vector<TenantConfig> boot,
              Epochs boot_epochs, std::string bin)
      : engine_(engine), hub_(hub), clock_(clock), boot_(std::move(boot)), boot_epochs_(boot_epochs),
        bin_(std::move(bin)) {}
  HttpReply handle(const std::string& method, const std::string& path, const std::string& body);

 private:
  HttpReply seed(const std::string& body);
  HttpReply inject(const std::string& body);
  Engine* engine_;
  Hub* hub_;
  ManualClock* clock_;
  std::vector<TenantConfig> boot_;
  Epochs boot_epochs_;
  std::string bin_;
  std::vector<TenantConfig> current_;
};

}  // namespace pcr
