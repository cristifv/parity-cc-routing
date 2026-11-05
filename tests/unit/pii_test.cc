// CR-052: the masking port (src/ops/pii) against parity-contracts' cross-language cases, the same
// file ie-obs (Rust) and TC's obs.Pii (Java) pass.
#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>

#include "src/ops/pii.h"

namespace pcr::pii {
namespace {

nlohmann::json g_cases;

TEST(Masking, Normalize) {
  for (const auto& c : g_cases.at("normalize")) EXPECT_EQ(normalize(c.at("in")), c.at("out").get<std::string>()) << c.dump();
}

TEST(Masking, Classify) {
  const std::map<std::string, Class> want = {{"phone", Class::kPhone}, {"secret", Class::kSecret}, {"plain", Class::kPlain}};
  int n = 0;
  for (const auto& [cls, names] : g_cases.at("classify").items()) {
    for (const auto& name : names) {
      EXPECT_EQ(static_cast<int>(classify(name)), static_cast<int>(want.at(cls))) << name << " should be " << cls;
      ++n;
    }
  }
  EXPECT_GT(n, 50);
}

TEST(Masking, MaskPhone) {
  for (const auto& c : g_cases.at("mask_phone")) EXPECT_EQ(mask_phone(c.at("in")), c.at("out").get<std::string>()) << c.dump();
}

TEST(Masking, MaskStructured) {
  for (const auto& c : g_cases.at("mask_structured")) EXPECT_EQ(mask_nested(c.at("in")), c.at("out")) << c.dump();
}

TEST(Masking, RoutingFieldsStayReadable) {
  // What RE logs: ids and codes stay as they are; an agent's contact URI is a phone-class name.
  for (const char* k : {"tenant_id", "interaction_id", "agent_id", "entry_id", "reservation_id", "queue_id", "replica_id", "code", "rpc"}) {
    EXPECT_EQ(classify(k), Class::kPlain) << k;
  }
  EXPECT_EQ(mask_field("agent_contact", std::string("sip:+447700900123@acme.agents.example")), "***123");
  EXPECT_EQ(mask_field("dtmf_digits", std::string("1234")), "[redacted]");
}

}  // namespace
}  // namespace pcr::pii

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (argc < 2) return 2;
  std::ifstream in(argv[1]);
  pcr::pii::g_cases = nlohmann::json::parse(in);
  return RUN_ALL_TESTS();
}
