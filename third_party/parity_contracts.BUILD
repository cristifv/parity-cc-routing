# Owned by parity-cc-routing: compiles parity-contracts' routing.v1 and agents.v1 protos.
load("@grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

exports_files(["VERSION"])

proto_library(
    name = "routing_proto",
    srcs = ["proto/parity/routing/v1/routing.proto"],
    strip_import_prefix = "proto",
)

cc_proto_library(
    name = "routing_cc_proto",
    deps = [":routing_proto"],
)

cc_grpc_library(
    name = "routing_cc_grpc",
    srcs = [":routing_proto"],
    grpc_only = True,
    deps = [":routing_cc_proto"],
)

proto_library(
    name = "agents_proto",
    srcs = ["proto/parity/agents/v1/agents.proto"],
    strip_import_prefix = "proto",
)

cc_proto_library(
    name = "agents_cc_proto",
    deps = [":agents_proto"],
)

cc_grpc_library(
    name = "agents_cc_grpc",
    srcs = [":agents_proto"],
    grpc_only = True,
    deps = [":agents_cc_proto"],
)

# The executable scripts (G5) and the shared test tenants, as test data.
filegroup(
    name = "routing_scripts",
    srcs = glob(["fixtures/routing.v1/**/*.json"]),
)

filegroup(
    name = "agents_scripts",
    srcs = glob(["fixtures/agents.v1/**/*.json"]),
)

filegroup(
    name = "test_tenants",
    srcs = ["tenants/test-tenants.v1.json"],
)

# routing-scenario.v1 (PLAN §7.1): the schema and the canonical scenarios.
filegroup(
    name = "routing_scenarios",
    srcs = glob(["scenarios/routing/*.json", "schemas/scenarios/*.json"]),
)

# The shared masking cases (ARCH §5.2): src/ops/pii must pass every one (CR-052).
filegroup(
    name = "masking_cases",
    srcs = ["masking/cases.v1.json"],
)
