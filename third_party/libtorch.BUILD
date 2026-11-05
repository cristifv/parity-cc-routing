# CPU libtorch 2.14.1 (MODULE.bazel; PLAN §5.9): the C++ frontend's headers and the three shared
# libraries the forecaster needs (libtorch_cpu, libc10, OpenMP). No libtorch_python, no -lpython3,
# no test or backend libraries. Shared libraries in one cc_library land in one _solib directory, so
# libtorch_cpu's $ORIGIN runpath finds libc10 and libgomp there.
load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "torch",
    srcs = [
        "lib/libc10.so",
        "lib/libgomp.so.1",
        "lib/libtorch.so",
        "lib/libtorch_cpu.so",
    ],
    hdrs = glob(
        [
            "include/**/*.h",
            "include/**/*.hpp",
            "include/**/*.cuh",
        ],
        allow_empty = True,
    ),
    includes = [
        "include",
        "include/torch/csrc/api/include",
    ],
    linkopts = ["-lpthread"],
)
