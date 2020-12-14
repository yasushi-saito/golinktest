load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["status.go"],
    importpath = "github.com/yasushi-saito/golinktest",
    visibility = ["//visibility:public"],
    deps = [
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
        "@org_golang_google_protobuf//types/known/anypb:go_default_library",
    ],
)

load("@bazel_gazelle//:def.bzl", "gazelle")

# gazelle:prefix github.com/yasushi-saito/golinktest
gazelle(
    name = "gazelle",
    extra_args = ["-build_file_name=BUILD"],
)
