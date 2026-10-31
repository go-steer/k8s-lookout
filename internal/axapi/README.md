# `internal/axapi` — Generated Agent Executor (`ax.v1alpha1`) Stubs

This package holds generated Go protobuf and gRPC client stubs (`*.pb.go` /
`*_grpc.pb.go`) for the Agent Executor (`ax.v1alpha1`) control-plane API used
by `internal/axsink`.

## Upstream Provenance

- **Canonical project:** [`github.com/google/ax`](https://github.com/google/ax)
- **Source fork & tag:** [`github.com/mastersingh24/ax`](https://github.com/mastersingh24/ax) (`substrate-v1-12dbe16`, on google/ax `f0228f1`)
- **Pinned commit:** `12dbe16`
- **Proto package:** `ax.v1alpha1` (`pkg/apis/v1alpha1/ax.proto`)
- **Fork-only fields:** `TaskSpec.egress` = 100, `http` = 101, `idle` = 102,
  `on_completion` = 103. These must match the AX server: lookout v0.42 and
  earlier carry the pre-rebase numbers (7–10), which the rebased server reads
  as `config` and unknown fields.

## Why Stubs Are Vendored Under `internal/`

1. **Zero public API surface leak:** `pkg/inject` and `pkg/emit` are standalone
   embeddable libraries with zero dependency on gRPC or Agent Substrate protos.
   Isolating generated stubs under `internal/axapi` guarantees external Go
   importers never inherit AX gRPC dependencies.
2. **Hermetic builds without unpublished upstream Go modules:** Upstream AX
   proto definitions live in an application repository rather than a standalone
   versioned Go module with SemVer stability guarantees. Generating the minimal
   `ax.v1alpha1` client stubs directly into `internal/axapi` keeps `go build`
   and CI presubmits completely hermetic.

## Regenerating Stubs

When pulling upstream proto changes from `ax`:

```bash
# Copy the proto from the pinned ax tag, keeping the vendoring comment at
# the top of ax.proto and changing only go_package:
git -C /path/to/ax show <tag>:pkg/apis/v1alpha1/ax.proto > /tmp/ax.proto
#   ...edit go_package to github.com/go-steer/k8s-lookout/internal/axapi...
go generate ./internal/axapi   # buf generate + goimports (generate.go)
```

Verify licence headers (`./dev/tools/add-license-headers`) and run
`./dev/tools/lint-go --build-tags=allproviders` and `go test ./internal/axsink/...`
after regenerating.
