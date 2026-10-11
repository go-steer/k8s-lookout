# `internal/axapi` — Generated Agent Executor (`ax.v1alpha1`) Stubs

This package holds generated Go protobuf and gRPC client stubs (`*.pb.go` /
`*_grpc.pb.go`) for the Agent Executor (`ax.v1alpha1`) control-plane API used
by `internal/axsink`.

## Upstream Provenance

- **Canonical project:** [`github.com/google/ax`](https://github.com/google/ax)
- **Source fork & branch:** [`github.com/mastersingh24/ax`](https://github.com/mastersingh24/ax) (`task-idle-suspend`)
- **Pinned commit:** `11844ef`
- **Proto package:** `ax.v1alpha1` (`proto/ax/v1alpha1/*.proto`)

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
# From a checkout of github.com/mastersingh24/ax (or github.com/google/ax):
protoc \
  --proto_path=proto \
  --go_out=/path/to/k8s-lookout/internal/axapi \
  --go_opt=paths=source_relative \
  --go-grpc_out=/path/to/k8s-lookout/internal/axapi \
  --go-grpc_opt=paths=source_relative \
  proto/ax/v1alpha1/*.proto
```

Verify licence headers (`./dev/tools/add-license-headers`) and run
`./dev/tools/lint-go --build-tags=allproviders` and `go test ./internal/axsink/...`
after regenerating.
