# `deploy/mcp/domain`: `DOMAIN` hostname replacement component

Kustomize `Component` copying `data.DOMAIN` from `ConfigMap/lookout-mcp` into `HTTPRoute/lookout-mcp` (`spec.hostnames.0`).

Included automatically by [`deploy/mcp/kustomization.yaml`](../kustomization.yaml). When overriding `ConfigMap/lookout-mcp` from an outer kustomization via `configMapGenerator` (`behavior: merge`), list `deploy/mcp/domain` in the outer kustomization's `components:` so the replacement runs against your merged `DOMAIN`.
