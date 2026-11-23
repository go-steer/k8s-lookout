# `deploy/components/no-secrets`: withhold `Secret` & `ServiceAccount` list grants

Kustomize `Component` removing `secrets: list` and `serviceaccounts: list` from `ClusterRole/lookout-watch` (and from `Role/lookout-watch` when composed after `scope-namespaced` or `scope-namespaced-strict`), and passing `--enrich-lists=all,-secrets,-serviceaccounts` to `lookout watch`.

## Trade-offs

- Under `--sources=auto`, the `expiry` source skips itself cleanly at startup with a single log line (losing TLS/token/webhook-CA/ACME expiry checks unless restored per-namespace via `Role` + `--expiry-namespaces`).
- Enrichment bundles continue running without `403` audit noise and record `skipped=secrets,serviceaccounts` on the bundle header.

## Usage

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
components:
  # When combining with scope-namespaced or scope-namespaced-strict,
  # list the scope component BEFORE no-secrets:
  # - ../../components/scope-namespaced
  - ../../components/no-secrets
```

**Helm equivalent:** `helm install lookout-watch ./deploy/chart -n agent-triage --set rbac.secrets=false`
