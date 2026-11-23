# `deploy/mcp`: standalone `lookout mcp` Gateway deployment

Stateless `lookout mcp` HTTP deployment behind a GKE external HTTPS Gateway (`gke-l7-global-external-managed`), running on the `-gke` provider image with zero cluster RBAC on `ServiceAccount/lookout-mcp`: every tool call authenticates and executes with the signed-in caller's own Google OAuth token.

## Contents

- `config.yaml` — `ConfigMap/lookout-mcp` (`DOMAIN`, `GOOGLE_CLIENT_ID`, `GOOGLE_ALLOW`, `GOOGLE_ALLOW_WRITE`, `GOOGLE_GROUPS_SERVICE_ACCOUNT`, `CLUSTERS_FROM`, `OAUTH_CUSTODY`, `OAUTH_VAULT_AUTH_PROVIDER`)
- `mcp.yaml` — `ServiceAccount/lookout-mcp`, zone-spread `Deployment/lookout-mcp`, and `Service/lookout-mcp`
- `gateway.yaml` — `Gateway`, `HTTPRoute`, `HealthCheckPolicy`, and `GCPBackendPolicy` (`300s` backend timeout)
- [`domain/`](./domain/README.md) — `Component` propagating `ConfigMap/lookout-mcp` `data.DOMAIN` into `HTTPRoute` `spec.hostnames[0]`
- [`oauth/`](./oauth/README.md) — optional `Component` enabling `--oauth-issuer` for generic MCP clients (`Claude Code`, `gemini-cli`, IDEs)

## Usage

```sh
# Kustomize:
kubectl apply -k deploy/mcp/
# Helm equivalent (parity-checked in CI):
helm install lookout-mcp ./deploy/chart -n agent-triage --create-namespace --set mode=mcp
```
