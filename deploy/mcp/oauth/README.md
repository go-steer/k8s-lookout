# `deploy/mcp/oauth`: generic MCP client OAuth server component

Optional Kustomize `Component` on [`deploy/mcp/`](../README.md) enabling `lookout mcp`'s built-in OAuth authorization server (`--oauth-issuer=https://$(DOMAIN)`) so generic MCP clients (`Claude Code`, `gemini-cli`, IDEs) can sign in alongside Gemini Enterprise on the same endpoint.

## Prerequisites

1. `Secret/lookout-oauth-client` (`client.json` — Google OAuth Web client JSON including `https://<DOMAIN>/callback` as an authorized redirect URI).
2. `Secret/lookout-oauth-keys` (`keyring` — `<id> <base64-of-32-random-bytes>`).
3. `OAUTH_CUSTODY` (`vault` or `sealed`) and, for `vault`, `OAUTH_VAULT_AUTH_PROVIDER` set in `ConfigMap/lookout-mcp`.

**Helm equivalent:** `helm install lookout-mcp ./deploy/chart -n agent-triage --set mode=mcp --set mcp.oauth.enabled=true`
