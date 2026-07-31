---
title: MCP setup
description: lookout mcp — every read command as an MCP tool over stdio or loopback HTTP, and why the HTTP transport is loopback-only.
sidebar:
  order: 7
---

Every read-path command is also exposed 1:1 as an MCP tool:

```sh
lookout mcp                        # stdio: JSON-RPC on stdin/stdout
lookout mcp --listen=127.0.0.1:8383   # streamable HTTP on a loopback address
```

This exists because of a hard-learned constraint: distroless images kill
`bash + curl`. A distroless core-agent daemon has no shell to run
`lookout triage delta` in — MCP is how it calls the same checks
natively. The server uses the same kube client bootstrap as the CLI
(kubeconfig outside a pod, the ServiceAccount inside one), and tool
results carry the exact payload the CLI prints: logfmt findings
terminated by the `scanned=/findings=/elapsed=` summary line, passed
through the same sanitizer.

## Transports

- **stdio (default)** — the transport a daemon uses when it spawns
  `lookout mcp` as a child process. Diagnostics go to stderr only.
- **`--listen=<host:port>`** — streamable HTTP, for the same-pod case
  where the daemon and `lookout` run as separate containers sharing the
  pod's network namespace. **Non-loopback binds are refused**:

  ```
  --listen="0.0.0.0:8383": refusing to bind a non-loopback address:
  lookout mcp has no auth; only 127.0.0.1, ::1, or localhost are allowed (§4.3)
  ```

  The rationale is exactly what the error says: the MCP server carries
  no authentication story, and a `lookout` reachable off-host would hand
  its cluster read access to the network. It never listens on a
  routable interface.

## Wiring into a core-agent daemon

Register `lookout mcp` in the daemon's MCP server configuration as a
stdio server (spawned command: `lookout`, args: `["mcp"]`), or — when the
daemon image cannot exec at all — run `lookout` as a second container in
the daemon's pod with `--listen=127.0.0.1:<port>` and register the HTTP
endpoint. In-cluster, the pod's ServiceAccount needs read RBAC covering
the checks you expect agents to call; the sentinel's shipped ClusterRole
(`deploy/12-clusterrole-watcher.yaml`) is a working superset for the
common ones.

## The tools

Tool names are the commands' MCP names — `triage delta` →
`k8s_triage_delta`, `bundle` → `k8s_triage_workload` — with input
schemas mirrored from the CLI flags (plus a `target` property where a
command takes a positional argument). The current surface:

| Tool | Command |
| --- | --- |
| `k8s_cluster_health` | [`health`](/reference/health/) |
| `k8s_triage_workload` | [`bundle`](/reference/bundle/) |
| `k8s_triage_delta` | [`triage delta`](/reference/triage-delta/) |
| `k8s_triage_logs` | [`triage logs`](/reference/triage-logs/) |
| `k8s_event_timeline` | [`triage events`](/reference/triage-events/) |
| `k8s_resource_top` | [`triage top`](/reference/triage-top/) |
| `k8s_blast_radius` | [`triage radius`](/reference/triage-radius/) |
| `k8s_recent_changes` | [`triage changes`](/reference/triage-changes/) |
| `k8s_resource_spec` | [`triage spec`](/reference/triage-spec/) |
| `k8s_triage_status` | [`triage status`](/reference/triage-status/) |
| `k8s_state_edges` | [`state edges`](/reference/state-edges/) |
| `k8s_admission_webhooks` | [`state webhooks`](/reference/state-webhooks/) |
| `k8s_workload_identity` | [`state wi`](/reference/state-wi/) |
| `k8s_volume_conflicts` | [`state volumes`](/reference/state-volumes/) |
| `k8s_gitops_drift` | [`stab drift`](/reference/stab-drift/) |
| `k8s_drain_blockers` | [`stab drain`](/reference/stab-drain/) |
| `k8s_perf_probe` | [`perf probe`](/reference/perf-probe/) |
| `k8s_cloud_stockout` | [`cloud stockout`](/reference/cloud-stockout/) |
| `k8s_cloud_orphans` | [`cloud orphans`](/reference/cloud-orphans/) |
| `k8s_cloud_ipspace` | [`cloud ipspace`](/reference/cloud-ipspace/) |
| `k8s_cloud_quota` | [`cloud quota`](/reference/cloud-quota/) |
| `k8s_net_probe` | [`net probe`](/reference/net-probe/) |

Commands added later appear automatically — the tool list, schemas, and
descriptions are generated from the same command metadata as `--help`
and this site's Reference section, so the surfaces cannot drift apart.
Tool descriptions are written as micro-skills ("when to reach for
this"), which is most of what an agent needs; the workflow-level
decision tree ships as skills in
[`skills/`](https://github.com/go-steer/k8s-lookout/tree/main/skills).

The loop is smoke-tested end to end in a live drill: `initialize` →
`tools/list`, then `tools/call k8s_cluster_health` returned
`isError:false` with the identical scorecard payload the CLI prints,
summary line included.
