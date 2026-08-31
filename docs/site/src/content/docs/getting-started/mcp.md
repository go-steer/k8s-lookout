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
  pod's network namespace. **Non-loopback binds are refused by
  default**:

  ```
  --listen="0.0.0.0:8383": refusing to bind a non-loopback address.
  To serve off-host, pass --allow-non-loopback together with
  --auth-token-file=<path> and --access-log=<path>; without all three
  lookout mcp is loopback-only (§4.3)
  ```

  A `lookout` reachable off-host hands its cluster read access to the
  network, so it is not something to open by accident. It is, however,
  something you can open on purpose — see below.

## Serving off-host

The one deployment shape the loopback rule blocks is the useful one:
the MCP server on one host, the agent somewhere else. That is
permitted, behind three flags that must all be present:

```sh
lookout mcp \
  --listen=0.0.0.0:8383 \
  --allow-non-loopback \
  --auth-token-file=/etc/lookout/mcp-token \
  --access-log=/var/log/lookout/mcp-access.log
```

Three rather than one, because each guards a different mistake:

- **`--allow-non-loopback`** — a token supplied for a localhost bind
  must not silently change which interface gets opened.
- **`--auth-token-file`** — a bind flag must not open an
  unauthenticated cluster-read API.
- **`--access-log`** — on loopback the log is a debugging convenience;
  off-host it is the only evidence that exists of who called what.

The token is a single shared bearer token, compared in constant time
against every request's `Authorization: Bearer <token>` header; anything
else gets a bare `401` that says nothing about what is behind it. The
file may have a trailing newline, must be one line, and must be at
least 16 characters — generate one with `head -c 32 /dev/urandom |
base64`. Permissions on the file are not checked, because the obvious
way to supply it in-cluster is a Secret volume and those mount `0644`.

Startup says what it did, on stderr:

```
lookout mcp: serving MCP over HTTP on 0.0.0.0:8383 — REACHABLE OFF-HOST.
lookout mcp: bearer-token authentication is REQUIRED; every call is recorded to /var/log/lookout/mcp-access.log.
```

**What this is not.** There is no authorization: every caller
presenting the token gets the full advertised tool surface. Narrow it
with [`--profile`](#profiles-dont-advertise-what-the-agent-will-never-call)
if a caller should not reach everything. mTLS is out of scope — it is
the right answer for a production deployment and a much larger piece of
work (cert distribution, rotation, a CA story).

## Wiring into a core-agent daemon

Register `lookout mcp` in the daemon's MCP server configuration as a
stdio server (spawned command: `lookout`, args: `["mcp"]`), or — when the
daemon image cannot exec at all — run `lookout` as a second container in
the daemon's pod with `--listen=127.0.0.1:<port>` and register the HTTP
endpoint. In-cluster, the pod's ServiceAccount needs read RBAC covering
the checks you expect agents to call; the sentinel's shipped ClusterRole
(`deploy/12-clusterrole-watcher.yaml`) is a working superset for the
common ones.

## Profiles: don't advertise what the agent will never call

Every advertised tool is paid for on **every** model call, whether or
not it is ever invoked. The full surface is over 130 KB of JSON schema
— roughly 35k tokens per turn — and the cost is not only tokens: an
agent choosing among thirty similar-sounding tools chooses worse than
one choosing among seven.

So the surface is selectable. The default is unchanged — every command,
for every client that asks for nothing — and the saving is opt-in:

```sh
lookout mcp --profile=triage              # the incident surface
lookout mcp --profile=audit               # the posture surface
lookout mcp --tools=all,-k8s_perf_probe   # everything but one tool
lookout mcp --profile=triage --tools=-k8s_triage_logs

lookout mcp --list-tools                  # what a selection costs, per tool
```

`--profile` and `--tools` are one left-to-right selection, `--profile`
first, in the same `all,-x` syntax as `bundle --lists`: `all` (or
`full`) adds every tool, a profile name adds its members, a tool name
adds one, and a `-` prefix removes. A selection that resolves to zero
tools is a usage error — a server with an empty tool list is
indistinguishable from a missing one.

`lookout mcp --help` prints the profiles with their sizes; each
command's Reference page names the profiles it belongs to. Membership
is declared on the command itself, so a new check joins a profile in
the same edit that creates it.

## The access log

`lookout mcp` is silent by default: it writes nothing but protocol
frames, so when an agent's tool call misbehaves there is no record it
happened at all. `--access-log` fixes that with one logfmt line per
call:

```sh
lookout mcp --access-log=/var/log/lookout/mcp-access.log
```

```
ts=2026-08-18T14:03:21Z tool=k8s_scan exit=0 dur=1.204s bytes=4096
ts=2026-08-18T14:03:29Z tool=k8s_triage_logs exit=1 dur=312ms bytes=118
ts=2026-08-18T14:03:33Z tool=k8s_triage_logs exit=2 dur=0s bytes=64
```

`exit` is the §4.2 code the tool call mapped from — `0` a payload, `1`
a tool error the model can see, `2` a rejected argument. Calls the
schema layer rejects before the command ever runs are logged too;
those are exactly the ones worth noticing.

The file is created if absent, **appended** if present (a supervisor
restart must not erase the evidence from the run that caused it), and
created mode `0600` — the tool names alone say which clusters an
operator has been reading. If the path cannot be opened, `lookout mcp`
exits 2 rather than serving without a log. It is optional on loopback
and **mandatory** for an off-host bind.

What a line deliberately does *not* carry is the arguments or the
response body. The [sanitizer](/concepts/sanitization/) guarantees
no secret value reaches an output surface; a log that copied payloads
would be a second place that guarantee has to hold, audited by nobody.
Tool, outcome, and size answer the operational questions — what was
called, did it work, what did it cost — without becoming a second data
path.

## The tools

Tool names are the commands' MCP names — `triage delta` →
`k8s_triage_delta`, `bundle` → `k8s_triage_workload` — with input
schemas mirrored from the CLI flags (plus a `target` property where a
command takes a positional argument).

Two conveniences for clients that guess: on every other tool `target`
is accepted as a synonym for `workload`, and an argument name the tool
does not know is rejected with the nearest one it does —
`unknown argument "form" for tool k8s_scan; did you mean "format"?
(accepts: …)`. Only the canonical names appear in the schemas.

The current surface:

| Tool | Command |
| --- | --- |
| `k8s_scan` | [`scan`](/reference/scan/) |
| `k8s_cluster_health` | [`health`](/reference/health/) |
| `k8s_triage_workload` | [`bundle`](/reference/bundle/) |
| `k8s_list_resources` | [`triage list`](/reference/triage-list/) |
| `k8s_triage_delta` | [`triage delta`](/reference/triage-delta/) |
| `k8s_triage_logs` | [`triage logs`](/reference/triage-logs/) |
| `k8s_event_timeline` | [`triage events`](/reference/triage-events/) |
| `k8s_resource_top` | [`triage top`](/reference/triage-top/) |
| `k8s_blast_radius` | [`triage radius`](/reference/triage-radius/) |
| `k8s_recent_changes` | [`triage changes`](/reference/triage-changes/) |
| `k8s_resource_spec` | [`triage spec`](/reference/triage-spec/) |
| `k8s_triage_status` | [`triage status`](/reference/triage-status/) |
| `k8s_findings_diff` | [`findings diff`](/reference/findings-diff/) |
| `k8s_findings_ack` | [`findings ack`](/reference/findings-ack/) |
| `k8s_state_edges` | [`state edges`](/reference/state-edges/) |
| `k8s_admission_webhooks` | [`state webhooks`](/reference/state-webhooks/) |
| `k8s_workload_identity` | [`state wi`](/reference/state-wi/) |
| `k8s_volume_conflicts` | [`state volumes`](/reference/state-volumes/) |
| `k8s_storage_binding` | [`state storage`](/reference/state-storage/) |
| `k8s_gateway_routes` | [`state gateway`](/reference/state-gateway/) |
| `k8s_gitops_drift` | [`stab drift`](/reference/stab-drift/) |
| `k8s_drain_blockers` | [`stab drain`](/reference/stab-drain/) |
| `k8s_perf_probe` | [`perf probe`](/reference/perf-probe/) |
| `k8s_cloud_stockout` | [`cloud stockout`](/reference/cloud-stockout/) |
| `k8s_cloud_orphans` | [`cloud orphans`](/reference/cloud-orphans/) |
| `k8s_cloud_ipspace` | [`cloud ipspace`](/reference/cloud-ipspace/) |
| `k8s_cloud_quota` | [`cloud quota`](/reference/cloud-quota/) |
| `k8s_net_probe` | [`net probe`](/reference/net-probe/) |
| `k8s_audit_workloads` | [`audit workloads`](/reference/audit-workloads/) |
| `k8s_audit_hardening` | [`audit hardening`](/reference/audit-hardening/) |
| `k8s_audit_netpol` | [`audit netpol`](/reference/audit-netpol/) |
| `k8s_audit_cluster` | [`audit cluster`](/reference/audit-cluster/) |
| `k8s_audit_upgrades` | [`audit upgrades`](/reference/audit-upgrades/) |
| `k8s_audit_exemptions` | [`audit exemptions`](/reference/audit-exemptions/) |

Commands added later become tools with no extra wiring — the list the
server serves, its schemas and its descriptions are generated from the
same command metadata as `--help` and this site's Reference section, so
`tools/list` is always the authoritative surface. The table above is a
hand-maintained copy of it, held to the registry by a test.
Tool descriptions are written as micro-skills ("when to reach for
this"), which is most of what an agent needs; the workflow-level
decision tree ships as skills in
[`skills/`](https://github.com/go-steer/k8s-lookout/tree/main/skills).

The loop is smoke-tested end to end in a live drill: `initialize` →
`tools/list`, then `tools/call k8s_cluster_health` returned
`isError:false` with the identical scorecard payload the CLI prints,
summary line included.
