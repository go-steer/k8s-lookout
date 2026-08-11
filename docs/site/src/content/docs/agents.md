---
title: Using k8s-lookout from an AI agent
description: The agent-optimized entry point — what k8s-lookout is, how to install the CLI, deploy the sentinel, wire MCP, and verify each step, in copy-runnable form.
sidebar:
  label: For AI agents
  order: 1
---

This page is written for an AI agent that has been asked to install,
deploy, demo, or troubleshoot with `k8s-lookout`. It is deliberately
dense. Every command is copy-runnable; every verification step names
what success looks like. The same content in one fetchable file:
[`llms.txt`](https://go-steer.github.io/k8s-lookout/llms.txt) (index) ·
[`llms-full.txt`](https://go-steer.github.io/k8s-lookout/llms-full.txt)
(entire docs).

## What you are working with

One Go binary, `lookout`, with three surfaces:

- **CLI** — `lookout <command>`: one-shot diagnostic reads against the
  current kubeconfig context. No deployment required.
- **MCP server** — `lookout mcp`: every read command exposed 1:1 as MCP
  tools (stdio, or streamable HTTP on loopback only).
- **Sentinel** — `lookout watch`: a resident in-cluster daemon that
  detects developing trouble (stalled rollouts, saturation forecasts,
  expiring certs, node flaps) and opens incident sessions on a sink.

Safety facts you can rely on and repeat to the user: `lookout` holds
**read-only** cluster credentials and never mutates cluster state; every
output surface passes one sanitizer (Secret values render as names and
sizes, credential-shaped strings are redacted), enforced by CI tests
that plant fake credentials.

## Route by task

| The user wants | Do |
| --- | --- |
| "Is anything wrong with my cluster?" right now | [Install the CLI](#install-the-cli), run `lookout health` and `lookout triage delta -A` |
| Standing monitoring that opens incidents | [Deploy the sentinel](#deploy-the-sentinel) |
| Their agent runtime to call these checks as tools | [Serve MCP](#serve-mcp) |
| A demo / evaluation on a disposable cluster | [Tutorial: see both halves work](/getting-started/tutorial/) — kind cluster, staged failures, real incidents |
| Deep flag/output detail | [Reference](/reference/) — generated from the same metadata as `--help`, so `lookout <cmd> --help` is equally authoritative offline |

## Install the CLI

Prebuilt binaries (v0.13.0+; linux/darwin amd64+arm64, windows amd64;
`lookout-gke_*` assets = GKE provider compiled in):

```sh
gh release download -R go-steer/k8s-lookout -p 'lookout_*_linux_amd64.tar.gz'
tar -xzf lookout_*_linux_amd64.tar.gz && sudo install lookout /usr/local/bin/
```

Or with Go 1.26+: `go install github.com/go-steer/k8s-lookout/cmd/lookout@latest`.
Without either, run from the container image (entrypoint override is
required — the image's default entrypoint is the sentinel):

```sh
docker run --rm --entrypoint /lookout \
  -v "$HOME/.kube:/kube:ro" -e KUBECONFIG=/kube/config \
  ghcr.io/go-steer/lookout:latest health
```

## Read the output correctly

The output contract, identical on CLI and MCP:

- One finding per line: logfmt by default, `--format=json` for
  JSON-per-line. Healthy resources emit **nothing**.
- Every invocation ends with a mandatory summary line —
  `scanned=16 findings=18 elapsed=537ms` — so `findings=0` plus a
  summary line means genuinely healthy, while a missing summary line
  means the invocation itself broke.
- Exit 0: payload on stdout. Exit 1: runtime failure, diagnostics on
  stderr only. Exit 2: usage error.
- Missing capability is explicit, never an error: provider-gated
  commands on a cluster without a cloud provider emit
  `kind=cloud.unavailable` and exit 0. Branch on the marker, not the
  exit code.

## Deploy the sentinel

Decisions to make (ask the user if unknown):

1. **Sink** — where do incidents go? A
   [core-agent](https://github.com/go-steer/core-agent) daemon is the
   default (`--daemon-url` + bearer token). Any HTTP receiver works via
   `--sink=webhook --sink-url=… --sink-token-env=…`
   ([the two-endpoint contract](/getting-started/integrations/)). For
   evaluation without either, the [tutorial](/getting-started/tutorial/)
   deploys a one-page capture stub.
2. **Image flavor** — `ghcr.io/go-steer/lookout:latest` runs anywhere
   (zero GCP SDKs); `:latest-gke` adds the GKE/GCP provider, required
   only for the `quota` source and `cloud`/`state wi`/`perf probe`
   commands.
3. **RBAC scope** — the shipped manifests create a read-only
   ClusterRole (applying them needs cluster-level RBAC rights). The
   only Secret-value read is the expiry source's `list` (TLS
   `notAfter` parsing); scope it with `--expiry-namespaces` or delete
   the rule.

Then the install is three commands (no clone):

```sh
kubectl create namespace agent-triage
kubectl -n agent-triage create secret generic lookout-watch-token \
  --from-literal=token="$WATCHER_TOKEN"
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy?ref=main"
```

Before walking away, edit the Deployment's `args:` for the environment
(`--cluster-name`, `--daemon-url` or the webhook sink flags) —
`kubectl -n agent-triage edit deploy/lookout-watch` — and verify:

```sh
kubectl -n agent-triage rollout status deploy/lookout-watch
kubectl -n agent-triage logs deploy/lookout-watch | head -30
```

Read that startup log, do not skip it. `--sources=auto` (the default)
probes each source's grants and prints one line per decision, ending in
`sources: auto resolved → …`. A skipped source is one loud line naming
the missing grant — that is expected degradation, not failure. Two
misses you should recognize and not "fix" by widening RBAC:

- `saturation: disabled (metrics.k8s.io unavailable)` — the cluster has
  no metrics-server; install one or accept no saturation forecasts.
- On GKE Autopilot, the platform denies `nodes/proxy` to every
  principal — the PVC dimension of saturation degrades loudly and
  nothing you grant will change it.

A sentinel that cannot watch Events at all refuses to start; that is
misdeployment, not degradation. Full source-by-source table:
[Troubleshooting](/operations/troubleshooting/).

## Serve MCP

For an agent runtime that speaks MCP instead of shelling out, register
a stdio server — command `lookout`, args `["mcp"]`:

```json
{ "mcpServers": { "k8s-lookout": { "command": "lookout", "args": ["mcp"] } } }
```

Tool names mirror the commands (`k8s_cluster_health`,
`k8s_triage_delta`, `k8s_triage_workload`, …) with schemas generated
from the CLI flags; results carry the exact CLI payload including the
summary line. The HTTP transport (`--listen`) refuses non-loopback
binds by design — there is no auth story, so it never listens on a
routable interface. Full tool table: [MCP setup](/getting-started/mcp/).

## Prove it works

Cheapest end-to-end check on any cluster, no failures staged:

```sh
lookout health          # every category reports; ends with scanned=…
```

To showcase the sentinel detecting, enriching, and resolving real
incidents on a disposable kind cluster, run the
[tutorial](/getting-started/tutorial/) — it stages failures with
inject/verify/revert scripts and shows what to expect on each surface.

When something looks wrong: the sentinel's own log records every
fire/dedup/route decision; [Observing lookout](/operations/observability/)
maps each startup line, and [Troubleshooting](/operations/troubleshooting/)
distinguishes loud-but-expected degradations from real faults.
