# examples/ — end-to-end scenarios on a real cluster

Everything needed to see lookout work against **real workloads and
real failures**: a kind cluster recipe, the sentinel wired to a
capture stub (no core-agent required), a small demo app, and ten
inject/verify/revert failure scenarios that drive both halves of the
binary — the sentinel's push path and the read-path CLI.

This sits between the unit/contract suites (CI) and the human-run
[`dev/drills/`](../dev/drills/) runbooks (real GKE, store forensics,
post-mortems): automated enough for `examples/e2e` to pass/fail in
minutes, real enough that every asserted payload crossed a live
apiserver and a live wire.

## Layout

```
examples/
├── kind/            # cluster.yaml + up/down (metrics-server, image pre-pull, --build)
├── sentinel/        # sentinel + stub daemon deploy (uses deploy/ manifests unmodified)
├── workloads/       # the lookout-demo baseline app: web, api(+PDB), worker, vantage
├── scenarios/       # one dir per failure: README + inject / verify / revert
├── kwok/            # + hundreds of fake nodes on the same cluster: scale, bench, fleet scenarios, mass node failure
├── e2e              # driver: inject → verify → revert per scenario, PASS/FAIL summary
├── uat              # driver: the read-path half — every command's output contract
├── uat-cases/       # one file per UAT case, discovered by filename
├── uat-lib.sh       # UAT assertions (exit code, summary line, stdout purity, JSON)
├── lib.sh           # shared helpers (context guard, wire/read-path await)
├── agent-harness.md # testing the CLI via skills / MCP in Claude, core-agent, etc.
└── gke/             # deltas for running the same scenarios on a GKE staging cluster
```

## Prerequisites

`docker`, `kind`, `kubectl`; `go` 1.26+ **or** a `lookout` binary on
PATH (set `LOOKOUT_BIN` to override); `openssl` for the cert-expiry
scenario.

## Quickstart

```sh
examples/kind/up                          # cluster + metrics-server (+ --build for a from-HEAD image)
examples/sentinel/up                      # RBAC + sentinel + capture stub (Service core-agent:7777)
kubectl apply -f examples/workloads/      # the demo app the scenarios break
examples/e2e                              # all non-destructive scenarios, ~20 minutes
```

Or drive one failure by hand and watch each surface:

```sh
kubectl -n agent-triage logs deploy/stub-daemon -f &   # the wire: SESSION-CREATE / INJECT lines
examples/scenarios/bad-rollout/inject
lookout health                                         # read-path, from your workstation
examples/scenarios/bad-rollout/verify
examples/scenarios/bad-rollout/revert                  # waits for the closed-loop kind=resolved
```

Three surfaces to verify on, weakest to strongest:

1. **Sentinel log** — `kubectl -n agent-triage logs deploy/lookout-watch`
   (fire/route/dedup decisions, startup source probes).
2. **Read-path CLI** — every command works from your kubeconfig with
   no deployment; `verify` scripts poll these too.
3. **The wire** — the stub daemon logs every `POST /sessions` and
   inject body verbatim; this is what a real core-agent would receive,
   schema-frozen per [docs/signal-schema-v1.md](../docs/signal-schema-v1.md).

## Scenario matrix

| Scenario | What breaks | Expected on the wire | Read-path proof |
| --- | --- | --- | --- |
| `crashloop` | worker exits on start | `k8s-event` (BackOff family) | `triage events`, later `health` crashloops |
| `image-pull` | api rolled to nonexistent tag | `k8s-event` (ImagePull family, one fingerprint) | `triage events`, `triage changes` |
| `failed-mount` | pod mounts missing ConfigMap | `k8s-event` FailedMount | `state edges` names the broken ref |
| `oom` | memory leak vs 64Mi limit | `k8s-event` BackOff / `objectstate.restart_burst` | `triage events`, `triage logs` |
| `pending` | 64-CPU request, unschedulable | `capacity.pending-aged` | `health` pending category |
| `cert-expiry` | TLS secret expiring in 48h | `expiry.warning` (critical, <72h) | `health` certs category |
| `pdb-gridlock` | PDB headroom drops to 0 | `objectstate.pdb_gridlocked` | `stab drain -A` |
| `endpoints-empty` | Service selector matches nothing | `objectstate.endpoints_empty` | `state edges` selected=0 |
| `bad-rollout` | user-invisible bad deploy (maxUnavailable=0) | `rollout.stall`, then `resolved` on undo | `health` rollouts + 5×200 mid-stall |
| `node-failure` | worker node dies (kind-only, explicit) | `objectstate.node_notready` + ONE `storm` | `health` nodes, `triage radius` |

Each scenario's README explains the timeline, the manual-exploration
commands, and an agent-harness prompt to try against it.

## The scale tier

Every scenario above runs on two real workers and ~10 pods, which is
the right size for asserting *what* lookout reports and no size at all
for asserting what it costs — or for asserting what lookout stays
*quiet* about, since a cluster with one PDB in it has no sound
lookalikes to leave alone. [`kwok/`](kwok/) adds hundreds of fake nodes
to the same cluster — kubelets simulated, control plane real — so the
read path can be timed at 300 nodes, thirty nodes can lose their
kubelet in the same second, and a fault can be planted in a haystack
that is actually a haystack:

```sh
examples/kwok/up                    # kwok controller, annotation-scoped to fake nodes
examples/kwok/scale-up 300 400 3    # 300 fake nodes, 400 workloads, 1000 pods
examples/kwok/bench                 # every read command, against its own --timeout default
examples/kwok/e2e                   # fleet-scale scenarios: inject → verify → revert
examples/kwok/node-fail 30          # the storm drill kind cannot run
examples/kwok/down                  # remove the layer; the real cluster is untouched
```

It is additive, not a replacement: kubelet-observed event grammar
(`BackOff`, `ErrImagePull`, `FailedMount`, `OOMKilled`) is not faithful
on a simulated kubelet, so those scenarios stay on kind. See
[`kwok/README.md`](kwok/README.md) for the full split.

## The read-path tier

`examples/e2e` asks whether breaking a workload produces the right
signal on the wire. `examples/uat` asks the other half: does every
read-path command return correct, well-shaped, secret-safe output?

```sh
examples/uat                # every case this tier can run
examples/uat contract       # one case file
UAT_TIER=T1 examples/uat    # also the cases that need metrics-server
```

It needs the cluster and the demo app, but **not** a sentinel — every
command reads the cluster directly through your kubeconfig. It breaks
nothing and reverts nothing, so it is safe to run at any point,
including immediately after a scenario.

The cross-cutting case (`uat-cases/00-contract.sh`) enumerates commands
from the registry via `lookout mcp --list-tools` rather than a list
kept by hand, and fails if a newly registered command has no invocation
in its table — so the coverage cannot silently rot as commands are
added. `UAT_TIER` (default `T0`) gates cases needing more than a bare
kind cluster; anything above the running tier is reported as skipped
rather than failed. The design, the per-command matrix and the tier
definitions are in [`docs/testing/cli-uat.md`](../docs/testing/cli-uat.md).

## CI

`.github/workflows/e2e-kind.yml` runs these scenarios non-blocking
against an image built from HEAD (`kind/up --build`): a smoke subset
(crashloop, failed-mount, bad-rollout) on every push to main, and the
full set plus node-failure weekly (or on demand via
workflow_dispatch). Both tiers then run `examples/uat` at T0 against
the same cluster. PR presubmits stay hermetic — a live cluster never
gates a PR. CI sets `LOOKOUT_E2E_TIMEOUT_SCALE=2` because runners are
slower than a workstation; set it locally if your machine needs more
headroom too.

## Re-runs, dedup, and storm absorption

The sentinel is doing its job when a re-run looks quieter than the
first run: repeats inside `--dedup-window` dedup into the existing
session, a recurrence shortly after a `resolved` arrives as
`kind=resolved.reverted`, and when several scenarios crash pods on the
same node inside the storm window, later incidents attach to the open
`kind=storm` session (or are suppressed as already-attached members)
instead of opening fresh ones — the M2 "1 storm session, not 30"
behavior. The verify scripts accept these routings where they're
likely, but if a scenario fails on the wire check right after a
previous run, let the cluster settle (~10 minutes) and read the
sentinel's own log — it records every fire/dedup/attach decision:

```sh
kubectl -n agent-triage logs deploy/lookout-watch
```

## Safety

Every script refuses to run unless the current kubectl context is the
examples kind cluster. To use a **staging** cluster instead, set
`LOOKOUT_EXAMPLES_CONTEXT=<context-name>` explicitly — see
[`gke/README.md`](./gke/README.md), and never point it at a cluster
users depend on: the scenarios break workloads on purpose.

## Testing lookout through an agent harness

The scenarios double as fixtures for skills/MCP testing — inject one,
then hand the investigation to Claude Code, core-agent, Antigravity,
or anything that speaks MCP, and compare its findings against
`verify`'s. Setup and per-scenario prompts:
[`agent-harness.md`](./agent-harness.md).

## What this does NOT cover

The parts that need a real cloud or a human judgment call stay in
[`dev/drills/`](../dev/drills/): GKE quota exhaustion (`-gke` image),
store-backed post-mortems (`--at` time travel), the saturation
forecast's slow ramp (run the oom scenario's leaker at default speed —
see its README), and wire-capture corpus harvesting. The docs site's
operations/drills page covers when to run those.

## Cleanup

```sh
examples/kind/down            # deletes the cluster and cached state
```
