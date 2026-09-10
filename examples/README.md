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
├── scenarios/       # one dir per failure (and per UAT fixture): README + inject / verify / revert
├── kwok/            # + hundreds of fake nodes on the same cluster: scale, bench, fleet scenarios, mass node failure
├── e2e              # driver: inject → verify → revert per scenario, PASS/FAIL summary
├── uat              # driver: the read-path half — every command's output contract
├── uat-cases/       # one file per UAT case, discovered by filename
├── uat-invocations.sh # what a valid call looks like per command, shared by the cases
├── uat-lib.sh       # UAT assertions (exit code, summary line, stdout purity, JSON) + an MCP client
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
| `probe-flap` | readiness gate flips, container never restarts | `degradation.probe_flap`, `resolved`/`object_deleted` | zero `restartCount` alongside the signal |
| `cron-missed` | CronJob's Jobs blocked by quota, schedule stalls | `workload.cron_missed`, then `resolved` | `health`; `lastScheduleTime` never advances |
| `config-storm` | shared ConfigMap deleted, four consumers break (own namespace) | 4 incidents → ONE `storm` keyed on the **ConfigMap** + `storm.member`×N | `state edges` names `shared-config` |
| `hpa-metrics-dead` | HPA on a Deployment with no cpu request (explicit) | `autoscaling.hpa_metrics_dead` after 15m sustain | `audit workloads` names it instantly; workload stays Available throughout |
| `node-failure` | worker node dies (kind-only, explicit) | `objectstate.node_notready` + ONE `storm` | `health` nodes, `triage radius` |

Each scenario's README explains the timeline, the manual-exploration
commands, and an agent-harness prompt to try against it.

`scenarios/` also holds ten **UAT fixtures** — `chatty-logs`,
`broken-edges`, `broken-webhook`, `config-drift`, `drain-blockers`,
`hpa-thrash`, `cpu-pressure`, `broken-workloads`, `secret-workload`,
`store-postmortem`.
Same three scripts, different job: nothing is expected on the wire and
`examples/e2e` skips them. They exist to give a read-path command
something to report — or, for `broken-workloads` and `secret-workload`,
to make a shared cluster answerable at all: one namespace whose
contents are known exactly, and one canary string consumed four ways.
`store-postmortem` is the odd one out twice over: it breaks nothing and
instead runs a local sentinel writing a graph history, so the `--at`
commands have a cluster state that no longer exists to answer about,
and it is the only fixture that touches the demo app (it scales `web`
2→3 inside the window and back on revert).
`cpu-pressure` is the only one
that needs more than a bare cluster (metrics-server, i.e. UAT tier T1),
and the only one whose own load has to be bounded — every container it
starts is limit-bound, and `examples/kind/up` additionally caps each
node at 4 cpu / 8g (`cap_kind_nodes`, tunable via
`LOOKOUT_KIND_NODE_CPUS` / `LOOKOUT_KIND_NODE_MEMORY`).
See [the read-path tier](#the-read-path-tier).

### Storms leak across scenarios

Three things bit `config-storm` on its first live runs and will bite
the next storm scenario the same way.

**Assert the ancestor, never just `kind=storm`.** §7.5 keys on node >
owner chain > shared ConfigMap/PVC > namespace, and four tiny pods
that all land on one node form a *node* storm that matches a bare
`kind=storm` while testing nothing you meant to test.

**An open storm swallows whatever comes next.** A folded incident's
own session is suppressed and its cross-source joins are never fanned
out, so a later scenario in the same namespace reads as a detection
miss. A storm scenario belongs in its own namespace and last in
`DEFAULT_SCENARIOS` — the node tier is not namespace-scoped, so
isolation alone does not close it.

**Deleting the objects does not close the storm.** Storm keys are
names, not object identities, and a key survives everything it was
keyed on. `Namespace//x` stays open for the 30m `stormIdleTTL` after
`x` is deleted, so recreating `x` re-attaches to the stale storm —
`StormCorrelator.Observe` checks open storms *before* it consults key
priority, so a lower tier that is already open beats a higher tier
that is not. What keeps a namespace storm alive that long here is a
`rollout_stall` on a deleted Deployment, which is never resolved
(`objectstate.onDeploymentDelete` drops the entry and emits no
clearance). A scenario that forms a storm should therefore use a
**fresh namespace name per run** rather than a fixed one; see
`scenarios/config-storm/ns.sh`. CI never sees this, because every CI
run gets a new cluster and a new sentinel — it only shows up when a
scenario is run twice against one long-lived sentinel, which is the
workflow the examples exist for.

### The resource budget a new scenario has to fit

`examples/kind/up` caps every node at 4 cpu / 8g of docker
(`cap_kind_nodes` in `lib.sh`), because two clusters of uncapped kind
nodes once took a workstation down mid-run. Two facts follow, and both
bite when writing a scenario rather than when running one:

**The cap is invisible to the scheduler.** A kind node is a container
sharing the host kernel, so cadvisor reports the *host's* cpu and
memory as node capacity — `kubectl describe node` will happily
advertise 32 cpu while the cgroup permits 4. Requests and limits are
therefore never checked against the cap. Nothing rejects an
over-budget scenario; it just runs slowly, or doesn't.

**Memory is the dimension that kills.** Over-limit cpu is throttled,
but a node whose pods exceed 8g gets the *node container* OOM-killed
by the host kernel — `--memory-swap` is pinned to the limit, so there
is no swap to escape into. That takes the kubelet with it and shows up
as `objectstate.node_notready`: a scenario that overcommits memory
does not fail, it manufactures a different signal and looks like a
product bug.

So: **bound every container's memory**, keep the sum of concurrent
memory limits per worker well inside 8g, and bound cpu with a limit
where you can. Where a missing request or limit *is* the fault —
`cpu-pressure`'s `nolimits`, `hpa-metrics-dead`'s `unscalable` — leave
unset only the exact dimension under test, set the other, and keep the
workload idle. Note that a cpu limit with no cpu request is not a
middle ground: admission defaults the request to the limit.

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
command reads the cluster directly through your kubeconfig, and the one
case that needs a sentinel's store starts its own, locally. It leaves
the demo app as it found it, so it is safe to run at any point,
including immediately after a scenario.

Mostly it only reads. The exception is the cases that need a
**fixture**. Six commands — `triage logs`, `state edges`,
`state webhooks`, `stab drift`, `stab drain`, and the HPA-thrash half of
`triage events` — have nothing to say about a healthy cluster, and a
check that only ever sees `findings=0` is not being tested
(`uat-cases/20-fixtures.sh`). `triage top` is the same problem one tier
up, in `uat-cases/50-t1.sh`: every row on an idle cluster sits near
zero, and without metrics-server there is no row at all. Two more —
`health` and `triage delta` — have the opposite problem: on a shared
cluster they report whatever the last scenario left behind, so
`uat-cases/30-root.sh` and `uat-cases/40-toplevel.sh` scope them to a
namespace they staged themselves — the same namespace the contract case
stages up front, because `findings ack` needs a finding that is
genuinely open and a healthy cluster has none. Those cases inject a
fixture scenario each, and the driver reverts them in reverse order from
its `EXIT` trap, including on failure and on Ctrl-C. Every fixture
lives in its own `lookout-uat-*` namespace and never in `lookout-demo`,
which is what makes revert a single `kubectl delete namespace` and
keeps a deliberately-wedged Deployment out of the namespace the demo
app and the scenarios share — the sole exception being
`store-postmortem` (`uat-cases/60-store.sh`), which needs a rescale on
a workload the graph feed actually tracks, so it scales `lookout-demo/web`
and is numbered last. Each also carries a negative control — an
object of the same shape that must *not* be reported — because a check
that fires on everything looks identical to one that fires correctly.
See [§ Fixtures](../docs/testing/cli-uat.md#fixtures).

The cross-cutting case (`uat-cases/00-contract.sh`) enumerates commands
from the registry via `lookout mcp --list-tools` rather than a list
kept by hand, and fails if a newly registered command has no invocation
in its table — so the coverage cannot silently rot as commands are
added. `UAT_TIER` (default `T0`) gates cases needing more than a bare
kind cluster; anything above the running tier is reported as skipped
rather than failed. The design, the per-command matrix and the tier
definitions are in [`docs/testing/cli-uat.md`](../docs/testing/cli-uat.md).

The MCP case (`uat-cases/10-mcp.sh`) replays those same invocations
through a real `lookout mcp --listen` server and compares each tool
result against the CLI's stdout, which is the strongest single check
that no command regresses its output contract — the MCP surface is how
agents consume lookout, and nothing else exercises it. It also covers
the bind rules: a non-loopback bind is refused, each opt-in flag alone
is still refused, and the one accepted off-host configuration answers
401 without the token and records tool calls to its mandatory access
log.

The store case (`uat-cases/60-store.sh`) is the post-mortem half:
everything that needs a `--store`, which is everything that can answer
about a cluster state that no longer exists. Every claim in it is a
**pair** — what the store answers, and what the live cluster answers to
the same question — because a case that only proved `--at` returns rows
would pass just as well if `--at` silently reported *now*, which is the
one wrong answer a post-mortem tool must never give. So its fixture
manufactures an object that exists only inside the window, and each
`--at` assertion has a live control that must fail to find what `--at`
finds. It also pins two known gaps as refutations naming their issues
(#393, #396), so a fix breaks the assertion instead of going unnoticed.

## CI

`.github/workflows/e2e-kind.yml` runs these scenarios non-blocking
against an image built from HEAD (`kind/up --build`): a smoke subset
(crashloop, failed-mount, bad-rollout) on every push to main, and the
full set plus node-failure weekly (or on demand via
workflow_dispatch). Both tiers then run `examples/uat` at T1 against
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
