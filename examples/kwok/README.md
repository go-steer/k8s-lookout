# examples/kwok/ — the scale tier

[KWOK](https://github.com/kubernetes-sigs/kwok) simulates kubelets. A
fake node is a `v1.Node` object the kwok controller heartbeats for; a
pod scheduled onto one is fast-forwarded to `Running`/`Ready` by a
lifecycle Stage. No container runtime, no image pull, no cgroup —
which makes a node cost roughly one etcd object and a lease renewal,
so a laptop holds hundreds of them.

This directory installs kwok **into the existing kind cluster** from
[`../kind/`](../kind/) and grows a synthetic fleet on top of it. It is
additive: the three real kind nodes keep their real kubelets, the
sentinel and the demo app keep running on them, and every script in
[`../scenarios/`](../scenarios/) behaves exactly as before.

```sh
examples/kind/up                    # the real cluster, as always
examples/sentinel/up                # sentinel + wire capture, as always
examples/kwok/up                    # + the kwok controller
examples/kwok/scale-up 300 400 3    # + 300 fake nodes, 400 workloads, 1000 pods
examples/kwok/bench                 # what the read path costs at that size
examples/kwok/e2e                   # inject → verify → revert, fleet-scale scenarios
examples/kwok/node-fail 30          # 30 nodes lose their kubelet at once
examples/kwok/node-heal             # give them back
examples/kwok/down                  # remove the whole layer
```

For **10k–200k+ pod runs** ([#495](https://github.com/go-steer/k8s-lookout/issues/495)), where `kind` hits etcd's 2 GiB quota, default controller QPS limits, and `/16` pod-CIDR exhaustion around ~8,800 S8-padded pods, `kwokctl-run` drives the entire benchmark end-to-end against a standalone tuned `kwokctl --runtime=binary` control plane (no Docker, no root, no DaemonSet overhead) using an isolated kubeconfig (`$STATE_DIR/kwokctl.kubeconfig`, never touching `~/.kube/config`):

```sh
examples/kwok/kwokctl-run                 # one-click: 2,000 nodes, 5,000 workloads, 100,500 S8-padded pods
examples/kwok/kwokctl-run 4000 5000 50    # full design target: 4,000 nodes, 5,000 workloads, 200,500 pods (auto 32 GiB etcd)
examples/kwok/kwokctl-run 400 1000 10     # quick tier (~9,100 pods)
```

Or step by step against the same isolated control plane:

```sh
KWOKCTL_ETCD_QUOTA=32Gi examples/kwok/kwokctl-up  # tuned etcd + QPS-unlimited apiserver/controllers + 64 kwok pod workers
examples/kwok/leeway-scale                        # rung 0 (empty cluster intercept)
examples/kwok/scale-up 4000 5000 50               # 4,000 fake nodes, 5,000 workloads, 200,500 S8-padded pods
examples/kwok/leeway-scale                        # quiet 200.5k-pod rung + marginal KiB/pod verdict
examples/kwok/bench                               # all 26 read-path commands at 200.5k pods
LEEWAY_SCALE_CHURN=24 examples/kwok/leeway-scale  # ~1,580 pod lifecycles/s rollout churn + #491 watch-event breakdown
examples/kwok/kwokctl-down                        # tear down cluster & remove isolated kubeconfig
```

Every script in `examples/kwok/` auto-selects `$STATE_DIR/kwokctl.kubeconfig` while that cluster is active and reverts cleanly after `kwokctl-down`.

## What this tier is for, and what it is not for

The split that matters is **kubelet versus control plane**. A kwokctl
or in-cluster kwok setup runs a real API server, a real
kube-controller-manager and a real kube-scheduler. Only the kubelet is
simulated. So:

**Faithful here.** Anything a controller decides. Scheduling and
unschedulability, ReplicaSet and Deployment progress, EndpointSlice
membership, PodDisruptionBudget status, Job and CronJob activation,
node lifecycle after a lease goes stale. These are the same
controllers running the same code as on a real cluster.

**Not faithful here.** Anything a kubelet observes. `BackOff`,
`ErrImagePull`, `FailedMount`, `OOMKilled` — the event grammar
`crashloop`, `image-pull`, `failed-mount` and `oom` are built on. You
*can* author a kwok Stage that emits those events, but then the
scenario asserts that lookout matches the event you already believed
kubelet emits. That tests the fixture, not the product. Those four
scenarios stay on kind, where a real kubelet produces the real string.

**Container logs are the interesting middle case.** They are also
authored — kwok serves a file off the controller's filesystem — but
what the `logs` scenario asserts is not "lookout recognises a Java
stack trace". It is the *reduction*: forty-eight streams collapsing to
a handful of templates that count their pods. The input being synthetic
does not weaken that claim, because the claim is about the arithmetic
on the way out, not the string on the way in. See
[`scenarios/logs`](scenarios/logs/).

So this is not a cheaper kind. It is a second tier that answers
questions kind cannot answer at all:

| Question | Why kind can't | Here |
| --- | --- | --- |
| What does `audit workloads -A` cost across 400 workloads? | 2 workers, ~10 pods | `bench` |
| Does `health` survive its own 10s `--timeout` default on a big cluster? | never gets big | `bench` |
| When 30 nodes fail at once, is that one incident or thirty? | 2 workers; killing one is destructive and slow | `node-fail 30`, `scenarios/node-storm` (this one found #334) |
| How does the sentinel's informer set behave at 1000+ pods? | never gets there | `scale-up` |
| Does `audit netpol` correctly separate covered from uncovered namespaces? | one namespace | 9, in three tiers |

## Safety: how the fake and the real stay apart

Three mechanisms, and each is checked rather than assumed:

1. **The controller is annotation-scoped.** kwok's shipped config sets
   `manageNodesWithAnnotationSelector: kwok.x-k8s.io/node=fake`, so it
   only ever writes status onto nodes that opt in. `up` asserts this
   after installing and refuses to leave the controller running if the
   setting is missing — a controller that managed *all* nodes would
   start fighting the real kind kubelets.
2. **Every fake node is tainted** `kwok.x-k8s.io/node=fake:NoSchedule`,
   and only generated pods tolerate it. Nothing real is ever scheduled
   onto a node with no kubelet behind it.
3. **Every fake pod is pinned** to `type: kwok` by nodeSelector, so the
   fleet never displaces the sentinel or the demo app off the real
   workers.

`scale-down` deletes by those same selectors — fleet-labelled
namespaces and `type=kwok` nodes — so there is no path from it to a
real node. `node-fail` refuses to touch anything that is not a fake
node at all.

### Where "additive" is not free

Keeping the fake fleet *scheduled* apart from the real one is the easy
half. The hard half is that the real cluster's own components watch
every node in the cluster, and cannot be told not to.

**A fake node's InternalIP has two consumers that want different
things.** Left alone, kwok reports its own controller-pod IP (a
`10.244.x` pod address) as the InternalIP of *every* fake node. kindnet
then tries to install `10.244.<n>.0/24 via <that pod IP>`, and Linux
rejects a gateway that is not on a directly-connected subnet:

```
Failed to reconcile routes, retrying after error: network is unreachable
panic: Maximum retries reconciling node routes: network is unreachable
```

That is the real CNI on the real nodes dying, and it happens with one
fake node as surely as with three hundred. So kindnet wants an
**on-link** address. But the apiserver wants a **reachable** one: it
dials `InternalIP:10247` — the port from
`daemonEndpoints.kubeletEndpoint` — for `kubectl logs`, `exec` and
`attach`, and an address nobody listens on gives

```
dial tcp 172.17.128.85:10247: connect: no route to host
```

Handing out unused addresses from the kind node subnet satisfies the
first consumer and not the second. The fix that satisfies both is to
stop synthesising addresses at all: `up` runs the kwok controller with
`hostNetwork: true` and `--node-ip=$(HOST_IP)`, so the address it serves
the fake kubelet endpoint on *is* a real node IP, and `scale-up` gives
every fake node that one address as its InternalIP. On-link because it
is a real node's address, reachable because the controller is listening
there. Every fake node sharing one address is fine for both: routes via
a local IP install cleanly, and the apiserver only ever needs the
endpoint to answer.

`node-initialize` fills in addresses only `{{ if not $hasInternalIP }}`,
so the explicit address in `scale-up`'s manifest wins and stays.

**kindnet's memory limit is sized for three nodes.** kind ships it with
a 50Mi cap; watching 300 nodes and 1000 pods takes 51–57Mi, so it is
OOMKilled. `scale-up` raises the limit to `128Mi + 1Mi/node`, records
the original in an annotation on the DaemonSet, and `scale-down` puts
it back. This is the one thing in this layer that reaches outside the
fake fleet, so it says so on the way past.

**DaemonSets land on the fake nodes, and that is fine.** kube-proxy and
kindnet tolerate every taint, so both schedule onto all 300 — 600-odd
extra pods. Left deliberately: it is what a real cluster looks like,
and it gives `stab drain` and `audit hardening` per-node DaemonSet pods
to reason about for free. It is also why `node-fail` leaves ~2.9 pods
per node behind after eviction.

## The synthetic fleet

`scale-up [nodes] [workloads] [replicas]` (default `100 200 3`) cycles
through ten posture archetypes, so a scan has to separate sound
workloads from six different defects rather than reporting one finding
N times:

| # | Archetype | What it should trip |
| --- | --- | --- |
| 0 | `sound` | nothing — PDB, probes, spread, requests |
| 1 | `no-pdb` | `audit workloads` — no PodDisruptionBudget |
| 2 | `singleton` | `audit workloads` — replicas: 1 |
| 3 | `no-probes` | `audit workloads` — no readiness/liveness probe |
| 4 | `no-spread` | `audit workloads` — no topology spread |
| 5 | `pinned` | `audit workloads` — `eligible_nodes=1` |
| 6 | `privileged` | `audit hardening` — `privileged_container` |
| 7 | `host-ns` | `audit hardening` — hostNetwork + hostPID |
| 8 | `hostpath` | `audit hardening` — hostPath mount |
| 9 | `cron-suspended` | `audit workloads` — suspended CronJob |

Namespaces come in three Pod Security Admission tiers so `audit
hardening` and `audit netpol` see partial coverage, which is the
harder read:

- `ns % 3 == 0` — `enforce: baseline`, plus a default-deny NetworkPolicy
- `ns % 3 == 1` — `warn`/`audit` labels only, so PSA is in dry-run and
  enforces nothing (lookout reports this, correctly, as a gap)
- `ns % 3 == 2` — no PSA labels, no NetworkPolicy

Archetypes 6–8 are routed away from the enforcing tier: PSA baseline
would *reject* those pods at admission, and a rejected pod is a broken
fixture, not a posture finding.

**The suspended CronJob needs wall clock.** `audit workloads` requires
a CronJob to have been suspended longer than `--cron-suspended`
(default 168h) *and* to have skipped at least one activation. The
generated CronJobs run `*/5 * * * *`, so pass `--cron-suspended=1m` and
give the fleet five minutes:

```sh
lookout audit workloads -A --cron-suspended=1m --timeout=120s
```

kwok does not fake time, and neither does this layer.

## Scenarios

`scale-up` builds a fleet with a *posture*. The scenarios in
[`scenarios/`](scenarios/) build a fleet with a *fault*, on the same
contract as [`../scenarios/`](../scenarios/) — `inject`, `verify`,
`revert`, one directory each, driven together by `examples/kwok/e2e`:

```sh
examples/kwok/e2e                    # the default four, ordered
examples/kwok/e2e logs unschedulable # or pick
examples/kwok/e2e node-storm         # opt-in; needs the sentinel
```

| Scenario | Fault | The claim that needs a fleet |
| --- | --- | --- |
| [`logs`](scenarios/logs/) | 24 pods × 2 containers, a Java exception and a Go panic under a flood of probe noise | 24 pods emitting one line arrive as **one** finding with `pods=24`, not 24 findings |
| [`unschedulable`](scenarios/unschedulable/) | 10 Pending pods, three different scheduler verdicts | a capacity *wall* that is not a capacity shortage; a workload placed 3-of-8 and stuck |
| [`pdb-gridlock`](scenarios/pdb-gridlock/) | 3 undrainable PDBs among 9 sound ones | which *nodes* you cannot drain, and silence about the nine |
| [`endpoints-empty`](scenarios/endpoints-empty/) | 3 Services selecting nothing among 9 that work | naming the three without naming the nine |
| [`node-storm`](scenarios/node-storm/) | 30 nodes lose their kubelet in the same second | one storm per failure domain, not one session per node (and does recovery close all of it) |

Two things are true of the default four. They assert the **read path
only** — the sentinel's wire is out of scope, not because it would not
work but because every claim they make is answerable from a read, which
is also why they need no sentinel deployed. And each one pairs a
positive assertion with a **negative** one: the sound lookalikes sitting
beside the fault must stay unreported. On a two-worker cluster there are
no lookalikes, so that half of the detector is unassertable, which is
the whole reason these live here rather than in `../scenarios/`.

`node-storm` is the exception on both counts, and is opt-in for it: its
entire claim is about what the sentinel **sends**, so it needs
`examples/sentinel/up`, and it spends most of ten minutes letting the
fleet go quiet before it marks the wire and then waiting on the real
recovery window. It is also the one
that found a product defect rather than confirming a behaviour: the wire
used to answer a 30-node outage with thirty sessions while `lookout
health` answered it in one line
([#334](https://github.com/go-steer/k8s-lookout/issues/334), fixed —
the drill now asserts three zone storms).

Each scenario's README says what it costs to get wrong; `logs` in
particular documents three failure modes that all present as *green*.

## Node failure, and why it is the strongest thing here

`node-fail N` labels N fake nodes, which makes the
`lookout-node-unreachable` Stage ([`stages/node-unreachable.yaml`](stages/node-unreachable.yaml))
write the four kubelet-owned conditions as
`Unknown`/`NodeStatusUnknown`/`"Kubelet stopped posting node status."`,
with the heartbeat frozen at its last real value. **The condition is
authored; everything downstream of it is real.** The
node-lifecycle-controller's taint manager sees `Ready=Unknown` and
applies `node.kubernetes.io/unreachable` NoSchedule + NoExecute, and
real eviction follows on the real `tolerationSeconds`. Measured on a
`node-fail 30`: 163 pods on those nodes, 86 left five minutes later —
the workload pods evicted, the DaemonSet pods that tolerate
`unreachable` forever correctly untouched. lookout's
`objectstate.node_notready` reads `Ready != True`, so `Unknown` trips
it the same way a real dead kubelet does.

Authoring the condition is not the first design. Letting kwok simply
stop heartbeating is more honest and does not work: kwok scopes node
ownership at its **informer**, so dropping the `kwok.x-k8s.io/node`
annotation from a node it is already managing does not stop the lease
renewing, and the node stays Ready indefinitely. Writing the condition
is the mechanism kwok itself ships for this
(`kustomize/stage/node/chaos/node-not-ready.yaml`).

Two details in that Stage are load-bearing, and both cost a debugging
session:

- **The selector must not require `Ready=True`.** `stage-fast.yaml`'s
  `node-initialize` selects on `Ready NotIn ["True"]` and nothing else,
  so it re-Readies any node it can reach. A failure Stage that stops
  matching once it has fired hands the node straight back, and the node
  oscillates NotReady/Ready instead of staying down.
- **`weight: 10000` is what starves it.** kwok picks one stage per
  event by weighted random choice and skips every weight-0 stage as
  long as some matching stage has a weight
  (`pkg/utils/lifecycle/lifecycle.go`). `node-initialize` ships with
  weight 0, so it always loses while the failure Stage keeps matching.

There is deliberately no recovery Stage: `node-initialize` *is* the
recovery path. `node-heal` drops the label, the failure Stage stops
matching, and `node-initialize` — now the only match — rebuilds the
full healthy status. Thirty nodes recover in about nine seconds.

One fidelity gap worth knowing: kwok's lease controller keeps renewing
the node's Lease even while the node reports `Unknown`, so a
`kube-node-lease` reader would still see a fresh `renewTime`. Nothing
in lookout reads Leases, but do not build an assertion on one here.

The kind equivalent ([`../scenarios/node-failure`](../scenarios/node-failure))
stops a docker container: destructive, slow, and limited to one of two
workers forever. Thirty nodes losing their kubelet in the same second
costs about ten seconds here.

Note which question that answers. DESIGN.md §14's *"one storm session,
not thirty"* is about the ~30 **pods** on a single dead node, and the
kind scenario already proves it (33 affected objects, 3 session
creates). What only a fleet can ask is what happens when the thing
that fails is many **nodes** — and the first answer, measured here, was
thirty sessions, because nothing keyed a storm across nodes
([#334](https://github.com/go-steer/k8s-lookout/issues/334)). That is
fixed: a node incident is now offered its zone as a last-ranked key,
and a fleet with no zone labels gets the `key_source=simultaneity`
cluster fallback. Same drill, three storms.
[`scenarios/node-storm`](scenarios/node-storm) is it with assertions
attached.

## bench

`bench` times every cluster-wide read command twice, writes
`$STATE_DIR/bench.tsv`, and reports:

- **ELAPSED** under a generous `--timeout`, so the command finishes and
  reports a real cost;
- **PEAK-MB** — peak child resident set (`ru_maxrss`), sizing transient
  decode + per-page trimming memory;
- **DEFAULT-TO** — whether it survives its own shipped `--timeout`
  default (`10s` for single checks, `30s`/`60s` for compositions). A
  command that cannot answer a 300-node cluster inside the default it
  documents fails closed for every operator who does not know to
  override it, and that is precisely the class of defect a two-node
  cluster can never surface.

`SCANNED` comes from each command's summary line, so a command that
returns fast because it silently scanned nothing or dropped a page shows
up as `scanned=0` or under-counted rather than passing as fast. When a
kwok fleet Deployment is present, `bench` also runs `state edges`
against one fleet workload to exercise its full-cluster graph snapshot.

Two optional guards turn the table into a CI regression gate (both off
by default for interactive runs, set in
[`.github/workflows/scale-kwok.yml`](../../.github/workflows/scale-kwok.yml)):

| | |
|---|---|
| `BENCH_REQUIRE_DEFAULT_TIMEOUT=1` | fail on a `void` stream, a shipped `--timeout` failure, or a pod sweep returning `scanned < fleet_pods` |
| `BENCH_MAX_PEAK_MB` | fail if any command's peak RSS exceeds this MiB ceiling |

`examples/kwok/bench --selftest` checks the parser and guards without a
cluster.

### What it measured

At 303 nodes / 1615 pods / 400 workloads, on a laptop-class kind
cluster:

```
COMMAND                        ELAPSED   SCANNED  FINDINGS DEFAULT-TO
audit workloads -A                0.2s       406       290        ok
audit hardening -A                0.4s       406       187        ok
health                            0.4s      2324        10        ok
triage delta -A                   0.4s      2604         0        ok
stab drain -A                     0.4s      1005        40        ok
scan                              0.9s      4889         2        ok
triage events -A                  1.3s      5985      4593        ok
```

Nothing fell over. Every target-free read command finishes in 0.1–1.3s
and every one survives its shipped 10s `--timeout` default with two
orders of magnitude to spare. That is a negative result, and it is the
useful kind: the read path's cost at 300 nodes is not where to look for
a scaling problem, and there is now a cheap way to re-check that claim
whenever the sources change.

At **2,000 nodes / 100,500 S8-padded pods / 5,000 workloads** (`examples/kwok/kwokctl-run 2000 5000 25`, 64-core workstation, shipped defaults `--kube-qps=50 --kube-burst=100`), **26/26** read-path commands pass `BENCH_REQUIRE_DEFAULT_TIMEOUT=1` inside their shipped `--timeout` defaults:

| Command class | Representative rows (`ELAPSED`, `PEAK-MB`, `SCANNED`, `DEFAULT-TO`) | Why it holds |
| --- | --- | --- |
| Non-pod / scoped reads | `audit workloads -A` (`1.8s`, `534.7M`, `5000`, `ok`), `stab drift -A` (`1.8s`, `535.3M`, `4500`, `ok`), `state edges (fleet)` (`1.3s`, `457.3M`, `9905`, `ok`), `state webhooks` (`0.0s`, `51.0M`, `ok`) | Same-namespace selector/PDB indexing and cached node-eligibility evaluations keep 5,000-workload posture checks under `2s`. |
| Streaming per-page sweeps | `triage events -A` (`1.4s`, `211.5M`, `107089`, `ok`), `triage top -A` (`4.7s`, `1611.3M`, `100500`, `ok`), `triage list -A` (`5.7s`, `1269.0M`, `109033`, `ok`) | `5000`-item Protobuf pages, 12-worker parallel per-namespace pod/event streaming, O(1) incremental event rollup, and typed Protobuf listers across all 22 built-in GVRs. |
| Projected pod-index checks | `triage delta -A` (`5.6s`, `1338.0M`, `111000`, `ok`), `state volumes -A` (`6.0s`, `1104.0M`, `112000`, `ok`), `stab drain -A` (`6.4s`, `1360.1M`, `100500`, `ok`), `stab scaledown` (`6.5s`, `1327.6M`, `2000`, `ok`) | Per-page compact pod projections discard `corev1.Pod` structs immediately (`1.1–1.4 GiB` peak RSS instead of `3.1–3.5 GiB` retaining full structs). |
| `audit` pod-aware detectors | `audit rbac -A` (`5.1s`, `1797.9M`, `133`, `ok`), `audit netpol -A` (`5.7s`, `1927.4M`, `5000`, `ok`), `audit hardening -A` (`7.8s`, `1734.7M`, `5000`, `ok`), `audit all -A` (`9.2s`, `1853.4M`, `20146`, `ok`) | Streaming naked-pod (`len(OwnerReferences)==0`) and active-pod-label extraction avoids materializing owned pods; `audit all` shares one `Snapshot` across all sub-audits. |
| Multi-pass compositions | `health` (`9.2s`, `4131.6M`, `211501`, `ok` vs `30s`), `scan` (`13.3s`, `4245.0M`, `339663`, `ok` vs `120s`) | Singleflight pod/node list caching (`WithListCache`) across concurrent sub-checks deduplicates cluster-wide pod LISTs (`health` finishes under `10s`; `scan` sweeps `339,663` objects in `13.3s`). |

On the same `100,500`-pod cluster, `kwokctl-run` also records both watch-path engines:
- **`lookout watch --dry-run --sources=auto`:** `/readyz` in `22.1s`, `4454.3 MiB` peak `VmHWM` (`4433.0 MiB` steady `VmRSS`, `2834.0 MiB` Go heap in use), `0.71 core` steady-state CPU.
- **`leeway-scale`:** `37.08 KiB/pod` marginal resident (`30.46 KiB/pod` Go heap, `60.4 MiB` empty-cluster intercept), `/readyz` in `19.0s` (`3699.3 MiB` peak RSS, `0.001 core` quiet); under 16 rollout drivers (`647.0` pod lifecycles/s, `31.66` evaluations/s), `1.26 core` whole-process (`0.57 core` evaluator share) and `p99 <= 100 ms` with `0` counter mismatches.

At the full **4,000 nodes / 200,500 S8-padded pods / 5,000 workloads** design target (`KWOKCTL_RUN_CHURN=24 examples/kwok/kwokctl-run 4000 5000 50`, `16,384 B` mean Pod, `21,061 B` mean Node, shipped defaults `--kube-qps=50 --kube-burst=100`), **26/26** read-path commands still pass `BENCH_REQUIRE_DEFAULT_TIMEOUT=1` inside their unchanged shipped `--timeout` defaults (`10s` / `30s` / `120s`):

| Command class | Measured at 200,500 pods (`ELAPSED`, `PEAK-MB`, `SCANNED`, `DEFAULT-TO`) | Why it holds at 200k+ |
| --- | --- | --- |
| Non-pod / scoped reads | `audit workloads -A` (`1.8s`, `591.1M`, `5000`, `ok`), `stab drift -A` (`1.8s`, `459.3M`, `4500`, `ok`), `state edges (fleet)` (`2.9s`, `779.7M`, `18905`, `ok`), `state webhooks` (`0.0s`, `54.3M`, `ok`) | Pre-sized edge maps, worker-trimmed pod pages (`ListClusterPodsTrim`), and cached node-eligibility checks keep posture/edge reads under `3s`. |
| Streaming per-page sweeps | `triage events -A` (`2.4s`, `331.8M`, `209222`, `ok`), `triage top -A` (`7.4s`, `1045.3M`, `200500`, `ok`), `triage list -A` (`8.5s`, `1382.5M`, `209033`, `ok`) | 8-shard HTTP/2 client pool (sharing one `50` QPS / `100` burst limiter), `2500`-item namespace pages (~86 requests fitting inside `100` burst), worker-goroutine `TrimReadPod`, and 6-worker parallel GVR listing. |
| Projected pod-index checks | `triage delta -A` (`7.6s`, `1522.7M`, `213000`, `ok`), `state volumes -A` (`7.6s`, `1731.3M`, `214000`, `ok`), `stab drain -A` (`7.8s`, `1682.2M`, `200500`, `ok`), `stab scaledown` (`7.9s`, `1782.8M`, `4000`, `ok`) | Parallelized list steps + worker-goroutine pod projections (`ListClusterPodsMap`) keep peak RSS at `1.5–1.8 GiB` and wall-clock under `8s` (`10s` default). |
| `audit` pod-aware detectors | `audit rbac -A` (`7.5s`, `1520.8M`, `133`, `ok`), `audit ai -A` (`7.5s`, `1440.2M`, `5000`, `ok`), `audit netpol -A` (`7.9s`, `1706.9M`, `5000`, `ok`), `audit hardening -A` (`9.1s`, `1712.5M`, `5000`, `ok`), `audit all -A` (`11.7s`, `1694.3M`, `20146`, `ok` vs `30s`) | Concurrent `Snapshot.warm` + worker-mapped `scanPods` + lossless `MaskString` byte/substring pre-filters across `25,519` findings keep `audit hardening -A` inside `10s`. |
| Multi-pass compositions | `scan` (`15.6s`, `6985.3M`, `645663`, `ok` vs `120s`), `health` (`16.4s`, `6844.7M`, `413501`, `ok` vs `30s`) | Per-batch COW edge slices in `pkg/graph` eliminate O(N²) hub-edge reallocation on `22,277`-pod namespaces (`default` SA / `default-deny` NetworkPolicy), bringing `health` from `44.7s` down to `16.4s`. |

Watch-path engines at `4,000` nodes / `200,500` S8-padded pods:
- **`lookout watch --dry-run --sources=auto`:** `/readyz` in **`43.8s`** (`7.6×` faster than before the `pkg/graph` hub-edge fix), **`6,772.5 MiB`** (`6.61 GiB`) peak `VmHWM` / `VmRSS`, **`5,587.3 MiB`** Go heap in use.
- **`leeway-scale`:** **`28.80 KiB/pod`** marginal resident (`22.86 KiB/pod` Go heap, `57.9 MiB` intercept; `1M`-pod linear extrapolation = `27.52 GiB`), `/readyz` in **`40.7s`** (**`5,697.1 MiB`** quiet peak RSS, `0.001 core`, `p99 = 25.0 ms`); under `24` rollout drivers (**`1,578.5` pod lifecycles/s** = `3.16×` the `500/s` design target), **`1,893.4` watch events/s** (`#491`: `81.2%` applied, `18.8%` inert, `1.02` pod events/lifecycle, `79.7` events folded per evaluation), **`23.76` evaluations/s** (`p99 <= 100.0 ms`), **`1.3840 core`** whole-process (`0.5871 core` evaluator + `0.7969 core` ingest/GC), and `0` counter mismatches.

## leeway-scale (deprecated — #842)

> **Deprecated (#842):** In-tree `cmd/leeway` and the `topology-drift` /
> `compute-class` watch sources are deprecated in favor of standalone
> [`github.com/go-steer/leeway`](https://github.com/go-steer/leeway).
> `examples/kwok/leeway-scale` remains in-tree during the Phase 1
> deprecation window solely to keep `.github/workflows/scale-kwok.yml`
> exercising the code until Phase 2 removes it.

`bench` measures the read path — a command runs, answers and exits.
`leeway-scale` measures the *watch* path, which is the one
[leeway-design §6.6](../../docs/leeway-design.md) sets an NFR on: what
the placement subsystem costs to hold a cluster in memory and to keep
up with it changing.

It runs the standalone `leeway` binary rather than `lookout watch`,
and that is the point. Measuring one source's marginal cost inside a
sentinel running eleven others measures the sentinel. Each run appends
a row to `$STATE_DIR/leeway-scale.tsv` and reprints the ladder:

```
examples/kwok/leeway-scale            # rung 0, on the empty cluster
examples/kwok/scale-up 400 4000 3
examples/kwok/leeway-scale            # a loaded rung
examples/kwok/leeway-scale --report   # just reprint
```

**Two rungs, not one.** A single measurement carries the fixed cost of
an idle Go process inside it, so it cannot be divided by the pod count.
The harness differences the extreme *quiet* rungs and reports the
slope, which is the number §6.6 extrapolates from, plus the intercept,
which is what the process costs before it caches anything.

**Two axes, never mixed.** Memory is a function of object count and CPU
is a function of event rate. A rung with no churn measures the first
cleanly and honestly reports ~0.001 core for the second. Set
`LEEWAY_SCALE_CHURN` to a number of parallel rollout drivers to get the
second:

```
LEEWAY_SCALE_CHURN=12 LEEWAY_SCALE_WINDOW=90 examples/kwok/leeway-scale
```

Churn is `kubectl rollout restart`, not pod deletion. A delete puts
kubectl on the critical path once per pod and tops out around seven
lifecycles a second; a rollout is one API call the Deployment
controller turns into `replicas` lifecycles at controller speed, and it
is the event the subsystem exists to watch. The rate that lands in the
ladder is **counted from the apiserver** — fleet pods whose
`creationTimestamp` falls inside the window — not inferred from what
the driver issued.

**Mind the denominator.** Pod lifecycles are what the driver produces;
they are not what the subsystem reacts to. One rollout is a Deployment
write, a ReplicaSet create, a long tail of status updates and
`replicas` pod lifecycles, and leeway re-evaluates on the Deployment
write. So the report differences both `lookout_leeway_evaluation_duration_seconds`
and `lookout_leeway_watch_events_total{resource,outcome}` ([#491](https://github.com/go-steer/k8s-lookout/issues/491)),
giving the per-*evaluation* cost, the raw watch-event rate (`applied` vs `inert`
share and `pod events / lifecycle`), the coalescer's `events / evaluation` folding
ratio, and the end-to-end `µs / watch-event` split between ingest/GC and evaluation.

Two optional ceilings turn the measurement into a regression guard,
both off by default because a human taking a ladder wants the number
rather than a verdict:

| | |
|---|---|
| `LEEWAY_SCALE_MAX_KIB_PER_POD` | fail if the ladder's marginal resident cost exceeds this |
| `LEEWAY_SCALE_MAX_P99_MS` | fail if this rung's evaluation p99 exceeds this |

CI sets both; see [`.github/workflows/scale-kwok.yml`](../../.github/workflows/scale-kwok.yml),
which takes rung 0 on the empty cluster, scales up and takes the second
with the verdict attached.

A rung whose fixtures are unpadded is not recorded, and neither is one
that reported a counter mismatch — the first would quote a fleet of toy
objects as a scale result, which is the specific mistake spike S8
exists to prevent, and the second would quote the cost of a subsystem
that was getting the answer wrong.

## soak

`leeway-scale` answers "what does it cost". `soak` answers the
question §6.6.1 cannot: **does it stay there.**

```
LEEWAY_SOAK_HOURS=2 examples/kwok/soak
examples/kwok/soak --report
examples/kwok/soak --selftest   # checks the verdict; needs no cluster
```

This one runs `lookout watch` with the full source set, a resident
`lookout mcp` HTTP server under periodic tool-call traffic
(`LEEWAY_SOAK_MCP=1`), and periodic `lookout audit all -A | lookout
findings diff --store=…` posture baselining (`LEEWAY_SOAK_AUDIT=1`),
deliberately the opposite choice from `leeway-scale`: a leak is a
property of the shipped processes and SQLite store, and a leak in the
plumbing eleven sources, MCP sessions, or posture baseline writes share
is still a leak an operator pages on. It churns the fleet throughout,
samples RSS and Go heap every `LEEWAY_SOAK_SAMPLE` seconds, fits a
least-squares slope over everything after `LEEWAY_SOAK_WARMUP` for
both `lookout watch` and `lookout mcp`, and verifies that every posture
and MCP round exits 0 with bounded SQLite store growth
(`LEEWAY_SOAK_STORE_TOLERANCE`, default `0.25`).

Both series are fitted because they answer different questions — a
rising heap is lookout retaining objects, a rising RSS with a flat heap
is the allocator not returning pages, and only the first is a bug.

**The third series is the control, and it is what stops the other two
lying.** Each sample also counts the fleet's own pods and ReplicaSets,
because the thing that drives this soak — a rollout, over and over —
*creates objects*. A rising RSS over a rising object count is the
process doing its job; a rising RSS over a flat one is the only shape
that is a leak, and without the count there is no way to tell them
apart. For the same reason the fleet template pins
`revisionHistoryLimit: 2`: at the default of 10, four thousand
Deployments rolled for two hours leave tens of thousands of dead
ReplicaSets behind, and a soak with no control column would read the
driver's own litter as the process leaking.

**So the verdict is defined rather than eyeballed, and which test it is
depends on what the control did.** When the fleet holds still across the
window, the verdict is RSS against time: the fitted slope projected over
24 hours must come to less than `LEEWAY_SOAK_TOLERANCE` (default 0.10)
of the mean. When the fleet grows, that test is meaningless, so the
verdict becomes the *marginal resident cost of an object* — RSS fitted
against the object count, over the first half of the window against the
second — and the same tolerance applies to the change between them.
Later objects costing more than earlier ones is what unbounded retention
looks like, and answering that needs no extrapolation in time at all.

Do not be tempted to collapse the two by fitting RSS-per-object against
time instead. That ratio is `(intercept + marginal·N)/N`, which is not
linear in *t* for any *N(t)*, so its slope extrapolates to nonsense: the
first full two-hour run projects it to −128% of its own mean over 24
hours.

`--selftest` feeds synthetic series with known answers through the real
verdict — a flat fleet with and without a leak, a growing fleet with and
without one, and the two refusals — and needs no cluster. It exists
because the verdict shipped gating on raw RSS, which is the opposite of
what the paragraphs above describe, and it would have failed the first
real run: the fleet grew 121% in two hours, RSS rose 847 MiB/h, and the
marginal cost per object *fell*, 53.4 KiB to 50.5. A verdict on RSS and
a verdict on marginal cost disagree only while the fleet is growing,
which is precisely the case a run against a healthy cluster never
distinguishes from a leak, so the check has to be synthetic. CI runs it
before it builds a cluster.

## Metrics

`up --metrics` installs kwok's resource-usage simulation, which serves
a synthetic `/metrics/resource` per fake node driven by the pod's
`kwok.x-k8s.io/usage-cpu` / `usage-memory` annotations. It is **off by
default**: metrics-server has to reach the fake kubelet endpoint the
kwok controller serves, and whether it can depends on the runtime.
`../kind/up` already installs a real metrics-server for the real nodes,
and the saturation source only needs `metrics.k8s.io` to answer at all,
which it does. Turn this on only when you specifically want fake nodes
to report utilisation.

## Known limits

- **Multi-cluster is out of reach.** `lookout watch --clusters` reaches
  clusters kubeconfig-free through a cloud provider
  (`internal/watch/wiring.go`), so N kwokctl clusters cannot stand in
  for a fleet. Testing that path still needs GKE.
- **`net` probes need real networking.** The `vantage` pod and
  `lookout net` are kind-only.
- **No time travel.** Anything gated on a duration — suspended
  CronJobs, `--since` windows, flap detection — still costs wall clock.
- **Pinned to one kwok release.** `KWOK_VERSION` (default `v0.8.0`) is
  a deliberate pin: the controller image, the CRDs and the default
  Stages are only guaranteed consistent within one release.
