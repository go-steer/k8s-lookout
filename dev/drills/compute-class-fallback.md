# Compute-class fallback drill — GKE

Drives the `compute-class` source (`docs/leeway-design.md` §7.7) from a
real GKE autoscaler to the wire. The shipped evidence for the source is
unit and fixture tests plus a counters-only acceptance run on
`std-simian-test` (§14 Phase 6: rank shares matched a hand audit, all
SLIs zero). **No rank finding has yet been observed end to end on a
live cluster.** This runbook is meant to change that, with real
autoscaler timing: rule skipping, node provisioning, and the
`ccc_priority_index` stamping lag.

It forces two conditions deterministically and without stockout luck:

| Part | Class shape | Expected finding | Tier → route |
| --- | --- | --- | --- |
| A — wedged | two priorities, both impossible, `DoNotScaleUp` | `leeway.rank_wedged` | A, critical → session |
| B — fallback | rank 0 impossible, rank 1 `n2`, `DoNotScaleUp` | `leeway.rank_degraded`, reason `last-rank` | B, warning → watchboard digest |

Part B is also a negative test: `rank_wedged` must **not** fire for it,
even though its pod sits Pending on a `DoNotScaleUp` class for the
whole provisioning wait (see step 4).

> **STAGING CLUSTERS ONLY, but a cheap drill.** Part A provisions
> nothing. Part B provisions one small `n2` node (NAP picks the
> smallest shape that fits a 100m pod, typically `n2-standard-2`) for
> about half an hour. Nothing on the cluster is disrupted. The cost
> is one node, plus one auto-created node pool that NAP removes after
> scale-down.

## Why rank 0 is "impossible by shape"

A rule that GKE can never satisfy is skipped instantly and never
attempted, so it costs nothing and does not depend on the weather.
`machineFamily: n4, spot: true, minCores: 128` is that rule: N4 tops
out at 80 vCPU. GKE marks the class `CrdMisconfigured` /
`RuleMisconfigured` with *"Machine type in the given machine family
with at least 128 cpu … doesn't exist"*, and still falls through to
the next priority. A stockout fallback (the preferred rule is
attempted, `FailedScaleUp`, then fall through) has the same shape on
the wire but is not reproducible on demand. The measurements behind
this are in §7.7.1–§7.7.3.

## Prerequisites

- A GKE **Standard** cluster with node auto-provisioning enabled, so
  that `nodePoolAutoCreation` works. Autopilot works too, but its four
  managed classes are single-priority and are never judged. Only the
  drill's own classes matter.
- `kubectl` access that can create a cluster-scoped `ComputeClass`
  and Deployments in a scratch namespace.
- The sentinel, either:
  - **local, read-only (recommended):** a `lookout` binary built from
    the tree under test, run out-of-cluster with `--dry-run`. Inject
    payloads print to stdout, and nothing is installed. Point it at
    the drill cluster with a single-context kubeconfig so your global
    current-context is never involved:

    ```
    kubectl config view --minify --flatten --context=<drill-context> > /tmp/ccdrill/kubeconfig
    go build -o /tmp/ccdrill/lookout ./cmd/lookout
    ```

  - **in-cluster:** `deploy/` applied unmodified (the watcher
    ClusterRole already grants `computeclasses`) with the flag block
    from step 2 appended, plus the capture stub
    ([`stub-daemon.py`](./stub-daemon.py)) deployed per
    [`node-failure.md`](./node-failure.md).

On `std-simian-test`, the class `leeway-fallback-probe` and the
namespace `leeway` already exist as a fixture, exactly in Part B's
shape. Reuse them, and leave both in place at cleanup.

## 1. The classes and workloads

```yaml
# Part A — no priority can ever be satisfied.
apiVersion: cloud.google.com/v1
kind: ComputeClass
metadata:
  name: leeway-drill-wedged
spec:
  priorities:
  - machineFamily: n4
    minCores: 128
    spot: true
  - machineFamily: n4
    minCores: 128
  nodePoolAutoCreation:
    enabled: true
  whenUnsatisfiable: DoNotScaleUp
---
# Part B — rank 0 impossible, rank 1 real. Already present on
# std-simian-test; skip this object there.
apiVersion: cloud.google.com/v1
kind: ComputeClass
metadata:
  name: leeway-fallback-probe
spec:
  priorities:
  - machineFamily: n4
    minCores: 128
    spot: true
  - machineFamily: n2
  activeMigration:
    optimizeRulePriority: true
  nodePoolAutoCreation:
    enabled: true
  whenUnsatisfiable: DoNotScaleUp
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: drill-wedged
  namespace: leeway
spec:
  replicas: 1
  selector:
    matchLabels: {app: drill-wedged}
  template:
    metadata:
      labels: {app: drill-wedged}
    spec:
      nodeSelector:
        cloud.google.com/compute-class: leeway-drill-wedged
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
        resources:
          requests: {cpu: 100m, memory: 64Mi}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: drill-fallback
  namespace: leeway
spec:
  replicas: 1
  selector:
    matchLabels: {app: drill-fallback}
  template:
    metadata:
      labels: {app: drill-fallback}
    spec:
      nodeSelector:
        cloud.google.com/compute-class: leeway-fallback-probe
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
        resources:
          requests: {cpu: 100m, memory: 64Mi}
```

Save this as `/tmp/ccdrill/drill.yaml`. Don't apply it yet: the
sentinel goes first (step 3).

Request the class with `nodeSelector`, not node affinity. That is the
form GKE documents, and the only one the wedged rule counts. A pod
asking by affinity reads as "not wedged", which is the safe direction
for a Tier A rule to be wrong in. GKE injects the matching
`compute-class=<name>:NoSchedule` toleration at admission.

## 2. Sentinel flags

```
--sources=compute-class
--storm=off                       # nothing here for the correlator; keeps the log readable
--compute-class-dwell=7m          # drill value; production default is 10m — see step 4 before going lower
--compute-class-window=10m        # drill value; production default is 1h
--metrics-addr=127.0.0.1:9464     # optional: the SLI gauges in step 5
```

Leave `--compute-class-last-rank-ceiling` at its default of 0.9. Part
B's axis runs 100% at its last rank, so any ceiling below 1 breaches.
Leave `--compute-class-rank0-floor` off. On a two-tier class it
measures the same thing from the other end, and turning it on gives
Part B a second `rank_degraded` (reason `rank0-share`) on its own
episode. Do that only if you want to see the episode split.

Two limits are fixed in code:

- **MinWindow = 5m.** A share is not judged over less than five
  minutes of samples.
- **Migration grace = 15m.**

The window's clock starts at the sentinel's first sample of the axis,
not at the first pod. Starting the sentinel at least 5m before Part
B's node lands therefore removes MinWindow from the critical path.

Local run:

```
/tmp/ccdrill/lookout watch --kubeconfig /tmp/ccdrill/kubeconfig --dry-run \
    --sources=compute-class --storm=off \
    --compute-class-dwell=7m --compute-class-window=10m \
    --metrics-addr=127.0.0.1:9464 \
    > /tmp/ccdrill/wire.log 2> /tmp/ccdrill/sentinel.log &
```

Confirm startup in `sentinel.log`:

```
recovery: compute-class clearance observer registered (§7.7.4 rank episode resolved / class gone → cleared)
```

The rest of the line set is `--dry-run`, watchboard and recovery
boilerplate.

Every other multi-priority class on the cluster is judged too. Any
finding about a class you didn't create is real, and belongs to that
class's owner, not to the drill. Note it, and don't count it.

## 3. Start the clock

```
kubectl --kubeconfig /tmp/ccdrill/kubeconfig apply -f /tmp/ccdrill/drill.yaml
date -u +%T     # T0
```

## 4. Expected timeline

Derived from the source (`pkg/sources/computeclass`, rules in
`pkg/leeway/rankjudge.go`) and the measured GKE timings in §7.7. The
judge runs once a minute and pod-seconds flush every 30s, so add up to
a minute of slack to every row.

| T0 + | Part A (`drill-wedged`) | Part B (`drill-fallback`) |
| --- | --- | --- |
| 0 | Pod Pending, `wedged_pods{class=leeway-drill-wedged}` = 1, wedged verdict breaches → §8.2 Pending | Same: pod Pending on a `DoNotScaleUp` class, wedged verdict breaches → Pending |
| ~7s | `NotTriggerScaleUp` on the pod (no rule can fit) | `TriggeredScaleUp`: rank 0 skipped instantly, an `n2` NAP pool created |
| ~4.5m | — | Node Ready, pod bound. The wedged verdict stops breaching, and the Pending episode is **discarded unemitted** because it was younger than the dwell. |
| ~5.2m | — | `ccc_priority_index: "1"` stamped, 33–44s after the node registered. Until then the pod sits at rank *unknown* (`rank_pending` gauge = 1) and accrues no tier time. After it, all of the axis's tier time is at rank 1, so last-rank share = 1.00 > 0.9 and the verdict breaches. |
| ~7–8m | **`leeway.rank_wedged`**, critical: session create + inject | — |
| ~12–13m | — | **`leeway.rank_degraded`**, reason `last-rank`, warning: one watchboard digest entry within the 1m flush |

What each finding should carry on the wire. The payload is `kind`,
`reason` (the rule), `kind_of_object` and `name`; the arithmetic is in
the message (`leeway.RankMessage`):

- **`rank_wedged`:**
  - `reason=wedged`, `kind_of_object=ComputeClass`,
    `name=leeway-drill-wedged`;
  - a message ending *"1 pod(s) Pending against a DoNotScaleUp class:
    no priority can be satisfied and the autoscaler will not provision
    outside the list; ordered by …, 2 tier(s)"*.
- **`rank_degraded`:**
  - `reason=last-rank`, `name=leeway-fallback-probe`;
  - a message reading *"… mean achieved rank 1.00 over 10m0s: rank 1
    100% (machineFamily=n2) — rank 1 share 1.00 is above the ceiling 0.90 …"*. The
    window is shorter than 10m if the sentinel started less than 10m
    earlier.

Neither part should produce `rank_no_migration` (rank 0 never came
back) or `rank_tier_unused` (that needs 30 days of lifetime, and it is
Tier C, so off the wire by default).

**The negative to watch for: no `rank_wedged` for
`leeway-fallback-probe`.** The wedged rule counts *any* Pending pod
on a `DoNotScaleUp` class. It cannot tell "no priority fits" apart
from "rank 1 fits and its node is still booting". The only thing
separating the two is the dwell outlasting the provisioning wait:
about 4.5m measured, against a 10m default and this drill's 7m. With
a dwell under ~5m, Part B produces a false `rank_wedged` whose message
is untrue. The same happens in production if a satisfiable rank takes
longer than the dwell to provision: a slow GPU shape, or a
stockout-and-retry. If you see it at 7m, record the provisioning time
from the pod's events. That is a finding about the rule, not the
drill.

## 5. Observe

```
grep -n 'leeway\.rank_' /tmp/ccdrill/wire.log
kubectl --kubeconfig /tmp/ccdrill/kubeconfig -n leeway get pods -o wide
kubectl --kubeconfig /tmp/ccdrill/kubeconfig -n leeway get events --sort-by=.lastTimestamp
kubectl --kubeconfig /tmp/ccdrill/kubeconfig get nodes -L cloud.google.com/compute-class \
    -o custom-columns=NAME:.metadata.name,CLASS:.metadata.labels.cloud\\.google\\.com/compute-class,IDX:.metadata.annotations.cloud\\.google\\.com/ccc_priority_index
curl -s 127.0.0.1:9464/metrics | grep -E 'lookout_leeway_preference_(wedged_pods|pods|rank_pending|alert_state|disagreement|unmatched)'
```

Record, against T0:

- the pod's `Scheduled` time;
- the node's creation and Ready times;
- the first time the `IDX` column reads `1`;
- the wire time of each finding.

`disagreement` and `unmatched` must stay 0 for both drill classes.
The rule matcher should agree with GKE's annotation on an `n2` node,
and a non-zero value is an integration defect (§7.7.4's last row).

## 6. Optional — migration back

This step shows `rank_no_migration` staying **silent** on a class that
does migrate. It costs one more node for about 10 minutes.

Edit `leeway-fallback-probe`'s rank 0 to a satisfiable rule, for
example `machineFamily: n4` with no `minCores` and no `spot`.
§7.7.1 measured `activeMigration.optimizeRulePriority` acting about
4m17s after the edit. The sequence is a new rank-0 node, then a new
pod (the ReplicaSet replaces it; no pod changes rank), then the old
node draining.

Expect:

- rank 0 restored (rank-0 pods > 0 after a loss);
- an improving transition inside the 15m migration grace, so
  `rank_no_migration` does **not** fire;
- the open `rank_degraded` episode entering §8.2's resolve dwell. That
  dwell is 30m and not tunable, and a `kind=resolved` follows it in
  the in-cluster variant.

**Put rank 0 back afterwards.** On the shared fixture, rank 0 must be
unsatisfiable so that the class provisions nothing while idle.

## 7. Cleanup

```
kubectl --kubeconfig /tmp/ccdrill/kubeconfig -n leeway delete deploy drill-wedged drill-fallback
kubectl --kubeconfig /tmp/ccdrill/kubeconfig delete computeclass leeway-drill-wedged
kill %1     # the local sentinel
```

On `std-simian-test`, keep `leeway-fallback-probe` and the `leeway`
namespace. Check that the class's rank 0 is back to `n4` / `spot` /
`minCores: 128`. The `n2` node scales down about 10 minutes after its
pod goes, and NAP then deletes the empty pool. Confirm with:

```
gcloud container node-pools list --cluster=<cluster> --region=<region>
```

Keep `wire.log`, `sentinel.log` and the metrics scrape with the drill
record.

## Reference run

Not yet recorded. The first run should replace this paragraph with the
step-5 timeline, and correct step 4 wherever reality differed.
