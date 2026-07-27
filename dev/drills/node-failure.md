# Node-failure drill — GKE staging replay

Replays the M2 exit-criterion drill (DESIGN.md §14: "Node-failure drill
produces 1 storm session, not 30; fix-verify round-trips without agent
polling") against a **real GKE cluster**. The kind-cluster original and its
captured evidence are in [`docs/milestones/M2.md`](../../docs/milestones/M2.md);
this runbook exists so the same claim can be verified on real
node-lifecycle timing (real node-monitor grace periods, real MIG
auto-repair, real system DaemonSets).

> **STAGING CLUSTERS ONLY.** This drill kills a node. Everything scheduled
> on it — including system DaemonSets, and anything an operator forgot was
> there — loses its kubelet mid-flight. PodDisruptionBudgets do NOT protect
> against a VM stop (there is no eviction API call to refuse). Do not run
> this on a cluster serving traffic, and do not run it on a node pool
> shared with workloads you did not deploy for this drill.

## Prerequisites

- A GKE **staging** cluster, ≥ 2 nodes outside the drill pool, and
  `container.admin`-ish access plus `compute.instances.{stop,start}` on the
  node VMs.
- A core-agent daemon the sentinel can reach — or, for a capture-only run,
  the stub from this directory
  ([`stub-daemon.py`](./stub-daemon.py), deployed as in the M2 evidence
  doc: python:3.12-alpine pod + Service `core-agent:7777` in
  `agent-triage`, script mounted from a ConfigMap). The stub logs every
  `POST /sessions` / `/inject` body to stdout; `kubectl logs` is your
  evidence capture.
- The `ghcr.io/go-steer/lookout` image (v0.3.0+ — the M2 pipeline stages)
  and the `deploy/` manifests.
- Know the two RBAC gaps recorded in M2's evidence before you read the
  enrichment output: the shipped ClusterRole lacks `get` on
  `apps/deployments` and on `pods/log`, so enrichment bundles arrive
  `outcome=partial` (spec + logs sections become `enrichment_error` /
  overflow trailers). The storm radius section is unaffected.

## 1. Target selection — make the blast radius yours

Create a dedicated, tainted drill pool so the ONLY collateral is what you
deploy (system DaemonSets still land there — that is realistic and shows
up as extra storm members, as it did in the kind run):

```
gcloud container node-pools create drill-pool \
    --cluster=<cluster> --zone=<zone> \
    --num-nodes=1 --machine-type=e2-standard-4 \
    --node-taints=drill=true:NoSchedule \
    --no-enable-autorepair --no-enable-autoupgrade
```

- `--no-enable-autorepair` matters: auto-repair recreates a NotReady node
  after ~10 min with a NEW node name — fine if you want GKE itself to be
  the "fix" in fix-verify, but it deletes the node object mid-drill and
  replaces your victim pods' ancestor. For a controlled first run, repair
  the node yourself (step 5).
- **VM stop vs cordon+drain:** `kubectl drain` is a *graceful* eviction —
  it produces `Evicted`/delete events, never the `Ready→NotReady`
  transition or the per-pod `NodeNotReady` burst, so it exercises a
  different storm key (owner chain, not node). It is the safe rehearsal,
  not the replay. The faithful replay is stopping the VM.

## 2. Deploy the sentinel

Apply `deploy/11..13` (RBAC) unmodified, then `deploy/51` with the drill
flag set appended to the shipped args (these are the exact flags of the
recorded kind run):

```
--sources=k8s-events,object-state
--storm=on
--enrich=critical
--recovery-stable-for=60s          # drill value; production default is 5m
--severity=objectstate.endpoints_empty=warning   # optional: watchboard demo
```

Confirm startup: the log must show `storm: topology graph ready`,
`recovery: tracking enabled`, `watchboard: enabled`, and NO §11 probe
error. Keep the sentinel and the daemon/stub OFF the drill pool (no
toleration for `drill=true`).

## 3. Spread the victims

```
kubectl create ns stormlab
kubectl -n stormlab create deployment victim --image=registry.k8s.io/pause:3.10 --replicas=30
kubectl -n stormlab patch deploy victim --type=strategic -p '{
  "spec":{"template":{"spec":{
    "tolerations":[{"key":"drill","operator":"Equal","value":"true","effect":"NoSchedule"}],
    "nodeSelector":{"cloud.google.com/gke-nodepool":"drill-pool"}}}}}'
kubectl -n stormlab wait --for=condition=Available deploy/victim --timeout=180s
```

Verify all 30 pods sit on the drill node
(`kubectl -n stormlab get pods -o wide`). Snapshot the daemon/stub session
count now — that is your baseline for the headline number.

## 4. Fail the node

```
NODE=$(kubectl get nodes -l cloud.google.com/gke-nodepool=drill-pool -o name | cut -d/ -f2)
gcloud compute instances stop "$NODE" --zone=<zone>
```

Timeline to expect (kind run for comparison: NotReady at T+47s, storm
formed the same second, all 33 members attached 2 s later): GKE's
node-monitor-grace-period is ~40 s, so `Ready→NotReady` lands under a
minute after the VM stops; the per-pod `NodeNotReady` burst follows within
seconds — well inside the default 60 s `--storm-window`.

## 5. Observe (the exit criterion, verbatim)

In the sentinel log:

- `fire node_notready pod=/<node>` — the leading object-state signal
  opens the node incident BEFORE the reactive event (they share a dedup
  family).
- Up to `--storm-min - 1` more per-incident sessions (the burst's first
  arrivals — they will be superseded), then
  `storm formed on Node <node>: N incidents ... → sid=<storm session>`
  and one `storm attach ...` line per remaining member.

In the daemon/stub capture:

- Exactly ONE `kind=storm` inject (ancestor_kind=Node, severity=critical,
  representative list, member fingerprints, `enrichment.bundle` with the
  radius section naming the victim pods).
- `kind=storm.member_superseded` in each pre-storm session; ~30
  `kind=storm.member` followups in the storm session; **no** flood of 30
  per-incident sessions.

Headline number = sessions created after your baseline. The kind run
scored 3 (storm + 2 superseded seeds) for 33 affected objects.

## 6. Fix-verify and rollback

```
gcloud compute instances start "$NODE" --zone=<zone>
```

The kubelet reconnects, victims return Ready, and after
`--recovery-stable-for` + one 15 s tick each member's `kind=resolved`
(`resolution=recovered`, `cleared_after`, `observed_stable_for`) lands in
the STORM session — no agent polled anything. Known gap (M2 evidence,
issue-tracked): the storm's own aggregate `kind=resolved` will NOT fire —
the Node member has no clearance observer yet — so expect members-1
resolved records, not a storm outcome record.

If the node does not come back (or you ran with auto-repair on and GKE
replaced it): the victim pods reschedule onto the NEW drill node once the
pool is healthy; member incidents then resolve owner-based
(`resolution=recovered` via ready replacements). Either path closes the
loop.

## 7. Cleanup

```
kubectl delete ns stormlab weblab --ignore-not-found
gcloud container node-pools delete drill-pool --cluster=<cluster> --zone=<zone>
```

Remove the drill flags from the sentinel Deployment (or the whole
`agent-triage` namespace if it was drill-only), and delete any stub-daemon
capture pods. Keep the captured logs with your drill record — the
`kind=resolved` payloads are §9.3 corpus records.
