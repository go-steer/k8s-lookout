# Bad-deploy drill — GKE staging replay

Replays the first half of the M3 exit criterion (DESIGN.md §14: "a
staged bad deploy … opens sessions before user-visible failure") against
a **real GKE cluster**, plus the drill-C half ("blast radius at onset
answerable 30 min after the fact") which rides on this drill's onset.
The kind-cluster original and its captured evidence are in
[`docs/milestones/M3.md`](../../docs/milestones/M3.md); this runbook
exists so the same claim can be verified with real image pulls, real
probe timing, and a real registry between you and the bad image.

> **STAGING CLUSTERS ONLY.** The drill itself is designed to be
> user-invisible (`maxUnavailable=0` — old pods keep serving), but you
> are still deliberately shipping a crashing image through a rollout
> pipeline. Do not run it against a Deployment users depend on; bring
> your own victim.

## Prerequisites

- A GKE **staging** cluster and rollout rights on one namespace, plus a
  registry you can push a knowingly-broken image to (or use two public
  image tags as the kind run did — the *image string* must change, not
  just the command, so `triage changes` has a rollout to name).
- A core-agent daemon the sentinel can reach — or the capture stub
  ([`stub-daemon.py`](./stub-daemon.py), deployed as in the M2/M3
  evidence docs: python pod + Service `core-agent:7777` in
  `agent-triage`; `kubectl logs` is the wire capture).
- The `ghcr.io/go-steer/lookout` image (v0.4.0+ — rollout source, store,
  graph history) and the `deploy/` manifests, applied unmodified.
- For the post-mortem half: `--store` must sit on a volume you can copy
  out later. The image is distroless — **`kubectl cp` does not work**
  (no tar in the container). Use a hostPath and copy via node access
  (`gcloud compute ssh <node> -- sudo cat …` or `gcloud compute scp`),
  or a PVC you can mount from a debug pod.

## 1. Deploy the sentinel

`deploy/11..13` RBAC unmodified, then `deploy/51` with these args
appended to the shipped set (the exact flags of the recorded kind run,
minus the kind-only bits):

```
--sources=k8s-events,object-state,rollout,saturation,degradation,expiry
--storm=on --enrich=critical
--store=/data/lookout.db
--graph-snapshot-interval=1m       # drill value; default 5m
--recovery-stable-for=60s          # drill value; default 5m
--severity=rollout.stall=critical  # the §7.7 promotion — see step 4
```

`--severity=rollout.stall=critical` is a *judgment*, not a requirement:
`rollout.stall` ships warning-class, which routes to the watchboard
digest in per-incident mode. On a staging cluster where bad deploys are
the thing you are hunting, promoting it buys a dedicated enriched
session (the M3 record shows both routings). Confirm startup: all six
§11 probes pass, `store: enabled`, `graph history: enabled`, `storm:
topology graph ready`. Note the `graph history: baseline snapshot
stored` line — `--at` answers begin there.

## 2. The victim

Any 2+-replica Deployment with a readiness probe, a Service in front,
and `maxUnavailable=0 maxSurge=1` (the kind run used `python:3.12-alpine
python -m http.server` behind a ClusterIP Service). The strategy is the
point of the drill: a bad rollout under `maxUnavailable=0` parks a
crashing surge pod NEXT TO the healthy revision — users never see it,
and only a Deployment-altitude signal notices in time. Verify baseline
200s through the Service from an in-cluster vantage pod.

## 3. The bad deploy — record the onset wall-clock

Roll out a valid-but-broken image (crashing entrypoint, so readiness
never passes; a wrong-arch tag or a missing-binary image works equally):

```
kubectl -n <ns> set image deploy/<victim> <container>=<registry>/<victim>:broken
date -u   # <-- T0, keep it
```

Expected timeline (kind run for comparison):

- **+seconds** — reactive pod-level sessions (`BackOff`, `Unhealthy`):
  right events, wrong altitude. They dedup separately by design.
- **+~3m** (`--rollout-observe`, default 3m) — `rollout.stall` fires on
  the DEPLOYMENT: `new_ready=0/1 old_ready=2/2 … probable bad deploy`.
  With the promotion flag it opens its own enriched session; without,
  it lands in the next watchboard digest (≤ ~4m). Record the fire time
  — this is the drill-C onset **T**.
- `objectstate.progress_deadline` fires at 80% of
  `progressDeadlineSeconds` (default 600s → +8m) — in the kind run
  `rollout.stall` won by ~4m46s and the rollback preempted it entirely.
  Record which fired first on your timing.

**While the stall is open**, prove no user-visible failure — this is the
exit-criterion half most worth capturing:

```
kubectl -n <ns> exec <vantage-pod> -- sh -c \
  'for i in 1 2 3 4 5; do wget -S -q -O /dev/null http://<victim>.<ns>.svc/ 2>&1 | grep HTTP/; done'
kubectl -n <ns> get pods    # broken pod CrashLoopBackOff, old pods Running
```

## 4. Fix-verify

```
kubectl -n <ns> rollout undo deploy/<victim>
```

Watch the sentinel inject `kind=resolved` (`resolution=recovered`) into
the stall's session. Known gap (M3 observation 4, issue-tracked): the
record currently arrives seconds after the rollback with
`cleared_after=0s` — the stability window is not yet honored for
rollout clearances, so treat its duration fields as unreliable until
that fix lands.

## 5. Post-mortem — blast radius at onset (the drill-C half)

Let the cluster move on first: the rollback already changed topology;
optionally delete one healthy pod so live and history visibly diverge.
Wait ≥ 2 snapshot intervals after the changes. Then, **without touching
the cluster from the query path**:

```
# copy the store off the node ('kubectl cp' won't work — distroless):
gcloud compute scp <node>:/var/lib/lookout/lookout.db* ./store/ --zone=<zone>

# who was in the blast radius when the stall fired?
lookout triage radius <broken-pod-name> -n <ns> --at=<T> --store=./store/lookout.db
lookout triage radius <broken-pod-name> -n <ns>          # live: pod no longer exists

# what changed in the 10 minutes before onset?
lookout triage changes <broken-pod-name> -n <ns> --at=<T> --since=10m --store=./store/lookout.db
```

Copy the WAL sidecar files (`lookout.db-wal`, `-shm`) along with the db.
Expect: `source=history at=<T>` on the summary line; the broken-revision
pod + ReplicaSet present in the at-onset radius while the live query
returns "not found"; the changes timeline ending with the bad-revision
rollout. Target the **Pod or ReplicaSet**, not the Deployment — the
sentinel graph holds Deployments identity-only (M3 observation 3), and
`--at` instants must lie within a single sentinel process's lifetime
(observation 1: replay cannot cross a restart yet).

## 6. Cleanup

Delete the victim namespace, remove the drill flags (in particular the
`rollout.stall` promotion if it was drill-only), and keep the captured
store + stub log with your drill record — the resolved payloads are §9.3
corpus records, and the store replays the whole post-mortem offline.
