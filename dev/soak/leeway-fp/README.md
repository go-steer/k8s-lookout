# leeway false-positive soak

Leeway (#416) is on by default, and its detection is proven end to end by
`examples/scenarios/placement-drift` and `zone-unavailable`. Those scenarios
show it fires when it should. This soak tests the other half: that it stays
quiet when it should. It runs the sentinel at production leeway settings
against a cluster that churns the way a healthy cluster churns, for days,
and records every leeway finding. With no fault injected the right number is
zero, so each finding is either a false positive or real drift that the
churn produced. The summary sets out the evidence for deciding which.

This is not a CI step. Like `dev/tools/soak-otlp`, it is something you run
on a spare machine and paste the verdict from.

## What it builds

- **Cluster.** A kind cluster with one control plane and six workers:
  - two workers in each of `zone-a`, `zone-b` and `zone-c` on the real
    `topology.kubernetes.io/zone` label;
  - two node pools (`cloud.google.com/gke-nodepool`), each spanning all
    three zones, so that node-group subjects are scored too.
- **Sentinel.** The shipped `deploy/` manifests with the shipped args.
  - Every `--topology-*` flag stays at its default: dwell 10m, learned
    baselines on, assumed cluster defaults.
  - `--topology-tier-c-signals` and `--compute-class-tier-c-signals` are
    added so that Tier C findings are counted as well.
  - `--sources` rides `auto`, as the shipped manifest does.
  - It runs on the control plane together with the capture stub. Churn
    never touches either.
- **Workloads.** 36 workloads in `fp-soak`, built by `workloads.py`:
  - no intent at all;
  - zone spread, soft and hard;
  - anti-affinity, required and preferred, on zone and on host;
  - podAffinity colocation;
  - two StatefulSets and five PDBs.
  - Every pod is a `pause` container, so the scheduler's spread logic
    decides placement and capacity never does.
  - Every pod requires a node that has the zone label, as every node a
    GKE workload can land on does. That keeps kind's unlabelled control
    plane out of leeway's eligible set (see below).
- **Capture stub.** `dev/drills/stub-daemon.py`'s handler, served by
  `serve.py` on a threaded server. The drill stub is single-threaded,
  and in the smoke run a blocked keep-alive connection lost injects.

### The unlabelled-node false positive

The first smoke run found one, and the soak works around it rather than
measuring it again.

- **What leeway does.** Leeway counts a node with no value for a spread
  key as an eligible domain, `__unknown__`. Under the default
  `nodeTaintsPolicy: Ignore`, a tainted node is eligible too.
- **What kube-scheduler does.** It leaves nodes without the key out of
  topology spread entirely.
- **What went wrong.** kind's control plane became an empty fourth zone,
  with an expected share of the pods. About 10 minutes after the
  workloads came up, every evenly spread zone-spread workload (3/3/3,
  4/4/4) had raised `rollout_bias` at Tier A, and the findings formed a
  storm.
- **Why cordoning doesn't help.** A cordoned node that has no label
  turns into an `__unknown__` domain with no schedulable nodes. Leeway
  then raises `domain_unavailable` instead.
- **To reproduce it,** run `SOAK_ZONE_AFFINITY=0 soak up`.
- **The fix in leeway** is #534. A run on a build that includes it can
  drop the workaround. The summary's `build` line shows which build a
  run used. The first 72-hour run used `v0.30.0-dev-535a379` (main at
  #531), so it predates #534.
- **Churn.** Run by `churn.py`, seeded, with every action logged.
  - Every 1–4 minutes it does one of: a rollout restart, an image-tag
    bump, a scale of ±1..3 within bounds, or a pod deletion.
  - An "autoscaled" workload follows a 90-minute sine.
  - Every 30–90 minutes it disrupts **one** node, in one of two ways:
    - drain it, hold it cordoned for 2–7 minutes, then uncordon;
    - `docker pause` it for 50–100 s, so the node blips NotReady.
  - No zone ever loses both of its nodes.

## What it records

All of it goes under `$SOAK_DIR`.

| file | what |
|---|---|
| `wire/wire.log` | every request the sentinel sent to the stub, timestamped (sessions, injects, digests, storms) |
| `sentinel.log`, `sentinel-final.log` | the sentinel's log, which includes one `info-store leeway.…` line per store-routed finding |
| `store/lookout.db` | the sentinel's `--store`, for the full payloads |
| `metrics.log` | the `lookout_leeway_*` families plus the routing counters, every 10 min |
| `placement.log` | every 10 min: node zones and readiness, and every pod's node and owner |
| `churn.log` | every churn action |
| `summary.txt` | the output of `summarize.py` |

The summary lists each distinct finding once. For each it gives:
- its route, tier, cause and transient;
- the churn that touched that workload, plus every node operation, in the
  preceding 30 minutes;
- the workload's per-zone placement at the nearest sample.

It ends with a `VERDICT:` line.

How to read a finding:
- A finding raised on a scale-down whose placement really is skewed is real
  drift. The ReplicaSet controller chooses victims without regard to spread.
- A finding whose placement is not skewed, or one that tracks a drain or a
  blip that §7.6 should have relaxed, is a false positive.

## Running it

On any Linux host with docker, kind, kubectl and python3:

```sh
docker build -t lookout:fp-soak .
SOAK_DIR=$PWD/out dev/soak/leeway-fp/soak up
SOAK_DIR=$PWD/out SOAK_CHURN_SCALE=0.25 dev/soak/leeway-fp/soak run 45m   # smoke
SOAK_DIR=$PWD/out dev/soak/leeway-fp/soak run 72h
dev/soak/leeway-fp/soak down
```

The script sets `KUBECONFIG=$SOAK_DIR/kubeconfig` and never reads yours.
For a full run, start from a fresh cluster (`down`, then `up`; `up`
reuses a cluster of the same name) and a fresh `SOAK_DIR`, so that the
smoke run's baselines, episodes and wire log don't carry over.

### On a throwaway GCE VM

A multi-day run outlives a workstation session, so use a VM: an
e2-standard-8 with no external IP, reached over IAP. It needs Cloud NAT in
its region for the one-off downloads; the kind nodes themselves never pull
from a registry.

```sh
gcloud compute instances create lookout-soak-leeway-fp --zone us-central1-a \
  --machine-type e2-standard-8 --image-family debian-12 --image-project debian-cloud \
  --boot-disk-size 50GB --no-address --labels purpose=lookout-soak,issue=416 \
  --max-run-duration 78h --instance-termination-action STOP
docker save lookout:fp-soak -o /tmp/lookout-fp-soak.tar
tar czf /tmp/bundle.tgz deploy dev/drills/stub-daemon.py dev/soak/leeway-fp
gcloud compute scp --tunnel-through-iap /tmp/bundle.tgz /tmp/lookout-fp-soak.tar lookout-soak-leeway-fp:/tmp/
gcloud compute ssh --tunnel-through-iap lookout-soak-leeway-fp --command '
  sudo mkdir -p /opt/fpsoak && sudo tar xzf /tmp/bundle.tgz -C /opt/fpsoak &&
  sudo mv /tmp/lookout-fp-soak.tar /opt/fpsoak/ && sudo /opt/fpsoak/dev/soak/leeway-fp/vm-bootstrap'
gcloud compute ssh --tunnel-through-iap lookout-soak-leeway-fp --command '
  cd /opt/fpsoak && sudo systemd-run --unit fpsoak --working-directory=/opt/fpsoak \
    --setenv=SOAK_DIR=/opt/fpsoak/run --setenv=LOOKOUT_IMAGE_TAR=/opt/fpsoak/lookout-fp-soak.tar \
    /bin/bash -c "dev/soak/leeway-fp/soak up && dev/soak/leeway-fp/soak run 72h; sync; poweroff" \
    >/dev/null'
```

The unit powers the VM off when the run ends, and `--max-run-duration`
stops it regardless. A VM in state `TERMINATED` after the planned end is
the normal finish. Start it again to read the results: the kind cluster
does not come back, and doesn't need to.

```sh
gcloud compute instances start lookout-soak-leeway-fp --zone us-central1-a
gcloud compute ssh --tunnel-through-iap lookout-soak-leeway-fp --command 'sudo cat /opt/fpsoak/run/summary.txt'
```

Delete the VM once the verdict is recorded.
