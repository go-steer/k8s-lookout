# UAT T2/T3 on GKE — the GKE and cloud commands, live

The live half of the CLI UAT plan ([`docs/testing/cli-uat.md`](../../docs/testing/cli-uat.md),
tiers T2 and T3; issues #179 and #180). It checks the commands that only
mean something against a real GKE cluster and real Google Cloud APIs:
`state wi`, `state volumes` (cross-node Multi-Attach), `perf probe`,
`stab drift --identity`, `triage top --history`, and the `cloud` group
(`orphans`, `quota`, `ipspace`, `stockout`). It also checks that the
default, GCP-free image degrades explicitly for each of them.

Like every runbook here it is human-run and never gates a PR.

> **STAGING PROJECTS ONLY.** `up` creates a GKE cluster, two service
> accounts, a disk, two addresses, a load balancer and an extra node
> pool, and plants faults on purpose. `cloud orphans` and `cloud
> stockout` also read whatever else the project holds, so their output
> includes other people's leftovers.

## How it runs

Every lookout invocation runs **inside the cluster**, as a pod under a
Workload-Identity service account, on the published images
(`IMAGE_GKE`, default `ghcr.io/go-steer/lookout:v0.42.0-gke`, and
`IMAGE_DEFAULT` for the GCP-free checks). That is how the `-gke` image is
deployed, and it is also the only place it can resolve the cluster's
location and name: the read commands take them from the GKE metadata
server, so the `-gke` binary run from a laptop cannot serve the
cluster-scoped cloud commands.

## Steps

```sh
cd dev/drills/uat-gke
export PROJECT=<staging-project>      # ZONE defaults to us-central1-a
./up          # cluster + fixtures, ~10 minutes; idempotent
./grant-iam   # the IAM step: review it, then run it yourself
# wait ~10 minutes: LB provisioning, metrics and audit-log ingestion
./run         # assertions; verdicts in out/summary.txt, raw output in out/<case>.txt
./down        # deletes everything up and grant-iam created
```

`./run t2_wi t3_ipspace` runs only the named case blocks.

## IAM the `-gke` image needs

`grant-iam` grants the runner's GSA these read-only project roles, one
per capability the commands call, plus `roles/iam.workloadIdentityUser`
for the runner's KSA:

| Role | Used by |
| --- | --- |
| `roles/compute.viewer` | `cloud orphans` (disks, addresses, forwarding rules), `cloud ipspace` (subnetworks), `cloud quota` (project and region quotas) |
| `roles/dns.reader` | `cloud orphans` address cross-check against Cloud DNS records |
| `roles/container.clusterViewer` | `cloud ipspace` (clusters.get), `cloud orphans --only=nodepools` |
| `roles/monitoring.viewer` | `perf probe`, `triage top --history` |
| `roles/logging.viewer` | `stab drift --identity` (Admin Activity audit logs), `cloud stockout` |
| `roles/cloudquotas.viewer` | `cloud quota` (quota IDs) |
| `roles/iam.securityReviewer` | `state wi` (each GSA's IAM policy) |
| `roles/serviceusage.serviceUsageConsumer` | quota-project billing for the API calls |

The in-cluster reads use the shipped `lookout-watch` ClusterRole
(`deploy/12-clusterrole-watcher.yaml`).

## Fixtures and what each case asserts

| Case | Fixture | Assertion |
| --- | --- | --- |
| `t0_*` | none (default image) | every cloud command exits 0 with `cloud.unavailable provider=none`; `state wi` and `perf probe` degrade the same way |
| `t2_wi` | `uat-wi`: a KSA annotating a GSA that doesn't exist, one annotating a GSA with no WI binding, a pod with `GOOGLE_APPLICATION_CREDENTIALS` and no annotation | `wi.gsa_missing`, `wi.unbound`, `wi.unannotated_use`; the correctly bound runner KSA is clean |
| `t2_volumes` | `uat-volumes`: one RWO claim, a holder pod on node 0 and a blocked pod on node 1 | `volume.multi_attach` naming both pods |
| `t2_perf_*` | control-plane metrics on the cluster | each pack exits 0 and is metric-backed; an unknown pack exits 2 |
| `t2_drift` | `uat-drift`: a Deployment applied client-side, then patched by `--field-manager rogue-operator` | `drift.manual_edit` naming `rogue-operator`, and `--identity` resolving the audited principal |
| `t2_top` | `uat-top`: a busybox loop pinned at its CPU limit | `top.saturation` for the burner with `max_pct`/`avg_pct`/`p95_pct` from Cloud Monitoring |
| `t3_orphans*` | an unattached disk, an unused external address, an internal reservation, a LoadBalancer selecting nothing, a tainted idle node pool | each leftover listed under its kind; the internal address never; `--only` filters; `--min-age` excludes fresh ones; the idle pool only under `--only=nodepools`, as info `IdleMinNodeCount` |
| `t3_quota*` | the project's real quotas | exits 0; `--quota-warn=1` and `--all` produce `quota.pressure` rows |
| `t3_ipspace*` | a /21 pod range in /23 per-node blocks (four blocks) | 3 nodes = pods at 75%, below the line; services reported as not cloud-visible; scaling the idle pool to a 4th node = pods critical at 100% |
| `t3_stockout*` | the project's real audit log | exits 0 with defaults; a 30-day window (`--since=720h`) needs `--timeout` above the 10s default |

Gotchas found while building it:

- `standard-rwo` binds on first consumer, so the Multi-Attach pods are
  pinned with a `kubernetes.io/hostname` nodeSelector, never `nodeName`
  (which skips the scheduler and leaves the claim unprovisioned).
- `ipspace` counts nodes from the cluster record's `currentNodeCount`,
  which trails a resize; `run` waits for it.
- Resizing must use the idle pool: removing a default-pool node deletes
  the Multi-Attach pods.
- `stab drift` needs `--manager` here, because no GitOps controller is
  installed to auto-detect.

## Results

The first run (2026-10-11, v0.35.0 images): 52 assertions passed and 3
failed. The failures and the other findings are filed as issues; see
the comments on #179 and #180.
