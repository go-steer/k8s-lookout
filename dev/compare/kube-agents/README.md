# Comparing lookout with kube-agents' fleet audits, live

`dev/compare/kube-agents/` runs [kube-agents](https://github.com/gke-labs/kube-agents)'
scheduled governance audits and `lookout` against the **same fleet, through
the same kubeconfigs**, and joins what each found object by object, beside
what each cost (wall time, CPU, subprocess fan-out, bytes pulled from the
API server and GCP, bytes an agent must read to reach a finding).

It installs nothing. kube-agents' collectors are stdlib-only Python that
take `--project` and self-wire their one sibling import; the harness runs
them in place from a checkout with their state directories redirected
under `--out`. Because they shell out to `kubectl` and `gcloud`, a PATH
shim ([`bin/`](bin/)) records every call's argv, latency, byte count and
exit code without touching kube-agents.

Everything is read-only: the collectors issue `gcloud … list/describe` and
`kubectl get`; lookout is read-only by design. Nothing in either repository,
`~/.kube/config`, or `/opt/data` is written.

## Prerequisites

- A kube-agents checkout.
- `gcloud` authenticated with read access to the project's clusters
  (`container.clusters.get`, `container.clusters.list`; Cloud Monitoring
  read for `fleet_waste.py`; Compute read for the waste/networking/GCE
  streams).
- `kubectl`, `jq`, `python3` (3.10+) with `pyyaml`, `/usr/bin/time`.
- A `lookout` built with the GKE provider:
  `go build -tags gke -o /tmp/lookout ./cmd/lookout`
- Optionally the standalone [`go-steer/leeway`](https://github.com/go-steer/leeway)
  binary (`--leeway`), whose one-shot `leeway report` is what kube-agents'
  `topology-drift` skill runs; without it the two placement-drift rows
  have no lookout-side evidence. (k8s-lookout's own `cmd/leeway` is the
  resident exporter and is not used here.)


## Run

```sh
dev/compare/kube-agents/run.sh \
  --kube-agents-dir ~/projects/kube-agents \
  --project my-gcp-project \
  --lookout /tmp/lookout \
  --out /somewhere/outside/the/repo/ka-compare-$(date +%F)
```

`--phase` runs one step; the default is all of them in order:

| phase | what runs | network |
| --- | --- | --- |
| `roster` | [`roster.py`](roster.py): the check roster from `audit_report.AUDITS`, per-slug collector coverage, SOP sections, the bytes each cron prompt mandates reading | no |
| `ka` | [`ka-run.sh`](ka-run.sh): the nine cron streams' collectors (`collect.py` ×3, `fleet_drift.py`, `patch_readiness.py`, `fleet_stockout.py`, `fleet_waste.py`, `networking_audit.py`, `compute_fleet_audit.py`) plus `upgrade_retrospective.py --dry-run`, each under `/usr/bin/time -v` and the shims | yes |
| `lookout` | [`lookout-run.sh`](lookout-run.sh): the read-path matrix per cluster with `--format json`, through the kubeconfigs the collectors fetched; then `scan` on every cluster at once | yes |
| `prose` | [`prose-run.sh`](prose-run.sh): the onboarding `cluster_inventory_audit_sop.md` recipes (the one audit kube-agents still runs as prose) as kubectl+jq | yes |
| `watchers` | [`watchers-run.sh`](watchers-run.sh): the `no_agent` watchers' leaf scripts — `stall_report.py --json` per namespace, `fleet_upgrade_report.py --readiness` | yes |
| `crosswalk` | [`crosswalk.py`](crosswalk.py): joins both sides on [`equivalence.yaml`](equivalence.yaml) and writes `report/{crosswalk,timing,cost}.md`, `summary.json`, `findings.json` | no |

`--only-streams a,b` limits the `ka` phase; `--waste-timeout` bounds
`fleet_waste.py` (default 45m: it reads a week of Cloud Monitoring per
workload); `--lookout-timeout` is passed as every lookout command's
`--timeout` (default 5m).

## Reading the output

`report/crosswalk.md` has one row per kube-agents check: how many objects
it flagged, how many lookout flagged under the mapped kinds, how many are
the same object, and a verdict (`parity`, `parity + beyond`, `partial
match`, `MISS, live`, `GAP, live`, `both silent`, `lookout-only`,
`intentional`, `out-of-scope`). The `claim` column is the pre-run
expectation from the source review; the live join grades it.

`report/timing.md` is the cost side; `report/cost.md` is what an agent
must read (SOP + skill + manifest vs. lookout stdout), with tokens as a
stated 4-bytes/token estimate.

`trace/*.jsonl` holds every subprocess the shims saw — sizes and argv
only, never payloads.

## Caveats

- Autopilot clusters: `collect.py` downgrades severities there; lookout's
  provider-backed checks may answer `unavailable` where a capability is
  off. The cross-walk counts keys, not severities, so both show up as
  rows, not as agreement.
- Clusters only one side could reach are reported as `unreached`, not as
  misses.
- Cluster-scoped GKE facets join on the cluster alone; the two tools name
  objects differently there.
- This is a live-project exercise and is **not** wired into
  `dev/tools/ci`; live-project tests stay out of CI (AGENTS.md).
