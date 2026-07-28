# k8s-lookout

Deterministic, token-dense eyes on Kubernetes/GKE clusters for LLM-driven
troubleshooting agents — the data-plane companion to
[`core-agent`](https://github.com/go-steer/core-agent).

One multicall binary, `lookout`, with two halves:

- **Read-path** — one-shot diagnostic commands an agent runs
  mid-investigation (`bundle`, `health`, `triage …`, `state …`, …).
  Each emits compressed, secret-safe logfmt/JSON findings instead of raw
  telemetry dumps, and always ends with an explicit summary line so
  "cluster healthy" is never ambiguous silence.
- **Watch-path** — `lookout watch`, a resident per-cluster sentinel that
  turns leading indicators (state transitions, trend slopes, expiry
  countdowns) into per-incident agent sessions with warm context —
  catching issues before, or as, they happen rather than after.

Not predictive/ML: every leading indicator is deterministic arithmetic.
The complete specification is [`docs/DESIGN.md`](./docs/DESIGN.md); for
where things live in the tree, see the
[repo architecture map](./docs/repo-map.md).

<!-- TODO: link the documentation site once docs/site/ + its Pages deploy workflow land. -->

[![CI](https://github.com/go-steer/k8s-lookout/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/go-steer/k8s-lookout/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)

---

## Install

**Container images** (multi-arch amd64 + arm64; distroless static;
Sigstore-signed):

```sh
docker pull ghcr.io/go-steer/lookout:latest       # default: GCP-free, runs on any conformant cluster
docker pull ghcr.io/go-steer/lookout:latest-gke   # same binary + GKE/GCP provider (-tags allproviders)
```

The image's `ENTRYPOINT` is `lookout watch`, drop-in image-swap
compatible with `ghcr.io/go-steer/k8s-event-watcher` deployments.
Verify signatures with:

```sh
cosign verify ghcr.io/go-steer/lookout:vX.Y.Z \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

**From source** (Go 1.26+):

```sh
go install github.com/go-steer/k8s-lookout/cmd/lookout@latest
```

## Quickstart — read-path

Every read command works against your current kubeconfig context, no
deployment needed. "Any issues with this cluster?" in one call:

```sh
lookout health
```

```console
kind=health.category severity=info reason=Unavailable message="requires cloud provider metrics (M4); no cloud provider configured" category=control-plane status=unavailable
kind=health.category severity=info category=nodes status=healthy
kind=health.category severity=warning category=crashloops status=degraded total=8 top="pod.restarts agent-sandbox-system/agent-sandbox-controller-7c69875fcc-n7xms; pod.restarts kube-system/coredns-7d764666f9-g82j9; …"
kind=health.category severity=info category=pending status=healthy
kind=health.category severity=info category=rollouts status=healthy
…
kind=pod.restarts severity=warning namespace=kube-system kind_of_object=Pod name=coredns-7d764666f9-g82j9 reason=ExcessiveRestarts fingerprint=sha256:e094a6ee… category=crashloops container=coredns restarts=62
scanned=16 findings=18 elapsed=537ms
```

(Real output against a kind cluster, abridged.) Healthy resources are
omitted; categories still answer explicitly. `lookout bundle
--workload=Deployment/prod/api` is the first call of an incident: one
correlated snapshot instead of 4–5 separate reads. Exit 0 is pure
payload on stdout; diagnostics go to stderr only.

## Quickstart — sentinel

The watch-path deploys per cluster from [`deploy/`](./deploy/):

```sh
kubectl apply -f deploy/
```

That gives the sentinel a dedicated ServiceAccount with a
minimum-necessary, **read-only** ClusterRole (it observes; all
mutations happen through the core-agent daemon's own permission gate).
Point `--daemon-url` at your core-agent daemon in
`deploy/51-deployment-watcher.yaml`. Namespace-scoped (Role-only) and
project-tier (quota source, `-gke` image) deployments are covered in
DESIGN.md §11 — sources whose RBAC scope isn't satisfied fail loudly at
startup, never as a silently empty watch.

Want to see both halves against real failures first?
[`examples/`](./examples/) stands up a kind cluster, the sentinel
against a capture stub, demo workloads, and ten inject/verify/revert
failure scenarios (`examples/e2e` runs the lot) — plus the recipe for
testing the CLI through agent harnesses via skills or MCP.

## Command surface

| Command | What it does |
| --- | --- |
| `watch` | resident sentinel: nine signal sources → filter → dedup → storm correlation → severity routing → enrichment → session injects |
| `mcp` | serve every read command 1:1 as MCP tools (stdio or localhost HTTP) — how a distroless daemon calls lookout |
| `bundle` | first call of every incident: sanitized spec + abnormal objects + broken edges + blast radius + distilled logs, one payload |
| `health` | ten-category cluster scorecard, merged with open sentinel findings and triage-status records |
| `triage delta` | one scan → everything abnormal: broken workloads, aged Pending, node pressure, gridlocked PDBs, degraded add-ons, hit quotas |
| `triage logs` | template-fingerprint log dedup: ~150k tokens of logs → ~350 |
| `triage events` | deduped chronological event timeline over the owner-reference tree |
| `triage top` | CPU/mem saturation vs limits, right now |
| `triage radius` | blast radius of a workload from the topology index; `--at` answers "at incident onset" |
| `triage changes` | "what changed before onset": rollouts, config updates, rescales, scoped to the graph neighborhood |
| `triage spec` | kubectl describe for agents: sanitized, token-dense, `--diff` against graph history |
| `triage status` | read/write §9.4 triage-status records so later scans report triaged reality, not a fresh unknown |
| `state edges` | dependency-graph verification: config/secret keys, selectors, endpoints, TLS expiry |
| `state webhooks` | admission webhooks failing closed with dead backends |
| `state wi` † | GKE Workload Identity KSA↔GSA binding verification |
| `state volumes` | RWO multi-attach / cross-zone PV locks |
| `stab drift` | out-of-band drift vs the GitOps manager via managedFields |
| `stab drain` | everything that will block a node drain |
| `perf probe` † | control-plane metric packs: `apiserver`, `apf`, `etcd`, `startup` |
| `cloud stockout\|orphans\|ipspace\|quota` † | GCP-side reads: zonal stockouts, orphaned disks/LBs, CIDR utilization, quota headroom |
| `net probe` | active DNS/TCP/HTTP checks from inside the cluster |

† needs a cloud provider (the `-gke` image / `-tags gke` build). ~80%
of the suite is pure client-go and runs on any conformant cluster; the
default build links zero GCP SDKs. Provider-gated commands never break
or lie on vanilla clusters — they emit an explicit finding and exit 0:

```console
kind=cloud.unavailable severity=info reason=CapabilityUnavailable message="cloud quota needs the provider quota capability: no cloud provider configured" capability=quota provider=none
scanned=0 findings=1 elapsed=0s unavailable="no cloud provider configured"
```

Agent education ships in [`skills/`](./skills/README.md): `k8s-triage`,
`cluster-health`, `gitops-drift`, and per-symptom `playbooks/` — skills
teach the decision tree across commands and install into the consuming
deployment's `.agents/skills/`.

## Ecosystem

| Repo | Role |
| --- | --- |
| [`core-agent`](https://github.com/go-steer/core-agent) | the agent daemon; lookout talks to it over `POST /sessions` + `/inject` |

## Documentation

- [`docs/DESIGN.md`](./docs/DESIGN.md) — the complete, normative specification
- [`docs/repo-map.md`](./docs/repo-map.md) — repo architecture map: layout, data paths, frozen contracts
- [`docs/signal-schema-v1.md`](./docs/signal-schema-v1.md) — the frozen fleet-rollup wire contract
- [`docs/milestones/`](./docs/milestones/) — per-milestone exit evidence (M0–M5 complete, v0.6.0)
- [`CHANGELOG.md`](./CHANGELOG.md)

## License

Apache 2.0 — see [LICENSE](./LICENSE).
