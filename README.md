# k8s-lookout

Data-plane intelligence for [`core-agent`](https://github.com/go-steer/core-agent):
deterministic, token-dense eyes on Kubernetes/GKE clusters for LLM-driven
troubleshooting agents.

Two halves, one multicall binary (`lookout`):

- **Read-path** — one-shot diagnostic commands an agent runs mid-investigation
  (`lookout triage|state|stab|perf|cloud|bundle|health`), emitting compressed,
  secret-safe, logfmt/JSON findings instead of raw telemetry dumps.
- **Watch-path** — `lookout watch`, a resident per-cluster sentinel that turns
  leading indicators (state transitions, trend slopes, expiry countdowns) into
  per-incident agent sessions with warm context — detecting issues before, or
  as, they happen rather than after.

**Status: all six milestones complete (M0–M5), v0.6.0.** The §14 phase
plan is done: every §4.1 command has shipped, all nine §7.2 signal
sources run in the sentinel, and M5 closed the fleet & corpus layer —
the signal schema is **frozen as v1**
([`docs/signal-schema-v1.md`](./docs/signal-schema-v1.md)): every
payload carries a stable incident-class `fingerprint` plus
`cluster`/`project`/`zone`, so AX rolls up a fleet-wide symptom as a
join, not a parsing project (demonstrated by the two-cluster staged
stockout simulation), and scan findings carry the SAME fingerprints as
sentinel pushes (one schema for push and pull). The §9.3 verified-fix
corpus is harvestable end-to-end: `dev/tools/harvest-corpus` extracts
labeled trajectories — symptom → diagnosis → action → externally
verified outcome — from a captured inject stream by pure schema walks,
no NLP. See [`docs/milestones/M5.md`](./docs/milestones/M5.md) for the
exit evidence and the post-M5 review backlog
([M4](./docs/milestones/M4.md): capacity & quota;
[M3](./docs/milestones/M3.md): leading indicators + history;
[M2](./docs/milestones/M2.md): closed loop;
[M1](./docs/milestones/M1.md): read-path core;
[M0](./docs/milestones/M0.md): `lookout watch`, the moved
`k8s-event-watcher`, image-swap compatible). Images are published at
`ghcr.io/go-steer/lookout` (`:<version>-gke` for the GCP-linked
flavor). The complete specification is
[`docs/DESIGN.md`](./docs/DESIGN.md).

## Command surface

| Group | Commands |
| --- | --- |
| resident | `watch` (the sentinel: nine sources, dedup, storms, severity routing, watchboard, enrichment, recovery injects, occurrence store, distiller) |
| serving | `mcp` (every read command 1:1 as an MCP tool) |
| composed | `bundle` (first call of every incident), `health` (ten-category scorecard + §9.4 triage merge) |
| `triage` | `delta`, `logs`, `events`, `top`, `radius`, `changes`, `spec`, `status` |
| `state` | `edges`, `webhooks`, `wi`, `volumes` |
| `stab` | `drift`, `drain` |
| `perf` | `probe --pack=apiserver\|apf\|etcd\|startup` |
| `cloud` | `stockout`, `orphans`, `ipspace`, `quota` |
| `net` | `probe --dns\|--tcp\|--http` |

Agent education ships in [`skills/`](./skills/README.md): `k8s-triage`,
`cluster-health`, `gitops-drift`, and per-symptom `playbooks/`.

Roughly 80% of the suite is pure `client-go` and runs on any conformant
Kubernetes cluster; GKE/GCP-specific capability lives behind a cloud-provider
boundary (`DESIGN.md` §2).

## Ecosystem

| Repo | Role |
| --- | --- |
| [`core-agent`](https://github.com/go-steer/core-agent) | the agent daemon; lookout talks to it over `POST /sessions` + `/inject` |
| [`ax`](https://github.com/go-steer/ax) | fleet layer; consumes lookout's rollup-ready signal schema across clusters |

## License

Apache 2.0 — see [LICENSE](./LICENSE).
