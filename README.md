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

**Status: M2 complete.** The closed loop has shipped on top of the M1
read-path core: `lookout watch` is now the §7.1 signal engine — pluggable
sources (`k8s-events` plus the leading-indicator `object-state` source),
storm correlation via graph blast-radius keys (`--storm`: a node failure
opens ONE `kind=storm` session, not one per evicted pod), recovery injects
(`kind=resolved` outcome records into the incident's own session when the
symptom verifiably clears — fix-verify without agent polling), severity
routing with the shared watchboard digest for warning-class noise, and
§7.6 enrichment (the in-process `bundle` attached to critical sessions, so
they start warm). Measured in the exit drill: 33 affected objects → 3
sessions (1 storm); fix → `resolved` in 76 s with zero polling — see
[`docs/milestones/M2.md`](./docs/milestones/M2.md) for the evidence
([M1](./docs/milestones/M1.md): read-path core;
[M0](./docs/milestones/M0.md): `lookout watch`, the moved
`k8s-event-watcher`, image-swap compatible). Images are published at
`ghcr.io/go-steer/lookout`. The complete specification is
[`docs/DESIGN.md`](./docs/DESIGN.md); next up are the leading-indicator
sources and history (M3, §14): `rollout`, `saturation`, `degradation`,
`expiry`, the raw store with graph snapshots, and the `--at` time-travel
queries.

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
