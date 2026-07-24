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

**Status: M1 complete.** The read-path core has shipped: `lookout
triage delta|logs|spec`, `state edges`, `bundle`, `health`, the §4.2 output
envelope with the §6.5 sanitizer on every surface, the `pkg/graph` topology
index, `lookout mcp` (every check as an MCP tool), and the first workflow
skills ([`skills/`](./skills/) — `k8s-triage`, `cluster-health`, and the
per-symptom playbooks). An incident session can be investigated with lookout
tools alone, and `lookout health` answers "any issues in this cluster?" in
one call — see [`docs/milestones/M1.md`](./docs/milestones/M1.md) for the
exit-check evidence ([M0](./docs/milestones/M0.md): `lookout watch`, the
moved `k8s-event-watcher`, image-swap compatible). Images are published at
`ghcr.io/go-steer/lookout`. The complete specification is
[`docs/DESIGN.md`](./docs/DESIGN.md); next up is the closed loop (M2, §14):
recovery injects, storm correlation, severity routing, and enrichment.

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
