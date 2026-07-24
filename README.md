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

**Status: design phase.** The complete specification is
[`docs/DESIGN.md`](./docs/DESIGN.md); implementation follows its §14 phase
plan, starting with the move of `k8s-event-watcher` out of `core-agent` (M0).

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
