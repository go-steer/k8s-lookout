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

**Status: M0 complete.** `lookout watch` has shipped: the `k8s-event-watcher`
sidecar moved here from `core-agent` verbatim — flags, wire payloads, and exit
codes are behavior-identical, so existing watcher deployments swap images with
zero config change (see [`docs/milestones/M0.md`](./docs/milestones/M0.md) for
the exit-check evidence). Images are published at `ghcr.io/go-steer/lookout`
starting with `v0.1.0`. The complete specification is
[`docs/DESIGN.md`](./docs/DESIGN.md); next up is the read-path core (M1, §14).

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
