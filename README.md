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

**Status: M3 complete.** Leading indicators and history have shipped on
top of the M2 closed loop: four new signal sources (`rollout` —
evidence-based stall detection ahead of `progressDeadlineSeconds`;
`saturation` — linear usage-vs-limit forecasts with wire-attached
`forecast{eta, confidence_basis}`; `degradation` — ready-ratio trends and
probe-flap counting; `expiry` — certificate/token countdowns), the §9.1
raw occurrence store (`--store`: one bounded SQLite recording every signal
with its routing outcome) with §6.6 graph history (compressed snapshots +
a per-delta change log), and the history-consuming read path: `triage
events|top|radius|changes` and `net probe`, with `--at=<instant>
--store=<file>` answering point-in-time questions offline. Measured in the
exit drills: a staged bad deploy opened a session 3m10s in while every
user request returned 200; a staged memory leak got a critical session 14
minutes before the OOM kill (forecast ETA accurate to 31 s); "blast radius
at onset" was answered 28m34s after the fact from a copied store — see
[`docs/milestones/M3.md`](./docs/milestones/M3.md) for the evidence
([M2](./docs/milestones/M2.md): closed loop;
[M1](./docs/milestones/M1.md): read-path core;
[M0](./docs/milestones/M0.md): `lookout watch`, the moved
`k8s-event-watcher`, image-swap compatible). Images are published at
`ghcr.io/go-steer/lookout`. The complete specification is
[`docs/DESIGN.md`](./docs/DESIGN.md); next up is capacity & quota (M4,
§14): the `capacity` + `quota` sources, `cloud
stockout|orphans|ipspace|quota`, distilled memories (§9.2), and the
store/memory-merged `health`.

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
