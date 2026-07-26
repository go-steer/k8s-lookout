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

**Status: M4 complete.** Capacity & quota have shipped on top of the M3
leading indicators: the `capacity` source (structured cluster-autoscaler
signals — CA events, the status ConfigMap's target/ready gap, provider
scale decisions — never the text log) and the per-PROJECT `quota` source
(usage-vs-limit slope forecasts: "exhausted in ~6d at current slope",
never just "at 87%"), correlated so a `GCE_QUOTA_EXCEEDED` scaleup
failure and the quota forecast that predicted it are ONE incident; every
`quota.forecast` carries a drafted increase request (slope-derived
suggested limit + human-grade justification) that the agent files through
core-agent's permission gate — lookout only drafts, never mutates. Plus
`cloud stockout|orphans|ipspace|quota` point-in-time reads, distilled
memories (recurring occurrences → durable facts, §9.2), and triage-status
records (§9.4): an incident agent's diagnosis re-routes followups
(downgraded incidents stop re-paging) and `lookout health --store`
reports `triage_status=triaged` + root cause at the agent's severity
instead of a fresh unknown — measured mid-crashloop in the exit drill.
See [`docs/milestones/M4.md`](./docs/milestones/M4.md) for the evidence
([M3](./docs/milestones/M3.md): leading indicators + history;
[M2](./docs/milestones/M2.md): closed loop;
[M1](./docs/milestones/M1.md): read-path core;
[M0](./docs/milestones/M0.md): `lookout watch`, the moved
`k8s-event-watcher`, image-swap compatible). Images are published at
`ghcr.io/go-steer/lookout`. The complete specification is
[`docs/DESIGN.md`](./docs/DESIGN.md); next up is fleet & corpus (M5,
§14): the remaining reads (`state wi|webhooks|volumes`, `stab
drift|drain`, `perf probe`), the fingerprint schema finalized with AX,
the `token-burn` source, and the §9.3 corpus harvester contract
validated end-to-end.

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
