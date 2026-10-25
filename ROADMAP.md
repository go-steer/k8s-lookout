# Roadmap

Where the project is going, at the resolution of "which epic". The
detail lives in the linked issues, deliberately — a roadmap that
duplicates the tracker is a roadmap that disagrees with it within a
month.

Dates are absent on purpose. This is ordered by what unblocks what.

## Where we are

The §14 phase plan in [`docs/DESIGN.md`](./docs/DESIGN.md) is
**complete through M5**; per-milestone exit evidence is in
[`docs/milestones/`](./docs/milestones/). Subsequent epics have shipped:

- **Zero-argument incident & posture entry points** ([#291](https://github.com/go-steer/k8s-lookout/issues/291), [#793](https://github.com/go-steer/k8s-lookout/issues/793)) —
  `lookout scan` (composed from the command registry with stage-2
  dependency-edge drill-down) and `lookout audit all` (single-pass
  shared-snapshot sweep across all posture detectors: `workloads`,
  `hardening`, `rbac`, `netpol`, `namespaces`, `computeclass`, `ai`,
  `cluster`, `upgrades`, `exemptions`).
- **Declarative posture policy & baselining** ([#779](https://github.com/go-steer/k8s-lookout/issues/779), [#792](https://github.com/go-steer/k8s-lookout/issues/792)) —
  built-in profiles (`default`, `strict`, `onboarding`, `gke-autopilot`),
  YAML `--policy` overlays, `lookout audit policy`, `--exemptions`,
  `--format=findings`, and stateful transition streams (`findings diff`,
  `findings ack`).
- **Optional-CRD & placement-drift coverage** — discovery-gated
  `state gateway`, `state keda`, and `state kcc` (`*.cnrm.cloud.google.com`),
  plus runtime placement-drift sources (`topology-drift`, `compute-class`)
  and standalone `cmd/leeway`.
- **Multi-cluster & multi-tenant surfaces** — read-path fleet fan-out
  (`--contexts`, `--kubeconfig-dir`, `--concurrency`), multi-cluster
  sentinel/MCP (`--clusters-from`), and per-caller Google OAuth identity
  on `lookout mcp`.

Pre-1.0. The signal schema is frozen at
[v1](./docs/signal-schema-v1.md) and the CLI surface is governed by
[`docs/cli-stability-policy.md`](./docs/cli-stability-policy.md), so
"pre-1.0" means the *shape* of the product is still open, not that
flags churn.

## Next

**[#188 — fleet consistency drift](https://github.com/go-steer/k8s-lookout/issues/188) & [#189 — fleet rollup](https://github.com/go-steer/k8s-lookout/issues/189).**
The per-cluster `audit` detectors and `--format=findings` ledger seam are
in place ([#793](https://github.com/go-steer/k8s-lookout/issues/793)).
What remains in [#182](https://github.com/go-steer/k8s-lookout/issues/182)
is cross-cluster consistency drift across config facets ([#188](https://github.com/go-steer/k8s-lookout/issues/188))
and rollup/drift derivation over N finding streams ([#189](https://github.com/go-steer/k8s-lookout/issues/189)).
Note the scope boundary from DESIGN §12: `lookout` emits signals and
findings carrying `fingerprint`/`cluster`/`project`/`zone` so a fleet
layer can aggregate without parsing.

**[#775 — CEL-in-YAML overlay rules](https://github.com/go-steer/k8s-lookout/issues/775).**
Extending `--policy` beyond built-in detector parameters with custom
declarative rules evaluated over the shared cluster snapshot.

**[#731 — apiserver latency & scale hardening](https://github.com/go-steer/k8s-lookout/issues/731).**
Bounding apiserver list/watch pressure on large clusters.

**[#171 — CLI UAT coverage](https://github.com/go-steer/k8s-lookout/issues/171).**
Every command exercised against a real cluster, as a driver under
`examples/uat`. Presubmits are hermetic by policy and always will be;
this is the tier that runs outside them.

## Later, and honestly

**[#272 — deferred analyzer coverage](https://github.com/go-steer/k8s-lookout/issues/272).**
The detections we know we are missing and are *not* building yet (such as
OLM [#266](https://github.com/go-steer/k8s-lookout/issues/266)/[#267](https://github.com/go-steer/k8s-lookout/issues/267)
and Prometheus config [#270](https://github.com/go-steer/k8s-lookout/issues/270)),
each filed with what it would detect and what would promote it.
`state gateway`, `state keda`, and `state kcc` established the
discovery-gated CRD detector pattern, so remaining CRD analyzers are
priority calls rather than blocked work.

**A second cloud provider
([#271](https://github.com/go-steer/k8s-lookout/issues/271))** is
filed as a question, not a task. `pkg/cloud` is GCP-only today, and
AWS/EKS is a real commitment — CI, credentials, release flavors, and
someone to own it. The honest answer may be "only if a contributor
does".

**1.0** is not scheduled. What it will mean: the command surface stops
moving, and the stability policy's deprecate-then-remove window starts
applying to every flag rather than to released-and-unmarked ones.

## How to influence this

File an issue. Detection gaps are the most useful kind — a cluster
state we failed to diagnose is worth more than a feature request,
because it is evidence. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md).
