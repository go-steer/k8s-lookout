# Roadmap

Where the project is going, at the resolution of "which epic". The
detail lives in the linked issues, deliberately — a roadmap that
duplicates the tracker is a roadmap that disagrees with it within a
month.

Dates are absent on purpose. This is ordered by what unblocks what.

## Where we are

The §14 phase plan in [`docs/DESIGN.md`](./docs/DESIGN.md) is
**complete through M5**; per-milestone exit evidence is in
[`docs/milestones/`](./docs/milestones/). Everything since has been
product work rather than new milestones: the sentinel's fleet-audit
detectors, the `findings diff` transition stream, multi-cluster watch,
and the docs site.

Pre-1.0. The signal schema is frozen at
[v1](./docs/signal-schema-v1.md) and the CLI surface is governed by
[`docs/cli-stability-policy.md`](./docs/cli-stability-policy.md), so
"pre-1.0" means the *shape* of the product is still open, not that
flags churn.

## Now

**[#291 — k8sgpt assessment response](https://github.com/go-steer/k8s-lookout/issues/291).**
A competitive assessment against k8sgpt
([`docs/assessments/`](./docs/assessments/)) produced a verdict worth
quoting: *we have the better architecture and the worse product.* This
epic is the response, organised around the four things that are broken
as a product rather than as engineering:

1. **No zero-argument entry point** — fixed by `lookout scan`, which
   composes the command *registry* rather than a hand-written list, so
   it picks up new checks automatically and a contract test makes
   adding a check without deciding its scan membership fail CI.
2. **Detection holes** — StorageClass/PV binding, imagePullSecret,
   ReplicaSet `ReplicaFailure`, StatefulSet governing Service,
   IngressClass, CronJob suspend, and Gateway API on the read path.
3. **The contributor onramp** — the finding-kind ledger, the scaffold,
   one blank-import list, and
   [`docs/adding-a-check.md`](./docs/adding-a-check.md).
4. **MCP and cluster-selection ergonomics** — tool profiles, an access
   log, an authenticated non-loopback bind, and `--context`.

Plus distribution and governance: a Helm chart
([#287](https://github.com/go-steer/k8s-lookout/issues/287)), SBOM
attestations, and this file.

## Next

**[#182 — deterministic fleet-audit detectors](https://github.com/go-steer/k8s-lookout/issues/182).**
The `audit` group exists and answers "what has no safety net while it
is still healthy". What is left is the fleet-shaped half:
[#188](https://github.com/go-steer/k8s-lookout/issues/188) fleet
consistency drift across 19 config facets, and
[#184](https://github.com/go-steer/k8s-lookout/issues/184) `audit
rbac` over-permission detectors.

**[#189 — fleet rollup](https://github.com/go-steer/k8s-lookout/issues/189).**
Rollup and drift derivation over N finding streams. Note the scope
boundary from DESIGN §12: fan-out stays with the *consumer*. lookout
emits signals carrying `fingerprint`/`cluster`/`project`/`zone` so a
fleet layer can aggregate without parsing; it does not become that
layer.

**[#171 — CLI UAT coverage](https://github.com/go-steer/k8s-lookout/issues/171).**
Every command exercised against a real cluster, as a driver under
`examples/uat`. Presubmits are hermetic by policy and always will be;
this is the tier that runs outside them.

## Later, and honestly

**[#272 — deferred analyzer coverage](https://github.com/go-steer/k8s-lookout/issues/272).**
The detections we know we are missing and are *not* building yet, each
one filed with what it would detect and what would promote it. Filed
rather than dropped, because "scoped out" and "nobody ever looked" are
indistinguishable to whoever reads the tracker in six months. Most of
it — OLM, KEDA, Kyverno, Prometheus config — sat behind one structural
gap: no read-path pattern for a discovery-gated CRD detector. `state
gateway` established that pattern and closed it
([#261](https://github.com/go-steer/k8s-lookout/issues/261)), so those
are priority calls now rather than blocked work.

**A second cloud provider
([#271](https://github.com/go-steer/k8s-lookout/issues/271))** is
filed as a question, not a task. `pkg/cloud` is GCP-only today, and
AWS/EKS is a real commitment — CI, credentials, release flavors, and
someone to own it. The honest answer may be "only if a contributor
does".

**1.0** is not scheduled. What it will mean: the command surface stops
moving, and the stability policy's deprecate-then-remove window starts
applying to every flag rather than to released-and-unmarked ones. The
gate is #291 landing and the read path having a shape we would defend
for two years, not a feature count.

## How to influence this

File an issue. Detection gaps are the most useful kind — a cluster
state we failed to diagnose is worth more than a feature request,
because it is evidence. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md).
