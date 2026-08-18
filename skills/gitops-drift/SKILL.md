---
name: gitops-drift
description: Answer "is the cluster still what Git says it should be?" — audit out-of-band edits with lookout stab drift (who diverged, which spec fields) and reconstruct the change timeline with lookout triage changes (what changed, when). Use for drift audits, "who touched prod", and pre-sync sanity checks on GitOps-managed clusters.
---

# gitops-drift — divergence auditing with lookout

Two different questions, two commands (DESIGN.md §4.4):

- **Who diverged?** — `lookout stab drift`: which spec fields of
  which workloads are owned by a manager OTHER than the GitOps
  controller, right now. Point-in-time ownership truth from
  `managedFields` — it does not matter *when* the edit happened, it
  matters that the field is no longer Git's.
- **What changed?** — `lookout triage changes`: the chronological
  record of rollouts, config/secret updates, rescales, and node ops
  around one workload. It answers sequence and timing, not ownership.

Run `stab drift` to FIND divergence; run `triage changes` on what it
names to explain HOW the divergence got there (and what else moved
around the same time).

## Who diverged: `stab drift`

```lookout
lookout stab drift
lookout stab drift --namespace=prod --manager=argocd-controller
```

The GitOps manager is auto-detected (owner of a strict majority of the
spec fields in scope) unless `--manager` declares it; the summary line
reports which (`manager=… detection=declared|majority share=…`).
Findings name the foreign manager, the operation, and the exact spec
paths it owns:

```lookout-golden
kind=drift.manual_edit severity=critical namespace=prod kind_of_object=Deployment name=api reason=KubectlManualEdit message="manager \"kubectl-edit\" (Update) owns 2 drifted spec fields: spec.replicas +1 more, last write 3h20m ago" manager=kubectl-edit operation=Update tool=kubectl fields=spec.replicas,spec.template.spec.containers[app].image field_count=2 age=3h20m
kind=drift.manual_edit severity=warning namespace=prod kind_of_object=Deployment name=worker reason=OutOfBandManager message="manager \"helm-legacy\" (Update) owns 1 drifted spec field: spec.template.spec.terminationGracePeriodSeconds" manager=helm-legacy operation=Update fields=spec.template.spec.terminationGracePeriodSeconds field_count=1
scanned=3 findings=2 elapsed=100ms manager=argocd-controller detection=majority share=86%
```

- `reason=KubectlManualEdit` (a kubectl-shaped manager string,
  `tool=kubectl`) is critical: a human edited prod by hand. Other
  foreign managers (`reason=OutOfBandManager`) are warning: a rogue
  co-manager (legacy Helm, an operator fighting the controller).
- `manager` is a TOOL name from `managedFields`, never a user
  identity. To attribute the edit to a person, re-run with
  `--identity`: on a cluster with a cloud audit trail (GKE) each
  finding gains `principal` (who wrote it), `principal_agent` (their
  client), and `other_principals`; without one the summary carries an
  explicit `identity=unavailable` marker. The sentinels
  `none-in-audit-window` / `no-write-time-anchor` mean the trail
  could not answer for that finding — retention, disabled audit
  logging, or a write the API server never timestamped.
- `fields` are the drifted spec paths — names, never values. Diff
  the paths against the Git manifest to see the intended state.
- `findings=0` with `detection=majority|declared` on the summary
  means the scope is clean. `detection=none` is a different answer:
  no manager was resolved, so nothing was measured. Read
  `detection_reason` — `no-spec-fields-in-scope` (nothing in scope
  owns a spec field) or `no-majority-manager` (the leading owner,
  named in `candidate` with its `share`, holds 50% or less; the
  normal shape of a cluster with no GitOps controller). If the
  candidate *is* the GitOps controller, re-run with
  `--manager=<candidate>`.
- `share` rides every run. On a declared manager it is the sanity
  check: `detection=declared share=4%` means most of the findings are
  other legitimate owners, not drift.

## What changed: `triage changes`

For each drifted workload `stab drift` named:

```lookout
lookout triage changes Deployment/prod/api --since=6h
```

```lookout-golden
kind=change.rollout severity=info namespace=prod kind_of_object=ReplicaSet name=web-rs-2 reason=NewReplicaSet message="new template revision created inside the window" at=2026-07-25T10:20:00Z relation=upstream origin=api revision=2 image=img:v2
kind=change.scale severity=info namespace=prod kind_of_object=Deployment name=web reason=ScalingReplicaSet message="Scaled up replica set web-rs-2 to 3" at=2026-07-25T10:22:00Z relation=self origin=event
scanned=8 findings=3 elapsed=100ms source=live-approximation window=2026-07-25T10:00:00Z..2026-07-25T10:30:00Z
```

Chronological, provenance-tagged (`origin=log|event|api`), scoped to
the workload's graph neighborhood. `source=live-approximation` cannot
see un-timestamped updates (ConfigMap edits, label flips); with a
sentinel store, add `--store` for the full recorded delta log — and
`--at` to ask as of an earlier instant (post-mortems):

```lookout
lookout triage changes Deployment/prod/api --since=6h --store=/var/lib/lookout/lookout.db
lookout triage changes Deployment/prod/api --since=1h --at=2026-07-25T10:00:00Z --store=/var/lib/lookout/lookout.db
```

## The workflow

1. `lookout stab drift` (whole cluster or `--namespace`) — list the
   divergences. Nothing found → done; the cluster is Git's.
2. Per drifted workload: `lookout triage changes <Kind>/<ns>/<name>
   --since=<window covering the drift age>` — when did it move, what
   else moved with it (`age=` on the drift finding sizes the window).
3. Inspect the current value of a drifted field with
   `lookout triage spec Deployment/prod/api` (token-dense, secret-safe)
   before deciding whether Git or the cluster is right.
4. Remediation is a GitOps decision, not a lookout one: sync (revert
   the cluster) or commit (adopt the change). lookout only ever reads
   — it will not revert anything.
5. If the drift explains an active incident, record the conclusion
   for the next scan/agent:
   `lookout triage status --store=/var/lib/lookout/lookout.db --fingerprint=sha256:… --resource=Deployment/prod/api --status=triaged --root-cause="manual kubectl edit of spec.replicas, diverged from Git"`

Per-command references (all flags, output-field glossaries) are in
`references/`, generated from the same metadata as `--help`.
