---
title: Audit cluster posture & track drift
description: Run a single-pass posture sweep with lookout audit all, tune rules with profiles and policy overlays, codify expiring exemptions, and alert only on new regressions with findings diff.
sidebar:
  order: 7
---

**The problem:** incident scans (`lookout scan`, `lookout health`) tell you
what is broken right now. They stay silent on the workloads that are
serving traffic with a single replica, no `PodDisruptionBudget`, writable
root filesystems, wildcard RBAC bindings, or deprecated APIs that will
block the next control-plane upgrade. Those are **standing posture claims** —
they never self-clear, and mixing them into an incident feed drowns the
signal.

`lookout audit` is the standing-posture half of the read path: the
posture detectors (`workloads`, `hardening`, `rbac`, `netpol`, `namespaces`,
`computeclass`, `ai`, `cluster`, `upgrades`, `exemptions`), one shared
snapshot pass (`lookout audit all`), a four-layer policy engine, and
stateful diffing (`lookout findings diff`) so scheduled runs only page on
newly introduced violations.

## 1. Run the single-pass sweep (`lookout audit all`)

Point `lookout audit all -A` at your current kubeconfig (or pass
`--skip-system-namespaces` to omit `kube-*`, `gke-*`, and `gmp-*`
platform namespaces from the sweep):

```sh
lookout audit all -A --skip-system-namespaces
```

```txt
kind=audit.no_pdb severity=warning namespace=prod kind_of_object=Deployment name=checkout reason=NoPodDisruptionBudget message="2 replicas and no PodDisruptionBudget selecting them: the eviction API will let a drain or upgrade take all 2 at once" fingerprint=sha256:7fe356f9… check="audit workloads" replicas=2 namespace_pdbs=0
kind=audit.no_resource_limits severity=warning namespace=prod kind_of_object=Deployment name=checkout reason=NoMemoryLimit message="1 of 1 container has no resources.limits.memory (and no namespace LimitRange default): a memory leak grows until node OOM" fingerprint=sha256:4a91bc02… check="audit workloads" containers=1 container_names=checkout total_containers=1
kind=audit.privileged_container severity=warning namespace=prod kind_of_object=DaemonSet name=log-shipper reason=DangerousCapability message="capabilities.add SYS_ADMIN on 1 of 1 container: not privileged: true, and equivalent to it" fingerprint=sha256:d679b78e… check="audit hardening" containers=1 container_names=shipper total_containers=1 capabilities=SYS_ADMIN
kind=audit.cluster_admin_binding severity=warning kind_of_object=ClusterRoleBinding name=ci-deployer reason=ClusterAdminClusterWide message="binds cluster-admin to 1 subject cluster-wide: they can do anything to any object in the cluster; 2 running workloads use a bound ServiceAccount" fingerprint=sha256:4ff633f0… check="audit rbac" role=ClusterRole/cluster-admin subjects=1 subject_names=ServiceAccount:prod/ci-runner workloads=2 workload_names=Deployment/prod/api,Job/prod/migrate
kind=audit.no_resource_quota severity=info namespace=prod kind_of_object=Namespace name=prod reason=NoResourceQuota message="workload-bearing namespace (3 workloads) has no ResourceQuota: one runaway Deployment can consume unreserved cluster capacity" fingerprint=sha256:c1820fa4… check="audit namespaces" workloads=3
…
scanned=48 findings=14 elapsed=310ms checks=9 skipped_system_namespaces=4
```

Every Kubernetes resource list (`Pods`, `Deployments`, `Namespaces`,
`Roles`, `NetworkPolicies`, `Nodes`, …) is fetched **once** into a shared
snapshot and handed to every detector in memory. Each finding carries
`check=<subcommand>` naming the specific detector to run when you want to
drill in on one layer.

Notice `workloads=2 workload_names=Deployment/prod/api,Job/prod/migrate` on
the `ClusterRoleBinding` finding: holder findings (`audit rbac` and `audit
hardening` namespace/ServiceAccount checks) automatically attribute which
running workloads actually hold the grant. Pass `--per-workload` when
remediating by service owner to emit one record per affected workload.

## 2. Choose a profile or policy overlay

Different clusters have different baselines: a GKE Autopilot cluster
enforces Pod Security and node-pool settings at admission, whereas a
regulated production cluster may treat missing `readOnlyRootFilesystem` or
`livenessProbe` as a `warning`.

Four built-in profiles ship in the binary:

- `--profile=default` — standard severities across all detectors.
- `--profile=strict` — promotes `info`-level hardening, probe, spread, and
  namespace governance findings to `warning`.
- `--profile=onboarding` — focuses on high-signal outage and security
  blockers (`warning` and `critical`) for brownfield clusters.
- `--profile=gke-autopilot` — disables checks that GKE Autopilot manages
  or enforces automatically (node-pool sizing, host namespaces,PSA).

Layer a repo-committed `--policy=policy.yaml` on top of any profile to
customize severities, disable specific kinds, or set detector thresholds
(such as required namespace onboarding labels or HPA replica ceilings):

```yaml
# policy.yaml
profile: default
checks:
  audit.writable_rootfs:
    severity: warning
  audit.no_liveness_probe:
    enabled: false
  audit.namespace_onboarding_gaps:
    severity: warning
    params:
      required_labels: "owner,cost-center,environment"
```

Before running the sweep, inspect the resolved rule table and see which
layer (`default`, `profile`, `policy`, or `cli`) set each rule:

```sh
lookout audit policy --profile=gke-autopilot --policy=policy.yaml
```

```txt
kind=audit.policy_rule severity=info name=audit.cluster_admin_binding reason=RuleResolved message="enabled=true severity=warning source=default" enabled=true effective_severity=warning default_severity=warning source=default
kind=audit.policy_rule severity=info name=audit.namespace_onboarding_gaps reason=RuleResolved message="enabled=true severity=warning source=policy params=required_labels=owner,cost-center,environment" enabled=true effective_severity=warning default_severity=info source=policy params="required_labels=owner,cost-center,environment"
kind=audit.policy_rule severity=info name=audit.no_liveness_probe reason=RuleResolved message="enabled=false severity=info source=policy" enabled=false effective_severity=info default_severity=info source=policy
…
scanned=44 findings=44 elapsed=1ms profile=gke-autopilot
```

## 3. Codify accepted risk with `--exemptions`

Many posture findings have a legitimate answer of *"yes, on purpose"*: a
log-shipping DaemonSet genuinely needs `hostPath`, or a singleton migration
worker runs `replicas: 1`. Hiding those findings in a script means nobody
notices when the reason stops being true.

Instead, record reviewed opt-outs in an `--exemptions` YAML file with an
explicit `expires` date:

```yaml
# exemptions.yaml
exemptions:
  - kind: audit.privileged_container
    namespace: prod
    name: log-shipper
    reason: "DaemonSet reads node journald; tracked in SEC-412"
    expires: 2026-12-31
```

When you run `lookout audit all -A --exemptions=exemptions.yaml`:

- The finding is **still emitted** and counted in `findings=`, never
  silently dropped — it gains `exempt=true exempt_reason="DaemonSet reads
  node journald; tracked in SEC-412"`, and the summary line reports
  `exempt=1`.
- `lookout audit all` automatically runs [`audit exemptions`](/reference/audit-exemptions/)
  against the file itself, emitting `audit.exemption_expired` (`warning`)
  or `audit.exemption_expiring` (`info`) when an opt-out lapses.

## 4. Page only on regressions (`findings diff`)

To run `lookout audit all` on a schedule (or across a fleet with
`--contexts=prod-east,prod-west`) without re-paging on known posture debt,
pipe the sweep into [`lookout findings diff`](/reference/findings-diff/):

```sh
lookout audit all -A --policy=policy.yaml --exemptions=exemptions.yaml \
  | lookout findings diff --store=/var/lib/lookout/lookout.db --report=- --cluster=prod-east
```

`findings diff` compares each finding's deterministic posture `fingerprint`
against the SQLite baseline store and emits only **transitions** (`new`,
`escalated`, `resolved`), while [`lookout findings ack`](/reference/findings-ack/)
suppresses a finding for a bounded review window. To export the raw
finding stream as a JSON array for a fleet ledger or compliance pipeline,
pass `--format=findings`.

## As an agent skill

The posture decision tree — choosing between `audit all` and targeted
subcommands (`upgrades --target-version`, `ai`, `computeclass`,
`namespaces`), interpreting holder workload attribution, and baselining
with `findings diff` — is packaged for agents in
[`skills/k8s-audit`](https://github.com/go-steer/k8s-lookout/tree/main/skills/k8s-audit).
