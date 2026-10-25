---
name: k8s-audit
description: Audit Kubernetes/GKE cluster security, workload reliability, network segmentation, namespace governance, RBAC privilege, ComputeClass/AI workload posture, upgrade safety, policy profiles, and exemption hygiene with lookout audit (all, policy, cluster, workloads, hardening, netpol, namespaces, rbac, computeclass, ai, upgrades, exemptions) and track posture deltas over time with findings diff and findings ack.
---

# k8s-audit — security & reliability posture auditing with lookout

`lookout audit` inspects cluster configuration posture before an incident occurs: missing disruption budgets, unhardened container security contexts, unsegmented namespaces, missing ResourceQuotas/LimitRanges, wildcard RBAC bindings, GKE ComputeClass/Spot fallback gaps, exposed AI/inference workloads, control-plane upgrade blockers, and lapsed exemptions.

Unlike `lookout health` or `lookout scan` (which report active failures happening right now), `audit` answers **"where is the safety net missing?"** Every audit finding carries a deterministic `fingerprint=` so teams can baseline findings in SQLite (`lookout findings diff`), acknowledge accepted risk (`lookout findings ack`), or codify time-bounded YAML exemptions (`--exemptions`).

## Decision tree across audit surfaces

Start with `lookout audit all -A` to run every posture detector in a single pass over a shared cluster snapshot, or run the targeted subcommand matching the posture layer under review:

```lookout
lookout audit all -A
lookout audit policy
lookout audit cluster
lookout audit upgrades
lookout audit workloads -A
lookout audit hardening -A
lookout audit netpol -A
lookout audit namespaces -A
lookout audit rbac -A
lookout audit computeclass -A
lookout audit ai -A
lookout audit exemptions
```

| Posture layer | Command | What it verifies |
| --- | --- | --- |
| Full single-pass sweep | `lookout audit all -A` | Runs every posture detector in one pass over a shared cluster snapshot, stamping `check=<subcommand>` on each finding |
| Effective policy table | `lookout audit policy` | Dumps the resolved enablement, severity, provenance layer (`default\|profile\|policy\|cli`), and threshold parameters for every posture rule |
| Cluster baseline | `lookout audit cluster` | Control-plane public endpoint exposure, binary authorization, workload identity enablement, shielding, etcd encryption, Dataplane V2, observability, and Backup for GKE |
| Upgrade readiness | `lookout audit upgrades` | Node pool skew against control plane, unsupported minor versions, deprecated/removed APIs (`--target-version`), upgrade-path webhooks, zero-disruption PDBs, and release channel drift |
| Workload reliability | `lookout audit workloads -A` | Missing readiness/liveness/startup probes, missing CPU/memory requests & limits, single-replica deployments, missing or overlapping PDBs, preStop >= grace period, HPA min==max or min==1, CronJob overlap/failure, and rigid scheduling |
| Pod & host security | `lookout audit hardening -A` | Privileged containers, added Linux capabilities, root UID / privilege escalation / missing seccomp, writable rootfs, host namespaces, writable `hostPath` mounts, Pod Security Admission gaps, and bound/unbound ServiceAccount token automounting |
| Network segmentation | `lookout audit netpol -A` | Namespaces missing default ingress/egress NetworkPolicies, workloads missed by policy selectors, policies selecting nothing, and allow-all peer rules (`0.0.0.0/0`, empty `from`/`to`) |
| Namespace governance | `lookout audit namespaces -A` | Workload-bearing namespaces missing a `ResourceQuota` or `LimitRange`, quotas >= 90% of hard limits, and missing required onboarding labels (`--required-labels`) |
| Access & privilege | `lookout audit rbac -A` | Wildcard verbs/resources (`audit.wildcard_rbac`), direct or equivalent `cluster-admin` bindings (`audit.cluster_admin_binding`), `pods/exec` or `pods/attach` grants, and `impersonate`/`bind`/`escalate` grants |
| Hardware & ComputeClass | `lookout audit computeclass -A` | Workloads referencing missing ComputeClasses or unsatisfiable node/accelerator selectors, multi-replica Spot workloads without fallback, and single-zone node pools |
| AI / inference posture | `lookout audit ai -A` | Unisolated inference Services, `hostNetwork` on accelerator pods, `hostPath` model caches, default SA token automount on GPU/TPU pods, and floating model-loader images |
| Exemption governance | `lookout audit exemptions` | Expired (`audit.exemption_expired`) and soon-to-expire (`audit.exemption_expiring`) YAML exemptions so accepted risk never rots silently |

## Profiles, policy overlays, and holder attribution

All audit commands share common policy and scoping flags:

- `--profile=default|strict|onboarding|gke-autopilot`: select a built-in rule preset (`gke-autopilot` disables checks Autopilot enforces at admission; `strict` promotes `info` hardening/reliability findings to `warning`; `onboarding` restricts to high-signal blockers).
- `--policy=<file>`, `--only`, `--skip`, `--min-severity`, `--set=kind.key=val`: layer custom YAML policy rules or CLI overrides. Preview the merged rule table first with:
  ```lookout
  lookout audit policy --profile=gke-autopilot
  ```
- `--skip-system-namespaces`: skip `kube-*`, `gke-*`, `gmp-*`, and `istio-system` when running `-A`.
- `--per-workload`: expand holder findings (`audit rbac` and `audit hardening` ServiceAccount/Namespace checks, which stamp `workloads=<n>` and `workload_names=...`) into one finding record per affected workload.

## Reading audit findings

All audit sweeps obey the standard `lookout` output envelope (`scanned=<n> findings=<n> elapsed=<d>`).

### Workload reliability (`audit workloads`)

```lookout-golden
kind=audit.no_pdb severity=warning namespace=batch kind_of_object=Deployment name=worker reason=NoPodDisruptionBudget message="2 replicas and no PodDisruptionBudget selecting them: the eviction API will let a drain or upgrade take all 2 at once" fingerprint=sha256:7fe356f9a64f75392625206434494ed78e8fd020d5d35459c93fe96db64390ec replicas=2 namespace_pdbs=0
kind=audit.hpa_cannot_scale severity=warning namespace=prod kind_of_object=HorizontalPodAutoscaler name=checkout-hpa reason=HPAMinEqualsMax message="minReplicas and maxReplicas are both 3: the autoscaler is wired up but can never change the replica count, and because it reports ScalingLimited=False no sustained-state detector will ever flag it either" fingerprint=sha256:9dd401d265cc13cf9c72b34309d570908cabf1a7066a15958c46bf3d48f0076f min_replicas=3 max_replicas=3 scale_target=Deployment/checkout
…
scanned=7 findings=15 elapsed=100ms pdbs=2 hpas=2 nodes=4 workloads=5/1/1/0 jobs=0
```

### Container & namespace hardening (`audit hardening`)

```lookout-golden
kind=audit.privileged_container severity=warning namespace=platform kind_of_object=Deployment name=mesh reason=DangerousCapability message="capabilities.add SYS_ADMIN on 1 of 1 container: not privileged: true, and equivalent to it — a check reading only that flag reports this template as clean" fingerprint=sha256:d679b78ef1abcf8c7942a93445677f4c7426bed7fd1da5822dbc8bc139f6dc27 containers=1 container_names=mesh total_containers=1 capabilities=SYS_ADMIN
kind=audit.podsecurity_gaps severity=warning namespace=platform kind_of_object=Namespace name=platform reason=NoPodSecurityEnforce message="no pod-security.kubernetes.io/enforce label: nothing at admission stops a pod in this namespace asking for privileged, the host namespaces, or a hostPath — the workload claims above are the only thing that will notice" fingerprint=sha256:14ecc494ba0d2f7874d37b77f31a479b8618aebf937a0e573e211b569a7e7b01 pss_warn=baseline workloads=4
…
scanned=7 findings=12 elapsed=100ms namespaces=3
```

### RBAC privilege hygiene (`audit rbac`)

```lookout-golden
kind=audit.wildcard_rbac severity=warning kind_of_object=ClusterRole name=platform-superuser reason=FullWildcard message="ClusterRole has a rule granting every verb on every resource in every API group — the rule that defines cluster-admin; 1 binding grants it" fingerprint=sha256:ae3c59354c53e042f4d5b50550bcbaa4bbcc31969d43442b5fe0235d901a055e rules=1 wildcard_rules=1 verbs=* resources=* api_groups=* bindings=1
kind=audit.cluster_admin_binding severity=warning kind_of_object=ClusterRoleBinding name=oncall-admins reason=ClusterAdminClusterWide message="binds cluster-admin to 2 subjects cluster-wide: they can do anything to any object in the cluster, including rewrite RBAC to keep that power" fingerprint=sha256:4ff633f04796a2a97b941933cfcd092ff6756eb038c9215d14e7ebd2b3cdaa6b role=ClusterRole/cluster-admin subjects=2 subject_names=Group:oncall,User:alice@example.com
…
scanned=19 findings=7 elapsed=100ms platform_managed=5 aggregated_roles=1
```

## Baseline tracking with `findings diff`, `findings ack`, and `--exemptions`

Scheduled posture scans produce actionable drift alerts when paired with stateful diffing and explicit exemptions:

1. **Annotate accepted exceptions declaratively** by configuring an exemptions YAML file (`--exemptions=<file>`) and auditing expiry hygiene on schedule:
   ```lookout
   lookout audit workloads -A
   lookout audit exemptions
   ```
2. **Track regressions against the baseline store** using `findings diff` so CI/scheduled runs only page on **newly introduced** posture violations:
   ```lookout
   lookout audit workloads -A | lookout findings diff --store=/var/lib/lookout/lookout.db --report=- --cluster=prod-east
   ```
3. **Acknowledge triaged open findings** in the baseline store once reviewed:
   ```lookout
   lookout findings ack prod-east/batch/Deployment/worker/NoPodDisruptionBudget --store=/var/lib/lookout/lookout.db
   ```
