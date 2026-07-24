# Master Technical Design Specification: `core-agent` Data-Plane Intelligence Layer

> **SUPERSEDED (2026-07-24)** by [`docs/DESIGN.md`](./DESIGN.md) (v3.x). v3
> consolidates the 25 binaries below into one multicall binary, respecs four
> tools that were designed against APIs that don't behave as assumed here
> (`exec-spy`, `top-analyzer`, `hpa-loop-catcher`, `field-sentinel`), cuts two,
> adds the watch-path signal engine (evolved `k8s-event-watcher`), and moves
> the whole suite out of `core-agent` into this repository. Retained as the
> historical failure-mode catalog; nothing here is normative.

**Document Version:** 2.0 (Merged Master Specification)

**Target Subsystem:** `core-agent` Native Binary Matrix (`/app/bin/{triage,state-machine,stability,performance,compliance}`)

**Objective:** Replace raw telemetry dumps with a matrix of compiled, deterministic Go binaries that execute local analysis on Kubernetes APIs and GCP SDKs. The suite converts raw cluster state into low-latency, token-dense payloads (`logfmt`/JSON) before hitting LLM context windows.

---

## 1. Architectural Principles & IPC Protocol

### 1.1 Guiding Principles

1. **Deterministic Read-Path, Managed Write-Path:** The agent uses binary tools to inspect and diagnose cluster state deterministically. All state modifications (the "Write Path") must be routed through a GitOps loop (e.g., generating a PR) rather than granting raw, unmonitored write authority to the LLM.
2. **Zero Nominal State in Context:** Healthy workloads (`Running`, `Ready=True`, `200 OK`) are completely omitted from diagnostic outputs unless explicitly queried.
3. **Pre-Computed Metrics & Graph Correlations:** The LLM must never perform manual arithmetic (e.g., capacity usage ratios, growth slope calculations) or cross-resource graph traversals in prompt context.
4. **Multi-Agent Intent Routing:** Telemetry outputs are formatted for fast, cost-effective intent routers (e.g., Gemini Flash or local Gemma variants), keeping cognitive model calls strictly reserved for deep reasoning.

### 1.2 Binary Inter-Process Communication (IPC) Protocol

Every binary in the toolset complies with a strict CLI interface:

* **Invocation Pattern:**
```bash
/app/bin/<category>/<binary-name> --namespace=<ns> --format=[logfmt|json] --timeout=10s

```


* **Exit 0 (Success):** Emits pure, token-dense data payloads directly to `stdout`. No progress bars, ASCII art, or decorative logs.
* **Exit 1+ (Failure):** Emits structured diagnostic logs to `stderr`. The agent engine captures `stderr` to log tool failures without corrupting the LLM's data stream.
* **Context Timeout:** Every binary wraps its operations in a strict Go `context.WithTimeout` (default: 10s) to prevent hanging execution calls from stalling the agent loop.

---

## 2. Directory Structure & Functional Matrix

```
/app/bin/
├── triage/          # Real-time incident forensics & log/event distillation
├── state-machine/   # K8s object controller, admission, & network handshake state
├── stability/       # Cluster drift, node pressures, & workload disruption checks
├── performance/     # Control-plane latency, APF queue, & etcd storage analytics
└── compliance/      # Cloud resource leaks, IP CIDR exhaustion, & capacity stockouts

```

---

## 3. Detailed Component Specifications

### Category 1: Workload Triage & Forensics (`/app/bin/triage/`)

#### 1.1 `workload-delta` (formerly `k8s-delta`)

* **Mission:** Filter out healthy workloads across the cluster, outputting **only** abnormal resources (`CrashLoopBackOff`, `OOMKilled`, `ImagePullBackOff`, `Pending`).
* **Technical Design:** Queries `v1.Pod`, `apps/v1.Deployment/StatefulSet/DaemonSet`, and `v1.Node`. Filters out resources where Phase == `Running` and `Ready == True`. Extracts container restart counts, exit codes, and failing condition messages.
* **Output Payload (`logfmt`):**
```text
ns=prod kind=Pod name=payment-api-89f status=CrashLoopBackOff container=api restarts=14 exit_code=137 msg="OOMKilled: Memory limit exceeded"

```


* **Token Reduction:** ~4,500 raw tokens $\rightarrow$ **~45 tokens (99% reduction)**.
* **Testing:** Seed `client-go/kubernetes/fake` with 50 healthy pods and 2 failing pods. Verify output contains strictly the 2 failing pods.

#### 1.2 `log-condenser` (formerly `k8s-log-distill`)

* **Mission:** Extract root-cause stack traces and deduplicate high-volume log streams into distinct error templates.
* **Technical Design:** Applies byte-scanning regex to strip HTTP liveness probes (`200 OK`) and dynamic values (`<TIME>`, `<UUID>`, `<IP>`). Computes a SHA256 slice (`fp=...`) to cluster identical log templates. Captures top 5 frames of multi-line stack traces.
* **Output Payload (`logfmt`):**
```text
count=142 fp=a7c81b sample="Connection refused to postgres:5432" trace="main.go:42 -> db.go:108"

```


* **Token Reduction:** ~150,000 raw tokens $\rightarrow$ **~350 tokens (99.7% reduction)**.
* **Testing:** Feed a 10,000-line log fixture containing panic traces and readiness probes. Assert line output count is $\le 5$.

#### 1.3 `ev-sifter` (formerly `k8s-event-timeline`)

* **Mission:** Correlate overlapping cluster `v1.Event` streams into a deduplicated, chronological cause-and-effect timeline.
* **Technical Design:** Ingests events via a `client-go` Informer, constructs the workload owner-reference tree (`Deployment` $\rightarrow$ `ReplicaSet` $\rightarrow$ `Pod`), and aggregates events occurring in 10-second sliding windows.
* **Output Payload (`logfmt`):**
```text
[12:00:01] obj=Node/node-1 reason=MemoryPressure msg="System memory low"
[12:00:05] obj=Pod/payment-api-89f reason=Evicted msg="Pod evicted due to node memory pressure"

```


* **Token Reduction:** ~8,000 raw tokens $\rightarrow$ **~120 tokens (98.5% reduction)**.
* **Testing:** Simulate out-of-order events using `fake.Clientset` and verify chronological sorting and count rollup.

#### 1.4 `top-analyzer` (formerly `k8s-saturation`)

* **Mission:** Calculate CPU/memory saturation percentages and memory growth slopes to identify quiet container throttling and OOM risks.
* **Technical Design:** Queries `metrics.k8s.io` alongside container spec limits. Applies linear regression over sequential metrics snapshots to calculate growth rate ($\Delta M / \Delta t$).
* **Output Payload (`logfmt`):**
```text
pod=checkout-api-6d9f container=app mem_used=492Mi mem_limit=512Mi mem_sat=96.1% slope=+12Mi/min risk=OOM_RISK

```


* **Token Reduction:** ~2,500 raw tokens $\rightarrow$ **~30 tokens (98.8% reduction)**.
* **Testing:** Feed fake metric time-series data demonstrating a linear memory leak; verify `OOM_RISK` flag generation.

#### 1.5 `edge-tracer` (formerly `k8s-dep-check`)

* **Mission:** Perform deterministic topology checks on workload dependencies (`ConfigMaps`, `Secrets`, `ServiceAccounts`, `Services`).
* **Technical Design:** Traverses the target workload's spec in-memory. Verifies that mounted secret keys exist, RBAC bindings are valid, and `Service` target selectors match active pod labels.
* **Output Payload (`logfmt`):**
```text
workload=Deployment/prod/payment-api err=MISSING_SECRET_KEY secret=db-credentials required_key=POSTGRES_PASSWORD

```


* **Token Reduction:** ~6,000 raw tokens $\rightarrow$ **~25 tokens (99.5% reduction)**.
* **Testing:** Pass a Pod manifest referencing a non-existent ConfigMap key to `fake.Clientset` and verify exact error output.

---

### Category 2: Distributed State Diagnostics (`/app/bin/state-machine/`)

#### 2.1 `webhook-inspector`

* **Mission:** Catch deployments or pod creations silently blocked by failing-closed Validating or Mutating Admission Webhooks.
* **Technical Design:** Queries `admissionregistration.k8s.io/v1` (`ValidatingWebhookConfiguration` and `MutatingWebhookConfiguration`). Checks backing service endpoints and evaluates `failurePolicy`. Emits webhooks configured as `Fail` whose backing endpoints are unreachable or unhealthy.
* **Output Payload (`logfmt`):**
```text
webhook=sidecar-injector.vault.io policy=Fail err=BACKING_ENDPOINT_DOWN svc=vault-injector-svc endpoints=0

```


* **Token Reduction:** ~3,000 raw tokens $\rightarrow$ **~30 tokens (99% reduction)**.
* **Testing:** Mock a ValidatingWebhook with `failurePolicy: Fail` targeting a service with 0 endpoints in `envtest`. Assert flag trigger.

#### 2.2 `endpoint-resolver`

* **Mission:** Detect network routing silent failures by identifying orphaned or misconfigured `EndpointSlices`.
* **Technical Design:** Queries `discovery.k8s.io/v1` `EndpointSlice` resources. Cross-references IP addresses against target Pod statuses to flag endpoints pointing to terminating, unready, or non-existent pods.
* **Output Payload (`logfmt`):**
```text
svc=payment-backend slice=payment-backend-x921 orphaned_ips=2 unready_endpoints=3 total=5

```


* **Token Reduction:** ~2,000 raw tokens $\rightarrow$ **~20 tokens (99% reduction)**.
* **Testing:** Seed an `EndpointSlice` referencing an IP of an evicted Pod; verify output flags `orphaned_ips`.

#### 2.3 `wi-scout`

* **Mission:** Validate GKE Workload Identity structural handshakes and Google Service Account (GSA) to Kubernetes Service Account (KSA) binding strings.
* **Technical Design:** Inspects `v1.ServiceAccount` annotations (`iam.gke.io/gcp-service-account`). Validates IAM policy bindings via GCP IAM API to ensure the KSA has permissions to impersonate the target GSA (`roles/iam.workloadIdentityUser`).
* **Output Payload (`logfmt`):**
```text
sa=prod/payment-sa gsa=payment-prod@proj.iam.gserviceaccount.com status=BINDING_MISSING err="KSA lacks workloadIdentityUser role on GSA"

```


* **Token Reduction:** ~4,000 raw tokens $\rightarrow$ **~35 tokens (99.1% reduction)**.
* **Testing:** Mock an IAM policy query missing the binding string; assert `BINDING_MISSING` status output.

#### 2.4 `volume-binder`

* **Mission:** Identify multi-attach PV/PVC volume locks stymieing pod scheduling or node migration across availability zones.
* **Technical Design:** Queries `storage.k8s.io/v1` `VolumeAttachment` resources. Detects `ReadWriteOnce` (RWO) volumes attached to nodes in Zone A while the requesting Pod is scheduled on a node in Zone B.
* **Output Payload (`logfmt`):**
```text
pvc=data-db-0 vol=pvc-89012 node_attached=gke-node-az1 node_requested=gke-node-az2 status=MULTI_ATTACH_LOCK

```


* **Token Reduction:** ~3,500 raw tokens $\rightarrow$ **~30 tokens (99.1% reduction)**.
* **Testing:** Seed a `VolumeAttachment` locked to Node A while Pod status reports `Pending` on Node B due to volume affinity.

---

### Category 3: Cluster Stability, Drift & Infra Anomalies (`/app/bin/stability/`)

#### 3.1 `field-sentinel`

* **Mission:** Extract out-of-band configuration drift by comparing `metadata.managedFields` managers against designated GitOps controllers (e.g., ArgoCD, Flux).
* **Technical Design:** Parses `metadata.managedFields` schema on K8s objects. Identifies fields modified by `kubectl-client-side-apply` or `kubectl-edit` that override the GitOps manager (`argocd-application-controller`).
* **Output Payload (`logfmt`):**
```text
obj=Deployment/prod/payment-api drifted_field=spec.template.spec.containers[0].image manager=kubectl-edit user=gari@dev.com

```


* **Token Reduction:** ~5,000 raw tokens $\rightarrow$ **~40 tokens (99.2% reduction)**.
* **Testing:** Create a manifest in `envtest` modified manually via `client-go`; verify `field-sentinel` pinpoints the user and drifted JSON path.

#### 3.2 `exec-spy`

* **Mission:** Detect active or historical interactive shell intrusions (`kubectl exec`) inside production containers.
* **Technical Design:** Audits `v1.Event` streams filtering for `InvolvedObject.Subresource == "exec"`.
* **Output Payload (`logfmt`):**
```text
pod=payment-api-89f container=api user=admin@company.com time=21:04:12 cmd="/bin/sh"

```


* **Token Reduction:** ~1,500 raw tokens $\rightarrow$ **~20 tokens (98.6% reduction)**.
* **Testing:** Emit a mock `exec` audit event; verify `exec-spy` parses the command and actor identity.

#### 3.3 `hpa-loop-catcher`

* **Mission:** Detect rapid, oscillating HPA scaling loop inversions (thrashing) over short time windows.
* **Technical Design:** Evaluates `autoscaling/v2` `HorizontalPodAutoscaler` metric history. Calculates scaling direction reversals within a 15-minute lookback.
* **Output Payload (`logfmt`):**
```text
hpa=checkout-hpa oscillations=8 range=2->20->2->20-replicas cause="CPU target threshold too narrow (50%)"

```


* **Token Reduction:** ~3,000 raw tokens $\rightarrow$ **~25 tokens (99.1% reduction)**.
* **Testing:** Feed HPA metric history showing 8 scaling inversions in 10 minutes; assert thrashing detection flag.

#### 3.4 `disruption-budget-analyzer`

* **Mission:** Pinpoint gridlocked `PodDisruptionBudgets` (PDBs) stalling cluster upgrades, node drains, or autoscaling.
* **Technical Design:** Scans `policy/v1` `PodDisruptionBudget` objects where `status.currentHealthy <= status.desiredHealthy` and `status.disruptionsAllowed == 0`.
* **Output Payload (`logfmt`):**
```text
pdb=redis-pdb ns=prod allowed_disruptions=0 current_healthy=2 min_available=2 status=GRIDLOCKED

```


* **Token Reduction:** ~1,500 raw tokens $\rightarrow$ **~20 tokens (98.6% reduction)**.
* **Testing:** Seed a PDB with `allowedDisruptions: 0`; verify `status=GRIDLOCKED` output.

#### 3.5 Additional Stability Utilities

* **`node-pressure-sifter`:** Evaluates node condition flags (`MemoryPressure`, `DiskPressure`, `PIDPressure`) and outputs impacted pod counts.
* **`drain-blocker`:** Identifies pods missing PDB allowances or utilizing local emptyDir storage without local storage eviction tolerations.
* **`kernel-sentry`:** Captures Node Problem Detector (NPD) events to isolate underlying kernel panics, bad memory pages, or filesystem read-only remounts.
* **`spot-countdown`:** Monitors node taints (`[cloud.google.com/gke-spot=true:NoSchedule](https://cloud.google.com/gke-spot=true:NoSchedule)`) and termination metadata endpoints to forecast Spot preemption events.

---

### Category 4: Performance & Control Plane Analytics (`/app/bin/performance/`)

#### 4.1 `api-latency-sifter`

* **Mission:** Dissect API server response times and P99 latency spikes across Kubernetes resource verbs (`LIST`, `POST`, `WATCH`).
* **Technical Design:** Queries Google Cloud Monitoring API metrics (`apiserver_request_duration_seconds_bucket`). Identifies specific API resource paths exceeding 1,000ms response times.
* **Output Payload (`logfmt`):**
```text
verb=LIST resource=secrets p99_latency=3450ms client=argocd status=SLOWNESS_WARNING

```


* **Token Reduction:** ~10,000 raw metric lines $\rightarrow$ **~25 tokens (99.7% reduction)**.
* **Testing:** Mock Cloud Monitoring time-series response with a 3.4s P99 LIST latency metric; verify detection.

#### 4.2 Additional Performance Utilities

* **`startup-profiler`:** Tracks P95 pod startup timelines (ImagePull $\rightarrow$ ContainerStart $\rightarrow$ Readyness) over 24 hours to spot performance decay.
* **`apf-inspector`:** Monitors API Priority & Fairness (APF) queue saturation and HTTP 429 rejection spikes.
* **`etcd-sentry`:** Measures etcd `fsync` disk write latencies and total database storage size.

---

### Category 5: Cloud Compliance & Resource Leaks (`/app/bin/compliance/`)

#### 5.1 `stockout-sentry`

* **Mission:** Detect GCE physical availability zone resource exhaustion events (`ZONE_RESOURCE_POOL_EXHAUSTED`).
* **Technical Design:** Queries Google Cloud Logging API (`resource.type="gce_instance"` AND `jsonPayload.event_subtype="compute.instances.insert"` AND `error.code="ZONE_RESOURCE_POOL_EXHAUSTED"`).
* **Output Payload (`logfmt`):**
```text
zone=us-east1-b machine_type=n2-standard-8 err=ZONE_RESOURCE_POOL_EXHAUSTED action="Reroute node pool to us-east1-c"

```


* **Token Reduction:** ~8,000 raw log tokens $\rightarrow$ **~30 tokens (99.6% reduction)**.
* **Testing:** Supply mock GCP audit log fixture containing stockout errors; assert correct extraction of machine type and zone.

#### 5.2 Additional Compliance Utilities

* **`ip-space-monitor`:** Evaluates VPC subnetwork Pod CIDR usage ratios, flagging clusters approaching IP exhaustion (>90%).
* **`disk-orphan-scout`:** Cross-references GCE Persistent Disks via GCP Compute API against active K8s `PersistentVolume` claims to locate unattached, billing-active disks.
* **`lb-ghost-buster`:** Identifies GCP Forwarding Rules / Load Balancers pointing to target pools with 0 active pods.
* **`stale-object-sweeper`:** Sweeps cluster namespaces for unreferenced `Secrets` and `ConfigMaps` over 30 days old.

---

## 4. Complete Master Manifest Matrix

| Category | Binary Name | Core API / SDK Dependency | High-Signal Core Focus |
| --- | --- | --- | --- |
| **Triage** | `workload-delta` | `client-go` (Pods/Apps) | Strips healthy workloads; emits broken pods/nodes. |
| **Triage** | `log-condenser` | `client-go` (Pod Logs API) | Normalizes logs, extracts stack traces, clusters patterns. |
| **Triage** | `ev-sifter` | `client-go` (CoreV1 Events) | Deduplicates and timeline-sorts workload event storms. |
| **Triage** | `top-analyzer` | `metrics.k8s.io` | Calculates CPU/memory saturation & OOM slope rates. |
| **Triage** | `edge-tracer` | `client-go` (Topology Graph) | Verifies spec dependency links (ConfigMaps, Secrets, SVCs). |
| **State Machine** | `webhook-inspector` | `admissionregistration.k8s.io` | Finds deployments blocked by failing-closed webhooks. |
| **State Machine** | `endpoint-resolver` | `discovery.k8s.io` | Pinpoints orphaned/misconfigured EndpointSlices. |
| **State Machine** | `wi-scout` | `client-go` + GCP IAM SDK | Validates GKE Workload Identity GSA/KSA handshakes. |
| **State Machine** | `volume-binder` | `storage.k8s.io` | Pinpoints cross-zone PV/PVC multi-attach locks. |
| **Stability** | `field-sentinel` | `client-go` (`managedFields`) | Pinpoints out-of-band GitOps configuration drift. |
| **Stability** | `exec-spy` | `client-go` (Subresource Events) | Detects interactive `kubectl exec` shell intrusions. |
| **Stability** | `hpa-loop-catcher` | `autoscaling/v2` | Detects high-frequency HPA scaling oscillations. |
| **Stability** | `disruption-budget-analyzer` | `policy/v1` (PDBs) | Pinpoints gridlocked PDBs blocking cluster operations. |
| **Stability** | `node-pressure-sifter` | `client-go` (CoreV1 Nodes) | Evaluates node pressure flags and impacted pod counts. |
| **Stability** | `drain-blocker` | `client-go` (Node Spec/Pods) | Pinpoints workloads resisting node drain eviction. |
| **Stability** | `kernel-sentry` | `client-go` (NPD Events) | Captures node kernel panics and hardware faults. |
| **Stability** | `spot-countdown` | `client-go` (Node Taints) | Forecasts imminent Spot node preemption reclaims. |
| **Performance** | `api-latency-sifter` | GCP Monitoring SDK | Dissects control-plane P99 request latencies by verb. |
| **Performance** | `startup-profiler` | GCP Monitoring SDK | Tracks P95 pod startup latency decay trends over 24h. |
| **Performance** | `apf-inspector` | GCP Monitoring SDK | Monitors API Priority & Fairness queue saturation/429s. |
| **Performance** | `etcd-sentry` | GCP Monitoring SDK | Pinpoints etcd storage `fsync` write delays. |
| **Compliance** | `stockout-sentry` | GCP Logging SDK | Catches GCE physical zone capacity exhaustion (`ZONE_RESOURCE_POOL_EXHAUSTED`). |
| **Compliance** | `ip-space-monitor` | GCP Compute SDK | Evaluates VPC subnet allocation ceilings for Pod CIDRs. |
| **Compliance** | `disk-orphan-scout` | GCP Compute SDK + `client-go` | Finds abandoned GCE Disks lingering post-PVC deletion. |
| **Compliance** | `lb-ghost-buster` | `client-go` + Discovery | Finds GCP Load Balancers pointing to 0 active pods. |
| **Compliance** | `stale-object-sweeper` | `client-go` (Spec Parsing) | Sweeps namespaces for orphaned Secrets & ConfigMaps. |

---