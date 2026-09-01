# CLI UAT — exercising every `lookout` command against real workloads

This is a **user-acceptance / end-to-end test plan for the read-path CLI
surface**: every `lookout` command and subcommand, the workloads each
one needs in order to produce meaningful output, the scripts that break
those workloads on purpose, and what to assert about each command's
result.

## Why this exists (the gap)

The repo already has a strong **signal-generation** e2e layer under
[`examples/`](../../examples/README.md): a kind recipe, the sentinel
wired to a capture stub, and ten inject/verify/revert scenarios. That
layer answers *"does the sentinel fire the right signal when a workload
breaks?"* — the **watch path**.

What it does **not** systematically cover is the other half of the
binary: the **read-path CLI**. The `examples/` scenario matrix touches a
handful of read commands as incidental proof (`health`, `triage events`,
`triage changes`, `state edges`, `stab drain`, `triage radius`), but the
following commands have **no dedicated UAT** today:

`bundle`, `triage delta`, `triage top`, `triage spec`, `triage status`,
`triage radius --at`, `triage changes --at`, `state webhooks`,
`state wi`, `state volumes`, `stab drift`, `perf probe`, all of
`cloud *`, `net probe`, `mcp`, and `version`.

This document closes that gap. It is organized so that a person (or an
agent) can walk the matrix top to bottom and confirm each command
returns correct, well-shaped, secret-safe output — with concrete
fixtures for the commands that need workloads the current demo app
doesn't provide.

## Where this sits in the test pyramid

```
unit / contract tests (CI, hermetic)      pkg/**/*_test.go, dev/tools/ci
        │
examples/e2e (kind, signal generation)    "does the RIGHT signal fire?"
        │
▶ CLI UAT (this doc)                       "does EVERY command return correct output?"
        │
dev/drills/ (real GKE, human judgment)    quota, --at post-mortems, corpus
```

It reuses the `examples/` cluster, demo app, and helper library
wherever it can, and only adds fixtures where a command needs input the
existing scenarios don't create.

## The command surface under test

Grounded in the actual multicall registry (`cmd/lookout/main.go`,
`cmd/lookout/checks.go`, `pkg/checks/*`). Not cobra — a hand-rolled
registry; `version` is inline, `watch`/`mcp` register directly, and
every read-path "check" mounts from `pkg/checks`.

| Kind | Commands |
| --- | --- |
| **Root / infra** | `version`, `watch` (live sentinel), `mcp` (server) |
| **Top-level checks** | `bundle`, `health` |
| **`triage`** (incident reads) | `delta`, `events`, `logs`, `top`, `spec`, `status` (writes), `radius` (graph), `changes` (graph) |
| **`state`** (dependency/config) | `edges`, `webhooks`, `wi`, `volumes` |
| **`stab`** (stability) | `drift`, `drain` |
| **`perf`** (control-plane perf) | `probe` |
| **`cloud`** (GCP-side) | `orphans`, `quota`, `ipspace`, `stockout` |
| **`net`** (active probes) | `probe` |

Contract every read command obeys (assert on all of them —
`pkg/emit`): **exit 0** = data, **1** = runtime error, **2** = usage
error; **stdout is pure payload**, diagnostics go to stderr; output ends
with a `scanned=N findings=N elapsed=D` summary line; `--format=json`
emits valid JSON, `logfmt` is the default; common flags `--namespace` /
`-A` / `--workload` / `--since` / `--timeout` are accepted everywhere,
and `--at` / `--store` **only** on the graph-backed commands
(`triage radius`, `triage changes`) — live-only commands must reject
`--at` as a usage error (exit 2).

## Environment tiers

Many commands are gated on cloud or cluster capabilities. Tag every UAT
case with the lowest tier that can run it, so a kind-only run knows what
it legitimately cannot cover (and asserts the *graceful-degradation*
message instead).

| Tier | Environment | Unlocks |
| --- | --- | --- |
| **T0** | kind, no cloud | `version`, `bundle`, `health`, `triage delta/events/logs/spec/status/radius/changes`, `state edges/webhooks/volumes*`, `stab drift/drain`, `net probe`, `mcp`; graceful-degradation of the cloud/GKE commands |
| **T1** | kind **+ metrics-server** (`examples/kind/up` installs it) | `triage top`, HPA-thrash in `triage events`, saturation ramps |
| **T2** | GKE staging cluster | `state wi`, `state volumes` (cross-node Multi-Attach), `perf probe` real packs, `triage top --history`, `ingress`/`gateway` signals, `stab drift --identity` |
| **T3** | GKE **+ cloud APIs + `-gke` image** | `cloud orphans/quota/ipspace/stockout`, quota/capacity-decision/notifications signals |
| **T4** | core-agent cost stack v2.7.0 | `bundle --incident` / `triage status` against a real daemon, tokenburn signals |

`*` `state volumes` runs at T0 for the "no conflicts / explicit
empty" path; provoking a real Multi-Attach needs a provisioner that
supports cross-node RWO races (T2).

---

# Part 1 — Per-command UAT matrix

For each command: **provoke** (what workload/edit produces meaningful
output), **assert** (what the output must contain and its shape), the
**tier**, and whether it **reuses** an existing `examples/` scenario or
needs a **new fixture** (Part 2).

## Root / infra

### `lookout version` — T0
- **Provoke:** none.
- **Assert:** prints a non-empty semver; exit 0; also works as
  `lookout --version` and `-version`. In a release build the string
  matches the `deploy/51` image pin.

### `lookout watch` — T0 (live)
Covered as a sentinel by `examples/sentinel`, but add these CLI-level
UAT cases that don't need the full stub-daemon loop:
- **`--dry-run`** — run the real watch pipeline but print inject
  payloads to stdout instead of calling a sink. Provoke with any
  existing scenario (e.g. `bad-rollout/inject`); **assert** a
  schema-valid payload appears on stdout, no HTTP call is made, and the
  payload's `kind` matches the scenario's expected wire kind.
- **`--sources=auto` startup probe** — **assert** each portable source
  logs an enabled/disabled line, and a denied grant produces a *loud
  skip*, not a crash (this is the Autopilot-safety contract). Force a
  miss by running against a cluster without metrics-server → saturation
  logs disabled, watch stays up.
- **`--sources=<explicit>` fail-fast** — naming a source whose grant is
  missing must be a **startup FAILURE** (exit non-zero), the strict-§11
  posture. Assert the process exits with the missing-grant named. The
  message half holds today; the *exit* does not — issue #364 wedges the
  process on the watchboard join, so `uat-cases/30-root.sh` reports that
  half as a skip naming the issue and will start asserting it the moment
  the process exits on its own.

> Note on exit codes: the read path follows §4.2 (2 for a usage error),
> but `watch` deliberately keeps the standalone sentinel's 0/1
> convention (`internal/watch/main.go`) — it is a daemon, and its
> supervisor only ever asks whether it came up. The UAT case asserts 1
> and spends its weight on the diagnosis instead.
- **`--metrics-addr=:9090`** — **assert** `/healthz` returns 200 and
  `/metrics` exposes Prometheus counters.
- **`/readyz` is not `/healthz`** — poll both from the first instant the
  port answers. `/healthz` must be 200 throughout; `/readyz` must return
  **503** with a body naming what it is waiting on — `informer caches
  syncing` / `cluster runner not started` for the unnamed single
  cluster, `… (syncing)` / `… (not started)` once `--cluster-name` or
  `--clusters` gives the clusters names — until the informers sync,
  then 200. A run where
  `/readyz` is 200 on the very first poll proves nothing — restart
  against a cluster with enough objects that the initial LIST is
  observable, or the case is not testing the gate.

### `lookout mcp` — T0 (server)
- **Provoke:** `lookout mcp --listen 127.0.0.1:8181` (a bare
  non-loopback bind must be refused — assert that too).
- **Assert:** every registered read-path check materializes as an MCP
  tool with a JSON schema for its flags; call one tool
  (e.g. `k8s_triage_delta`) over the wire against a broken cluster and
  confirm the result matches the equivalent CLI invocation. This is the
  cheapest way to assert the *whole* check surface is registered.
- **`--access-log=<path>`** — **assert** one line per call, including
  for a call with a bogus argument, and that neither the arguments nor
  the finding text appears in it.
- **Off-host bind** — `--listen=0.0.0.0:8181` must be refused unless
  `--allow-non-loopback`, `--auth-token-file`, and `--access-log` are
  *all* present; assert each of the three missing in turn produces its
  own refusal. With all three, **assert** `curl` without a token gets
  `401` and with the token gets a session.

## Top-level checks

### `lookout bundle` — T0
The first call of every incident: one correlated workload snapshot.
- **Provoke:** any broken workload with dependents — reuse
  `crashloop`, `image-pull`, or `bad-rollout` on `Deployment/lookout-demo/web`.
- **Assert:** output includes (a) a **sanitized, default-elided** spec
  with **no secret values**, (b) the abnormal child objects, (c) broken
  dependency edges, (d) blast radius to depth 2, (e) distilled log
  templates. Flags: `--depth`, `--max-templates`, `--cert-warn`.
- **`--incident <payload.json>`** (T4 or offline): feed a captured
  inject payload (grab one from the stub daemon's `INJECT` line) as the
  target selector; assert the bundle targets exactly that workload.
- **`--store`** (needs a sentinel store): assert open triage-status
  records for the workload are merged into the bundle.

### `lookout health` — T0
Cluster-wide "everything abnormal, categorized" — already used by
several scenarios; formalize the assertion.
- **Provoke:** run several scenarios at once (`crashloop` + `pending` +
  `cert-expiry` + `bad-rollout`).
- **Assert:** each failure appears under the right category (crashloops,
  pending, certs, rollouts, nodes); a **clean** cluster yields zero
  findings and a `findings=0` summary line (the healthy-path case is a
  real UAT — assert no false positives).

## `triage` group

### `lookout triage delta` — T0
Every abnormal object in one scan.
- **Provoke:** run multiple scenarios simultaneously so several
  categories are non-empty at once.
- **Assert:** pods (restarts ≥ `--restarts`), pending (age ≥
  `--pending-age`), pdb, nodes, quota categories populate correctly;
  `--only=pods` restricts to that category; `--quota-warn` gates the
  quota category. A clean cluster → `findings=0`.

### `lookout triage events` — T0/T1
Deduped event timeline + HPA thrash detection.
- **Provoke (T0):** `crashloop` or `image-pull` (rich event stream).
- **Provoke HPA thrash (T1):** **new fixture `hpa-thrash`** (Part 2) —
  an HPA driven to flip scale direction ≥ `--hpa-flips` within
  `--hpa-window`.
- **Assert:** events deduped and ordered chronologically over the owner
  tree; the HPA-thrash annotation appears only when flips exceed the
  threshold.

### `lookout triage logs` — T0
kubectl logs distilled via Drain clustering into templates + counts.
- **Provoke:** **new fixture `chatty-logs`** (Part 2) — a container that
  emits many lines matching a few templates (with variable fields:
  timestamps, IDs) plus a burst of a distinct error template. Also point
  it at a crashlooped container for `--previous`.
- **Assert:** the distinct templates are clustered with correct counts,
  variable fields are abstracted, `--previous` reads the prior
  container's logs, `--keep-probes` toggles probe-line suppression, and
  `--max-templates` caps output.

### `lookout triage top` — T1
Point-in-time CPU/memory vs limits, OOM-asymmetry judged.
- **Provoke:** **new fixture `cpu-pressure`** (Part 2) — a container
  with a CPU limit under sustained load; and reuse the `oom` leaker
  mid-ramp for the memory-vs-limit path.
- **Assert:** the hot container is flagged past `--top-warn`; memory
  pressure is judged more severely than CPU (OOM asymmetry);
  `--show-unlimited` surfaces limitless containers; `--all`/`--limit`
  paginate. **`--history` is T2** (needs the cloud provider window
  stats) — on kind assert it degrades to an explicit unavailable note.

### `lookout triage spec` — T0
Read one resource's spec, token-dense, secret-safe, default-elided.
- **Provoke:** target a workload that mounts a Secret and sets many
  fields at their defaults (e.g. `Deployment/lookout-demo/api`).
- **Assert:** **secret values are redacted/omitted** (critical
  security assertion), default-valued fields are elided, and `--diff`
  shows only the delta from defaults. Point it at a nonexistent resource
  → usage/runtime error with the right exit code.

### `lookout triage status` — T0 (needs a store)
The **only read-path command that writes** — the §9.4 triage-status
record.
- **Provoke:** a running sentinel with `--store=<path>`; take an open
  incident's fingerprint.
- **Assert (write→read round-trip):** writing with
  `--status/--root-cause/--action/--severity-override` then reading back
  (empty `--status`) returns the record; `bundle --store` and
  `health --store` then surface it. Recur a resolved incident and assert
  a `triage.regressed` record. Missing `--store` → usage error (exit 2).

### `lookout triage radius` — T0 live / T-store post-mortem
Blast radius (upstream / lateral / downstream); **graph-backed**.
- **Provoke live (T0):** `endpoints-empty` or `node-failure` — a
  workload with real dependents.
- **Provoke post-mortem:** run the sentinel with `--store` +
  `--graph-snapshot-interval`, inject `bad-rollout`, let it resolve,
  then `triage radius --at <onset> --store <path>` — **assert the
  radius reflects topology as of onset**, not now.
- **Assert:** upstream/lateral/downstream sets are correct; `--depth`
  bounds traversal; without `--store`, `--at` on a live cluster is
  best-effort with a clear caveat line.

### `lookout triage changes` — T0 live / T-store post-mortem
What changed in the window before onset; **graph-backed**.
- **Provoke:** `image-pull` or `bad-rollout` (a rollout *is* a change);
  edit a ConfigMap/Secret the workload consumes; rescale it.
- **Assert:** the rollout, config/secret update, and rescale are each
  reported with direction and time; `--at`/`--store` gives full fidelity
  from a sentinel store vs. best-effort live; `--since` bounds the
  window.

## `state` group

### `lookout state edges` — T0
Verify every dependency edge; report only broken ones.
- **Provoke:** reuse `failed-mount` (missing ConfigMap) and
  `endpoints-empty` (selector matches nothing); plus **new fixture
  `broken-edges`** (Part 2) bundling a missing-Secret-key ref, a
  dangling Service selector, and a TLS Secret expiring inside
  `--cert-warn`.
- **Assert:** each broken edge is named with the specific missing
  key/selector; **healthy edges are NOT listed** (report-only-broken);
  the expiring cert is flagged with time-to-expiry.

### `lookout state webhooks` — T0
Audit admission webhooks: dead backends × failurePolicy, blast radius,
timeout stall, CA expiry.
- **Provoke:** **new fixture `broken-webhook`** (Part 2) — a
  `ValidatingWebhookConfiguration` pointing at a **dead** Service, scoped
  **narrowly** to a throwaway label/namespace, with `failurePolicy=Fail`
  and a near-expiry CA bundle. ⚠️ Keep the `namespaceSelector`/`rules`
  tight so it can't wedge real admission on the test cluster.
- **Assert:** the dead-backend × `Fail` combination is flagged as
  high-risk with its blast radius; timeout stall and CA expiry are
  reported; a `failurePolicy=Ignore` variant is noted as lower risk.

### `lookout state wi` — T0 (unavailable path) / T2 (real chain)
GKE Workload Identity chain verification.
- **Provoke (T0):** run on kind/vanilla — **assert an explicit
  "unavailable / not a GKE WI cluster" result**, not an error.
- **Provoke (T2):** on GKE, a KSA annotated for WI with a **missing or
  wrong IAM binding**; assert the 403/metadata break is diagnosed at the
  KSA→IAM link.

### `lookout state volumes` — T0 (clean) / T2 (conflict)
Multi-Attach / FailedAttachVolume diagnosis.
- **Provoke (T0):** healthy PVCs → **assert clean / no conflicts**.
- **Provoke (T2):** **new fixture `multi-attach`** (Part 2) — an RWO PVC
  claimed by two pods pinned to different nodes so one hangs in
  `Multi-Attach`/`FailedAttachVolume`; assert the VolumeAttachment ↔
  PV/PVC ↔ pods join names the holder and the blocked pod.

## `stab` group

### `lookout stab drift` — T0 (drift) / T2 (`--identity`)
Spec fields owned by a manager other than the GitOps controller.
- **Provoke:** **new fixture `drift`** (Part 2) — `kubectl apply` a
  Deployment (fieldManager `kubectl-client-side-apply`), then mutate a
  field with a **different** fieldManager, e.g.
  `kubectl scale deploy/... --replicas=5`
  (manager `kubectl-scale`) or
  `kubectl patch ... --field-manager=rogue-operator`.
- **Assert:** the drifted field is reported with the owning manager;
  `--manager` empty auto-detects the GitOps controller; **`--identity`
  (T2)** resolves the writer to an audited principal via the cloud audit
  trail (on kind, assert it degrades to "identity unavailable").

### `lookout stab drain` — T0
Everything that will block or be destroyed by a node drain.
- **Provoke:** reuse `pdb-gridlock` (PDB at 0 disruptions); add a bare
  pod, an `emptyDir` pod, and a single-replica Deployment on the target
  node (fold into the `drain` fixture or the demo app).
- **Assert:** PDBs at 0, bare pods, emptyDir data-loss, and
  single-replica evictions are each listed for `--node <name>`; exactly
  one of `--node`/`-A` is required (both/neither → usage error).

## `perf` group

### `lookout perf probe` — T0 (unavailable) / T2 (real)
Control-plane & startup performance via Cloud Monitoring query packs.
- **Provoke (T0):** run on kind → **assert every `--pack` reports
  `pack_unavailable`** explicitly (graceful degradation, not error).
- **Provoke (T2):** on GKE with monitoring, run `--pack=apiserver`,
  `apf`, `etcd`, `startup`; assert each returns metric-backed findings;
  a missing pack name → usage error.

## `cloud` group — T3 (or T0 refusal path)

All four are GKE-only, gated by build tag. **On the GCP-free default
image they must refuse loudly** (assert that at T0); the real assertions
need the `-gke` image + credentials at T3.

### `lookout cloud orphans` — T3
- **Provoke:** create then delete a PVC/Deployment so an unattached GCE
  PD or a zero-endpoint forwarding rule lingers past `--min-age`.
- **Assert:** the billing-active leftover is listed; `--only=disks|lbs`
  filters; recently-created resources under `--min-age` are excluded.

### `lookout cloud quota` — T3
- **Provoke:** a project near a compute quota limit.
- **Assert:** usages ranked nearest-to-exhaustion; `--quota-warn` gates
  the warn set; `--all` shows everything.

### `lookout cloud ipspace` — T3
- **Provoke:** a cluster on a small pod/service CIDR.
- **Assert:** per-subnet utilization with warn 80% / crit 95% banding.

### `lookout cloud stockout` — T3
- **Provoke:** a zone/machine-type with recent stockout history over
  `--since`.
- **Assert:** stockouts per zone/machine-type with reroute candidates.

## `net` group

### `lookout net probe` — T0
Active DNS/TCP/HTTP from wherever lookout runs; zero cluster mutation.
- **Provoke:** **new fixture `net-targets`** (Part 2) — one reachable
  Service and one deliberately unreachable target (e.g. the
  `endpoints-empty` Service, a closed port, a bad DNS name).
- **Assert:** `--dns` resolves (or reports NXDOMAIN), `--tcp` connects
  (or times out per `--probe-timeout`), `--http` returns status/latency;
  **no pods are spawned and nothing in the cluster mutates** (diff the
  object inventory before/after).

---

# Part 2 — Workloads & fixtures we need

Reuse the existing `examples/` demo app (`lookout-demo` ns: `web`,
`api`+PDB, `worker`, `vantage`) and the ten scenarios. The commands
above need the following **new** fixtures. Model each as an
`examples/scenarios/<name>/` directory with `README.md` + executable
`inject` / `verify` / `revert`, matching the existing convention and the
`examples/lib.sh` context guard + await helpers.

> All snippets below are sketches to adapt into the scenario scripts —
> they assume the `lookout-examples` kind context and the `lookout-demo`
> namespace unless noted. **Fixtures** (immediately below) records what
> was actually built and supersedes the sketches for those five.

## Fixtures

A *fixture* is a scenario directory that exists to give a command
something to say. It is not a failure scenario: nothing is expected on
the wire, no sentinel needs to be running, and `verify` is a smoke
check rather than a contract — the contract lives in
`examples/uat-cases/20-fixtures.sh`.

The distinction matters because several commands have nothing to report
about a healthy cluster. `state webhooks`, `state edges`, `stab drift`,
`stab drain` and `triage logs` all return a perfectly well-formed
`findings=0` against stock kind, and a check that only ever sees
`findings=0` is not testing the check — it is testing that the binary
starts.

Two fixtures exist for the opposite reason. `health` and `triage delta`
have *too much* to say about a shared cluster: what is broken right now
is whatever the last scenario left behind, so an assertion either fails
on a clean cluster or passes for the wrong reason on a dirty one —
hence `broken-workloads`, a namespace whose contents are known exactly.
And no demo workload mounts a Secret, so the redaction contract had
nothing to redact until `secret-workload`.

**Every fixture owns its own `lookout-uat-*` namespace.** Nothing a
fixture creates may sit in `lookout-demo`, where the demo app and the
failure scenarios live. Three reasons, in increasing order of how much
they cost when ignored:

1. `revert` becomes one `kubectl delete namespace` and cannot miss an
   object.
2. Later UAT cases never see fixture leftovers, and a fixture never
   perturbs a scenario's `verify`.
3. A fixture is allowed to create something that never becomes
   healthy — `broken-edges` deliberately wedges a Deployment in
   `CreateContainerConfigError` forever. In `lookout-demo` that
   poisons anything waiting on the namespace. It did: the driver's
   preflight used to `kubectl wait --for=condition=Available deploy
   --all`, which blocks the whole timeout on the first Deployment that
   never arrives and then reports every Deployment it never reached as
   timed out. The preflight now waits only on the Deployments read back
   from `examples/workloads/`, and warns rather than aborting if they
   are degraded — a still-injected scenario is a legitimate state, and
   several commands have *more* to say about a broken cluster.

| Fixture | Namespace | Feeds | Negative control |
| --- | --- | --- | --- |
| [`chatty-logs`](../../examples/scenarios/chatty-logs/README.md) | `lookout-uat-logs` | `triage logs` | probe lines survive `--keep-probes` |
| [`broken-edges`](../../examples/scenarios/broken-edges/README.md) | `lookout-uat-edges` | `state edges` | `edgy-ok`/`edgy-web`, ready and in the Endpoints |
| [`broken-webhook`](../../examples/scenarios/broken-webhook/README.md) | `lookout-uat-webhook` | `state webhooks` | `lookout-uat-slow`, live and not reported dead |
| [`config-drift`](../../examples/scenarios/config-drift/README.md) | `lookout-uat-drift` | `stab drift` | `drift-clean`, never edited |
| [`drain-blockers`](../../examples/scenarios/drain-blockers/README.md) | `lookout-uat-drain` | `stab drain` | — (the cluster supplies plenty) |
| [`broken-workloads`](../../examples/scenarios/broken-workloads/README.md) | `lookout-uat-broken` | `health`, `triage delta`, `bundle`, `watch --dry-run`, `findings ack` | `steady`, healthy and named by none of them |
| [`secret-workload`](../../examples/scenarios/secret-workload/README.md) | `lookout-uat-secrets` | secret-safety across every command; the healthy path | the workload is the control — nothing about it is a finding |

Each one carries a negative control on purpose. A check that fires on
everything is indistinguishable from a check that fires correctly
unless something adjacent stays quiet, so every fixture holds at least
one object of the same shape that must *not* be reported.

The cases register a fixture with `uat_fixture <name>`, which injects
it and pushes it onto a stack; the driver reverts the stack in reverse
order from its `EXIT` trap, so a failure or a Ctrl-C leaves the cluster
as it found it. A fixture that fails to inject skips its section rather
than failing the run.

One case has no scenario directory: `net probe` is exercised against a
listener the case starts on `127.0.0.1`. A ClusterIP is not reachable
from where the CLI runs, so a cluster-side fixture would test nothing;
the point of `net probe` is precisely that it answers from the caller's
vantage point.

### `hpa-thrash` (T1) — for `triage events` HPA-thrash, `autoscaling.*`
An HPA whose target oscillates so scale direction flips repeatedly, plus
(held long enough) a pin at max and a metrics-dead variant.

```sh
# inject
kubectl -n lookout-demo autoscale deploy/web --min=1 --max=6 \
  --cpu-percent=50
# oscillate load: alternate a CPU burst and idle so utilization
# crosses the target back and forth faster than the stabilization window
kubectl -n lookout-demo run thrash --image=python:3.12-alpine --restart=Never -- \
  sh -c 'while true; do timeout 40 yes >/dev/null; sleep 40; done'
```
- Drives `triage events` HPA-thrash (flips ≥ `--hpa-flips` in
  `--hpa-window`).
- Hold at max for **10m+** → `autoscaling.hpa_pinned` (warn);
  break metrics-server and wait **15m** → `autoscaling.hpa_metrics_dead`.

### `chatty-logs` (T0) — for `triage logs`
A container emitting a few high-count templates with variable fields
plus a distinct error burst.

```sh
# inject: a pod that prints patterned lines Drain should cluster
kubectl -n lookout-demo run chatty --image=python:3.12-alpine --restart=Never -- \
  python -c '
import time,random,sys
for i in range(100000):
    print(f"req id={random.randint(1,9999)} path=/api/v1/x status=200 ms={random.randint(1,50)}")
    if i % 500 == 0:
        print("ERROR db connection refused host=10.0.0.5 port=5432", file=sys.stderr)
    time.sleep(0.01)'
```
- `lookout triage logs --pod chatty` → assert the `req id=… status=200`
  template and the distinct `ERROR db connection refused` template
  cluster with correct counts and abstracted variables.

### `cpu-pressure` (T1) — for `triage top`, `saturation.forecast` (cpu)
A container with a CPU limit held under sustained load.

```sh
# inject
kubectl -n lookout-demo run cpuhog --image=python:3.12-alpine --restart=Never \
  --limits=cpu=200m,memory=64Mi -- \
  sh -c 'yes >/dev/null'
```
- `lookout triage top -n lookout-demo` → assert `cpuhog` past
  `--top-warn`. Sustain ~45m with a ramp for `saturation.forecast`.

### `broken-edges` (T0) — for `state edges`
One workload with several broken dependency edges at once.

```sh
# inject: consume a missing Secret key + a Service whose selector is stale
kubectl -n lookout-demo create deploy edgy --image=python:3.11-alpine -- sleep infinity
kubectl -n lookout-demo set env deploy/edgy --from=secret/does-not-exist   # missing Secret
kubectl -n lookout-demo expose deploy/edgy --port=80 --name=edgy-svc
kubectl -n lookout-demo patch svc edgy-svc -p '{"spec":{"selector":{"app":"ghost"}}}'  # dangling selector
# expiring TLS (reuse the cert-expiry helper: a tls secret valid <14d)
openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/k -out /tmp/c \
  -days 10 -subj /CN=edgy >/dev/null 2>&1
kubectl -n lookout-demo create secret tls edgy-tls --cert=/tmp/c --key=/tmp/k
```
- `lookout state edges --workload Deployment/lookout-demo/edgy --cert-warn 336h`
  → assert the missing Secret key, the dangling selector, and the
  <14d TLS expiry are each named; healthy edges are absent.

### `broken-webhook` (T0) — for `state webhooks`
⚠️ Scope tightly so it cannot wedge real admission.

```yaml
# inject (mounter.yaml) — dead backend + Fail policy, scoped to one label
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: uat-dead-webhook
webhooks:
  - name: dead.uat.lookout
    admissionReviewVersions: ["v1"]
    sideEffects: None
    failurePolicy: Fail          # the risky combo we want flagged
    timeoutSeconds: 5
    namespaceSelector:
      matchLabels: { lookout-uat: "webhook" }   # only a throwaway ns
    rules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["configmaps"]
    clientConfig:
      service:                    # points at a Service with no endpoints
        namespace: lookout-demo
        name: no-such-backend
        path: /validate
      caBundle: <near-expiry-ca-b64>
```
- `lookout state webhooks -A` → assert `uat-dead-webhook` flagged
  high-risk (dead backend × `Fail`), blast radius = the scoped ns, CA
  expiry noted. **`revert` must delete the webhook config.**

### `multi-attach` (T2) — for `state volumes`
Cross-node RWO contention (needs a provisioner that races on
attach; GKE PD, not kind's local-path).

```sh
# inject: one RWO PVC, two pods pinned to different nodes
kubectl -n lookout-demo apply -f pvc-rwo.yaml
kubectl -n lookout-demo run holder --image=python:3.11-alpine \
  --overrides='{"spec":{"nodeName":"<node-a>"}}' -- sleep infinity   # mounts pvc
kubectl -n lookout-demo run blocked --image=python:3.11-alpine \
  --overrides='{"spec":{"nodeName":"<node-b>"}}' -- sleep infinity   # same pvc → hangs
```
- `lookout state volumes -A` → assert the `Multi-Attach` hang names the
  holder pod, the blocked pod, and the PV/PVC.

### `drift` (T0) — for `stab drift`
Create a co-manager on a GitOps-managed field.

```sh
# inject
kubectl -n lookout-demo apply -f examples/workloads/10-web.yaml   # manager: apply
kubectl -n lookout-demo scale deploy/web --replicas=5             # manager: kubectl-scale
kubectl -n lookout-demo patch deploy/web --field-manager=rogue-operator \
  --type=merge -p '{"spec":{"template":{"metadata":{"annotations":{"tuned":"by-rogue"}}}}}'
```
- `lookout stab drift --workload Deployment/lookout-demo/web` → assert
  `spec.replicas` (kubectl-scale) and the annotation (rogue-operator)
  are reported as drift from the apply manager.

### `net-targets` (T0) — for `net probe`
A reachable and an unreachable target.

```sh
# reachable: the demo web Service (has endpoints); unreachable: endpoints-empty svc + bad DNS
lookout net probe --dns web.lookout-demo.svc.cluster.local \
                  --tcp web.lookout-demo.svc.cluster.local:80 \
                  --http http://web.lookout-demo.svc.cluster.local/
lookout net probe --dns no-such-host.invalid \
                  --tcp 10.255.255.1:9 --probe-timeout 2s
```
- Assert the reachable target succeeds and the unreachable one reports
  NXDOMAIN / timeout — and that the cluster object inventory is
  unchanged (no mutation).

### `store-postmortem` (T-store) — for `triage radius --at`, `changes --at`, `status`
Not a break scenario but a **harness**: run the sentinel with a
persistent store and snapshots, inject an existing scenario, let it
resolve, then run the graph-backed queries against the store.

```sh
# in examples/sentinel/up, add to the watcher args:
#   --store=/var/lib/lookout/lookout.db
#   --graph-snapshot-interval=30s
# then:
examples/scenarios/bad-rollout/inject
onset=$(date -u +%FT%TZ)     # capture onset for --at
examples/scenarios/bad-rollout/revert
kubectl -n agent-triage cp <pod>:/var/lib/lookout/lookout.db /tmp/lookout.db  # or PVC
lookout triage radius  --workload Deployment/lookout-demo/web --at "$onset" --store /tmp/lookout.db
lookout triage changes --workload Deployment/lookout-demo/web --at "$onset" --store /tmp/lookout.db
```
- Note: the image is distroless (no tar) so `kubectl cp` off a live pod
  doesn't work — mount the store on a PVC (see `deploy/51` PVC
  alternative) and copy it from a debug pod, as the drills do.

---

# Part 3 — Cross-cutting assertions (run against every command)

Independent of any single command, wrap the whole matrix in these checks
(a table-driven helper in the UAT driver):

1. **Exit codes** — data → 0, forced runtime error (e.g. unreachable
   apiserver) → 1, bad flag/usage → 2.
2. **stdout purity** — with `2>/dev/null`, stdout is only payload +
   summary line; all diagnostics went to stderr.
3. **Summary line** — every read command ends with
   `scanned=N findings=N elapsed=D`.
4. **`--format=json`** — output parses as valid JSON for every command;
   `logfmt` is the default.
5. **Secret-safety** — grep the output of `bundle`, `triage spec`,
   `state edges` for any known secret value in the fixtures; **must not
   appear** (the redaction contract).
6. **Scope flags** — `-A` widens, `--namespace` narrows, `--workload`
   targets; a command given `--at` when it isn't graph-backed → exit 2.
7. **`--timeout`** — a tiny `--timeout` against a slow path returns a
   clean timeout error, not a panic.
8. **Healthy-path (no false positives)** — on a clean cluster, the
   detection commands return `findings=0`.

---

# Part 4 — MCP & agent-harness UAT

- **MCP tool parity:** for each check, assert the `lookout mcp` tool
  result equals the CLI result on the same broken cluster (Part 1,
  `mcp`). This is the single strongest guarantee that no command
  regresses its output contract.
- **Agent harness:** the fixtures double as agent test cases — inject
  one, hand the investigation to an MCP-speaking agent, and judge its
  findings against the scenario's `verify`. Extend the existing
  [`examples/agent-harness.md`](../../examples/agent-harness.md) with a
  per-command prompt for each new fixture.

---

# Part 5 — Automation & coverage checklist

`examples/uat` is the driver, alongside `examples/e2e` and sharing its
`lib.sh` — same context guard, same binary resolution, same PASS/FAIL
summary. `examples/uat-lib.sh` holds the assertions; each case is a
file in `examples/uat-cases/` defining `uat_case_<name>`, discovered by
filename, so adding coverage means adding a file. `UAT_TIER` (default
`T0`) gates the cases that need more than a bare kind cluster; a case
above the running tier is reported as skipped and does not fail the
run. The kind tier runs in `.github/workflows/e2e-kind.yml`; T2–T4 stay
as `dev/drills/` runbooks, because they need real GKE / cloud / the
cost stack.

The **cross-cutting** checks (Part 3) are enumerated from the command
registry rather than a list kept by hand: `uat-cases/00-contract.sh`
reads `lookout mcp --list-tools` and asserts the generic contract
against every command it finds. It carries a guard in both directions —
a newly registered command with no invocation in the table fails the
run, as does a table entry for a command that no longer exists. That is
what keeps this checklist from quietly going stale.

The **MCP surface** (Part 4) is checked by replaying those same
invocations — from the shared table in `examples/uat-invocations.sh` —
through a real server over HTTP and comparing the tool result against
the CLI's stdout byte for byte. Driving both sides from one description
of a valid call is what makes the comparison mean anything; two
hand-written lists would eventually agree about the wrong thing. Note
that the CLI-name-to-tool-name mapping is *read* from the registry and
never derived: 20 of the 34 differ (`bundle` is served as
`k8s_triage_workload`, `state webhooks` as `k8s_admission_webhooks`).

Parity is byte-for-byte after normalizing six fields, and the list is
kept short on purpose because each entry is something the check stops
covering. All six are observations of something that moves on its own
between two calls rather than values a command chooses: `elapsed=`,
the `first_seen=`/`last_seen=` ends of the sliding log window, the
`sample=` line drawn from it, the `window=` lookback `triage changes`
anchors to now, and `age=`, which is now minus a creation or
transition timestamp and renders to the second below 48h. Counters are
deliberately *not* normalized — `count=` and `scanned=` are stable
across calls, so a change in one is a real difference and should fail.

`age=` is the entry worth knowing about, because a workstation cluster
hides it completely: objects there are days old, so `age=12d` reads the
same twice, while on a six-minute-old CI cluster every inventory line
ticks between the CLI run and the tool call. Local green is not
evidence for this class of field; the post-merge `e2e-kind` run is.

**Command coverage checklist** (tick when a UAT case exists). The
cross-cutting contract already covers every command below for exit
code, summary line, stdout purity, JSON, scope flags and `--timeout`;
these ticks are for the command's *own* behaviour.

- [x] `version`
- [x] `watch --dry-run`, `--sources=auto` probe, `--sources` fail-fast,
      `/healthz`+`/metrics`, and `/readyz` red-then-green (the fail-fast
      *exit* is skipped, not asserted — issue #364)
- [x] `mcp --listen` tool listing + one tool call (+ non-loopback refusal,
      `--access-log`, and the three-flag off-host bind with a 401);
      also `--profile` / `--tools` surface selection
- [x] `bundle` (+ `--incident`, `--depth`, `--max-templates`; `--store` still open)
- [x] `health` (+ healthy-path)
- [x] `triage delta` (+ `--only`, thresholds, the shared §8 fingerprint)
- [ ] `triage events` (+ HPA thrash)
- [x] `triage logs` (+ `--keep-probes`, `--max-templates`, `--previous`)
- [ ] `triage top` (+ `--history` degradation)
- [x] `triage spec` (+ `--diff` as declared-unimplemented, secret-safety)
- [ ] `triage status` (write→read round-trip, `triage.regressed`)
- [ ] `triage radius` (live + `--at`)
- [ ] `triage changes` (live + `--at`)
- [x] `state edges` (+ `--cert-warn` both directions)
- [x] `state webhooks` (+ `--cert-warn`)
- [ ] `state wi` (unavailable + real)
- [ ] `state volumes` (clean + Multi-Attach)
- [x] `stab drift` (+ `--manager`, the no-GitOps path; `--identity` degradation still open)
- [x] `stab drain` (+ `--node`, `-A` roll-up)
- [ ] `perf probe` (unavailable + real packs)
- [ ] `cloud orphans` / `quota` / `ipspace` / `stockout` (+ GCP-free refusal)
- [x] `net probe` (reachable + unreachable, no mutation)
- [x] Cross-cutting: exit codes, stdout purity, summary line, JSON, scope flags, `--at`, `--timeout`
- [x] Cross-cutting: secret-safety and healthy-path (`secret-workload`
      supplies both: one canary string consumed four ways, swept across
      every command's output, in a namespace that is otherwise quiet)
- [x] MCP tool-vs-CLI parity for every check (`uat-cases/10-mcp.sh`,
      byte-for-byte after normalizing the six fields two invocations
      cannot agree on — see `uat_normalize_payload`)

---

## Related

- [`examples/README.md`](../../examples/README.md) — the signal-generation e2e layer this extends
- [`examples/agent-harness.md`](../../examples/agent-harness.md) — CLI-through-agent testing
- [`docs/signal-schema-v1.md`](../signal-schema-v1.md) — the 48 signal kinds the fixtures provoke
- [`docs/cli-stability-policy.md`](../cli-stability-policy.md) — the output/flag contract these UAT cases assert
- [`dev/drills/`](../../dev/drills/) — the human-run GKE runbooks for T2–T4
