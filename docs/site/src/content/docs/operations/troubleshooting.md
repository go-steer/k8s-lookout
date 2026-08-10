---
title: Troubleshooting
description: RBAC probe failures, source-by-source requirements, common startup errors verbatim with fixes, and what the unavailable markers mean.
sidebar:
  order: 5
---

The sentinel's failure philosophy is that nothing degrades silently:
misconfiguration refuses to start with a named cause, missing capability
announces itself, and the one failure mode this design exists to prevent
is the **silent empty watch** — an informer without list/watch permission
would log a warning once and then report nothing forever, which reads as
"cluster healthy".

## The `--sources=auto` startup summary

With the default `--sources=auto`, startup probes each portable
source's declared needs and prints a summary block — one line per
candidate, enabled lines included, so what auto decided is read from
the log, never inferred from silence. A worked example from a cluster
with the shipped RBAC but no metrics-server:

```
sources: auto — probing the portable set (RBAC per source; metrics.k8s.io for saturation); misses are skipped loudly — pin --sources explicitly to make a miss fatal (§11)
source k8s-events: enabled (always on — a sentinel that cannot watch events is misdeployed)
source object-state: enabled
source rollout: enabled
source saturation: disabled (metrics.k8s.io unavailable — install metrics-server)
source degradation: enabled
source expiry: enabled
source capacity: enabled
sources: auto resolved → k8s-events,object-state,rollout,degradation,expiry,capacity (quota and token-burn stay explicit-only: project tier and the core-agent cost stack)
storm: auto — on (pods/nodes/replicasets graph grants verified; independent of object-state — the graph feed runs its own informers, shared with the sources' when both are on)
```

Line anatomy: the header states the rules; each `source <name>:` line
is enabled or `disabled (missing <grant> — <how to fix it>, or name
it in --sources to make this fatal)`; the `resolved →` footer is the
effective source list the rest of startup uses; and the final
`storm: auto` line is `--storm=auto`'s resolution the same way. Two
things are fatal even under auto: `k8s-events` failing its probe (a
sentinel that cannot watch events is misdeployed — fix the
deployment), and a probe that cannot be *evaluated* at all (see
below).

## RBAC probe failures at startup

With an **explicit** `--sources` list (or `--storm=on`), every named
source's declared RBAC needs are verified against the sentinel's
actual credentials (via SelfSubjectAccessReview — the probe itself
needs no RBAC beyond authenticating) and a miss is fatal, naming
exactly what to fix — explicit lists never downgrade to a skip:

```
source "object-state" requires permission to "list nodes cluster-wide" (scope: Cluster)
and this ServiceAccount does not have it; grant it or disable the source —
refusing to run a silently empty watch
```

The requirement is rendered the way you would write it in a
(Cluster)Role rule: `<verb> <resource>[.<group>] [<name>] <scope>`. The
fix is one of exactly the two the message offers — apply the missing
grant (the shipped `deploy/12`–`15` manifests carry everything every
source needs), or drop the source from `--sources`.

A probe that cannot be *evaluated* is also fatal ("could not verify"
must not degrade into "assumed fine"):

```
source "…": capability probe for "…" failed: …
```

That means the API server rejected or could not answer the access
review — a cluster/credentials problem, not a Role problem.

## What each source needs

| Source / feature | Requires |
| --- | --- |
| `k8s-events` (default) | `events` get/list/watch; `pods` list/watch for the recovery clearance observer (without it the sentinel runs but recovery tracking is disabled — logged loudly) and `pods` get for inject enrichment. |
| `object-state` | `pods`, `nodes`, `deployments.apps`, `endpointslices.discovery.k8s.io`, `poddisruptionbudgets.policy` list/watch. Cluster scope — a namespaced Role cannot satisfy the nodes watch. |
| `rollout` | `statefulsets.apps` list/watch (its pods/deployments/replicasets informers ride the shared set). |
| `saturation` | `pods.metrics.k8s.io` get/list — a metrics API must exist (on kind, install metrics-server with `--kubelet-insecure-tls`; the probe fails loudly without it, by design). `nodes/proxy` get is OPTIONAL: denied (RBAC, or GKE Autopilot's platform policy) the source still runs with the PVC dimension disabled, reported at startup and again if the kubelet endpoint fails at runtime. |
| `degradation` | `endpointslices.discovery.k8s.io` list/watch. |
| `expiry` | `secrets`, `serviceaccounts` list (the secrets `list` is the sentinel's only read of secret values — scope it with `--expiry-namespaces`, which the probe then verifies exactly); `validatingwebhookconfigurations`/`mutatingwebhookconfigurations` list; cert-manager `certificates` list (discovery-gated — skipped with one loud log line when the CRD is absent). |
| `capacity` | Rides the events + pods grants, plus the `kube-system` Role (`deploy/14`/`15`): `get` on the `cluster-autoscaler-status` ConfigMap. The provider scale-decision sub-source additionally needs the `-gke` image with cloud credentials. |
| `quota` | The `-gke` image (or a `-tags gke/allproviders` build) plus read-only project credentials (`compute.regions.get`, `monitoring.timeSeries.list`, `logging.logEntries.list`, `cloudquotas.quotaInfos.list`). One instance per GCP project. |
| `token-burn` | No Kubernetes RBAC — it polls the core-agent cost stack at `--daemon-url` (or `--token-endpoint`). |
| `--storm` | `pods`, `nodes`, `replicasets.apps` list/watch for the topology-graph informers. `--storm=auto` (the default) resolves on/off against these grants with a loud line either way; `--storm=on` makes a miss fatal, like an explicitly named source. Independent of `object-state` — the graph feed runs its own informers. |
| Enrichment (`--enrich`) | `pods/log` get, `get` on the workload kinds, and `list` on the incident namespace's workload/service/configmap/ingress/RBAC kinds for the scoped-list fallback. All in the shipped ClusterRole; a gap here is not fatal — a denied (or `--enrich-lists`-deselected) list is dropped with a `skipped=` note on the bundle head, and any section that fails becomes an `enrichment_error` trailer with `enrichments_total{outcome="partial"}`. Withhold `secrets: list` to keep the watcher SA out of Secret values entirely; the bundle degrades to a documented partial. See [Narrowing the role](/getting-started/deploy/#narrowing-the-role--partial-bundles-not-errors). |

## Common startup errors, verbatim

| Error | Fix |
| --- | --- |
| `--daemon-url is required (unless --dry-run)` | Point it at the core-agent daemon (`http://…:7777`, no trailing slash — `--daemon-url must not end with '/'` is its own error). |
| `--token-env is required (unless --dry-run)` | Name the env var holding the bearer token, and make sure the Deployment sources it from the token Secret. |
| `--owner is required in per-incident mode (must match a proxy identity in the daemon's users.json)` | Set `--owner`; if session creates then fail with 4xx (`inject_errors_total`), the identity is missing on the daemon side. |
| `--target-session is required in shared mode` | `--mode=shared` posts everything to one existing session; name it. |
| `--sources: unknown source "…" (known: k8s-events, object-state, rollout, saturation, degradation, expiry, capacity, quota, token-burn; or auto)` | Typo in the source list. |
| `--storm must be auto, on, or off (got "…"; true/false are aliases for on/off, and bare --storm is no longer valid — write --storm=on)` | The bool-era `--storm` syntax; the flag is a three-mode string now. |
| `source "quota" requires a cloud provider with the quota capability …; build with -tags gke/allproviders and run with cloud credentials, or drop "quota" from --sources` | You enabled the quota source in the default (GCP-free) image. Pin the `-gke` flavor — the refusal is the conformance boundary working as designed. |
| `--saturation-window must be > --saturation-interval (the regression needs a window of samples)` | Flags of a disabled source are still validated — a nonsensical value is a config error in every mode. The same pattern covers every numeric flag (`--storm-min must be >= 2 (a storm of one is an incident)`, …). |

## Loud degradations that are *not* errors

These startup lines report reduced capability and keep running —
expected on the clusters they describe (all captured verbatim from
recorded drill runs):

```
capacity: provider scale-decision sub-source (§10.1 source 3) disabled:
  unavailable reason="no cloud provider configured" — Events +
  status-ConfigMap sub-sources still fire on scaleup failures, without
  the structured why
capacity: ConfigMap kube-system/cluster-autoscaler-status not found —
  no cluster autoscaler on this cluster? status sub-source idle until it appears
expiry: cert-manager CRD not found — Certificate renewal-state scanning
  disabled; TLS secrets and webhook CA bundles are still scanned
```

## The `unavailable` markers

Absent capability on the read-path is explicit output, never an error
and never silence. Three shapes to recognize:

- **Provider-gated commands** without a cloud provider emit one finding
  and exit 0, with the reason repeated on the summary line:

  ```console
  kind=cloud.unavailable severity=info reason=CapabilityUnavailable message="cloud quota needs the provider quota capability: no cloud provider configured" capability=quota provider=none
  scanned=0 findings=1 elapsed=0s unavailable="no cloud provider configured"
  ```

- **`health` categories** answer `status=unavailable` (e.g.
  `category=control-plane … message="requires cloud provider metrics;
  no cloud provider configured"`) rather than pretending healthy.
- **`perf probe` packs** whose backing metrics are not enabled on the
  cluster degrade to an explicit `pack_unavailable` finding.

Read them as "cannot answer", not "healthy" and not "broken": scripts
and agents should branch on the marker, and the exit code stays 0
because the tool did exactly what it could and said so.

## Assorted

- **`kubectl cp` fails against the sentinel pod** — the image is
  distroless (no tar). Copying the store off a pod is covered in
  [The occurrence store](/operations/store/).
- **Injects failing at runtime** — watch `inject_errors_total` by
  `http_code`: transport errors mean the `--daemon-url` is unreachable;
  401/403 mean the token or the `--owner` proxy identity.
- **A crashloop keeps "not re-paging"** after a triage-status downgrade —
  expected: kubelet's steady BackOff cadence keeps the incident inside
  the dedup window, so the override's visible effect is the per-signal
  `triage-status: downgraded …` log and the store severity until the
  loop actually pauses.
