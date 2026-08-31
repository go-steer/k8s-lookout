---
title: Deploy the sentinel
description: One kubectl apply -k, no clone — what each manifest is, the RBAC tiers, namespace-tier caveats, and a walkthrough of the flags that matter.
sidebar:
  order: 4
---

The sentinel — `lookout watch`, the resident per-cluster watcher —
deploys from the shipped manifests, no clone needed. Three commands,
assuming a core-agent daemon is answering at the in-cluster
`--daemon-url` (wiring the daemon is the
[next page](/getting-started/connect-core-agent/); `$WATCHER_TOKEN` is
its bearer token):

```sh
kubectl create namespace agent-triage
kubectl -n agent-triage create secret generic lookout-watch-token \
  --from-literal=token="$WATCHER_TOKEN"
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy?ref=v0.23.0"
```

Pin `?ref=` to the release you are deploying — each tag's manifest
pins its matching image. The first two commands exist because the
manifests reference the namespace and Secret but deliberately do not
create them. From a clone, `kubectl apply -k deploy/` applies the same
set (use `-k`, not `-f` — the directory carries the kustomization).

## What each manifest is

| Manifest | What it is |
| --- | --- |
| `11-serviceaccount-watcher.yaml` | The sentinel's ServiceAccount (`lookout-watch`). Bound to no GCP IAM role: the sentinel talks only to the local API server and the daemon. |
| `12-clusterrole-watcher.yaml` | The minimum-necessary, **read-only** ClusterRole. No patch/update/delete on anything — the sentinel observes; mutations happen through the core-agent daemon's own permission gate. Each rule is annotated with the source that needs it; rules for disabled sources are harmless. |
| `13-clusterrolebinding-watcher.yaml` | Binds the ServiceAccount to the ClusterRole. |
| `14-role-watcher-capacity.yaml` | A `kube-system`-namespaced Role for the capacity source's one extra read: `get` on the `cluster-autoscaler-status` ConfigMap, pinned by `resourceNames` rather than widening the ClusterRole. Only the capacity source needs it — under `--sources=auto` its absence skips the source loudly; with capacity named explicitly it is fatal. |
| `15-rolebinding-watcher-capacity.yaml` | Binds the ServiceAccount to the capacity Role. |
| `16-networkpolicy-watcher.yaml` | Default-deny ingress for the sentinel pod: only same-namespace scrapers reach `/metrics` + `/healthz` on `:9090`, since that surface is an incident-topology map a co-tenant should not scrape. Admit off-namespace monitoring (e.g. a `gmp-system` namespace) by uncommenting the `namespaceSelector` block. **Inert without an enforcing CNI** — if your CNI does not enforce NetworkPolicy the manifest is a no-op, and the Secret-read RBAC tradeoff in `deploy/12` still applies. Egress is left unrestricted (the API server, daemon, and cloud API addresses are environment-specific). |
| `17-service-watcher.yaml` | A ClusterIP Service, `lookout-watch-metrics`, publishing the metrics port. Until #288 `deploy/` shipped no Service at all and `16`'s NetworkPolicy assumed a scraper reaching the pod IP directly — which works for a hand-rolled scrape config and for nothing else. Prometheus-operator users additionally apply `deploy/prometheus-operator/`, which is a separate `-k` target because `ServiceMonitor` is a CRD the base bundle must not require. |
| `51-deployment-watcher.yaml` | The sentinel Deployment: one replica, distroless image, nonroot, a `/healthz` liveness probe and a `/readyz` readiness probe on the metrics port, and the shipped `args:`. `strategy: Recreate`, because a rolling update of a single-replica watcher briefly runs two sentinels double-emitting every signal — a few seconds of downtime is the better trade. A separate Deployment from the daemon (not a sidecar) so the two scale and restart independently; for another cluster, copy it and change `--cluster-name` + `--daemon-url`. |

One deliberate tradeoff to know about: the expiry source's `secrets`
rule is the sentinel's only read of Secret values (`tls.crt` to parse
`notAfter`; the token JWT for its `exp` claim), and it is `list` only —
no watch, no get, no informer cache of secret material. Scope it with
`--expiry-namespaces`, or remove the rule entirely if the expiry source
stays disabled.

### Narrowing the role — partial bundles, not errors

`list` on `secrets` returns the full value of every Secret at the API
level, so some operators would rather the sentinel's ServiceAccount
never hold that grant — even given the masking guarantees. As of #192
you can drop any resource from your copy of `deploy/12` and the bundle
and enrichment paths **degrade to a documented partial** instead of
failing: a per-resource `Forbidden` (or `NotFound`, e.g. a CRD that is
not installed) is caught, the resource is left out of the topology
pass, and the bundle's `bundle.target` head carries a `skipped=` note
naming exactly what was dropped (`skipped=secrets` when you withhold the
secrets grant). Nothing that reads a skipped resource errors; the rest
of the bundle is unaffected and, without the secrets grant, provably
secret-free at the source rather than only at the sanitizer. This is
opt-in tolerance: the strict RBAC probe (`--sources`, above) and
`state edges`/`triage` still fail loudly on a missing grant, so a
narrowed role is a deliberate choice a bundle documents, never a
silent hole in a signal source.

You do not have to edit the role to try this. Two flags select, per
run, which lists the pass reads — accepting `all` (default), a
comma-separated allowlist (`pods,deployments`), or subtractions
(`all,-secrets`):

- **`--enrich-lists`** (on `lookout watch`) narrows the scoped-list
  enrichment fallback; **`--enrich-lists-preflight`** SelfSubjectAccessReviews
  each selected resource first and drops the denied ones proactively
  (fewer 403s in the watcher log), falling back to the reactive
  `Forbidden`-skip when SSAR itself is not permitted.
- **`--lists`** / **`--lists-preflight`** are the same knobs on the
  one-shot `lookout bundle` command.

The [`lookout bundle` reference](/reference/bundle/) documents both, and
the `skipped=` field, in full.

## Or install the chart

The same deployment ships as a Helm chart, published to the same
registry as the images and signed with the same keyless identity:

```sh
helm install lookout-watch oci://ghcr.io/go-steer/charts/lookout \
  --version 0.23.0 \
  --namespace agent-triage --create-namespace \
  --set-string 'args[0]=--daemon-url=http://core-agent.agent-triage.svc.cluster.local:7777' \
  --set-string 'args[4]=--cluster-name=prod-us-east1'
```

The chart version tracks the release, minus the `v` — chart `0.23.0`
deploys `v0.23.0`. There is no compatibility matrix to consult because
there is only one version line. (v0.22.0 is the first release that
publishes a chart; for anything earlier, `helm install lookout-watch
deploy/chart` from a clone is the only route.) Verify it the same way
you verify an image:

```sh
cosign verify ghcr.io/go-steer/charts/lookout:0.23.0 \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The namespace and the token Secret are the same two prerequisites as
above — the chart does not create either. The Secret in particular is
deliberate: Helm stores the rendered release manifest in a Secret of
its own, so a token passed as a chart value ends up readable by anyone
who can read the release.

The chart is **not** a second description of the deployment. Its
defaults are the values in `deploy/*.yaml`, and a CI job renders both
and diffs them:

```
helm template lookout-watch deploy/chart -n agent-triage
  ==  kustomize build deploy/
```

They must match resource for resource and field for field, modulo the
three provenance labels Helm stamps on everything it renders. So the
manifest table above remains the one place to read about what a rule
or a flag is for, and a chart that quietly keeps deploying last
quarter's RBAC is a build failure rather than a discovery you make
during an incident. Run it yourself with
`dev/tools/verify-helm-parity`.

What the chart adds over `kubectl apply -k` is the toggles: RBAC tiers
(`rbac.create`, `rbac.capacity`), a PVC for the occurrence store
(`persistence.enabled`), the prometheus-operator ServiceMonitor
(`serviceMonitor.enabled`, which is the `deploy/prometheus-operator/`
add-on as a flag), the `-gke` image flavor (`image.flavor`), and extra
NetworkPolicy ingress peers for an off-namespace scraper. Everything is
documented in [`deploy/chart/README.md`](https://github.com/go-steer/k8s-lookout/blob/main/deploy/chart/README.md)
and in the comments in `values.yaml`.

One value behaves unlike the rest: `args` is a flat list you replace
wholesale, not a map of individual flags. The flags interact — `--mode`
with `--dedup-window`, `--storm` with `--store` — and a chart that let
you override one in isolation would happily render a combination nobody
has ever run.

## Deployment tiers

| Tier | Unit | Mechanism |
| --- | --- | --- |
| Namespace | `lookout watch` under a `Role` | `--namespace`/`--exclude-namespace`. Cluster-scoped sources cannot run: `--sources=auto` (the default) skips each with a loud line naming the missing grant, while an explicit list fails loudly at startup — never a silently empty watch either way. The topology graph builds a namespace-local subgraph. |
| Cluster | one sentinel per cluster (canonical) | One informer cache, one topology index, one credential boundary, one failure domain. One daemon may serve many sentinels. |
| Project | quota source only | One instance per GCP project, regardless of cluster count. Needs the `-gke` image. |
| Fleet | the fleet layer, not `lookout` | Sentinel-per-cluster fan-in; a fleet-level consumer joins signals on `fingerprint` + `cluster`/`zone`/`project`. |

### Namespace-tier caveats — failures are loud

A namespace-scoped (Role-only) deployment cannot satisfy sources that
watch cluster-scoped objects (`object-state`'s nodes, `capacity`, PDB
checks, the `--storm` graph informers). Under the default
`--sources=auto`/`--storm=auto`, those resolve OFF — each with one
startup line naming the missing grant — and the sentinel runs with
what the Role supports (`k8s-events` at minimum; events access itself
is non-negotiable). Name a source explicitly and the same probe
refuses to start instead, naming exactly what is missing:

```
source "object-state" requires permission to "list nodes cluster-wide" (scope: Cluster)
and this ServiceAccount does not have it; grant it or disable the source —
refusing to run a silently empty watch
```

The same discipline covers scoped grants: with `--expiry-namespaces`
set, the probe verifies exactly the scoped namespaces. See
[Troubleshooting](/operations/troubleshooting/) for the source-by-source
requirements table.

## The flags that matter

The shipped `args:` in `51-deployment-watcher.yaml` carry the wiring
(`--daemon-url`, `--token-env`, `--mode=per-incident`, `--owner`,
`--cluster-name`, `--dedup-window=5m`, `--in-cluster`,
`--metrics-addr=:9090`, `--log-level=info`) plus `--storm=on` and a
`--store` on an emptyDir volume; sources ride the binary's
`--sources=auto` default, so the manifest enables everything its RBAC
supports and runs unchanged on platforms that deny a grant to every
principal. The capability flags:

- **`--sources`** — which signal sources run. The default is `auto`:
  probe every portable source's needs at startup — RBAC per source,
  metrics.k8s.io presence for `saturation` — and enable what the
  deployment supports, skipping misses with one loud line each
  (`k8s-events` must pass; a sentinel that cannot watch events fails
  to start). An explicit list is the strict mode: a named
  source's missing REQUIRED grant is fatal (optional dimensions, like
  `saturation`'s `nodes/proxy` PVC read, degrade loudly instead —
  platform policies such as GKE Autopilot's Warden deny that one to
  every principal). The shipped `args:` ride the auto default so the
  manifest runs unchanged on such platforms; pin the list explicitly
  for strict fail-fast semantics. `quota`, `notifications` and
  `token-burn` are never auto-enabled. What each source watches for,
  with example triggers and extra needs, is [What the sentinel
  watches](/detect/sentinel/).
- **`--storm`** — storm correlation: incidents sharing a blast-radius
  key (nearest common topology ancestor) within `--storm-window` form
  one `kind=storm` session instead of dozens of per-pod pages. Takes
  `auto` (the default: the graph informers' pods/nodes/replicasets
  list+watch grants present resolve on; a miss resolves off with a
  loud line), `on` (a missing grant is a fatal startup error), or
  `off` — `true`/`false` are accepted as aliases, but the old bare
  `--storm` bool syntax now errors; write `--storm=on`. Independent
  of `object-state`: the graph feed runs its own informers.
- **`--store`** — the sentinel-local SQLite occurrence store: every
  emitted signal with its routing outcome, graph snapshots + change log
  (which unlock `--at` post-mortem queries), and triage-status records.
  Put it on the same volume as `--dedup-persist`. Bounded by
  `--store-ttl` (default 30 days) and `--store-max-mb` (default 512).
  See [The occurrence store](/operations/store/).
- **`--enrich`** — which severities get a pre-warmed bundle attached to
  their session's initial inject (`critical` by default; `warning`
  extends it; `off` disables). `--enrich-cap`, `--enrich-log-lines`,
  `--enrich-timeout` bound the work; failures become `enrichment_error`
  trailers, never crashes. `--enrich-lists` (and `--enrich-lists-preflight`)
  narrow which cluster resources the scoped-list fallback reads — see
  [Narrowing the role](#narrowing-the-role--partial-bundles-not-errors).
- **Severity and watchboard knobs** — `--severity kind=level`
  (repeatable) overrides the per-kind default routing;
  warning-class signals batch into the shared watchboard session
  (`--watchboard-batch`, `--watchboard-flush`, `--watchboard-rotate`).
  See [The watchboard](/operations/watchboard/).
- **`--dedup-persist`** and **`--recovery-stable-for`** — persist dedup
  bindings across restarts (recovery tracking resumes instead of
  re-firing), and how long a cleared symptom must stay clear before
  `kind=resolved` is injected (default 5m).

The generated [`lookout watch` reference](/reference/watch/) is the full
table of all 57 flags, derived from the live flag surface.

A battle-tested full-capability flag set — the exact args a live
validation drill appended to the shipped set (drill-tuned values
marked):

```
--sources=k8s-events,object-state,rollout,saturation,degradation,expiry
--storm=on --enrich=critical
--store=/data/lookout.db
--graph-snapshot-interval=1m      # drill value; default 5m
--recovery-stable-for=60s         # drill value; default 5m
```

After `kubectl apply`, read the startup log before walking away: every
armed stage announces itself (`store: enabled …`, `graph history:
enabled …`, `storm: topology graph ready …`), and every problem is a
named, loud line — see
[Observing `lookout`](/operations/observability/).
