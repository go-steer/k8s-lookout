# lookout — Helm chart

Deploys the `lookout watch` sentinel: a single-replica, read-only
watcher that turns Kubernetes state changes into triage signals and
posts them to a core-agent daemon.

This chart is a parameterization of `deploy/*.yaml`, not a second
description of the deployment. `dev/tools/verify-helm-parity` renders
both and diffs them in CI:

```
helm template lookout-watch deploy/chart -n agent-triage
  ==  kustomize build deploy/
```

With default values the two are identical, resource for resource and
field for field, modulo the three provenance labels Helm stamps on
everything (`helm.sh/chart`, `app.kubernetes.io/managed-by`,
`app.kubernetes.io/version`). If you are reading a manifest to
understand what a flag or an RBAC rule is for, read the one under
`deploy/` — the rationale comments live there, and the templates carry
only a pointer back.

## Install

The chart needs two things it does not create.

**A namespace.** `helm install --create-namespace` will make it. The
kustomize bundle does not create one either — both assume
`agent-triage` already exists, on the theory that namespace ownership
belongs to whatever manages namespaces in your cluster.

**The daemon token Secret.** The sentinel authenticates to core-agent
with a bearer token read from the `WATCHER_TOKEN` env var. The chart
deliberately does not take the token as a value: Helm stores the
rendered release manifest in a Secret in the release namespace, so a
token passed as a value is readable by anyone who can read the release.
Create it out of band:

```sh
kubectl create secret generic lookout-watch-token \
  --namespace agent-triage \
  --from-literal=token="$(cat /path/to/token)"
```

Then, from the published OCI artifact:

```sh
helm install lookout-watch oci://ghcr.io/go-steer/charts/lookout \
  --version 0.23.0 \
  --namespace agent-triage --create-namespace \
  --set-string 'args[0]=--daemon-url=http://core-agent.agent-triage.svc.cluster.local:7777' \
  --set-string 'args[4]=--cluster-name=prod-us-east1'
```

or from a clone, swapping the OCI reference for `deploy/chart`. The
chart version is the release version without the `v`, and it is signed
with the same keyless identity as the images:

```sh
cosign verify ghcr.io/go-steer/charts/lookout:0.23.0 \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The release name matters for one boring reason: `lookout-watch` is what
makes the rendered object names match `deploy/*.yaml` exactly, which is
what makes the parity diff possible. Install under another name and
everything still works — you just get `myrelease-lookout` objects.

`args` is a flat list you replace wholesale rather than a map of
individual flags. That is deliberate. The flags interact (`--mode` with
`--dedup-window`, `--storm` with `--store`), and a chart that let you
override one in isolation would happily render a combination nobody has
run. For anything beyond a value or two, write a values file.

## What gets created

| Resource | Toggle | Notes |
|---|---|---|
| ServiceAccount | `serviceAccount.create` | Turn off to bring your own — a Workload-Identity-annotated one, say |
| ClusterRole + binding | `rbac.create` | Read-only, cluster-wide. Pinned to what the code actually reads by `pkg/checks/state/rbac_test.go` |
| Role + binding in `kube-system` | `rbac.capacity` | Reads the one `cluster-autoscaler-status` ConfigMap. Independent because it is the grant a cluster admin may reasonably refuse |
| Deployment | always | `replicas: 1`, `strategy: Recreate` |
| Service (`-metrics`) | `service.create` | ClusterIP on `:9090` |
| NetworkPolicy | `networkPolicy.create` | Ingress to the metrics port from the release namespace only |
| ServiceMonitor | `serviceMonitor.enabled` | Off by default: it is a CRD, and rendering it on a cluster without prometheus-operator fails the install |
| PersistentVolumeClaim | `persistence.enabled` | Only when `persistence.existingClaim` is empty |

## Values worth knowing about

**`image.flavor`.** The default image links zero GCP SDKs. If you enable
the quota source you need the `-gke` flavor: `--set image.flavor=gke`
appends the suffix to the chart's appVersion, or pin `image.tag`
outright.

**`persistence`.** Off by default, and the store is an `emptyDir`. The
occurrence store holds info-signal history and `--at` post-mortem
graphs — telemetry, never control flow — so losing it on a reschedule
costs history, not correctness. Turn persistence on if you want
post-mortems to survive a restart. The chart's PVC carries no
`helm.sh/resource-policy: keep`: `helm uninstall` deletes it, because a
volume that outlives the release it belongs to is a surprise, and a
`storageClass` with `Retain` is the right place to express "keep this".

**`networkPolicy.extraIngressFrom`.** The default rule admits scrapers
in the release namespace. Prometheus in `monitoring/`, or GMP in
`gmp-system/`, needs a peer added here or it will time out against a
policy nobody thought to look at.

**`replicaCount`.** One. Raising it does not give you high
availability — it gives you two sentinels watching the same cluster and
emitting every signal twice. That is also why the strategy is
`Recreate`: a rolling update of a single replica still overlaps.

## Upgrading

`helm upgrade` recreates the pod (the strategy is `Recreate`), so there
is a gap of a few seconds where nothing is watching. Changes that
happen in that gap are picked up as current state on the next sync, not
replayed as events.

Selector labels are `app.kubernetes.io/name` and
`app.kubernetes.io/component` only — deliberately narrow, because a
Deployment's selector is immutable and anything folded into it (the
chart version, the app version, your `commonLabels`) turns a routine
`helm upgrade` into a delete-and-reinstall.

## Uninstalling

```sh
helm uninstall lookout-watch --namespace agent-triage
```

The token Secret and the namespace survive, since the chart did not
create them. The PVC does not, if the chart created it.
