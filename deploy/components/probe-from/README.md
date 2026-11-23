# `deploy/components/probe-from`: guarded `net probe-from` grant

Privileged, opt-in Kustomize `Component` granting `patch` on `pods/ephemeralcontainers` paired with a Kubernetes 1.30+ `ValidatingAdmissionPolicy` for `lookout net probe-from` ([`docs/in-pod-probe-design.md`](../../../docs/in-pod-probe-design.md)).

> [!WARNING]
> **Kubernetes 1.30+ only.** Do not apply with `kubectl apply -k` on clusters older than 1.30 (where `ValidatingAdmissionPolicy` v1 is absent), or `kubectl` will create the `ClusterRoleBinding` and fail on the policy, leaving an unguarded grant (`net probe-from` refuses to run with `reason=PolicyMissing`, but remove the binding immediately if that happens).

## Usage

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
components:
  - ../../components/probe-from
```

**Helm equivalent:** `helm install lookout-watch ./deploy/chart -n agent-triage --set rbac.probeFrom=true`
