# `deploy/components/sink-ax`: Agent Executor (`--sink=ax`) component

Kustomize `Component` switching `Deployment/lookout-watch` from `--daemon-url` to `--sink=ax` with Agent Substrate v0.6+ router mTLS ([`docs/ax-sink-design.md`](../../../docs/ax-sink-design.md)).

## What it changes

1. Rewrites `--daemon-url=...` on container `watcher` to `--sink=ax` via named `replacements` (independent of argument position) and appends `--ax-server=ax-server.ax-system.svc:8080` and `--ax-task-template=/etc/lookout/ax/task.yaml`.
2. Generates a content-hashed `ConfigMap` (`lookout-ax-task-*`) from [`task.yaml`](./task.yaml) and mounts it at `/etc/lookout/ax`.
3. Mounts Substrate's `podidentity` (`podCertificate`) and `servicedns-ca` (`clusterTrustBundle`) projected volumes at `/run/podidentity.podcert.ate.dev` and `/run/servicedns-ca`.

## Prerequisites

- Override `task.yaml` (`spec.image` is a placeholder) via a `patches:` entry on `ConfigMap/lookout-ax-task` in your outer `kustomization.yaml` (see below — Kustomize runs `patches:` after `components:`, and recomputes the `lookout-ax-task-<hash>` suffix automatically) or via Helm `sink.ax.taskTemplate`.
- Allowlist the sentinel pod's SPIFFE ID (`spiffe://cluster.local/ns/<namespace>/sa/lookout-watch`) in Agent Substrate's `ingressAllowedSPIFFEIDs`.
- Create the `lookout-watch-token` `Secret` (`WATCHER_TOKEN` authenticates to the agent session API inside each AX task).

## Usage

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
components:
  - ../../components/sink-ax
  # Composes cleanly with ../no-secrets, ../scope-namespaced, ../probe-from
patches:
  - target:
      kind: ConfigMap
      name: lookout-ax-task
    patch: |-
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: lookout-ax-task
      data:
        task.yaml: |
          apiVersion: ax.io/v1alpha1
          kind: Task
          metadata:
            atespace: default
          spec:
            image: ghcr.io/example/triage-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
            resources:
              limits:
                cpu: 500m
                memory: 1Gi
            http:
              port: 8484
            idle:
              suspendAfter: 10m
```

**Helm equivalent:** `helm install lookout-watch ./deploy/chart -n agent-triage --set sink.type=ax`
