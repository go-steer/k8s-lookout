# failed-mount — a pod referencing a ConfigMap that doesn't exist

Deploys `mounter`, whose pod mounts ConfigMap `missing-config` —
which was never created. The pod parks in `ContainerCreating` with
`FailedMount` events.

```sh
examples/scenarios/failed-mount/inject
examples/scenarios/failed-mount/verify
examples/scenarios/failed-mount/revert
```

## What to expect

- **Sentinel (wire)** — a per-incident session with `kind=k8s-event`
  `reason=FailedMount` naming the missing ConfigMap.
- **Read-path** — `lookout state edges
  --workload=Deployment/lookout-demo/mounter` reports exactly the
  broken dependency edge (volume → ConfigMap `missing-config`), which
  events alone can't tell you structurally.

## Explore by hand

```sh
lookout state edges --workload=Deployment/lookout-demo/mounter
lookout triage events --workload=Deployment/lookout-demo/mounter
```

The `skills/playbooks/failedmount.md` playbook walks this exact
symptom — a good scenario for exercising skills in an agent harness:
> A pod in lookout-demo is stuck in ContainerCreating. Find the broken
> reference and tell me the one-line fix.
