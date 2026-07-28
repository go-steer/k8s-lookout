# image-pull — a rollout to an image that doesn't exist

`api` is rolled to a nonexistent GHCR tag. The new pod sits in
`ErrImagePull`/`ImagePullBackOff` while the old ReplicaSet keeps one
pod serving (default RollingUpdate surge).

```sh
examples/scenarios/image-pull/inject
examples/scenarios/image-pull/verify
examples/scenarios/image-pull/revert
```

## What to expect

- **Sentinel (wire)** — a per-incident session with `kind=k8s-event`
  and a pull-shaped reason. `ErrImagePull` and `ImagePullBackOff`
  hash to the SAME fingerprint (the canonical reason-class collapse,
  docs/signal-schema-v1.md) — you get one incident, not one per
  kubelet retry flavor.
- **Read-path** — `lookout triage events --namespace=lookout-demo`
  names the pull failure and the exact image string.

## Explore by hand

```sh
lookout triage events --workload=Deployment/lookout-demo/api
lookout triage changes --workload=Deployment/lookout-demo/api   # names the rollout
lookout bundle --workload=Deployment/lookout-demo/api
```

Agent-harness prompt to try:
> The api deployment in lookout-demo isn't finishing its rollout.
> What changed, and is it safe to roll back?
