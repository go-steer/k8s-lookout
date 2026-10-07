# placement-drift — replicas stuck in one zone after the other comes back

**kind only.** It labels the workers into two placement domains on
`lookout-examples/zone` (zone-a, zone-b) and briefly taints the second
one. This is leeway, the `topology-drift` source, catching a real
drift end to end: real scheduler, real informers, real wire.

Not part of the default `examples/e2e` set; run it explicitly (the
weekly CI run does):

```sh
examples/e2e placement-drift
```

## Timeline

1. zone-b gets a `lookout-examples/leeway-hold:NoSchedule` taint that
   nothing tolerates.
2. Two four-replica Deployments spread on the zone axis, both with
   `nodeTaintsPolicy: Honor`, so the taint takes zone-b out of their
   domain set and all eight replicas correctly land in zone-a:
   - `spread-hard` uses `DoNotSchedule`;
   - `spread-soft` uses `ScheduleAnyway`.
3. The taint lifts. zone-b is eligible again and nothing moves, because
   the scheduler never revisits a placed pod. That is drift: the
   cluster changed under the workload.

Why a taint and not a cordon: a cordon leaves zone-b with no usable
node. That is `leeway.domain_unavailable`, and it also relaxes every
workload's tolerance for 10 minutes (§7.6 drain). A taint leaves the
node Ready and schedulable, so the domain census never sees a loss;
only the subjects that honour the taint lose the domain.

## What to expect

- **Sentinel (wire):** both findings come after the 60s
  `--topology-dwell` that `examples/sentinel/up` sets (default 10m).
  - `leeway.contract_violated` for `spread-hard`: Tier A, critical, so
    a session. The observed skew is 4 against a declared maxSkew of 1.
  - `leeway.placement_drift` for `spread-soft`: Tier B, warning, so it
    goes to the watchboard digest. ρ = 0.5 against an even split.
- **Never:** `leeway.domain_unavailable`. `verify` fails if the taint
  is reported as a lost zone.

The findings resolve only after leeway's 30m resolve dwell, which is
not tunable, so nothing here waits for that. On a long-lived local
sentinel, a re-run inside the 5m `--dedup-window` dedups into the
first run's sessions.

## Explore by hand

```sh
kubectl -n agent-triage port-forward deploy/lookout-watch 9090 &
curl -s localhost:9090/metrics | grep -E 'lookout_leeway_(drift|alert_state)'
kubectl -n leeway-drift get pods -o wide
```

To give `spread-soft` a session without promoting every
`placement_drift` in the cluster, install the optional policy CRD,
restart the watcher so it discovers the CRD, and promote just that
workload before you run `inject`:

```sh
kubectl apply -f deploy/crds/leewaypolicies.yaml
kubectl -n agent-triage rollout restart deploy/lookout-watch
kubectl apply -f - <<'EOF'
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata: { name: spread-soft-pages, namespace: leeway-drift }
spec:
  selector: { matchLabels: { app.kubernetes.io/name: spread-soft } }
  topologyKeys:
    - key: lookout-examples/zone
      mode: Spread
      thresholds: { severity: critical }
EOF
```

The finding is still Tier B, now at `critical`, and its message ends
`severity critical set by policy`. `verify` does not expect this, so
delete the policy before you run the scenario unattended.

Agent-harness prompt to try:
> leeway says spread-hard broke its spread contract. Show me where its
> replicas are, why the scheduler put them there, and what it would
> take to rebalance.
