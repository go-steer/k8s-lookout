# broken-workloads — a namespace whose contents are known exactly

A **UAT fixture**, not a failure scenario: nothing is expected on the
wire and no sentinel needs to be running.

`lookout health` and `lookout triage delta` are the two commands whose
whole job is to answer *"what is wrong here"*, and they are the two
hardest to assert on a shared cluster. Whatever is broken at the moment
is whatever the last scenario left behind, so an assertion either fails
on a clean cluster or passes for the wrong reason on a dirty one. This
fixture makes one namespace whose contents are known exactly, and the
cases scope both commands to it with `--namespace`.

```sh
examples/scenarios/broken-workloads/inject
examples/scenarios/broken-workloads/verify
examples/scenarios/broken-workloads/revert
```

It lands in **its own namespace**, `lookout-uat-broken`, like every UAT
fixture: nothing a fixture creates may sit in `lookout-demo`, where the
demo app and the failure scenarios live, so revert is one
`kubectl delete namespace`. It matters more here than usual — `faulty`
and `stuck` never become healthy by design.

## What it stages

| Workload | State | Lands in |
| --- | --- | --- |
| `faulty` (+ `Service/faulty`) | prints 40 `boot step=<*> phase=init` lines, then a FATAL to stderr, then exits 1 — forever | `health` crashloops, `triage delta` `pod.crashloop`, `bundle` logs + a 0-endpoint edge |
| `stuck` | `requests: cpu: "200"` — unschedulable by arithmetic on any tier this suite runs on | `health` pending, `triage delta` `pod.pending reason=Unschedulable` |
| `steady` (+ `Service/steady`) | healthy, ready, in its Endpoints | nothing — the negative control |

The control is the point. A scorecard that reports two degraded
categories out of a namespace containing two broken workloads is
indistinguishable from one that reports everything it sees, until there
is something healthy sitting next to them that it leaves alone.

`faulty` prints before it dies on purpose: a bundle of a crash-looping
workload is worth little without the last thing the container said, so
the fixture gives the log distiller a fixed corpus in the *previous*
container's output — 40 lines that collapse to one template with
`count=40`, plus one `level=fatal` line.

## Why the inject waits for four restarts

`inject` does not return when `faulty` first reaches
`CrashLoopBackOff`; it waits until the container has restarted at least
four times. Early on the backoff window is 10s, then 20s, so the
container spends most of each cycle *running* and a scan a second later
sees no waiting reason at all — the case then fails for timing rather
than for behaviour. By the fourth restart the window is ~80s and grows
to a 300s cap, which comfortably covers the several `health`, `triage
delta` and `bundle` invocations the cases make. It costs a couple of
minutes on inject and buys a fixture that does not flake.

## What to expect

```sh
lookout health --namespace=lookout-uat-broken
lookout triage delta --namespace=lookout-uat-broken --restarts=1 --pending-age=10s
lookout bundle --workload=Deployment/lookout-uat-broken/faulty
```

- `health` reports `crashloops` and `rollouts` degraded, naming
  `faulty` and `stuck` inline; `steady` appears in neither. The
  cluster-scoped categories answer `unavailable` **with a reason**
  (`run without --namespace`) rather than pretending to be healthy.
- `pending` needs `triage delta --pending-age=10s` to go degraded
  quickly: `health` has no `--pending-age` and delegates to delta's 5m
  default, so on a fresh inject the category is honestly still healthy.
- `triage delta` reports `pod.crashloop` (with `restarts=`,
  `last_state=Error`, `exit_code=1`) and `pod.pending` (with the
  scheduler's own `Unschedulable` message), and nothing about `steady`.
- Both `faulty` and `stuck` also produce `workload.rollout`, and the
  two findings share **one** `fingerprint=` — the §8 fingerprint hashes
  the incident *class*, not the object, which is what lets a fleet
  roll up "17 workloads failing this way" into one line.

The full contract is asserted by `examples/uat-cases/40-toplevel.sh`
and `examples/uat-cases/30-root.sh`; this scenario's `verify` is only a
smoke check that the objects reached the states they claim.

## Explore by hand

```sh
lookout bundle --workload=Deployment/lookout-uat-broken/faulty --max-templates=1
lookout triage delta --namespace=lookout-uat-broken --only=pods --restarts=1
lookout watch --dry-run --storm=off --namespace=lookout-uat-broken
kubectl -n lookout-uat-broken get pods
```

Agent-harness prompt to try:
> Something is wrong in the lookout-uat-broken namespace. What, and is
> anything there still healthy?
