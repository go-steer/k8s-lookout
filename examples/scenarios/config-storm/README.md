# config-storm — four workloads, one shared ConfigMap, one incident

Deploys four unrelated Deployments (`consumer-a`…`consumer-d`) that
all mount the same ConfigMap, then deletes the ConfigMap and restarts
all four at once. Every consumer fails to mount. Nothing in the
Kubernetes API connects them: different Deployments, different
ReplicaSets, deliberately spread across nodes. The only thing they
share is the config object — which is exactly the key the correlator
is supposed to find.

```sh
examples/scenarios/config-storm/inject
examples/scenarios/config-storm/verify
examples/scenarios/config-storm/revert
```

## What to expect

- **Sentinel (wire)** — four `FailedMount` incidents inside
  `--storm-window`, folded into ONE `kind=storm` keyed on
  `shared-config`. Not one `storm.member` per folded incident: that
  kind is only for arrivals *after* formation, so an incident folded
  at formation leaves a `storm.member_superseded` if it had already
  opened a session and no wire record at all if it had not. The
  authoritative count is `affected_count` inside the storm payload,
  which is what `verify` reads. The
  §7.5 key priority is node > owner chain > **shared ConfigMap/PVC** >
  namespace: node cannot reach the threshold and the owner chain does
  not span the four, so the config object wins and namespace never
  gets a look in. `verify` asserts `ancestor_kind=ConfigMap`, not just
  that some storm formed.
- **Read-path** — `lookout state edges --workload=Deployment/<ns>/consumer-a`
  names `shared-config` as the broken reference, and `verify` asks all
  four separately. (`state edges` traces the edges *of* a target and
  rejects a bare `--namespace`.) `<ns>` is this run's namespace — see
  below.
- **On revert** — restoring the ConfigMap clears the aggregate:
  `kind=resolved` at storm altitude, one fix for four symptoms.

## Why this one matters

It is the scenario a pod-centric tool gets loudest and least useful
about: four alerts that look independent. The assertion worth keeping
is the *count* — `verify` fails if the storm folded fewer than
`--storm-min` (3) incidents, and a passing run is proof the sentinel
paged once.

The four restarts have to land inside `--storm-window` (60s by
default). If they drift apart the correlator is right to leave them as
four incidents, and `verify` says so rather than pretending it is a
detection miss.

## Why the spread is load-bearing

The first live run of this scenario failed, and it failed in the most
instructive way available: a storm formed, on time, with all four
members — keyed on `Node/lookout-examples-worker`. The consumers are
tiny and the scheduler had put all four on the same worker, so the
node key reached `--storm-min` first and §7.5 handed it the win. The
correlator was right; the scenario was wrong. It claimed to exercise
the shared-ConfigMap tier while feeding the correlator a
higher-priority key that would always beat it.

The manifests now carry a `topologySpreadConstraints` over
`kubernetes.io/hostname` selecting all four consumers, so on the
two-worker examples cluster no node holds more than two — below
`--storm-min` (3). The correlator evaluates candidates best-key-first
and *falls through* a tier that cannot reach the threshold, so the
ConfigMap is the first key that can form a storm. `inject` checks the
placement before the blast and fails there with the node counts,
rather than leaving `verify` to time out on a storm that did form
under a different key.

Worth remembering when writing any storm scenario: a passing
`kind=storm` match proves almost nothing on its own. Assert the
ancestor.

## Why it owns a namespace

The second live run found the other half of the same lesson. Deleting
the ConfigMap does not only break four mounts — it also stalls four
rollouts, and four `rollout_stall` incidents on four unrelated
Deployments have nothing finer in common than the namespace. So a
*second*, namespace-keyed storm forms behind the first one, and it
stays open for minutes after `revert` returns. The next scenario to
raise an incident in that namespace is folded into it as a
`storm.member` — its own session suppressed, its cross-source joins
never fanned out. `probe-flap` failed exactly this way, looking for
all the world like a detection miss in the degradation source.

Running the consumers in their own namespace confines both storms to a
namespace nothing else uses, and `revert` deletes the namespace rather
than the objects. The scenario still runs last in `examples/e2e`: the
*node* tier is not namespace-scoped, so two consumers plus one
unrelated incident on the same node inside 60s could still form a node
storm across namespaces. Last place makes that impossible rather than
unlikely.

### …and a new name every run

Deleting the namespace does **not** take the namespace-keyed storm
with it, which the third live run discovered by failing the way the
second one did. A storm key is a *name*, not an object identity:
`Namespace//<ns>` stays keyable for as long as the storm is open, and
an open storm lives for the full 30m `stormIdleTTL` — refreshed on
every attach. Re-running inside that window recreates the same
namespace name, and step 1 of `StormCorrelator.Observe` attaches the
new `FailedMount`s to the *stale* storm before key priority is ever
consulted. No ConfigMap storm forms and `verify` times out at 240s
blaming the tier it was testing.

What held that first storm open for the whole TTL was #397 — a
`rollout_stall` whose Deployment is deleted never resolved, so its
members never cleared. That is fixed here, and the storm now closes
when its last member does. It does not make the fixed name safe: a
storm that is still open (a run in progress, a scenario left
un-reverted) is still re-attachable by name.

So `inject` mints `lookout-storm-<epoch>` per run and records it in
`$STATE_DIR/config-storm.ns` for `verify`/`revert` (`ns.sh`);
`manifests.yaml` therefore carries no `metadata.namespace` and is
applied with `-n`. `inject` also sweeps namespaces left by runs that
died before `revert`.

This never bit CI, where every run gets a fresh cluster and a fresh
sentinel. It bites the workflow the examples exist for: running a
scenario twice against one long-lived sentinel.

## Explore by hand

```sh
ns="$(cat "${TMPDIR:-/tmp}/lookout-examples/config-storm.ns")"
lookout triage radius --namespace="$ns"
lookout state edges --workload="Deployment/$ns/consumer-a"
```

Agent-harness prompt to try:
> Four services in the same namespace just started failing at the same
> time. Are these four problems or one?
