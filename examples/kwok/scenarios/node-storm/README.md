# node-storm — thirty nodes die in the same second

Thirty fake nodes lose their simulated kubelet at once, and the
scenario watches the **wire** rather than the read path: what the
sentinel actually sends a daemon when a third of the fleet disappears,
and whether recovery closes it all again without anyone asking.

```sh
examples/kwok/scenarios/node-storm/inject
examples/kwok/scenarios/node-storm/verify
examples/kwok/scenarios/node-storm/revert
```

Unlike its four neighbours this one needs the sentinel deployed
(`examples/sentinel/up`), and it is not in `examples/kwok/e2e`'s
default set: it takes about fifteen minutes, most of it spent waiting
for the real taint manager to evict pods on the real
`tolerationSeconds` and then for `--recovery-stable-for` to clear
them. Run it by name.

## Why this is the scale tier and not `examples/scenarios/`

`examples/scenarios/node-failure` already proves the M2 headline —
**one** node dies, ~33 objects go with it, and lookout opens 3 sessions
instead of 33. That claim is about the *pods* on a dead node, and two
real workers are enough to make it.

The claim that needs a fleet is one level up: what happens when the
thing that fails is **many nodes**. A rack, a zone, a bad kernel
rollout, a cloud provider losing an AZ. You cannot express that on a
two-worker cluster at all — and it turns out to behave differently
from the single-node case in a way nobody had measured.

## What to expect

**Detection.** All thirty node failures reach the wire, each naming its
node, within about a minute.

**The read path** summarizes the whole outage in one line:

```
kind=health.category severity=critical category=nodes status=degraded total=30 \
  top="node.notready kwok-node-0000; node.notready kwok-node-0001; …"
```

**The push path agrees with it** — three storms, one per zone, not
thirty sessions. `scale-up` labels every fake node
`topology.kubernetes.io/zone=zone-N` for N in 0..2, and `node-fail`
takes nodes in name order, so a 30-node outage spans all three failure
domains and each one groups:

```
INJECT kind=storm ancestor_kind=Zone ancestor_name=zone-0  affected_count=10
INJECT kind=storm ancestor_kind=Zone ancestor_name=zone-1  affected_count=10
INJECT kind=storm ancestor_kind=Zone ancestor_name=zone-2  affected_count=10
```

This is what [#334](https://github.com/go-steer/k8s-lookout/issues/334)
fixed, and this scenario is the measurement that found it. Before the
fix the same drill opened **thirty sessions**: a Node incident's only
blast-radius key was its own Node, the mined dimensions are
image/node/container, and although every node carried a zone label and
`Signal.Zone` already reached the correlator, zone fed only the storm
*fingerprint* and was never offered as a candidate key. The sharpest
version of the old symptom: all thirty incidents computed the **same
fingerprint** and still opened thirty sessions.

A fleet with no zone labels at all — bare metal, a plain kind cluster —
takes the other path #334 added, the `key_source=simultaneity` cluster
fallback. That one is covered by unit tests rather than by this drill,
because kwok's nodes are labelled and the modelled zone key correctly
wins here.

The pod fallout correlates as it always did. About five minutes in the
taint manager starts evicting, and the evicted pods — which share
declared ancestors — form their own storms keyed on Deployment and
Namespace. `verify` asserts that too: it is the control that says a
zone storm is the correlator working, not the correlator over-grouping.

## The closed loop, which passes

`revert` is an assertion, not just cleanup. `node-heal` gives the
kubelets back, and recovery has to travel the whole way home on its
own: the sentinel notices, holds the nodes for
`--recovery-stable-for`, and injects into the sessions that already
exist. Measured on a 30-node run: **48 `resolved` injects, every one
`resolution=recovered`**, closing all thirty node sessions plus the pod
storms, with zero new sessions and no agent polling. That measurement
predates #334 — thirty node sessions was the state of the world then —
and it is the half of this scenario that always worked.

## Explore by hand

```sh
kubectl -n agent-triage logs deploy/stub-daemon -f &   # the wire
examples/kwok/node-fail 30

# how many pages did one event produce?
kubectl -n agent-triage logs deploy/stub-daemon | grep -c SESSION-CREATE

# what did the storms key on? (expect three Zone storms + pod fallout)
kubectl -n agent-triage logs deploy/stub-daemon | grep 'kind=storm '

lookout health                       # one line, total=30
lookout triage radius --node=kwok-node-0000
examples/kwok/node-heal
```

## Agent-harness prompt

> A third of my nodes just went NotReady at the same time. Tell me
> whether this is one failure or thirty separate ones, what evidence
> you have either way, and which workloads actually lost capacity.

The interesting part of the answer used to be whether the agent could
reconstruct "this is one event" from thirty separate sessions. Now that
the sessions arrive as three zone storms, the question is the next one
down: does the agent notice that *all three* zones went, which is a
different failure from one zone going.
