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
default set: it takes about ten minutes, most of it spent letting the
fleet go quiet before the wire is marked and then waiting for
`--recovery-stable-for` to close the sessions again. Run it by name.

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
node, within about a minute — though only the first two per zone get
an `objectstate.node_notready` inject of their own. The third forms the
storm and the rest arrive as `storm.member`, so `verify` counts nodes
across all three shapes.

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
INJECT kind=storm ancestor_kind=Zone ancestor_name=zone-0  affected_count=3
INJECT kind=storm ancestor_kind=Zone ancestor_name=zone-1  affected_count=3
INJECT kind=storm ancestor_kind=Zone ancestor_name=zone-2  affected_count=3
```

`affected_count=3` is the count at *formation*, not the final size:
the storm opens on its third member and the remaining seven per zone
attach afterwards as `storm.member` injects into the same session. Ten
sessions for the whole outage, measured.

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

**The workload fallout is reported, not asserted.** `verify` prints
every storm key it saw and moves on. The eviction itself is real and
reliable — 163 pods on the failed nodes, 86 left five minutes later —
but it does not reliably produce an *incident*: fake nodes have no
capacity pressure, so the replacement pods schedule onto the survivors
at once and no Deployment misses its 600s `progressDeadlineSeconds`
because of this drill. One run did emit two `progress_deadline` storms
keyed on Namespace, from Deployments that were already mid-rollout when
it started; the next run, identical command and fleet, emitted none.

The claim this looks like it should be testing — a dead node's pods
arrive as one storm, not thirty sessions — is DESIGN.md §7.5's, and
`examples/scenarios/node-failure` asserts it deterministically on kind.
Asserting it here again would only buy a flake.

## The closed loop, which passes

`revert` is an assertion, not just cleanup. `node-heal` gives the
kubelets back, and recovery has to travel the whole way home on its
own: the sentinel notices, holds the nodes for
`--recovery-stable-for`, and injects into the sessions that already
exist. Measured post-#334 on a 30-node run: **33 `resolved` injects,
every one `resolution=recovered`**, naming all thirty nodes and closing
the ten sessions the outage opened, with zero new sessions and no agent
polling. The same measurement before #334 read 48 injects across thirty
sessions — the grouping changed, the closed loop did not. This is the
half of the scenario that always worked.

## Explore by hand

```sh
kubectl -n agent-triage logs deploy/stub-daemon -f &   # the wire
examples/kwok/node-fail 30

# how many pages did one event produce?  (10, not 30)
kubectl -n agent-triage logs deploy/stub-daemon | grep -c SESSION-CREATE

# what did the storms key on?  (expect three Zone storms)
kubectl -n agent-triage logs deploy/stub-daemon | grep 'kind=storm '

# where did the other 24 nodes go?  (attached, not dropped)
kubectl -n agent-triage logs deploy/stub-daemon | grep -c 'kind=storm.member '

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
