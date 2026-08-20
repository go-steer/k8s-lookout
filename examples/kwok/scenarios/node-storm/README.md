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

Three results, and they do not agree with each other.

**Detection is fine.** All thirty node failures reach the wire, each
naming its node, within about a minute.

**The read path summarizes correctly** — one line for the whole
outage:

```
kind=health.category severity=critical category=nodes status=degraded total=30 \
  top="node.notready kwok-node-0000; node.notready kwok-node-0001; …"
```

**The push path fragments.** The sentinel opens **thirty sessions**,
one per node, with no storm formed between them
([#334](https://github.com/go-steer/k8s-lookout/issues/334)). Each Node
incident's only blast-radius key is its own Node, the mined dimensions
are image/node/container, and although every node carries
`topology.kubernetes.io/zone` and `Signal.Zone` is already plumbed into
the correlator, zone feeds only the storm *fingerprint* — it is never
offered as a candidate key. The sharpest version of the symptom: all
thirty incidents compute the **same fingerprint** and still open thirty
sessions.

That last one is the scenario's `soft` check, for the same reason the
gaps in `endpoints-empty` are soft — a driver that goes red on a filed
defect stops being a signal. It becomes a hard assertion when #334
lands.

The pod fallout, meanwhile, correlates exactly as designed. About five
minutes in the taint manager starts evicting, and the evicted pods —
which *do* share declared ancestors — form storms keyed on their
Deployment and Namespace. `verify` asserts that too, which is what
keeps the soft check honest: the correlator works, it just has no key
for nodes.

## The closed loop, which passes

`revert` is an assertion, not just cleanup. `node-heal` gives the
kubelets back, and recovery has to travel the whole way home on its
own: the sentinel notices, holds the nodes for
`--recovery-stable-for`, and injects into the sessions that already
exist. Measured on a 30-node run: **48 `resolved` injects, every one
`resolution=recovered`**, closing all thirty node sessions plus the pod
storms, with zero new sessions and no agent polling. The closed loop
scales; only the correlation does not.

## Explore by hand

```sh
kubectl -n agent-triage logs deploy/stub-daemon -f &   # the wire
examples/kwok/node-fail 30

# how many pages did one event produce?
kubectl -n agent-triage logs deploy/stub-daemon | grep -c SESSION-CREATE

# what did the storms that DID form key on?
kubectl -n agent-triage logs deploy/stub-daemon | grep 'kind=storm '

lookout health                       # one line, total=30
lookout triage radius --node=kwok-node-0000
examples/kwok/node-heal
```

## Agent-harness prompt

> A third of my nodes just went NotReady at the same time. Tell me
> whether this is one failure or thirty separate ones, what evidence
> you have either way, and which workloads actually lost capacity.

The interesting part of the answer is whether the agent reconstructs
"this is one event" from thirty separate sessions, or reports thirty
incidents because that is how they arrived.
