# node-failure — a worker dies; one storm, not thirty sessions

**kind only, and destructive by design** — `docker stop`s the second
worker node, replaying the M2 exit-check drill (docs/milestones/M2.md,
dev/drills/node-failure.md for the GKE variant). Every pod on that
node fails at once; the point of the drill is what the sentinel does
NOT do: open a session per pod.

Not part of the default `examples/e2e` set — run it explicitly:

```sh
examples/e2e node-failure
# or by hand:
examples/scenarios/node-failure/inject
examples/scenarios/node-failure/verify
examples/scenarios/node-failure/revert     # docker start; the node rejoins
```

The sentinel and stub daemon survive because examples/sentinel/up
pinned them to the FIRST worker; this scenario refuses to run if
they're on the target node.

## What to expect

- **Sentinel (wire)** — the leading `objectstate.node_notready` fires
  ~40s after the stop (before the reactive `NodeNotReady` event);
  a `kind=storm` session forms on the node's blast-radius key and the
  per-pod incidents attach as `storm.member` instead of opening
  sessions. After revert, storm-level recovery resolves the lot.
- **Read-path** — `lookout health` scores the nodes category
  degraded; `lookout triage radius <node-name>` shows the blast
  radius from the topology graph.

## Explore by hand

```sh
lookout health
lookout stab drain --node=lookout-examples-worker2
lookout triage delta --only=nodes
```

Agent-harness prompt to try:
> The sentinel says there's a storm on a node. Summarize the blast
> radius and tell me whether anything user-facing lost capacity.
