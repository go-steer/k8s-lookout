# store-postmortem — a question about a cluster that no longer exists

A **UAT harness**, not a failure scenario: nothing here breaks, and
nothing is expected on the wire. It manufactures the one situation the
§6.6 graph-history flags exist for — you want to know what the topology
looked like at 03:14, and it is now 09:00.

```sh
examples/scenarios/store-postmortem/inject
examples/scenarios/store-postmortem/verify
examples/scenarios/store-postmortem/revert
```

## The window

1. Start a **local** sentinel writing a `--store` with graph snapshots on.
2. Create `postmortem-canary`: a Pod and the ConfigMap it mounts.
3. Wait until the **store** — not the API — can answer for the canary.
4. Scale `lookout-demo/web` 2→3, a change to the target's own pods.
5. Record the **onset** instant.
6. Delete the canary, and wait until the store's newest snapshot has
   lost it too. Record the **after** instant.

From step 6 on, the canary exists only in the store. That is the whole
design: every `--at` assertion has a live control that must **fail** to
find what `--at` finds. A fixture that merely proved `--at` returns rows
would pass just as well if `--at` silently reported *now*, which is the
one wrong answer a post-mortem tool must never give.

The two instants bracket the window because the answer changes across
it: at **onset** the canary is in the topology; at **after** it is not,
and — issue #393 — its `Added` record from earlier in the same window
has retroactively vanished with no `Deleted` record in its place.

## Why a local sentinel

The scenario runs `lookout watch --dry-run --store=… --storm=on` against
the current kubeconfig instead of adding a store to `examples/sentinel/up`.
Three reasons, in order of weight, and the long version is in
`examples/lib.sh`:

1. The shipped image is **distroless**, so `kubectl cp` cannot lift the
   SQLite file off a running pod. Reading it in-cluster means a PVC plus
   a debug pod that mounts it after the sentinel releases it.
2. A local sentinel is the **binary under test**. The deployed one is
   whatever `$LOOKOUT_IMAGE` resolves to — on a default run, a released
   build, whose store proves nothing about the working tree.
3. It leaves `examples/sentinel/up` alone, so every other scenario's
   wire assertions keep running against the sentinel they were written
   for.

Two flags are load-bearing rather than preferences:

- **`--dry-run`** is what lets this run with no daemon: the dispatcher
  prints payloads instead of POSTing them, and `--daemon-url` stops
  being required. The store is written either way — it hangs off
  `--store`, not off the sink.
- **`--storm=on`** is a *precondition*. The graph-history loop is wired
  inside the storm block (`internal/watch/wiring.go`), because the
  topology graph the snapshots serialize only exists under storm
  correlation. `--store` without it writes occurrences and no history
  at all.

`--graph-snapshot-interval=10s` against a 5m default is the only thing
tuned for fixture time; the bootstrap poll is already 10s, so it is the
interval the loop settles onto rather than a change of shape.

## The one fixture that touches the demo app

Every other UAT fixture lands in its own namespace. This one scales
`lookout-demo/web` 2→3, because it needs a rescale on a workload the
graph feed actually tracks and that already has upstream, lateral and
downstream structure around it. `revert` scales back, and `inject`
normalises to 2 **before** starting the sentinel — an interrupted run
leaves `web` at 3, and `scale --replicas=3` on a Deployment already at 3
writes nothing, which would look like success and fail the assertion
later. `examples/uat-cases/60-store.sh` is numbered last for the same
reason.

## What to expect

```sh
onset=$(cat "${TMPDIR:-/tmp}/lookout-examples/store-postmortem/onset")
store="${TMPDIR:-/tmp}/lookout-examples/store-postmortem/lookout.db"

lookout triage radius  Pod/lookout-demo/postmortem-canary --at "$onset" --store "$store"
lookout triage changes Deployment/lookout-demo/web        --at "$onset" --store "$store" --since=30m
```

- `radius --at` resolves the canary and its `Mounts` edge; the same
  command **without** `--at` exits 1 with *not found in the topology*.
- `changes --at` has all three relations from one query — the canary at
  `relation=lateral`, `change.scale` on the ReplicaSet at
  `relation=upstream` with `fields="replicas=2→3"`, and the new pod at
  `relation=self` — all `origin=log`. Live gives one row, `origin=event`,
  and says `source=live-approximation`.
- History rows carry no `ready=` and mark referenced-only kinds
  `observed=unknown`; live rows do the opposite.

## Known gaps, asserted rather than ignored

- **#396** — history has no `Selects`/`RoutesTo` at all: the graph feed
  watches pods, nodes and replicasets, so the whole routing layer is
  missing from `--at` and nothing in the output says so.
- **#393** — `reason=Deleted` is structurally unreachable, and a deleted
  object's earlier records disappear with it.

`examples/uat-cases/60-store.sh` pins both with refutations that will
fail — loudly, naming the issue — the moment either is fixed.

## What is left behind

`revert` stops the sentinel with **SIGINT**, not SIGKILL: the store's
change-record writer is buffered and flushes on shutdown, so killing it
hard is how you lose the tail of the delta log. It deliberately **keeps**
the store and the sentinel log under `$LOOKOUT_EXAMPLES_STATE` — after a
failed run they are the only evidence of what the sentinel saw, and
`examples/kind/down` removes the directory wholesale.

## Explore by hand

```sh
lookout triage radius  Deployment/lookout-demo/web --at "$onset" --store "$store"
lookout triage radius  Deployment/lookout-demo/web                 # the same query, live
lookout triage changes Deployment/lookout-demo/web --at 5s --store "$store" --since=30m
lookout triage status  --store "$store" --resource=Deployment/lookout-demo/web
```
