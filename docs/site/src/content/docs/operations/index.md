---
title: Operations
description: Running the sentinel in production — the occurrence store, the watchboard, drills, metrics, and troubleshooting.
sidebar:
  order: 0
---

Day-2 material for a deployed sentinel (deploying it in the first place
is [Getting started → Deploy the sentinel](/getting-started/deploy/)):

- [The occurrence store](/operations/store/) — what `--store` records,
  its TTL/size bounds, copying it off a pod for post-mortems, `--at`
  time-travel queries, and epoch semantics across restarts.
- [The watchboard](/operations/watchboard/) — how warning-class noise is
  batched into digests, the size-based rotation lifecycle, and lineage.
- [Drills & verification](/operations/drills/) — the staged-failure
  runbooks in `dev/drills/` and when to run them.
- [Observing lookout](/operations/observability/) — the Prometheus
  metrics, `/healthz`, startup-log verification, and what to alert on.
- [Troubleshooting](/operations/troubleshooting/) — RBAC probe failures,
  source-by-source requirements, common startup errors verbatim, and
  what the `unavailable` markers mean.

The generated [`lookout watch` flag table](/reference/watch/) and
[Prometheus metrics reference](/reference/metrics/) are the authoritative
surfaces these pages link into.
