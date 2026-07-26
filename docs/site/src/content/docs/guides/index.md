---
title: Guides
description: Scenario walkthroughs with real captured output — broken workloads, stuck rollouts, resource exhaustion, node failures, post-mortems, and capacity planning.
sidebar:
  order: 0
---

Each guide starts from a problem and walks the real workflow, using output
captured during the milestone exit drills (abridged, never invented):

- [Investigate a broken workload](/guides/broken-workload/) — the
  bundle-first flow: root-causing a double fault in one call.
- [Your rollout is stuck](/guides/stuck-rollout/) — `rollout.stall` fires
  while the old revision still serves; roll back to a verified `resolved`.
- [Catch resource exhaustion early](/guides/resource-exhaustion/) — a
  memory-leak forecast lands a session 14 minutes before the OOM kill.
- [A node just died](/guides/node-failure/) — storm correlation: one
  session for a 33-object blast, and its full member lifecycle.
- [What changed before the incident](/guides/what-changed/) — `--at`
  post-mortems from a copied sentinel store, offline.
- [Capacity & quota ahead of time](/guides/capacity-quota/) — the
  correlated quota incident, the drafted increase request, and the cloud
  sweep commands.

Every guide ends with a pointer to the matching agent skill in
[`skills/`](https://github.com/go-steer/k8s-lookout/tree/main/skills) — the
same workflows, packaged for the agents themselves.
