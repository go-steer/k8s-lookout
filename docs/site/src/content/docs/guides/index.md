---
title: Guides
description: Scenario walkthroughs with real captured output — broken workloads, stuck rollouts, resource exhaustion, node failures, post-mortems, and capacity planning.
sidebar:
  order: 0
---

This section is for the moment something is wrong — or just was. Each
guide starts from a symptom you might be staring at and walks the real
investigation, command by command, to a diagnosis and a verified
outcome. By the end of a guide you can run the same workflow on your
own cluster, and you will know which commands answer which questions.

## Which guide do I need?

| What you're seeing | Guide |
| --- | --- |
| Pods are crashing or won't start, and you don't know why | [Investigate a broken workload](/guides/broken-workload/) |
| You shipped a deploy and it never finished rolling out | [Your rollout is stuck](/guides/stuck-rollout/) |
| Memory or CPU keeps climbing and an OOM kill looks inevitable | [Catch resource exhaustion early](/guides/resource-exhaustion/) |
| A node went down and everything on it is failing at once | [A node just died](/guides/node-failure/) |
| The incident is over and you need to know what changed before it | [What changed before the incident](/guides/what-changed/) |
| You'd rather hit quota and capacity limits on your terms than in an outage | [Capacity & quota ahead of time](/guides/capacity-quota/) |

Each guide walks the real workflow, using output captured during the
milestone exit drills (abridged, never invented):

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
