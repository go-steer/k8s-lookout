---
title: Operations
description: Deploying and running the sentinel — manifests, RBAC, metrics, migration. Placeholder — content lands in a follow-up PR.
---

Placeholder — the operations pages (deploying `lookout watch`, RBAC tiers,
the occurrence store volume, dashboards, migrating from
`k8s-event-watcher`) land in a follow-up PR.

Until then:

- [`deploy/`](https://github.com/go-steer/k8s-lookout/tree/main/deploy)
  carries the shipped manifests.
- [Reference → lookout watch](/reference/watch/) (generated, current) is the
  full sentinel flag table;
  [Reference → Prometheus metrics](/reference/metrics/) documents the
  `--metrics-addr` surface.
