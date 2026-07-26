---
title: Concepts
description: How k8s-lookout is put together — the two data paths, the topology graph, signals and fingerprints, the closed loop, sanitization, and the provider boundary.
sidebar:
  order: 0
---

Six pages, best read in order — each one is the "why" behind a set of
commands and flags in the [Reference](/reference/):

- [Architecture: two paths, one binary](/concepts/architecture/) — the
  read-path (one-shot diagnostic commands) and the watch-path (the resident
  sentinel), and why both live in a single `lookout` binary.
- [The topology graph](/concepts/topology-graph/) — the pod-nexus index that
  answers "what relates to this pod", live and at any past instant.
- [Signals & fingerprints](/concepts/signals-and-fingerprints/) — the frozen
  wire schema, severity classes, dedup families, and the incident-class
  fingerprint fleets join on.
- [The closed loop](/concepts/closed-loop/) — recovery injects, storm
  correlation, the watchboard, and triage-status records: sessions with
  verified outcomes instead of alerts.
- [Sanitization guarantees](/concepts/sanitization/) — what is stripped and
  masked on every output surface, how CI enforces it, and the documented
  gaps.
- [Portability & providers](/concepts/portability/) — what runs on any
  conformant Kubernetes cluster, what needs a cloud provider, and how
  absence degrades loudly instead of silently.

The normative specification behind all of this is
[`docs/DESIGN.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/DESIGN.md)
in the repository; these pages are the user-facing distillation.
