---
title: "Concepts: how lookout thinks"
description: The mental model behind lookout — the two halves, one map of the cluster, signals instead of alerts, sessions instead of pages, the safety stance, and who is trusted to do what.
sidebar:
  order: 0
---

This page is the ramp into the detail pages: the mental model behind
`lookout`'s behavior, in plain terms, before any schema or flag. Read it
if you have run a few commands (or are deciding whether to deploy the
sentinel) and want to understand why the output looks the way it does.
Nothing here requires having read the design spec.

## Two halves

`lookout` splits along one line: are you asking the cluster a question,
or is the cluster telling you something? The first half is a set of
one-shot diagnostic commands you (or your agent) run mid-investigation —
"what's broken", "what changed", "show me the logs, distilled" — each
answering one question and always ending with an explicit summary line,
so "nothing wrong" never looks like a command that silently failed. The
second half is the **sentinel** (`lookout watch`), a long-running
process deployed in the cluster that notices trouble as it develops —
a rollout that stalled, memory climbing toward a limit, a certificate
counting down — and hands your agent an incident with the relevant
context already attached. Both halves are the same binary running the
same underlying checks; the details are in
[Architecture: two paths, one binary](/concepts/architecture/).

## One map of the cluster

Most incident questions are about relationships, and the Kubernetes API
does not answer those directly: finding everything connected to one pod
takes a dozen separate lookups. `lookout` maintains an in-memory map — the
**topology graph** — that answers "what does this workload depend on,
who is affected if it breaks, and what changed around it" in a single
query, against the live cluster or against any past moment the sentinel
recorded. How the graph is built, kept consistent, and queried back in
time is [The topology graph](/concepts/topology-graph/).

## Fewer, richer signals — not more alerts

Traditional monitoring optimizes for detection: fire an alert per
symptom and let a human sort the pile. `lookout` optimizes for
investigation: fewer observations, each carrying more, already
correlated. Everything it emits — a scan finding, a sentinel incident —
has one fixed shape (a **signal**) and carries a **fingerprint**, a
stable hash that names the *class* of problem rather than the
individual occurrence, so the same incident seen by a scan and by the
sentinel is recognized as one thing, not reported twice. The schema,
the severity classes, and the fingerprint recipe are in
[Signals & fingerprints](/concepts/signals-and-fingerprints/).

## Sessions, not pages

When the sentinel finds something serious, it does not page — it opens
a **session**: an ongoing record of one incident that your agent joins.
Follow-up observations land in the same session; thirty pods evicted by
one dead node become one session, not thirty; warning-level noise is
batched into a shared digest instead of interrupting anyone; and when
the symptom stays clear, the sentinel writes a verified "resolved" into
the session — so every incident ends with an observed outcome, not a
human guessing it is safe to close. That whole lifecycle is
[The closed loop](/concepts/closed-loop/).

## Nothing secret ever leaves

Every output surface passes through one sanitizer before anything is
printed, returned over MCP, or injected into a session: Secret values
render as names and sizes, credential-shaped strings are redacted, and
the topology graph never stores secret values at all. This is enforced
by CI tests that plant fake credentials and fail the build if one ever
appears in output. What exactly is masked, how it is proven, and the
documented limits are in
[Sanitization guarantees](/concepts/sanitization/).

## Runs anywhere, degrades loudly

Most of `lookout` is plain Kubernetes and works on any conformant
cluster, a local kind cluster included. The cloud-specific parts
(GKE/GCP today) sit behind a strict boundary, and a missing capability
always announces itself — an explicit "unavailable" finding or a named
startup error, never a crash and never silence an agent could mistake
for "all clear". The exact split is
[Portability & providers](/concepts/portability/).

## Who is trusted to do what

The trust model is an escalation with a hard stop. `lookout` *observes*:
scans produce findings, the sentinel turns findings into sessions. Your
agent *decides*: it reads those sessions, diagnoses, and records its
judgment. And when something must actually change in the cluster, the
agent *acts through its own permission gates* — `lookout` holds read-only
credentials and never writes to the cluster, so the blast radius of the
eyes is zero by construction. How an agent's judgment feeds back into
routing and scan output is part of
[The closed loop](/concepts/closed-loop/).

---

The detail pages are best read in the order above. The normative
specification behind all of them is
[`docs/DESIGN.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/DESIGN.md)
in the repository; these pages are the user-facing distillation.
