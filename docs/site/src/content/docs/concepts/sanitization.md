---
title: Sanitization guarantees
description: What is stripped and masked on every output surface, how CI enforces it with golden tripwires, and the documented recall gaps.
sidebar:
  order: 5
---

Anything `lookout` emits can end up in a model's context window, a chat
transcript, or a log file — places a database password must never
appear. This page explains what is stripped or masked before any output
leaves the process, how CI proves it, and the guarantee's honest
limits. The mechanism is a single sanitizer applied in the emit layer,
before anything reaches stdout, an MCP response, or a session inject —
not opt-in per tool, not skippable by a new command that forgets to
call it.

## What is stripped and masked

- **Secret material is masked everywhere.** `Secret.data` values render as
  key names plus byte sizes only (`keys=password(19B)`); env vars sourced
  from Secrets render as the *reference*
  (`DB_PASSWORD=secretKeyRef:checkout-db.password`), never the value;
  credential-shaped strings (key-anchored values, JWTs, PEM blocks) become
  `[REDACTED]` wherever they appear.
- **System metadata is stripped.** `managedFields`, `resourceVersion`,
  `uid`, and noisy status are removed; defaulted fields are elided. This is
  the "kubectl describe, but token-dense and secret-safe" contract of
  `triage spec`.
- **The topology graph never stores secret values at all** — only names,
  keys, and content hashes, so `triage changes` can report "secret
  db-credentials changed" without ever having held the payload.

## How it is enforced

Two layers of proof, not policy:

- **Golden tripwire tests in CI.** Fixtures plant secrets in every position
  the sanitizer knows about — env, envFrom, volumes, annotations — and a
  payload containing an unmasked credential fixture fails CI. The fixtures
  are the promise: adding a new output surface without sanitizer coverage
  breaks the build.
- **Live exit-check evidence.** The M1 milestone drill planted a marker
  value (`SUPERSECRETVALUE_M1`) in a cluster Secret, mounted it as env in a
  broken workload, and ran the full investigation surface over it — bundle,
  spec, edges, health, and a complete MCP session. Every captured stdout and
  stderr byte was then grepped for the marker and its base64 form:

  ```
  $ grep -r SUPERSECRETVALUE_M1 /tmp/kl-m1-evidence/          # → no matches (exit 1)
  $ grep -r U1VQRVJTRUNSRVRWQUxVRV9NMQ /tmp/kl-m1-evidence/   # base64 form → no matches (exit 1)
  ```

  The value's only traces in any output were the reference and the length.

## The documented gaps — honest scope

The guarantee covers **what `lookout` renders from cluster objects**. Two
recall limits are documented rather than papered over:

- **Free-form application logs.** If an application prints its own secret
  into its log stream, `triage logs` masks it only when it is
  credential-shaped (key-anchored, JWT, PEM, …). An arbitrary string with
  no credential shape — a passphrase that looks like a sentence — is not
  recognizable as a secret in free text. The heuristics' scope is
  documented in the sanitizer source.
- **Value-shape heuristics are heuristics.** They are tuned for recall on
  known credential shapes, and the golden fixtures pin exactly which shapes
  are covered. A shape outside that set is a fixture to add, and the
  tripwire convention makes that a one-file change.

If you need a secret's *value*, `lookout` will not give it to you — by
design, on every surface.
