# Audit ingestion contract — `emit.Finding` → an audit ledger

Companion to [`fleet-audit-detectors-design.md`](./fleet-audit-detectors-design.md)
(the proposal and its four decisions) and
[`signal-schema-v1.md`](./signal-schema-v1.md) (the frozen fleet-rollup wire
contract). This note specifies the seam between them: **what an audit consumer
must do with the bytes a `checks.Command` writes.**

The consumer in question is `kube-agents`' `fleet-audit` skill, whose scanning
half is being replaced by deterministic detectors while its publishing half —
delta, remediation PRs, stale-close semantics — is kept. Roughly fifty-five
detectors are queued behind this note. Writing them against an unstated
ingestion contract means discovering the contract fifty-five times, and
discovering it differently at least twice.

Status: **normative for the consumer seam**, additive within signal-schema v1.
Nothing here changes the wire format; it states what the format already
guarantees and what it deliberately does not.

## 0. The shortest version

| Question | Answer |
| --- | --- |
| What is a ledger finding's stable id? | The **subject key**, not the fingerprint. See §4. |
| What is the fleet rollup's join key? | The **fingerprint**, not the subject key. See §4. |
| What does an empty `fingerprint` mean? | The finding has no incident class. It is not an error and not a gap. See §5. |
| Which cluster is a finding from? | Not in the record. The consumer supplies it. See §6. |
| How does a consumer know a run completed? | The terminating summary line, and only that. See §7. |
| What does an exempt finding mean? | It fired and it is real; the owners have accepted it until `exempt_expires`. Ingest it, suppress it in the UI if you like, never drop it. See §3. |
| What must a consumer do with a field it does not recognize? | Carry it as evidence, never reject the record. See §8. |

## 1. Invocation

A consumer runs a `checks.Command` on one of two surfaces. Both produce the
same record stream:

- **CLI.** `lookout <group> <command> [flags]`. Findings on stdout, diagnostics
  on stderr, `--format=logfmt` (default) or `--format=json`.
- **MCP.** Every command is already a tool named by its `MCPName`; no extra
  wiring is needed to expose a new detector. Exit 0 returns the stdout payload
  as the tool result text; exit 1 returns a tool error.

Exit codes are the first thing to branch on (`pkg/emit/run.go`):

| Code | Meaning | Consumer action |
| --- | --- | --- |
| `0` | Data. stdout carries the records **and** the summary line. | Ingest. |
| `1` | Runtime failure. stdout has **no** summary line. | Fail the run. Do **not** compute a delta. |
| `2` | Usage error — bad flags or arguments. | Fail the run; this is a consumer bug, not a cluster condition. |

Exit 0 with `findings=0` is the valuable case and must not be conflated with
either failure code: it is a scan that ran and found nothing, which is the
whole point of moving off SOPs.

## 2. The record stream

Every line is one flat, ordered `key=value` record (§4.2, `pkg/emit`). The
guarantees a consumer may rely on:

- **One record per line**, in both formats. A JSON line is a single flat
  object, never nested, never an array.
- **Ordered keys.** Envelope fields first, in the fixed order returned by
  `emit.EnvelopeFields()`, then the command's `Details` keys in their declared
  order. JSON preserves the same order as logfmt — that is deliberate, not a
  logfmt artifact.
- **Zero nominal state.** Empty fields are *omitted*, not emitted empty. A
  consumer must treat "key absent" and "key empty" identically.
- **Values are strings**, in both formats, with one exception: `scanned` and
  `findings` on the JSON summary line are numbers. That exception is why a
  naive `map[string]string` decode fails on precisely the line a consumer most
  needs to recognize.
- **Sanitized.** Every finding passes through the §6.5 sanitizer before
  encoding; there is no output path that bypasses it. A consumer does not need
  to re-scrub for secrets, and should not try to reconstruct a masked value.

### Reference implementation

`pkg/findings/report.go` (`ParseReport`) is an in-tree, tested parser of this
exact stream — `lookout findings diff --report` re-reads scan output as input.
A consumer implementing ingestion in another language should treat it as the
normative reference, in particular for:

- per-line format detection (a line starting with `{` is JSON, else logfmt),
  because the obvious invocation is a pipe and logfmt is the *default*;
- logfmt value unquoting, which is Go `strconv.Quote` syntax, applied only when
  the value contains a space, `=`, `"`, or a control character;
- the summary-line skip rule in §7.

## 3. Field mapping

`emit.Finding`'s envelope, and what each field is for on ingestion:

| Wire key | Load-bearing? | Ledger role |
| --- | --- | --- |
| `kind` | **Required.** The only field every finding carries. | The check slug — which detector fired. Dot-namespaced by its owning check (`top.unrequested`, `audit.no_pdb`). |
| `severity` | Yes | `info` / `warning` / `critical`, matching the sentinel's routing levels so a scan finding and a pushed signal for the same symptom compare directly. |
| `namespace`, `kind_of_object`, `name` | Yes | The subject resource. Any may be absent: a cluster-scoped finding has no namespace, a cluster-wide posture finding may have no name. |
| `reason` | Yes | Machine-matchable cause, CamelCase, mirroring `Event.Reason` where one exists. Part of subject identity (§4) — a subject whose reason changed is a *different* finding, not a continuing one. |
| `message` | Evidence only | Human/agent-readable one-liner. **Never** parse it, and never derive an id from it. Message wording is not a contract and will change. |
| `fingerprint` | Conditionally | The §8 incident-class hash when the finding has an incident class; absent otherwise (§5). |
| `exempt_reason`, `exempt_expires` | Yes, when present | Present together or not at all. The finding is covered by a reviewed entry in the operator's `--exemptions` file: it is real, it fired, and someone has accepted it until `exempt_expires`. |
| *`Details` keys* | Evidence | Check-specific fields, `[a-z][a-z0-9_]*`, each declared in the owning command's output glossary and therefore visible in `--help`, the MCP tool schema, and the generated reference page. |

The `Details` keys are exactly the evidence an SOP used to paste by hand. They
are a per-command surface, not a global one: a consumer should carry them
through as an opaque key/value bag rather than modelling each detector's keys.

`kind` and `Details` keys are additive-only in practice — a command's output
glossary is contract-tested, and every emitted key must appear in it — but a
consumer must still tolerate keys it has never seen (§8).

### Exempt findings are annotated, never withheld

An exemption is an assertion by the cluster's owners that a finding is
*intentional*, recorded in a git-reviewed file with a mandatory reason and a
mandatory expiry. It is not a filter. The finding is emitted, counted in
`findings=<n>`, and carries `exempt_reason=` and `exempt_expires=` — so the
ledger can always answer "what is actually true about this cluster" separately
from "what have we agreed to live with." A tool that dropped exempt findings at
the source would make coverage unverifiable, which is the failure this whole
surface exists to prevent.

For the consumer this means three things:

- **Ingest exempt findings like any other.** Same subject key, same
  fingerprint, same ledger row. Exemption is an attribute of the row, not a
  reason to skip it.
- **Suppress at presentation, not at ingestion.** Hiding exempt rows from a
  dashboard is fine and expected; never hiding them from the store.
- **`exempt_expires` is a deadline, not decoration.** It is RFC 3339. Once it
  passes the annotation simply stops appearing — nothing in the stream announces
  the lapse. A consumer that wants to chase renewals should either watch for the
  annotation disappearing or ingest `lookout audit exemptions`, which reports
  lapsed and soon-to-lapse entries as findings in their own right.

Exemption is one of three suppression axes and the only one carried on the wire
here. `findings ack` is operator-driven, transient, and lives in lookout's own
store; §9.4 `severity_override` is agent-driven and asserts a diagnosis. An
exemption is owner-driven, durable, and reviewed in git. A consumer should not
collapse them into one "muted" boolean.

## 4. The two identity grains

This is the part most likely to be got wrong, because the epic's own
description compresses two different keys into "stable id."

There are two, they answer different questions, and a ledger needs both:

**`fingerprint` — the incident CLASS.**
`engine.Fingerprint(kind, reasonClass, objectClass, zone)`, a frozen
cross-cluster contract (`pkg/engine/fingerprint.go`, pinned by cross-cluster
vectors). It deliberately **excludes the object**: `ImagePullBackOff on Pods in
us-east4` is one fingerprint whether it hit one workload or four hundred, in
one cluster or forty. Cluster is deliberately *outside* the hash and rides
alongside as a join dimension; zone is *inside* it, because zone-scoped causes
are exactly what a fleet rollup must group.

**subject key — the INSTANCE.**
`findings.SubjectKey(cluster, namespace, kindOfObject, name, canonicalReason)`,
composed as `<cluster>/<namespace>/<Kind>/<normalized-name>/<reason>`. It is
*derived from finding fields*, not stored on the wire — which is why it works
on posture findings for free, today, with no detector changes. Two properties
matter on ingestion:

- The name is passed through `findings.NormalizeName`, which strips
  Kubernetes-generated suffixes so a rescheduled pod stays the same subject.
  It is conservative by design: StatefulSet ordinals and CronJob time-stamps
  survive.
- The reason is canonicalized (`engine.CanonicalReason`), so the
  `ErrImagePull`/`ImagePullBackOff` family collapses the same way dedup and
  fingerprinting collapse it. Reasons outside the mapping — which is every
  posture reason — pass through unchanged.

**Which to use where:**

| Consumer operation | Key | Why |
| --- | --- | --- |
| Ledger finding id; run-over-run delta; stale-close | **subject key** | The ledger tracks *"is `payment-backend` still missing a PDB"*, an instance question. A class key would collapse every workload with the same posture gap into one ledger row and make per-workload remediation untrackable. |
| Remediation PR targeting | **subject key** | A PR edits one workload's manifest. |
| Fleet rollup across clusters | **fingerprint** | `(fingerprint, cluster)`, exactly as §8 intends: identical posture across clusters collapses to one rolled-up finding with a per-cluster breakdown. |
| Joining a scan finding to a pushed sentinel signal | **fingerprint** | The push and pull paths are designed to dedupe on this one key. |

A consumer that joins on the wrong grain fails in a characteristic way:
fingerprint-as-ledger-id merges unrelated workloads into one row; subject-key-
as-rollup-key produces one "fleet finding" per workload per cluster and no
rollup at all.

**A caveat that is not yet closed.** The design note and the epic both assume
posture findings will carry a `ScanFingerprint`. They do not today, and
`engine.ScanFingerprint` cannot produce one as written: it hardcodes the
`k8s-event` kind and runs its input through `CanonicalReason`, an *event-reason*
canonicalizer, and posture findings have no event reason. Only `lookout health`
and `lookout triage delta` stamp a fingerprint at all. The posture recipe is
settled in the exemptions/scaffolding change (#234), not here. Until it lands,
a consumer ingesting posture findings has the subject key and only the subject
key — which is sufficient for the ledger, and insufficient for rollup.

## 5. An empty `fingerprint` is a shape, not a gap

`emit.Finding.Fingerprint` is explicitly reserved as empty for findings with no
incident-class identity: **scorecard lines, inventory records, probe results.**
It is then omitted from the record entirely, like every other empty field.

A consumer must therefore:

- **Not** treat an absent `fingerprint` as a malformed record, a detector bug,
  or a reason to drop the finding.
- **Not** synthesize a substitute fingerprint. A locally-derived hash is the
  `derive_finding_id` failure mode returning in a new costume: two consumers
  would derive it differently and the cross-cluster join would silently stop
  matching.
- Fall back to the subject key, which is defined for every finding.

This shape is load-bearing for consistency drift, not an edge case. Per
decision 2, per-cluster detectors emit their config facet values *as* inventory
records — fingerprint-less by construction — and the rollup layer derives
`drift` findings by comparing the set. A consumer that rejects fingerprint-less
records rejects the entire input of the drift stream.

## 6. Cluster identity is supplied, not parsed

No `checks.Command` emits a `cluster` key. The envelope has no such field, and
`findings.SubjectKey` takes the cluster as an argument precisely because it
cannot be read from the record.

This is correct, not an omission: `lookout` is one process per cluster, and the
process that *invoked* it authoritatively knows which cluster it pointed at,
whereas a value the binary self-reported would be a second source of truth able
to disagree with the credentials actually used.

The consumer therefore stamps the cluster (and project/zone, where its rollup
needs them) as it ingests each stream, from its own fan-out topology. Per
decision 2 the consumer owns fan-out, so it always has this in hand.

## 7. The terminating summary line

Every successful invocation ends with exactly one summary line:

```
scanned=<n> findings=<n> elapsed=<d> [note=… …]
```

In JSON, the same keys in the same order as one object, with `scanned` and
`findings` as numbers.

**Recognizing it.** The summary carries no `kind`; every finding must have one.
That is the discriminator `ParseReport` uses, and the one a consumer should
use. Do not match on `scanned`/`findings` alone as a *positive* test for
records — a kind-less record that is not summary-shaped is malformed and should
be a hard error, because silently dropping records from a delta surface
manufactures false `resolved` transitions.

**Its absence is a failed run.** This is the single most important rule in this
note, because it is what replaces "trust the model's transcript":

> A stream that ends without a summary line is a **truncated or failed run**.
> The consumer must fail it, and must **not** compute a delta against it.

Ingesting a truncated stream as if it were complete turns every unreported
finding into a `resolved` transition and closes the ledger rows for them. That
is the rename-as-resolution class of bug, arriving through a different door.
`findings=0` *with* a summary line means scanned-and-clean; no summary line
means nothing at all is known.

**Notes.** Keys after `elapsed` are §6.6 annotations — the "one place that
cannot be missed" seam. Graph-backed commands already stamp `source=live|history`
and a resolved `at=`. A consumer must read the notes it knows about and carry
the rest, not reject the line for carrying a key it has not seen.

**`exempt=<n>`.** When the invocation was given `--exemptions`, the Writer
appends `exempt=<n>` as the last note: how many of the `findings=<n>` just
emitted were covered by a reviewed entry. Three properties the consumer can
rely on:

- **`exempt` is a subset of `findings`, always.** Both counters move on the
  same successful write, so `exempt` can never exceed what `findings` accounts
  for. `findings=12 exempt=3` means nine unexempted findings, computable
  without reading a single record.
- **`exempt=0` is meaningful and is not the same as the key being absent.**
  Present-and-zero says an exemption file was in effect and nothing matched —
  a file that has aged out of relevance. Absent says no file was supplied, so
  nothing about exemption state is known for that run.
- **It is Writer-owned.** No command can set, shadow, or forget it, for the
  same reason the sanitizer lives on that path: there is no output path that
  bypasses it.

**Two-stage coverage.** Decision 2 splits detection from rollup, which splits
"a detector that ran is its own coverage proof" in two. Per-cluster, the
summary line is still that proof. At fleet level it is not sufficient: a rollup
over four reports when five clusters were scheduled looks identical to a clean
rollup over five. **The rollup must assert reports-expected against
reports-received and fail loudly on a short count.** Expected count comes from
the consumer's fan-out list; per decision 3 the cohort is exactly the set of
reports passed in, so this is a length check, not a roster lookup.

## 8. Forward compatibility

The detector roster is expected to roughly triple. A consumer must survive that
without a coordinated release:

- **Unknown `Details` keys** — carry through as evidence. Never reject.
- **Unknown summary notes** — carry through. Never reject.
- **Unknown `kind` values** — ingest and publish. A consumer that allow-lists
  kinds turns "a new detector shipped" into "a new detector's findings are
  silently dropped", which is unverifiable coverage again.
- **New commands** — appear as new MCP tools automatically, by `MCPName`.
- **Removed or renamed envelope fields, or a changed fingerprint recipe** — do
  not happen within v1. Any such change is a v2 negotiation with fleet
  consumers (`signal-schema-v1.md`), not a patch.

Note on kinds: a scan finding's `kind` is a **check-local label**, not a v1
signal kind, and is not in `signal-schema-v1.md`'s kind inventory — that
inventory enumerates the wire kinds carrying a `pkg/inject` payload struct.
Scan kinds are documented where they are produced: the emitting command's
output glossary, rendered into `--help`, the MCP tool schema, and the generated
reference page. A consumer discovers them from the tool schema, not from the
schema doc.

## 9. What the consumer deletes

The prize, restated concretely. Once findings arrive as `emit` envelopes, the
machinery whose only purpose is policing an LLM has nothing left to police:

| Consumer-side machinery | Why it goes |
| --- | --- |
| `FINDING_ID_RE` | Ids are no longer free-text strings needing shape validation. |
| `derive_finding_id` | The subject key is derived deterministically from the record's own fields, by shared code, identically on every run. |
| `ID_SCHEME` versioning | An id scheme needs versioning because it was guesswork that had to be revised. `SubjectKey` is a fixed-arity composition; the fingerprint is a frozen contract. |
| Attestation-not-verification command validators | A detector that ran emits a summary line. That is verification, not attestation, and §7 makes its absence a hard failure. |

These deletions are coordinated with the `kube-agents` repo and are not part of
this repository's change.

## 10. Non-goals

- **Not a replacement for the publishing harness.** Delta over the ledger, PR
  authoring, and remediation-state tracking remain the consumer's job. This
  note ends where a finding becomes a ledger row.
- **Not a transport spec.** How a consumer runs `lookout` — subprocess, MCP,
  Job-per-cluster — is out of scope; all of them produce the same stream.
- **Not the posture fingerprint recipe.** That is #234; see the caveat in §4.
- **Not the exemption wire shape.** Exemption annotations and the summary-line
  count are additive fields landing with the first `audit` detector (#234);
  they slot into §3 and §7 without changing anything stated here.
