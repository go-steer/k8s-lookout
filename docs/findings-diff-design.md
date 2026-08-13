# Findings diff — run-to-run transitions and ack windows, decided

DESIGN.md §9.4 gives *triaged reality at a point in time*: a health
scan reports the diagnosis and the paper trail instead of a fresh
unknown. What it does not give is the difference between two scans.
An unattended agent scanning every fifteen minutes therefore re-lists
the same forty open findings every fifteen minutes, and the reader
learns to skim. The signal that makes unattended operation tolerable
is **`new` vs `ongoing` vs `resolved`** — "3 things changed", not "40
things are wrong".

`go-steer/mast` reached the same conclusion from the other side while
assessing LangChain's autonomous Kubernetes SRE agent
(`docs/assessments/langchain-sre-agent.md`), and resolved the open
question "where does finding state live?" in favor of **lookout, not
mast**: mast and core-agent are deliberately domain-neutral, and a
`Finding` type in the substrate would be its first domain-aware type.
Issue #212 is the lookout side of that decision, and this note is the
§4.1 design-doc change that admits the surface.

**Decision: a new `findings` command group — `lookout findings diff`
(MCP tool `k8s_findings_diff`) and `lookout findings ack`
(`k8s_findings_ack`) — over a new `finding_state` table in the §9.1
store (migration v6), summarized as DESIGN.md §9.5.**

## Settled decisions

- **A second key grain, alongside the frozen fingerprint.**
  `engine.Fingerprint(kind, reasonClass, objectClass, zone)` hashes
  the incident **class** and deliberately excludes the affected
  object; it is a frozen cross-cluster contract and drives fleet
  rollup. The diff needs **instance** identity. So `pkg/findings`
  adds a *subject key* —
  `(cluster, namespace, kind_of_object, normalize(name), canonical_reason)`
  — and the fingerprint is not touched. Both grains ride every
  transition record: the envelope's `fingerprint` for the class, the
  `subject_key` detail for the instance.

- **Generated pod suffixes are stripped, conservatively.** A
  CrashLooping `payment-backend-7d9f8-x9k2l` rescheduled as
  `payment-backend-7d9f8-b2ndf` is one ongoing finding, not a
  `resolved` plus a `new`. `NormalizeName` strips trailing segments
  drawn from apimachinery's generated alphabet
  (`bcdfghjklmnpqrstvwxz2456789` — no vowels, no `0`/`1`/`3`), which
  is what keeps StatefulSet ordinals, CronJob timestamps, and ordinary
  words from being eaten. **At most two suffixes are stripped**, so a
  Helm release installed as `myapp-7f8bd` keeps its release segment
  rather than folding two releases' findings into one `myapp`. That
  cap is the one place normalization is deliberately not idempotent,
  and `TestNormalizeNameCapIsNotIdempotent` pins it.

- **Transitions are a wire enum, append-only.** `new`, `ongoing`,
  `escalated`, `resolved`, `suppressed`. mast reads these strings to
  build its digest; renaming one is a breaking change to a consumer
  that does not compile against us.

- **A de-escalation stays `ongoing`.** critical → warning is reported
  as `ongoing` with `prev_severity=critical`, not as a sixth class.
  The finding did not change state; the severity pair tells the fuller
  story to anyone who wants it, and the four "something happened"
  classes stay clean.

- **The state is the answer, so the surface is durable by
  construction.** `--store` is mandatory. A diff with nowhere to
  persist reports everything `new` on every run — worse than useless,
  because it looks like it works. Per §9.1 there is no default store
  path anywhere in lookout, and the usage error says so.

- **Built by generalizing the §9.1 store, not by adding a second
  tracker.** Migration v6 adds `finding_state`, one row per currently
  **open** subject, next to the occurrence and triage-status tables in
  the sentinel's own SQLite file.

- **Resolved rows are deleted, and the table is exempt from the §9.1
  TTL prune.** Dropping the row is what makes a recurrence three days
  later read as genuinely `new` rather than as a permanently-ongoing
  zombie. Because the differ deletes its own rows, the table needs no
  TTL — and must not have one, since an ack-bearing row that a prune
  removed would silently un-ack a finding.

- **The whole state set is swapped in one transaction.** The diff's
  `Next` *is* the complete new state by construction. A crash between
  "insert the new rows" and "delete the resolved ones" would strand
  resolved subjects and report them `ongoing` forever; one transaction
  makes that unrepresentable. Volume is "how many things are broken
  right now" — tens — so rewriting the table costs nothing worth
  optimizing.

- **State is persisted BEFORE transitions are emitted.** A failed
  write yields exit 1 and no output, which the operator recovers from
  by rerunning. The other order would hand them a digest whose
  transitions never happened as far as the next run is concerned, and
  every `new` in it would be announced again.

- **Acks are time-boxed and assert nothing.** `--for 4h` (default 4h)
  writes an absolute expiry on the state row. This is deliberately
  *not* §9.4's `severity_override`, which is a standing routing
  judgment backed by a diagnosis; an ack is "I have this, stop
  telling me until lunch". There is no "forever" — an ack that cannot
  expire is an outage nobody remembers muting.

- **The ack outranks escalation, and pins severity.** Inside an open
  window a subject is reported `suppressed` even if it got worse: a
  severity bump during someone's own remediation (a rollback
  restarting pods) is usually their own churn, not news. The recorded
  severity is **not** advanced inside the window, which is what makes
  the deferred escalation fire the moment the ack expires instead of
  being silently absorbed. Expiry resurfaces the subject as `ongoing`,
  never `new` — it never went away.

- **Acking an unknown subject is an error, not an upsert.** An
  operator acks a subject they were just shown, so an unknown key is a
  typo or a stale digest. And a row conjured by an ack alone would be
  absent from the next report, be classified `resolved`, and be
  deleted — an ack that evaporated one run later with no error
  anywhere.

- **The report is read from a pipe, in either wire format.** `--report
  -` reads stdin; format is detected per line, because logfmt is the
  §4.2 default and requiring `--format=json` upstream would make the
  obvious `lookout health | lookout findings diff --report -` silently
  parse nothing. The §4.2 summary line is skipped, since a report is
  an output contract being re-read as input.

- **An unparseable record is a hard error.** Silently dropping records
  would report the dropped subjects as `resolved` — the exact false
  recovery this surface exists to prevent. Partial input is refused
  before any state is advanced.

- **`--dry-run` exists because a normal run is not repeatable.** After
  a real diff, the same report classifies as all-`ongoing`. Previewing
  needs a mode that does not consume.

- **Both commands are `Writes: true`.** The diff advances state as a
  side effect of answering, so the MCP surface advertises
  `ReadOnlyHint: false` and a convention-following client does not
  auto-approve it as a read (issue #105). `--dry-run` is the read-only
  mode.

- **Lookout owns the state; mast owns identity.** `--by` is recorded
  verbatim and not authenticated. Acks originate from an operator in
  Slack/Chat and traverse mast, which asserts the caller and keeps the
  audit trail. Note that §9.4's triage-status write is likewise not
  permission-gated today (§614); if that is revisited, the ack path
  should be revisited with it.

- **The boundary is a wire contract, not a Go import.** mast pipes a
  §4.2 report in and reads transition records out. It never learns the
  Kubernetes-shaped schema, and lookout never imports mast. That is
  also why `lookout findings` is the one command group that does not
  talk to a cluster at all: its input is another command's output, so
  any producer of §4.2 findings can be diffed — including one that
  does not exist yet.

## Out of scope

- **Wiring the multi-cluster watch runner to this state** waits on
  #208. The storage layer is already cluster-scoped — the subject key
  leads with a cluster segment, and both the read and the whole-set
  swap are filtered by it — so N clusters can share one store file
  today by running the diff N times with a different `--cluster`. An
  unscoped swap would have been the real hazard: one cluster's run
  would resolve another's findings and emit a fleet-wide false
  all-clear. What is *not* here is the sentinel's own `--clusters`
  path feeding `finding_state`; that runner keeps its state in memory,
  so nothing it observes is diffable across restarts yet. Acceptable
  because it is named here: an operator must not discover it via a
  four-hour ack that quietly evaporated during a rollout.

- **Digest rendering.** `findings diff` emits classified transitions
  under the §4.2 envelope, ordered changed-first (escalated, new,
  resolved, then ongoing and suppressed) so a truncated read keeps the
  useful half. Turning that into prose an operator reads in Slack is
  mast's job, and `--transitions=new,escalated,resolved` is the flag
  that hands it just the changed set.

- **Acking a class rather than a subject.** An ack takes one subject
  key. "Silence every ImagePullBackOff in staging" is a different
  feature with different failure modes, and §9.4's `severity_override`
  already covers the standing-judgment half of it.

- **Recording ack history.** The row carries the current window, not a
  journal of who acked what when. mast is the store of record for the
  request; §9.1 occurrences remain the store of record for the
  telemetry.
