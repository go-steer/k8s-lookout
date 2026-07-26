# Triage-status write surface — M4 drill observation 1, decided

DESIGN.md §9.4 makes the incident agent the WRITER of triage-status
records ("incident playbooks instruct the agent to write a compact
triage-status record at each material transition"), but until this
change lookout deliberately shipped no CLI/MCP/API write command —
`pkg/store/memory.go` documented that adding one is a §4.1 design-doc
change first. The M4 drill hit that wall exactly as documented: the
feature was consumer-complete (routing, health, bundle, lifecycle) but
producer-blocked — the drill had to run the
`dev/drills/write-triage-status` Go fixture ON THE NODE to play the
diagnosing agent (`docs/milestones/M4.md` §Observations, finding 1).
That fixture is deleted in the change that lands this note, as its
own header promised.

**Decision: the producer is a lookout subcommand — `lookout triage
status` (MCP tool `k8s_triage_status`).** This note is the §4.1
design-doc change that admits it to the surface.

## Settled decisions

- **A subcommand, not a separate tool or daemon endpoint.** Playbooks
  must be able to write the record from ANY §4.3 invocation surface —
  a shell-holding agent (CLI), a distroless `core-agent` daemon (MCP),
  or a test harness — and the multicall registry gives all three from
  one `checks.Command` declaration, with `--help`, the MCP schema, and
  the skill reference doc generated from the same metadata (§4.4.3).
- **It lives in the `triage` group.** The record is the closing move
  of an incident investigation — the same workflow every other
  `triage` verb serves. No new §4.1 group is introduced.
- **Writes go through `pkg/memory`'s `TriageWriter` against
  `--store`** — the identical contract the sentinel's recovery flip
  and the drill fixture use, bound in-tree to the sentinel's SQLite
  store (`pkg/store` migration v4). One writer contract, three
  callers; the §9.4 schema validation (`TriageStatusRecord.Validate`)
  applies unchanged: status enum, §7.7 severity classes for
  `--severity-override`, fingerprint + resource key required.
- **`--store` is mandatory, and the usage error names this note.**
  There is no default store path anywhere in lookout (§9.1), and a
  triage write without the sentinel's store is a write to nowhere.
- **Read mode rides the same command.** Without `--status` the
  command prints the current record(s) for `--fingerprint` /
  `--resource` — the agent's "what did I (or a previous session)
  already conclude?" question, answered from the same table the
  health/bundle join reads.
- **`--status` accepts the four agent-written values only**
  (`investigating|triaged|actioned|escalated`). `resolved` is the
  SENTINEL's lifecycle terminal (§7.4 recovery flips it); an agent
  claiming "resolved" without the observed stability window would
  corrupt the §9.3 corpus labels.
- **Concurrency posture is unchanged:** WAL + busy-timeout absorb the
  CLI writer next to the resident sentinel (proven live in the M4
  drill); per §9.4 there is deliberately no locking/claiming — the
  sentinel remains the single writer of record LIFECYCLE, agents write
  record CONTENT, and upsert-by-(fingerprint, resource_key) makes the
  record current state, not a journal.

## Out of scope

- **Daemon-mediated writes.** The §9.2/§9.4 destination of record is
  core-agent's shared Memory interface, which core-agent v2.7.0 does
  not ship (no `package memory`, no daemon write endpoint —
  `pkg/memory`'s package comment carries the full binding decision and
  the `TODO(core-agent)`). When that surface exists, writes gain the
  daemon's caller identity and permission gate, and the adapter
  replaces the direct store binding — a field mapping, not a redesign.
  Until then, `--store` access control IS the write authorization
  (same trust boundary as `--dedup-persist`).
- **Auto-re-page on regression.** When a downgraded incident's symptom
  rate escalates, the sentinel emits schema-stable evidence
  (`kind=triage.regressed`, M4 drill observation 3) into the bound
  session — it does NOT rewrite or expire the record, and it does not
  re-page. The severity override is the agent's judgment; overriding
  the agent's judgment automatically on a rate heuristic would make
  the record untrustworthy in both directions. The agent (or a human
  reading the session) decides whether to re-triage or escalate.
- **Distributed locking / `assigned_agent` claiming.** Rejected in
  DESIGN.md §9.4; nothing here reopens it.
