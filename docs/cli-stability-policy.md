# CLI stability policy

**Status:** settled (2026-07-29, closes #89). Governs the `lookout`
command-line surface — flag names, flag syntax, defaults, subcommand
names, and exit codes — from now until 1.0, and names the rule that
takes over at 1.0. Wire payloads and output schemas are NOT covered
here: they were frozen at M0 and are governed by the frozen-contract
rules (DESIGN.md §8, AGENTS.md "Back-compat", the repo-map guardian
table), which are stricter than anything below.

## Why this exists

0.9.0 changed `--storm` from bool to string; previously-valid bare
`--storm` became a startup error, the sentinel crash-looped in the
first e2e-kind CI run, and #78 had to fix `examples/sentinel/up` the
same day. The change was justified by dated "zero-deployed-users"
comments — a one-shot amendment with no expiry and no successor rule,
while the install docs push `ghcr :latest` and the deploy manifests
advertise image-swap upgrades. This document is the successor rule.

## Settled decisions

1. **The zero-deployed-users clause is expired.** As of v0.9.0 +
   a published docs site + `:latest` images, we assume deployed users
   we cannot see. No future change may cite zero-deployed-users.

2. **Stability tiers.** Every CLI-surface element is in exactly one
   tier:

   | Tier | Contents | Rule |
   | --- | --- | --- |
   | **Frozen** | the 19 predecessor flags (`TestFlagSurfaceFrozen`); inject kinds and payload fields (AGENTS.md) | never change; guardian-tested |
   | **Stable** | every flag/subcommand that has appeared in a tagged release | deprecate-then-remove only (rule 3) |
   | **Experimental** | flags marked `(experimental)` in their help text, and everything in an unreleased tree | may change without a window; the marker is the contract |

   A flag becomes Stable the moment a release tag ships it without the
   experimental marker. There is no silent third state: if it's in a
   release and unmarked, it's Stable.

3. **Deprecation window for Stable elements.** A breaking change to a
   Stable flag (rename, syntax change, removal — a bool→string
   conversion is a syntax change) must:
   - keep the old form **accepted for ≥2 minor releases**, emitting a
     one-line deprecation warning to stderr (never stdout — stdout is
     payload, DESIGN.md §4.2);
   - say so in CHANGELOG under `### Deprecated` when the window opens
     and `### Removed` when it closes;
   - update `skills/`, `deploy/`, and `examples/` in the SAME PR that
     opens the window (the #78 lesson: our own tree is a deployed
     user).

4. **Defaults are behavior.** Changing a default (e.g. a source
   flipping to `auto`) is a minor-version event, needs a CHANGELOG
   `### Changed` bullet naming the old and new behavior, and must keep
   the old behavior reachable by explicit flag. It does not need a
   deprecation window.

5. **Exit codes are Frozen.** `0` data / `1` runtime error / `2` usage
   error (DESIGN.md §4.2) — agents branch on these.

6. **At 1.0** the Stable tier converts to semver: breaking CLI changes
   only at major versions, window rules unchanged otherwise.

## Out of scope

- Wire payloads, logfmt/JSON field names, summary-line format — frozen
  contracts, governed elsewhere (stricter).
- The MCP tool surface — follows this policy implicitly because MCP
  tools are generated from the same command registry.
- Library API stability under `pkg/` (pre-1.0 Go module semantics;
  see the repo-map's importable-surface note).

## Enforcement

- `TestFlagSurfaceFrozen` continues to pin the Frozen tier.
- Reviewers (human or agent) hold PRs to rule 3's same-PR checklist;
  AGENTS.md "Hard rules" points here so the policy is in every
  contributor's first read.
