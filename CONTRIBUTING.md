# Contributing to k8s-lookout

Thanks for looking. This document is the short path from "I have a
change" to "it is merged": what the project will ask of a patch, and
where the answers already are.

- **Adding a check** — [`docs/adding-a-check.md`](./docs/adding-a-check.md)
  walks one real command end to end, then lists all nine touchpoints.
  Start there; it is the most common contribution by a wide margin.
- **How the tree is laid out** — [`docs/repo-map.md`](./docs/repo-map.md).
- **What the project is trying to be** — [`docs/DESIGN.md`](./docs/DESIGN.md),
  the normative specification. It records *rejected* alternatives with
  reasons (§2, §6.2, §6.6, §9.4). If a review says "that was
  considered", this is where it was considered.

## Before you open a PR

Four things, in this order:

```console
$ dev/tools/gen-skill-refs      # if you touched a command declaration
$ dev/tools/gen-site-docs       # ALWAYS last of the two generators
$ dev/tools/ci                  # the same checks CI runs
```

1. **Sign your commits off.** This project uses the
   [Developer Certificate of Origin](https://developercertificate.org/).
   `git commit -s` adds the trailer; a commit without one will not
   merge. That is the whole CLA story — there is no separate
   agreement to sign.
2. **Regenerate docs in that order.** Four surfaces are generated from
   the `checks.Command` declarations — `--help`, the MCP tool schema,
   the skill references, and the site reference pages. `gen-site-docs`
   reads output the skill generator writes, so running it first
   produces a stale tree that CI then fails on. There is a drift test
   for each.
3. **Run `dev/tools/ci`.** Build, vet, lint, unit tests, gofmt, `go mod
   tidy`, docs lint, govulncheck, version fallback — the same set the
   `ci.yml` workflow runs, so a green local run is a green PR. The
   individual checks live in `dev/ci/presubmits/` if you want to run
   one.
4. **Add a `CHANGELOG.md` entry** under `## [Unreleased]` if the change
   is user-visible. Ours are prose, not one-liners: say what changed,
   and say *why* it is the right change. The release notes are that
   section verbatim, so the entry is the only description most people
   will ever read.

Two things that catch people out:

- **A stale golden in a package you did not touch.** Adding or
  changing a command moves `lookout --help`, the MCP tool list, and
  both doc generators. `UPDATE_GOLDEN=1 go test ./...` regenerates
  every golden in the tree; that env var is the only update mechanism.
- **Lint findings pointing at a file that does not exist.** A stale
  shared golangci cache, usually from another worktree.
  `golangci-lint cache clean`.

## What review will ask about

The house style is heavy on rationale comments, and reviews reflect
that. Specifically:

- **State the claim.** Every check makes one, in one sentence, and it
  has to be true, actionable, and silent on a healthy cluster. A
  finding that does not imply a next step is noise with a severity
  attached.
- **Name the look-alike.** Every rule worth writing has a legitimate
  configuration that resembles the defect. Write down which one you
  excluded and why — in a comment, not just in the PR description.
- **Never empty and silent.** DESIGN §2: a check that could not run
  says so with a named sentinel finding and exits 0. Reporting a clean
  bill of health you did not earn is the one failure mode that
  destroys the tool's usefulness, because the whole product is "when
  lookout says nothing, nothing is wrong".
- **Prove the silence.** A healthy fixture asserting `findings=0` is
  the test most often left out and the one that catches the check that
  fires on everything.
- **Sanitize.** No secret value may reach stdout, an MCP response, or
  an inject. `pkg/emit`'s sanitizer is a security boundary with golden
  fixtures behind it — extend them when you add an output path.
- **Respect the provider boundary.** `pkg/checks` and `pkg/sources`
  never import cloud SDKs; cloud functionality goes through
  `pkg/cloud.Provider`. A CI conformance test keeps the default build
  free of them, and the published SBOMs make that checkable from
  outside.

## Two designs that look like omissions

Both come up often enough to be worth stating here rather than in a
review comment each time.

**Exemptions are file-only, on purpose.** `--exemptions` takes a file;
`pkg/exempt` deliberately refuses to read exemptions from object
annotations or to resolve label selectors. Annotations are rejected
because "let the team requesting the exemption grant it to itself" is
the failure mode the whole feature exists to prevent — an exemption
should require a reviewed change to a file someone owns. Label
selectors are rejected for a second, independent reason: `findings
diff` re-reads a *piped report*, with no cluster to query, so a
selector-based exemption could not be evaluated consistently between
the two commands. A mechanism that means different things depending on
which side of a pipe it runs on is worse than no mechanism.

**Dependencies are minimized, deliberately.** The module has close to
zero build-time dependencies outside `client-go` and the Prometheus
client, and that is a maintained property rather than an accident. A
PR that adds one should say what it buys and why the standard library
does not cover it. Dependabot is configured for security updates, not
for churning to latest.

## Reporting things

- **A bug or a missed detection** — an issue, with the cluster state
  that produced it if you can share it. Redacted `lookout` output is
  usually enough; the format is designed to be shareable.
- **A security vulnerability** — **not** an issue. See
  [`SECURITY.md`](./SECURITY.md).
- **A design question** — an issue is fine. DESIGN.md §15 collects the
  open ones.

## Licensing

Apache 2.0. Go, shell, YAML, and Python files carry the license header
(`dev/tools/add-license-headers` adds it in bulk; `goheader` in
golangci-lint enforces it for Go). Markdown files carry none. By
signing off with the DCO you certify you have the right to submit the
work under that license.
