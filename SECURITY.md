# Security policy

## Reporting a vulnerability

**Please do not open a public issue.**

Use GitHub's private vulnerability reporting —
[**Report a vulnerability**](https://github.com/go-steer/k8s-lookout/security/advisories/new)
on the Security tab. It goes only to the maintainers, and it gives us
a private fork to develop and review the fix in before anything is
published.

If that form is unavailable to you for any reason, email a maintainer
listed in [`CODEOWNERS`](./.github/CODEOWNERS) directly and say
"security" in the subject.

Please include, as far as you can:

- what an attacker gets, concretely;
- the version or commit — `lookout version` prints the version, commit
  and build date stamped at release;
- whether it needs the `-gke` flavor, a particular RBAC grant, or the
  MCP server running;
- a reproduction, ideally as a cluster fixture or a `lookout` command
  line.

We will acknowledge within **3 business days** and give you an
assessment within **10**. If we agree it is a vulnerability we will
tell you the fix timeline and credit you in the advisory unless you
would rather we did not. If we disagree we will say why, in enough
detail that you can push back.

Please give us reasonable time to ship a fix before disclosing
publicly. We do not run a bug bounty.

## What is supported

Pre-1.0: **the latest released minor only**. Fixes land on `main` and
go out in the next release; there are no backport branches. Pinning to
an older minor means picking up the fix requires an upgrade, and the
[CLI stability policy](./docs/cli-stability-policy.md) is what makes
that upgrade safe.

## What we consider a vulnerability

lookout is a **read-only** diagnostic tool that is nonetheless pointed
at production clusters with credentials, so the interesting boundaries
are about disclosure and about the trust placed in its output:

- **A secret value reaching an output surface.** This is the one that
  matters most. Every path to stdout, an MCP response, or an agent
  inject goes through the `pkg/emit` sanitizer, which redacts values
  and keeps key *names*. A way to get a Secret's contents, a token, a
  private key, or a connection string past it is a vulnerability even
  though the tool was authorized to read the object.
- **Privilege beyond the shipped RBAC.** The manifests in `deploy/`
  grant a minimum-necessary read-only ClusterRole. Anything that
  mutates cluster state, or that reaches resources the role does not
  name, is a bug of this class. All mutation belongs to the
  core-agent daemon's own permission gate; lookout observes.
- **Unauthenticated reach into the MCP server.** `lookout mcp` binds
  loopback and refuses a routable address unless you pass *both*
  `--auth-token-file` and `--allow-non-loopback`; the bearer compare
  is constant-time and the access log is mandatory in that mode. A way
  around any part of that is a vulnerability. Note the deliberate
  limit: the token is authentication, not authorization — every caller
  presenting a valid token gets the whole advertised tool surface, so
  narrow it with `--profile`. That is documented, not a finding.
- **Supply chain.** Images are keyless-signed with cosign and carry an
  SPDX SBOM attestation per platform under the same identity; release
  binaries have a signed `SHA256SUMS`. Anything that lets an artifact
  verify that we did not build is in scope. Verification recipes are
  on the [install page](https://go-steer.github.io/k8s-lookout/getting-started/install/).
- **A finding that lies.** Softer, but real: the product's value rests
  entirely on "silence means healthy". A way to make a check report a
  clean bill of health on a cluster that is not — by feeding it
  crafted object state, say — is worth reporting even though nothing
  is technically exploited.

## What we do not

- **Findings from a scanner, unaccompanied by an exploit path.** We
  run `govulncheck` on every CI run, which reports only advisories
  reachable from our code. A CVE in a dependency we do not call is not
  a lookout vulnerability, though we will happily take the bump.
- **Output containing cluster information you gave it access to.**
  lookout reveals what your kubeconfig can read. Redacted secret
  *names*, namespace names, image references, and node names in output
  are the tool working.
- **Denial of service against your own cluster's API server** from
  running lookout with pathological flags on a very large cluster. Use
  `--timeout` and the scoping flags.
- **Anything requiring an attacker who already has the credentials.**
  If they have the kubeconfig, lookout is the least of it.
