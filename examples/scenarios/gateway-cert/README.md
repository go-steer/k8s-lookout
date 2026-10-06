# gateway-cert — an ingress frozen on a certificate that never issues

A working Gateway gains an HTTPS listener whose TLS Secret is supposed
to come from a cert-manager Certificate. The Issuer behind it can never
sign anything, so the Secret is never written and the listener is never
programmed. The plain-HTTP listener next to it keeps serving, which is
what makes this failure easy to miss: the Gateway is up, and only the
HTTPS side is dead.

This is the only scenario that exercises the `gateway` source and the
expiry source's cert-manager handling. It is the cluster half of
core-agent's "frozen ingress" demo (go-steer/core-agent#1216).

**Heavy, so not in the default set.** inject installs cert-manager and
Envoy Gateway, which also brings the Gateway API CRDs. Both are pinned
in `stack.sh` and fetched from their GitHub releases, so the machine
needs network access to github.com and the image registries. revert
uninstalls both. Run it explicitly (the weekly CI run does):

```sh
examples/e2e gateway-cert
```

## Timeline

1. inject installs cert-manager and Envoy Gateway, then re-runs
   `examples/sentinel/up` with the image the sentinel is already
   running and restarts it. Both sources read the cluster's API
   surface only at startup: sentinel/up names `gateway` in `--sources`
   only when the Gateway API CRDs are served, and the expiry source
   only scans Certificates if it found their CRD when it started.
2. A Gateway with one HTTP listener is applied, and inject waits until
   its Envoy data plane is programmed.
3. The fault (`fault.yaml`): a CA `Issuer` whose CA Secret does not
   exist, a `Certificate` for `frozen.lookout-examples.invalid` from
   that Issuer, and an HTTPS listener on the same Gateway that
   terminates TLS with the Certificate's Secret.

Why a CA Issuer with no CA rather than ACME for a domain nobody
controls: it fails identically every time with no network at all. There
is no ACME account to register, no DNS lookup, and nothing a CI runner's
egress can change. The Certificate reads `Ready=False`
(`DoesNotExist`) as soon as cert-manager sees it. A side effect is that
no ACME `Order` or `Challenge` is ever created.

Why Envoy Gateway: one controller plus one Envoy Deployment per
Gateway, it runs on kind with only a Service type override (ClusterIP,
since kind has no load balancer), and it reports a missing
certificateRef on that listener alone: `ResolvedRefs=False`
(`InvalidCertificateRef`) and `Programmed=False` (`Invalid`).

## What to expect

- **Sentinel (wire):**
  - `expiry.warning` for Certificate `frozen-ingress-tls` at the next
    expiry scan (`--expiry-interval=2m`), with `renewal=FAILED`. It is
    **critical**, so it opens its own session: to the expiry source a
    Certificate reading `Ready=False` is a failed renewal, and a failed
    renewal is critical whatever the countdown says. A Certificate that
    never issued has no `notAfter`, so the source reports epoch as the
    expiry date ("certificate EXPIRED … ago (notAfter
    1970-01-01T00:00:00Z)"). That wording is tracked as #552; verify
    does not assert on it.
  - `gateway.programming_failed` and `gateway.route_rejected` for
    Gateway `frozen-ingress`, after the 60s `--gateway-grace` that
    `examples/sentinel/up` sets (default 5m). Both are **warning**, not
    critical, so they arrive in a `watchboard.digest` (or as a
    `family.member` followup if correlation finds a live ancestor first).
    The missing Secret breaks two conditions on one listener:
    `Programmed=False` maps to `programming_failed` and
    `ResolvedRefs=False` maps to `route_rejected`.
- **Read-path:** `lookout state gateway` reports
  `gateway.listener_invalid` for `listener=https`. It names the
  listener and the missing Secret, which the digest entry does not.
- **Negative control:** the Gateway itself and its `http` listener stay
  `Programmed=True`. verify fails if either does not, or if
  `state gateway` blames anything but the `https` listener.

Nothing here waits for the incidents to resolve. revert deletes the
objects and then the CRDs, and restarts the sentinel without the
gateway source.

## Explore by hand

```sh
kubectl -n lookout-demo get certificate,certificaterequest,issuer
kubectl -n lookout-demo get gateway frozen-ingress -o yaml
lookout state gateway --namespace lookout-demo
```

Agent-harness prompt to try:
> HTTPS to frozen.lookout-examples.invalid stopped working but plain
> HTTP through the same Gateway is fine. Find out why and what would
> fix it.
