# cert-expiry — a TLS certificate about to run out

Creates a TLS Secret in lookout-demo whose certificate expires in 48
hours. Nothing is failing yet — this is the purest leading indicator
in the set: deterministic arithmetic on `notAfter`, days before the
outage.

Requires `openssl` on the machine running inject.

```sh
examples/scenarios/cert-expiry/inject
examples/scenarios/cert-expiry/verify
examples/scenarios/cert-expiry/revert
```

## What to expect

- **Sentinel (wire)** — `kind=expiry.warning` at the next expiry scan
  (examples value `--expiry-interval=2m`, scoped to lookout-demo via
  `--expiry-namespaces`) at severity **critical**: 48h is inside the
  design-fixed 72h critical threshold.
- **Read-path** — `lookout health` scores the certs category
  degraded, with `days_left` on the finding.

## Explore by hand

```sh
lookout health --cert-warn=336h
lookout state webhooks     # same expiry arithmetic on webhook CA bundles
```

Agent-harness prompt to try:
> Audit this cluster for anything that will break in the next two
> weeks without anyone touching it.
