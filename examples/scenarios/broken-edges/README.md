# broken-edges — five different broken references on one workload

A **UAT fixture**, not a failure scenario. `lookout state edges` walks
the references a workload depends on — ConfigMap keys, Secrets,
Service selectors, Endpoints, ingress classes, TLS certificates — and
reports the ones that are broken. The claim under test is not "it
notices something is wrong": it is that each broken reference is named
*specifically*. A fixture with one break cannot tell "reports the
missing key" apart from "reports that something is wrong", so this one
breaks five things at once, in five different ways.

```sh
examples/scenarios/broken-edges/inject
examples/scenarios/broken-edges/verify
examples/scenarios/broken-edges/revert
```

Everything lands in **its own namespace**, `lookout-uat-edges`, like
every UAT fixture: nothing a fixture creates may sit in `lookout-demo`,
where the demo app and the failure scenarios live. This one especially
— `edgy` never becomes Available by design, and a permanently
unavailable Deployment in the demo namespace would wedge anything
waiting on it. Revert is one `kubectl delete namespace`, and this
fixture and the failure scenarios can be injected in either order.

## What it breaks

| Object | Break | Expected finding |
| --- | --- | --- |
| `Deployment/edgy` env | `configMapKeyRef` to `absent-key` in a ConfigMap that *does* exist | `edge.missing_key` naming the key |
| `Deployment/edgy` envFrom | `secretRef` to a Secret that does not exist | `edge.missing_ref` (Secret) |
| `Service/edgy-ghost` | a selector no pod carries | `edge.selector_empty` |
| `Ingress/edgy-ing` | `ingressClassName: nginx` with no such IngressClass | `edge.missing_ref` (IngressClass) |
| `Secret/edgy-tls` | a valid certificate expiring in 10 days | `edge.cert_expiring days_left=9` |

Plus one object that is deliberately **fine**: `Deployment/edgy-ok`
behind `Service/edgy-web`, ready and in the Endpoints. The
report-only-broken claim needs an edge that is healthy and must
therefore *not* be listed. It cannot be `edgy` itself — that pod is
wedged in `CreateContainerConfigError` forever by design, so a Service
aimed at it would emit `selector_unready` and `endpoints_unready` and
the negative assertion would have nothing to stand on.

## What to expect

```sh
lookout state edges --workload=Deployment/lookout-uat-edges/edgy
```

- `edge.missing_key` — names `absent-key` *and* the ConfigMap that has
  the other keys, which is the difference between a useful message and
  "config error".
- `edge.missing_ref` — the Secret, with the container and the fact that
  it came in through `envFrom`.
- `edge.selector_empty` — reported from either end. Asked about the
  Deployment it says the Service selects nothing; asked about
  `Service/edgy-ghost` it adds `likely_workload=` — the workload it
  guesses you meant, found by comparing label *values* in the namespace.
- `edge.cert_expiring days_left=9` on the TLS Secret, reached *through*
  the Ingress (`via=ingress`) rather than by looking at Secrets. Ask
  about `edgy-ok`, not `edgy`: the certificate is an edge of whatever
  the routing chain reaches, and `edgy-ing` routes to `edgy-web`, which
  selects `edgy-ok`.
- `--cert-warn` sets the window. The certificate is 10 days out, so the
  default (720h ≈ 30d) and `--cert-warn=336h` both report it and
  `--cert-warn=1h` does not — which is how you tell "the flag is
  honoured" from "the finding happens to be on".

The pod never becomes Ready, and that is intentional: `state edges`
answers from the spec. It does not need the failure to have happened
yet, which is what makes it usable before a rollout rather than after.

The full contract is asserted by `examples/uat-cases/20-fixtures.sh`;
this scenario's `verify` is only a smoke check that the objects landed.

## Explore by hand

```sh
lookout state edges --workload=Service/lookout-uat-edges/edgy-ghost
lookout state edges --workload=Service/lookout-uat-edges/edgy-web   # healthy selector
lookout state edges --workload=Deployment/lookout-uat-edges/edgy-ok --cert-warn=336h
kubectl -n lookout-uat-edges get pod -l app.kubernetes.io/name=edgy   # CreateContainerConfigError
```

Agent-harness prompt to try:
> The `edgy` deployment in lookout-uat-edges won't start. What is it
> missing?
