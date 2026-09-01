# broken-webhook — three admission webhooks, five ways to be wrong

A **UAT fixture**, not a failure scenario. `lookout state webhooks`
answers the question you ask at 3am when writes are being rejected and
nobody knows why: *which admission webhook is doing this, and what does
it gate?* This fixture creates every shape the check reports.

```sh
examples/scenarios/broken-webhook/inject
examples/scenarios/broken-webhook/verify
examples/scenarios/broken-webhook/revert
```

## Safety

An admission webhook is **cluster-scoped**, and one with
`failurePolicy: Fail` and a dead backend rejects every matching write
in the cluster. So each webhook here is fenced twice: its
`namespaceSelector` matches one sacrificial namespace,
`lookout-uat-webhook`, that nothing else in the examples touches, and
its rules match `configmaps` `CREATE` only. The worst thing that can
happen if `revert` never runs is that you cannot create a ConfigMap in
that one namespace.

The namespace is created *before* the webhooks, so the
`kube-root-ca.crt` ConfigMap the API server injects into every new
namespace is already there — otherwise creating the namespace would
itself be gated by the fail-closed webhook and hang.

`revert` deletes the webhook configurations first and the namespace
last, for the same reason.

## What it creates

| Configuration | Backend | Policy | Expected findings |
| --- | --- | --- | --- |
| `lookout-uat-failclosed` (Validating) | Service that does not exist | `Fail` | `webhook.failing_closed` |
| `lookout-uat-ignore` (Mutating) | Service that does not exist | `Ignore` | `webhook.dead_backend` + `webhook.ca_expired` |
| `lookout-uat-slow` (Validating) | live, `timeoutSeconds: 25` | `Fail` | `webhook.slow_risk` + `webhook.ca_expiring` |

The two dead backends differ only in `failurePolicy`, and that single
field is the difference between a critical and a warning — between
"every gated write is rejected" and "the policy you think is enforced
is silently not running". That pair is the point of the fixture.

`slow_risk` needs a backend that is genuinely alive: a dead backend is
reported as `failing_closed` and never also as slow. So `wh-backend`
is a real Deployment with a real ready endpoint on port 443 (the
`ServiceReference` default, and what the webhook asks for — a service
exposing some other port would be reported dead, "port 443 not on
service").

## Certificates

`caBundle` holds only a public certificate, so both are throwaways with
no key in the cluster.

The **expired** one is a checked-in literal. `openssl req -x509` cannot
produce a past `notAfter` before OpenSSL 3.2 (`-days` rejects a
negative and `-not_after` does not exist yet), and a `NotAfter` of
2021-01-01 is a property that stays true rather than a fixture that
rots. The **expiring** one has to be generated at inject time: "10 days
from now" is only inside the warn window if `now` is now.

## What to expect

```sh
lookout state webhooks --cert-warn=336h
```

- `webhook.failing_closed` — critical, and it carries the blast radius:
  `gates="1/17 namespaces: lookout-uat-webhook"` and
  `rules="CREATE configmaps"`. "A webhook is broken" is not actionable;
  "this webhook is rejecting configmap creates in one namespace" is.
- `webhook.dead_backend` — warning, same dead service, different
  sentence, because the consequence is different.
- `webhook.ca_expired days_left=-2070` — negative days, not an error.
- `webhook.ca_expiring days_left=9` — inside the 720h default; use
  `--cert-warn=1h` to watch it drop out.
- `webhook.slow_risk timeout=25s` — info. Nothing is wrong yet, which
  is why it is info and not a warning.

`state webhooks` takes no `-A` and no `--namespace`: the objects it
reads are cluster-scoped, so it rejects both rather than pretending to
filter.

The full contract is asserted by `examples/uat-cases/20-fixtures.sh`;
this scenario's `verify` is only a smoke check that the webhooks landed.

## Explore by hand

```sh
lookout state webhooks --cert-warn=1h        # the expiring one drops out
lookout state webhooks --format=json | jq '.findings[].kind'
kubectl -n lookout-uat-webhook create configmap nope --from-literal=a=b   # rejected
```

Agent-harness prompt to try:
> I can't create ConfigMaps in the lookout-uat-webhook namespace and
> the error mentions a webhook. What's going on and how bad is it?
