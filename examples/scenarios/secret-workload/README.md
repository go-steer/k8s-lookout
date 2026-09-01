# secret-workload — one canary string, consumed four ways

A **UAT fixture**, not a failure scenario: nothing here breaks, nothing
is expected on the wire, and the workload is deliberately *healthy*.

It exists for the one claim in this layer where a false PASS is a
security bug rather than a missing test: **no secret value may reach
any output surface** (DESIGN §6.5). The demo app mounts no Secret, so
without a fixture the redaction check would be asserting against a
cluster that has nothing to leak.

```sh
examples/scenarios/secret-workload/inject
examples/scenarios/secret-workload/verify
examples/scenarios/secret-workload/revert
```

It lands in **its own namespace**, `lookout-uat-secrets`, like every
UAT fixture, so revert is one `kubectl delete namespace`.

## The canary

Every secret value in the fixture is the same distinctive string:

```
c4nary-Sh0uld-Never-Appear-9f3a2b
```

Three character classes and no dictionary word, so it cannot collide
with anything a command legitimately prints. A single `grep` for it
across a command's whole output is then a *complete* answer to "did
anything leak", and — unlike a check that looks for the word
"REDACTED" — it cannot be satisfied by a command that happens to print
nothing at all.

## What it stages

`Deployment/keeper` consumes `Secret/vault-creds` four different ways,
because the redaction has to hold on all of them and they take
different code paths:

| Path | Field |
| --- | --- |
| the whole Secret as environment | `envFrom[].secretRef` |
| one key | `env[].valueFrom.secretKeyRef` |
| a mounted volume | `volumes[].secret` → `/etc/vault` |
| a pull credential | `imagePullSecrets[]` → `Secret/vault-pull` (`dockerconfigjson`, password = the canary) |

Plus `Service/keeper` in front of it, so the workload has edges to walk.

The pull secret names a registry that does not exist, but the image is
already on the node, so it is never used and the pod starts. That is
deliberate: the spec field has to be *populated* for the redaction path
to run, and a pod stuck on an image pull would change the findings
everywhere else.

## Second job: the healthy path

Because `keeper` is healthy and alone in its namespace, this fixture is
also how the suite asserts that the commands say so *explicitly* —
`health --namespace=lookout-uat-secrets` answers `status=healthy` per
category rather than staying silent, and `triage delta` there returns
`findings=0`, which is data, not an error. "Reports nothing when
nothing is wrong" is a claim in its own right, and it needs a namespace
that is genuinely quiet.

## What to expect

```sh
lookout triage spec --workload=Deployment/lookout-uat-secrets/keeper
lookout triage spec --workload=Secret/lookout-uat-secrets/vault-creds
lookout bundle --workload=Deployment/lookout-uat-secrets/keeper
lookout health --namespace=lookout-uat-secrets
```

- `triage spec` on the Deployment names every one of the four
  consumption paths — the Secret name, the key, the mount path, the
  pull secret — and prints no value from any of them.
- `triage spec` on the Secret itself reports its type and its **key
  names**, never a value. Naming a Secret is not reading it.
- `bundle` reaches the Secret as a `radius.neighbor` by name, from the
  mount, and stays value-free.
- Nothing in `triage delta`, `health` or `state edges` output contains
  the canary either. The cases sweep all of them, because redaction
  that holds only on the command you thought of is not redaction.

The full contract is asserted by `examples/uat-cases/40-toplevel.sh`;
this scenario's `verify` is only a smoke check that the objects landed.

## Explore by hand

```sh
lookout triage spec --workload=Deployment/lookout-uat-secrets/keeper |
  grep -c 'c4nary-Sh0uld-Never-Appear-9f3a2b'   # must be 0
kubectl -n lookout-uat-secrets get secret vault-creds -o yaml   # it really is in there
```

Agent-harness prompt to try:
> What secrets does the keeper deployment in lookout-uat-secrets use,
> and where does each one come from?
