---
name: k8s-triage
description: Investigate Kubernetes/GKE incidents (crashloops, image pulls, pending pods, config errors, broken services) with the lookout CLI — token-dense, secret-safe reads. Use when triaging a k8s-event inject, an alert, or any "why is X broken" question against a cluster.
---

# k8s-triage — incident investigation with lookout

`lookout` replaces kubectl describe/logs/get spelunking with compressed,
deterministic, secret-safe findings. Prefer it over raw kubectl for every
diagnostic read; fall back to kubectl only for surfaces lookout does not
cover yet (listed at the end).

## Decision tree

**Start with `bundle`.** It converts 4–5 separate reads into one correlated
payload — sanitized spec, everything abnormal, broken dependency edges,
blast radius, distilled logs — scoped to one workload:

```lookout
lookout bundle --workload=Deployment/prod/api
```

If the session began with a lookout-watch inject (a JSON message with
`"kind":"k8s-event"`), pass the payload straight in — the object reference
resolves to the owning workload via the graph's owner chain:

```lookout
lookout bundle --incident='{"kind":"k8s-event","reason":"BackOff","namespace":"prod","kind_of_object":"Pod","name":"api-6d5f8c-x2v9k"}'
```

Abridged real output (logfmt; one finding per line; `section=` says which
part of the bundle a line belongs to):

```lookout-golden
kind=bundle.target severity=info namespace=prod kind_of_object=Deployment name=api workload=Deployment/prod/api pods=2 sections=spec,delta,edges,radius,logs
kind=pod.imagepull severity=critical namespace=prod kind_of_object=Pod name=api-7c9d8-bbbbb reason=ImagePullBackOff message="Back-off pulling image \"registry.example.com/api:v3-typo\"" section=delta container=api image=registry.example.com/api:v3-typo
kind=edge.missing_key severity=critical namespace=prod kind_of_object=ConfigMap name=app-config reason=CreateContainerConfigError message="key log.level not found in configmap app-config (env LOG_LEVEL in container api)" section=edges workload=Deployment/prod/api container=api env=LOG_LEVEL key=log.level pods=2
kind=radius.neighbor severity=info namespace=prod kind_of_object=Service name=api section=radius relation=upstream hop=1
kind=log.template severity=warning namespace=prod kind_of_object=Pod name=api-7c9d8-aaaaa section=logs template="ERROR config key log.level missing, using default" count=1 level=error first_seen=2026-07-01T08:00:02Z last_seen=2026-07-01T08:00:02Z
…
scanned=12 findings=16 elapsed=100ms
```

Read it in this order: `bundle.target` (the head line — what was targeted,
which sections follow), then critical `delta`/`edges` findings (usually the
root cause is here), then `logs` templates for the application's own words,
then `radius` for what else is affected.

**Go direct when the question is already narrow** — each command is also an
MCP tool with the same payload:

| You are asking | Run | Notes |
| --- | --- | --- |
| anything wrong in this cluster / namespace? | `lookout triage delta` | whole cluster by default; `--namespace=X` to scope; `--only=pods,nodes,pdb,system,quota` to trim classes |
| what is this container actually logging? | `lookout triage logs --workload=Deployment/prod/api --since=30m` | Drain-clustered templates with counts, not raw lines; `--previous` reads what a crashed container said before it died |
| is its config/dependency wiring broken? | `lookout state edges --workload=Deployment/prod/api` | verifies ConfigMap/Secret keys, Service selectors + endpoints, Ingress backends, RBAC refs, TLS expiry — emits only broken edges |
| what exactly is this object's spec? | `lookout triage spec Deployment/prod/api` | kubectl describe, but token-dense and secret-safe; accepts kubectl aliases (`po`, `deploy`, `svc`, `cm`, …) and CRDs via discovery |

Concrete `state edges` output when a referenced ConfigMap key is missing
(the classic CreateContainerConfigError) and a Service has unready
endpoints:

```lookout-golden
kind=edge.missing_key severity=critical namespace=prod kind_of_object=ConfigMap name=app-config reason=CreateContainerConfigError message="key log.level not found in configmap app-config (env LOG_LEVEL in container api)" workload=Deployment/prod/api container=api env=LOG_LEVEL key=log.level pods=2
kind=edge.endpoints_unready severity=warning namespace=prod kind_of_object=Service name=api reason=EndpointMismatch message="1/3 endpoints ready across 1 slice(s); selector selects 2 pod(s), 1 ready" workload=Deployment/prod/api endpoints=3 ready=1 slices=1 selected=2
…
scanned=16 findings=8 elapsed=100ms
```

Typical narrowing sequence when `bundle` showed a crash but not the cause:

```lookout
lookout triage logs --workload=Deployment/prod/api --previous --since=1h
lookout triage spec cm/prod/app-config
lookout state edges --workload=Deployment/prod/api --format=json
```

## Reading any lookout output

- Every command emits findings (one per line) plus a **mandatory final
  summary line** `scanned=<n> findings=<n> elapsed=<d>`. `findings=0` with a
  summary present means *scanned and healthy* — healthy resources are never
  echoed. A stream without a summary line is void: treat it as a failed
  invocation, not as "no findings".
- Exit codes: `0` = data (including zero findings), `1` = runtime error
  (diagnostics on stderr only — stdout stays pure payload), `2` = usage
  error (bad flag/argument; fix the invocation, don't retry).
- `--format=logfmt` (default) and `--format=json` carry identical fields —
  one record per line either way. Use json when piping into `jq`; logfmt is
  denser to read inline.
- Findings are ordered critical-first. `severity` is one of
  critical/warning/info.
- Secret values never appear on any surface: Secret data renders as keys +
  byte sizes, credential-shaped env values render as `[REDACTED]`, and every
  emitted string passes the sanitizer. If you need a secret's *value*,
  lookout will not give it to you — by design.

## Not here yet (M3) — and what to do meanwhile

- `triage events` (deduped event timeline), `triage radius --at` (blast
  radius at incident onset), `triage changes` ("what changed before
  onset"), `net probe` (active DNS/TCP/HTTP checks) land in M3.
- Fallbacks: the `radius` section of `bundle` gives the *current* blast
  radius; for event history use
  `kubectl get events -n <ns> --sort-by=.lastTimestamp` (raw, undeduped —
  budget tokens accordingly); for "what changed", compare
  `lookout triage spec` output against the GitOps repo.

## Per-symptom playbooks and per-command references

- Playbooks (exact command sequences per symptom): `../playbooks/`
  — `crashloopbackoff.md`, `failedmount.md`.
- Per-command deep docs (all flags, output-field glossaries):
  `references/` — generated from the same metadata as `--help`, so they
  never drift from the binary.
