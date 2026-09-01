# chatty-logs — a fixed log corpus to distill

A **UAT fixture**, not a failure scenario: nothing here breaks and
nothing is expected on the wire. It exists because `lookout triage logs`
cannot be tested against a quiet pod — Drain clustering, probe
stripping, stack-trace collapse and `--max-templates` overflow all need
a pod that has actually said something.

```sh
examples/scenarios/chatty-logs/inject
examples/scenarios/chatty-logs/verify
examples/scenarios/chatty-logs/revert
```

It lands in **its own namespace**, `lookout-uat-logs`, like every UAT
fixture: nothing a fixture creates may sit in `lookout-demo`, where the
demo app and the failure scenarios live, so revert is one
`kubectl delete namespace`.

## What it emits

A single `chatty` pod writes its whole corpus in one burst and then
sleeps for a day. **That is the point:** `triage logs` reads a window of
the tail, so a pod that keeps printing answers differently every second
and no assertion about `count=` can hold. A fixed corpus makes the
counts real numbers.

| Population | Lines | What it exercises |
| --- | --- | --- |
| `req id=… path=/api/v1/widgets status=200 ms=…` | 600 | one template, three variable positions → `<*>` |
| `GET /healthz … ua=kube-probe/1.34` | 60 | probe stripping (`log.probe_noise`, `--keep-probes`) |
| `ERROR db connection refused host=… port=5432` | 20 | a second template, `level=error` ordering |
| a Python traceback | 1 | `log.stacktrace` with `lang=python` and `frames=` |

## What to expect

```sh
lookout triage logs --pod=chatty --namespace=lookout-uat-logs
```

- `kind=log.template` for the request lines with `count=600` and `<*>`
  in place of the id, host and latency.
- `kind=log.probe_noise count=60` — stripped, and *reported* as
  stripped. `--keep-probes` makes the probe lines their own template and
  the `log.probe_noise` finding disappears.
- `kind=log.stacktrace lang=python frames=…` — the traceback collapsed
  to its top frames rather than reproduced whole.
- `--max-templates=1` → one template plus `kind=log.overflow` naming the
  clusters it dropped.

The full contract is asserted by `examples/uat-cases/20-fixtures.sh`;
this scenario's `verify` is only a smoke check that the corpus landed.

## Explore by hand

```sh
lookout triage logs --pod=chatty --namespace=lookout-uat-logs --keep-probes
lookout triage logs --pod=chatty --namespace=lookout-uat-logs --max-templates=1
kubectl -n lookout-uat-logs logs chatty | wc -l     # what you did not have to read
```
