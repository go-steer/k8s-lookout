# endpoints-empty — a Service whose selector matches nothing

Patches the `web` Service's selector to a label no pod carries: its
EndpointSlices empty out while the pods keep running healthy. Nothing
crashes, nothing restarts — the outage exists only at the wiring
altitude, which is exactly what the object-state source and
`state edges` are for.

```sh
examples/scenarios/endpoints-empty/inject
examples/scenarios/endpoints-empty/verify
examples/scenarios/endpoints-empty/revert
```

## What to expect

- **Sentinel (wire)** — `kind=objectstate.endpoints_empty` on the
  transition to zero serving endpoints.
- **Read-path** — `lookout state edges
  --workload=Deployment/lookout-demo/web` reports the Service
  selector selecting 0 pods (`selected=0` / empty endpoints) — the
  structural "who unplugged the cable" answer.

## Explore by hand

```sh
lookout state edges --workload=Deployment/lookout-demo/web
lookout triage delta --namespace=lookout-demo    # nothing crashing — that's the point
kubectl -n lookout-demo exec vantage -- python -c \
  "import urllib.request; urllib.request.urlopen('http://web.lookout-demo.svc', timeout=3)"
# → connection refused/timeout: the user-visible symptom the signals explain
```

Agent-harness prompt to try:
> web.lookout-demo.svc stopped answering but every pod looks healthy.
> Find out why.
