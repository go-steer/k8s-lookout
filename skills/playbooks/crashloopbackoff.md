# Playbook: CrashLoopBackOff

Trigger: a lookout-watch inject with `"kind":"k8s-event"` and
`"reason":"BackOff"` (message like `Back-off restarting failed container
<c> in pod <pod>_<ns>(<uid>)`), a `pod.crashloop` finding from
`triage delta`/`health`, or an operator reporting a restart loop.
The inject payload's `namespace`/`kind_of_object`/`name` name the pod;
`context.controller_ref` names the owning workload.

## Steps

1. **One correlated read.** Feed the inject payload straight in (or name
   the workload if you already know it):

   ```lookout
   lookout bundle --incident='{"kind":"k8s-event","reason":"BackOff","namespace":"prod","kind_of_object":"Pod","name":"api-6d5f8c-x2v9k"}'
   lookout bundle --workload=Deployment/prod/api
   ```

   In the `delta` section, find the `pod.crashloop` finding and read
   `exit_code` and `last_state` — they choose the branch below.

2. **Read what the container said before it died.** The current container
   has restarted; the evidence is in the *previous* instance:

   ```lookout
   lookout triage logs --workload=Deployment/prod/api --previous --since=1h
   ```

   `log.template` findings with `level=fatal|error` and `log.stacktrace`
   findings (top frames in `frames`) usually state the cause outright.

3. **Branch on the exit evidence.**

   | Evidence | Likely cause | Verify with |
   | --- | --- | --- |
   | `last_state=OOMKilled` (exit 137) | memory limit too low or a leak | `lookout triage spec Deployment/prod/api` — compare `limits` vs the app's needs |
   | exit code 1/2, logs name a missing config key or file | broken ConfigMap/Secret wiring | `lookout state edges --workload=Deployment/prod/api` — `edge.missing_key` / `edge.missing_ref` name the exact key, env var, and container |
   | exit code 0 but still restarting | process exits cleanly; probe or command wrong | `lookout triage spec Deployment/prod/api` — check `liveness`/`readiness` one-liners and the container command |
   | logs show connect/DNS failures to a dependency | dependency down, not this workload | `lookout state edges --workload=Deployment/prod/api` for selector/endpoint health of the dependency's Service; then re-run this playbook against the dependency |
   | started crashing right after a rollout | bad new revision | `triage changes` lands M3 — meanwhile compare `lookout triage spec Deployment/prod/api` against the GitOps repo's previous revision |

4. **Confirm the blast radius before acting.** The bundle's `radius`
   section (`relation=upstream` neighbors — Services/Ingresses routing
   here) tells you who is affected; check `edge.selector_unready` /
   `edge.endpoints_unready` in the `edges` section to see whether traffic
   is already failing over.

5. **Close the loop.** Fixes route through GitOps, never raw writes. After
   the fix rolls out, re-run:

   ```lookout
   lookout triage delta --namespace=prod --only=pods
   ```

   `findings=0` on the summary line (for this workload) is the verified
   all-clear; the sentinel's followup inject (`k8s-event-followup`)
   confirms the loop stopped from the watch side.
