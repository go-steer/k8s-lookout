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

2. **Was it healthy recently? Ask what changed first.** A workload that
   was fine 30 minutes ago rarely breaks by itself — check for a rollout
   or config change before reading any logs:

   ```lookout
   lookout triage changes Deployment/prod/api --since=30m
   ```

   A `change.rollout` (new `image`/`revision`) or `change.config`/
   `change.secret` just before the first restart is usually the cause —
   jump straight to verifying it in the branch table below. Mind the
   summary's `source=` note: `live-approximation` (no sentinel store)
   cannot see ConfigMap/Secret edits or label flips; `source=history`
   (with `--store`) sees everything.

3. **Read what the container said before it died.** The current container
   has restarted; the evidence is in the *previous* instance:

   ```lookout
   lookout triage logs --workload=Deployment/prod/api --previous --since=1h
   ```

   `log.template` findings with `level=fatal|error` and `log.stacktrace`
   findings (top frames in `frames`) usually state the cause outright.

4. **No logs, or need the restart cadence? Pull the event timeline.**
   When the container dies before logging (probe kills, OOM, image
   pulls) or you need to know when the looping started and whether it is
   still recurring:

   ```lookout
   lookout triage events --workload=Deployment/prod/api --since=1h
   ```

   One deduped entry per (object, reason family) over the whole owner
   tree — `count`/`first_seen`/`last_seen` date the loop; `Unhealthy` →
   `Killing` sequences expose probe kills the logs never show.

5. **Branch on the exit evidence.**

   | Evidence | Likely cause | Verify with |
   | --- | --- | --- |
   | `last_state=OOMKilled` (exit 137) | memory limit too low or a leak | `lookout triage spec Deployment/prod/api` — compare `limits` vs the app's needs |
   | exit code 1/2, logs name a missing config key or file | broken ConfigMap/Secret wiring | `lookout state edges --workload=Deployment/prod/api` — `edge.missing_key` / `edge.missing_ref` name the exact key, env var, and container |
   | exit code 0 but still restarting | process exits cleanly; probe or command wrong | `lookout triage spec Deployment/prod/api` — check `liveness`/`readiness` one-liners and the container command |
   | logs show connect/DNS failures to a dependency | dependency down, not this workload | `lookout state edges --workload=Deployment/prod/api` for selector/endpoint health of the dependency's Service; then confirm the hypothesis actively — `lookout net probe --dns=db.prod.svc.cluster.local --tcp=db.prod.svc:5432` from the cluster vantage — and re-run this playbook against the dependency |
   | started crashing right after a rollout | bad new revision | `lookout triage changes Deployment/prod/api --since=1h` — the `change.rollout` finding names the `revision` and `image`; diff that revision in the GitOps repo |

6. **Confirm the blast radius before acting.**

   ```lookout
   lookout triage radius Deployment/prod/api
   ```

   `direction=upstream` neighbors (Services/Ingresses routing here) are
   the user-facing impact; `direction=lateral` co-tenants share the node
   or config. Cross-check `edge.selector_unready` /
   `edge.endpoints_unready` in the bundle's `edges` section to see
   whether traffic is already failing over. For a post-mortem, add
   `--at=<onset> --store=<sentinel db>` to get the radius as it was at
   onset, not as it is now.

7. **Close the loop.** Fixes route through GitOps, never raw writes. After
   the fix rolls out, re-run:

   ```lookout
   lookout triage delta --namespace=prod --only=pods
   ```

   `findings=0` on the summary line (for this workload) is the verified
   all-clear; the sentinel's followup inject (`k8s-event-followup`)
   confirms the loop stopped from the watch side.
