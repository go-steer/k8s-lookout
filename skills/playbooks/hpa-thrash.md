# Playbook: HPA thrash (rescale oscillation)

Trigger: an `event.hpa_thrash` finding from `lookout triage events`, a
watch inject whose reason is `SuccessfulRescale` arriving over and over
for the same HPA, or an operator reporting replica counts that bounce
instead of converging. Thrash burns scheduler and node-pool churn, keeps
caches cold, and often masquerades as intermittent latency.

## Steps

1. **Characterize the oscillation.** The HPA object keeps no replica
   history — the sequence is recovered from `SuccessfulRescale` events,
   so the timeline command is the analysis:

   ```lookout
   lookout triage events --workload=Deployment/prod/api --since=2h
   ```

   Abridged real output:

```lookout-golden
kind=event.hpa_thrash severity=warning namespace=prod kind_of_object=HorizontalPodAutoscaler name=web-hpa reason=HPAThrash message="replica count changed direction 2 times within 30m0s — the HPA is oscillating, not converging" replicas=6->3->7->3 flips=2 window=30m0s target=Deployment/web
…
scanned=9 findings=4 elapsed=100ms
```

   Read `replicas` (the chronological sequence), `flips`
   (scale-direction changes inside `window`), and `target` (the HPA's
   scaleTargetRef). A monotonic ramp never fires — if you see
   `event.hpa_thrash`, the HPA really is chasing itself. Tune
   sensitivity when the default (2 flips in 30m) is too loose or too
   tight for the workload:

   ```lookout
   lookout triage events --workload=Deployment/prod/api --hpa-window=15m --hpa-flips=4
   ```

2. **Ask what changed.** Thrash that starts abruptly usually follows an
   HPA edit, a rollout that changed resource requests, or a traffic
   shift:

   ```lookout
   lookout triage changes Deployment/prod/api --since=1h
   ```

   A `change.rollout` whose new revision changed `requests`, or a config
   change on the HPA itself, right before the first flip is your prime
   suspect.

3. **Read the two specs that must agree.**

   ```lookout
   lookout triage spec hpa/prod/web-hpa
   lookout triage spec Deployment/prod/api
   ```

   CPU-utilization targets are percentages *of requests*: `requests` set
   far below (or above) real usage make measured utilization swing
   wildly around the target and the HPA chase it. Also check
   `minReplicas`/`maxReplicas` — a sequence bouncing off a bound (e.g.
   `replicas=6->3->7->3` with `maxReplicas=7`) means the band is too
   narrow, not that the metric is noisy.

4. **Gauge who feels the flapping.** Every scale-down passes through a
   window of reduced capacity:

   ```lookout
   lookout triage radius Deployment/prod/api
   lookout state edges --workload=Deployment/prod/api
   ```

   `direction=upstream` neighbors (Services/Ingresses) are where the
   intermittent latency lands; `edge.endpoints_unready` during a
   trough confirms traffic is hitting the shrunken replica set.

5. **Remediate via GitOps — the fix lives in the HPA settings**, never
   in raw writes:

   - `behavior.scaleDown.stabilizationWindowSeconds` (default 300) —
     raise it so scale-down stops following every dip; this is the
     first knob for classic up-down-up thrash.
   - Fix `resources.requests` to reflect real usage if utilization
     percentages are computed off wrong requests (step 3).
   - Raise the target utilization (or switch to a less spiky metric)
     if requests are accurate but the load is bursty.
   - Widen `minReplicas`/`maxReplicas` if the sequence bounces off a
     bound.
   - If two controllers fight — the HPA scaling live while GitOps
     re-asserts a fixed `spec.replicas` — remove `spec.replicas` from
     the manifest; that conflict thrashes forever.

6. **Write your triage status.** Thrash fixes settle over a full
   stabilization window, and other scans (and the sentinel's followup
   routing) must not treat the still-oscillating HPA as an untriaged
   incident meanwhile (§9.4). Record the diagnosis against the HPA's
   scale target, with the `fingerprint` from the triggering inject:

   ```lookout
   lookout triage status --store=/var/lib/lookout/lookout.db --fingerprint=sha256:5641487571b8 --resource=Deployment/prod/api --session=sess-0012 --status=actioned --severity-override=info --root-cause="CPU target computed off stale requests; utilization swings around 60% target" --action="stabilizationWindowSeconds 300->600 + requests fixed in GitOps PR; settling"
   ```

   A `triage.regressed` followup arriving in your session afterwards
   means the flip rate accelerated past the downgrade-time rate —
   re-read the sequence before assuming the fix took.

7. **Verify.** After the fix has had one full window to settle:

   ```lookout
   lookout triage events --workload=Deployment/prod/api --since=1h
   ```

   No new `event.hpa_thrash` and a converging (monotonic, then flat)
   replica sequence in the remaining `SuccessfulRescale` entries is the
   all-clear. The sentinel's `resolved` inject then flips your
   triage-status record automatically.
