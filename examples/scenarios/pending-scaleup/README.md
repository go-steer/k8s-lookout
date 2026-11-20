# pending-scaleup

Exercises `#835`'s `FailedScheduling` autoscaler-verdict gate end-to-end on kind
without a real cluster autoscaler, using two sequential unschedulable pods
(avoiding the `events.k8s.io/v1` series-only post-occurrence-2 write race):

1. **Pod A (`hog-scaleup-a`)**: receives a `TriggeredScaleUp` Event from
   `source.component: cluster-autoscaler`. Verifies
   `lookout_events_filtered_total{gate="scaleup_hold"}` increments and no
   `kind=k8s-event` `FailedScheduling` opens on the wire.
2. **Pod B (`hog-scaleup-b`)**: receives a wrong-reporter `TriggeredScaleUp`
   (`source.component: fake-controller`, verified rejected via
   `lookout_watch_scaleup_verdicts_rejected_total`) followed by a
   `cluster-autoscaler` `NotTriggerScaleUp` before kube-scheduler's occurrence
   #2 (~5 min on kind). Verifies `kind=k8s-event` `FailedScheduling`
   short-circuits the count/span backstop and opens with `count >= 1`
   (inheriting `#833`'s live `Event.Series` verification).
