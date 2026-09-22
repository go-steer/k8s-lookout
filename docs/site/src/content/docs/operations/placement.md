---
title: Tuning placement findings
description: What the three confidence tiers mean, which flags move each one, how to keep the per-domain series under control, and the SLIs that say whether the numbers are worth reading at all.
sidebar:
  order: 9
---

Two of the sentinel's sources answer questions nothing else in a cluster
does. `topology-drift` asks whether a workload's objects are spread the
way somebody meant them to be. `compute-class` asks whether the nodes a
workload actually landed on are the ones its compute class preferred.
Both emit under the `leeway.` prefix, both are on by default, and both
are quiet on a cluster where nothing has moved.

This page is about turning them up and down. For what each flag does
line by line see the [`watch` reference](/reference/watch/); for what
each metric is see the [metrics reference](/reference/metrics/); for
running the subsystem as its own process see [The standalone `leeway`
binary](/operations/leeway-standalone/). There is a [prebuilt Grafana
dashboard](#the-dashboard) over everything on this page.

## The three tiers

Every placement finding carries a tier, and the tier is not a severity
dressed up — it is a statement about **where the expectation came from**.

| Tier | Where the expectation came from | Severity |
| --- | --- | --- |
| **A** | You declared it, in a `DoNotSchedule` topology spread constraint or a required pod anti-affinity, and Kubernetes is not honouring it. | `critical` |
| **B** | You signalled it — a `ScheduleAnyway` constraint, a preferred anti-affinity — or the spread is far enough off an even split to stand out on its own. | `warning` |
| **C** | Nobody declared anything. The workload has moved away from **its own** history. | `info`, or `warning` on a max-domain-share breach |

Tier A is the interesting one. The scheduler enforces a `DoNotSchedule`
constraint at admission, so a violation means the pods were placed
correctly and *then* the world changed underneath them — a node was
relabelled, a zone's nodes went away, or the pods predate the
constraint. No admission-time control can catch those, which is most of
the reason this subsystem exists.

**An expectation the sentinel assumed can never reach Tier A.** If you
give it your cluster's scheduler defaults with
`--topology-cluster-defaults`, findings derived from them are capped at
Tier B even when the default says `DoNotSchedule`. Raising a `critical`
on a contract nobody wrote down is the failure mode that makes an
operator stop trusting the whole source.

### Tier C is off the wire by default

Tier C findings are exported as metrics but do not become signals unless
you ask:

```
--topology-tier-c-signals        # leeway.topology_drift at Tier C
--compute-class-tier-c-signals   # leeway.rank_tier_unused
```

They are off because Tier C has no declared intent behind it. A
workload that has always run three pods in one zone and now runs them in
two has *changed*, and that is worth a graph; it is not obviously worth
waking anybody. Turn them on once the metrics have convinced you the
baselines are sane on your cluster — which usually takes a few days,
because a baseline needs history before it means anything.

## Which flag moves which tier

Nothing here changes what is measured. Every flag below changes what is
*reported*, and the metrics stay complete either way — which is why the
honest order of operations is to watch the series first and tighten the
signals afterwards.

**Tier A and B — `topology-drift`**

| Flag | Default | What moving it does |
| --- | --- | --- |
| `--topology-dwell` | `10m` | How long a breach must persist before it is a finding. Placement is rebuilt constantly by rollouts and evictions; the dwell is what keeps a rollout from being an incident. Raise it on a cluster that deploys continuously. |
| `--topology-keys` | zone, region | The axes scored at all. Adding an axis multiplies subjects; a cluster that partitions on something else entirely (a rack label, a cell) names it here. |
| `--topology-capacity-ratio` | `1.25` | How unequal a subject's eligible zones must be, by allocatable CPU, before an even split stops being the expectation. Lower it on a fleet of deliberately heterogeneous pools. |
| `--topology-cluster-defaults` | — | Your kube-scheduler `defaultConstraints`. Without them, a workload that declares nothing is scored at Tier C rather than against the rule the scheduler is actually applying. |

**Tier C — baselines**

| Flag | Default | What moving it does |
| --- | --- | --- |
| `--topology-learn-baselines` | `true` | Off means a workload with no declared constraint is never scored against its own history. The Tier C tier disappears; A and B are unaffected. |
| `--topology-baseline-band` | `4` | How many learned deviations wide the tolerance band is. **The false-positive knob.** Widen it first if Tier C is noisy; narrowing it below 3 will find ordinary rescheduling. |
| `--topology-baseline-half-life` | `12h` | How fast a baseline absorbs a step change. Shorter follows a cluster that is legitimately rebalancing; longer keeps a memory of what normal was before somebody started moving things. |

**`compute-class`**

| Flag | Default | What moving it does |
| --- | --- | --- |
| `--compute-class-dwell` | `10m` | As `--topology-dwell`, for rank verdicts. |
| `--compute-class-window` | `1h` | How much history a rank share is a share of. The counters are cumulative, so the window is what stops a bad hour six weeks ago from being permanent. |
| `--compute-class-last-rank-ceiling` | `0.9` | Fires when more than this share of a class's pod-time ran at its **least**-preferred priority. |
| `--compute-class-rank0-floor` | off | Fires when less than this share ran at its **most**-preferred priority. Off by default because a class whose top priority is a scarce machine type is *expected* not to reach it. |

## Cardinality

The per-domain breakdown — `lookout_leeway_domain_objects` and
`lookout_leeway_domain_expected` — is one series per subject, per axis,
per domain, per state. That is the one part of this subsystem that can
hurt a Prometheus, so it is off for most subjects by default and the
controls are deliberately several:

| Flag | Default | Effect |
| --- | --- | --- |
| `--topology-per-domain-min-drift` | `0.05` | Subjects drifting at least this much get a breakdown. This is the default admission rule: a subject that is placed correctly does not need per-domain series to prove it. |
| `--topology-per-domain-series` | off | Every tracked subject gets one, ignoring the drift floor. Know your subject count before setting this. |
| `--topology-per-domain-namespaces` | all | Narrow the breakdown to named namespaces. |
| `--topology-per-domain-exclude-namespaces` | — | Applied **ahead** of the include list: a namespace in both is excluded. |
| `--topology-per-domain-max-keys` | `4` | How many axes one subject may break down on. |
| `--topology-per-domain-collapse-states` | `true` | Folds four scheduling states onto two labels, halving the count. |
| `--topology-max-node-groups` | `200` | Past the bound, **no** node group is scored — not an arbitrary subset, because a partial answer to "is this pool concentrated" is worse than none. |

Whenever one of these withholds something,
`lookout_leeway_domain_series_withheld` says so, by reason. Read it
before concluding a subject has no objects in a domain: an absent series
and a zero are not the same statement, and this gauge is how you tell
them apart.

## Is the measurement sound?

A dashboard that shows drift without showing whether drift is being
measured correctly is exactly the failure this next set exists to
prevent. Four series answer "can I believe the four above":

| Metric | Threshold | What a non-zero reading means |
| --- | --- | --- |
| `lookout_leeway_counter_mismatch_total` | **zero** | A subject's incrementally-maintained distribution disagreed with a rebuild from the pod cache. The disagreement is repaired in place, so the numbers you see are right — but the increments that produced them were not, and a rate here means a delta rule has a hole. |
| `lookout_leeway_preference_disagreement` | **zero** | The node's compute-class rank annotation and the sentinel's own inference disagree. The annotation wins, so this is not a wrong number on a graph; it is a reading that our understanding of the class's rules is wrong, which is worse. |
| `lookout_leeway_transient_subjects` | context | Subject-axes whose judgement was suppressed or relaxed, by the state responsible. **Look here before believing a quiet estate.** A fleet-wide `domain-outage` reading means the silence is suppression, not health. |
| `lookout_leeway_domains_unavailable` | context | Domains with no usable node, per axis. Reported as zero when nothing is out, so the series exists before the first outage rather than appearing during one. |

Add `lookout_leeway_preference_rank_pending` to that list with an
asterisk: it is normally non-zero and brief. GKE writes the rank
annotation 33 to 44 seconds after a node registers, and inference has
nothing to read until it does. A reading that **stays** up is a class
whose rank never resolved.

If you push metrics over OTLP, the export SLIs apply here as much as
anywhere — see [Observability](/operations/observability/#pushing-metrics-over-otlp).
A dead collector makes these dashboards stale without making them wrong,
which is the specific way they mislead.

## What to alert on

Two threshold-zero alerts, `counter_mismatch_total` and
`preference_disagreement`, both about the sentinel rather than the
cluster. Beyond those, alert on findings, not on the gauges: a Tier A
finding is already a `critical` and already routed. Graph
`lookout_leeway_drift`, `lookout_leeway_subjects_tracked` and
`lookout_leeway_alert_state`; page on neither.

`lookout_leeway_alert_state` is worth one note. It carries only subjects
with an **open episode** — 1 for pending, 2 for firing — so a series
disappearing is a subject that recovered, not a subject that stopped
being scraped. Rate-of-change on it reads as noise; use it to answer
"what is open right now".

## The dashboard

`deploy/dashboards/leeway.json` is a Grafana dashboard over the whole
instrument set, built the way this page is ordered. Its first row is
deliberately **not** drift — it is the four SLIs above, because a
dashboard that shows drift without showing whether drift is being
measured correctly is the failure they exist to prevent. Drift,
compute-class ranks, and baselines-and-cost follow.

The JSON is the artifact. Import it through Grafana's UI, point
Terraform or the Grafana API at it, drop it in a git-synced folder — it
carries no provisioning assumptions and prompts for a Prometheus data
source on import. For the sidecar, there is a ConfigMap wrapper:

```sh
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy/dashboards?ref=v0.26.0"
```

That lands `lookout-leeway-dashboard` in `agent-triage` with the
`grafana_dashboard: "1"` label. **The sidecar only picks it up if it is
searching that namespace**, and Grafana's chart defaults
`sidecar.dashboards.searchNamespace` to its own release namespace, so
more often than not it is not. Wrap the overlay with a `namespace:` of
your own rather than editing it in place; the comment at the top of
`deploy/dashboards/kustomization.yaml` has the four lines.

It is outside the base bundle for the same reason the ServiceMonitor is:
`kubectl apply -k deploy/` must not assume a monitoring stack. Unlike
the ServiceMonitor, there is no Helm equivalent — a chart can only
`.Files.Get` inside its own directory, so shipping it through the chart
would mean a second copy of an eight-hundred-line JSON kept in sync by
convention, and this file is short enough to apply on its own that the
trade is not worth making.

Four choices in it are worth knowing about, because they are the ones
you would otherwise have to rediscover:

- **Mean achieved rank** divides rank-weighted pod-time by pod-time over
  **tier ranks only** — `rank!~"unknown|unsatisfiable|off-axis"`. A mean
  over a bucket whose rank is `unsatisfiable` is not a mean of anything,
  and leaving those in the denominator drags the number toward zero
  exactly when a class stops being satisfiable.
- **Pod-time by rank** is a rate over a counter, not a pod count. Ninety
  seconds of rank-3 pods during a scale-up and three weeks parked on a
  spot fallback are the same picture to a gauge.
- **Both episode tables are instant queries**, for the reason in the
  note above: the series only exists while an episode is open.
- **The subject panels are `topk(20, …)`**, which keeps them readable on
  a large estate and means they are a worst-offenders view rather than a
  census. `lookout_leeway_subjects_tracked` is the census.
