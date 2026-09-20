// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package computeclass

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// MeterName is the instrumentation scope §7.7's metrics are declared under.
const MeterName = "github.com/go-steer/k8s-lookout/pkg/sources/computeclass"

// Instrument names, in OpenTelemetry form (§8.4). The Prometheus spelling is
// derived by the exporter, never written twice — see the same note on
// pkg/sources/topologydrift/metrics.go, and TestInstrumentNames_PrometheusSpelling
// which pins the derivation here too.
const (
	metricPodTime          = "lookout.leeway.preference.pod_time"
	metricRankWeightedTime = "lookout.leeway.preference.rank_weighted_time"
	metricPods             = "lookout.leeway.preference.pods"
	metricNodes            = "lookout.leeway.preference.nodes"
	metricTransitions      = "lookout.leeway.preference.transitions"
	metricUnmatched        = "lookout.leeway.preference.unmatched"
	metricAmbiguous        = "lookout.leeway.preference.ambiguous"
	metricDisagreement     = "lookout.leeway.preference.disagreement"
	metricOutOfRange       = "lookout.leeway.preference.out_of_range"
	metricAxisInvalid      = "lookout.leeway.preference.axis_invalid"
	metricNoRuleMatching   = "lookout.leeway.preference.no_rule_matching"
	metricOffAxis          = "lookout.leeway.preference.off_axis"
	metricRankPending      = "lookout.leeway.preference.rank_pending"
	metricUnsupported      = "lookout.leeway.preference.unsupported_rules"
	metricUnreadableClass  = "lookout.leeway.preference.unreadable_class_nodes"
	metricDecodeErrors     = "lookout.leeway.preference.decode_errors"
	metricUnderflows       = "lookout.leeway.preference.tracker_underflows"
	metricAxisInfo         = "lookout.leeway.preference.axis_info"
)

// Instrument descriptions, hoisted for the same reason topologydrift hoists
// its own: they are the help string on both sides of the export, and a help
// string that says two different things is worse than one that says nothing.
const (
	descPodTime = "Pod-seconds accumulated at each preference rank of a compute class (leeway §7.7.3). " +
		"This is the series the whole source exists for, and it is time-weighted on purpose: a ninety-second burst of " +
		"rank-3 pods during a scale-up and three weeks parked on a spot fallback look identical to a gauge. " +
		"`rank` is the derived preference TIER, not the raw ccc_priority_index — on a class that sets priorityScore " +
		"those are different numbers and can even run in opposite directions. Three ranks are not numbers at all: " +
		"`unknown` is a node whose rank is not yet resolved, `unsatisfiable` one GKE could not fit to any rule, and " +
		"`off-axis` one provisioned outside the priority list entirely. " +
		"`spec_hash` changes when the class is edited, which starts a new series rather than averaging two different " +
		"definitions of rank 1 together."
	descRankWeightedTime = "Sum of rank times pod-seconds, over tier ranks only (leeway §7.7.3). " +
		"Divide by the pod_time summed over the same ranks to get mean achieved rank: 0.0 is an estate getting its " +
		"first choice, and a number that climbs is capacity quietly draining out from under it. " +
		"The unknown, unsatisfiable and off-axis buckets are excluded from both halves, because a mean over a bucket " +
		"whose rank is `unsatisfiable` is not a mean of anything."
	descPods  = "Pods currently occupying each preference rank. The supporting gauge to pod_time, not a substitute for it."
	descNodes = "Nodes currently resolved to each preference rank, by where the rank came from. " +
		"`source=annotation` is GKE's own ccc_priority_index, `inferred` is k8s-lookout matching the node's attributes " +
		"against the class's priority rules, and `none` is neither answering. `rule_index` is the raw list position and " +
		"`rule` renders it for humans — both identity, never ordering."
	descTransitions = "Nodes moving between placements on one axis (leeway §7.7.3). " +
		"Node-level, not pod-level: spike S1 established that a compute-class fallback provisions a NEW node and the " +
		"ReplicaSet creates a new pod on it, so no pod ever changes rank. " +
		"`lateral=true` is a move between two rules sharing a tier — capacity churn within a preference level, counted " +
		"because it shows where the estate is thrashing, and excluded from every degradation signal because an " +
		"equal-score alternative is not a demotion. A from_rank of `unknown` is the node's annotation arriving, which " +
		"happens once in the first minute of every node's life and is not a fallback."
	descUnmatched = "Nodes no priority rule matched. " +
		"The alert to write: threshold zero. Every one of these is a node whose rank rests on GKE's undocumented " +
		"annotation with nothing checking it, and a sustained non-zero reading means the matcher has fallen behind " +
		"the rules people are actually writing. Counted even when the annotation answered, because the coverage gap " +
		"that matters is the one the working primary path is hiding."
	descAmbiguous = "Nodes more than one priority rule matched. " +
		"Two rules admitting the same node is legal and usually harmless — GKE takes the first — but it means the " +
		"inferred rank is a guess between them, so the cross-check on these nodes is worth less than it looks."
	descDisagreement = "Nodes where the annotation and the inferred rank disagree. " +
		"The other threshold-zero alert, and the more serious of the two: the annotation wins, so a non-zero reading " +
		"is not a wrong rank — it is evidence that the matcher's model of the rules is wrong, and therefore that " +
		"unmatched and ambiguous cannot be trusted either."
	descOutOfRange = "Nodes whose annotation names a priority index the class no longer has. " +
		"This is a class that was edited under a running node: GKE stamped index 3 and somebody has since deleted a " +
		"rule. The node keeps running; its rank is simply unknowable, and it reads as rank `unknown` until it is replaced."
	descAxisInvalid = "Nodes on a class whose priorities are partially scored. " +
		"A class where some rules set priorityScore and others do not has no well-defined order — list position and " +
		"score would rank it differently — so k8s-lookout declines to score it at all rather than pick one. " +
		"Fix the class: score every rule or none."
	descNoRuleMatching = "Nodes GKE itself could not fit to any priority rule (ccc_no_rule_matching). " +
		"GKE's own verdict, not ours, and a different statement from `unmatched`: this is the provisioner saying the " +
		"node it made satisfies nothing the class asked for. Reads as rank `unsatisfiable`."
	descOffAxis = "Nodes GKE provisioned outside the priority list (ccc_scale_up_anyway). " +
		"The class's whenUnsatisfiable let the autoscaler ignore the priorities rather than leave pods Pending, so " +
		"these nodes are not at any rank — they are off the axis. Reads as rank `off-axis`, and their time is still reported " +
		"under pod_time, because a week spent off-axis is itself the finding."
	descRankPending = "Nodes carrying a compute class with no rank resolved yet. " +
		"Normal and brief: GKE writes ccc_priority_index 33 to 44 seconds after a node registers, and inference has " +
		"nothing to say about a node whose labels have not all landed either. A reading that does not decay to zero " +
		"is the one to look at."
	descUnsupported = "Priority rules the matcher could not evaluate, by the field responsible. " +
		"The matcher fails closed: a rule naming a field pkg/leeway does not model is excluded from inference rather " +
		"than matched on the fields it does understand, because a matcher that ignores what it cannot read attributes " +
		"nodes to the wrong rule and corrupts the cross-check into agreement with nothing. " +
		"A new field appearing here is a feature request with the field name already filled in."
	descUnreadableClass = "Nodes labelled with a compute class this source has no decoded spec for. " +
		"Either the class object has not synced, which is brief, or it failed to decode, which is not — see " +
		"decode_errors_total. These nodes are ranked against nothing and their pods accrue no pod-seconds, so a " +
		"sustained reading means the rank shares are being computed over less than the whole estate."
	descDecodeErrors = "ComputeClass objects this source refused to read, by name. " +
		"A refusal is deliberate: a spec whose priorities are not a list, or whose rules are not objects, would " +
		"produce a plausible-looking axis that is wrong. The row survives the object's deletion, because an error " +
		"counter that vanishes with the broken object hides the edit that broke it."
	descUnderflows = "Unbalanced pod departures in the rank accounting. " +
		"The alert to write: threshold zero. This counts a pod leaving a rank that had nobody at it, which is a BUG " +
		"IN K8S-LOOKOUT and not a cluster condition — every rank share is skewed by an unknown amount until it is " +
		"fixed. The count is clamped rather than allowed to run negative, so the pod-second counters stay monotonic; " +
		"that keeps them readable, it does not make them right."
	descAxisInfo = "One preference axis, as labels on a constant 1. " +
		"`ordering` is how the tiers were derived — `list-position` is bare list order, `priority-score` is the field of " +
		"that name (higher is more preferred, the OPPOSITE direction to list position), and `invalid` is a partially-scored class. " +
		"`rules` is how many priorities the class declares and `tiers` how many distinct preference levels they " +
		"collapse to; `tiers` of 1 means every node on the class is rank 0 by construction and nothing here can " +
		"degrade. `scale_up` is whenUnsatisfiable as DECLARED — `unset` is not the same evidence as GKE's documented " +
		"default, and §7.7.4 gates a finding on the difference."
)

// Attribute keys, typed so a typo is a compile error in one place rather than
// a split series in production.
var (
	attrProvider  = attribute.Key("provider")
	attrAxis      = attribute.Key("axis")
	attrSpecHash  = attribute.Key("spec_hash")
	attrRank      = attribute.Key("rank")
	attrRuleIndex = attribute.Key("rule_index")
	attrRule      = attribute.Key("rule")
	attrSource    = attribute.Key("source")
	attrFromRank  = attribute.Key("from_rank")
	attrToRank    = attribute.Key("to_rank")
	attrLateral   = attribute.Key("lateral")
	attrField     = attribute.Key("field")
	attrClass     = attribute.Key("class")
	attrOrdering  = attribute.Key("ordering")
	attrRules     = attribute.Key("rules")
	attrTiers     = attribute.Key("tiers")
	attrScaleUp   = attribute.Key("scale_up")
	attrMigration = attribute.Key("active_migration")
)

// MetricDoc documents one series in its PROMETHEUS spelling — the name an
// operator greps for, not the OpenTelemetry name it was declared under.
type MetricDoc struct {
	Name   string
	Type   string
	Labels []string
	Help   string
}

// MetricDocs returns every series this source exports, for the generated
// metrics reference. Hand-kept for the same reason topologydrift's is — these
// instruments have no prometheus.Collector to describe themselves — and held
// to the exporter by TestMetricDocs_MatchTheExporter.
func MetricDocs() []MetricDoc {
	axis := []string{"provider", "axis"}
	return []MetricDoc{
		{
			Name:   "lookout_leeway_preference_pod_time_seconds_total",
			Type:   "counter",
			Labels: []string{"provider", "axis", "spec_hash", "rank"},
			Help:   descPodTime,
		},
		{
			Name:   "lookout_leeway_preference_rank_weighted_time_seconds_total",
			Type:   "counter",
			Labels: []string{"provider", "axis", "spec_hash"},
			Help:   descRankWeightedTime,
		},
		{
			Name:   "lookout_leeway_preference_pods",
			Type:   "gauge",
			Labels: []string{"provider", "axis", "rank"},
			Help:   descPods,
		},
		{
			Name:   "lookout_leeway_preference_nodes",
			Type:   "gauge",
			Labels: []string{"provider", "axis", "rank", "rule_index", "rule", "source"},
			Help:   descNodes,
		},
		{
			Name:   "lookout_leeway_preference_transitions_total",
			Type:   "counter",
			Labels: []string{"provider", "axis", "from_rank", "to_rank", "lateral"},
			Help:   descTransitions,
		},
		{Name: "lookout_leeway_preference_unmatched", Type: "gauge", Labels: axis, Help: descUnmatched},
		{Name: "lookout_leeway_preference_ambiguous", Type: "gauge", Labels: axis, Help: descAmbiguous},
		{Name: "lookout_leeway_preference_disagreement", Type: "gauge", Labels: axis, Help: descDisagreement},
		{Name: "lookout_leeway_preference_out_of_range", Type: "gauge", Labels: axis, Help: descOutOfRange},
		{Name: "lookout_leeway_preference_axis_invalid", Type: "gauge", Labels: axis, Help: descAxisInvalid},
		{Name: "lookout_leeway_preference_no_rule_matching", Type: "gauge", Labels: axis, Help: descNoRuleMatching},
		{Name: "lookout_leeway_preference_off_axis", Type: "gauge", Labels: axis, Help: descOffAxis},
		{Name: "lookout_leeway_preference_rank_pending", Type: "gauge", Labels: axis, Help: descRankPending},
		{
			Name:   "lookout_leeway_preference_unsupported_rules",
			Type:   "gauge",
			Labels: []string{"provider", "axis", "field"},
			Help:   descUnsupported,
		},
		{
			Name:   "lookout_leeway_preference_unreadable_class_nodes",
			Type:   "gauge",
			Labels: []string{"provider", "class"},
			Help:   descUnreadableClass,
		},
		{
			Name:   "lookout_leeway_preference_decode_errors_total",
			Type:   "counter",
			Labels: []string{"provider", "class"},
			Help:   descDecodeErrors,
		},
		{
			Name: "lookout_leeway_preference_tracker_underflows_total",
			Type: "counter",
			Help: descUnderflows,
		},
		{
			Name:   "lookout_leeway_preference_axis_info",
			Type:   "gauge",
			Labels: []string{"provider", "axis", "spec_hash", "ordering", "rules", "tiers", "scale_up", "active_migration"},
			Help:   descAxisInfo,
		},
	}
}

// instruments holds the observable instruments and the callback registration.
//
// Everything here is observable — filled at scrape time from the state maps —
// including the counters. A synchronous counter would need a context on every
// informer callback and a second copy of the numbers already in those maps;
// reading them once per scrape keeps one source of truth, and the counters are
// monotonic because the underlying totals only ever grow.
type instruments struct {
	podTime          metric.Float64ObservableCounter
	rankWeightedTime metric.Float64ObservableCounter
	pods             metric.Int64ObservableGauge
	nodes            metric.Int64ObservableGauge
	transitions      metric.Int64ObservableCounter
	unmatched        metric.Int64ObservableGauge
	ambiguous        metric.Int64ObservableGauge
	disagreement     metric.Int64ObservableGauge
	outOfRange       metric.Int64ObservableGauge
	axisInvalid      metric.Int64ObservableGauge
	noRuleMatching   metric.Int64ObservableGauge
	offAxis          metric.Int64ObservableGauge
	rankPending      metric.Int64ObservableGauge
	unsupported      metric.Int64ObservableGauge
	unreadableClass  metric.Int64ObservableGauge
	decodeErrors     metric.Int64ObservableCounter
	underflows       metric.Int64ObservableCounter
	axisInfo         metric.Int64ObservableGauge

	reg metric.Registration
}

// all is every instrument the callback fills, for RegisterCallback. Kept
// beside the struct so that adding a field and forgetting to register it is
// one edit from being noticed rather than a series silently never collected.
func (in *instruments) all() []metric.Observable {
	return []metric.Observable{
		in.podTime, in.rankWeightedTime, in.pods, in.nodes, in.transitions,
		in.unmatched, in.ambiguous, in.disagreement, in.outOfRange, in.axisInvalid,
		in.noRuleMatching, in.offAxis, in.rankPending, in.unsupported,
		in.unreadableClass, in.decodeErrors, in.underflows, in.axisInfo,
	}
}

// startMetrics declares the instruments and registers the scrape callback.
func (s *Source) startMetrics() error {
	meter := s.cfg.Meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter(MeterName)
	}

	in := &instruments{}
	var err error
	declare := func(name string, fn func() error) {
		if err == nil {
			if e := fn(); e != nil {
				err = fmt.Errorf("%s: declare %s: %w", Name, name, e)
			}
		}
	}

	// Unit "s" on the two pod-second counters: the exporter injects it into
	// the middle of the name, ahead of the _total, so these reach /metrics as
	// ..._pod_time_seconds_total, which is the name we want.
	declare(metricPodTime, func() (e error) {
		in.podTime, e = meter.Float64ObservableCounter(metricPodTime,
			metric.WithUnit("s"), metric.WithDescription(descPodTime))
		return
	})
	declare(metricRankWeightedTime, func() (e error) {
		in.rankWeightedTime, e = meter.Float64ObservableCounter(metricRankWeightedTime,
			metric.WithUnit("s"), metric.WithDescription(descRankWeightedTime))
		return
	})
	declare(metricPods, func() (e error) {
		in.pods, e = meter.Int64ObservableGauge(metricPods, metric.WithDescription(descPods))
		return
	})
	declare(metricNodes, func() (e error) {
		in.nodes, e = meter.Int64ObservableGauge(metricNodes, metric.WithDescription(descNodes))
		return
	})
	declare(metricTransitions, func() (e error) {
		in.transitions, e = meter.Int64ObservableCounter(metricTransitions, metric.WithDescription(descTransitions))
		return
	})

	// The seven condition gauges and the rank-pending one. Same shape, so they
	// go through a table — a copy-pasted block of eight near-identical
	// declarations is where a description ends up attached to the wrong name.
	conditions := []struct {
		name string
		desc string
		into *metric.Int64ObservableGauge
	}{
		{metricUnmatched, descUnmatched, &in.unmatched},
		{metricAmbiguous, descAmbiguous, &in.ambiguous},
		{metricDisagreement, descDisagreement, &in.disagreement},
		{metricOutOfRange, descOutOfRange, &in.outOfRange},
		{metricAxisInvalid, descAxisInvalid, &in.axisInvalid},
		{metricNoRuleMatching, descNoRuleMatching, &in.noRuleMatching},
		{metricOffAxis, descOffAxis, &in.offAxis},
		{metricRankPending, descRankPending, &in.rankPending},
	}
	for _, c := range conditions {
		declare(c.name, func() (e error) {
			*c.into, e = meter.Int64ObservableGauge(c.name, metric.WithDescription(c.desc))
			return
		})
	}

	declare(metricUnsupported, func() (e error) {
		in.unsupported, e = meter.Int64ObservableGauge(metricUnsupported, metric.WithDescription(descUnsupported))
		return
	})
	declare(metricUnreadableClass, func() (e error) {
		in.unreadableClass, e = meter.Int64ObservableGauge(metricUnreadableClass, metric.WithDescription(descUnreadableClass))
		return
	})
	declare(metricDecodeErrors, func() (e error) {
		in.decodeErrors, e = meter.Int64ObservableCounter(metricDecodeErrors, metric.WithDescription(descDecodeErrors))
		return
	})
	declare(metricUnderflows, func() (e error) {
		in.underflows, e = meter.Int64ObservableCounter(metricUnderflows, metric.WithDescription(descUnderflows))
		return
	})
	declare(metricAxisInfo, func() (e error) {
		in.axisInfo, e = meter.Int64ObservableGauge(metricAxisInfo, metric.WithDescription(descAxisInfo))
		return
	})
	if err != nil {
		return err
	}

	in.reg, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			s.observe(o, in)
			return nil
		},
		in.all()...,
	)
	if err != nil {
		return fmt.Errorf("%s: register metric callback: %w", Name, err)
	}
	s.in = in
	return nil
}

// closeMetrics unregisters the callback.
func (s *Source) closeMetrics() error {
	if s.in == nil || s.in.reg == nil {
		return nil
	}
	return s.in.reg.Unregister()
}

// observe fills every instrument from the state maps.
//
// It flushes the tracker first, on purpose. The pod-second buckets advance on
// transitions and on the flush ticker, so a scrape that read them without
// flushing would report a total that is up to one tick stale — and since the
// tick is coarser than a scrape, consecutive scrapes would see the counter
// move in steps rather than smoothly. Flushing here makes every scrape exact
// as of itself.
func (s *Source) observe(o metric.Observer, in *instruments) {
	now := s.clock()
	s.tracker.Flush(now)
	snap := s.tracker.Snapshot()

	s.mu.Lock()
	defer s.mu.Unlock()

	provider := attrProvider.String(string(leeway.ProviderGKEComputeClass))

	// spec_hash comes from the live class, not from the tracker — the tracker
	// is reset on a re-tiering, so whatever it holds was accumulated under the
	// hash that is current now.
	hashes := map[leeway.AxisKey]string{}
	for _, c := range s.classes {
		hashes[c.Axis.Key] = c.Axis.SpecHash
	}

	for _, r := range snap.Ranks {
		o.ObserveFloat64(in.podTime, r.PodSeconds, metric.WithAttributes(
			provider,
			attrAxis.String(r.Axis.Name),
			attrSpecHash.String(hashes[r.Axis]),
			attrRank.String(r.Rank.String()),
		))
		o.ObserveInt64(in.pods, int64(r.Pods), metric.WithAttributes(
			provider,
			attrAxis.String(r.Axis.Name),
			attrRank.String(r.Rank.String()),
		))
	}
	for _, a := range snap.Axes {
		o.ObserveFloat64(in.rankWeightedTime, a.RankWeightedSeconds, metric.WithAttributes(
			provider,
			attrAxis.String(a.Axis.Name),
			attrSpecHash.String(hashes[a.Axis]),
		))
	}
	o.ObserveInt64(in.underflows, snap.Underflows)

	for key, n := range s.transitions {
		o.ObserveInt64(in.transitions, n, metric.WithAttributes(
			provider,
			attrAxis.String(key.Axis.Name),
			attrFromRank.String(key.From.String()),
			attrToRank.String(key.To.String()),
			attrLateral.Bool(key.Lateral),
		))
	}
	for class, n := range s.decodeFails {
		o.ObserveInt64(in.decodeErrors, n, metric.WithAttributes(provider, attrClass.String(class)))
	}

	s.observeNodes(o, in, provider)
	s.observeAxes(o, in, provider)
}

// nodeCounts is one axis's tally of the conditions §7.7.3 reports.
type nodeCounts struct {
	unmatched, ambiguous, disagreement      int64
	outOfRange, axisInvalid, noRuleMatching int64
	offAxis, rankPending                    int64
	byPlacement                             map[placementKey]int64
	unsupported                             map[string]int64
}

type placementKey struct {
	Rank      leeway.Rank
	RuleIndex int
	Rule      string
	Source    leeway.RankSource
}

// observeNodes walks the node map once and reports the per-axis tallies.
// Caller holds s.mu.
func (s *Source) observeNodes(o metric.Observer, in *instruments, provider attribute.KeyValue) {
	byAxis := map[leeway.AxisKey]*nodeCounts{}
	unreadable := map[string]int64{}

	for _, ns := range s.nodes {
		if !ns.known {
			if ns.class != "" {
				unreadable[ns.class]++
			}
			continue
		}
		c, ok := byAxis[ns.axis]
		if !ok {
			c = &nodeCounts{
				byPlacement: map[placementKey]int64{},
				unsupported: map[string]int64{},
			}
			byAxis[ns.axis] = c
		}
		p := ns.place
		if p.Unmatched {
			c.unmatched++
		}
		if p.Ambiguous {
			c.ambiguous++
		}
		if p.Disagreed {
			c.disagreement++
		}
		switch p.Outcome {
		case leeway.OutcomeOutOfRange:
			c.outOfRange++
		case leeway.OutcomeAxisInvalid:
			c.axisInvalid++
		case leeway.OutcomeNoRuleMatching:
			c.noRuleMatching++
		case leeway.OutcomeOffAxis:
			c.offAxis++
		case leeway.OutcomePending:
			c.rankPending++
		}
		for _, u := range p.Unsupported {
			c.unsupported[u.Field]++
		}
		c.byPlacement[placementKey{
			Rank:      p.Rank,
			RuleIndex: p.RuleIndex,
			Rule:      s.renderRule(ns.axis, p),
			Source:    p.Source,
		}]++
	}

	for class, n := range unreadable {
		o.ObserveInt64(in.unreadableClass, n, metric.WithAttributes(provider, attrClass.String(class)))
	}
	for key, c := range byAxis {
		axis := attrAxis.String(key.Name)
		set := metric.WithAttributes(provider, axis)
		o.ObserveInt64(in.unmatched, c.unmatched, set)
		o.ObserveInt64(in.ambiguous, c.ambiguous, set)
		o.ObserveInt64(in.disagreement, c.disagreement, set)
		o.ObserveInt64(in.outOfRange, c.outOfRange, set)
		o.ObserveInt64(in.axisInvalid, c.axisInvalid, set)
		o.ObserveInt64(in.noRuleMatching, c.noRuleMatching, set)
		o.ObserveInt64(in.offAxis, c.offAxis, set)
		o.ObserveInt64(in.rankPending, c.rankPending, set)
		for field, n := range c.unsupported {
			o.ObserveInt64(in.unsupported, n, metric.WithAttributes(provider, axis, attrField.String(field)))
		}
		for pk, n := range c.byPlacement {
			o.ObserveInt64(in.nodes, n, metric.WithAttributes(
				provider, axis,
				attrRank.String(pk.Rank.String()),
				attrRuleIndex.String(ruleIndexLabel(pk.RuleIndex)),
				attrRule.String(pk.Rule),
				attrSource.String(pk.Source.String()),
			))
		}
	}
}

// observeAxes reports one row per decoded class. Caller holds s.mu.
func (s *Source) observeAxes(o metric.Observer, in *instruments, provider attribute.KeyValue) {
	for _, c := range s.classes {
		o.ObserveInt64(in.axisInfo, 1, metric.WithAttributes(
			provider,
			attrAxis.String(c.Axis.Key.Name),
			attrSpecHash.String(c.Axis.SpecHash),
			attrOrdering.String(c.Axis.Ordering.String()),
			attrRules.String(strconv.Itoa(len(c.Axis.Rules))),
			attrTiers.String(strconv.Itoa(c.Axis.Tiers)),
			attrScaleUp.String(c.ScaleUp.String()),
			attrMigration.Bool(c.OptimizeRulePriority),
		))
	}
}

// renderRule turns a placement's rule index into the human label. Caller holds
// s.mu.
func (s *Source) renderRule(key leeway.AxisKey, p leeway.RankPlacement) string {
	class, ok := s.classes[key.Name]
	if !ok || p.RuleIndex < 0 {
		return ""
	}
	rule, ok := class.Axis.RuleAt(p.RuleIndex)
	if !ok {
		return ""
	}
	return rule.Render()
}

// ruleIndexLabel renders an index, keeping "none" distinct from index 0 —
// which is the whole point of the label, since 0 is the most preferred
// position and is exactly what a naive default would invent.
func ruleIndexLabel(i int) string {
	if i < 0 {
		return "none"
	}
	return strconv.Itoa(i)
}
