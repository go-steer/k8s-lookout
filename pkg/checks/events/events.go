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

// Package events implements `lookout triage events` (DESIGN.md §5):
// the deduped chronological event timeline over a target's
// owner-reference tree, absorbing v2's ev-sifter and
// hpa-loop-catcher. It is the PULL mode of the `lookout watch`
// pipeline's filter/dedup stage: one paged List of core/v1 Events
// instead of an informer stream, collapsed by the same
// (involvedObject.uid, canonical reason) identity the sentinel dedups
// on.
//
// Dedup sharing with pkg/engine — what is reused and what is not
// (the §5 "pull mode of the same filter/dedup" note, made precise):
// the piece that DEFINES incident identity, engine.CanonicalReason
// (ErrImagePull→ImagePullBackOff, BackOff→CrashLoopBackOff, …), and
// the engine.EventKey shape are reused verbatim, so a timeline entry
// here collapses exactly the reason families a sentinel session
// does. engine.DedupCache itself is NOT constructed: it is a
// rolling-WINDOW session router — TTL cooldown that re-fires expired
// incidents, LRU bounds, session/storm bindings, restart persistence
// — and every one of those semantics is wrong for a one-shot bounded
// timeline, where dedup is pure aggregation (sum counts, min first
// seen, max last seen) with no notion of expiry or routing.
//
// HPA thrash detection is an analysis mode here rather than a check
// on the HPA object because the HPA keeps no replica history: its
// status has current/desired replicas and a couple of timestamps,
// nothing to read an oscillation from. The controller's
// SuccessfulRescale events are the only surviving record of the
// replica sequence, so the oscillation is recovered from the event
// stream ("New size: N" messages) — see hpa.go.
package events

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// defaultSince is the lookback when --since is 0 (§4.2: 0 means the
// command default).
const defaultSince = time.Hour

// hpaKind is the involvedObject.Kind of HPA controller events.
const hpaKind = "HorizontalPodAutoscaler"

func init() {
	checks.Register(New(kube.DefaultSource()))
}

// New returns the `triage events` command bound to a client source.
func New(source kube.ClientSource) checks.Command {
	return newCommand(source, time.Now)
}

// newCommand additionally injects the clock; tests pin it so the
// --since cutoff and the --hpa-window math are golden-testable.
func newCommand(source kube.ClientSource, now func() time.Time) checks.Command {
	e := &events{source: source, now: now}
	return checks.Command{
		Name:    "triage events",
		MCPName: "k8s_event_timeline",
		Summary: "Deduped chronological event timeline: kubectl get events, but collapsed by (object, reason family) over a workload's whole owner-reference tree, with HPA rescale-oscillation (thrash) detection.",
		Flags: []emit.FlagSpec{
			{Name: "hpa-window", Type: emit.FlagDuration, Default: "30m",
				Help: "report event.hpa_thrash when enough scale-direction changes fall inside a window this long"},
			{Name: "hpa-flips", Type: emit.FlagInt, Default: "2",
				Help: "scale-direction changes within --hpa-window that count as thrash (2 = up→down→up)"},
		},
		Output: []checks.OutputField{
			{Name: "count", Doc: "events collapsed into this timeline entry: k8s per-event repeat counts summed across the entry's reason family"},
			{Name: "first_seen", Doc: "RFC3339 timestamp of the entry's oldest activity"},
			{Name: "last_seen", Doc: "RFC3339 timestamp of the entry's newest activity (the timeline sort key)"},
			{Name: "source", Doc: "reporting component (kubelet, horizontal-pod-autoscaler, …)"},
			{Name: "variants", Doc: "raw Event.Reason values collapsed into this entry, comma-separated (present only when a reason family merged more than one)"},
			{Name: "replicas", Doc: "event.hpa_thrash: the chronological replica sequence recovered from SuccessfulRescale events, e.g. 2->6->2->6"},
			{Name: "flips", Doc: "event.hpa_thrash: most scale-direction changes observed inside one --hpa-window"},
			{Name: "window", Doc: "event.hpa_thrash: the --hpa-window the flips were counted in"},
			{Name: "target", Doc: "event.hpa_thrash: the HPA's scaleTargetRef as Kind/name (when the HPA object was readable)"},
		},
		Examples: []string{
			"lookout triage events --workload=Deployment/prod/api",
			"lookout triage events --workload=Pod/prod/api-6d5f8c-x2v9k --since=30m",
			"lookout triage events --namespace=prod",
			"lookout triage events -A --since=2h --format=json",
			"lookout triage events --workload=Deployment/prod/api --hpa-window=15m --hpa-flips=4",
		},
		Run: e.run,
	}
}

// events carries the injected seams of one command instance.
type events struct {
	source kube.ClientSource
	now    func() time.Time
}

func (e *events) run(ctx context.Context, inv emit.Invocation) (int, error) {
	wl := inv.Scope.Workload
	switch {
	case wl.IsZero() && inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("no target: pass --workload=<Kind>/<namespace>/<name> for an owner-tree timeline, or --namespace=<ns> / -A for a namespace timeline")
	case !wl.IsZero() && inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("-A does not apply: a workload's owner-reference tree lives in one namespace (%s)", wl.Namespace)
	case !wl.IsZero() && inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace:
		return 0, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
	}
	window := inv.Flags.Duration("hpa-window")
	if window <= 0 {
		return 0, emit.UsageErrorf("--hpa-window must be positive, got %s", window)
	}
	minFlips := inv.Flags.Int("hpa-flips")
	if minFlips <= 0 {
		return 0, emit.UsageErrorf("--hpa-flips must be positive, got %d", minFlips)
	}
	since := inv.Scope.Since
	if since == 0 {
		since = defaultSince
	}
	cutoff := e.now().Add(-since)

	client, err := e.source(ctx)
	if err != nil {
		return 0, err
	}

	// Workload mode: the match set is the target's whole
	// owner-reference tree (climb to the root owner, then every
	// descendant — siblings included) plus any HPA whose
	// scaleTargetRef points into that tree, resolved from the same
	// one-List-pass graph `state edges` and `bundle` use.
	var match map[string]struct{}
	hpaTargets := map[string]string{} // "ns/name" → scaleTargetRef Kind/name
	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	if !wl.IsZero() {
		listNS = wl.Namespace
		cluster, err := state.LoadCluster(ctx, client, listNS)
		if err != nil {
			return 0, err
		}
		id, err := cluster.WorkloadNode(wl)
		if err != nil {
			return 0, err
		}
		match = ownerTree(cluster.Snapshot(), id)
		hpas, err := listHPAs(ctx, client, listNS)
		if err != nil {
			return 0, err
		}
		for i := range hpas {
			h := &hpas[i]
			ref := h.Spec.ScaleTargetRef
			if _, ok := match[matchKey(ref.Kind, h.Namespace, ref.Name)]; !ok {
				continue
			}
			match[matchKey(hpaKind, h.Namespace, h.Name)] = struct{}{}
			hpaTargets[h.Namespace+"/"+h.Name] = ref.Kind + "/" + ref.Name
		}
	}

	evs, err := listEvents(ctx, client, listNS)
	if err != nil {
		return 0, err
	}

	timeline := map[engine.EventKey]*entry{}
	var rescales []rescale
	for i := range evs {
		ev := &evs[i]
		first, last := eventTimes(ev)
		if last.Before(cutoff) {
			continue
		}
		if match != nil {
			io := ev.InvolvedObject
			if _, ok := match[matchKey(io.Kind, io.Namespace, io.Name)]; !ok {
				continue
			}
		}
		accumulate(timeline, ev, first, last)
		if ev.Reason == "SuccessfulRescale" && ev.InvolvedObject.Kind == hpaKind {
			rescales = append(rescales, newRescale(ev, first, last))
		}
	}

	for _, en := range chronological(timeline) {
		if err := inv.Out.Emit(en.finding()); err != nil {
			return 0, err
		}
	}
	for _, f := range thrashFindings(rescales, hpaTargets, window, minFlips) {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	// The summary counts EVENTS scanned — the timeline's raw
	// material — not the graph's List pass; the tree resolution is
	// plumbing, not scan surface.
	return len(evs), nil
}

// matchKey keys the owner-tree membership set. Graph kind names
// (graph.NodeKind.String) are the canonical k8s kind spellings, so
// they compare directly against Event.InvolvedObject.Kind.
func matchKey(kind, namespace, name string) string {
	return kind + "\x00" + namespace + "\x00" + name
}

// ownerTree returns the identity set of id's owner-reference tree:
// climb the controller chain to the root owner, then collect the
// root and every transitive descendant over Owns edges (§6). For a
// Pod target this includes its ReplicaSet, its Deployment, AND its
// sibling pods — the tree, not just the chain.
func ownerTree(snap *graph.Snapshot, id graph.NodeID) map[string]struct{} {
	root := id
	if chain := snap.OwnerChain(id); len(chain) > 0 {
		root = chain[len(chain)-1]
	}
	set := map[string]struct{}{}
	visited := map[graph.NodeID]struct{}{root: {}}
	stack := []graph.NodeID{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if ref, ok := snap.Resolve(n); ok {
			set[matchKey(ref.Kind.String(), ref.Namespace, ref.Name)] = struct{}{}
		}
		for _, e := range snap.Out(n) {
			if e.Kind != graph.EdgeOwns {
				continue
			}
			if _, dup := visited[e.To]; dup {
				continue
			}
			visited[e.To] = struct{}{}
			stack = append(stack, e.To)
		}
	}
	return set
}

// entry is one timeline row: every event that shares the sentinel's
// dedup identity (involvedObject.uid, canonical reason), aggregated.
type entry struct {
	namespace, kind, name string
	reason                string // canonical (engine.CanonicalReason)
	variants              map[string]struct{}
	warning               bool
	count                 int
	first, last           time.Time
	message               string // newest variant's message
	source                string
}

// accumulate folds one listed event into its timeline entry, keyed
// exactly like the sentinel's dedup cache: (uid, canonical reason).
// Events without an involvedObject UID (rare, but legal) fall back
// to the object identity so they still collapse per object.
func accumulate(timeline map[engine.EventKey]*entry, ev *corev1.Event, first, last time.Time) {
	io := ev.InvolvedObject
	uid := string(io.UID)
	if uid == "" {
		uid = io.Kind + "/" + io.Namespace + "/" + io.Name
	}
	// Message-aware canonical (engine.CanonicalReasonForEvent): the
	// listed event's message is in hand, so kubelet's generic
	// BackOff/Failed reasons land in the same family the sentinel's
	// dedup cache files them under (pull vs crash-loop).
	key := engine.EventKey{UID: uid, Reason: engine.CanonicalReasonForEvent(ev.Reason, ev.Message)}
	count := int(ev.Count)
	if count < 1 {
		count = 1
	}
	en, ok := timeline[key]
	if !ok {
		en = &entry{
			namespace: io.Namespace,
			kind:      io.Kind,
			name:      io.Name,
			reason:    key.Reason,
			variants:  map[string]struct{}{},
			first:     first,
			last:      last,
			message:   ev.Message,
			source:    eventSource(ev),
		}
		timeline[key] = en
	}
	en.variants[ev.Reason] = struct{}{}
	en.count += count
	if first.Before(en.first) {
		en.first = first
	}
	if !last.Before(en.last) {
		en.last = last
		en.message = ev.Message
		en.source = eventSource(ev)
	}
	if ev.Type == corev1.EventTypeWarning {
		en.warning = true
	}
}

// chronological orders the timeline by newest activity ascending
// (§5: timeline ordering by lastTimestamp), with a full deterministic
// tie-break so the same fixture always renders byte-identically.
func chronological(timeline map[engine.EventKey]*entry) []*entry {
	out := make([]*entry, 0, len(timeline))
	for _, en := range timeline {
		out = append(out, en)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if !a.last.Equal(b.last) {
			return a.last.Before(b.last)
		}
		if !a.first.Equal(b.first) {
			return a.first.Before(b.first)
		}
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.name != b.name {
			return a.name < b.name
		}
		return a.reason < b.reason
	})
	return out
}

func (en *entry) finding() emit.Finding {
	kind, severity := "event.normal", emit.SeverityInfo
	if en.warning {
		kind, severity = "event.warning", emit.SeverityWarning
	}
	f := emit.Finding{
		Kind:         kind,
		Severity:     severity,
		Namespace:    en.namespace,
		KindOfObject: en.kind,
		Name:         en.name,
		Reason:       en.reason,
		Message:      en.message,
		Details: []emit.Field{
			{Key: "count", Value: fmt.Sprintf("%d", en.count)},
			{Key: "first_seen", Value: en.first.UTC().Format(time.RFC3339)},
			{Key: "last_seen", Value: en.last.UTC().Format(time.RFC3339)},
		},
	}
	if len(en.variants) > 1 {
		vs := make([]string, 0, len(en.variants))
		for v := range en.variants {
			vs = append(vs, v)
		}
		sort.Strings(vs)
		f.Details = append(f.Details, emit.Field{Key: "variants", Value: strings.Join(vs, ",")})
	}
	if en.source != "" {
		f.Details = append(f.Details, emit.Field{Key: "source", Value: en.source})
	}
	return f
}

// eventTimes normalizes an Event's activity interval, preferring
// LastTimestamp and falling back to EventTime / CreationTimestamp —
// the same convention as the watch-path's k8s-events source.
func eventTimes(ev *corev1.Event) (first, last time.Time) {
	first = ev.FirstTimestamp.Time
	if first.IsZero() {
		first = ev.EventTime.Time
	}
	if first.IsZero() {
		first = ev.CreationTimestamp.Time
	}
	last = ev.LastTimestamp.Time
	if last.IsZero() {
		last = ev.EventTime.Time
	}
	if last.IsZero() {
		last = ev.CreationTimestamp.Time
	}
	return first, last
}

func eventSource(ev *corev1.Event) string {
	if ev.Source.Component != "" {
		return ev.Source.Component
	}
	return ev.ReportingController
}

// eventPageLimit matches the state package's one-shot page size.
const eventPageLimit = 500

func listEvents(ctx context.Context, client kubernetes.Interface, ns string) ([]corev1.Event, error) {
	var out []corev1.Event
	opts := metav1.ListOptions{Limit: eventPageLimit}
	for {
		l, err := client.CoreV1().Events(ns).List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("listing events: %w", err)
		}
		out = append(out, l.Items...)
		if l.Continue == "" {
			return out, nil
		}
		opts.Continue = l.Continue
	}
}

func listHPAs(ctx context.Context, client kubernetes.Interface, ns string) ([]autoscalingv2.HorizontalPodAutoscaler, error) {
	var out []autoscalingv2.HorizontalPodAutoscaler
	opts := metav1.ListOptions{Limit: eventPageLimit}
	for {
		l, err := client.AutoscalingV2().HorizontalPodAutoscalers(ns).List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("listing horizontalpodautoscalers: %w", err)
		}
		out = append(out, l.Items...)
		if l.Continue == "" {
			return out, nil
		}
		opts.Continue = l.Continue
	}
}
