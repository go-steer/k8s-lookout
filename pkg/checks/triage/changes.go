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

package triage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

const (
	// defaultChangesWindow is the lookback when --since is 0 (§4.2:
	// 0 means the command's default).
	defaultChangesWindow = 30 * time.Minute
	// defaultChangesDepth scopes the neighborhood tighter than
	// radius' default: "what changed near the target", not the whole
	// impact cone.
	defaultChangesDepth = 2
)

// ChangesCommand builds `lookout triage changes` (DESIGN.md §5): what
// changed in the N minutes before onset — rollouts, ConfigMap/Secret
// updates, HPA rescales, node ops — scoped to the target's graph
// neighborhood. The #1 SRE question. Full fidelity comes from a
// sentinel's §6.6 delta log (--store): every graph-visible field
// change, timestamped, joined with the live event timeline; --at
// shifts the window to end at incident onset and answers entirely
// from the store (no cluster access needed). Without a store the
// command answers from what the API can still tell NOW — ReplicaSet
// rollout revisions and recent scaling events — and says so
// (source=live-approximation): one-shot reads cannot see updates the
// API does not timestamp (ConfigMap/Secret edits, label changes,
// node cordons outside the event retention window).
func ChangesCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:        "triage changes",
		MCPName:     "k8s_recent_changes",
		GraphBacked: true,
		Summary:     "What changed around one workload in the window before onset — rollouts, config/secret updates, rescales, node ops — chronological, scoped to the target's graph neighborhood; full fidelity from a sentinel store, best-effort live otherwise.",
		Positional: &checks.Positional{
			Meta: "<Kind>/[<namespace>/]<name>",
			Doc:  targetDoc,
		},
		Flags: []emit.FlagSpec{
			{Name: "depth", Type: emit.FlagInt, Default: strconv.Itoa(defaultChangesDepth),
				Help: "neighborhood radius: graph edges followed per direction to decide which objects' changes are in scope"},
		},
		Kinds: []checks.KindField{
			checks.Kind(kindRollout, "a workload's pod template changed — a new image, container or mount, or a controller churn event", emit.SeverityInfo),
			checks.Kind(kindScale, "a workload's replica count changed", emit.SeverityInfo),
			checks.Kind(kindConfig, "a ConfigMap in the neighborhood changed", emit.SeverityInfo),
			checks.Kind(kindSecret, "a Secret in the neighborhood changed (names and shortened hashes only, never values — §6.5)", emit.SeverityInfo),
			checks.Kind(kindNode, "a Node in the neighborhood changed", emit.SeverityInfo),
			checks.Kind(kindLabel, "only labels changed on a neighborhood object — enough to move it in or out of a selector", emit.SeverityInfo),
			checks.Kind(kindTopology, "a neighborhood object appeared, disappeared, or changed in a way none of the other classes name", emit.SeverityInfo),
			state.UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "at", Doc: "when the change happened, RFC 3339 (also the summary-line note for the resolved --at instant)"},
			{Name: "relation", Doc: "the changed object's place in the target's neighborhood: self (the target or its pods), upstream, lateral, downstream"},
			{Name: "fields", Doc: "changed fields as path=from→to pairs — names, counts, and shortened hashes only, never values (§6.5)"},
			{Name: "origin", Doc: "where the change was seen: log (§6.6 delta log), event (Kubernetes Event), api (reconstructed from current API state)"},
			{Name: "revision", Doc: "deployment.kubernetes.io/revision of a rollout's ReplicaSet (live approximation)"},
			{Name: "image", Doc: "first container image of a rollout's new pod template (live approximation)"},
			{Name: "window", Doc: "summary-line note: the (from, to] window the answer covers, RFC 3339"},
			{Name: "source", Doc: "summary-line note: history (delta log from --store) or live-approximation (no store; see the fidelity gap in --help)"},
			{Name: "unrecorded", Doc: "summary-line note, history only: comma-separated kinds the delta log and its snapshots cannot contain (the sentinel's graph feed never watched them), so a change to one can never be reported"},
		}, state.UnreadFields()...),
		Examples: []string{
			"lookout triage changes Deployment/prod/api --store=/var/lib/lookout/lookout.db",
			"lookout triage changes Deployment/prod/api --since=1h --at=2026-07-25T10:00:00Z --store=/var/lib/lookout/lookout.db",
			"lookout triage changes payments-api-7d9c4b-x2n8p --namespace=prod",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runChanges(ctx, deps, inv)
		},
	}
}

// changeEntry pairs a finding with its instant for chronological
// emission (ties keep discovery order).
type changeEntry struct {
	at  time.Time
	seq int
	f   emit.Finding
}

func runChanges(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	depth := inv.Flags.Int("depth")
	if depth < 1 {
		return 0, emit.UsageErrorf("--depth must be at least 1, got %d", depth)
	}
	wl, err := parseTarget(inv)
	if err != nil {
		return 0, err
	}
	window := inv.Scope.Since
	if window == 0 {
		window = defaultChangesWindow
	}
	// The window ends at --at (the incident-onset question: what
	// changed in the N minutes BEFORE then) or now.
	to := inv.Scope.At
	if to.IsZero() {
		to = deps.now()
	}
	from := to.Add(-window)

	var (
		scanned int
		entries []changeEntry
		cluster *state.Cluster
		client  kubernetes.Interface
	)

	// The neighborhood snapshot: as of onset in --at mode (from the
	// store, no cluster access), live otherwise.
	var snap *graph.Snapshot
	if inv.Scope.At.IsZero() {
		client, err = deps.client(ctx)
		if err != nil {
			return 0, err
		}
		listNS := wl.Namespace
		if inv.Scope.AllNamespaces {
			listNS = metav1.NamespaceAll
		}
		// Tolerant (#546): under the built-in `view` role Nodes,
		// Secrets and the RBAC kinds are refused. The neighborhood is
		// built from what was read, and each gap that narrows it is
		// one read.unavailable record leading the answer.
		cluster, err = state.LoadCluster(ctx, client, listNS, state.Tolerate())
		if err != nil {
			return 0, err
		}
		snap = cluster.Snapshot()
		scanned += cluster.Scanned()
	}
	// emitGaps writes the live load's read.unavailable records; called
	// once the target has resolved, so a lookup failure stays exit 1
	// with nothing on stdout.
	emitGaps := func() error {
		if cluster == nil {
			return nil
		}
		for _, f := range cluster.UnreadFindings(changesUnverified) {
			if err := inv.Out.Emit(f); err != nil {
				return err
			}
		}
		return nil
	}
	// readEvents adds the rescale events to the timeline. Events are
	// one source among several: a role that refuses them (#584) costs
	// the timeline its scaling entries, named by a read.unavailable
	// record, not the rollout and delta-log changes beside them.
	readEvents := func(hood map[string]string, approximation bool) error {
		evs, n, err := eventEntries(ctx, client, wl.Namespace, hood, from, to, approximation)
		if r, ok := checks.ForbiddenRefusal(err); ok {
			return inv.Out.Emit(checks.RefusedFinding(r, "rescale events not read: scaling changes are missing from this timeline"))
		}
		if err != nil {
			return err
		}
		scanned += n
		entries = append(entries, evs...)
		return nil
	}

	if inv.Scope.Store != "" {
		st, err := store.OpenRead(inv.Scope.Store)
		if err != nil {
			return 0, err
		}
		defer func() { _ = st.Close() }()
		if err := inv.Out.Note("source", "history"); err != nil {
			return 0, err
		}
		if !inv.Scope.At.IsZero() {
			snap, err = st.GraphAt(ctx, inv.Scope.At)
			if err != nil {
				return 0, err
			}
			scanned += snap.NumNodes()
			if err := inv.Out.Note("at", inv.Scope.At.UTC().Format(time.RFC3339)); err != nil {
				return 0, err
			}
		}
		if err := noteLogCeiling(ctx, inv.Out, st, snap, inv.Scope.At, to); err != nil {
			return 0, err
		}
		id, err := lookupTarget(snap, wl, inv.Scope.At)
		if err != nil {
			return 0, targetRefused(cluster, wl, err)
		}
		if err := emitGaps(); err != nil {
			return 0, err
		}
		hood := neighborhood(snap, id, depth)
		rows, err := st.GraphChanges(ctx, from, to)
		if err != nil {
			return 0, err
		}
		scanned += len(rows)
		if err := placeDeleted(ctx, st, rows, hood, wl, depth); err != nil {
			return 0, err
		}
		entries = append(entries, logEntries(rows, hood)...)
		// Events still come from the live API (they are not in the
		// delta log); in --at mode there is no client and the window
		// usually predates event retention anyway.
		if client != nil {
			if err := readEvents(hood, false); err != nil {
				return 0, err
			}
		}
	} else {
		// PURE live mode: no delta log to read — approximate from
		// what the API still shows: ReplicaSet rollout revisions and
		// recent scaling events (§6.6: answer live-only and say so).
		id, err := lookupTarget(snap, wl, time.Time{})
		if err != nil {
			return 0, targetRefused(cluster, wl, err)
		}
		if err := emitGaps(); err != nil {
			return 0, err
		}
		hood := neighborhood(snap, id, depth)
		entries = append(entries, rolloutEntries(cluster, hood, from, to)...)
		if err := readEvents(hood, true); err != nil {
			return 0, err
		}
		if err := inv.Out.Note("source", "live-approximation"); err != nil {
			return 0, err
		}
	}

	if err := inv.Out.Note("window", from.UTC().Format(time.RFC3339)+".."+to.UTC().Format(time.RFC3339)); err != nil {
		return 0, err
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].at.Equal(entries[j].at) {
			return entries[i].at.Before(entries[j].at)
		}
		return entries[i].seq < entries[j].seq
	})
	for _, e := range entries {
		if err := inv.Out.Emit(e.f); err != nil {
			return 0, err
		}
	}
	return scanned, nil
}

// changesUnverified names what a skipped List costs `triage changes`.
// The neighborhood is the scope filter, so a gap matters exactly when
// it removes objects from it. ConfigMaps and Secrets do not: a pod's
// reference keeps them in the graph identity-only, which is all the
// scope check needs (and their changes come from the delta log, never
// from a List). Nodes do, one step removed: pods keep each Node as an
// identity, but the zone a Node sits in is read from its labels, so
// shared-zone neighbors drop out. Every other graph kind drops out
// whole. The RBAC kinds, ServiceAccounts and classes are no part of a
// neighborhood.
func changesUnverified(req state.ListRequirement) string {
	kinds := state.GraphKinds(req)
	switch {
	case len(kinds) == 0:
		return ""
	case req.Resource == "secrets" || req.Resource == "configmaps":
		return ""
	case req.Resource == "nodes":
		return "zones are read from Node labels, so neighbors reached only through a shared zone, and their changes, are out of scope"
	}
	return kinds[0].String() + " objects not referenced from a listed object are missing from the neighborhood, so their changes are out of scope"
}

// neighborhood maps every object in the target's radius (plus the
// target and its pods themselves) to its relation label — the scope
// filter for change records.
func neighborhood(snap *graph.Snapshot, id graph.NodeID, depth int) map[string]string {
	m := map[string]string{}
	add := func(ref graph.Ref, rel string) {
		m[refKey(ref.Kind.String(), ref.Namespace, ref.Name)] = rel
	}
	if ref, ok := snap.Resolve(id); ok {
		add(ref, "self")
	}
	for _, pid := range snap.PodsUnder(id) {
		if ref, ok := snap.Resolve(pid); ok {
			add(ref, "self")
		}
	}
	for _, nb := range bundle.RadiusNeighbors(snap, id, depth) {
		key := refKey(nb.Ref.Kind.String(), nb.Ref.Namespace, nb.Ref.Name)
		if _, taken := m[key]; !taken {
			m[key] = nb.Direction
		}
	}
	return m
}

// graphHistory is the slice of *store.Store placeDeleted reads.
type graphHistory interface {
	GraphAt(ctx context.Context, at time.Time) (*graph.Snapshot, error)
}

// placeDeleted adds to hood every object the window deleted that was
// in the target's neighborhood while it lived (#393).
//
// hood is resolved against one graph — the one at --at, or the live
// one — and an object deleted before that instant is by construction
// absent from it, so without this its delete record, and every add
// and update it made in the window, would fail the scope lookup and
// vanish. A post-mortem that silently omits a deletion reports a
// clean window over a destructive change.
//
// Each delete is placed against the graph as of the instant before
// it, which is the one graph that is certain to still hold the
// object; the window start is not, because an object created and
// deleted inside the window never appears there. The relation it
// takes is the one it had then. One GraphAt per delete that is not
// already placed, oldest first, and a graph read for one delete
// places every other pending delete it also holds — which in the
// common case of one rollout's worth of pods is a single read.
// Scoped to the target's namespace and to cluster-scoped objects,
// the only places a neighborhood reaches, so deletes elsewhere in
// the cluster cost nothing.
//
// A delete that predates the store's first snapshot, or one from a
// moment the target itself did not exist, is left unplaced: there is
// no graph that can say what it was to the target.
func placeDeleted(ctx context.Context, st graphHistory, rows []store.GraphChange, hood map[string]string, wl emit.WorkloadRef, depth int) error {
	pending := map[string]time.Time{}
	var order []string
	for _, rec := range rows {
		if rec.Op != "delete" || (rec.Namespace != "" && rec.Namespace != wl.Namespace) {
			continue
		}
		key := refKey(rec.Kind, rec.Namespace, rec.Name)
		if _, placed := hood[key]; placed {
			continue
		}
		if _, seen := pending[key]; !seen {
			order = append(order, key)
		}
		pending[key] = rec.At
	}
	for _, key := range order {
		at, ok := pending[key]
		if !ok {
			continue // placed by an earlier read
		}
		then, err := st.GraphAt(ctx, at.Add(-time.Nanosecond))
		if errors.Is(err, store.ErrNoHistory) {
			delete(pending, key)
			continue
		}
		if err != nil {
			return err
		}
		id, err := lookupTarget(then, wl, at)
		if err != nil {
			delete(pending, key)
			continue
		}
		for k, rel := range neighborhood(then, id, depth) {
			if _, want := pending[k]; want {
				hood[k] = rel
				delete(pending, k)
			}
		}
		delete(pending, key)
	}
	return nil
}

func refKey(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

// opReasons maps delta-log ops to finding reasons.
var opReasons = map[string]string{"add": "Added", "update": "Updated", "delete": "Deleted"}

// logEntries converts the delta-log rows that touch the neighborhood
// into change findings. Skipped on purpose: EndpointSlice records
// (pure pod-IP churn — the pod-level changes are the signal) and
// updates whose tracked fields did not change (zero nominal state).
func logEntries(rows []store.GraphChange, hood map[string]string) []changeEntry {
	var out []changeEntry
	for i, rec := range rows {
		rel, ok := hood[refKey(rec.Kind, rec.Namespace, rec.Name)]
		if !ok || rec.Kind == graph.KindEndpointSlice.String() {
			continue
		}
		if rec.Op == "update" && len(rec.FieldChanges) == 0 {
			continue
		}
		details := []emit.Field{
			{Key: "at", Value: rec.At.UTC().Format(time.RFC3339)},
			{Key: "relation", Value: rel},
			{Key: "origin", Value: "log"},
		}
		if fields := renderFieldChanges(rec.FieldChanges); fields != "" {
			details = append(details, emit.Field{Key: "fields", Value: fields})
		}
		out = append(out, changeEntry{
			at:  rec.At,
			seq: i,
			f: emit.Finding{
				Kind:         classify(rec),
				Severity:     emit.SeverityInfo,
				Namespace:    rec.Namespace,
				KindOfObject: rec.Kind,
				Name:         rec.Name,
				Reason:       opReasons[rec.Op],
				Details:      details,
			},
		})
	}
	return out
}

// The change classes. classify returns one of these whole rather
// than a suffix to concatenate: the emitter, the ledger, and the
// generated glossary then all read the same spelling, and a class
// cannot be added to the classifier without appearing in the ledger.
const (
	kindRollout  = "change.rollout"
	kindScale    = "change.scale"
	kindConfig   = "change.config"
	kindSecret   = "change.secret"
	kindNode     = "change.node"
	kindLabel    = "change.label"
	kindTopology = "change.topology"
)

// workloadChurnKinds classify adds/deletes of pod-bearing kinds as
// rollout churn.
var workloadChurnKinds = map[string]bool{
	"Pod": true, "Deployment": true, "ReplicaSet": true, "StatefulSet": true,
	"DaemonSet": true, "Job": true, "CronJob": true,
}

// classify buckets one delta-log record into the change class it is
// reported as, and returns that class's finding kind whole.
// ConfigMap/Secret/Node classify by
// kind; everything else by what changed: replicas → scale,
// image/mount references → rollout, labels alone → label; adds and
// deletes of pod-bearing kinds are rollout churn, of anything else
// (Service, Ingress, NetworkPolicy, PVC, …) topology.
func classify(rec store.GraphChange) string {
	switch rec.Kind {
	case "Node":
		return kindNode
	case "ConfigMap":
		return kindConfig
	case "Secret":
		return kindSecret
	}
	if rec.Op == "update" {
		class := ""
		for _, fc := range rec.FieldChanges {
			switch {
			case fc.Path == "replicas":
				return kindScale
			case strings.HasPrefix(fc.Path, "container/") || strings.HasPrefix(fc.Path, "mount/"):
				class = kindRollout
			case strings.HasPrefix(fc.Path, "label/") && class == "":
				class = kindLabel
			}
		}
		if class != "" {
			return class
		}
	}
	if workloadChurnKinds[rec.Kind] {
		return kindRollout
	}
	return kindTopology
}

// renderFieldChanges renders the changed-field summary compactly:
// path=from→to, comma-joined, content hashes shortened to 8 hex
// (they are change detectors, not payloads — §6.5).
func renderFieldChanges(fcs []graph.FieldChange) string {
	parts := make([]string, 0, len(fcs))
	for _, fc := range fcs {
		from, to := fc.From, fc.To
		if fc.Path == "data" {
			from, to = shortenHash(from), shortenHash(to)
		}
		parts = append(parts, fc.Path+"="+from+"→"+to)
	}
	return strings.Join(parts, ", ")
}

func shortenHash(h string) string {
	if len(h) > 8 {
		return h[:8]
	}
	return h
}

// rolloutEntries is the live approximation's rollout detector: a
// ReplicaSet in the neighborhood created inside the window is a
// rollout (the deployment controller creates one per template
// revision — the revision annotation and creation timestamp survive
// the rollout itself, unlike anything on the Deployment).
func rolloutEntries(cluster *state.Cluster, hood map[string]string, from, to time.Time) []changeEntry {
	var out []changeEntry
	for i, obj := range cluster.Objects() {
		rs, ok := obj.(*appsv1.ReplicaSet)
		if !ok {
			continue
		}
		rel, inHood := hood[refKey("ReplicaSet", rs.Namespace, rs.Name)]
		created := rs.CreationTimestamp.Time
		if !inHood || !created.After(from) || created.After(to) {
			continue
		}
		details := []emit.Field{
			{Key: "at", Value: created.UTC().Format(time.RFC3339)},
			{Key: "relation", Value: rel},
			{Key: "origin", Value: "api"},
		}
		if rev := rs.Annotations["deployment.kubernetes.io/revision"]; rev != "" {
			details = append(details, emit.Field{Key: "revision", Value: rev})
		}
		if cs := rs.Spec.Template.Spec.Containers; len(cs) > 0 {
			details = append(details, emit.Field{Key: "image", Value: cs[0].Image})
		}
		out = append(out, changeEntry{
			at:  created,
			seq: i,
			f: emit.Finding{
				Kind:         kindRollout,
				Severity:     emit.SeverityInfo,
				Namespace:    rs.Namespace,
				KindOfObject: "ReplicaSet",
				Name:         rs.Name,
				Reason:       "NewReplicaSet",
				Message:      "new template revision created inside the window",
				Details:      details,
			},
		})
	}
	return out
}

// scaleEventReasons are the event reasons that signal a rescale.
// SuccessfulRescale (the HPA's own record — the HPA object keeps no
// replica history, §5) is read in every mode; ScalingReplicaSet (the
// deployment controller's) only in the live approximation, where the
// delta log's replicas changes are not available to say it better.
var scaleEventReasons = map[string]bool{"SuccessfulRescale": true, "ScalingReplicaSet": false}

// eventEntries scans the namespace's events for rescale signals
// involving the neighborhood, inside the window. Returns the entries
// plus the number of events examined.
func eventEntries(ctx context.Context, client kubernetes.Interface, namespace string, hood map[string]string, from, to time.Time, approximation bool) ([]changeEntry, int, error) {
	list, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("listing events: %w", err)
	}
	var out []changeEntry
	for i := range list.Items {
		ev := &list.Items[i]
		wanted, known := scaleEventReasons[ev.Reason]
		if !known || (!wanted && !approximation) {
			continue
		}
		rel, inHood := hood[refKey(ev.InvolvedObject.Kind, ev.InvolvedObject.Namespace, ev.InvolvedObject.Name)]
		at := eventTime(ev)
		if !inHood || !at.After(from) || at.After(to) {
			continue
		}
		out = append(out, changeEntry{
			at:  at,
			seq: i,
			f: emit.Finding{
				Kind:         kindScale,
				Severity:     emit.SeverityInfo,
				Namespace:    ev.InvolvedObject.Namespace,
				KindOfObject: ev.InvolvedObject.Kind,
				Name:         ev.InvolvedObject.Name,
				Reason:       ev.Reason,
				Message:      ev.Message,
				Details: []emit.Field{
					{Key: "at", Value: at.UTC().Format(time.RFC3339)},
					{Key: "relation", Value: rel},
					{Key: "origin", Value: "event"},
				},
			},
		})
	}
	return out, len(list.Items), nil
}

// eventTime picks the most recent timestamp an Event carries (the
// same precedence `pkg/sources/k8sevents` uses: series, then
// lastTimestamp, then eventTime, then firstTimestamp).
func eventTime(ev *corev1.Event) time.Time {
	if ev.Series != nil && !ev.Series.LastObservedTime.IsZero() {
		return ev.Series.LastObservedTime.Time
	}
	if !ev.LastTimestamp.IsZero() {
		return ev.LastTimestamp.Time
	}
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time
	}
	return ev.FirstTimestamp.Time
}
