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

package watch

// Enrichment (DESIGN.md §7.6): before injecting a per-incident
// session's first message, the sentinel runs the `lookout bundle`
// composition IN-PROCESS — the same pkg/checks code the CLI runs
// (§4.3 surface 3: no fork/exec), scoped to the affected object — and
// attaches the size-capped result to the initial inject payload. The
// session starts warm: the first 2–3 predictable tool calls are
// already answered.
//
// Two read paths, one composition:
//
//   - live (--storm on): the topology graph the graph feed maintains
//     and the shared informer caches already hold the owner chain,
//     the pods, and the blast radius — enrichment reuses them and
//     pays only one API GET (the workload object) plus the log
//     fetches. The edges section is NOT computed here: its validity
//     checks need the Service/EndpointSlice/RBAC index the live
//     informer set deliberately does not carry (§7.5 watches pods/
//     nodes/replicasets only), so the bundle instead carries an
//     `overflow section=edges cmd="lookout state edges …"` trailer —
//     the §4.4.4 posture: the inject itself teaches the next move.
//   - scoped fallback (--storm off, or the object is not in the live
//     topology yet): one namespace-scoped state.LoadCluster pass —
//     exactly what `lookout bundle` does — feeding all five sections.
//
// Failure honesty: enrichment NEVER blocks the inject. Every stage
// error becomes a schema-stable `enrichment_error stage=<s>
// error="…"` trailer plus enrichment_failures_total{stage}; whatever
// succeeded is still attached. The whole run is hard-capped by
// --enrich-timeout via context.
//
// Size cap (§15 Q3 — SESSION DECISION: fixed byte budget now, revisit
// with M2 telemetry): --enrich-cap (default 16KiB) bounds the head +
// section content. Truncation happens ONLY at section boundaries —
// the section order is fixed (spec, delta, edges, radius, logs), and
// the first section that does not fit drops it and every later one
// (a prefix cut, never a mid-line splice). Dropped and uncomputed
// sections become `overflow section=<s> cmd="lookout …"` trailers
// with real arguments; trailers (bounded, ≤ ~200B each) ride above
// the cap on purpose so even a tiny cap still teaches the follow-up
// commands.
//
// §6.5: every finding is rendered through an emit.Writer, whose
// DefaultSanitizer masks secret material on every Emit — there is no
// enrichment output path that bypasses it (the trailer lines carry
// only command strings and MaskString-ed error text).

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/checks/logs"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// Section names, in the fixed emission (and truncation) order. They
// mirror pkg/checks/bundle's sections — the enrichment bundle IS a
// bundle, so agents parse one shape.
const (
	enrichSectionSpec   = "spec"
	enrichSectionDelta  = "delta"
	enrichSectionEdges  = "edges"
	enrichSectionRadius = "radius"
	enrichSectionLogs   = "logs"
)

// stageResolve is the pre-section stage: target resolution + the
// scoped List pass. Its failure means no section can be computed.
const stageResolve = "resolve"

const (
	// enrichRadiusDepth matches `lookout bundle`'s --depth default.
	enrichRadiusDepth = 2
	// enrichMaxTemplates is the distilled-log template cap.
	// Deliberately tighter than bundle's 15: the enrichment rides
	// inside an inject, not a terminal. Not a flag — any incident
	// needing more runs `lookout triage logs` (the overflow trailer
	// says exactly that).
	enrichMaxTemplates = 10
	// enrichErrMax bounds one enrichment_error trailer's error text.
	enrichErrMax = 200
)

// enrichPolicies are the valid --enrich values: which severities get
// enrichment. "warning" includes critical (§7.7: severity is ordered).
var enrichPolicies = map[string]bool{"critical": true, "warning": true, "off": true}

// enricher runs the §7.6 stage. Constructed in realMain (per-incident
// mode, --enrich != off); nil on the dispatcher means the stage is
// disabled and un-enriched payloads stay byte-identical to M0.
type enricher struct {
	client kubernetes.Interface
	// logGetter streams one container's logs; nil means the client's
	// GetLogs subresource (tests inject fixture streams, §13).
	logGetter logs.PodLogGetter
	// snapshot yields the LIVE topology snapshot (graph feed, --storm
	// on); nil routes every enrichment through the scoped fallback.
	snapshot func() (*graph.Snapshot, error)
	// livePod reads one pod from the shared informer cache; nil (or a
	// cache miss) falls back to an API GET.
	livePod func(namespace, name string) (*corev1.Pod, error)
	now     func() time.Time
	metrics *metrics

	policy   string // critical | warning (off never constructs one)
	cap      int
	logLines int
	timeout  time.Duration
}

// enabledFor reports whether the policy enriches sev.
func (e *enricher) enabledFor(sev engine.Severity) bool {
	switch e.policy {
	case "warning":
		return sev == engine.SeverityCritical || sev == engine.SeverityWarning
	default: // "critical"
		return sev == engine.SeverityCritical
	}
}

// Incident builds the enrichment bundle for one new incident signal.
// Never returns an error: whatever failed is IN the returned string
// as enrichment_error trailers (§7.6 failure honesty).
func (e *enricher) Incident(ctx context.Context, sig engine.Signal) string {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	b := newEnrichBundle(e.cap)
	if e.snapshot == nil || !e.liveIncident(ctx, sig, b) {
		e.scopedIncident(ctx, sig, b)
	}
	return e.finish(b, fmt.Sprintf("%s/%s", sig.Namespace, sig.Name))
}

// Storm builds the (deliberately cheap) storm enrichment: the
// ancestor's blast radius from the live graph, radius only — no
// logs, no per-member reads. Storms only exist when the graph feed
// runs, so there is no scoped fallback here.
func (e *enricher) Storm(ctx context.Context, info engine.StormInfo) string {
	if e.snapshot == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	b := newEnrichBundle(e.cap)
	b.head = emit.Finding{
		Kind:         "bundle.target",
		Severity:     emit.SeverityInfo,
		Namespace:    info.Ancestor.Namespace,
		KindOfObject: info.Ancestor.Kind,
		Name:         info.Ancestor.Name,
		Details: []emit.Field{
			{Key: "ancestor", Value: info.Ancestor.Display()},
			{Key: "affected", Value: strconv.Itoa(info.AffectedCount)},
			{Key: "affected_namespaces", Value: strconv.Itoa(info.NamespaceCount)},
		},
	}
	snap, err := e.snapshot()
	if err != nil {
		b.fail(stageResolve, err)
		return e.finish(b, "storm "+info.Ancestor.Display())
	}
	kind, known := stormObjectKinds[info.Ancestor.Kind]
	var id graph.NodeID
	ok := known
	if known {
		id, ok = snap.Lookup(kind, info.Ancestor.Namespace, info.Ancestor.Name)
	}
	if !ok {
		b.fail(stageResolve, fmt.Errorf("storm ancestor %s not in the topology graph", info.Ancestor.Display()))
		return e.finish(b, "storm "+info.Ancestor.Display())
	}
	b.stage(ctx, enrichSectionRadius, ancestorRadiusCmd(info.Ancestor), func() ([]emit.Finding, error) {
		return bundle.RadiusFindings(snap, id, enrichRadiusDepth), nil
	})
	return e.finish(b, "storm "+info.Ancestor.Display())
}

// finish renders, records metrics, and logs one line.
func (e *enricher) finish(b *enrichBundle, what string) string {
	out, truncated := b.render()
	outcome := "ok"
	if len(b.errs) > 0 {
		outcome = "partial"
	}
	if b.computed == 0 {
		outcome = "failed"
	}
	if e.metrics != nil {
		e.metrics.enrichments.WithLabelValues(outcome).Inc()
		e.metrics.enrichmentBytes.Observe(float64(len(out)))
		if truncated {
			e.metrics.enrichmentTruncated.Inc()
		}
		for _, se := range b.errs {
			e.metrics.enrichmentFailures.WithLabelValues(se.stage).Inc()
		}
	}
	log.Printf("enrich %s: %dB (outcome=%s, sections=%d, truncated=%v, errors=%d)",
		what, len(out), outcome, b.computed, truncated, len(b.errs))
	return out
}

// liveWorkloadKinds are the graph kinds the live path accepts as an
// owner-chain target — the same set `bundle` targets. Unlike
// bundle's resolution the live walk accepts UNOBSERVED owners: the
// Deployment behind a ReplicaSet exists in the live graph only as a
// referenced identity (the §7.5 informer set watches pods/nodes/
// replicasets), and the object itself is fetched with one API GET.
var liveWorkloadKinds = map[graph.NodeKind]bool{
	graph.KindPod:         true,
	graph.KindDeployment:  true,
	graph.KindReplicaSet:  true,
	graph.KindStatefulSet: true,
	graph.KindDaemonSet:   true,
	graph.KindJob:         true,
	graph.KindCronJob:     true,
}

// liveIncident is the cheap path over the live topology + informer
// caches. Returns false — WITHOUT touching the builder — when the
// object cannot be resolved there (graph not ready yet, kind not in
// the topology, object unseen): the scoped fallback takes over.
// Correctness never depends on the live graph; it is an optimization
// of enrichment cost, exactly like §7.5's "correlation is an
// optimization of session count".
func (e *enricher) liveIncident(ctx context.Context, sig engine.Signal, b *enrichBundle) bool {
	snap, err := e.snapshot()
	if err != nil {
		return false
	}
	kind, ok := stormObjectKinds[sig.KindOfObject]
	if !ok {
		return false
	}
	id, ok := snap.Lookup(kind, sig.Namespace, sig.Name)
	if !ok {
		return false
	}
	wid, wl := liveTopOwner(snap, id, sig)

	// Pods from the shared informer cache — the same cache the graph
	// was built from, so a resolved pod node is a cache hit.
	var pods []*corev1.Pod
	for _, pid := range snap.PodsUnder(wid) {
		ref, ok := snap.Resolve(pid)
		if !ok || !ref.Observed {
			continue
		}
		if p, err := e.podByName(ctx, ref.Namespace, ref.Name); err == nil {
			pods = append(pods, p)
		}
	}
	slices.SortFunc(pods, func(a, b *corev1.Pod) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	b.setTarget(wl, len(pods))

	// spec + delta share the one API GET this path pays.
	var obj any
	b.stage(ctx, enrichSectionSpec, specCmd(wl), func() ([]emit.Finding, error) {
		o, err := e.workloadObject(ctx, wl)
		if err != nil {
			return nil, err
		}
		obj = o
		return checks.SpecFindings(wl.Kind, wl.Namespace, wl.Name, o)
	})
	b.stage(ctx, enrichSectionDelta, deltaCmd(wl), func() ([]emit.Finding, error) {
		return delta.ScanObjects(e.now(), delta.Config{}, bundle.DeltaObjectsFor(obj, pods)), nil
	})
	// edges: not computable from the live informer set (see file
	// comment) — the trailer names the command that computes it.
	b.skip(enrichSectionEdges, edgesCmd(wl))
	b.stage(ctx, enrichSectionRadius, radiusCmd(wl), func() ([]emit.Finding, error) {
		return bundle.RadiusFindings(snap, wid, enrichRadiusDepth), nil
	})
	b.stage(ctx, enrichSectionLogs, logsCmd(wl), func() ([]emit.Finding, error) {
		return e.distillFindings(ctx, pods)
	})
	return true
}

// scopedIncident is the fallback read path: one namespace-scoped
// state.LoadCluster pass (the CLI bundle's own List pass, §6.3
// one-shot) feeding all five sections.
func (e *enricher) scopedIncident(ctx context.Context, sig engine.Signal, b *enrichBundle) {
	ref := emit.WorkloadRef{Kind: sig.KindOfObject, Namespace: sig.Namespace, Name: sig.Name}
	var (
		cluster *state.Cluster
		wl      emit.WorkloadRef
		id      graph.NodeID
		pods    []*corev1.Pod
	)
	if err := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		c, err := state.LoadCluster(ctx, e.client, sig.Namespace)
		if err != nil {
			return err
		}
		w, err := bundle.ResolveIncidentTarget(c, ref, sig.ControllerRef)
		if err != nil {
			return err
		}
		i, err := c.WorkloadNode(w)
		if err != nil {
			return err
		}
		p, err := c.WorkloadPods(w)
		if err != nil {
			return err
		}
		cluster, wl, id, pods = c, w, i, p
		return nil
	}(); err != nil {
		b.fail(stageResolve, err)
		return
	}
	b.setTarget(wl, len(pods))

	b.stage(ctx, enrichSectionSpec, specCmd(wl), func() ([]emit.Finding, error) {
		obj := cluster.WorkloadObject(wl)
		if obj == nil {
			return nil, nil // identity referenced but never listed
		}
		return checks.SpecFindings(wl.Kind, wl.Namespace, wl.Name, obj)
	})
	b.stage(ctx, enrichSectionDelta, deltaCmd(wl), func() ([]emit.Finding, error) {
		return delta.ScanObjects(e.now(), delta.Config{}, bundle.DeltaObjectsFor(cluster.WorkloadObject(wl), pods)), nil
	})
	b.stage(ctx, enrichSectionEdges, edgesCmd(wl), func() ([]emit.Finding, error) {
		return cluster.EdgeFindings(wl, 0, e.now())
	})
	b.stage(ctx, enrichSectionRadius, radiusCmd(wl), func() ([]emit.Finding, error) {
		return bundle.RadiusFindings(cluster.Snapshot(), id, enrichRadiusDepth), nil
	})
	b.stage(ctx, enrichSectionLogs, logsCmd(wl), func() ([]emit.Finding, error) {
		return e.distillFindings(ctx, pods)
	})
}

// distillFindings runs the tail-limited log distillation over the
// target pods (--enrich-log-lines per container stream, template cap
// enrichMaxTemplates).
func (e *enricher) distillFindings(ctx context.Context, pods []*corev1.Pod) ([]emit.Finding, error) {
	getter := e.logGetter
	if getter == nil {
		getter = logs.ClientGetter(e.client)
	}
	var targets []logs.Target
	for _, p := range pods {
		targets = append(targets, logs.PodTargets(p)...)
	}
	_, fs, err := logs.Distill(ctx, getter, targets, logs.DistillOptions{
		Tail:         e.logLines,
		MaxTemplates: enrichMaxTemplates,
	})
	return fs, err
}

// podByName reads one pod: informer cache first, API GET fallback.
func (e *enricher) podByName(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if e.livePod != nil {
		if p, err := e.livePod(namespace, name); err == nil {
			return p, nil
		}
	}
	return e.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}

// workloadObject GETs the resolved target object — the live path's
// one API read.
func (e *enricher) workloadObject(ctx context.Context, wl emit.WorkloadRef) (any, error) {
	opts := metav1.GetOptions{}
	switch wl.Kind {
	case "Pod":
		return e.podByName(ctx, wl.Namespace, wl.Name)
	case "Deployment":
		return e.client.AppsV1().Deployments(wl.Namespace).Get(ctx, wl.Name, opts)
	case "ReplicaSet":
		return e.client.AppsV1().ReplicaSets(wl.Namespace).Get(ctx, wl.Name, opts)
	case "StatefulSet":
		return e.client.AppsV1().StatefulSets(wl.Namespace).Get(ctx, wl.Name, opts)
	case "DaemonSet":
		return e.client.AppsV1().DaemonSets(wl.Namespace).Get(ctx, wl.Name, opts)
	case "Job":
		return e.client.BatchV1().Jobs(wl.Namespace).Get(ctx, wl.Name, opts)
	case "CronJob":
		return e.client.BatchV1().CronJobs(wl.Namespace).Get(ctx, wl.Name, opts)
	}
	return nil, fmt.Errorf("unsupported workload kind %q", wl.Kind)
}

// liveTopOwner walks the live owner chain to the topmost
// workload-kind owner (observed or referenced — see
// liveWorkloadKinds), defaulting to the object itself.
func liveTopOwner(snap *graph.Snapshot, id graph.NodeID, sig engine.Signal) (graph.NodeID, emit.WorkloadRef) {
	chain := snap.OwnerChain(id)
	for i := len(chain) - 1; i >= 0; i-- {
		ref, ok := snap.Resolve(chain[i])
		if !ok || !liveWorkloadKinds[ref.Kind] {
			continue
		}
		return chain[i], emit.WorkloadRef{Kind: ref.Kind.String(), Namespace: ref.Namespace, Name: ref.Name}
	}
	return id, emit.WorkloadRef{Kind: sig.KindOfObject, Namespace: sig.Namespace, Name: sig.Name}
}

// Follow-up commands per section — REAL invocations with real
// arguments (§7.6: overflow keys reference lookout commands the
// agent can run itself; §4.4.4: the inject teaches by example).
func specCmd(wl emit.WorkloadRef) string { return "lookout triage spec --workload=" + wl.String() }
func deltaCmd(wl emit.WorkloadRef) string {
	return "lookout triage delta --namespace=" + wl.Namespace
}
func edgesCmd(wl emit.WorkloadRef) string  { return "lookout state edges --workload=" + wl.String() }
func radiusCmd(wl emit.WorkloadRef) string { return "lookout bundle --workload=" + wl.String() }
func logsCmd(wl emit.WorkloadRef) string {
	return "lookout triage logs --workload=" + wl.String() + " --namespace=" + wl.Namespace
}

// ancestorRadiusCmd names the follow-up for a storm ancestor's
// radius: `bundle` for workload ancestors; for the kinds bundle does
// not target, the closest real read.
func ancestorRadiusCmd(a engine.Ancestor) string {
	switch a.Kind {
	case "Node":
		return "lookout triage delta --only=nodes"
	case "Namespace":
		return "lookout triage delta --namespace=" + a.Name
	case "ConfigMap", "Secret", "PersistentVolumeClaim":
		return "lookout triage spec " + a.Kind + "/" + a.Namespace + "/" + a.Name
	}
	return "lookout bundle --workload=" + a.Kind + "/" + a.Namespace + "/" + a.Name
}

// ---------------------------------------------------------------------------
// Bundle builder: sections, cap, trailers
// ---------------------------------------------------------------------------

type enrichSection struct {
	name string
	body []byte // rendered lines; empty = computed, nothing abnormal
	cmd  string // the follow-up command reproducing this section
	// skipped marks a section deliberately not computed in-process
	// (live-path edges): overflow trailer, no error.
	skipped bool
}

type enrichStageErr struct {
	stage string
	err   error
}

// enrichBundle accumulates rendered sections and renders the final
// capped string.
type enrichBundle struct {
	cap      int
	head     emit.Finding
	secs     []enrichSection
	errs     []enrichStageErr
	computed int // sections computed successfully (drives the outcome label)
}

func newEnrichBundle(cap int) *enrichBundle {
	return &enrichBundle{cap: cap}
}

// setTarget writes the head finding: the same bundle.target shape
// `lookout bundle` opens with, so agents parse one format. The
// `sections` detail is appended at render time (it lists what
// actually shipped under the cap).
func (b *enrichBundle) setTarget(wl emit.WorkloadRef, pods int) {
	b.head = emit.Finding{
		Kind:         "bundle.target",
		Severity:     emit.SeverityInfo,
		Namespace:    wl.Namespace,
		KindOfObject: wl.Kind,
		Name:         wl.Name,
		Details: []emit.Field{
			{Key: "workload", Value: wl.String()},
			{Key: "pods", Value: strconv.Itoa(pods)},
		},
	}
}

// stage runs one section computation with the ctx budget guard,
// routing failure into an enrichment_error trailer — never out.
func (b *enrichBundle) stage(ctx context.Context, name, cmd string, fn func() ([]emit.Finding, error)) {
	if err := ctx.Err(); err != nil {
		b.fail(name, err)
		return
	}
	fs, err := fn()
	if err != nil {
		b.fail(name, err)
		return
	}
	body, err := renderEnrichFindings(name, fs)
	if err != nil {
		b.fail(name, err)
		return
	}
	b.computed++
	b.secs = append(b.secs, enrichSection{name: name, body: body, cmd: cmd})
}

// skip records a section deliberately not computed in-process; it
// renders as an overflow trailer naming cmd.
func (b *enrichBundle) skip(name, cmd string) {
	b.secs = append(b.secs, enrichSection{name: name, cmd: cmd, skipped: true})
}

// fail records one stage failure for the enrichment_error trailer.
func (b *enrichBundle) fail(stage string, err error) {
	b.errs = append(b.errs, enrichStageErr{stage: stage, err: err})
}

// render assembles head + sections under the cap (prefix cut at a
// section boundary) + the trailers, returning the final string and
// whether the cap dropped anything.
func (b *enrichBundle) render() (string, bool) {
	// Size the head against its longest possible `sections` value so
	// the include decision can never overshoot the cap.
	var allNames []string
	for _, s := range b.secs {
		if !s.skipped && len(s.body) > 0 {
			allNames = append(allNames, s.name)
		}
	}
	budget := b.cap - len(b.renderHead(allNames))

	truncated := false
	included := map[string]bool{}
	var names []string
	for _, s := range b.secs {
		if s.skipped || len(s.body) == 0 {
			continue
		}
		// Prefix cut: once one section does not fit, every later one
		// is dropped too — the bundle is always a clean prefix of the
		// canonical section order.
		if truncated || len(s.body) > budget {
			truncated = true
			continue
		}
		budget -= len(s.body)
		included[s.name] = true
		names = append(names, s.name)
	}

	var sb strings.Builder
	sb.Write(b.renderHead(names))
	for _, s := range b.secs {
		if included[s.name] {
			sb.Write(s.body)
		}
	}
	// Trailers, above the cap by design (bounded; see file comment).
	for _, s := range b.secs {
		if s.skipped || (len(s.body) > 0 && !included[s.name]) {
			sb.WriteString(trailerLine("overflow",
				emit.Field{Key: "section", Value: s.name},
				emit.Field{Key: "cmd", Value: s.cmd}))
		}
	}
	for _, se := range b.errs {
		msg := se.err.Error()
		if len(msg) > enrichErrMax {
			msg = msg[:enrichErrMax] + "…"
		}
		sb.WriteString(trailerLine("enrichment_error",
			emit.Field{Key: "stage", Value: se.stage},
			emit.Field{Key: "error", Value: emit.MaskString(msg)}))
	}
	return sb.String(), truncated
}

// renderHead renders the head finding with the given sections list
// appended (omitted when empty — zero nominal state).
func (b *enrichBundle) renderHead(sections []string) []byte {
	f := b.head
	if f.Kind == "" {
		return nil // resolution failed before a target existed
	}
	if len(sections) > 0 {
		f.Details = append(slices.Clone(f.Details),
			emit.Field{Key: "sections", Value: strings.Join(sections, ",")})
	}
	body, err := renderEnrichFindings("", []emit.Finding{f})
	if err != nil {
		return nil // cannot happen: head fields are static
	}
	return body
}

// renderEnrichFindings renders findings as logfmt lines through an
// emit.Writer — the §6.5 seam: DefaultSanitizer masks every finding,
// exactly as on stdout and MCP. section, when non-empty, is prepended
// as the first detail, mirroring `lookout bundle`.
func renderEnrichFindings(section string, fs []emit.Finding) ([]byte, error) {
	var buf strings.Builder
	w, err := emit.NewWriter(&buf, emit.FormatLogfmt)
	if err != nil {
		return nil, err
	}
	for _, f := range fs {
		if section != "" {
			f.Details = append([]emit.Field{{Key: "section", Value: section}}, f.Details...)
		}
		if err := w.Emit(f); err != nil {
			return nil, err
		}
	}
	return []byte(buf.String()), nil
}

// trailerLine renders one schema-stable trailer: a bare marker token
// (`overflow` / `enrichment_error`) followed by logfmt fields, quoted
// by the same rule pkg/emit applies.
func trailerLine(marker string, fields ...emit.Field) string {
	var sb strings.Builder
	sb.WriteString(marker)
	for _, f := range fields {
		sb.WriteByte(' ')
		sb.WriteString(f.Key)
		sb.WriteByte('=')
		sb.WriteString(quoteLogfmt(f.Value))
	}
	sb.WriteByte('\n')
	return sb.String()
}

// quoteLogfmt mirrors pkg/emit's logfmt quoting rule: quote only when
// the value would break splitting on spaces.
func quoteLogfmt(v string) string {
	needs := strings.ContainsAny(v, " =\"")
	if !needs {
		for _, r := range v {
			if r < 0x20 || r == 0x7f {
				needs = true
				break
			}
		}
	}
	if needs {
		return strconv.Quote(v)
	}
	return v
}
