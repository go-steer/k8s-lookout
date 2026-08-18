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

package stab

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// driftKinds are the workload kinds `stab drift` scans: the
// GitOps-managed pod owners. (Jobs/CronJobs churn managers by design;
// Pods are owned by controllers, not the GitOps tool.)
var driftKinds = map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true}

const driftKindNames = "Deployment|StatefulSet|DaemonSet"

// managerMinShare is the floor an auto-detected GitOps manager must
// clear: a strict majority of every spec leaf field owned in scope.
// Not a tunable — `--manager` is already the escape hatch for a
// cluster whose real GitOps controller does not own half the fields,
// and a knob that lowers the bar for believing a guess is a knob for
// manufacturing drift.
const managerMinShare = 0.5

// The detection_reason values: why auto-detection resolved to nothing.
// Explicit sentinels (§2), never empty-and-silent.
const (
	// detectionNoSpecFields: nothing in scope owns any spec field —
	// an empty namespace, or objects the API server has no
	// managedFields for.
	detectionNoSpecFields = "no-spec-fields-in-scope"
	// detectionNoMajority: a leading candidate exists but does not
	// clear managerMinShare, so there is no manager to measure drift
	// against. The candidate and its share ride the summary.
	detectionNoMajority = "no-majority-manager"
)

// managerShare is one manager's fraction of every spec leaf field
// owned across the scanned objects.
func managerShare(ownedByManager, ownedTotal int) float64 {
	if ownedTotal <= 0 {
		return 0
	}
	return float64(ownedByManager) / float64(ownedTotal)
}

// formatShare renders a share as a rounded whole percent.
func formatShare(share float64) string {
	return fmt.Sprintf("%d%%", int(math.Round(share*100)))
}

// DriftCommand builds `lookout stab drift` (§5 tool matrix row,
// RESPECCED): out-of-band drift vs the GitOps manager, read from
// managedFields. The honesty constraint is structural, not stylistic:
// managedFields carries the MANAGER STRING (e.g. "kubectl-edit"),
// never who ran it — user identity lives in audit logs, and only
// `--identity` (the §5 identity query pack, #128) resolves it, via
// the provider's audit trail through the §2 boundary. Every surface
// of this command says which of the two it is reporting.
func DriftCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "stab drift",
		MCPName: "k8s_gitops_drift",
		Summary: "Find spec fields of Deployments/StatefulSets/DaemonSets owned by a manager other than the GitOps controller (managedFields) — out-of-band kubectl edits and rogue co-managers. Reports manager strings (tool names, not people); --identity additionally resolves each drift write to the audited principal via the cloud provider's audit trail (GKE Cloud Audit Logs), reporting an explicit unavailable on clusters without one. Default scope: all namespaces; scanned counts workload objects examined.",
		Flags: []emit.FlagSpec{
			{Name: "manager", Type: emit.FlagString, Default: "",
				Help: "the declared GitOps manager (e.g. argocd-controller); empty auto-detects it as the manager owning a strict majority (>50%) of the spec leaf fields summed across the scanned objects. No manager clears the majority — the usual shape of a cluster with no GitOps controller at all — and the scan resolves to detection=none and emits nothing rather than measuring drift against a guess; the summary then names the leading candidate (ties to the lexicographically smallest) and its share, to pass back here if it is in fact the GitOps controller"},
			{Name: "identity", Type: emit.FlagBool, Default: "false",
				Help: "resolve each finding's last drift write to the audited principal (who ran it) via the cloud provider's audit trail; requires a provider with the audit capability (GKE: Cloud Audit Logs admin-activity read), otherwise the summary line reports an explicit unavailable"},
		},
		Kinds: []checks.KindField{
			checks.Kind("drift.manual_edit", "a manager other than the GitOps controller owns spec fields on this object; critical when one of them is high blast radius (image, replicas, resources)", emit.SeverityCritical, emit.SeverityWarning),
		},
		Output: []checks.OutputField{
			{Name: "manager", Doc: "on findings: the foreign manager string from managedFields (a tool name like kubectl-edit — never a user identity; see --identity); on the summary line: the resolved GitOps manager"},
			{Name: "detection", Doc: "summary note: how the GitOps manager was resolved — declared (--manager), majority (auto-detected owner of >50% of the spec leaf fields in scope), or none (no manager resolved; nothing emitted)"},
			{Name: "detection_reason", Doc: "summary note on detection=none, naming why: no-spec-fields-in-scope (nothing in scope owns a spec field) or no-majority-manager (a leading candidate exists but owns 50% or less)"},
			{Name: "candidate", Doc: "summary note on detection=none/no-majority-manager: the leading manager that fell short of the majority — pass it to --manager if it is in fact the GitOps controller"},
			{Name: "share", Doc: "summary note: the resolved manager's (or, on detection=none, the candidate's) percentage of every spec leaf field owned across the scanned objects, rounded. A declared manager with a low share means most findings are other legitimate owners"},
			{Name: "operation", Doc: "managedFields operation of the foreign manager's last write: Apply or Update"},
			{Name: "tool", Doc: "client tool recognized from the manager string (kubectl for kubectl-edit/kubectl-patch/kubectl-*)"},
			{Name: "fields", Doc: "compact spec paths the foreign manager owns (e.g. spec.template.spec.containers[app].image), capped at 8 with a +N more tail"},
			{Name: "field_count", Doc: "total spec leaf fields the foreign manager owns on this object (uncapped)"},
			{Name: "age", Doc: "how long ago the foreign manager last wrote (managedFields time); omitted when the API server recorded no time"},
			{Name: "principal", Doc: "--identity: the audited principal of the write nearest the drift time (GKE: principalEmail), or the explicit sentinel none-in-audit-window / no-write-time-anchor when the trail cannot answer"},
			{Name: "principal_agent", Doc: "--identity: the caller-supplied client string of that write (a kubectl or controller user-agent), when the trail records one; caller-controlled text, display-only"},
			{Name: "other_principals", Doc: "--identity: other distinct principals that wrote the object inside the audit window, capped at 8 with a +N more tail"},
			{Name: "identity", Doc: "summary note when --identity could not be served: the §2 unavailable marker naming why (no provider / audit capability absent)"},
		},
		Examples: []string{
			"lookout stab drift",
			"lookout stab drift --namespace=prod --manager=argocd-controller",
			"lookout stab drift --workload=Deployment/prod/api --identity",
			"lookout stab drift --workload=Deployment/prod/api --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runDrift(ctx, deps, inv)
		},
	}
}

// driftObject is one scanned workload plus its per-manager spec
// ownership, aggregated across that manager's managedFields entries.
type driftObject struct {
	kind, namespace, name string
	owners                map[string]*specOwnership
}

// specOwnership aggregates one manager's spec-field ownership on one
// object. A manager can appear in several entries (an Apply plus an
// Update); paths union, operation/time follow the newest entry.
type specOwnership struct {
	paths   map[string]bool
	op      string
	last    metav1.Time // zero when the API server recorded no time
	hasTime bool
}

func runDrift(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	wl := inv.Scope.Workload
	if !wl.IsZero() {
		if !driftKinds[wl.Kind] {
			return 0, emit.UsageErrorf("--workload kind %q is not scanned by stab drift (want %s)", wl.Kind, driftKindNames)
		}
		if inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace {
			return 0, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
		}
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	// Empty scope means all namespaces, mirroring `triage delta`:
	// "who is editing around the GitOps tool" is a cluster question.
	ns := inv.Scope.Namespace
	if wl.Namespace != "" {
		ns = wl.Namespace
	}
	if ns == "" || inv.Scope.AllNamespaces {
		ns = metav1.NamespaceAll
	}

	objs, err := listDriftObjects(ctx, client, ns)
	if err != nil {
		return 0, err
	}
	if !wl.IsZero() {
		filtered := objs[:0]
		for _, o := range objs {
			if o.kind == wl.Kind && o.namespace == wl.Namespace && o.name == wl.Name {
				filtered = append(filtered, o)
			}
		}
		objs = filtered
	}

	// Resolve the GitOps manager: declared wins; otherwise the
	// manager owning a MAJORITY of the spec leaf fields summed across
	// the scanned objects. A scope owning no spec fields at all
	// resolves to nothing — detection=none, no findings, no guessing.
	totals := map[string]int{}
	owned := 0
	for _, o := range objs {
		for mgr, own := range o.owners {
			totals[mgr] += len(own.paths)
			owned += len(own.paths)
		}
	}
	if len(totals) == 0 {
		return len(objs), undetectedManager(inv, detectionNoSpecFields, "", 0)
	}
	manager, detection := inv.Flags.String("manager"), "declared"
	if manager == "" {
		// Ties break to the lexicographically smallest manager. Since
		// the floor below is a STRICT majority, a tie for the lead can
		// never be accepted — two managers tied at the top are 50% at
		// best. The tie-break therefore only decides which candidate
		// the detection=none summary names, which still has to be
		// deterministic run to run.
		for mgr, n := range totals {
			if manager == "" || n > totals[manager] || (n == totals[manager] && mgr < manager) {
				manager = mgr
			}
		}
		// The floor (#286). Without one the winner needed only to be
		// first past the other candidates, so on a cluster with no
		// GitOps controller at all the "manager" resolved to whatever
		// happened to own the most fields — kubectl-client-side-apply,
		// or a controller — and everything owned by anything else was
		// reported as drift against it. The check was most confidently
		// wrong exactly where it had the least evidence, and the note
		// called the result a majority when it was a plurality.
		//
		// Below the floor we resolve to none and emit nothing: the
		// same shape the no-spec-fields case already had for "we
		// cannot tell". The leading candidate and its share ride the
		// summary so the answer stays actionable — it is precisely
		// what to pass to --manager if it is in fact the right one.
		if share := managerShare(totals[manager], owned); share <= managerMinShare {
			return len(objs), undetectedManager(inv, detectionNoMajority, manager, share)
		}
		detection = "majority"
	}
	share := managerShare(totals[manager], owned)

	now := deps.now()
	var hits []driftHit
	for _, o := range objs {
		for mgr, own := range o.owners {
			if mgr == manager {
				continue
			}
			hits = append(hits, driftHit{f: driftFinding(o, mgr, own, now), obj: o, own: own})
		}
	}

	// Identity enrichment (--identity, §5 identity query pack #128):
	// resolve each finding's drift write to the audited principal via
	// the provider's audit trail. Capability absent → the findings
	// still emit (the portable read owes nothing to the cloud) and
	// the summary carries the §2 explicit unavailable marker; a real
	// backend failure fails the scan like any other read error.
	if inv.Flags.Bool("identity") {
		provider, err := deps.provider(ctx)
		if err != nil {
			return 0, err
		}
		if api, ok := provider.Audit(); ok {
			if err := enrichIdentity(ctx, api, hits); err != nil {
				return 0, err
			}
		} else if err := inv.Out.Note("identity", cloud.Unavailable(provider, cloud.CapabilityAudit).Marker()); err != nil {
			return 0, err
		}
	}

	findings := make([]emit.Finding, len(hits))
	for i, h := range hits {
		findings[i] = h.f
	}
	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("manager", manager); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("detection", detection); err != nil {
		return 0, err
	}
	// The share rides the accepted cases too. Under --manager it is
	// the one number that says whether the declared manager is really
	// running the show: a declared manager at share=3% means the
	// findings are mostly other legitimate owners, not drift.
	if err := inv.Out.Note("share", formatShare(share)); err != nil {
		return 0, err
	}
	return len(objs), nil
}

// undetectedManager writes the detection=none summary: the reason
// always, plus the leading candidate and its share when there was one.
// No findings are emitted — with no manager to measure against, every
// owner would be drift.
func undetectedManager(inv emit.Invocation, reason, candidate string, share float64) error {
	if err := inv.Out.Note("detection", "none"); err != nil {
		return err
	}
	if err := inv.Out.Note("detection_reason", reason); err != nil {
		return err
	}
	if candidate == "" {
		return nil
	}
	if err := inv.Out.Note("candidate", candidate); err != nil {
		return err
	}
	return inv.Out.Note("share", formatShare(share))
}

// driftHit pairs one rendered finding with the object and ownership
// it came from, so the identity enrichment can anchor its audit query
// on the drift write's time without re-parsing the finding.
type driftHit struct {
	f   emit.Finding
	obj driftObject
	own *specOwnership
}

// identitySlack is the audit-query half-window around the drift
// write's managedFields time: the API server stamps managedFields and
// the audit trail from the same request, so a generous slack only
// needs to absorb clock skew and entry batching, not real drift.
const identitySlack = 15 * time.Minute

// The principal sentinels: explicit values, never empty-and-silent
// (§2), when --identity ran but the trail could not answer for this
// finding.
const (
	// principalNoneInWindow: the audit query returned no writes in
	// the ±identitySlack window — retention expired, audit logging
	// disabled for the cluster, or the write predates the trail.
	principalNoneInWindow = "none-in-audit-window"
	// principalNoAnchor: the API server recorded no managedFields
	// time for the drift write, so there is nothing to anchor the
	// audit window on.
	principalNoAnchor = "no-write-time-anchor"
)

// driftAuditRefs maps the drift kinds to their audit REST identity.
// All three live in apps/v1; the namespace/name are stamped per hit.
var driftAuditRefs = map[string]cloud.AuditRef{
	"Deployment":  {APIGroup: "apps", Version: "v1", Resource: "deployments"},
	"StatefulSet": {APIGroup: "apps", Version: "v1", Resource: "statefulsets"},
	"DaemonSet":   {APIGroup: "apps", Version: "v1", Resource: "daemonsets"},
}

// enrichIdentity resolves each hit's drift write to the audited
// principal: the write nearest the managedFields time wins the
// `principal` field (plus its client string as `principal_agent`);
// every other distinct principal inside the window lands in
// `other_principals`. Findings the trail cannot answer for get an
// explicit sentinel, never silence.
func enrichIdentity(ctx context.Context, api cloud.AuditAPI, hits []driftHit) error {
	for i := range hits {
		h := &hits[i]
		if !h.own.hasTime {
			h.f.Details = append(h.f.Details, emit.Field{Key: "principal", Value: principalNoAnchor})
			continue
		}
		ref, ok := driftAuditRefs[h.obj.kind]
		if !ok {
			// A kind added to driftKinds but not here would otherwise
			// query a garbage filter and report a misleading
			// none-in-audit-window — fail loudly instead.
			return fmt.Errorf("stab drift: no audit REST identity for kind %q", h.obj.kind)
		}
		ref.Namespace, ref.Name = h.obj.namespace, h.obj.name
		anchor := h.own.last.Time
		writes, err := api.ObjectWrites(ctx, ref, cloud.TimeWindow{
			Start: anchor.Add(-identitySlack),
			End:   anchor.Add(identitySlack),
		})
		if err != nil {
			return err
		}
		if len(writes) == 0 {
			h.f.Details = append(h.f.Details, emit.Field{Key: "principal", Value: principalNoneInWindow})
			continue
		}
		nearest := writes[0]
		for _, w := range writes[1:] {
			if absDuration(w.Time.Sub(anchor)) < absDuration(nearest.Time.Sub(anchor)) {
				nearest = w
			}
		}
		h.f.Details = append(h.f.Details, emit.Field{Key: "principal", Value: nearest.Principal})
		if nearest.UserAgent != "" {
			h.f.Details = append(h.f.Details, emit.Field{Key: "principal_agent", Value: nearest.UserAgent})
		}
		var others []string
		seen := map[string]bool{nearest.Principal: true}
		for _, w := range writes {
			if !seen[w.Principal] {
				seen[w.Principal] = true
				others = append(others, w.Principal)
			}
		}
		if len(others) > 0 {
			sort.Strings(others)
			h.f.Details = append(h.f.Details, emit.Field{Key: "other_principals", Value: cappedList(others)})
		}
	}
	return nil
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// driftFinding renders one (object, foreign manager) pair. Message
// names the manager string, the drifted-field summary, and the age —
// and never claims a user identity itself (§5 respec); identity is
// exclusively the audited `principal` detail --identity appends.
func driftFinding(o driftObject, mgr string, own *specOwnership, now time.Time) emit.Finding {
	paths := make([]string, 0, len(own.paths))
	for p := range own.paths {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	severity := emit.SeverityWarning
	for _, p := range paths {
		if highBlastRadius(p) {
			severity = emit.SeverityCritical
			break
		}
	}
	reason, tool := "OutOfBandManager", ""
	if strings.HasPrefix(mgr, "kubectl") {
		reason, tool = "KubectlManualEdit", "kubectl"
	}

	details := []emit.Field{
		{Key: "manager", Value: mgr},
		{Key: "operation", Value: own.op},
	}
	if tool != "" {
		details = append(details, emit.Field{Key: "tool", Value: tool})
	}
	details = append(details,
		emit.Field{Key: "fields", Value: cappedList(paths)},
		emit.Field{Key: "field_count", Value: itoa(len(paths))},
	)
	var age string
	if own.hasTime {
		age = compactDuration(now.Sub(own.last.Time))
		details = append(details, emit.Field{Key: "age", Value: age})
	}

	summary := paths[0]
	if len(paths) > 1 {
		summary += fmt.Sprintf(" +%d more", len(paths)-1)
	}
	msg := fmt.Sprintf("manager %q (%s) owns %d drifted spec %s: %s",
		mgr, own.op, len(paths), plural(len(paths), "field"), summary)
	if age != "" {
		msg += ", last write " + age + " ago"
	}

	return emit.Finding{
		Kind:         "drift.manual_edit",
		Severity:     severity,
		Namespace:    o.namespace,
		KindOfObject: o.kind,
		Name:         o.name,
		Reason:       reason,
		Message:      msg,
		Details:      details,
	}
}

// highBlastRadius classifies the drifted spec paths that escalate a
// finding to critical: container image, replica count, and env —
// the field classes whose out-of-band edits take workloads down or
// silently fork configuration.
func highBlastRadius(path string) bool {
	return strings.HasSuffix(path, ".image") ||
		path == "spec.replicas" ||
		strings.Contains(path, ".env[") ||
		strings.HasSuffix(path, ".env")
}

// listDriftObjects lists the three workload kinds in ns and reduces
// each object's managedFields to per-manager spec ownership.
func listDriftObjects(ctx context.Context, client kubernetes.Interface, ns string) ([]driftObject, error) {
	var objs []driftObject
	add := func(kind, namespace, name string, entries []metav1.ManagedFieldsEntry) {
		objs = append(objs, driftObject{
			kind: kind, namespace: namespace, name: name,
			owners: specOwners(entries),
		})
	}
	steps := []func() error{
		func() error {
			return listPages("deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
				l, err := client.AppsV1().Deployments(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.Deployment) { add("Deployment", d.Namespace, d.Name, d.ManagedFields) })
		},
		func() error {
			return listPages("statefulsets", func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
				l, err := client.AppsV1().StatefulSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *appsv1.StatefulSet) { add("StatefulSet", s.Namespace, s.Name, s.ManagedFields) })
		},
		func() error {
			return listPages("daemonsets", func(o metav1.ListOptions) ([]appsv1.DaemonSet, string, error) {
				l, err := client.AppsV1().DaemonSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.DaemonSet) { add("DaemonSet", d.Namespace, d.Name, d.ManagedFields) })
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, err
		}
	}
	return objs, nil
}

// specOwners reduces managedFields entries to per-manager spec
// ownership. Status-only co-managers are the nominal state — entries
// touching a subresource (status/scale) or owning nothing under
// f:spec are ignored, so kube-controller-manager never shows up as
// drift.
func specOwners(entries []metav1.ManagedFieldsEntry) map[string]*specOwnership {
	owners := map[string]*specOwnership{}
	for _, e := range entries {
		if e.Subresource != "" || e.FieldsV1 == nil {
			continue
		}
		paths := specLeafPaths(e.FieldsV1.GetRawBytes())
		if len(paths) == 0 {
			continue
		}
		own := owners[e.Manager]
		if own == nil {
			own = &specOwnership{paths: map[string]bool{}}
			owners[e.Manager] = own
		}
		for _, p := range paths {
			own.paths[p] = true
		}
		// Operation/time track the manager's newest entry; entries
		// without a time only win when nothing timed exists yet.
		newer := e.Time != nil && (!own.hasTime || e.Time.After(own.last.Time))
		if own.op == "" || newer {
			own.op = string(e.Operation)
		}
		if newer {
			own.last, own.hasTime = *e.Time, true
		}
	}
	return owners
}

// specLeafPaths parses one FieldsV1 blob and returns the compact leaf
// paths owned under f:spec, sorted: "spec.replicas",
// "spec.template.spec.containers[app].image". Unparseable blobs yield
// nothing — this is a read of server-produced JSON, not user input.
func specLeafPaths(raw []byte) []string {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil
	}
	spec, ok := root["f:spec"].(map[string]any)
	if !ok {
		return nil
	}
	set := map[string]bool{}
	walkFields(spec, "spec", set)
	paths := make([]string, 0, len(set))
	for p := range set {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

// walkFields recurses one FieldsV1 subtree, collecting leaf paths.
// Keys are prefixed by kind: "f:" a field, "k:" a list item by merge
// key, "v:" a set item by value, "i:" a set item by index, and "."
// the this-field marker, which folds into its parent path.
func walkFields(node map[string]any, prefix string, out map[string]bool) {
	keys := make([]string, 0, len(node))
	for k := range node {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if k == "." {
			out[prefix] = true
			continue
		}
		path := fieldSegment(prefix, k)
		if child, ok := node[k].(map[string]any); ok && len(child) > 0 {
			walkFields(child, path, out)
			continue
		}
		out[path] = true
	}
}

// fieldSegment appends one FieldsV1 key to a rendered path.
func fieldSegment(prefix, key string) string {
	switch {
	case strings.HasPrefix(key, "f:"):
		return prefix + "." + key[2:]
	case strings.HasPrefix(key, "k:"):
		return prefix + "[" + listKey(key[2:]) + "]"
	case strings.HasPrefix(key, "v:"):
		return prefix + "[" + compactJSON(key[2:]) + "]"
	case strings.HasPrefix(key, "i:"):
		return prefix + "[" + key[2:] + "]"
	}
	// Unknown marker kinds stay visible rather than vanishing.
	return prefix + "." + key
}

// listKey renders a k:{...} merge key: the item's "name" alone when
// it has one (the overwhelmingly common case — containers, env,
// volumes), else the whole key object compacted.
func listKey(raw string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err == nil {
		if name, ok := m["name"].(string); ok {
			return name
		}
	}
	return compactJSON(raw)
}

// compactJSON strips JSON punctuation from a small key/value blob:
// {"containerPort":8080,"protocol":"TCP"} → containerPort:8080,protocol:TCP.
func compactJSON(raw string) string {
	return strings.NewReplacer("{", "", "}", "", `"`, "", " ", "").Replace(raw)
}
