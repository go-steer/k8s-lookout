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
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// driftKinds are the workload kinds `stab drift` scans: the
// GitOps-managed pod owners. (Jobs/CronJobs churn managers by design;
// Pods are owned by controllers, not the GitOps tool.)
var driftKinds = map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true}

const driftKindNames = "Deployment|StatefulSet|DaemonSet"

// DriftCommand builds `lookout stab drift` (§5 tool matrix row,
// RESPECCED): out-of-band drift vs the GitOps manager, read from
// managedFields. The honesty constraint is structural, not stylistic:
// managedFields carries the MANAGER STRING (e.g. "kubectl-edit"),
// never who ran it — user identity lives in audit logs and ships as a
// later query pack, not here. Every surface of this command says so.
func DriftCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "stab drift",
		MCPName: "k8s_gitops_drift",
		Summary: "Find spec fields of Deployments/StatefulSets/DaemonSets owned by a manager other than the GitOps controller (managedFields) — out-of-band kubectl edits and rogue co-managers; reports manager strings only, never user identities (identity needs audit logs; later query pack). Default scope: all namespaces; scanned counts workload objects examined.",
		Flags: []emit.FlagSpec{
			{Name: "manager", Type: emit.FlagString, Default: "",
				Help: "the declared GitOps manager (e.g. argocd-controller); empty auto-detects it as the manager owning the most spec leaf fields summed across the scanned objects, ties broken to the lexicographically smallest"},
		},
		Output: []checks.OutputField{
			{Name: "manager", Doc: "on findings: the foreign manager string from managedFields (a tool name like kubectl-edit — never a user identity; that requires audit logs); on the summary line: the resolved GitOps manager"},
			{Name: "detection", Doc: "summary note: how the GitOps manager was resolved — declared (--manager), majority (auto-detected), or none (scope owns no spec fields)"},
			{Name: "operation", Doc: "managedFields operation of the foreign manager's last write: Apply or Update"},
			{Name: "tool", Doc: "client tool recognized from the manager string (kubectl for kubectl-edit/kubectl-patch/kubectl-*)"},
			{Name: "fields", Doc: "compact spec paths the foreign manager owns (e.g. spec.template.spec.containers[app].image), capped at 8 with a +N more tail"},
			{Name: "field_count", Doc: "total spec leaf fields the foreign manager owns on this object (uncapped)"},
			{Name: "age", Doc: "how long ago the foreign manager last wrote (managedFields time); omitted when the API server recorded no time"},
		},
		Examples: []string{
			"lookout stab drift",
			"lookout stab drift --namespace=prod --manager=argocd-controller",
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
	// manager owning the most spec leaf fields summed across the
	// scanned objects, ties to the lexicographically smallest. A
	// scope owning no spec fields at all resolves to nothing —
	// detection=none, no findings, no guessing.
	totals := map[string]int{}
	for _, o := range objs {
		for mgr, own := range o.owners {
			totals[mgr] += len(own.paths)
		}
	}
	if len(totals) == 0 {
		if err := inv.Out.Note("detection", "none"); err != nil {
			return 0, err
		}
		return len(objs), nil
	}
	manager, detection := inv.Flags.String("manager"), "declared"
	if manager == "" {
		detection = "majority"
		for mgr, n := range totals {
			if manager == "" || n > totals[manager] || (n == totals[manager] && mgr < manager) {
				manager = mgr
			}
		}
	}

	now := deps.now()
	var findings []emit.Finding
	for _, o := range objs {
		for mgr, own := range o.owners {
			if mgr == manager {
				continue
			}
			findings = append(findings, driftFinding(o, mgr, own, now))
		}
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
	return len(objs), nil
}

// driftFinding renders one (object, foreign manager) pair. Message
// names the manager string, the drifted-field summary, and the age —
// and never claims a user identity (§5 respec).
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
