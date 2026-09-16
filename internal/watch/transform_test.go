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

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/randfill"
)

// The guard on the shared-transform field registry.
//
// The obvious way to write this test is to declare the strip list as data and
// diff it against the registry. That only moves the drift up one level: the
// next person edits trimPod directly, the declared list is now a lie, and the
// test still passes. So this observes BEHAVIOUR instead. It fills a Pod and a
// Node so that every field is non-zero, runs the transform, and reflectively
// collects the JSON path of everything that actually changed. That set must
// equal the registry's stripped set exactly.
//
// Equality in both directions is the point:
//   - a field stripped with no registry entry fails (the case the design asked for)
//   - a registry entry for something the code no longer strips also fails, so
//     the justifications cannot rot into fiction

const fillSeed = 0x1ee7a7

// TestSharedTransform_StripsExactlyTheRegistry is the gate. See the comment
// above and transform_registry.go.
func TestSharedTransform_StripsExactlyTheRegistry(t *testing.T) {
	for _, tc := range []struct {
		kind string
		obj  func() any
		fn   func(any) (any, error)
	}{
		{"Pod", func() any { return fullPod(t) }, trimPod},
		{"Node", func() any { return fullNode(t) }, trimNode},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			before := tc.obj()
			after, err := tc.fn(tc.obj())
			if err != nil {
				t.Fatalf("transform returned an error: %v", err)
			}

			got := changedPaths(before, after)
			want := registryPaths(tc.kind, fieldStripped)

			for _, p := range diff(got, want) {
				t.Errorf("%s: the transform changes %q but transform_registry.go has no entry for it.\n"+
					"A field stripped from the shared cache is invisible to every other consumer, and "+
					"absent is indistinguishable from empty downstream. Add a fieldEntry saying why "+
					"nothing reads it — or stop stripping it.", tc.kind, p)
			}
			for _, p := range diff(want, got) {
				t.Errorf("%s: transform_registry.go claims %q is stripped, but the transform leaves it "+
					"untouched.\nEither the strip was removed and the entry is now a false claim about "+
					"what reaches other consumers, or the path no longer matches the field. Fix whichever "+
					"is wrong.", tc.kind, p)
			}
		})
	}
}

// TestSharedTransform_PreservesLoadBearingFields asserts the specific fields
// the registry says other consumers depend on survive the transform.
//
// This overlaps with the exact-match test above, which would already fail if a
// preserved field started being stripped. It is here for the failure message:
// this one names the consumer that breaks and the file:line that reads it,
// which is what someone editing trimPod at 2am actually needs.
func TestSharedTransform_PreservesLoadBearingFields(t *testing.T) {
	pod, err := trimPod(fullPod(t))
	if err != nil {
		t.Fatalf("trimPod: %v", err)
	}
	p := pod.(*corev1.Pod)

	c := p.Spec.Containers[0]
	if c.Env[0].Name == "" {
		t.Error("spec.containers[].env[].name was stripped; pkg/checks/state/wi.go:330 reads it")
	}
	if c.Env[0].ValueFrom == nil {
		t.Error("spec.containers[].env[].valueFrom was stripped; pkg/graph/derive.go:160 builds " +
			"ConfigMap and Secret edges from it, and would silently derive none")
	}
	if len(c.EnvFrom) == 0 {
		t.Error("spec.containers[].envFrom was stripped; pkg/graph/derive.go:169 reads it")
	}
	if len(p.Status.ContainerStatuses) == 0 {
		t.Error("status.containerStatuses was stripped; objectstate/podclearance.go:349, " +
			"rollout/rollout.go:501 and checks/delta/pods.go:98 all read it")
	}
	if len(p.Status.InitContainerStatuses) == 0 {
		t.Error("status.initContainerStatuses was stripped; rollout/rollout.go:501 reads it")
	}
	if p.Status.Phase == "" {
		t.Error("status.phase was stripped; objectstate.go:1220 reads Phase==Failed to detect evictions")
	}
	if p.Spec.EphemeralContainers[0].Name == "" {
		t.Error("spec.ephemeralContainers[].name was stripped; pkg/checks/logs/fetch.go:183 " +
			"enumerates it to decide which container logs it can fetch")
	}

	node, err := trimNode(fullNode(t))
	if err != nil {
		t.Fatalf("trimNode: %v", err)
	}
	n := node.(*corev1.Node)

	if n.Annotations["ccc_priority_index"] == "" {
		t.Error("the ccc_priority_index node annotation was dropped by the annotation filter; " +
			"docs/leeway-design.md §7.7.2 resolves compute-class preference rank from it, and " +
			"without it rank tracking silently reports nothing. Add it to retainedNodeAnnotations")
	}
	if !hasCondition(n.Status.Conditions, corev1.NodeReady) {
		t.Error("the Ready condition was dropped by the condition filter; node readiness gates " +
			"whether a domain counts as available")
	}
	if len(n.Status.Allocatable) == 0 {
		t.Error("status.allocatable was stripped; capacity weighting for node-group subjects reads it")
	}
	if len(n.Spec.Taints) == 0 {
		t.Error("spec.taints was stripped; eligible-domain computation reads it")
	}
}

// TestSharedTransform_StripsSecretEnvValues states the security goal directly
// rather than leaving it implied by a path in a table: no literal environment
// variable value survives, in any of the three container slices.
func TestSharedTransform_StripsSecretEnvValues(t *testing.T) {
	const secret = "s3cr3t-literal"

	pod := fullPod(t)
	pod.Spec.Containers[0].Env[0].Value = secret
	pod.Spec.InitContainers[0].Env[0].Value = secret
	pod.Spec.EphemeralContainers[0].Env[0].Value = secret

	out, err := trimPod(pod)
	if err != nil {
		t.Fatalf("trimPod: %v", err)
	}
	p := out.(*corev1.Pod)

	for _, tc := range []struct {
		where string
		got   string
	}{
		{"spec.containers[0]", p.Spec.Containers[0].Env[0].Value},
		{"spec.initContainers[0]", p.Spec.InitContainers[0].Env[0].Value},
		{"spec.ephemeralContainers[0]", p.Spec.EphemeralContainers[0].Env[0].Value},
	} {
		if tc.got != "" {
			t.Errorf("%s.env[0].value survived the transform as %q; secret values must never "+
				"enter the shared cache or a heap dump", tc.where, tc.got)
		}
	}
}

// TestSharedTransform_PassesThroughUnknownTypes covers the tombstone path. A
// DeletedFinalStateUnknown reaching a transform that type-asserts without the
// comma-ok would panic the whole watch process.
func TestSharedTransform_PassesThroughUnknownTypes(t *testing.T) {
	for _, fn := range []struct {
		name string
		f    func(any) (any, error)
	}{{"trimPod", trimPod}, {"trimNode", trimNode}} {
		in := &corev1.Service{}
		out, err := fn.f(in)
		if err != nil {
			t.Errorf("%s returned an error on an unexpected type: %v", fn.name, err)
		}
		if out != any(in) {
			t.Errorf("%s did not pass an unexpected type through unchanged", fn.name)
		}
	}
}

// TestSharedTransformRegistry_WellFormed checks the registry's own invariants.
// A stripped field claims nothing reads it, so listing a reader is a
// contradiction; a preserved field with no reader has no reason to be exempt
// from trimming and should be stripped instead.
func TestSharedTransformRegistry_WellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, e := range sharedTransformRegistry {
		key := e.Kind + " " + e.Path
		if seen[key] {
			t.Errorf("duplicate registry entry for %s", key)
		}
		seen[key] = true

		if e.Kind != "Pod" && e.Kind != "Node" {
			t.Errorf("%s: Kind must be Pod or Node, got %q", key, e.Kind)
		}
		if strings.TrimSpace(e.Why) == "" {
			t.Errorf("%s: Why is empty — the justification is the entire point of the entry", key)
		}
		switch e.Action {
		case fieldStripped:
			if len(e.Readers) > 0 {
				t.Errorf("%s: stripped but lists readers %v — stripping a field that something "+
					"reads is the exact failure this registry exists to prevent", key, e.Readers)
			}
		case fieldPreserved:
			if len(e.Readers) == 0 {
				t.Errorf("%s: preserved but names no reader — if nothing reads it, strip it", key)
			}
		default:
			t.Errorf("%s: unknown fieldAction %d", key, e.Action)
		}
	}
}

// ---------------------------------------------------------------- helpers

// fullPod returns a Pod with every field non-zero, so that a strip shows up as
// a change rather than being masked by a field that was already empty.
func fullPod(t *testing.T) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	filler().Fill(pod)

	// randfill leaves unexported fields alone and gives slices one element,
	// which is what we want, but a few fields need pinning so the test asserts
	// something specific rather than something random.
	pod.Status.Phase = corev1.PodFailed
	if len(pod.Spec.Containers) == 0 || len(pod.Spec.InitContainers) == 0 ||
		len(pod.Spec.EphemeralContainers) == 0 {
		t.Fatalf("filler produced a pod with an empty container slice; the test would "+
			"vacuously pass. containers=%d init=%d ephemeral=%d",
			len(pod.Spec.Containers), len(pod.Spec.InitContainers), len(pod.Spec.EphemeralContainers))
	}
	return pod
}

// fullNode returns a Node with every field non-zero and the two filtered
// collections seeded with both a retained and a dropped member, so the filters
// are exercised in both directions.
func fullNode(t *testing.T) *corev1.Node {
	t.Helper()
	node := &corev1.Node{}
	filler().Fill(node)

	node.Annotations = map[string]string{
		"ccc_priority_index":                "1",
		"container.googleapis.com/instance": "some large provider blob",
	}
	node.Status.Conditions = []corev1.NodeCondition{
		{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
	}
	if len(node.Status.Images) == 0 || len(node.Spec.Taints) == 0 {
		t.Fatal("filler produced a node with no images or no taints; the test would vacuously pass")
	}
	return node
}

// filler produces deterministic, fully-populated objects: a fixed seed so a
// failure reproduces, no nils so every field is observable, and exactly one
// element per slice so collapsed paths stay unambiguous.
func filler() *randfill.Filler {
	return randfill.NewWithSeed(fillSeed).NilChance(0).NumElements(1, 1)
}

func hasCondition(cs []corev1.NodeCondition, want corev1.NodeConditionType) bool {
	for i := range cs {
		if cs[i].Type == want {
			return true
		}
	}
	return false
}

func registryPaths(kind string, action fieldAction) map[string]bool {
	out := map[string]bool{}
	for _, e := range sharedTransformRegistry {
		// Entries keyed to a single map key or slice member — annotations
		// [ccc_priority_index], conditions[Ready] — document a member of a
		// filtered collection, not a field of their own. The collection itself
		// carries the stripped entry.
		if e.Kind == kind && e.Action == action && !strings.HasSuffix(e.Path, "]") {
			out[e.Path] = true
		}
	}
	return out
}

func diff(a, b map[string]bool) []string {
	var out []string
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// changedPaths reports every JSON path whose value the transform altered,
// with slice indices collapsed to "[]".
//
// It records the HIGHEST path at which a change is visible and stops
// descending there, so zeroing a whole struct reports "status.nodeInfo" rather
// than one path per leaf inside it. That is the granularity a registry entry
// should be written at.
func changedPaths(before, after any) map[string]bool {
	out := map[string]bool{}
	walk(reflect.ValueOf(before), reflect.ValueOf(after), "", out)
	return out
}

func walk(b, a reflect.Value, path string, out map[string]bool) {
	if !b.IsValid() || !a.IsValid() {
		if b.IsValid() != a.IsValid() {
			record(path, out)
		}
		return
	}
	if b.Type() != a.Type() {
		record(path, out)
		return
	}
	// Unchanged subtrees cannot contain a change; this prunes almost
	// everything, and incidentally keeps types with unexported fields
	// (metav1.Time, resource.Quantity) out of the recursion entirely.
	if reflect.DeepEqual(b.Interface(), a.Interface()) {
		return
	}

	switch b.Kind() {
	case reflect.Pointer, reflect.Interface:
		if b.IsNil() != a.IsNil() {
			record(path, out) // nilled wholesale — this is the strip
			return
		}
		walk(b.Elem(), a.Elem(), path, out)

	case reflect.Struct:
		// A struct replaced wholesale by its zero value is one strip, not one
		// per leaf inside it — status.nodeInfo is the case in point.
		if a.IsZero() && !b.IsZero() {
			record(path, out)
			return
		}
		for i := 0; i < b.NumField(); i++ {
			f := b.Type().Field(i)
			if f.PkgPath != "" {
				continue // unexported
			}
			name := jsonName(f)
			if name == "-" {
				continue
			}
			sub := path
			if name != "" { // "" means an inline/embedded struct: no path segment
				sub = join(path, name)
			}
			walk(b.Field(i), a.Field(i), sub, out)
		}

	case reflect.Slice, reflect.Array:
		// A length change means the collection was dropped or filtered, which
		// is the strip itself; descending would report its members instead.
		if b.Len() != a.Len() {
			record(path, out)
			return
		}
		for i := 0; i < b.Len(); i++ {
			walk(b.Index(i), a.Index(i), path+"[]", out)
		}

	case reflect.Map:
		if b.Len() != a.Len() {
			record(path, out)
			return
		}
		for _, k := range b.MapKeys() {
			av := a.MapIndex(k)
			if !av.IsValid() {
				record(path, out)
				return
			}
			walk(b.MapIndex(k), av, path, out)
		}

	default:
		record(path, out) // a leaf that differs
	}
}

func record(path string, out map[string]bool) {
	if path != "" {
		out[path] = true
	}
}

func join(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return fmt.Sprintf("%s.%s", prefix, name)
}

// jsonName returns the JSON path segment a field contributes, or "" for an
// embedded struct that is inlined into its parent.
//
// The empty-string case is not hypothetical: corev1.EphemeralContainer embeds
// EphemeralContainerCommon with the tag `json:""` — an empty VALUE, not an
// absent tag and not the `,inline` spelling used elsewhere. Keying off the tag
// alone yields paths like "spec.ephemeralContainers[].EphemeralContainerCommon
// .env[].value", which match no registry entry and no real JSON document, so
// the anonymous check has to come first.
func jsonName(f reflect.StructField) string {
	if name := strings.Split(f.Tag.Get("json"), ",")[0]; name != "" {
		return name
	}
	if f.Anonymous {
		return ""
	}
	return f.Name
}
