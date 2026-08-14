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

package state

// LimitRange awareness for the missing-request / missing-limit
// censuses (#235). Nothing else in the tree reads LimitRange; this is
// the one place that interprets it, so every check that must qualify
// a "no request configured" claim asks the same index.
//
// # What a defaulting LimitRange actually does, and when it matters
//
// LimitRanger is a MUTATING admission plugin: it writes its defaults
// into the pod spec at CREATE, and it never touches pods that already
// exist ("LimitRange validations occur only at Pod admission stage,
// not on running Pods. If you add or modify a LimitRange, the Pods
// that already exist in that namespace continue unchanged.").
//
// That splits the false-positive risk in two, and the split decides
// what a caller should do with this index:
//
//   - A check reading a LIVE POD (`triage top`) already sees the
//     defaults, because admission wrote them into the spec it is
//     reading. A live pod still missing a request in a defaulting
//     namespace is NOT a false positive — it genuinely has no
//     request, because it predates the LimitRange. Suppressing it
//     would hide a real, schedulable-today problem. The honest move
//     is to ANNOTATE: name the LimitRange, so the reader knows
//     recreating the pod is the fix.
//   - A check reading a POD TEMPLATE (a Deployment's
//     `.spec.template`, the shape posture detectors take) sees the
//     author's spec, which admission has NOT touched. There a
//     defaulting LimitRange means the pods really will get a value
//     and the finding really would be wrong — that is the case for
//     suppression.
//
// Both callers need the same question answered ("does this namespace
// default this dimension?"), so the index is shared and the verdict
// is the caller's.
//
// The apiserver's own defaulting is reproduced here (Default←Max,
// DefaultRequest←Default, DefaultRequest←Min, `type: Container` only)
// rather than assumed: a stored LimitRange normally arrives with
// those fields already filled in, but re-deriving costs nothing and
// keeps the index correct against hand-written fixtures too.

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// LimitRangeDefaults answers, per namespace, which compute dimensions
// a LimitRange supplies a default for at admission. The zero value is
// usable and answers "no" to everything — a caller that could not
// load LimitRanges degrades to today's unqualified behavior rather
// than to silence.
type LimitRangeDefaults struct {
	byNamespace map[string]*nsDefaults
}

// nsDefaults is one namespace's defaulting surface: which dimensions
// get a default limit, which get a default request, and the
// LimitRange name(s) responsible (for the annotation).
type nsDefaults struct {
	names   map[string]bool
	limit   map[string]bool
	request map[string]bool
}

// LoadLimitRanges lists LimitRanges in ns (metav1.NamespaceAll for the
// whole cluster) and builds the defaulting index. It returns the
// number of LimitRange objects read so the caller can add them to its
// scanned= count.
//
// This is a standalone List rather than a step in LoadCluster's pass:
// `triage top` reaches it on the namespace and -A paths, which never
// build a Cluster, and adding an eighteenth-plus resource to the
// strict LoadCluster pass would make every existing caller fail on a
// role that cannot list LimitRanges. A check that DOES hold a Cluster
// can pass the same index around; the interpretation lives here
// either way.
func LoadLimitRanges(ctx context.Context, client kubernetes.Interface, ns string) (*LimitRangeDefaults, int, error) {
	d := &LimitRangeDefaults{byNamespace: map[string]*nsDefaults{}}
	scanned := 0
	err := listPages("limitranges", func(o metav1.ListOptions) ([]corev1.LimitRange, string, error) {
		l, err := client.CoreV1().LimitRanges(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(lr *corev1.LimitRange) {
		scanned++
		d.add(lr)
	})
	if err != nil {
		return nil, 0, err
	}
	return d, scanned, nil
}

// add folds one LimitRange into the index.
func (d *LimitRangeDefaults) add(lr *corev1.LimitRange) {
	for _, item := range lr.Spec.Limits {
		// Only `type: Container` items default anything. Pod- and
		// PVC-typed items carry min/max constraints, which reject a
		// pod rather than complete it.
		if item.Type != corev1.LimitTypeContainer {
			continue
		}
		limit, request := effectiveDefaults(item)
		if len(limit) == 0 && len(request) == 0 {
			continue
		}
		ns := d.namespace(lr.Namespace)
		ns.names[lr.Name] = true
		for r := range limit {
			ns.limit[r] = true
		}
		for r := range request {
			ns.request[r] = true
		}
	}
}

func (d *LimitRangeDefaults) namespace(name string) *nsDefaults {
	if d.byNamespace == nil {
		d.byNamespace = map[string]*nsDefaults{}
	}
	n := d.byNamespace[name]
	if n == nil {
		n = &nsDefaults{names: map[string]bool{}, limit: map[string]bool{}, request: map[string]bool{}}
		d.byNamespace[name] = n
	}
	return n
}

// effectiveDefaults reproduces the apiserver's LimitRangeItem
// defaulting for a `type: Container` item, returning the dimension
// sets that end up with a default limit and a default request. The
// three fallbacks are the apiserver's, in its order: an unset default
// limit takes Max; an unset default request takes the default limit;
// an unset default request otherwise takes Min.
func effectiveDefaults(item corev1.LimitRangeItem) (limit, request map[string]bool) {
	limit, request = map[string]bool{}, map[string]bool{}
	for r := range item.Default {
		limit[string(r)] = true
	}
	for r := range item.Max {
		limit[string(r)] = true
	}
	for r := range item.DefaultRequest {
		request[string(r)] = true
	}
	for r := range limit {
		request[r] = true
	}
	for r := range item.Min {
		request[string(r)] = true
	}
	return limit, request
}

// DefaultsLimit reports whether ns has a LimitRange supplying a
// default LIMIT for resource ("cpu"/"memory"), and names the
// LimitRange(s) doing so.
func (d *LimitRangeDefaults) DefaultsLimit(ns, resource string) (string, bool) {
	return d.lookup(ns, resource, func(n *nsDefaults) map[string]bool { return n.limit })
}

// DefaultsRequest reports whether ns has a LimitRange supplying a
// default REQUEST for resource ("cpu"/"memory"), and names the
// LimitRange(s) doing so.
func (d *LimitRangeDefaults) DefaultsRequest(ns, resource string) (string, bool) {
	return d.lookup(ns, resource, func(n *nsDefaults) map[string]bool { return n.request })
}

func (d *LimitRangeDefaults) lookup(ns, resource string, dims func(*nsDefaults) map[string]bool) (string, bool) {
	if d == nil || d.byNamespace == nil {
		return "", false
	}
	n := d.byNamespace[ns]
	if n == nil || !dims(n)[resource] {
		return "", false
	}
	return n.Names(), true
}

// Names renders the namespace's LimitRange names, sorted and
// comma-joined. Two LimitRanges may both default a dimension and the
// apiserver does not define which wins, so the annotation names all
// of them rather than picking one.
func (n *nsDefaults) Names() string {
	out := make([]string, 0, len(n.names))
	for name := range n.names {
		out = append(out, name)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// String renders the index for debugging: namespaces in sorted order
// with the dimensions they default.
func (d *LimitRangeDefaults) String() string {
	if d == nil || len(d.byNamespace) == 0 {
		return "limitranges: none"
	}
	nss := make([]string, 0, len(d.byNamespace))
	for ns := range d.byNamespace {
		nss = append(nss, ns)
	}
	sort.Strings(nss)
	parts := make([]string, 0, len(nss))
	for _, ns := range nss {
		n := d.byNamespace[ns]
		parts = append(parts, fmt.Sprintf("%s(%s): limit=%s request=%s",
			ns, n.Names(), dimList(n.limit), dimList(n.request)))
	}
	return "limitranges: " + strings.Join(parts, "; ")
}

// dimList renders a dimension set for String(), "-" when empty.
func dimList(m map[string]bool) string {
	if len(m) == 0 {
		return "-"
	}
	return strings.Join(sortedKeys(m), ",")
}
