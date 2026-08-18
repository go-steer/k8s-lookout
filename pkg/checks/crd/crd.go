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

// Package crd is the read path's seam for detectors over API groups
// that may or may not be installed: Gateway API, OLM, KEDA, Kyverno.
//
// Every other check in pkg/checks reads built-in kinds through the
// typed clientset, where "is this served" is not a question. A CRD
// detector has three problems those checks do not, and this package
// answers all three the same way for everyone:
//
//  1. **Is the group served?** One discovery call, resolved through a
//     Resolver that caches per group — so a composition running
//     several CRD-gated checks pays one round trip per group, not one
//     per check.
//
//  2. **What does it say when the CRD is absent?** The same shape the
//     cloud group already settled on for an unavailable capability
//     (pkg/checks/cloudcheck): one explicit `crd.unavailable` info
//     finding, an `unavailable reason="…"` summary note, exit 0 with
//     scanned=0. A cluster without the CRDs gets an honest "nothing
//     was examined, here is why" rather than a clean bill of health
//     it did not earn (§2 "explicit, not broken", §11 no coverage
//     lies).
//
//  3. **How are the objects read?** Dynamically, as unstructured.
//     Taking a build-time dependency on the Gateway API, OLM, KEDA
//     and Kyverno Go modules to read a handful of status conditions
//     is a real cost against the dependency policy, and field access
//     through unstructured is only slightly uglier. The Nested*
//     helpers here keep that ugliness in one place.
//
// RBAC is the fourth question and its answer is a rule rather than
// code: an optional CRD read never enters
// state.LoadClusterListRequirements(), because that list is pinned by
// state/rbac_test.go to the shipped ClusterRole and would turn an
// optional read into an unconditional grant. CRD detectors do their
// own List pass, like `state volumes` and `state storage`.
package crd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Group is one optional API group a detector reads.
type Group struct {
	// Name is how the group is spoken about in messages ("Gateway
	// API"), not its DNS name.
	Name string
	// GV is the group-version discovery is asked about.
	GV schema.GroupVersion
	// Resources are the plural resource names the detector reads.
	// Discovery reports each one separately: a partially served group
	// is normal (an old Gateway API install serves gateways but not
	// grpcroutes), and a detector should degrade to the resources it
	// has rather than refuse.
	Resources []string
	// Install is the one-line hint appended to the unavailable
	// message — what the operator would do to make the check work.
	Install string
}

// Availability is the resolved answer for one Group.
type Availability struct {
	Group Group
	// Served and Missing partition Group.Resources; both are sorted.
	Served  []string
	Missing []string
	// Reason is empty when at least one resource is served, and
	// otherwise says why nothing could be read.
	Reason string
}

// Any reports whether anything at all can be read.
func (a Availability) Any() bool { return len(a.Served) > 0 }

// Serves reports whether one specific resource is served.
func (a Availability) Serves(resource string) bool {
	for _, r := range a.Served {
		if r == resource {
			return true
		}
	}
	return false
}

// Resolver answers "is this group served", caching per group-version.
//
// The cache is per Resolver, and a Resolver is built per invocation —
// deliberately. A process-wide cache would be wrong for the MCP
// server, which outlives CRD installs: a check would keep reporting
// unavailable for as long as the server ran. Per invocation, a single
// `lookout` run or a single MCP tool call pays exactly one discovery
// round trip per group however many checks it composes.
type Resolver struct {
	disc discovery.DiscoveryInterface

	mu    sync.Mutex
	cache map[string]Availability
}

// NewResolver builds a Resolver over a discovery client.
func NewResolver(disc discovery.DiscoveryInterface) *Resolver {
	return &Resolver{disc: disc, cache: map[string]Availability{}}
}

// Resolve returns g's availability, asking discovery at most once per
// group-version for the lifetime of this Resolver.
func (r *Resolver) Resolve(g Group) Availability {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a, ok := r.cache[g.GV.String()]; ok {
		// Same group-version, and the caller is asking about the same
		// resources: the cached partition already answers it.
		return a
	}
	a := discover(r.disc, g)
	r.cache[g.GV.String()] = a
	return a
}

// discover asks discovery which of g.Resources the group-version
// serves. A discovery error — which is what an absent group looks
// like — reads as "none served", with the error kept as the reason so
// an RBAC denial and a missing CRD do not read the same.
func discover(disc discovery.DiscoveryInterface, g Group) Availability {
	a := Availability{Group: g}
	if disc == nil {
		a.Missing = append([]string(nil), g.Resources...)
		sort.Strings(a.Missing)
		a.Reason = fmt.Sprintf("%s not checked: no discovery client", g.Name)
		return a
	}
	list, err := disc.ServerResourcesForGroupVersion(g.GV.String())
	if err != nil || list == nil {
		a.Missing = append([]string(nil), g.Resources...)
		sort.Strings(a.Missing)
		a.Reason = fmt.Sprintf("%s is not installed: the %s API group is not served by this cluster", g.Name, g.GV.String())
		if err != nil && !discovery.IsGroupDiscoveryFailedError(err) && !isNotFound(err) {
			a.Reason = fmt.Sprintf("%s could not be checked: discovery for %s failed: %v", g.Name, g.GV.String(), err)
		}
		return a
	}
	served := map[string]bool{}
	for _, res := range list.APIResources {
		served[res.Name] = true
	}
	for _, want := range g.Resources {
		if served[want] {
			a.Served = append(a.Served, want)
		} else {
			a.Missing = append(a.Missing, want)
		}
	}
	sort.Strings(a.Served)
	sort.Strings(a.Missing)
	if len(a.Served) == 0 {
		a.Reason = fmt.Sprintf("%s is not installed: %s serves none of %s",
			g.Name, g.GV.String(), strings.Join(g.Resources, ", "))
	}
	return a
}

// isNotFound recognises the 404 discovery returns for an absent group
// without importing apierrors for one call.
func isNotFound(err error) bool {
	return strings.Contains(err.Error(), "could not find the requested resource") ||
		strings.Contains(err.Error(), "the server could not find")
}

// EmitUnavailable writes the standard degradation record for a group
// that is not served: one crd.unavailable finding, the summary note,
// exit 0 with scanned=0. Call it and return its result directly.
func EmitUnavailable(inv emit.Invocation, a Availability) (int, error) {
	message := a.Reason
	if a.Group.Install != "" {
		message += " — " + a.Group.Install
	}
	if err := inv.Out.Emit(emit.Finding{
		Kind:     "crd.unavailable",
		Severity: emit.SeverityInfo,
		Reason:   "APIGroupNotServed",
		Message:  message,
		Details: []emit.Field{
			{Key: "api_group", Value: a.Group.GV.String()},
			{Key: "resources", Value: strings.Join(a.Group.Resources, ",")},
		},
	}); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("unavailable", a.Reason); err != nil {
		return 0, err
	}
	return 0, nil
}

// UnavailableFields are the output-glossary entries every CRD-gated
// command declares for the degradation path, so `--help` and the
// generated reference pages document it without each check restating
// it.
func UnavailableFields() []checks.OutputField {
	return []checks.OutputField{
		{Name: "api_group", Doc: "crd.unavailable: the API group-version this command needed"},
		{Name: "resources", Doc: "crd.unavailable: the resources it would have read"},
		{Name: "unavailable", Doc: "summary-line note: why the group could not be read (absent CRDs, or discovery denied)"},
	}
}

// PartialNote reports the resources a partially served group could
// not provide, as a summary note rather than a finding: the check did
// return real answers, and silently narrowing coverage would be the
// coverage lie §11 forbids. It is a no-op when nothing is missing.
func PartialNote(inv emit.Invocation, a Availability) error {
	if len(a.Missing) == 0 || !a.Any() {
		return nil
	}
	return inv.Out.Note("not_served", strings.Join(a.Missing, ","))
}

// The rest of this file is unstructured field access: the small set
// of readers a status-condition detector needs, named so a call site
// reads like the field path it is reaching for.

// Str reads a string at path, returning "" when absent or not a
// string. Every CRD field this package reads is optional by
// construction — a controller that has not reconciled yet has written
// none of them — so a missing field is a value, never an error.
func Str(obj map[string]any, path ...string) string {
	v, ok, err := unstructured.NestedString(obj, path...)
	if !ok || err != nil {
		return ""
	}
	return v
}

// Int reads an integer at path. The bool reports whether the field
// was present and numeric — callers need the distinction, because an
// absent port and port 0 mean different things.
func Int(obj map[string]any, path ...string) (int64, bool) {
	v, ok, err := unstructured.NestedInt64(obj, path...)
	if !ok || err != nil {
		return 0, false
	}
	return v, true
}

// Slice reads a list of objects at path, dropping any entry that is
// not an object. An absent list is an empty list.
func Slice(obj map[string]any, path ...string) []map[string]any {
	raw, ok, err := unstructured.NestedSlice(obj, path...)
	if !ok || err != nil {
		return nil
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// Map reads a nested object at path. An absent object is nil, which
// every reader in this file treats as empty — so a chain like
// Str(Map(o, "spec", "ref"), "name") is safe on a partially written
// object without a presence check at each hop.
func Map(obj map[string]any, path ...string) map[string]any {
	v, ok, err := unstructured.NestedMap(obj, path...)
	if !ok || err != nil {
		return nil
	}
	return v
}

// Condition is one entry of a standard metav1.Condition list, flattened.
type Condition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

// True reports whether the condition is affirmatively satisfied.
func (c Condition) True() bool { return c.Status == "True" }

// Conditions reads a metav1.Condition list at path. Kubernetes
// controllers write these identically across every API, which is why
// a detector can be CRD-agnostic about status even when it must be
// specific about spec.
func Conditions(obj map[string]any, path ...string) []Condition {
	entries := Slice(obj, path...)
	out := make([]Condition, 0, len(entries))
	for _, e := range entries {
		out = append(out, Condition{
			Type:    Str(e, "type"),
			Status:  Str(e, "status"),
			Reason:  Str(e, "reason"),
			Message: Str(e, "message"),
		})
	}
	return out
}

// FindCondition returns the condition of the given type and whether
// it was present. A condition a controller has not written yet is
// absent, not False — the difference is "no controller has looked at
// this" versus "a controller looked and said no", and only the second
// is a finding.
func FindCondition(conds []Condition, want string) (Condition, bool) {
	for _, c := range conds {
		if c.Type == want {
			return c, true
		}
	}
	return Condition{}, false
}

// ListPageSize matches the typed checks' paging (pkg/checks/state):
// large enough that a normal cluster is one round trip, small enough
// that a huge one does not build a single enormous response.
const ListPageSize = 500

// ListAll pages a dynamic List into a slice of unstructured items.
// Namespace "" lists across all namespaces; a cluster-scoped resource
// ignores it.
func ListAll(ctx context.Context, dyn dynamic.Interface, ns string, gvr schema.GroupVersionResource) ([]*unstructured.Unstructured, error) {
	ri := dynamic.ResourceInterface(dyn.Resource(gvr))
	if ns != "" {
		ri = dyn.Resource(gvr).Namespace(ns)
	}
	var out []*unstructured.Unstructured
	opts := metav1.ListOptions{Limit: ListPageSize}
	for {
		list, err := ri.List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", gvr.Resource, err)
		}
		for i := range list.Items {
			item := list.Items[i]
			out = append(out, &item)
		}
		opts.Continue = list.GetContinue()
		if opts.Continue == "" {
			return out, nil
		}
	}
}
