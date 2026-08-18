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

// Cluster is the exported seam over `state edges`' List pass: one
// paged List of the pod-nexus + RBAC kinds plus the pkg/graph
// snapshot built from it. `bundle` (§5) loads it once and feeds
// every section — spec, delta, edge validity, blast radius, log
// targets — from the same consistent read, per its one-List-pass
// rule.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// Cluster is one consistent List pass plus its topology snapshot.
type Cluster struct {
	ix   *index
	snap *graph.Snapshot
}

// ListRequirement is one (apiGroup, resource) LoadCluster's List pass
// reads with the `list` verb. Exported so RBAC surfaces that must
// support LoadCluster can be tested against the REAL requirement list
// instead of a hand-copied one: the sentinel's shipped ClusterRole
// (deploy/12-clusterrole-watcher.yaml) serves enrichment's scoped-list
// fallback (§7.6), which is exactly one LoadCluster pass — the M4
// drill found the role missing several of these and every enrichment
// failing at resolve (docs/milestones/M4.md §Observations, finding 2).
// A test parses the shipped YAML against this list so the two cannot
// drift again.
type ListRequirement struct {
	Group    string // "" for core
	Resource string // lowercase plural, e.g. "daemonsets"
}

// LoadClusterListRequirements returns every group/resource
// listCluster pages through, in list order. Keep in lockstep with the
// steps in listCluster — the deploy/12 RBAC test enforces the role
// side, and the §13 fake-clientset tests enforce the code side.
func LoadClusterListRequirements() []ListRequirement {
	return []ListRequirement{
		{"", "pods"},
		{"", "nodes"},
		{"apps", "deployments"},
		{"apps", "replicasets"},
		{"apps", "statefulsets"},
		{"apps", "daemonsets"},
		{"batch", "jobs"},
		{"batch", "cronjobs"},
		{"", "services"},
		{"discovery.k8s.io", "endpointslices"},
		{"networking.k8s.io", "ingresses"},
		{"networking.k8s.io", "ingressclasses"},
		{"", "configmaps"},
		{"", "secrets"},
		{"", "serviceaccounts"},
		{"rbac.authorization.k8s.io", "rolebindings"},
		{"rbac.authorization.k8s.io", "roles"},
		{"rbac.authorization.k8s.io", "clusterrolebindings"},
		{"rbac.authorization.k8s.io", "clusterroles"},
		{"storage.k8s.io", "storageclasses"},
	}
}

// String renders the requirement as "resource" (core group) or
// "resource.group" — the form the --lists flag and the skipped= note
// use.
func (r ListRequirement) String() string {
	if r.Group == "" {
		return r.Resource
	}
	return r.Resource + "." + r.Group
}

// loadOptions is the resolved configuration of one LoadCluster pass.
// The zero value is the original all-or-nothing behavior: every
// requirement selected, no tolerance, no preflight.
type loadOptions struct {
	// only, when non-nil, is the allow-set of resources to read (keyed
	// by ListRequirement); a nil map selects everything.
	only map[ListRequirement]bool
	// tolerate skips a step whose List returns Forbidden/NotFound
	// instead of aborting the pass.
	tolerate bool
	// preflight runs a SelfSubjectAccessReview per selected resource and
	// drops the denied ones before listing.
	preflight bool
}

// selects reports whether req is in scope for this pass.
func (o loadOptions) selects(req ListRequirement) bool {
	if o.only == nil {
		return true
	}
	return o.only[req]
}

// LoadOption configures a LoadCluster pass. The default (no options)
// is the strict, all-or-nothing load every non-bundle caller relies
// on; the bundle read paths (§5, §7.6) opt into partial loads.
type LoadOption func(*loadOptions)

// Tolerate makes the List pass skip any resource whose List returns
// Forbidden or NotFound — the least-privilege posture (§7.6): the
// caller reads what it can and Cluster.Skipped() names the gap. Any
// other error still aborts.
func Tolerate() LoadOption { return func(o *loadOptions) { o.tolerate = true } }

// Preflight adds a SelfSubjectAccessReview per selected resource so
// known-denied lists are dropped without a 403 in the logs; it falls
// back to Tolerate's reactive skip if SSAR itself is not permitted.
// Preflight only drops what it can prove is denied, so pair it with
// Tolerate to also catch races and SSAR gaps.
func Preflight() LoadOption { return func(o *loadOptions) { o.preflight = true } }

// Lists restricts the pass to the given requirements (the resolved
// value of the --lists flag). Passing every requirement is equivalent
// to no restriction. An empty/nil slice is a no-op (read everything) —
// so a caller that always passes Lists(parsed) degrades gracefully
// rather than reading nothing. Deselected resources are reported via
// Cluster.Skipped() exactly like a denied one.
func Lists(reqs []ListRequirement) LoadOption {
	return func(o *loadOptions) {
		if len(reqs) == 0 {
			o.only = nil
			return
		}
		o.only = make(map[ListRequirement]bool, len(reqs))
		for _, r := range reqs {
			o.only[r] = true
		}
	}
}

// ParseListSelection resolves a --lists flag value into the set of
// resources LoadCluster should read. Syntax (comma-separated,
// left-to-right): "all" expands to every LoadClusterListRequirements()
// entry; a bare resource name adds it; a "-" prefix removes it. So the
// default "all" reads everything, "all,-secrets" reads everything but
// Secrets, and "pods,deployments" is a bare allowlist. Names match a
// requirement's String() form ("secrets", "deployments",
// "endpointslices.discovery.k8s.io", …) or its bare resource when
// unambiguous. Unknown names error.
func ParseListSelection(spec string) ([]ListRequirement, error) {
	all := LoadClusterListRequirements()
	byName := make(map[string]ListRequirement, 2*len(all))
	bare := make(map[string][]ListRequirement, len(all))
	for _, r := range all {
		byName[r.String()] = r
		bare[r.Resource] = append(bare[r.Resource], r)
	}
	resolve := func(tok string) (ListRequirement, error) {
		if r, ok := byName[tok]; ok {
			return r, nil
		}
		switch rs := bare[tok]; len(rs) {
		case 1:
			return rs[0], nil
		case 0:
			return ListRequirement{}, fmt.Errorf("unknown list %q (want one of %s)", tok, listNames(all))
		default:
			return ListRequirement{}, fmt.Errorf("ambiguous list %q — qualify it as resource.group", tok)
		}
	}
	if strings.TrimSpace(spec) == "" {
		spec = "all" // an unset flag reads everything
	}
	set := map[ListRequirement]bool{}
	for _, tok := range strings.Split(spec, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if tok == "all" {
			for _, r := range all {
				set[r] = true
			}
			continue
		}
		remove := false
		if strings.HasPrefix(tok, "-") {
			remove, tok = true, strings.TrimSpace(tok[1:])
		}
		r, err := resolve(tok)
		if err != nil {
			return nil, err
		}
		if remove {
			delete(set, r)
		} else {
			set[r] = true
		}
	}
	// Return in canonical requirement order for stable skip accounting.
	out := make([]ListRequirement, 0, len(set))
	for _, r := range all {
		if set[r] {
			out = append(out, r)
		}
	}
	return out, nil
}

// listNames renders the requirement set for an error message.
func listNames(reqs []ListRequirement) string {
	names := make([]string, len(reqs))
	for i, r := range reqs {
		names[i] = r.String()
	}
	return strings.Join(names, ", ")
}

// canList asks the API server whether the caller may list group/resource
// cluster-wide (the preflight's per-resource SelfSubjectAccessReview).
func canList(ctx context.Context, client kubernetes.Interface, req ListRequirement) (bool, error) {
	review := &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Group:    req.Group,
				Resource: req.Resource,
				Verb:     "list",
			},
		},
	}
	resp, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return resp.Status.Allowed, nil
}

// LoadCluster runs the paged List pass over ns (metav1.NamespaceAll
// for the whole cluster) and builds the graph snapshot (§6.3 one-shot
// path). With no options the pass is all-or-nothing; Tolerate,
// Preflight, and Lists opt into a partial load whose gaps are reported
// by Cluster.Skipped().
func LoadCluster(ctx context.Context, client kubernetes.Interface, ns string, opts ...LoadOption) (*Cluster, error) {
	var o loadOptions
	for _, opt := range opts {
		opt(&o)
	}
	ix, err := listCluster(ctx, client, ns, o)
	if err != nil {
		return nil, err
	}
	g := graph.New(graph.Options{SwapInterval: -1})
	if err := g.Writer().FromObjects(slices.Values(ix.graphObjs)); err != nil {
		return nil, err
	}
	snap, err := g.Snapshot()
	if err != nil {
		return nil, err
	}
	return &Cluster{ix: ix, snap: snap}, nil
}

// Scanned is the number of objects the List pass returned — the
// caller's summary-line contribution.
func (c *Cluster) Scanned() int { return c.ix.scanned }

// Skipped returns the (group, resource) pairs this load did not read —
// deselected via Lists, denied by the SSAR preflight, or reactively
// dropped on a Forbidden/NotFound under Tolerate. Empty on a full
// pass. Consumers surface it (e.g. a skipped= note) so a partial
// bundle is never mistaken for a clean one.
func (c *Cluster) Skipped() []ListRequirement { return c.ix.skipped }

// SkippedNote renders Skipped() as the comma-separated value of a
// skipped= detail field (e.g. "secrets,configmaps"); "" when the load
// was complete.
func (c *Cluster) SkippedNote() string {
	if len(c.ix.skipped) == 0 {
		return ""
	}
	names := make([]string, len(c.ix.skipped))
	for i, r := range c.ix.skipped {
		names[i] = r.String()
	}
	return strings.Join(names, ",")
}

// Snapshot is the topology index built from the List pass.
func (c *Cluster) Snapshot() *graph.Snapshot { return c.snap }

// Pod returns the typed pod from the List pass (nil when not
// listed) — the same pointer the graph ingested, read-only by
// convention. `triage radius` uses it to report neighbor readiness,
// which the graph itself deliberately does not store (§6.5).
func (c *Cluster) Pod(namespace, name string) *corev1.Pod {
	return c.ix.pods[key(namespace, name)]
}

// Objects returns every object the List pass handed to the graph, in
// list order — the typed side of the topology for consumers that
// need fields the graph does not keep (e.g. `triage changes`' live
// approximation reading ReplicaSet revision annotations). Read-only
// by convention; the slice is shared, not copied.
func (c *Cluster) Objects() []any { return c.ix.graphObjs }

// WorkloadNode resolves wl to its graph node, requiring the object
// to have actually been observed (a merely-referenced identity is
// not a valid target).
func (c *Cluster) WorkloadNode(wl emit.WorkloadRef) (graph.NodeID, error) {
	kind, ok := workloadKinds[wl.Kind]
	if !ok {
		// A kind we do not support is the caller's mistake (exit 2);
		// a supported kind that was not observed is a lookup failure
		// (exit 1, below). Every CLI caller reaches this with a
		// user-supplied --workload or positional target.
		return graph.NoNode, emit.UsageErrorf("unsupported workload kind %q (want %s)", wl.Kind, workloadKindNames())
	}
	id, ok := c.snap.Lookup(kind, wl.Namespace, wl.Name)
	if ok {
		ref, resolved := c.snap.Resolve(id)
		ok = resolved && ref.Observed
	}
	if !ok {
		return graph.NoNode, fmt.Errorf("workload %s not found (%d objects listed)", wl, c.ix.scanned)
	}
	return id, nil
}

// TopWorkload rolls one finding's subject up to the workload an edge
// drill-down should target: the outermost controller that owns it
// (Pod → ReplicaSet → Deployment yields the Deployment), or the
// subject itself when nothing above it is a workload. It reports
// false for a kind `state edges` cannot target and for an object this
// List pass never observed — a drill-down on either would be a
// guaranteed lookup failure.
//
// `lookout scan` uses it so that twenty crashlooping pods of one
// Deployment become one drill-down rather than twenty identical ones.
func (c *Cluster) TopWorkload(kind, namespace, name string) (emit.WorkloadRef, bool) {
	nk, ok := workloadKinds[kind]
	if !ok {
		return emit.WorkloadRef{}, false
	}
	id, ok := c.snap.Lookup(nk, namespace, name)
	if !ok {
		return emit.WorkloadRef{}, false
	}
	if ref, resolved := c.snap.Resolve(id); !resolved || !ref.Observed {
		return emit.WorkloadRef{}, false
	}
	out := emit.WorkloadRef{Kind: kind, Namespace: namespace, Name: name}
	// OwnerChain is immediate-owner-first, so the last workload-kind
	// entry is the outermost controller.
	for _, owner := range c.snap.OwnerChain(id) {
		ref, resolved := c.snap.Resolve(owner)
		if !resolved || !ref.Observed {
			continue
		}
		if _, ok := workloadKinds[ref.Kind.String()]; !ok {
			continue
		}
		out = emit.WorkloadRef{Kind: ref.Kind.String(), Namespace: ref.Namespace, Name: ref.Name}
	}
	return out, true
}

// WorkloadPods resolves wl to its live pods via the graph's
// owner-chain traversal (PodsUnder), sorted by name.
func (c *Cluster) WorkloadPods(wl emit.WorkloadRef) ([]*corev1.Pod, error) {
	id, err := c.WorkloadNode(wl)
	if err != nil {
		return nil, err
	}
	var pods []*corev1.Pod
	for _, pid := range c.snap.PodsUnder(id) {
		ref, ok := c.snap.Resolve(pid)
		if !ok || !ref.Observed {
			continue
		}
		if p := c.ix.pods[key(ref.Namespace, ref.Name)]; p != nil {
			pods = append(pods, p)
		}
	}
	slices.SortFunc(pods, func(a, b *corev1.Pod) int {
		return strings.Compare(key(a.Namespace, a.Name), key(b.Namespace, b.Name))
	})
	return pods, nil
}

// WorkloadObject returns the typed API object behind wl from the
// List pass (nil when the identity was never listed). The caller
// gets the same pointer the graph ingested — read-only by
// convention.
func (c *Cluster) WorkloadObject(wl emit.WorkloadRef) any {
	match := func(m metav1.ObjectMeta, kind string) bool {
		return kind == wl.Kind && m.Namespace == wl.Namespace && m.Name == wl.Name
	}
	for _, o := range c.ix.graphObjs {
		switch t := o.(type) {
		case *corev1.Pod:
			if match(t.ObjectMeta, "Pod") {
				return t
			}
		case *appsv1.Deployment:
			if match(t.ObjectMeta, "Deployment") {
				return t
			}
		case *appsv1.ReplicaSet:
			if match(t.ObjectMeta, "ReplicaSet") {
				return t
			}
		case *appsv1.StatefulSet:
			if match(t.ObjectMeta, "StatefulSet") {
				return t
			}
		case *appsv1.DaemonSet:
			if match(t.ObjectMeta, "DaemonSet") {
				return t
			}
		case *batchv1.Job:
			if match(t.ObjectMeta, "Job") {
				return t
			}
		case *batchv1.CronJob:
			if match(t.ObjectMeta, "CronJob") {
				return t
			}
		}
	}
	return nil
}

// EdgeFindings runs the `state edges` validity checks for wl over
// this Cluster's objects and snapshot. certWarn <= 0 means the
// command's 720h default.
func (c *Cluster) EdgeFindings(wl emit.WorkloadRef, certWarn time.Duration, now time.Time) ([]emit.Finding, error) {
	id, err := c.WorkloadNode(wl)
	if err != nil {
		return nil, err
	}
	if certWarn <= 0 {
		certWarn = 720 * time.Hour
	}
	scan := &edgeScan{
		wl:       wl,
		ix:       c.ix,
		snap:     c.snap,
		id:       id,
		now:      now.UTC(),
		certWarn: certWarn,
	}
	return scan.run(), nil
}
