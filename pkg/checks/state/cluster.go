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
		{"", "configmaps"},
		{"", "secrets"},
		{"", "serviceaccounts"},
		{"rbac.authorization.k8s.io", "rolebindings"},
		{"rbac.authorization.k8s.io", "roles"},
		{"rbac.authorization.k8s.io", "clusterrolebindings"},
		{"rbac.authorization.k8s.io", "clusterroles"},
	}
}

// LoadCluster runs the paged List pass over ns (metav1.NamespaceAll
// for the whole cluster) and builds the graph snapshot (§6.3 one-shot
// path).
func LoadCluster(ctx context.Context, client kubernetes.Interface, ns string) (*Cluster, error) {
	ix, err := listCluster(ctx, client, ns)
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
		return graph.NoNode, fmt.Errorf("unsupported workload kind %q (want %s)", wl.Kind, workloadKindNames())
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
