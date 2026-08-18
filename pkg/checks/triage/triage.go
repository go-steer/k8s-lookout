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

// Package triage implements the graph-backed commands of the
// `lookout triage` group that consume §6.6 history: `triage radius`
// (blast radius, live or point-in-time via --at) and `triage changes`
// (what changed in the window before onset, from the delta log the
// sentinel writes). Both answer from a *graph.Snapshot — live ones
// built by state.LoadCluster's one List pass, historical ones
// reconstructed by store.GraphAt — and both say WHICH they answered
// from in the summary line (source=live|history|live-approximation),
// per §6.6's "answer live-only and say so".
package triage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func init() {
	checks.Register(RadiusCommand(Deps{}))
	checks.Register(ChangesCommand(Deps{}))
	checks.Register(StatusCommand())
}

// Deps are the injectable dependencies of the triage graph commands.
// The zero value gives production behavior; tests inject a fake
// clientset and a fixed clock.
type Deps struct {
	// Client builds the Kubernetes client for live-mode reads. Nil
	// means kube.DefaultSource. Never called in --at mode: a
	// point-in-time question is answered entirely from the store, so
	// post-mortems work without cluster access.
	Client kube.ClientSource
	// Now is the clock anchoring lookback windows. Nil means time.Now.
	Now func() time.Time
}

func (d Deps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.DefaultSource()(ctx)
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// workloadKinds are the target kinds both commands accept: the
// pod-owning workload kinds plus Pod itself — the same set `state
// edges` and `bundle` support. Keys are lowercase; values canonical.
var workloadKinds = map[string]string{
	"pod": "Pod", "pods": "Pod", "po": "Pod",
	"deployment": "Deployment", "deployments": "Deployment", "deploy": "Deployment",
	"replicaset": "ReplicaSet", "replicasets": "ReplicaSet", "rs": "ReplicaSet",
	"statefulset": "StatefulSet", "statefulsets": "StatefulSet", "sts": "StatefulSet",
	"daemonset": "DaemonSet", "daemonsets": "DaemonSet", "ds": "DaemonSet",
	"job": "Job", "jobs": "Job",
	"cronjob": "CronJob", "cronjobs": "CronJob", "cj": "CronJob",
}

// graphKinds maps canonical workload kind names to graph node kinds.
var graphKinds = map[string]graph.NodeKind{
	"Pod":         graph.KindPod,
	"Deployment":  graph.KindDeployment,
	"ReplicaSet":  graph.KindReplicaSet,
	"StatefulSet": graph.KindStatefulSet,
	"DaemonSet":   graph.KindDaemonSet,
	"Job":         graph.KindJob,
	"CronJob":     graph.KindCronJob,
}

func workloadKindNames() string {
	seen := map[string]bool{}
	var names []string
	for _, v := range workloadKinds {
		if !seen[v] {
			seen[v] = true
			names = append(names, v)
		}
	}
	sort.Strings(names)
	return strings.Join(names, "|")
}

// targetDoc is the shared positional documentation.
const targetDoc = "the pod or workload at the center of the question: <Kind>/<namespace>/<name>, " +
	"<Kind>/<name> (namespace from --namespace, else \"default\"), or a bare pod name; " +
	"kinds are case-insensitive with the usual short forms (po, deploy, rs, sts, ds, cj). " +
	"--workload=<Kind>/<ns>/<name> is the flag-shaped alternative."

// parseTarget merges the positional argument and --workload into one
// canonical target reference, usage-checking the combination.
func parseTarget(inv emit.Invocation) (emit.WorkloadRef, error) {
	wl := inv.Scope.Workload
	var pos emit.WorkloadRef
	if len(inv.Args) == 1 {
		p, err := parsePositional(inv.Args[0], inv.Scope.Namespace)
		if err != nil {
			return emit.WorkloadRef{}, emit.UsageErrorf("%v", err)
		}
		pos = p
	}
	switch {
	case pos.IsZero() && wl.IsZero():
		return emit.WorkloadRef{}, emit.UsageErrorf("no target: pass <Kind>/<namespace>/<name> (or a bare pod name) or --workload=<Kind>/<namespace>/<name>")
	case !pos.IsZero() && !wl.IsZero():
		return emit.WorkloadRef{}, emit.UsageErrorf("give the target either positionally or via --workload, not both")
	case pos.IsZero():
		canonical, ok := workloadKinds[strings.ToLower(wl.Kind)]
		if !ok {
			return emit.WorkloadRef{}, emit.UsageErrorf("unsupported workload kind %q (want %s)", wl.Kind, workloadKindNames())
		}
		wl.Kind = canonical
		if inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace {
			return emit.WorkloadRef{}, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
		}
		return wl, nil
	default:
		return pos, nil
	}
}

// parsePositional parses the target argument forms documented in
// targetDoc.
func parsePositional(arg, namespace string) (emit.WorkloadRef, error) {
	fallbackNS := namespace
	if fallbackNS == "" {
		fallbackNS = "default"
	}
	parts := strings.Split(arg, "/")
	for _, p := range parts {
		if p == "" {
			return emit.WorkloadRef{}, emit.UsageErrorf("invalid target %q (want <Kind>/[<namespace>/]<name> or a bare pod name)", arg)
		}
	}
	switch len(parts) {
	case 1:
		return emit.WorkloadRef{Kind: "Pod", Namespace: fallbackNS, Name: parts[0]}, nil
	case 2, 3:
		kind, ok := workloadKinds[strings.ToLower(parts[0])]
		if !ok {
			return emit.WorkloadRef{}, emit.UsageErrorf("unsupported workload kind %q (want %s)", parts[0], workloadKindNames())
		}
		if len(parts) == 2 {
			return emit.WorkloadRef{Kind: kind, Namespace: fallbackNS, Name: parts[1]}, nil
		}
		if namespace != "" && namespace != parts[1] {
			return emit.WorkloadRef{}, emit.UsageErrorf("--namespace=%s contradicts target namespace %s", namespace, parts[1])
		}
		return emit.WorkloadRef{Kind: kind, Namespace: parts[1], Name: parts[2]}, nil
	default:
		return emit.WorkloadRef{}, fmt.Errorf("invalid target %q (want <Kind>/[<namespace>/]<name> or a bare pod name)", arg)
	}
}

// lookupTarget resolves the target on any snapshot — live or
// point-in-time (at is the resolved --at instant, zero for live).
// Observed objects always resolve. For kinds the snapshot's ingest
// never watches, an identity-only node still resolves when its owner
// chain connects it into the topology: the sentinel graph feed holds
// Deployments only through ReplicaSet ownerReferences, so a
// historical Deployment target is answerable via its Owns edges to
// the observed RS/pods (M3 drill observation 3) — refusing it would
// force every post-mortem to re-derive the owner chain by hand. A
// disconnected identity (or one of a watched kind that is genuinely
// absent) is not a valid target; the historical error says the
// object was not in the WATCHED topology at the asked instant, which
// is the actionable statement (try the ReplicaSet/Pod, or a time the
// object existed).
func lookupTarget(snap *graph.Snapshot, wl emit.WorkloadRef, at time.Time) (graph.NodeID, error) {
	kind := graphKinds[wl.Kind]
	if id, ok := snap.Lookup(kind, wl.Namespace, wl.Name); ok {
		if ref, resolved := snap.Resolve(id); resolved {
			if ref.Observed {
				return id, nil
			}
			// Identity-only fallback: unwatched kind, but the owner
			// chain (or any other edge) places it in the topology.
			if !snap.Watches(kind) && (len(snap.Out(id)) > 0 || len(snap.In(id)) > 0) {
				return id, nil
			}
		}
	}
	if !at.IsZero() {
		return graph.NoNode, fmt.Errorf(
			"workload %s was not in the watched topology as of %s — the sentinel's graph feed may hold this kind only through owner references; target the workload's ReplicaSet or a Pod, or pick an instant the object existed",
			wl, at.UTC().Format(time.RFC3339))
	}
	return graph.NoNode, fmt.Errorf("workload %s not found in the topology", wl)
}

// historicalSnapshot opens the sentinel store read-only and resolves
// the topology as of at (§6.6 nearest-snapshot + replay-forward).
func historicalSnapshot(ctx context.Context, path string, at time.Time) (*graph.Snapshot, error) {
	st, err := store.OpenRead(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
	return st.GraphAt(ctx, at)
}

// isPodReady reports the PodReady condition.
func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
