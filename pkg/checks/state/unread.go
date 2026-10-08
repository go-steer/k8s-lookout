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

// A forbidden read degrades, it does not fail (#546). The built-in
// `view` ClusterRole — the one many teams hand a read-only agent —
// deliberately excludes Secrets and every rbac.authorization.k8s.io
// object, and a strict List pass turned that one refusal into "no
// answer at all" for every command built on it. This file is the one
// seam that turns a refused List into an explicit, reasoned gap: the
// List pass records why each resource was skipped, the graph is told
// which kinds it could not observe (so an unread object is "unknown",
// never "missing"), and read.unavailable is the record a command emits
// for each gap that touches its answer — never silence.

import (
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// KindReadUnavailable is the degradation record for one resource a
// command's List pass could not read.
const KindReadUnavailable = checks.KindReadUnavailable

// UnreadKind is checks.UnreadKind, the ledger entry for
// KindReadUnavailable.
func UnreadKind() checks.KindField { return checks.UnreadKind() }

// UnreadFields is checks.UnreadFields.
func UnreadFields() []checks.OutputField { return checks.UnreadFields() }

// skipCause is why one requirement was not read.
type skipCause int

const (
	skipForbidden    skipCause = iota // RBAC refused the List (reactively or by SSAR preflight)
	skipNotServed                     // the API server does not serve the resource
	skipNotRequested                  // deselected via Lists (--lists)
)

func (c skipCause) reason() string {
	switch c {
	case skipNotServed:
		return "ListNotServed"
	case skipNotRequested:
		return "ListNotRequested"
	}
	return "ListForbidden"
}

func (c skipCause) describe(req ListRequirement) string {
	switch c {
	case skipNotServed:
		return "not served: list " + req.String()
	case skipNotRequested:
		return "not requested: list " + req.String()
	}
	return checks.Refused("list", req.Group, req.Resource).String()
}

// ListForbidden is checks.ListForbidden, kept here so state callers
// need not reach past their own package for it.
func ListForbidden(err error) (string, bool) { return checks.ListForbidden(err) }

// SkipReason says why this load did not read req — e.g. "forbidden:
// list secrets" — or "" when it was read.
func (c *Cluster) SkipReason(req ListRequirement) string {
	why, ok := c.ix.skipWhy[req]
	if !ok {
		return ""
	}
	return why.describe(req)
}

// RefusedKind reports the refusal that kept graph kind out of this
// load — the List that would have observed it was forbidden — so a
// target of that kind that "was not found" can say it was never
// looked for (#584).
func (c *Cluster) RefusedKind(kind graph.NodeKind) (checks.Refusal, bool) {
	for _, req := range c.ix.skipped {
		if c.ix.skipWhy[req] != skipForbidden {
			continue
		}
		for _, k := range listKinds[req] {
			if k == kind {
				return checks.Refused("list", req.Group, req.Resource), true
			}
		}
	}
	return checks.Refusal{}, false
}

// memberChain is, per workload kind, every graph kind on the
// owner-reference path from the workload down to its pods.
var memberChain = map[string][]graph.NodeKind{
	"Pod":         {graph.KindPod},
	"Deployment":  {graph.KindDeployment, graph.KindReplicaSet, graph.KindPod},
	"ReplicaSet":  {graph.KindReplicaSet, graph.KindPod},
	"StatefulSet": {graph.KindStatefulSet, graph.KindPod},
	"DaemonSet":   {graph.KindDaemonSet, graph.KindPod},
	"Job":         {graph.KindJob, graph.KindPod},
	"CronJob":     {graph.KindCronJob, graph.KindJob, graph.KindPod},
}

// MemberPodsRefused reports the refusal, if any, that leaves wl's
// member pods unresolvable: a forbidden List of a kind on the
// owner-reference path from wl to its pods (#584). A refused kind off
// that path does not touch the member set and reports false. A caller
// whose whole answer is the member pods fails on it; WorkloadPods
// alone would return an empty set that reads as "no pods".
func (c *Cluster) MemberPodsRefused(wl emit.WorkloadRef) (checks.Refusal, bool) {
	for _, k := range memberChain[wl.Kind] {
		if r, ok := c.RefusedKind(k); ok {
			return r, true
		}
	}
	return checks.Refusal{}, false
}

// UnreadFindings renders one read.unavailable record per skipped
// resource that matters to the caller. affects names, for a skipped
// requirement, what the caller could not verify without it ("Secret
// key references … not verified"); returning "" means the gap does not
// touch this caller's answer and gets no record. lead is prepended to
// every record's details (`state edges` stamps workload= on every
// finding it emits). Records come out in requirement order.
func (c *Cluster) UnreadFindings(affects func(ListRequirement) string, lead ...emit.Field) []emit.Finding {
	var out []emit.Finding
	for _, req := range c.ix.skipped {
		what := affects(req)
		if what == "" {
			continue
		}
		why := c.ix.skipWhy[req]
		details := append(append([]emit.Field(nil), lead...), emit.Field{Key: "resource", Value: req.String()})
		out = append(out, emit.Finding{
			Kind:     KindReadUnavailable,
			Severity: emit.SeverityInfo,
			Reason:   why.reason(),
			Message:  why.describe(req) + " — " + what,
			Details:  details,
		})
	}
	return out
}

// listKinds maps each LoadCluster requirement to the graph node kinds
// its List observes. Requirements absent here (RBAC objects, classes,
// ServiceAccounts) are indexed only, never graph kinds.
var listKinds = map[ListRequirement][]graph.NodeKind{
	{"", "pods"}:                           {graph.KindPod, graph.KindContainer},
	{"", "nodes"}:                          {graph.KindNode, graph.KindZone},
	{"apps", "deployments"}:                {graph.KindDeployment},
	{"apps", "replicasets"}:                {graph.KindReplicaSet},
	{"apps", "statefulsets"}:               {graph.KindStatefulSet},
	{"apps", "daemonsets"}:                 {graph.KindDaemonSet},
	{"batch", "jobs"}:                      {graph.KindJob},
	{"batch", "cronjobs"}:                  {graph.KindCronJob},
	{"", "services"}:                       {graph.KindService},
	{"discovery.k8s.io", "endpointslices"}: {graph.KindEndpointSlice},
	{"networking.k8s.io", "ingresses"}:     {graph.KindIngress},
	{"", "configmaps"}:                     {graph.KindConfigMap},
	{"", "secrets"}:                        {graph.KindSecret},
}

// GraphKinds reports the graph node kinds req's List observes (nil for
// an index-only requirement such as the RBAC kinds).
func GraphKinds(req ListRequirement) []graph.NodeKind { return listKinds[req] }

// watchedKinds is the graph's WatchedKinds for a load that skipped
// these requirements: every kind but the ones a skipped List would
// have observed. Nil when nothing graph-relevant was skipped, which
// keeps a full load's snapshot exactly as it was (watches everything).
// Without it a pod's reference to an unread Secret would resolve to an
// unobserved node of a "watched" kind, and blast radius would report
// the Secret as ReferencedNotFound — a missing object invented from a
// permission gap.
func watchedKinds(skipped []ListRequirement) []graph.NodeKind {
	drop := map[graph.NodeKind]bool{}
	for _, req := range skipped {
		for _, k := range listKinds[req] {
			drop[k] = true
		}
	}
	if len(drop) == 0 {
		return nil
	}
	var out []graph.NodeKind
	for _, k := range graph.AllKinds() {
		if !drop[k] {
			out = append(out, k)
		}
	}
	return out
}

// edgeUnverified names what `state edges --workload=<workload>` could
// not verify without each requirement. Nodes feed only the graph's
// placement edges, which edge validity never reads.
func edgeUnverified(req ListRequirement) string {
	switch req.Resource {
	case "nodes":
		return ""
	case "secrets":
		return "Secret references (env, envFrom and volume keys, imagePullSecrets, Ingress TLS secrets and their certificate expiry) not verified"
	case "configmaps":
		return "ConfigMap references (env, envFrom and volume keys) not verified"
	case "serviceaccounts":
		return "the ServiceAccount reference and the imagePullSecrets it contributes not verified"
	case "rolebindings":
		return "RoleBindings naming the workload's ServiceAccount, and their roleRefs, not verified"
	case "clusterrolebindings":
		return "ClusterRoleBindings naming the workload's ServiceAccount, and their roleRefs, not verified"
	case "roles":
		return "RoleBinding roleRefs to Roles not verified"
	case "clusterroles":
		return "roleRefs to ClusterRoles not verified"
	case "services":
		return "Service selector, endpoint and StatefulSet governing-Service edges not verified"
	case "endpointslices":
		return "Service endpoint edges not verified"
	case "ingresses":
		return "Ingress backend, class and TLS edges not verified"
	case "ingressclasses":
		return "Ingress class edges not verified"
	case "storageclasses":
		return "StatefulSet volumeClaimTemplate storage classes not verified"
	}
	return "edges that depend on " + req.String() + " not verified"
}

// serviceEdgeUnverified is edgeUnverified for the Service-entry pass,
// which checks the Service's selector, endpoints, Ingresses and their
// certificates and nothing workload-scoped — so ConfigMap,
// ServiceAccount and RBAC gaps do not touch its answer.
func serviceEdgeUnverified(req ListRequirement) string {
	switch req.Resource {
	case "secrets":
		return "Ingress TLS secret references and their certificate expiry not verified"
	case "pods", "services", "endpointslices", "ingresses", "ingressclasses":
		return edgeUnverified(req)
	case "deployments", "replicasets", "statefulsets", "daemonsets", "jobs", "cronjobs":
		return "likely_workload attribution did not consider " + req.String()
	}
	return ""
}
