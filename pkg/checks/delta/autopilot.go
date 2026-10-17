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

package delta

// An idle Autopilot cluster is not an outage (#735).
//
// An Autopilot cluster with no workloads of its own scales to zero nodes,
// by design, and GKE's own system pods then wait, Pending, for the first
// workload to bring a node up. Judged object by object that looks like
// the worst day a cluster can have: every system pod unschedulable for
// months, every add-on unavailable, rollouts past their deadline, a
// gridlocked PDB. A live answer headlined it "Urgent incident: down".
// It is one benign state, and is reported as one: an info finding about
// the cluster, counting what it explains, in place of the findings it
// explains.
//
// The recognition is narrow on purpose. It needs the provider to say
// the cluster is Autopilot, the Node list to have been read and be
// empty, and every finding to be in a GKE-managed namespace. One finding
// anywhere else (a user pod Pending, a user rollout short) keeps the
// whole incident path, scale-up explanation included.

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// KindAutopilotIdle is the idle-Autopilot finding's kind; scan folds the
// consequences it explains (GKE-managed webhooks without a backend).
const KindAutopilotIdle = "cluster.autopilot_idle"

// GKEManagedNamespace reports a namespace GKE creates and runs its own
// components in.
func GKEManagedNamespace(ns string) bool {
	switch ns {
	case "kube-system", "kube-public", "kube-node-lease":
		return true
	}
	return strings.HasPrefix(ns, "gke-") || strings.HasPrefix(ns, "gmp-")
}

// IdleWebhook reports a webhook finding the idle-Autopilot finding also
// explains: a GKE-managed webhook whose backend cannot run because the
// cluster has, by design, no node. A webhook backed by a Service outside
// the GKE-managed namespaces is never explained away.
func IdleWebhook(f emit.Finding) bool {
	if !strings.HasPrefix(f.Kind, "webhook.") {
		return false
	}
	for _, d := range f.Details {
		if d.Key == "service" {
			ns, _, _ := strings.Cut(d.Value, "/")
			return GKEManagedNamespace(ns)
		}
	}
	return false
}

// autopilotIdle reports an Autopilot cluster with no nodes whose every
// finding is in a GKE-managed namespace.
func autopilotIdle(cfg cloud.ClusterConfig, findings []emit.Finding, nodes []corev1.Node, nodesRead bool) bool {
	if !cfg.Autopilot || !nodesRead || len(nodes) != 0 || len(findings) == 0 {
		return false
	}
	for _, f := range findings {
		if !GKEManagedNamespace(f.Namespace) {
			return false
		}
	}
	return true
}

// foldAutopilotIdle replaces the findings with the one that explains
// them.
func foldAutopilotIdle(cfg cloud.ClusterConfig, findings []emit.Finding, pending int) []emit.Finding {
	var namespaces []string
	for _, f := range findings {
		if !slices.Contains(namespaces, f.Namespace) {
			namespaces = append(namespaces, f.Namespace)
		}
	}
	slices.Sort(namespaces)
	name := cfg.Name
	if name == "" {
		name = "cluster"
	}
	return []emit.Finding{{
		Kind:         KindAutopilotIdle,
		Severity:     emit.SeverityInfo,
		KindOfObject: "Cluster",
		Name:         name,
		Reason:       "AutopilotIdle",
		Message: fmt.Sprintf("Autopilot cluster with no workloads of its own: GKE has scaled it to 0 nodes, as designed, and its %d system pod(s) wait for the first workload to bring a node up. The %d finding(s) this replaces, in GKE-managed namespaces (%s), are that state, not faults; they clear when a workload is deployed",
			pending, len(findings), strings.Join(namespaces, ", ")),
		Details: []emit.Field{
			{Key: "pods", Value: strconv.Itoa(pending)},
			{Key: "nodes", Value: "0/0"},
			{Key: "autopilot", Value: "true"},
			{Key: "folded", Value: strconv.Itoa(len(findings))},
			{Key: "namespaces", Value: strings.Join(namespaces, ",")},
		},
	}}
}
