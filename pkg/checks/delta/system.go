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

// The system class (§5, added by the health-check review): critical
// kube-system add-ons — DNS, kube-proxy, CNI, CSI — judged by
// desired-vs-ready. CoreDNS at 0/2 means the cluster is down while
// every workload object still claims Running; a generic rollout
// check would rank it like any lagging Deployment, so this class
// exists to name the role and raise the floor.

import (
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/emit"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// addonRoles maps well-known kube-system workload names (and their
// k8s-app label values) to the add-on role reported in the finding.
// The set spans upstream, GKE, EKS, AKS, and the common CNIs; the
// csi/cni substring fallbacks in addonRole cover driver DaemonSets
// we have not enumerated.
var addonRoles = map[string]string{
	// DNS
	"coredns":        "dns",
	"kube-dns":       "dns",
	"node-local-dns": "dns",
	// kube-proxy
	"kube-proxy": "proxy",
	// CNI / dataplane
	"calico-node":  "cni",
	"cilium":       "cni",
	"anetd":        "cni", // GKE Dataplane V2 (Cilium)
	"netd":         "cni", // GKE
	"aws-node":     "cni", // EKS VPC CNI
	"azure-cni":    "cni",
	"kindnet":      "cni",
	"kube-flannel": "cni",
	"weave-net":    "cni",
	// control-plane connectivity + metrics
	"konnectivity-agent": "connectivity",
	"metrics-server":     "metrics",
}

// addonRole classifies a kube-system workload; "" means "not a
// recognized critical add-on".
func addonRole(name string, labels map[string]string) string {
	for _, key := range []string{name, labels["k8s-app"], labels["app.kubernetes.io/name"]} {
		if key == "" {
			continue
		}
		if role, ok := addonRoles[key]; ok {
			return role
		}
		for prefix, role := range addonRoles {
			if strings.HasPrefix(key, prefix) {
				return role
			}
		}
		if strings.Contains(key, "csi") {
			return "csi"
		}
		if strings.Contains(key, "cni") {
			return "cni"
		}
	}
	return ""
}

// checkSystem derives addon.degraded from kube-system Deployments
// and DaemonSets that match a known add-on role. Objects outside
// kube-system are skipped, so callers can hand over an unfiltered
// scope-wide list.
func (s *scanner) checkSystem(deps []appsv1.Deployment, dss []appsv1.DaemonSet) {
	for i := range deps {
		d := &deps[i]
		if d.Namespace != metav1.NamespaceSystem {
			continue
		}
		role := addonRole(d.Name, d.Labels)
		if role == "" {
			continue
		}
		desired := int32(1)
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		s.addonFinding("Deployment", d.Namespace, d.Name, role, desired, d.Status.ReadyReplicas)
	}
	for i := range dss {
		d := &dss[i]
		if d.Namespace != metav1.NamespaceSystem {
			continue
		}
		role := addonRole(d.Name, d.Labels)
		if role == "" {
			continue
		}
		s.addonFinding("DaemonSet", d.Namespace, d.Name, role, d.Status.DesiredNumberScheduled, d.Status.NumberReady)
	}
}

// addonFinding emits when ready lags desired. Fully down is
// critical (the cluster's substrate is gone); partially degraded is
// a warning (one node's CNI pod restarting must not open an
// incident session).
func (s *scanner) addonFinding(kindOfObject, ns, name, role string, desired, ready int32) {
	if desired == 0 || ready >= desired {
		return
	}
	sev, reason := emit.SeverityWarning, "AddonDegraded"
	if ready == 0 {
		sev, reason = emit.SeverityCritical, "AddonUnavailable"
	}
	s.add(emit.Finding{
		Kind:         "addon.degraded",
		Severity:     sev,
		Namespace:    ns,
		KindOfObject: kindOfObject,
		Name:         name,
		Reason:       reason,
		Details: []emit.Field{
			{Key: "addon", Value: role},
			{Key: "desired", Value: itoa32(desired)},
			{Key: "ready", Value: itoa32(ready)},
		},
	})
}
