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

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func pendingFinding(ns string) emit.Finding {
	return emit.Finding{Kind: "pod.pending", Severity: emit.SeverityCritical, Namespace: ns, KindOfObject: "Pod", Name: "p", Reason: "Unschedulable"}
}

func autopilotCfg() cloud.ClusterConfig {
	return cloud.ClusterConfig{Name: "container-days-ap-demo", Autopilot: true}
}

func TestAutopilotIdle(t *testing.T) {
	system := []emit.Finding{pendingFinding("gke-gmp-system"), pendingFinding("kube-system"), {Kind: "addon.degraded", Severity: emit.SeverityCritical, Namespace: "kube-system"}, {Kind: "pdb.gridlocked", Severity: emit.SeverityWarning, Namespace: "kube-system"}}
	for _, tc := range []struct {
		name      string
		cfg       cloud.ClusterConfig
		findings  []emit.Finding
		nodes     []corev1.Node
		nodesRead bool
		want      bool
	}{
		{"idle", autopilotCfg(), system, nil, true, true},
		{"a user pod is pending", autopilotCfg(), append(system, pendingFinding("shop")), nil, true, false},
		{"nodes exist", autopilotCfg(), system, []corev1.Node{*healthyNode("n1")}, true, false},
		{"nodes were not read", autopilotCfg(), system, nil, false, false},
		{"a Standard cluster at 0 nodes is an outage", cloud.ClusterConfig{Name: "obi"}, system, nil, true, false},
		{"a cluster-scoped finding", autopilotCfg(), append(system, emit.Finding{Kind: "x", KindOfObject: "Node"}), nil, true, false},
	} {
		if got := autopilotIdle(tc.cfg, tc.findings, tc.nodes, tc.nodesRead); got != tc.want {
			t.Errorf("%s: idle = %t, want %t", tc.name, got, tc.want)
		}
	}
}

func TestGKEManagedNamespaceList(t *testing.T) {
	for ns, want := range map[string]bool{
		"kube-system": true, "kube-public": true, "kube-node-lease": true,
		"gke-gmp-system": true, "gke-managed-cim": true, "gmp-public": true,
		"default": false, "shop": false, "gkeish": false, "kube-systems": false,
	} {
		if got := GKEManagedNamespace(ns); got != want {
			t.Errorf("GKEManagedNamespace(%q) = %t, want %t", ns, got, want)
		}
	}
}

// TestTriageDeltaFoldsAnIdleAutopilotCluster: end to end, the system
// pods pending on an empty Autopilot cluster come out as one info
// finding; one user pod pending puts every finding back.
func TestTriageDeltaFoldsAnIdleAutopilotCluster(t *testing.T) {
	provider := func(context.Context) (cloud.Provider, error) {
		return scaleProvider{Provider: cloud.NoProvider, cfg: autopilotCfg()}, nil
	}
	run := func(objs ...runtime.Object) checktest.Result {
		client := fake.NewClientset(objs...)
		cmd := newCommand(func(context.Context) (kubernetes.Interface, error) { return client, nil }, func() time.Time { return testNow })
		cmd.Run = (&delta{source: func(context.Context) (kubernetes.Interface, error) { return client, nil }, now: func() time.Time { return testNow }, provider: provider}).run
		return checktest.Run(t, cmd)
	}

	res := run(pendingPod("gke-gmp-system", "alertmanager-0", 5000*time.Hour, true), pendingPod("kube-system", "kube-dns-1", 5000*time.Hour, true))
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "kind=cluster.autopilot_idle severity=info kind_of_object=Cluster name=container-days-ap-demo reason=AutopilotIdle") {
		t.Fatalf("want one idle finding and the summary:\n%s", res.Stdout)
	}
	for _, want := range []string{"its 2 system pod(s)", "folded=2", "namespaces=gke-gmp-system,kube-system"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("missing %q in %s", want, lines[0])
		}
	}
	if err := checktest.Verify(newCommand(nil, nil), res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("output contract: %v", err)
	}

	res = run(pendingPod("gke-gmp-system", "alertmanager-0", time.Hour, true), pendingPod("shop", "web-1", time.Hour, true))
	if strings.Contains(res.Stdout, "autopilot_idle") || strings.Count(res.Stdout, "kind=pod.pending") != 2 || !strings.Contains(res.Stdout, "kind=autoscaler.scaleup_blocked") {
		t.Errorf("a user pod pending must keep the incident path:\n%s", res.Stdout)
	}
}
