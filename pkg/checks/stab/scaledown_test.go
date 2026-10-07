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

package stab_test

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// sdNode is a 4-CPU / 16Gi node.
func sdNode(name string, mut ...func(*corev1.Node)) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("16Gi"),
		}},
	}
	for _, m := range mut {
		m(n)
	}
	return n
}

// requesting sets one container's CPU/memory requests.
func requesting(cpu, mem string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
			Name: "c",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(mem),
			}},
		})
	}
}

func safeToEvict(v string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": v}
	}
}

// scaledownCluster has one node per case. The ones that must report:
// n-pinned (gridlocked PDB + bare pod, 30% CPU), n-annot
// (safe-to-evict=false, on a named pool), n-gpu (25% of its GPUs,
// whatever its CPU says) and n-mem (memory-bound, raised by a one-shot
// init container). The ones that must not: n-busy (75%, not
// underused), n-free (underused, nothing pinning it), n-released (its
// bare pod is annotated safe-to-evict=true), and the three nodes the
// autoscaler never removes.
func scaledownCluster() []runtime.Object {
	gridlock := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-pdb"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
		Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, CurrentHealthy: 1, DesiredHealthy: 1},
	}
	gpuNode := sdNode("n-gpu", func(n *corev1.Node) {
		n.Status.Allocatable["nvidia.com/gpu"] = resource.MustParse("4")
	})
	gpuPod := drainPod("ml", "trainer", "n-gpu", requesting("3500m", "2Gi"))
	gpuPod.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
	initPod := drainPod("batch", "loader", "n-mem", requesting("100m", "1Gi"), func(p *corev1.Pod) {
		p.Spec.InitContainers = []corev1.Container{{
			Name: "warm",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("7Gi"),
			}},
		}}
	})
	return []runtime.Object{
		gridlock,
		sdNode("n-pinned"),
		drainPod("prod", "web-0", "n-pinned", requesting("500m", "1Gi"), withLabels(map[string]string{"app": "web"}), ownedBy("ReplicaSet", "web-rs")),
		drainPod("prod", "one-off", "n-pinned", requesting("500m", "1Gi")),
		// A DaemonSet pod is skipped by a drain but counted in the
		// autoscaler's utilization: 1.2 of 4 CPUs is 30%.
		drainPod("kube-system", "agent-1", "n-pinned", requesting("200m", "128Mi"), ownedBy("DaemonSet", "agent")),

		sdNode("n-busy"),
		drainPod("prod", "big-one-off", "n-busy", requesting("3", "4Gi")),

		sdNode("n-free"),
		drainPod("prod", "steady-0", "n-free", requesting("400m", "1Gi"), ownedBy("ReplicaSet", "steady-rs")),

		sdNode("n-released"),
		drainPod("prod", "evictable", "n-released", requesting("500m", "2Gi"), safeToEvict("true")),

		sdNode("n-annot", func(n *corev1.Node) { n.Labels = map[string]string{"cloud.google.com/gke-nodepool": "batch-pool"} }),
		drainPod("batch", "checkpointer", "n-annot", requesting("1", "2Gi"), ownedBy("ReplicaSet", "ckpt-rs"), safeToEvict("false")),

		gpuNode, gpuPod,
		sdNode("n-mem"), initPod,

		sdNode("n-cp", func(n *corev1.Node) { n.Labels = map[string]string{"node-role.kubernetes.io/control-plane": ""} }),
		drainPod("kube-system", "static-thing", "n-cp", requesting("100m", "100Mi")),
		sdNode("n-disabled", func(n *corev1.Node) {
			n.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/scale-down-disabled": "true"}
		}),
		drainPod("prod", "pinned-on-purpose", "n-disabled", requesting("100m", "100Mi")),
		sdNode("n-deleting", func(n *corev1.Node) {
			n.Spec.Taints = []corev1.Taint{{Key: "ToBeDeletedByClusterAutoscaler", Effect: corev1.TaintEffectNoSchedule}}
		}),
		drainPod("prod", "leaving", "n-deleting", requesting("100m", "100Mi")),

		replicaSet("prod", "web-rs", 3, ""),
		replicaSet("prod", "steady-rs", 3, ""),
		replicaSet("batch", "ckpt-rs", 2, ""),
	}
}

func TestScaledownBlockedNodes(t *testing.T) {
	res := checktest.Run(t, stab.ScaledownCommand(testDeps(scaledownCluster()...)))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	byNode := map[string]map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] != "scaledown.blocked" || r["severity"] != emit.SeverityWarning || r["reason"] != "ScaleDownBlocked" {
			t.Errorf("unexpected finding %v", r)
		}
		byNode[r["name"]] = r
	}
	var names []string
	for n := range byNode {
		names = append(names, n)
	}
	if len(byNode) != 4 {
		t.Fatalf("blocked nodes = %v, want n-annot, n-gpu, n-mem, n-pinned", names)
	}

	pinned := byNode["n-pinned"]
	if pinned["utilization"] != "30" || pinned["basis"] != "cpu" || pinned["threshold"] != "50" ||
		pinned["blockers"] != "2" || pinned["pdb_gridlock"] != "1" || pinned["bare_pods"] != "1" ||
		pinned["not_safe_to_evict"] != "" || pinned["blocked_by"] != "Pod/prod/one-off,PodDisruptionBudget/prod/web-pdb" {
		t.Errorf("n-pinned = %v, want 30%% cpu with the PDB and the bare pod (DaemonSet pod counted in utilization, not as a blocker)", pinned)
	}
	annot := byNode["n-annot"]
	if annot["not_safe_to_evict"] != "1" || annot["nodepool"] != "batch-pool" || annot["blocked_by"] != "Pod/batch/checkpointer" || annot["utilization"] != "25" {
		t.Errorf("n-annot = %v, want the safe-to-evict=false pod on batch-pool at 25%%", annot)
	}
	if gpu := byNode["n-gpu"]; gpu["basis"] != "gpu" || gpu["utilization"] != "25" {
		t.Errorf("n-gpu = %v, want 25%% judged on GPUs alone (its CPU is 87%%)", gpu)
	}
	if mem := byNode["n-mem"]; mem["basis"] != "memory" || mem["utilization"] != "43" {
		t.Errorf("n-mem = %v, want 43%% memory (7Gi init container over 16Gi)", mem)
	}

	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "10" || sum["nodes"] != "10" || sum["excluded"] != "3" || sum["underused"] != "6" {
		t.Errorf("summary = %v, want scanned=10 nodes=10 excluded=3 underused=6", sum)
	}
}

// Lowering the threshold below a node's utilization takes it out of
// the claim: at 26%, n-pinned (30%) and n-mem (43%) are no longer
// removal candidates, while n-annot and n-gpu (25%) still are.
func TestScaledownThresholdIsTheAutoscalers(t *testing.T) {
	res := checktest.Run(t, stab.ScaledownCommand(testDeps(scaledownCluster()...)), "--utilization=26")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var names []string
	for _, r := range findingLines(t, res.Stdout) {
		names = append(names, r["name"])
		if r["threshold"] != "26" {
			t.Errorf("%s: threshold = %s, want 26", r["name"], r["threshold"])
		}
	}
	if strings.Join(names, ",") != "n-annot,n-gpu" {
		t.Errorf("--utilization=26 reports %v, want n-annot,n-gpu", names)
	}
}

func TestScaledownHealthyIsSilent(t *testing.T) {
	objs := []runtime.Object{
		sdNode("n-1"), sdNode("n-2"),
		drainPod("prod", "steady-0", "n-1", requesting("3", "8Gi"), ownedBy("ReplicaSet", "steady-rs")),
		drainPod("prod", "steady-1", "n-2", requesting("200m", "1Gi"), ownedBy("ReplicaSet", "steady-rs")),
		replicaSet("prod", "steady-rs", 2, ""),
	}
	res := checktest.Run(t, stab.ScaledownCommand(testDeps(objs...)))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "scanned=2 findings=0 elapsed=100ms nodes=2 excluded=0 underused=1\n"
	if res.Stdout != want {
		t.Errorf("a cluster with nothing pinning an underused node must be silent:\ngot:  %qwant: %q", res.Stdout, want)
	}
}

func TestScaledownUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--namespace=prod"},
		{"-A"},
		{"--workload=Deployment/prod/web"},
		{"--utilization=0"},
		{"--utilization=101"},
	} {
		res := checktest.Run(t, stab.ScaledownCommand(testDeps(scaledownCluster()...)), args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("%v: exit %d, want %d (stderr %q)", args, res.Code, emit.ExitUsage, res.Stderr)
		}
		if strings.Contains(res.Stdout, "scanned=") {
			t.Errorf("%v: a usage error must not emit a summary: %q", args, res.Stdout)
		}
	}
}

func TestScaledownContract(t *testing.T) {
	checktest.VerifyContract(t, stab.ScaledownCommand(testDeps(scaledownCluster()...)))
}

func TestScaledownGolden(t *testing.T) {
	res := checktest.Run(t, stab.ScaledownCommand(testDeps(scaledownCluster()...)))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/scaledown.golden", res.Stdout)
}
