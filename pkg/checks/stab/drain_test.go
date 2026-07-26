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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// §13 fixtures: one node exhibiting every blocker class, one clean
// node, one node with only skippable pods.

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func drainPod(ns, name, nodeName string, mut ...func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, m := range mut {
		m(p)
	}
	return p
}

func ownedBy(kind, name string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.OwnerReferences = append(p.OwnerReferences, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: kind, Name: name, Controller: ptr(true),
		})
	}
}

func withLabels(l map[string]string) func(*corev1.Pod) {
	return func(p *corev1.Pod) { p.Labels = l }
}

func replicaSet(ns, name string, replicas int32, deployment string) *appsv1.ReplicaSet {
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.ReplicaSetSpec{Replicas: ptr(replicas)},
	}
	if deployment != "" {
		rs.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "Deployment", Name: deployment, Controller: ptr(true),
		}}
	}
	return rs
}

func deploymentReplicas(ns, name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(replicas)},
	}
}

// drainCluster is the shared fixture. gke-a carries one blocker of
// every class plus every skippable pod shape; gke-b is clean
// (healthy multi-replica pod only); gke-c has only skippable pods.
func drainCluster() []runtime.Object {
	gridlockPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-pdb"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
		Status: policyv1.PodDisruptionBudgetStatus{
			DisruptionsAllowed: 0, CurrentHealthy: 1, DesiredHealthy: 1,
		},
	}
	return []runtime.Object{
		node("gke-a"), node("gke-b"), node("gke-c"),
		gridlockPDB,
		// gke-a: the four blocker classes.
		drainPod("prod", "web-0", "gke-a", withLabels(map[string]string{"app": "web"}), ownedBy("ReplicaSet", "web-rs")),
		drainPod("prod", "one-off", "gke-a"), // bare: no owner
		drainPod("prod", "cache-0", "gke-a", ownedBy("ReplicaSet", "cache-rs"), func(p *corev1.Pod) {
			p.Spec.Volumes = []corev1.Volume{
				{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "tmpfs", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
			}
		}),
		drainPod("prod", "single-0", "gke-a", ownedBy("ReplicaSet", "single-rs")),
		drainPod("prod", "steady-0", "gke-a", ownedBy("ReplicaSet", "steady-rs")), // healthy multi-replica: nothing
		// gke-a: the standard-drain skips.
		drainPod("kube-system", "node-agent-x1", "gke-a", ownedBy("DaemonSet", "node-agent")),
		drainPod("kube-system", "etcd-gke-a", "gke-a", func(p *corev1.Pod) {
			p.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "mirror"}
		}),
		drainPod("prod", "backup-job-x", "gke-a", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }),
		// gke-b: clean.
		drainPod("prod", "steady-1", "gke-b", ownedBy("ReplicaSet", "steady-rs")),
		// gke-c: only skippable pods → drainable.
		drainPod("kube-system", "node-agent-x2", "gke-c", ownedBy("DaemonSet", "node-agent")),
		drainPod("prod", "old-job-y", "gke-c", func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }),
		// Controller chains.
		replicaSet("prod", "web-rs", 3, "web"),
		replicaSet("prod", "cache-rs", 3, "cache"),
		replicaSet("prod", "single-rs", 1, "single"),
		replicaSet("prod", "steady-rs", 3, "steady"),
		deploymentReplicas("prod", "web", 3),
		deploymentReplicas("prod", "cache", 3),
		deploymentReplicas("prod", "single", 1),
		deploymentReplicas("prod", "steady", 3),
	}
}

func TestDrainNodeBlockers(t *testing.T) {
	cmd := stab.DrainCommand(testDeps(drainCluster()...))
	res := checktest.Run(t, cmd, "--node=gke-a")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	byKind := map[string]map[string]string{}
	for _, r := range recs {
		byKind[r["kind"]] = r
	}
	if len(recs) != 4 {
		t.Fatalf("want 4 blocker findings (one per class), got %d: %v", len(recs), recs)
	}

	pdb := byKind["drain.pdb_gridlock"]
	if pdb["severity"] != "critical" || pdb["kind_of_object"] != "PodDisruptionBudget" ||
		pdb["name"] != "web-pdb" || pdb["pods"] != "1" || pdb["pod_names"] != "web-0" ||
		pdb["disruptions_allowed"] != "0" || pdb["current_healthy"] != "1" || pdb["desired_healthy"] != "1" ||
		pdb["node"] != "gke-a" {
		t.Errorf("pdb_gridlock finding = %v", pdb)
	}
	bare := byKind["drain.bare_pod"]
	if bare["severity"] != "warning" || bare["name"] != "one-off" || bare["reason"] != "NoController" {
		t.Errorf("bare_pod finding = %v", bare)
	}
	local := byKind["drain.local_storage"]
	if local["name"] != "cache-0" || local["volumes"] != "scratch,tmpfs(medium=Memory)" {
		t.Errorf("local_storage finding = %v", local)
	}
	single := byKind["drain.singleton"]
	if single["name"] != "single-0" || single["workload"] != "Deployment/prod/single" || single["replicas"] != "1" {
		t.Errorf("singleton finding = %v", single)
	}

	sum := summaryLine(t, res.Stdout)
	// scanned counts examined pods only: web-0, one-off, cache-0,
	// single-0, steady-0 — the DaemonSet, mirror, and Succeeded pods
	// are skipped like a standard drain skips them.
	if sum["scanned"] != "5" || sum["drainable"] != "no" || sum["blockers"] != "4" {
		t.Errorf("summary = %v, want scanned=5 drainable=no blockers=4", sum)
	}
}

// TestDrainDrainableNode: a node with only skippable pods is
// explicitly drainable — zero findings, and the notes say so.
func TestDrainDrainableNode(t *testing.T) {
	cmd := stab.DrainCommand(testDeps(drainCluster()...))
	res := checktest.Run(t, cmd, "--node=gke-c")
	if recs := findingLines(t, res.Stdout); len(recs) != 0 {
		t.Fatalf("want no findings on a drainable node, got %v", recs)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "0" || sum["drainable"] != "yes" || sum["blockers"] != "0" {
		t.Errorf("summary = %v, want scanned=0 drainable=yes blockers=0", sum)
	}
}

// TestDrainAllNodes: -A rolls each blocked node into one drain.node
// finding; blocker-free nodes emit nothing.
func TestDrainAllNodes(t *testing.T) {
	cmd := stab.DrainCommand(testDeps(drainCluster()...))
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("want exactly the blocked node's rollup, got %v", recs)
	}
	r := recs[0]
	want := map[string]string{
		"kind":           "drain.node",
		"severity":       "critical", // a gridlocked PDB is on the node
		"kind_of_object": "Node",
		"name":           "gke-a",
		"reason":         "DrainBlocked",
		"blockers":       "4",
		"pdb_gridlock":   "1",
		"bare_pods":      "1",
		"local_storage":  "1",
		"singletons":     "1",
	}
	for k, v := range want {
		if r[k] != v {
			t.Errorf("finding[%s] = %q, want %q (finding: %v)", k, r[k], v, r)
		}
	}
	if !strings.HasPrefix(r["message"], "not drainable:") {
		t.Errorf("message = %q, want a not drainable summary", r["message"])
	}
	sum := summaryLine(t, res.Stdout)
	// scanned: 5 examined on gke-a + 1 on gke-b + 0 on gke-c.
	if sum["scanned"] != "6" || sum["nodes"] != "3" || sum["blocked"] != "1" {
		t.Errorf("summary = %v, want scanned=6 nodes=3 blocked=1", sum)
	}
}

// TestDrainSingletonVariants: StatefulSets and bare ReplicaSets with
// one replica are singletons too.
func TestDrainSingletonVariants(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "pg"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(int32(1))},
	}
	cmd := stab.DrainCommand(testDeps(
		node("gke-x"),
		sts,
		replicaSet("prod", "lone-rs", 1, ""), // bare RS, no Deployment owner
		drainPod("db", "pg-0", "gke-x", ownedBy("StatefulSet", "pg")),
		drainPod("prod", "lone-rs-abc", "gke-x", ownedBy("ReplicaSet", "lone-rs")),
	))
	recs := findingLines(t, checktest.Run(t, cmd, "--node=gke-x").Stdout)
	workloads := map[string]bool{}
	for _, r := range recs {
		if r["kind"] != "drain.singleton" {
			t.Errorf("unexpected finding %v", r)
		}
		workloads[r["workload"]] = true
	}
	if len(recs) != 2 || !workloads["StatefulSet/db/pg"] || !workloads["ReplicaSet/prod/lone-rs"] {
		t.Errorf("want singleton findings for StatefulSet/db/pg and ReplicaSet/prod/lone-rs, got %v", recs)
	}
}

// TestDrainUnknownNode: naming a node that does not exist is a
// runtime error (exit 1) that names the node.
func TestDrainUnknownNode(t *testing.T) {
	cmd := stab.DrainCommand(testDeps(drainCluster()...))
	res := checktest.Run(t, cmd, "--node=nope")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, `"nope"`) {
		t.Errorf("exit %d stderr %q, want exit 1 naming the node", res.Code, res.Stderr)
	}
}

// TestDrainUsageErrors: drain is node-scoped — namespace/workload
// scoping and ambiguous node selection are usage errors (exit 2).
func TestDrainUsageErrors(t *testing.T) {
	cmd := stab.DrainCommand(testDeps(drainCluster()...))
	for name, args := range map[string][]string{
		"namespace":    {"--node=gke-a", "--namespace=prod"},
		"workload":     {"--node=gke-a", "--workload=Deployment/prod/web"},
		"node_and_all": {"--node=gke-a", "-A"},
		"no_selection": {},
	} {
		t.Run(name, func(t *testing.T) {
			res := checktest.Run(t, cmd, args...)
			if res.Code != emit.ExitUsage {
				t.Errorf("args %v: exit %d stderr %q, want usage error", args, res.Code, res.Stderr)
			}
		})
	}
}

func TestDrainContract(t *testing.T) {
	deps := testDeps(drainCluster()...)
	checktest.VerifyContract(t, stab.DrainCommand(deps), "--node=gke-a")
	checktest.VerifyContract(t, stab.DrainCommand(deps), "--node=gke-c")
	checktest.VerifyContract(t, stab.DrainCommand(deps), "-A")
}

func TestDrainGolden(t *testing.T) {
	res := checktest.Run(t, stab.DrainCommand(testDeps(drainCluster()...)), "--node=gke-a")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checkGolden(t, "drain-node.golden", res.Stdout)
}
