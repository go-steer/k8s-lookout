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

package cloudcheck_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// fullOrphanProvider serves both capabilities `cloud orphans` can
// need: orphans for disks/lbs/addresses, cluster-config for nodepools.
type fullOrphanProvider struct {
	cloud.Provider
	orphans cloud.OrphanAPI
	config  cloud.ClusterConfigAPI
}

func (p fullOrphanProvider) Orphans() (cloud.OrphanAPI, bool) { return p.orphans, true }
func (p fullOrphanProvider) ClusterConfig() (cloud.ClusterConfigAPI, bool) {
	return p.config, true
}

// fakeClusterConfig serves a canned cluster record.
type fakeClusterConfig struct {
	cfg    cloud.ClusterConfig
	err    error
	called bool
}

func (f *fakeClusterConfig) Config(context.Context) (cloud.ClusterConfig, error) {
	f.called = true
	return f.cfg, f.err
}

func (f *fakeClusterConfig) UpgradeTargets(context.Context, string) (cloud.UpgradeTargets, error) {
	return cloud.UpgradeTargets{}, nil
}

// poolsFixture is the provider half of the node-pool fixture: one pool
// per case the judgment distinguishes.
func poolsFixture() *fakeClusterConfig {
	return &fakeClusterConfig{cfg: cloud.ClusterConfig{
		Name: "batch",
		NodePools: []cloud.NodePoolConfig{
			// Busy: workload on one of its nodes. Silent.
			{Name: "default-pool", MachineType: "e2-standard-4", Autoscaling: cloud.NodePoolAutoscaling{Enabled: true, MinNodeCount: 1, MaxNodeCount: 5}},
			// Scaled to zero: no nodes, no bill. Silent.
			{Name: "batch-pool", MachineType: "n2-standard-16", Autoscaling: cloud.NodePoolAutoscaling{Enabled: true, MaxNodeCount: 20}},
			// Idle, held up by a per-zone minimum; DaemonSet and
			// mirror pods on it do not make it busy.
			{Name: "idle-min-pool", MachineType: "e2-standard-8", Autoscaling: cloud.NodePoolAutoscaling{Enabled: true, MinNodeCount: 1, MaxNodeCount: 3}},
			// Idle, held up by a pool-wide minimum; its only workload
			// pod has Succeeded.
			{Name: "gpu-pool", MachineType: "g2-standard-8", Autoscaling: cloud.NodePoolAutoscaling{Enabled: true, TotalMinNodeCount: 2, TotalMaxNodeCount: 6}},
			// Idle and not autoscaled at all.
			{Name: "fixed-pool", MachineType: "n2-standard-8"},
			// Idle, auto-provisioned, floor zero: should go away alone.
			{Name: "nap-pool", Autoscaling: cloud.NodePoolAutoscaling{Enabled: true, MaxNodeCount: 1000, Autoprovisioned: true}},
			// A kube-system Deployment is workload: silent.
			{Name: "system-pool", MachineType: "e2-small"},
		},
	}}
}

func node(name, pool string) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if pool != "" {
		n.Labels = map[string]string{"cloud.google.com/gke-nodepool": pool}
	}
	return n
}

// pod builds a running pod on nodeName controlled by ownerKind ("" for
// a bare pod).
func pod(ns, name, nodeName, ownerKind string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if ownerKind != "" {
		yes := true
		p.OwnerReferences = []metav1.OwnerReference{{Kind: ownerKind, Name: name + "-owner", Controller: &yes}}
	}
	return p
}

// clusterFixture is the cluster half: the nodes and what runs on them.
func clusterFixture() []runtime.Object {
	mirror := pod("kube-system", "kube-proxy-idle-min-1", "idle-min-1", "")
	mirror.Annotations = map[string]string{"kubernetes.io/config.mirror": "abc"}
	done := pod("ml", "train-7f9", "gpu-1", "Job")
	done.Status.Phase = corev1.PodSucceeded
	pending := pod("web", "api-pending", "", "ReplicaSet")
	pending.Status.Phase = corev1.PodPending
	return []runtime.Object{
		node("default-1", "default-pool"),
		node("default-2", "default-pool"),
		node("idle-min-1", "idle-min-pool"),
		node("gpu-1", "gpu-pool"),
		node("gpu-2", "gpu-pool"),
		node("fixed-1", "fixed-pool"),
		node("fixed-2", "fixed-pool"),
		node("fixed-3", "fixed-pool"),
		node("nap-1", "nap-pool"),
		node("system-1", "system-pool"),
		node("unmanaged-1", ""),
		pod("web", "api-1", "default-1", "ReplicaSet"),
		pod("kube-system", "fluentbit-default-1", "default-1", "DaemonSet"),
		pod("kube-system", "fluentbit-idle-min-1", "idle-min-1", "DaemonSet"),
		mirror,
		done,
		pod("kube-system", "fluentbit-fixed-1", "fixed-1", "DaemonSet"),
		pod("kube-system", "kube-dns-5d8", "system-1", "ReplicaSet"),
		pending,
	}
}

func nodePoolDeps(p cloud.Provider, client kubernetes.Interface) cloudcheck.Deps {
	deps := testDeps(p)
	deps.Client = func(context.Context) (kubernetes.Interface, error) { return client, nil }
	return deps
}

func nodePoolCmd(cfg *fakeClusterConfig, client kubernetes.Interface) cloudcheckCommand {
	p := fullOrphanProvider{Provider: cloud.NoProvider, orphans: orphanFixture(), config: cfg}
	return cloudcheckCommand{deps: nodePoolDeps(p, client)}
}

// cloudcheckCommand defers building the command so a test can tweak
// deps first.
type cloudcheckCommand struct{ deps cloudcheck.Deps }

func (c cloudcheckCommand) run(t *testing.T, args ...string) checktest.Result {
	t.Helper()
	return checktest.Run(t, cloudcheck.OrphansCommand(c.deps), args...)
}

func TestOrphansNodePools(t *testing.T) {
	res := nodePoolCmd(poolsFixture(), fake.NewClientset(clusterFixture()...)).run(t, "--only=nodepools")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	byName := map[string]map[string]string{}
	var order []string
	for _, r := range recs {
		if r["kind"] != "orphan.nodepool" {
			t.Errorf("unexpected record %v", r)
			continue
		}
		byName[r["name"]] = r
		order = append(order, r["name"])
	}
	if got := strings.Join(order, ","); got != "fixed-pool,gpu-pool,idle-min-pool,nap-pool" {
		t.Fatalf("idle pools = %s, want fixed-pool,gpu-pool,idle-min-pool,nap-pool (busy, empty and kube-system-busy pools silent)", got)
	}
	for name, r := range byName {
		if r["severity"] != emit.SeverityInfo || r["kind_of_object"] != "NodePool" || r["namespace"] != "" {
			t.Errorf("%s = %v, want info NodePool with no namespace", name, r)
		}
	}

	if r := byName["idle-min-pool"]; r["reason"] != "IdleMinNodeCount" || r["node_count"] != "1" ||
		r["excluded_pods"] != "2" || r["autoscaling"] != "enabled" || r["min_node_count"] != "1" ||
		r["max_node_count"] != "3" || r["machine_type"] != "e2-standard-8" ||
		!strings.Contains(r["message"], "min_node_count=1") {
		t.Errorf("idle-min-pool = %v, want IdleMinNodeCount 1 node, 2 excluded (DaemonSet + mirror), min 1 max 3", r)
	}
	if r := byName["gpu-pool"]; r["reason"] != "IdleMinNodeCount" || r["node_count"] != "2" ||
		r["excluded_pods"] != "0" || r["total_min_node_count"] != "2" || r["total_max_node_count"] != "6" ||
		r["min_node_count"] != "" || !strings.Contains(r["message"], "total_min_node_count=2") {
		t.Errorf("gpu-pool = %v, want IdleMinNodeCount on the pool-wide floor, the Succeeded pod not counted", r)
	}
	if r := byName["fixed-pool"]; r["reason"] != "IdleNotAutoscaled" || r["node_count"] != "3" ||
		r["autoscaling"] != "disabled" || r["min_node_count"] != "" || r["excluded_pods"] != "1" {
		t.Errorf("fixed-pool = %v, want IdleNotAutoscaled 3 nodes autoscaling=disabled, no floor", r)
	}
	if r := byName["nap-pool"]; r["reason"] != "IdleAwaitingScaleDown" || r["autoprovisioned"] != "true" ||
		r["min_node_count"] != "0" || r["machine_type"] != "" || !strings.Contains(r["message"], "stab scaledown") {
		t.Errorf("nap-pool = %v, want IdleAwaitingScaleDown autoprovisioned min 0 pointing at stab scaledown", r)
	}
	// scanned = the seven pools the provider reported.
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "7" {
		t.Errorf("scanned = %s, want 7", sum["scanned"])
	}
}

// TestOrphansNodePoolsOptIn: the default --only never reads the
// cluster record — nodepools is opt-in.
func TestOrphansNodePoolsOptIn(t *testing.T) {
	cfg := poolsFixture()
	res := nodePoolCmd(cfg, fake.NewClientset(clusterFixture()...)).run(t)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if cfg.called {
		t.Error("default --only read the cluster record; nodepools must be opt-in")
	}
	if strings.Contains(res.Stdout, "orphan.nodepool") {
		t.Errorf("default sweep emitted orphan.nodepool:\n%s", res.Stdout)
	}
}

// TestOrphansNodePoolsHealthySilent: every pool with nodes runs
// workload — nothing reported.
func TestOrphansNodePoolsHealthySilent(t *testing.T) {
	cfg := &fakeClusterConfig{cfg: cloud.ClusterConfig{NodePools: []cloud.NodePoolConfig{
		{Name: "default-pool", Autoscaling: cloud.NodePoolAutoscaling{Enabled: true, MinNodeCount: 1, MaxNodeCount: 5}},
		{Name: "spot-pool", Autoscaling: cloud.NodePoolAutoscaling{Enabled: true, MaxNodeCount: 5}},
	}}}
	client := fake.NewClientset(
		node("default-1", "default-pool"),
		pod("web", "api-1", "default-1", "ReplicaSet"),
		pod("kube-system", "fluentbit-default-1", "default-1", "DaemonSet"),
	)
	res := nodePoolCmd(cfg, client).run(t, "--only=nodepools")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["findings"] != "0" || sum["scanned"] != "2" {
		t.Errorf("summary = %v, want findings=0 scanned=2", sum)
	}
}

// TestOrphansNodePoolsAutopilot: an Autopilot cluster's pools are not
// the operator's to size — nothing swept, and the note says why.
func TestOrphansNodePoolsAutopilot(t *testing.T) {
	cfg := poolsFixture()
	cfg.cfg.Autopilot = true
	res := nodePoolCmd(cfg, fake.NewClientset(clusterFixture()...)).run(t, "--only=nodepools")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["findings"] != "0" || sum["scanned"] != "0" || !strings.HasPrefix(sum["nodepools_skipped"], "autopilot") {
		t.Errorf("summary = %v, want findings=0 scanned=0 nodepools_skipped=autopilot...", sum)
	}
}

// TestOrphansNodePoolsConfigDenied: a 403 on the cluster read degrades
// the class (#559's per-class rule) while the other classes still run.
func TestOrphansNodePoolsConfigDenied(t *testing.T) {
	cfg := poolsFixture()
	cfg.err = denied("container.clusters.get")
	res := nodePoolCmd(cfg, fake.NewClientset(clusterFixture()...)).run(t, "--only=disks,nodepools")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	kinds := map[string]int{}
	var u map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		kinds[r["kind"]]++
		if r["kind"] == "cloud.unavailable" {
			u = r
		}
	}
	if kinds["orphan.disk"] != 3 || kinds["orphan.nodepool"] != 0 || kinds["cloud.unavailable"] != 1 {
		t.Fatalf("kinds = %v, want 3 orphan.disk and one cloud.unavailable", kinds)
	}
	if u["reason"] != "PermissionDenied" || u["class"] != "nodepools" || u["permission"] != "container.clusters.get" {
		t.Errorf("unavailable = %v, want PermissionDenied class=nodepools permission=container.clusters.get", u)
	}
	if sum := summaryLine(t, res.Stdout); sum["unavailable"] != "nodepools: needs container.clusters.get" || sum["scanned"] != "4" {
		t.Errorf("summary = %v, want scanned=4 unavailable=\"nodepools: needs container.clusters.get\"", sum)
	}
}

// TestOrphansNodePoolsListForbidden: a refused Node or Pod List skips
// the judgment with a read.unavailable record (#546) — a pool whose
// pods cannot be seen is not called idle.
func TestOrphansNodePoolsListForbidden(t *testing.T) {
	for _, resource := range []string{"nodes", "pods"} {
		t.Run(resource, func(t *testing.T) {
			client := fake.NewClientset(clusterFixture()...)
			client.PrependReactor("list", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: resource}, "", errors.New("RBAC: access denied"))
			})
			res := nodePoolCmd(poolsFixture(), client).run(t, "--only=nodepools")
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, want 0; stderr: %s", res.Code, res.Stderr)
			}
			recs := findingLines(t, res.Stdout)
			if len(recs) != 1 || recs[0]["kind"] != "read.unavailable" || recs[0]["reason"] != "ListForbidden" ||
				recs[0]["resource"] != resource {
				t.Fatalf("records = %v, want one read.unavailable resource=%s", recs, resource)
			}
			if sum := summaryLine(t, res.Stdout); sum["unavailable"] != "nodepools: forbidden: list "+resource || sum["scanned"] != "0" {
				t.Errorf("summary = %v, want scanned=0 unavailable=\"nodepools: forbidden: list %s\"", sum, resource)
			}
		})
	}
}

// TestOrphansNodePoolsListErrorFatal: a List failure that is not a
// refusal stays a runtime error.
func TestOrphansNodePoolsListErrorFatal(t *testing.T) {
	client := fake.NewClientset(clusterFixture()...)
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("etcd leader changed"))
	})
	res := nodePoolCmd(poolsFixture(), client).run(t, "--only=nodepools")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, "node-pool sweep") {
		t.Errorf("exit %d stderr %q, want exit 1 from the node-pool sweep", res.Code, res.Stderr)
	}
}

// TestOrphansNodePoolsUnavailable: no provider — the cluster-config
// capability is named, never an all-clear.
func TestOrphansNodePoolsUnavailable(t *testing.T) {
	res := checktest.Run(t, cloudcheck.OrphansCommand(testDeps(cloud.NoProvider)), "--only=nodepools")
	assertUnavailable(t, res, string(cloud.CapabilityClusterConfig))
}

// TestOrphansNodePoolsCapabilityMissingAlone: a provider with the
// orphans capability but not cluster-config sweeps the classes it can
// and reports the one it cannot.
func TestOrphansNodePoolsCapabilityMissingAlone(t *testing.T) {
	cmd := cloudcheck.OrphansCommand(testDeps(orphanProvider{Provider: cloud.NoProvider, api: orphanFixture()}))
	res := checktest.Run(t, cmd, "--only=disks,nodepools")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kinds := map[string]int{}
	for _, r := range findingLines(t, res.Stdout) {
		kinds[r["kind"]]++
		if r["kind"] == "cloud.unavailable" && (r["reason"] != "CapabilityUnavailable" || r["capability"] != string(cloud.CapabilityClusterConfig)) {
			t.Errorf("unavailable = %v, want CapabilityUnavailable capability=cluster-config", r)
		}
	}
	if kinds["orphan.disk"] != 3 || kinds["cloud.unavailable"] != 1 {
		t.Errorf("kinds = %v, want 3 orphan.disk and one cloud.unavailable", kinds)
	}
	if sum := summaryLine(t, res.Stdout); sum["unavailable"] != "nodepools: "+cloud.NoProviderReason {
		t.Errorf("summary unavailable = %q, want %q", sum["unavailable"], "nodepools: "+cloud.NoProviderReason)
	}
}

// TestOrphansNodePoolsExemption: intentional headroom is annotated by
// the existing --exemptions file, matched on kind + pool name.
func TestOrphansNodePoolsExemption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exemptions.yaml")
	body := "exemptions:\n- kind: orphan.nodepool\n  name: gpu-pool\n  reason: warm GPU capacity for the inference failover\n  expires: \"2099-01-01\"\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	res := nodePoolCmd(poolsFixture(), fake.NewClientset(clusterFixture()...)).run(t, "--only=nodepools", "--exemptions="+path)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, r := range findingLines(t, res.Stdout) {
		exempted := r["exempt_reason"] != ""
		if exempted != (r["name"] == "gpu-pool") {
			t.Errorf("%s exempt_reason=%q, want only gpu-pool annotated", r["name"], r["exempt_reason"])
		}
	}
	if sum := summaryLine(t, res.Stdout); sum["exempt"] != "1" || sum["findings"] != "4" {
		t.Errorf("summary = %v, want findings=4 exempt=1 (annotated, never removed)", sum)
	}
}

func TestOrphansNodePoolsContract(t *testing.T) {
	c := nodePoolCmd(poolsFixture(), fake.NewClientset(clusterFixture()...))
	checktest.VerifyContract(t, cloudcheck.OrphansCommand(c.deps), "--only=nodepools")
}

func TestOrphansNodePoolsGolden(t *testing.T) {
	res := nodePoolCmd(poolsFixture(), fake.NewClientset(clusterFixture()...)).run(t, "--only=nodepools")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/orphans-nodepools.golden", res.Stdout)
}
