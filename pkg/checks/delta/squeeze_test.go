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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// The live case behind #748, reduced: a fixed three-node generic pool,
// one node of which carries a sandbox taint set by `kubectl taint` that
// its pool does not; compute-class pools that can grow but take only
// pods selecting their class; node auto-provisioning off; and a
// DaemonSet two days old requesting 130m on every node, filling the two
// remaining generic nodes.

func cpuMem(cpu, mem string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem),
	}}
}

func sizedNode(name, poolName string, taints ...corev1.Taint) *corev1.Node {
	n := poolNode(name, poolName, true)
	n.Spec.Taints = taints
	n.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("940m"), corev1.ResourceMemory: resource.MustParse("2760Mi")}
	return &n
}

func runningOn(ns, name, node, cpu string, owner ...metav1.OwnerReference) *corev1.Pod {
	p := healthyPod(ns, name)
	p.Spec.NodeName = node
	p.Spec.Containers = []corev1.Container{{Name: "c", Resources: cpuMem(cpu, "64Mi")}}
	p.OwnerReferences = owner
	return p
}

func unschedulableNeeding(ns, name, cpu string) *corev1.Pod {
	p := pendingPod(ns, name, 6*time.Hour, true)
	p.Spec.Containers = []corev1.Container{{Name: "c", Resources: cpuMem(cpu, "64Mi")}}
	return p
}

var sandboxTaint = corev1.Taint{Key: "ate.dev/sandboxClass", Value: "gvisor", Effect: corev1.TaintEffectNoSchedule}

func squeezeCluster() []runtime.Object {
	tainted := sizedNode("generic-a", "default-pool", sandboxTaint)
	tainted.ManagedFields = []metav1.ManagedFieldsEntry{
		{Manager: "gcp-controller-manager", Time: ptr(ago(30 * 24 * time.Hour)), FieldsV1: metav1.NewFieldsV1(`{"f:metadata":{"f:labels":{}}}`)},
		{Manager: "kubectl-taint", Time: ptr(ago(9 * 24 * time.Hour)), FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:taints":{}}}`)},
	}
	classTaint := corev1.Taint{Key: computeClassTaint, Value: "n4-preferred", Effect: corev1.TaintEffectNoSchedule}
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "obi-system", Name: "obi-agent", UID: types.UID("ds-obi"), CreationTimestamp: ago(47 * time.Hour)},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "loader", Resources: cpuMem("50m", "64Mi")}, {Name: "agent", Resources: cpuMem("80m", "64Mi")},
		}}}},
		Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: 5},
	}
	dsOwner := metav1.OwnerReference{Kind: "DaemonSet", Name: "obi-agent", UID: ds.UID}
	return []runtime.Object{
		tainted,
		sizedNode("generic-b", "default-pool"),
		sizedNode("generic-c", "default-pool"),
		sizedNode("class-a", "nap-n4", classTaint),
		sizedNode("class-b", "nap-n4", classTaint),
		ds,
		runningOn("obi-system", "obi-b", "generic-b", "130m", dsOwner),
		runningOn("obi-system", "obi-c", "generic-c", "130m", dsOwner),
		runningOn("kube-system", "anetd-b", "generic-b", "780m"),
		runningOn("kube-system", "anetd-c", "generic-c", "785m"),
		unschedulableNeeding("ax-system", "ax-server-1", "100m"),
		unschedulableNeeding("substrate-scope", "substrate-scope-1", "50m"),
	}
}

func squeezeProvider(nap cloud.Toggle) func(context.Context) (cloud.Provider, error) {
	return func(context.Context) (cloud.Provider, error) {
		return scaleProvider{Provider: cloud.NoProvider, noCap: true, cfg: cloud.ClusterConfig{
			Name:                 "agent-substrate",
			NodeAutoprovisioning: nap,
			NodePools:            []cloud.NodePoolConfig{pool("default-pool", false, 0, 0), pool("nap-n4", true, 0, 1000)},
		}}, nil
	}
}

func runSqueeze(t *testing.T, nap cloud.Toggle, objs ...runtime.Object) (checktest.Result, string) {
	t.Helper()
	client := fake.NewClientset(objs...)
	src := func(context.Context) (kubernetes.Interface, error) { return client, nil }
	cmd := newCommand(src, func() time.Time { return testNow })
	cmd.Run = (&delta{source: src, now: func() time.Time { return testNow }, provider: squeezeProvider(nap)}).run
	res := checktest.Run(t, cmd)
	for _, line := range strings.Split(res.Stdout, "\n") {
		if strings.HasPrefix(line, "kind=autoscaler.scaleup_blocked") {
			return res, line
		}
	}
	t.Fatalf("no scale-up finding:\n%s", res.Stdout)
	return res, ""
}

// TestSqueezeExplainsTheLiveCase: the finding names the node-level
// taint and who set it, the room left against the smallest request,
// the new DaemonSet, and calls the pods stuck.
func TestSqueezeExplainsTheLiveCase(t *testing.T) {
	res, line := runSqueeze(t, cloud.ToggleDisabled, squeezeCluster()...)
	for _, want := range []string{
		"severity=critical", "reason=NoPoolCanGrow",
		"the pools that can grow (nap-n4) have no node these pods could use, and node auto-provisioning is off",
		"of 5 Ready node(s), 3 carry a taint the pods do not tolerate, 1 of them set on the node alone rather than by its pool",
		"ate.dev/sandboxClass=gvisor:NoSchedule on generic-a (not on the other 2 node(s) of pool default-pool; taints last set by kubectl-taint at 2025-12-23T12:00:00Z)",
		"2 the pods could use lack room for even the smallest of them",
		"DaemonSet obi-system/obi-agent, created 47h ago, requests 130m CPU on each of 5 node(s)",
		`taints="ate.dev/sandboxClass=gvisor:NoSchedule on generic-a; cloud.google.com/compute-class=n4-preferred:NoSchedule on class-a, class-b (compute class n4-preferred: only pods that select it)"`,
		`full_nodes="generic-b 30m CPU, 2.6Gi memory free of 940m, 2.7Gi; generic-c 25m CPU, 2.6Gi memory free of 940m, 2.7Gi (smallest pending request: 50m CPU, 64Mi memory)"`,
		`largest_requests="kube-system/anetd-c 785m, kube-system/anetd-b 780m, obi-system/obi-agent 130m (DaemonSet, created 47h ago)"`,
		`new_daemonsets="obi-system/obi-agent 130m CPU, 128Mi memory on each of 5 node(s), created 47h ago"`,
		"auto_provisioning=disabled",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("missing %q in:\n%s", want, line)
		}
	}
	if strings.Contains(line, "likelier cause") {
		t.Errorf("an accounted squeeze must not fall back to the generic hint:\n%s", line)
	}
	if err := checktest.Verify(newCommand(nil, nil), res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("output contract: %v", err)
	}
}

// TestSqueezeWithAutoprovisioningIsNotStuck: with node auto-provisioning
// on, a new pool could be made for the pods; the account still says
// why nothing fits today, but the finding is not critical.
func TestSqueezeWithAutoprovisioningIsNotStuck(t *testing.T) {
	_, line := runSqueeze(t, cloud.ToggleEnabled, squeezeCluster()...)
	if strings.Contains(line, "severity=critical") || strings.Contains(line, "node auto-provisioning is off") {
		t.Errorf("auto-provisioning on must not read as stuck:\n%s", line)
	}
	if !strings.Contains(line, "auto_provisioning=enabled") || !strings.Contains(line, "set on the node alone") {
		t.Errorf("the account must still be there:\n%s", line)
	}
}

// TestSqueezeToleratedTaintOpensTheNode: a pending pod that tolerates
// the sandbox taint can use the tainted node, which has room, so it is
// not counted as tainted and the default pool is one the pods could use.
func TestSqueezeToleratedTaintOpensTheNode(t *testing.T) {
	objs := squeezeCluster()
	for _, o := range objs {
		if p, ok := o.(*corev1.Pod); ok && p.Name == "ax-server-1" {
			p.Spec.Tolerations = []corev1.Toleration{{Key: "ate.dev/sandboxClass", Operator: corev1.TolerationOpExists}}
		}
	}
	_, line := runSqueeze(t, cloud.ToggleDisabled, objs...)
	if !strings.Contains(line, "of 5 Ready node(s), 2 carry a taint") || !strings.Contains(line, "1 have room by requests") {
		t.Errorf("the tolerated node should count as usable, with room:\n%s", line)
	}
}

func TestToleratesTaint(t *testing.T) {
	ns := corev1.TaintEffectNoSchedule
	taint := corev1.Taint{Key: "k", Value: "v", Effect: ns}
	for _, tc := range []struct {
		name string
		tol  corev1.Toleration
		want bool
	}{
		{"exists, no key: everything", corev1.Toleration{Operator: corev1.TolerationOpExists}, true},
		{"equal, no key: nothing", corev1.Toleration{}, false},
		{"key exists", corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists}, true},
		{"key equal value", corev1.Toleration{Key: "k", Value: "v"}, true},
		{"key wrong value", corev1.Toleration{Key: "k", Value: "w"}, false},
		{"other key", corev1.Toleration{Key: "j", Operator: corev1.TolerationOpExists}, false},
		{"other effect", corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute}, false},
		{"matching effect", corev1.Toleration{Key: "k", Value: "v", Effect: ns}, true},
	} {
		if got := toleratesTaint([]corev1.Toleration{tc.tol}, taint); got != tc.want {
			t.Errorf("%s: got %t, want %t", tc.name, got, tc.want)
		}
	}
}

// TestPodRequestsCountsSidecars: a sidecar (restartPolicy Always init
// container) keeps running and adds to the request; a regular init
// container only raises it if it asks for more.
func TestPodRequestsCountsSidecars(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	spec := corev1.PodSpec{
		InitContainers: []corev1.Container{
			{Name: "sidecar", RestartPolicy: &always, Resources: cpuMem("5m", "8Mi")},
			{Name: "setup", Resources: cpuMem("150m", "16Mi")},
		},
		Containers: []corev1.Container{{Name: "main", Resources: cpuMem("200m", "64Mi")}},
	}
	if cpu, mem := podRequests(spec); cpu != 205 || mem != 72<<20 {
		t.Errorf("got %dm, %d bytes; want 205m, 72Mi", cpu, mem)
	}
	spec.InitContainers[1].Resources = cpuMem("500m", "16Mi")
	if cpu, _ := podRequests(spec); cpu != 505 {
		t.Errorf("a larger init container (with the sidecar before it) sets the request: got %dm, want 505m", cpu)
	}
}

func TestBytesText(t *testing.T) {
	for b, want := range map[int64]string{64 << 20: "64Mi", 2760 << 20: "2.7Gi", 2 << 30: "2Gi", 1144315648: "1.1Gi", 3<<20 + 1<<19: "3.5Mi", 512: "512"} {
		if got := bytesText(b); got != want {
			t.Errorf("bytesText(%d) = %q, want %q", b, got, want)
		}
	}
}
