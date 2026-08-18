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

package triage

// §13 conventions: fake.Clientset fixture cluster with a golden for
// the live path; a real SQLite store seeded through the production
// graph writer for the --at path (history and live must demonstrably
// differ); usage-error table; checktest contract round-trip.

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

var fixedNow = time.Date(2026, 7, 25, 10, 30, 0, 0, time.UTC)

func fakeDeps(objs ...runtime.Object) Deps {
	cs := fake.NewClientset(objs...)
	return Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	}
}

// noClusterDeps fails the test if the command touches the API — the
// --at contract: a point-in-time question is answered entirely from
// the store.
func noClusterDeps(t *testing.T) Deps {
	t.Helper()
	return Deps{
		Client: func(context.Context) (kubernetes.Interface, error) {
			t.Error("client must not be built in --at mode")
			return nil, errors.New("no cluster in --at mode")
		},
		Now: func() time.Time { return fixedNow },
	}
}

func podReady(ready bool) []corev1.PodCondition {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
}

func ownedBy(kind, name string) []metav1.OwnerReference {
	ctrl := true
	return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &ctrl, UID: types.UID(kind + "-" + name)}}
}

// radiusFixture is the live cluster: Ingress edge → Service web →
// EndpointSlice web-abc → pods web-1/web-2 (ReplicaSet web-rs ←
// Deployment web), both on node n1 mounting ConfigMap cm-app; web-1
// also references a Secret that does not exist. Co-tenants: pod lone
// shares n1, pod other-1 (node n2) shares cm-app. Pod isolated (node
// n2, own config) shares nothing and must not appear.
func radiusFixture() []runtime.Object {
	webPod := func(name, node string, ready bool) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: name,
				Labels:          map[string]string{"app": "web"},
				OwnerReferences: ownedBy("ReplicaSet", "web-rs"),
			},
			Spec: corev1.PodSpec{
				NodeName:   node,
				Containers: []corev1.Container{{Name: "app", Image: "img:v1"}},
				Volumes: []corev1.Volume{{
					Name:         "cfg",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm-app"}}},
				}},
			},
			Status: corev1.PodStatus{Conditions: podReady(ready)},
		}
	}
	web1 := webPod("web-1", "n1", true)
	web1.Spec.Volumes = append(web1.Spec.Volumes, corev1.Volume{
		Name:         "tls",
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "ghost"}},
	})
	web2 := webPod("web-2", "n1", false)

	plainPod := func(name, node, cm string) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: name},
			Spec: corev1.PodSpec{
				NodeName:   node,
				Containers: []corev1.Container{{Name: "app", Image: "img:v1"}},
			},
			Status: corev1.PodStatus{Conditions: podReady(true)},
		}
		if cm != "" {
			p.Spec.Volumes = []corev1.Volume{{
				Name:         "cfg",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cm}}},
			}}
		}
		return p
	}

	replicas := int32(2)
	return []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
		},
		&appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-rs", OwnerReferences: ownedBy("Deployment", "web")},
			Spec:       appsv1.ReplicaSetSpec{Replicas: &replicas},
		},
		web1, web2,
		plainPod("lone", "n1", ""),
		plainPod("other-1", "n2", "cm-app"),
		plainPod("isolated", "n2", "cm-iso"),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2"}},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "web"}},
		},
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: "web-abc",
				Labels: map[string]string{discoveryv1.LabelServiceName: "web"},
			},
			Endpoints: []discoveryv1.Endpoint{
				{TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "web-1"}},
				{TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "web-2"}},
			},
		},
		&netv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "edge"},
			Spec: netv1.IngressSpec{
				DefaultBackend: &netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: "web"}},
			},
		},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "cm-app"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "cm-iso"}},
	}
}

// TestRadius_LiveGolden pins the whole live payload: directions,
// relations, hops, lateral anchors, readiness, the missing-reference
// warning, and the source=live summary note.
func TestRadius_LiveGolden(t *testing.T) {
	res := checktest.Run(t, RadiusCommand(fakeDeps(radiusFixture()...)), "Deployment/prod/web")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/radius-live.golden", res.Stdout)

	// Spot assertions the golden alone would bury.
	for _, want := range []string{
		"direction=upstream relation=Owns hop=1",   // ReplicaSet web-rs
		"direction=lateral relation=shared-node",   // lone via n1
		"direction=lateral relation=shared-config", // other-1 via cm-app
		"kind=radius.missing",                      // Secret ghost
		"source=live",                              // §6.6 summary note
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
	if strings.Contains(res.Stdout, "isolated") {
		t.Errorf("pod outside the neighborhood leaked in:\n%s", res.Stdout)
	}
}

// TestRadius_DepthLimit: --depth=1 keeps direct neighbors but drops
// the hop-2 traffic chain (the Ingress).
func TestRadius_DepthLimit(t *testing.T) {
	res := checktest.Run(t, RadiusCommand(fakeDeps(radiusFixture()...)), "Deployment/prod/web", "--depth=1")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	if strings.Contains(res.Stdout, "name=edge") {
		t.Errorf("--depth=1 leaked the hop-2 Ingress:\n%s", res.Stdout)
	}
	for _, want := range []string{"name=web-rs", "name=n1", "name=cm-app"} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("--depth=1 lost direct neighbor %q:\n%s", want, res.Stdout)
		}
	}
}

// TestRadius_PodTargetReadiness: a bare pod name resolves as a Pod
// in --namespace, and sibling pods report readiness from the live
// List pass.
func TestRadius_PodTargetReadiness(t *testing.T) {
	res := checktest.Run(t, RadiusCommand(fakeDeps(radiusFixture()...)), "web-1", "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "name=web-2") || !strings.Contains(res.Stdout, "ready=false") {
		t.Errorf("sibling pod with readiness missing:\n%s", res.Stdout)
	}
}

// seedHistory writes a real store: baseline snapshot (pod pay-1 on
// n1 mounting cm-pay) at t0, then pod pay-2 lands on n1 one minute
// later. Blast radius of pay-1 at t0 has no lateral neighbors; at
// t0+1m it has pay-2.
func seedHistory(t *testing.T) (path string, t0 time.Time) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "lookout.db")
	t0 = time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
	cur := t0
	clock := func() time.Time { return cur }

	st, err := store.Open(path, store.WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New(graph.Options{SwapInterval: -1, OnChange: st.RecordGraphChange, Now: clock})
	w := g.Writer()
	pod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "pay", Name: name},
			Spec: corev1.PodSpec{
				NodeName:   "n1",
				Containers: []corev1.Container{{Name: "app", Image: "img:v1"}},
				Volumes: []corev1.Volume{{
					Name:         "cfg",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm-pay"}}},
				}},
			},
		}
	}
	if err := w.FromObjects(slices.Values([]any{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "pay", Name: "cm-pay"}},
		pod("pay-1"),
	})); err != nil {
		t.Fatal(err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutGraphSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}

	cur = t0.Add(time.Minute)
	if err := w.Apply(graph.Delta{Op: graph.OpAdd, Object: pod("pay-2")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return path, t0
}

// TestRadius_At: the same question at two instants answers from two
// topologies — before pay-2 existed and after — without ever
// touching the cluster.
func TestRadius_At(t *testing.T) {
	path, t0 := seedHistory(t)
	cmd := RadiusCommand(noClusterDeps(t))

	before := checktest.Run(t, cmd, "Pod/pay/pay-1",
		"--at="+t0.Format(time.RFC3339), "--store="+path)
	if before.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", before.Code, before.Stderr)
	}
	if strings.Contains(before.Stdout, "pay-2") {
		t.Errorf("at t0, pay-2 must not exist yet:\n%s", before.Stdout)
	}
	if !strings.Contains(before.Stdout, "source=history") ||
		!strings.Contains(before.Stdout, "at="+t0.Format(time.RFC3339)) {
		t.Errorf("summary must say source=history at=<resolved>:\n%s", before.Stdout)
	}

	after := checktest.Run(t, cmd, "Pod/pay/pay-1",
		"--at="+t0.Add(time.Minute).Format(time.RFC3339), "--store="+path)
	if after.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", after.Code, after.Stderr)
	}
	if !strings.Contains(after.Stdout, "name=pay-2") ||
		!strings.Contains(after.Stdout, "direction=lateral relation=shared-node") {
		t.Errorf("at t0+1m, pay-2 must appear as a shared-node co-tenant:\n%s", after.Stdout)
	}
	// History stores topology, not status: no readiness claims.
	if strings.Contains(after.Stdout, "ready=") {
		t.Errorf("--at answers must not claim readiness:\n%s", after.Stdout)
	}
	if before.Stdout == after.Stdout {
		t.Error("historical and later answers must differ")
	}

	// A target that does not exist at the asked instant is a runtime
	// error naming the time, with stdout clean.
	missing := checktest.Run(t, cmd, "Pod/pay/pay-2",
		"--at="+t0.Format(time.RFC3339), "--store="+path)
	if missing.Code != emit.ExitRuntime {
		t.Fatalf("missing-at-t0 target: exit %d (stdout %q)", missing.Code, missing.Stdout)
	}
	if !strings.Contains(missing.Stderr, "as of "+t0.Format(time.RFC3339)) || missing.Stdout != "" {
		t.Errorf("stderr %q / stdout %q", missing.Stderr, missing.Stdout)
	}
}

// TestRadius_UsageErrors: exit 2 with a diagnostic, stdout clean.
func TestRadius_UsageErrors(t *testing.T) {
	cmd := RadiusCommand(fakeDeps(radiusFixture()...))
	for name, args := range map[string][]string{
		"no target":          {},
		"two targets":        {"Deployment/prod/web", "--workload=Deployment/prod/web"},
		"bad kind":           {"Ingress/prod/edge"},
		"bad workload kind":  {"--workload=Ingress/prod/edge"},
		"empty segment":      {"Pod//web-1"},
		"zero depth":         {"Deployment/prod/web", "--depth=0"},
		"at without store":   {"Deployment/prod/web", "--at=20m"},
		"namespace conflict": {"Pod/prod/web-1", "--namespace=staging"},
	} {
		res := checktest.Run(t, cmd, args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("%s: exit %d, want %d (stderr %q)", name, res.Code, emit.ExitUsage, res.Stderr)
		}
		if res.Stdout != "" {
			t.Errorf("%s: stdout must stay clean, got %q", name, res.Stdout)
		}
	}
}

// TestRadius_Contract: the §13 round-trip in both formats.
func TestRadius_Contract(t *testing.T) {
	checktest.VerifyContract(t, RadiusCommand(fakeDeps(radiusFixture()...)), "Deployment/prod/web")
	path, t0 := seedHistory(t)
	checktest.VerifyContract(t, RadiusCommand(Deps{Now: func() time.Time { return fixedNow }}),
		"Pod/pay/pay-1", "--at="+t0.Add(time.Minute).Format(time.RFC3339), "--store="+path)
}

// TestRadiusRegistered: the command is in the default registry with
// the §4.3 MCP name.
func TestRadiusRegistered(t *testing.T) {
	c, ok := checks.Lookup("triage radius")
	if !ok {
		t.Fatal("triage radius is not registered")
	}
	if c.MCPName != "k8s_blast_radius" {
		t.Errorf("MCP name = %q, want k8s_blast_radius", c.MCPName)
	}
	if !c.GraphBacked {
		t.Error("triage radius must be graph-backed (--at/--store)")
	}
}

// seedSentinelHistory writes a store the way the SENTINEL does: the
// graph feed's partial watched set (pods/nodes/replicasets), a
// Deployment present only as an owner-reference identity, and a
// ConfigMap present only as a mount reference. Returns the store path
// and the snapshot instant.
func seedSentinelHistory(t *testing.T) (path string, t0 time.Time) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "lookout.db")
	t0 = time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time { return t0 }

	st, err := store.Open(path, store.WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New(graph.Options{
		SwapInterval: -1,
		OnChange:     st.RecordGraphChange,
		Now:          clock,
		WatchedKinds: []graph.NodeKind{graph.KindPod, graph.KindNode, graph.KindReplicaSet},
	})
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "pay", Name: "web-7b9d",
		OwnerReferences: ownedBy("Deployment", "web"),
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "pay", Name: "web-7b9d-x1",
			OwnerReferences: ownedBy("ReplicaSet", "web-7b9d"),
		},
		Spec: corev1.PodSpec{
			NodeName:   "n1",
			Containers: []corev1.Container{{Name: "app", Image: "img:v1"}},
			Volumes: []corev1.Volume{{
				Name:         "cfg",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm-web"}}},
			}},
		},
	}
	if err := g.Writer().FromObjects(slices.Values([]any{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}, rs, pod,
	})); err != nil {
		t.Fatal(err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutGraphSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return path, t0
}

// TestRadius_At_DeploymentTarget is the M3 drill observation 3
// regression: the sentinel's graph holds Deployments identity-only
// (via ReplicaSet ownerRefs), and a historical radius centered on the
// Deployment must resolve through that owner chain instead of failing
// at every instant. It also pins observation 2 at the CLI surface:
// the restored snapshot keeps the feed's watched set, so the mounted
// ConfigMap (unwatched kind, identity-only) renders observed=unknown,
// never the pre-#46 radius.missing claim.
func TestRadius_At_DeploymentTarget(t *testing.T) {
	path, t0 := seedSentinelHistory(t)
	cmd := RadiusCommand(noClusterDeps(t))

	res := checktest.Run(t, cmd, "Deployment/pay/web",
		"--at="+t0.Format(time.RFC3339), "--store="+path)
	if res.Code != emit.ExitData {
		t.Fatalf("Deployment target must resolve through the owner chain: exit %d, stderr %q", res.Code, res.Stderr)
	}
	for _, want := range []string{"name=web-7b9d", "name=n1", "source=history"} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
	// Observation 2 at the surface: the unwatched ConfigMap is
	// unknown, not "missing".
	if !strings.Contains(res.Stdout, "name=cm-web") || !strings.Contains(res.Stdout, "observed=unknown") {
		t.Errorf("unwatched ConfigMap must render observed=unknown:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "radius.missing") {
		t.Errorf("restored watched set lost — ReferencedNotFound claimed for an unwatched kind:\n%s", res.Stdout)
	}

	// A workload genuinely absent at t errors with the actionable
	// watched-topology message naming the instant.
	missing := checktest.Run(t, cmd, "Deployment/pay/ghost",
		"--at="+t0.Format(time.RFC3339), "--store="+path)
	if missing.Code != emit.ExitRuntime {
		t.Fatalf("absent target: exit %d (stdout %q)", missing.Code, missing.Stdout)
	}
	if !strings.Contains(missing.Stderr, "watched topology") ||
		!strings.Contains(missing.Stderr, "as of "+t0.Format(time.RFC3339)) {
		t.Errorf("error must say the object was not in the watched topology at t: %q", missing.Stderr)
	}
}

// TestChanges_At_DeploymentTarget: the same owner-chain fallback
// serves `triage changes --at` — the drill's other blocked surface.
func TestChanges_At_DeploymentTarget(t *testing.T) {
	path, t0 := seedSentinelHistory(t)
	res := checktest.Run(t, ChangesCommand(noClusterDeps(t)), "Deployment/pay/web",
		"--at="+t0.Format(time.RFC3339), "--store="+path, "--since=10m")
	if res.Code != emit.ExitData {
		t.Fatalf("Deployment target must resolve: exit %d, stderr %q", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "source=history") {
		t.Errorf("summary must say source=history:\n%s", res.Stdout)
	}
}
