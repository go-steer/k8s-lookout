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

package inventory

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// noDiscovery is the Deps a built-in-only resolution must run under:
// reaching discovery at all is the failure.
var noDiscovery = Deps{Discovery: func(context.Context) (discovery.DiscoveryInterface, error) {
	panic("built-in kind resolution reached discovery")
}}

// TestDefaultKindsResolveWithoutDiscovery: the default listing must
// work on a cluster whose aggregated APIs are broken — the moment an
// operator most wants to know what is in a namespace is not the moment
// to depend on a discovery round trip.
func TestDefaultKindsResolveWithoutDiscovery(t *testing.T) {
	got, err := resolve(context.Background(), noDiscovery, defaultKinds)
	if err != nil {
		t.Fatalf("resolve(defaultKinds): %v", err)
	}
	if len(got) != len(defaultKinds) {
		t.Fatalf("resolved %d kinds, want %d", len(got), len(defaultKinds))
	}
	for i, k := range got {
		if k.gvr.Resource != defaultKinds[i] {
			t.Errorf("kind %d = %q, want %q (order is the truncation order)", i, k.gvr.Resource, defaultKinds[i])
		}
		if !k.namespaced {
			t.Errorf("%s is cluster-scoped and does not belong in the default set", k.kind)
		}
	}
}

// TestReplicaSetsAreOptIn pins the inherited decision: one per
// Deployment revision would swamp the listing, but they stay reachable.
func TestReplicaSetsAreOptIn(t *testing.T) {
	for _, k := range defaultKinds {
		if k == "replicasets" {
			t.Fatal("replicasets are in the default set; they would be the bulk of every listing")
		}
	}
	got, err := resolve(context.Background(), noDiscovery, []string{"replicasets"})
	if err != nil {
		t.Fatalf("resolve(replicasets): %v", err)
	}
	if len(got) != 1 || got[0].kind != "ReplicaSet" {
		t.Fatalf("resolve(replicasets) = %+v", got)
	}
}

func TestKindSpellings(t *testing.T) {
	for _, tc := range []struct{ token, want string }{
		{"pods", "Pod"},
		{"pod", "Pod"},
		{"po", "Pod"},
		{"Pods", "Pod"}, // callers paste kubectl output
		{"deploy", "Deployment"},
		{"svc", "Service"},
		{"ep", "Endpoints"},
		{"netpol", "NetworkPolicy"},
		{"hpa", "HorizontalPodAutoscaler"},
		{"pdb", "PodDisruptionBudget"},
		{"pvc", "PersistentVolumeClaim"},
		{"deployments.apps", "Deployment"},
		{"pods.", "Pod"}, // the core group, spelled explicitly
	} {
		got, err := resolve(context.Background(), noDiscovery, []string{tc.token})
		if err != nil {
			t.Errorf("resolve(%q): %v", tc.token, err)
			continue
		}
		if got[0].kind != tc.want {
			t.Errorf("resolve(%q) = %s, want %s", tc.token, got[0].kind, tc.want)
		}
	}
}

func TestKindsAreDedupedInCallerOrder(t *testing.T) {
	got, err := resolve(context.Background(), noDiscovery, []string{"services", "po", "pods", " svc ", "pod"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	var kinds []string
	for _, k := range got {
		kinds = append(kinds, k.kind)
	}
	if strings.Join(kinds, ",") != "Service,Pod" {
		t.Errorf("resolve = %v, want [Service Pod]", kinds)
	}
}

// TestUnknownKindIsAUsageError: returning the other kinds as though
// nothing happened would hide a typo behind a plausible listing, and
// the caller would read the missing kind as an absence.
func TestUnknownKindIsAUsageError(t *testing.T) {
	// A kind the built-in table does not know is only unknown once
	// discovery has also declined it, so this command gets a real (but
	// bare) discovery client.
	client := k8sfake.NewSimpleClientset()
	cmd := newCommand(Deps{
		Dynamic:   func(context.Context) (dynamic.Interface, error) { return newDynamic(t, allListKinds()), nil },
		Discovery: func(context.Context) (discovery.DiscoveryInterface, error) { return client.Discovery(), nil },
	}, func() time.Time { return testNow })
	for _, token := range []string{"widgets", "pods!", "--all", "Pods/foo", ","} {
		res := checktest.Run(t, cmd, "--namespace=shop", "--kinds="+token)
		if res.Code != emit.ExitUsage {
			t.Errorf("--kinds=%q: exit %d, want %d (usage)", token, res.Code, emit.ExitUsage)
		}
	}
}

// TestUnknownKindResolvesThroughDiscovery is the CRD path: an operator
// asking for `certificates` on a cert-manager cluster gets them, and
// the built-in table stays small.
func TestUnknownKindResolvesThroughDiscovery(t *testing.T) {
	certGVR := schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
	client := k8sfake.NewSimpleClientset()
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = []*metav1.APIResourceList{{
		GroupVersion: "cert-manager.io/v1",
		APIResources: []metav1.APIResource{
			{Name: "certificates", SingularName: "certificate", Kind: "Certificate", Namespaced: true, ShortNames: []string{"cert"}},
			{Name: "certificates/status", Kind: "Certificate", Namespaced: true},
		},
	}}
	cert := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"name": "shop-tls", "namespace": "shop", "creationTimestamp": ago(6 * time.Hour),
		},
	}}
	listKinds := allListKinds()
	listKinds[certGVR] = "CertificateList"
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds)
	if err := dyn.Tracker().Create(certGVR, cert, "shop"); err != nil {
		t.Fatal(err)
	}
	cmd := newCommand(Deps{
		Dynamic:   func(context.Context) (dynamic.Interface, error) { return dyn, nil },
		Discovery: func(context.Context) (discovery.DiscoveryInterface, error) { return client.Discovery(), nil },
	}, func() time.Time { return testNow })

	for _, token := range []string{"certificates", "cert", "certificate", "certificates.cert-manager.io"} {
		res := checktest.Run(t, cmd, "--namespace=shop", "--kinds="+token)
		if res.Code != emit.ExitData {
			t.Fatalf("--kinds=%s: exit %d, stderr: %s", token, res.Code, res.Stderr)
		}
		// A kind with no formatter is target + age: for a CRD,
		// existence IS the fact, and inventing columns for a schema we
		// do not know would be the diagnosis this command refuses.
		if !strings.Contains(res.Stdout, "target=Certificate/shop/shop-tls age=6h\n") {
			t.Errorf("--kinds=%s did not list the CRD:\n%s", token, res.Stdout)
		}
	}
}

func TestCompactAge(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{-time.Second, "0s"},
		{45 * time.Second, "45s"},
		{90 * time.Second, "1m30s"},
		{5 * time.Minute, "5m"},
		{90 * time.Minute, "1h30m"},
		{3 * time.Hour, "3h"},
		{3*time.Hour + 20*time.Minute, "3h20m"},
		{47 * time.Hour, "47h"},
		{12 * 24 * time.Hour, "12d"},
	} {
		if got := compactAge(tc.d); got != tc.want {
			t.Errorf("compactAge(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}
