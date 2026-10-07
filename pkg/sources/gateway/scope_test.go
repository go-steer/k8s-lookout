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

package gateway

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// TestRun_NamespaceScopesTheDynamicInformers pins the #407 half of this
// source: its Gateway/HTTPRoute informers live on its own dynamic factory, not
// the sentinel's shared one, so --watch-scope=namespace has to reach them
// through Config.Namespaces, one informer pair per namespace. Every LIST and WATCH must name one of them — a
// cluster-wide one would be a 403 under a namespaced Role, and an informer
// retrying a 403 never syncs.
func TestRun_NamespaceScopesTheDynamicInformers(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.Resources = []*metav1.APIResourceList{{
		GroupVersion: gatewayGV.String(),
		APIResources: []metav1.APIResource{{Name: "gateways", Namespaced: true}, {Name: "httproutes", Namespaced: true}},
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gatewayGVR:   "GatewayList",
		httprouteGVR: "HTTPRouteList",
	})
	cfg := DefaultConfig()
	cfg.Namespaces = []string{"team-a", "team-b"}
	s := New(client, dyn, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(engine.Signal) {}) }()
	for !s.HasSynced() {
		select {
		case <-ctx.Done():
			t.Fatal("gateway source never synced in namespace scope")
		case err := <-done:
			t.Fatalf("Run returned before syncing: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	var seen int
	for _, a := range dyn.Actions() {
		if a.GetVerb() != "list" && a.GetVerb() != "watch" {
			continue
		}
		seen++
		if ns := a.GetNamespace(); ns != "team-a" && ns != "team-b" {
			t.Errorf("%s %s in namespace %q, want team-a or team-b", a.GetVerb(), a.GetResource().Resource, a.GetNamespace())
		}
	}
	if seen == 0 {
		t.Fatal("no list/watch recorded; the assertion above proved nothing")
	}
}
