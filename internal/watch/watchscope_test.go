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

package watch

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// --watch-scope=namespace (issue #407). The fixture below is a ServiceAccount
// holding a namespaced Role in scopeNS and nothing cluster-wide: every
// SelfSubjectAccessReview outside scopeNS is denied, and — the part that makes
// these tests able to fail — every cluster-wide LIST or WATCH is refused with
// 403, the way the API server refuses it. An informer that issues one never
// syncs, exactly as on a real cluster, so a scope leak shows up here as a
// timeout rather than as a pass.

const scopeNS = "team-a"

// roleOnlyClient builds the fixture: discovery that knows which resources
// are namespaced, an authorizer that grants scopeNS only, and a 403 on every
// cluster-wide read.
func roleOnlyClient(objs ...runtime.Object) *fake.Clientset {
	return scopedClient([]string{scopeNS}, nil, objs...)
}

// scopedClient is the general fixture: Roles in allowNS, plus cluster-wide
// read of the resources in clusterAllow (the hybrid tier grants nodes and
// persistentvolumes). Everything else is refused, list and watch included.
func scopedClient(allowNS, clusterAllow []string, objs ...runtime.Object) *fake.Clientset {
	permitted := func(ns, resource string) bool {
		if ns == "" {
			return slices.Contains(clusterAllow, resource)
		}
		return slices.Contains(allowNS, ns)
	}
	client := fake.NewSimpleClientset(objs...)
	client.Resources = []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "pods", Namespaced: true}, {Name: "pods/log", Namespaced: true},
			{Name: "events", Namespaced: true}, {Name: "services", Namespaced: true},
			{Name: "configmaps", Namespaced: true}, {Name: "secrets", Namespaced: true},
			{Name: "serviceaccounts", Namespaced: true}, {Name: "persistentvolumeclaims", Namespaced: true},
			{Name: "nodes"}, {Name: "nodes/proxy"}, {Name: "persistentvolumes"}, {Name: "namespaces"},
		}},
		{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{
			{Name: "deployments", Namespaced: true}, {Name: "replicasets", Namespaced: true},
			{Name: "statefulsets", Namespaced: true}, {Name: "daemonsets", Namespaced: true},
		}},
		{GroupVersion: "batch/v1", APIResources: []metav1.APIResource{
			{Name: "jobs", Namespaced: true}, {Name: "cronjobs", Namespaced: true},
		}},
		{GroupVersion: "autoscaling/v2", APIResources: []metav1.APIResource{
			{Name: "horizontalpodautoscalers", Namespaced: true},
		}},
		{GroupVersion: "discovery.k8s.io/v1", APIResources: []metav1.APIResource{
			{Name: "endpointslices", Namespaced: true},
		}},
		{GroupVersion: "policy/v1", APIResources: []metav1.APIResource{
			{Name: "poddisruptionbudgets", Namespaced: true},
		}},
		{GroupVersion: "networking.k8s.io/v1", APIResources: []metav1.APIResource{
			{Name: "ingresses", Namespaced: true}, {Name: "networkpolicies", Namespaced: true}, {Name: "ingressclasses"},
		}},
		{GroupVersion: "metrics.k8s.io/v1beta1", APIResources: []metav1.APIResource{
			{Name: "pods", Namespaced: true}, {Name: "nodes"},
		}},
		{GroupVersion: "admissionregistration.k8s.io/v1", APIResources: []metav1.APIResource{
			{Name: "validatingwebhookconfigurations"}, {Name: "mutatingwebhookconfigurations"},
		}},
	}
	client.PrependReactor("create", "selfsubjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		review := a.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		attrs := review.Spec.ResourceAttributes
		allowed := attrs != nil && permitted(attrs.Namespace, attrs.Resource)
		review.Status = authorizationv1.SubjectAccessReviewStatus{Allowed: allowed}
		return true, review, nil
	})
	forbidden := func(a k8stesting.Action) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: a.GetResource().Group, Resource: a.GetResource().Resource}, "",
			errors.New("the fixture does not grant this"))
	}
	client.PrependReactor("list", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if !permitted(a.GetNamespace(), a.GetResource().Resource) {
			return true, nil, forbidden(a)
		}
		return false, nil, nil
	})
	client.PrependWatchReactor("*", func(a k8stesting.Action) (bool, watch.Interface, error) {
		if !permitted(a.GetNamespace(), a.GetResource().Resource) {
			return true, nil, forbidden(a)
		}
		return false, nil, nil
	})
	return client
}

func warningEvent(ns, name string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-" + ns + "-" + name)},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: name + "-pod", UID: types.UID("pod-" + ns + "-" + name)},
		Reason:         "BackOff",
		Message:        "Back-off restarting failed container",
		Type:           corev1.EventTypeWarning,
		Count:          1,
	}
}

func namespaceScopeFlags(t *testing.T, extra ...string) *flags {
	t.Helper()
	f, err := parseFlags(append([]string{"--dry-run", "--watch-scope=namespace", "--namespace=" + scopeNS}, extra...))
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return f
}

// TestWatchScopeNamespace_RoleOnlyStartsSyncsAndStaysInside is the issue's
// acceptance test: under a Role-only grant the sentinel resolves auto, skips
// every node-dependent source with one line each, turns storm off, passes the
// §11 probe, syncs (bounded — a hang fails), and emits for its namespace and
// nothing else.
func TestWatchScopeNamespace_RoleOnlyStartsSyncsAndStaysInside(t *testing.T) {
	client := roleOnlyClient(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: scopeNS, Name: "web"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "web"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	)
	f := namespaceScopeFlags(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buf, restore := captureLogOutput(t)
	err := resolveAutoDefaults(ctx, f, client)
	restore()
	if err != nil {
		t.Fatalf("resolveAutoDefaults: %v", err)
	}
	logs := buf.String()

	if want := "k8s-events,rollout,workload,autoscaling,degradation,ingress"; f.sources != want {
		t.Errorf("auto resolved --sources=%s, want %s", f.sources, want)
	}
	for _, name := range []string{"object-state", "saturation", "capacity", "topology-drift"} {
		line := lineWith(logs, "source "+name+": disabled")
		if line == "" {
			t.Errorf("no skip line for %s:\n%s", name, logs)
			continue
		}
		if !strings.Contains(line, "nodes") || !strings.Contains(line, "cluster-wide") {
			t.Errorf("%s skip line does not name the cluster-wide node grant: %s", name, line)
		}
	}
	if f.storm != stormOff {
		t.Errorf("--storm=auto resolved %q, want off", f.storm)
	}
	if line := lineWith(logs, "storm: auto — off"); !strings.Contains(line, "nodes") {
		t.Errorf("storm skip line does not name nodes: %q", line)
	}

	bs, err := buildSources(f, "", client, nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	attachSharedFactories(bs, newSharedFactories(client, nil, f.scopeNamespaces()))
	if _, err := sources.Probe(ctx, newAccessReviewer(f, client), bs.registry.All()...); err != nil {
		t.Fatalf("§11 probe refused the resolved set under a Role it fits: %v", err)
	}

	var mu sync.Mutex
	var got []engine.Signal
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runErr := make(chan error, 1)
	go func() {
		runErr <- sources.RunAll(runCtx, bs.registry.All(), func(s engine.Signal) {
			mu.Lock()
			got = append(got, s)
			mu.Unlock()
		})
	}()

	waitFor(t, "every source to sync in namespace scope (a cluster-wide informer never would)", func() bool {
		ok, _ := sources.AllSynced(bs.registry.All())
		return ok
	})

	for _, ns := range []string{"other", scopeNS} {
		if _, err := client.CoreV1().Events(ns).Create(ctx, warningEvent(ns, "crash"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create event in %s: %v", ns, err)
		}
	}
	waitFor(t, "the in-namespace event to be emitted", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, s := range got {
			if s.Kind == engine.KindK8sEvent && s.Namespace == scopeNS {
				return true
			}
		}
		return false
	})
	// The out-of-namespace event was created first, so had it been
	// watched it would have arrived first too; a short grace covers any
	// reordering between the two watch channels.
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	for _, s := range got {
		if s.Namespace != scopeNS {
			t.Errorf("signal from outside the scope: kind=%s ns=%s name=%s", s.Kind, s.Namespace, s.Name)
		}
	}
	mu.Unlock()

	stop()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("RunAll: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("sources did not stop after cancel")
	}
}

// TestWatchScopeNamespace_FixtureHasTeeth proves the fixture above would
// catch a scope leak: the same k8s-events source on the cluster-scoped
// factory never syncs against it.
func TestWatchScopeNamespace_FixtureHasTeeth(t *testing.T) {
	client := roleOnlyClient()
	f := namespaceScopeFlags(t, "--sources=k8s-events")
	bs, err := buildSources(f, "", client, nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	attachSharedFactories(bs, newSharedFactories(client, nil, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = sources.RunAll(ctx, bs.registry.All(), func(engine.Signal) {}) }()
	<-ctx.Done()
	if ok, _ := sources.AllSynced(bs.registry.All()); ok {
		t.Fatal("a cluster-wide Event informer synced against the Role-only fixture; it cannot detect a scope leak")
	}
}

// TestWatchScopeNamespace_NamedNodeSourceFailsLoudly keeps §11's rule for
// explicit lists: a named source that needs nodes refuses to start, naming
// the cluster-wide grant, rather than being skipped.
func TestWatchScopeNamespace_NamedNodeSourceFailsLoudly(t *testing.T) {
	client := roleOnlyClient()
	f := namespaceScopeFlags(t, "--sources=k8s-events,object-state", "--storm=off")
	ctx := context.Background()
	if err := resolveAutoDefaults(ctx, f, client); err != nil {
		t.Fatalf("resolveAutoDefaults: %v", err)
	}
	bs, err := buildSources(f, "", client, nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	_, err = sources.Probe(ctx, newAccessReviewer(f, client), bs.registry.All()...)
	var denied *sources.DeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("Probe = %v, want a DeniedError for object-state", err)
	}
	if denied.Source != "object-state" || denied.Requirement.Resource != "nodes" || denied.Requirement.Namespace != "" {
		t.Errorf("denied %s / %v, want object-state's cluster-wide nodes requirement", denied.Source, denied.Requirement)
	}
}

// TestWatchScopeNamespace_StormOnFailsLoudly: --storm=on is explicit, so the
// node grant the topology graph needs is fatal, not a skip.
func TestWatchScopeNamespace_StormOnFailsLoudly(t *testing.T) {
	client := roleOnlyClient()
	f := namespaceScopeFlags(t)
	err := probeGraphAccess(context.Background(), newAccessReviewer(f, client))
	if err == nil || !errors.Is(err, sources.ErrAccessDenied) || !strings.Contains(err.Error(), "nodes cluster-wide") {
		t.Fatalf("probeGraphAccess = %v, want a denial naming the cluster-wide node grant", err)
	}
}

// TestNewSharedFactories_NamespaceScope pins the request shapes: namespaced
// informers list in the scope namespace, the node informer cluster-wide.
func TestNewSharedFactories_NamespaceScope(t *testing.T) {
	client := fake.NewSimpleClientset()
	sf := newSharedFactories(client, []string{"kube-system"}, []string{scopeNS})
	if !sf.Split() {
		t.Error("namespace scope shares one factory between namespaced and node informers")
	}
	sf.Namespaced.Core().V1().Pods().Informer()
	sf.Namespaced.Core().V1().Events().Informer()
	sf.Cluster.Core().V1().Nodes().Informer()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sf.Start(ctx.Done())
	sf.Namespaced.WaitForCacheSync(ctx.Done())
	sf.Cluster.WaitForCacheSync(ctx.Done())

	for _, a := range client.Actions() {
		if a.GetVerb() != "list" {
			continue
		}
		la := a.(k8stesting.ListAction)
		want := scopeNS
		if a.GetResource().Resource == "nodes" {
			want = ""
		}
		if a.GetNamespace() != want {
			t.Errorf("list %s in namespace %q, want %q", a.GetResource().Resource, a.GetNamespace(), want)
		}
		// The deny list is a post-filter under a namespace scope; a field
		// selector here would be noise in the audit log at best.
		if fs := la.GetListRestrictions().Fields.String(); fs != "" {
			t.Errorf("list %s carries field selector %q under a namespace scope", a.GetResource().Resource, fs)
		}
	}
}

// TestWatchScope_DefaultChangesNothing pins the CLI-stability half: the flag
// defaults to cluster, and the cluster scope builds exactly what a sentinel
// built before the flag existed — the plain reviewer, one shared factory.
func TestWatchScope_DefaultChangesNothing(t *testing.T) {
	f, err := parseFlags([]string{"--dry-run", "--namespace=a"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.watchScope != watchScopeCluster {
		t.Errorf("default --watch-scope = %q, want cluster", f.watchScope)
	}
	if ns := f.scopeNamespaces(); ns != nil {
		t.Errorf("cluster scope reports scope namespace %q; --namespace must stay an output filter", ns)
	}
	client := fake.NewSimpleClientset()
	if _, scoped := newAccessReviewer(f, client).(sources.RequirementScoper); scoped {
		t.Error("cluster scope wraps the reviewer; requirements must be asked exactly as declared")
	}
	if sf := newSharedFactories(client, nil, f.scopeNamespaces()); sf.Split() {
		t.Error("cluster scope split the factories")
	}
}

// TestWatchScope_Validate covers the flag's usage errors — every one exits 2.
func TestWatchScope_Validate(t *testing.T) {
	tests := []struct {
		name string
		args []string
		ok   bool
	}{
		{"default is cluster", nil, true},
		{"cluster with a namespace filter", []string{"--watch-scope=cluster", "--namespace=a,b"}, true},
		{"namespace with one namespace", []string{"--watch-scope=namespace", "--namespace=a"}, true},
		{"namespace without --namespace", []string{"--watch-scope=namespace"}, false},
		{"namespace with two namespaces", []string{"--watch-scope=namespace", "--namespace=a,b"}, true},
		{"namespace named twice is one", []string{"--watch-scope=namespace", "--namespace=a,a"}, true},
		{"a listed namespace that excludes itself", []string{"--watch-scope=namespace", "--namespace=a,b", "--exclude-namespace=b"}, false},
		{"namespace that excludes itself", []string{"--watch-scope=namespace", "--namespace=a", "--exclude-namespace=a"}, false},
		{"unknown scope", []string{"--watch-scope=namespaces", "--namespace=a"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := parseFlags(append([]string{"--dry-run"}, tc.args...))
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			err = f.validate()
			if tc.ok {
				if err != nil {
					t.Errorf("validate = %v, want ok", err)
				}
				return
			}
			if err == nil {
				t.Fatal("validate accepted it")
			}
			if !emit.IsUsageError(err) {
				t.Errorf("validate = %v, want a usage error (exit 2)", err)
			}
		})
	}
}

// TestWatchScope_UsageErrorExitsTwo runs the real entry point: a namespace
// scope without a namespace exits 2 (§4.2), before touching any cluster.
func TestWatchScope_UsageErrorExitsTwo(t *testing.T) {
	if got := Main([]string{"--dry-run", "--watch-scope=namespace"}); got != emit.ExitUsage {
		t.Errorf("Main exit = %d, want %d", got, emit.ExitUsage)
	}
}

// TestWatchScope_ExpiryFollowsTheScope: with no --expiry-namespaces the
// expiry scan narrows to the scope namespace, so the probe and the scan agree.
func TestWatchScope_ExpiryFollowsTheScope(t *testing.T) {
	if got := namespaceScopeFlags(t).expiryNamespaceList(); len(got) != 1 || got[0] != scopeNS {
		t.Errorf("expiryNamespaceList = %v, want [%s]", got, scopeNS)
	}
	if got := namespaceScopeFlags(t, "--expiry-namespaces=certs").expiryNamespaceList(); len(got) != 1 || got[0] != "certs" {
		t.Errorf("expiryNamespaceList = %v, want the explicit [certs]", got)
	}
	f, _ := parseFlags([]string{"--dry-run"})
	if got := f.expiryNamespaceList(); got != nil {
		t.Errorf("cluster scope expiryNamespaceList = %v, want nil (every namespace)", got)
	}
}

func lineWith(logs, needle string) string {
	for _, l := range strings.Split(logs, "\n") {
		if strings.Contains(l, needle) {
			return l
		}
	}
	return ""
}
