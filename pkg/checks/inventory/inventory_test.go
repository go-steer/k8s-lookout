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
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// leakMarker is the fixture-credential tripwire (§13): every planted
// secret value in this file carries it, and no output may. A listing
// walks Secrets by design, so this is the one command where the
// tripwire is load-bearing rather than belt-and-braces.
const leakMarker = "SUPERSECRETVALUE"

// testNow pins the clock the ages are rendered against; fixtures date
// themselves backwards from it.
var testNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func ago(d time.Duration) string { return testNow.Add(-d).UTC().Format(time.RFC3339) }

// ---- fixtures ---------------------------------------------------------------

// obj builds an unstructured fixture: identity plus whatever top-level
// sections (spec, status, data, …) the kind's line reads.
func obj(apiVersion, kind, ns, name string, age time.Duration, body map[string]any) *unstructured.Unstructured {
	meta := map[string]any{"name": name, "creationTimestamp": ago(age)}
	if ns != "" {
		meta["namespace"] = ns
	}
	o := map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": meta}
	for k, v := range body {
		o[k] = v
	}
	return &unstructured.Unstructured{Object: o}
}

// allListKinds registers every built-in kind with the dynamic fake, so
// a default listing does not fail on a kind the test forgot.
func allListKinds() map[schema.GroupVersionResource]string {
	m := map[schema.GroupVersionResource]string{}
	for _, k := range builtins {
		m[k.gvr] = k.kind + "List"
	}
	return m
}

// newDynamic seeds the fake through its tracker keyed by the GVR from
// the built-in table, NOT by letting the fake guess a resource name
// from the kind: the guess pluralizes Endpoints to "endpointses" and
// the object would then be invisible to the very listing under test.
func newDynamic(t *testing.T, listKinds map[schema.GroupVersionResource]string, objs ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	t.Helper()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds)
	for _, o := range objs {
		var found bool
		for _, k := range builtins {
			if k.kind != o.GetKind() {
				continue
			}
			if err := dyn.Tracker().Create(k.gvr, o, o.GetNamespace()); err != nil {
				t.Fatalf("seeding %s/%s: %v", o.GetKind(), o.GetName(), err)
			}
			found = true
			break
		}
		if !found {
			t.Fatalf("fixture kind %q is not in the built-in table", o.GetKind())
		}
	}
	return dyn
}

// listCmd builds the command over a dynamic fake carrying the given
// objects. Discovery panics if reached: the default listing resolves
// every kind from the built-in table and must never pay for a
// discovery round trip.
func listCmd(t *testing.T, objs ...*unstructured.Unstructured) checks.Command {
	t.Helper()
	return cmdFor(newDynamic(t, allListKinds(), objs...))
}

func cmdFor(dyn dynamic.Interface) checks.Command {
	return newCommand(Deps{
		Dynamic: func() (dynamic.Interface, error) { return dyn, nil },
		Discovery: func() (discovery.DiscoveryInterface, error) {
			panic("built-in kind resolution reached discovery")
		},
	}, func() time.Time { return testNow })
}

// storefront is the mixed fixture namespace: a healthy Deployment
// behind a Service, a broken one, and one object of every kind the
// default listing covers.
func storefront() []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		obj("apps/v1", "Deployment", "storefront", "web", 12*24*time.Hour, map[string]any{
			"spec":   map[string]any{"replicas": int64(2)},
			"status": map[string]any{"readyReplicas": int64(2), "updatedReplicas": int64(2), "availableReplicas": int64(2)},
		}),
		obj("apps/v1", "Deployment", "storefront", "worker", 3*time.Hour, map[string]any{
			"spec":   map[string]any{"replicas": int64(3)},
			"status": map[string]any{"updatedReplicas": int64(3)},
		}),
		obj("apps/v1", "StatefulSet", "storefront", "cache", 40*24*time.Hour, map[string]any{
			"spec":   map[string]any{"replicas": int64(1)},
			"status": map[string]any{"readyReplicas": int64(1)},
		}),
		obj("apps/v1", "DaemonSet", "storefront", "log-agent", 40*24*time.Hour, map[string]any{
			"status": map[string]any{
				"desiredNumberScheduled": int64(4), "numberReady": int64(3),
				"updatedNumberScheduled": int64(4), "numberAvailable": int64(3),
			},
		}),
		obj("batch/v1", "CronJob", "storefront", "nightly-reindex", 90*24*time.Hour, map[string]any{
			"spec":   map[string]any{"schedule": "0 2 * * *", "timeZone": "UTC", "suspend": true},
			"status": map[string]any{"lastScheduleTime": ago(22 * time.Hour)},
		}),
		obj("batch/v1", "Job", "storefront", "nightly-reindex-29001600", 22*time.Hour, map[string]any{
			"spec": map[string]any{"completions": int64(1)},
			"status": map[string]any{
				"succeeded":  int64(1),
				"conditions": []any{map[string]any{"type": "Complete", "status": "True"}},
			},
		}),
		obj("v1", "Pod", "storefront", "web-6f4c9d7b8-2xqzt", 90*time.Minute, map[string]any{
			"spec": map[string]any{"containers": []any{map[string]any{"name": "web"}, map[string]any{"name": "sidecar"}}},
			"status": map[string]any{
				"phase": "Running",
				"containerStatuses": []any{
					map[string]any{"name": "web", "ready": true, "restartCount": int64(0)},
					map[string]any{"name": "sidecar", "ready": true, "restartCount": int64(2)},
				},
			},
		}),
		obj("v1", "Pod", "storefront", "worker-59d4f7c66-nq8vl", 45*time.Second, map[string]any{
			"spec": map[string]any{"containers": []any{map[string]any{"name": "worker"}}},
			"status": map[string]any{
				"phase": "Pending",
				"containerStatuses": []any{map[string]any{
					"name": "worker", "ready": false, "restartCount": int64(0),
					"state": map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff"}},
				}},
			},
		}),
		obj("v1", "Service", "storefront", "web", 12*24*time.Hour, map[string]any{
			"spec": map[string]any{
				"type": "ClusterIP", "clusterIP": "10.4.1.17",
				"selector": map[string]any{"app": "web"},
				"ports":    []any{map[string]any{"port": int64(80), "protocol": "TCP"}},
			},
		}),
		obj("v1", "Service", "storefront", "edge", 2*time.Hour, map[string]any{
			"spec": map[string]any{
				"type": "LoadBalancer", "clusterIP": "10.4.1.99",
				"selector": map[string]any{"app": "edge"},
				"ports":    []any{map[string]any{"port": int64(443), "nodePort": int64(31443), "protocol": "TCP"}},
			},
		}),
		obj("v1", "Endpoints", "storefront", "web", 12*24*time.Hour, map[string]any{
			"subsets": []any{map[string]any{"addresses": []any{
				map[string]any{"ip": "10.8.0.4"}, map[string]any{"ip": "10.8.1.7"},
			}}},
		}),
		obj("v1", "Endpoints", "storefront", "edge", 2*time.Hour, nil),
		obj("networking.k8s.io/v1", "Ingress", "storefront", "site", 12*24*time.Hour, map[string]any{
			"spec": map[string]any{
				"ingressClassName": "gce",
				"rules":            []any{map[string]any{"host": "shop.example.com"}},
			},
			"status": map[string]any{"loadBalancer": map[string]any{"ingress": []any{map[string]any{"ip": "34.120.8.9"}}}},
		}),
		obj("v1", "ConfigMap", "storefront", "web-config", 12*24*time.Hour, map[string]any{
			"data": map[string]any{"APP_MODE": "live", "TIMEOUT": "5s"},
		}),
		obj("v1", "Secret", "storefront", "web-tls", 12*24*time.Hour, map[string]any{
			"type": "kubernetes.io/tls",
			"data": map[string]any{
				"tls.crt": leakMarker + "-CERTIFICATE",
				"tls.key": leakMarker + "-PRIVATE-KEY",
			},
		}),
		obj("v1", "PersistentVolumeClaim", "storefront", "data-cache-0", 40*24*time.Hour, map[string]any{
			"spec": map[string]any{
				"volumeName": "pvc-8c2f", "storageClassName": "standard-rwo",
				"accessModes": []any{"ReadWriteOnce"},
			},
			"status": map[string]any{"phase": "Bound", "capacity": map[string]any{"storage": "20Gi"}},
		}),
		obj("autoscaling/v2", "HorizontalPodAutoscaler", "storefront", "web", 12*24*time.Hour, map[string]any{
			"spec": map[string]any{
				"scaleTargetRef": map[string]any{"kind": "Deployment", "name": "web"},
				"minReplicas":    int64(2), "maxReplicas": int64(10),
			},
			"status": map[string]any{"currentReplicas": int64(2)},
		}),
		obj("policy/v1", "PodDisruptionBudget", "storefront", "web", 12*24*time.Hour, map[string]any{
			"spec":   map[string]any{"minAvailable": "50%"},
			"status": map[string]any{"disruptionsAllowed": int64(1)},
		}),
		obj("v1", "ServiceAccount", "storefront", "default", 90*24*time.Hour, nil),
		obj("networking.k8s.io/v1", "NetworkPolicy", "storefront", "default-deny", 90*24*time.Hour, map[string]any{
			"spec": map[string]any{"podSelector": map[string]any{}},
		}),
		obj("v1", "ResourceQuota", "storefront", "compute", 90*24*time.Hour, nil),
		obj("v1", "LimitRange", "storefront", "defaults", 90*24*time.Hour, nil),
	}
}

func assertNoLeak(t *testing.T, stdout string) {
	t.Helper()
	if strings.Contains(stdout, leakMarker) {
		t.Errorf("SECRET LEAK: a %s marker reached stdout:\n%s", leakMarker, stdout)
	}
}

// ---- the contract -----------------------------------------------------------

func TestContract(t *testing.T) {
	checktest.VerifyContract(t, listCmd(t, storefront()...), "--namespace=storefront")
}

func TestMixedNamespaceGolden(t *testing.T) {
	res := checktest.Run(t, listCmd(t, storefront()...), "--namespace=storefront")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	assertNoLeak(t, res.Stdout)
	checktest.Golden(t, "testdata/storefront.golden", res.Stdout)
}

func TestRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("triage list")
	if !ok {
		t.Fatal("triage list is not registered in the default registry")
	}
	// The MCP name is the agent-facing contract from #252 and does not
	// track the CLI name.
	if c.MCPName != "k8s_list_resources" {
		t.Errorf("MCPName = %q, want k8s_list_resources", c.MCPName)
	}
}

// ---- property 1: every line opens with a usable target ----------------------

// TestEveryLineLeadsWithATarget is the reason the command exists: the
// rest of the read surface takes <Kind>/<namespace>/<name>, and until
// now nothing produced one. A line without a target is a line the
// caller cannot act on.
func TestEveryLineLeadsWithATarget(t *testing.T) {
	res := checktest.Run(t, listCmd(t, storefront()...), "--namespace=storefront")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, line := range findingLines(t, res.Stdout) {
		target, ok := field(line, "target")
		if !ok {
			t.Errorf("line has no target: %s", line)
			continue
		}
		parts := strings.Split(target, "/")
		if len(parts) != 3 || parts[0] == "" || parts[1] != "storefront" || parts[2] == "" {
			t.Errorf("target %q is not <Kind>/storefront/<name>: %s", target, line)
		}
		// Spelled identically to the envelope, which is what makes it
		// paste-compatible with `triage spec` and SubjectKey alike.
		if kind, _ := field(line, "kind_of_object"); kind != parts[0] {
			t.Errorf("target kind %q disagrees with kind_of_object %q", parts[0], kind)
		}
		if name, _ := field(line, "name"); name != parts[2] {
			t.Errorf("target name %q disagrees with name %q", parts[2], name)
		}
	}
}

// TestClusterScopedTargetOmitsNamespace: `triage spec` rejects a
// namespace segment on a cluster-scoped kind, so emitting one would
// hand the caller a target that fails on paste.
func TestClusterScopedTargetOmitsNamespace(t *testing.T) {
	node := obj("v1", "Node", "", "gke-prod-pool-1-8f2a", 30*24*time.Hour, map[string]any{
		"metadata": map[string]any{
			"name":              "gke-prod-pool-1-8f2a",
			"creationTimestamp": ago(30 * 24 * time.Hour),
			"labels":            map[string]any{"node-role.kubernetes.io/worker": ""},
		},
		"status": map[string]any{
			"conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
			"nodeInfo":   map[string]any{"kubeletVersion": "v1.31.4-gke.1183000"},
		},
	})
	res := checktest.Run(t, listCmd(t, node), "--kinds=nodes")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "target=Node/gke-prod-pool-1-8f2a ") {
		t.Errorf("cluster-scoped target is not <Kind>/<name>:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "roles=worker") || !strings.Contains(res.Stdout, "status=Ready") {
		t.Errorf("node line lost its kubectl columns:\n%s", res.Stdout)
	}
}

// ---- property 2: it is an inventory, not a check ----------------------------

// TestItDoesNotDiagnose is the property that keeps this command from
// eating the rest of the surface, pinned on the exact case that
// motivated #252: a Service whose selector matches nothing, in front
// of a perfectly healthy Deployment.
//
// The listing must NAME both objects and judge neither. "This
// Service's selector matches no pods" is `state edges`' answer and it
// is a better one; if the enumeration says it too, the caller stops
// making the second call — and gets a worse answer for every case
// `state edges` handles that a selector-vs-labels string compare does
// not.
func TestItDoesNotDiagnose(t *testing.T) {
	objs := []*unstructured.Unstructured{
		obj("apps/v1", "Deployment", "shop", "api", 5*24*time.Hour, map[string]any{
			"spec":   map[string]any{"replicas": int64(2)},
			"status": map[string]any{"readyReplicas": int64(2), "updatedReplicas": int64(2), "availableReplicas": int64(2)},
		}),
		obj("v1", "Service", "shop", "api", 5*24*time.Hour, map[string]any{
			"spec": map[string]any{
				"type": "ClusterIP", "clusterIP": "10.4.2.8",
				// The typo that breaks routing: the pods are app=api.
				"selector": map[string]any{"app": "api-v2"},
				"ports":    []any{map[string]any{"port": int64(8080), "protocol": "TCP"}},
			},
		}),
		obj("v1", "Endpoints", "shop", "api", 5*24*time.Hour, nil),
		obj("v1", "Pod", "shop", "api-7d9c4b-x2n8p", 5*24*time.Hour, map[string]any{
			"metadata": map[string]any{
				"name": "api-7d9c4b-x2n8p", "namespace": "shop",
				"creationTimestamp": ago(5 * 24 * time.Hour),
				"labels":            map[string]any{"app": "api"},
			},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "api"}}},
			"status": map[string]any{
				"phase":             "Running",
				"containerStatuses": []any{map[string]any{"name": "api", "ready": true, "restartCount": int64(0)}},
			},
		}),
	}
	res := checktest.Run(t, listCmd(t, objs...), "--namespace=shop")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}

	// It names what exists — that is the whole deliverable.
	for _, want := range []string{"target=Service/shop/api ", "target=Deployment/shop/api ", "target=Endpoints/shop/api "} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("listing did not name %q:\n%s", want, res.Stdout)
		}
	}
	// And it withholds every judgement.
	if strings.Contains(res.Stdout, "selector") {
		t.Errorf("a Service selector reached the output — that check belongs to `state edges`:\n%s", res.Stdout)
	}
	for _, line := range findingLines(t, res.Stdout) {
		if _, ok := field(line, "reason"); ok {
			t.Errorf("inventory line carries a reason: %s", line)
		}
		if _, ok := field(line, "message"); ok {
			t.Errorf("inventory line carries a message: %s", line)
		}
		if sev, _ := field(line, "severity"); sev != "info" {
			t.Errorf("inventory line is graded %q, want info: %s", sev, line)
		}
	}
	// The facts a diagnosis would be BUILT from are all present, which
	// is what makes the next call worth making.
	if !strings.Contains(res.Stdout, "addresses=0") {
		t.Errorf("the Endpoints address count is missing — the caller has nothing to notice:\n%s", res.Stdout)
	}
}

// ---- property 3: no Secret values, ever -------------------------------------

// TestSecretsAreCountedNeverRead: a listing walks every Secret in the
// namespace, so a single careless field would turn the read surface
// into an exfiltration tool.
func TestSecretsAreCountedNeverRead(t *testing.T) {
	secrets := []*unstructured.Unstructured{
		obj("v1", "Secret", "shop", "api-token", time.Hour, map[string]any{
			"type": "Opaque",
			"data": map[string]any{"token": leakMarker + "-TOKEN", "password": leakMarker + "-PASSWORD"},
		}),
		obj("v1", "Secret", "shop", "empty", time.Hour, map[string]any{"type": "Opaque"}),
	}
	res := checktest.Run(t, listCmd(t, secrets...), "--namespace=shop", "--kinds=secrets")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	assertNoLeak(t, res.Stdout)
	if strings.Contains(res.Stdout, "token=") || strings.Contains(res.Stdout, "password=") {
		t.Errorf("a Secret key name reached the output as a field:\n%s", res.Stdout)
	}
	// The count is what kubectl prints, and it separates a populated
	// Secret from an empty one without revealing anything.
	if !strings.Contains(res.Stdout, "target=Secret/shop/api-token type=Opaque keys=2") {
		t.Errorf("populated Secret line wrong:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "target=Secret/shop/empty type=Opaque keys=0") {
		t.Errorf("empty Secret line wrong:\n%s", res.Stdout)
	}
}

// ---- property 4: a refusal is a result, not an error ------------------------

// TestForbiddenKindIsReportedNotFatal: an operator role that may list
// sixteen kinds and not Secrets still gets sixteen kinds. The
// difference between "no Secret here" and "I was not allowed to look"
// is a blind spot the caller must be told about, so it lands on the
// summary line rather than being swallowed.
func TestForbiddenKindIsReportedNotFatal(t *testing.T) {
	dyn := newDynamic(t, allListKinds(), storefront()...)
	dyn.PrependReactor("list", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Resource: "secrets"}, "", errors.New("secrets are not readable by this role"))
	})
	res := checktest.Run(t, cmdFor(dyn), "--namespace=storefront")
	if res.Code != emit.ExitData {
		t.Fatalf("a forbidden kind failed the run: exit %d, stderr: %s", res.Code, res.Stderr)
	}
	summary := summaryLine(t, res.Stdout)
	if !strings.Contains(summary, "skipped=Secret:forbidden") {
		t.Errorf("the blind spot is not on the summary line: %s", summary)
	}
	if !strings.Contains(summary, "kinds=17") {
		t.Errorf("kinds= should count the kinds actually listed: %s", summary)
	}
	if strings.Contains(res.Stdout, "target=Secret/") {
		t.Errorf("a Secret was listed despite the refusal:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "target=Deployment/storefront/web ") {
		t.Errorf("one refusal cost the caller the other kinds:\n%s", res.Stdout)
	}
}

// TestTruncationSaysSo: a silently short listing is worse than no
// listing, because absence is exactly the signal the caller is reading
// it for.
func TestTruncationSaysSo(t *testing.T) {
	res := checktest.Run(t, listCmd(t, storefront()...), "--namespace=storefront", "--max=3")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if got := len(findingLines(t, res.Stdout)); got != 3 {
		t.Errorf("emitted %d lines under --max=3", got)
	}
	summary := summaryLine(t, res.Stdout)
	if !strings.Contains(summary, "truncated=19") {
		t.Errorf("summary does not say what was left out: %s", summary)
	}
	// scanned= stays the true total: the caller needs the size of the
	// namespace, not the size of the page.
	if !strings.Contains(summary, "scanned=22 findings=3") {
		t.Errorf("scanned/findings wrong under truncation: %s", summary)
	}
	// Workloads survive truncation; configuration is what falls off.
	if !strings.Contains(res.Stdout, "target=Deployment/storefront/web ") {
		t.Errorf("truncation dropped the workloads first:\n%s", res.Stdout)
	}
}

// TestAbsentNamespaceIsDistinguishedFromEmpty: an empty listing alone
// cannot tell "nothing is deployed here" from "you typed the namespace
// wrong", and those call for opposite next moves.
func TestAbsentNamespaceIsDistinguishedFromEmpty(t *testing.T) {
	res := checktest.Run(t, listCmd(t), "--namespace=ghost")
	if res.Code != emit.ExitData {
		t.Fatalf("a missing namespace is a result, not an error: exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if summary := summaryLine(t, res.Stdout); !strings.Contains(summary, "namespace_absent=true") {
		t.Errorf("summary does not say the namespace is missing: %s", summary)
	}
}

func TestEmptyNamespaceIsNotReportedAbsent(t *testing.T) {
	ns := obj("v1", "Namespace", "", "quiet", 24*time.Hour, map[string]any{
		"status": map[string]any{"phase": "Active"},
	})
	res := checktest.Run(t, listCmd(t, ns), "--namespace=quiet")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if strings.Contains(res.Stdout, "namespace_absent") {
		t.Errorf("an existing but empty namespace was reported absent:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "findings=0") {
		t.Errorf("an empty namespace must still produce a summary:\n%s", res.Stdout)
	}
}

// ---- scope and usage --------------------------------------------------------

func TestAllNamespacesOrdersByKindThenNamespace(t *testing.T) {
	objs := []*unstructured.Unstructured{
		obj("v1", "Pod", "b-team", "b-pod", time.Hour, map[string]any{"status": map[string]any{"phase": "Running"}}),
		obj("v1", "Pod", "a-team", "a-pod", time.Hour, map[string]any{"status": map[string]any{"phase": "Running"}}),
		obj("v1", "Service", "a-team", "a-svc", time.Hour, map[string]any{"spec": map[string]any{"type": "ClusterIP", "clusterIP": "10.0.0.1"}}),
	}
	res := checktest.Run(t, listCmd(t, objs...), "-A", "--kinds=pods,services")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var targets []string
	for _, line := range findingLines(t, res.Stdout) {
		v, _ := field(line, "target")
		targets = append(targets, v)
	}
	want := []string{"Pod/a-team/a-pod", "Pod/b-team/b-pod", "Service/a-team/a-svc"}
	if strings.Join(targets, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want %v", targets, want)
	}
}

func TestWorkloadFlagIsRejected(t *testing.T) {
	res := checktest.Run(t, listCmd(t), "--workload=Deployment/shop/api")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d (usage); stdout: %s", res.Code, emit.ExitUsage, res.Stdout)
	}
	if !strings.Contains(res.Stderr, "--namespace") {
		t.Errorf("the usage error does not point at the flag that works: %s", res.Stderr)
	}
}

func TestBadNamespaceIsAUsageError(t *testing.T) {
	res := checktest.Run(t, listCmd(t), "--namespace=Prod Namespace")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d (usage)", res.Code, emit.ExitUsage)
	}
}

func TestMaxMustBePositive(t *testing.T) {
	res := checktest.Run(t, listCmd(t), "--namespace=shop", "--max=0")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d (usage)", res.Code, emit.ExitUsage)
	}
}

// ---- helpers ----------------------------------------------------------------

func findingLines(t *testing.T, stdout string) []string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) == 0 {
		t.Fatal("no output at all")
	}
	return lines[:len(lines)-1]
}

func summaryLine(t *testing.T, stdout string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	last := lines[len(lines)-1]
	if !strings.HasPrefix(last, "scanned=") {
		t.Fatalf("last line is not the summary: %q", last)
	}
	return last
}

// field reads one logfmt key from a line. Values in this command's
// output never contain a space, so a split is enough.
func field(line, key string) (string, bool) {
	for _, tok := range strings.Fields(line) {
		if k, v, ok := strings.Cut(tok, "="); ok && k == key {
			return v, true
		}
	}
	return "", false
}
