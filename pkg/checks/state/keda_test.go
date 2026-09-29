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

package state_test

// `state keda` tests. As with `state gateway`, the CRD gate and the
// status-driven claim are what is new; the chain rule (one cause, not
// three consequences) is what is easiest to get wrong. All helpers
// are keda-prefixed.

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const kedaAPIVersion = "keda.sh/v1alpha1"

var kedaGV = schema.GroupVersion{Group: "keda.sh", Version: "v1alpha1"}

// kedaGVR is spelled out for the same reason as gwGVR: the dynamic
// fake's pluralizer is a guess.
var kedaGVR = map[string]schema.GroupVersionResource{
	"ScaledObject":                 kedaGV.WithResource("scaledobjects"),
	"ScaledJob":                    kedaGV.WithResource("scaledjobs"),
	"TriggerAuthentication":        kedaGV.WithResource("triggerauthentications"),
	"ClusterTriggerAuthentication": kedaGV.WithResource("clustertriggerauthentications"),
}

var kedaAllResources = []string{"scaledobjects", "scaledjobs", "triggerauthentications", "clustertriggerauthentications"}

// kedaCommand wires `state keda` over a fake cluster: served names the
// KEDA resources discovery advertises (nil means all four), crdObjs
// are the unstructured KEDA objects, core the built-in workloads.
func kedaCommand(t *testing.T, served []string, crdObjs []*unstructured.Unstructured, core ...runtime.Object) checks.Command {
	t.Helper()
	cs := fake.NewClientset(core...)
	if served == nil {
		served = kedaAllResources
	}
	if len(served) > 0 {
		list := &metav1.APIResourceList{GroupVersion: kedaAPIVersion}
		for _, r := range served {
			list.APIResources = append(list.APIResources, metav1.APIResource{Name: r})
		}
		cs.Resources = []*metav1.APIResourceList{list}
	}
	listKinds := map[schema.GroupVersionResource]string{}
	for kind, gvr := range kedaGVR {
		listKinds[gvr] = kind + "List"
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds)
	for _, obj := range crdObjs {
		gvr, ok := kedaGVR[obj.GetKind()]
		if !ok {
			t.Fatalf("no GVR for kind %q", obj.GetKind())
		}
		if err := dyn.Tracker().Create(gvr, obj, obj.GetNamespace()); err != nil {
			t.Fatalf("seed %s %s: %v", obj.GetKind(), obj.GetName(), err)
		}
	}
	return state.KEDACommand(state.Deps{
		Client:  func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Dynamic: func(context.Context) (dynamic.Interface, error) { return dyn, nil },
		Now:     func() time.Time { return fixedNow },
	})
}

// kedaObj builds one KEDA object; spec and status are applied verbatim
// so a test can leave status out entirely (an unreconciled scaler).
func kedaObj(kind, namespace, name string, spec, status map[string]any) *unstructured.Unstructured {
	meta := map[string]any{"name": name}
	if namespace != "" {
		meta["namespace"] = namespace
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kedaAPIVersion,
		"kind":       kind,
		"metadata":   meta,
	}}
	if spec != nil {
		u.Object["spec"] = spec
	}
	if status != nil {
		u.Object["status"] = status
	}
	return u
}

// kedaConds is a status carrying the given conditions.
func kedaConds(conds ...any) map[string]any { return map[string]any{"conditions": conds} }

// kedaReady is the status of a scaler KEDA is happy with.
func kedaReady() map[string]any {
	return kedaConds(gwCond("Ready", "True", "ScaledObjectReady", "ScaledObject is defined correctly and is ready for scaling"))
}

// kedaTrigger is one trigger, authenticated through authKind/authName
// when authName is non-empty (authKind "" means KEDA's default).
func kedaTrigger(typ, authKind, authName string) any {
	tr := map[string]any{"type": typ, "metadata": map[string]any{}}
	if authName != "" {
		ref := map[string]any{"name": authName}
		if authKind != "" {
			ref["kind"] = authKind
		}
		tr["authenticationRef"] = ref
	}
	return tr
}

// kedaScaledObject scales target (a <Kind>/<name> ref, Kind "" for
// KEDA's Deployment default) on the given triggers.
func kedaScaledObject(namespace, name string, target map[string]any, status map[string]any, triggers ...any) *unstructured.Unstructured {
	return kedaObj("ScaledObject", namespace, name, map[string]any{"scaleTargetRef": target, "triggers": triggers}, status)
}

func kedaScaledJob(namespace, name string, status map[string]any, triggers ...any) *unstructured.Unstructured {
	return kedaObj("ScaledJob", namespace, name, map[string]any{"jobTargetRef": map[string]any{}, "triggers": triggers}, status)
}

func kedaDeployment(namespace, name string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

func kedaStatefulSet(namespace, name string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// kedaHealthyCluster is KEDA in ordinary use: a ScaledObject over a
// Deployment with its TriggerAuthentication, one over a StatefulSet
// with a ClusterTriggerAuthentication, and a ScaledJob.
func kedaHealthyCluster() (crdObjs []*unstructured.Unstructured, core []runtime.Object) {
	return []*unstructured.Unstructured{
			kedaObj("TriggerAuthentication", ns, "prom-auth", map[string]any{}, nil),
			kedaObj("ClusterTriggerAuthentication", "", "kafka-auth", map[string]any{}, nil),
			kedaScaledObject(ns, "api", map[string]any{"name": "api"}, kedaReady(),
				kedaTrigger("prometheus", "", "prom-auth")),
			kedaScaledObject(ns, "consumer", map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "consumer"}, kedaReady(),
				kedaTrigger("kafka", "ClusterTriggerAuthentication", "kafka-auth")),
			kedaScaledJob(ns, "batch", kedaReady(), kedaTrigger("rabbitmq", "", "prom-auth")),
		}, []runtime.Object{
			kedaDeployment(ns, "api"),
			kedaStatefulSet(ns, "consumer"),
		}
}

func TestKEDAHealthyIsSilent(t *testing.T) {
	crdObjs, core := kedaHealthyCluster()
	res := checktest.Run(t, kedaCommand(t, nil, crdObjs, core...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	// 5 KEDA objects + 1 Deployment + 1 StatefulSet.
	if got := strings.TrimSpace(res.Stdout); !strings.HasPrefix(got, "scanned=7 findings=0 ") {
		t.Errorf("healthy cluster should emit only a summary: %q", res.Stdout)
	}
}

func TestKEDANotInstalled(t *testing.T) {
	res := checktest.Run(t, kedaCommand(t, []string{}, nil))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d — an absent CRD is a degradation, not an error; stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want one finding and a summary, got %q", res.Stdout)
	}
	want := `kind=crd.unavailable severity=info reason=APIGroupNotServed message="KEDA is not installed: the keda.sh/v1alpha1 API group is not served by this cluster — install KEDA (keda.sh), or enable a managed offering that bundles it" api_group=keda.sh/v1alpha1 resources=scaledobjects,scaledjobs,triggerauthentications,clustertriggerauthentications`
	if lines[0] != want {
		t.Errorf("finding\n got: %s\nwant: %s", lines[0], want)
	}
	if !strings.HasPrefix(lines[1], "scanned=0 findings=1 ") {
		t.Errorf("nothing was examined: %s", lines[1])
	}
}

// With KEDA installed and unused, the workloads are never listed.
func TestKEDAUnusedListsNoWorkloads(t *testing.T) {
	res := checktest.Run(t, kedaCommand(t, nil, nil, kedaDeployment(ns, "api")))
	if got := strings.TrimSpace(res.Stdout); !strings.HasPrefix(got, "scanned=0 findings=0 ") {
		t.Errorf("an unused KEDA install should scan nothing: %q", res.Stdout)
	}
}

func TestKEDAMissingTarget(t *testing.T) {
	for name, target := range map[string]map[string]any{
		"defaulted Deployment": {"name": "api"},
		"explicit StatefulSet": {"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "api"},
	} {
		t.Run(name, func(t *testing.T) {
			crdObjs := []*unstructured.Unstructured{
				// Not Ready too, as KEDA would say: the chain reports the
				// cause once.
				kedaScaledObject(ns, "api", target,
					kedaConds(gwCond("Ready", "False", "ScaledObjectCheckFailed", "Target resource doesn't exist")),
					kedaTrigger("cpu", "", "")),
			}
			// A workload of the other kind with the same name must not
			// satisfy the ref.
			var core runtime.Object = kedaStatefulSet(ns, "api")
			if target["kind"] == "StatefulSet" {
				core = kedaDeployment(ns, "api")
			}
			got := gwFindings(t, kedaCommand(t, nil, crdObjs, core))
			if len(got) != 1 {
				t.Fatalf("want exactly the missing target, got %v", got)
			}
			wantKind := "Deployment"
			if target["kind"] == "StatefulSet" {
				wantKind = "StatefulSet"
			}
			for _, want := range []string{
				"kind=keda.missing_target severity=warning namespace=prod kind_of_object=ScaledObject name=api reason=ScaleTargetNotFound",
				"target=" + wantKind + "/api",
			} {
				if !strings.Contains(got[0], want) {
					t.Errorf("missing %q in %s", want, got[0])
				}
			}
		})
	}
}

// A target outside apps/v1 Deployments and StatefulSets is not
// resolved here; KEDA's Ready condition is the only judge.
func TestKEDAOtherTargetKindsAreNotResolved(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		kedaScaledObject(ns, "rollout", map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "Rollout", "name": "web"}, kedaReady()),
		kedaScaledObject(ns, "custom", map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web"}, kedaReady()),
	}
	if got := gwFindings(t, kedaCommand(t, nil, crdObjs)); len(got) != 0 {
		t.Errorf("unresolvable target kinds must be silent, got %v", got)
	}
}

func TestKEDAMissingAuth(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		kedaObj("TriggerAuthentication", "staging", "prom-auth", map[string]any{}, nil),
		kedaScaledObject(ns, "api", map[string]any{"name": "api"},
			kedaConds(gwCond("Ready", "False", "ScaledObjectCheckFailed", "error getting scaler")),
			// A TriggerAuthentication is namespaced: one of the same
			// name elsewhere does not count.
			kedaTrigger("prometheus", "", "prom-auth"),
			kedaTrigger("kafka", "ClusterTriggerAuthentication", "kafka-auth"),
			// The same missing ref twice is one finding.
			kedaTrigger("prometheus", "TriggerAuthentication", "prom-auth")),
	}
	got := gwFindings(t, kedaCommand(t, nil, crdObjs, kedaDeployment(ns, "api")))
	if len(got) != 2 {
		t.Fatalf("want one finding per missing ref, got %v", got)
	}
	for i, want := range []string{
		"trigger=prometheus authentication=TriggerAuthentication/prom-auth",
		"trigger=kafka authentication=ClusterTriggerAuthentication/kafka-auth",
	} {
		if !strings.Contains(got[i], "kind=keda.missing_auth severity=warning") || !strings.Contains(got[i], want) {
			t.Errorf("finding %d: want %q in %s", i, want, got[i])
		}
	}
}

// A ref to a kind whose resource is not served cannot be judged: the
// check did not look, so it does not claim absence.
func TestKEDAAuthUnjudgedWhenNotServed(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		kedaScaledObject(ns, "api", map[string]any{"name": "api"}, kedaReady(),
			kedaTrigger("kafka", "ClusterTriggerAuthentication", "kafka-auth")),
	}
	res := checktest.Run(t, kedaCommand(t, []string{"scaledobjects", "scaledjobs", "triggerauthentications"}, crdObjs, kedaDeployment(ns, "api")))
	if strings.Contains(res.Stdout, "keda.missing_auth") {
		t.Errorf("an unserved auth kind must not be reported missing:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "not_served=clustertriggerauthentications") {
		t.Errorf("summary should name what could not be read: %q", res.Stdout)
	}
}

func TestKEDANotReady(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		kedaObj("TriggerAuthentication", ns, "prom-auth", map[string]any{}, nil),
		kedaScaledObject(ns, "api", map[string]any{"name": "api"},
			kedaConds(gwCond("Ready", "False", "ScaledObjectCheckFailed", "failed to query prometheus: connection refused")),
			kedaTrigger("prometheus", "", "prom-auth")),
		kedaScaledJob(ns, "batch",
			kedaConds(gwCond("Ready", "False", "", "error getting queue length"))),
	}
	got := gwFindings(t, kedaCommand(t, nil, crdObjs, kedaDeployment(ns, "api")))
	want := []string{
		`kind=keda.not_ready severity=warning namespace=prod kind_of_object=ScaledJob name=batch reason=ScalerNotReady message="KEDA reports the ScaledJob not ready (Ready: error getting queue length) — scaling has stopped at the current size" condition="Ready=False"`,
		`kind=keda.not_ready severity=warning namespace=prod kind_of_object=ScaledObject name=api reason=ScalerNotReady message="KEDA reports the ScaledObject not ready (ScaledObjectCheckFailed: failed to query prometheus: connection refused) — scaling has stopped at the current size" condition="Ready=False"`,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("finding %d\n got: %s\nwant: %s", i, got[i], want[i])
		}
	}
}

// Not-yet-reconciled, still-reconciling and deliberately paused
// scalers have not failed.
func TestKEDANotReadySilentCases(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		kedaScaledObject(ns, "fresh", map[string]any{"name": "api"}, nil),
		kedaScaledObject(ns, "unknown", map[string]any{"name": "api"},
			kedaConds(gwCond("Ready", "Unknown", "", ""))),
		kedaScaledObject(ns, "paused", map[string]any{"name": "api"},
			kedaConds(gwCond("Ready", "False", "ScaledObjectCheckFailed", "paused"), gwCond("Paused", "True", "ScaledObjectPaused", ""))),
	}
	if got := gwFindings(t, kedaCommand(t, nil, crdObjs, kedaDeployment(ns, "api"))); len(got) != 0 {
		t.Errorf("want silence, got %v", got)
	}
}

func TestKEDANamespaceScoping(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		kedaScaledObject(ns, "api", map[string]any{"name": "gone-a"}, nil),
		kedaScaledObject("staging", "api", map[string]any{"name": "gone-b"}, nil),
	}
	got := gwFindings(t, kedaCommand(t, nil, crdObjs), "--namespace="+ns)
	if len(got) != 1 || !strings.Contains(got[0], "namespace=prod") {
		t.Fatalf("want only the prod finding, got %v", got)
	}
}

func TestKEDAWorkloadIsUsageError(t *testing.T) {
	res := checktest.Run(t, kedaCommand(t, nil, nil), "--workload=Deployment/prod/api")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d", res.Code, emit.ExitUsage)
	}
	if !strings.Contains(res.Stderr, "cluster-wide") {
		t.Errorf("error should explain the scope: %s", res.Stderr)
	}
}

// kedaMixed is one of each failure alongside working scalers.
func kedaMixed() (crdObjs []*unstructured.Unstructured, core []runtime.Object) {
	crdObjs, core = kedaHealthyCluster()
	crdObjs = append(crdObjs,
		kedaScaledObject(ns, "renamed", map[string]any{"name": "web-v1"}, nil, kedaTrigger("cpu", "", "")),
		kedaScaledObject(ns, "orders", map[string]any{"name": "api"}, nil, kedaTrigger("aws-sqs-queue", "", "sqs-auth")),
		kedaScaledJob("jobs", "nightly",
			kedaConds(gwCond("Ready", "False", "ScaledJobCheckFailed", "error parsing redis metadata"))),
	)
	return crdObjs, core
}

func TestKEDAMixedGolden(t *testing.T) {
	crdObjs, core := kedaMixed()
	res := checktest.Run(t, kedaCommand(t, nil, crdObjs, core...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/keda-mixed.golden", res.Stdout)
}

func TestKEDAContract(t *testing.T) {
	crdObjs, core := kedaMixed()
	checktest.VerifyContract(t, kedaCommand(t, nil, crdObjs, core...))
}

func TestKEDAUnavailableContract(t *testing.T) {
	checktest.VerifyContract(t, kedaCommand(t, []string{}, nil))
}

func TestKEDARegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("state keda")
	if !ok {
		t.Fatal("state keda is not registered in the default registry")
	}
	if c.MCPName != "k8s_keda_scalers" {
		t.Errorf("MCP tool name = %q, want k8s_keda_scalers", c.MCPName)
	}
}
