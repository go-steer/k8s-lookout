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

package bundle_test

// §13 testing conventions: fake.Clientset fixture clusters, a
// fixture PodLogGetter for the log streams, exact findings asserted
// on the broken workload (golden), the healthy workload proving
// zero nominal state per section, and the checktest contract
// round-trip.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var fixedNow = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

// fakeLogs is the fixture PodLogGetter, content per
// "namespace/pod/container" (the logs package's own test seam,
// §13: the fake clientset cannot serve log streams).
type fakeLogs struct {
	streams map[string]string
	errs    map[string]error
}

func (f *fakeLogs) Stream(_ context.Context, ns, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	key := ns + "/" + pod + "/" + opts.Container
	if err := f.errs[key]; err != nil {
		return nil, err
	}
	s, ok := f.streams[key]
	if !ok {
		return nil, fmt.Errorf("no log fixture for %s", key)
	}
	return io.NopCloser(strings.NewReader(s)), nil
}

func testCommand(logs *fakeLogs, objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return bundle.New(bundle.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Logs:   logs,
		Now:    func() time.Time { return fixedNow },
	})
}

const (
	ns   = "prod"
	hash = "7c9d8"
)

func deployment(ready, updated, available int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api", Labels: map[string]string{"app": "api"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(2)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "api",
					Image: "registry.example.com/api:v2",
				}}},
			},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: ready, UpdatedReplicas: updated, AvailableReplicas: available},
	}
}

func replicaSet() *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: "api-" + hash,
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api"}},
		},
	}
}

// apiPod is a running pod of the workload; broken toggles the second
// container state to ImagePullBackOff and drops readiness.
func apiPod(name string, broken bool) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			Labels:          map[string]string{"app": "api", "pod-template-hash": hash},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "api-" + hash}},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name:  "api",
				Image: "registry.example.com/api:v2",
				Env: []corev1.EnvVar{{
					Name: "LOG_LEVEL",
					ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "app-config"}, Key: "log.level"}},
				}},
			}},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: ptr(metav1.Time{Time: fixedNow.Add(-time.Hour)}),
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "api", Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Time{Time: fixedNow.Add(-time.Hour)}}},
			}},
		},
	}
	if broken {
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "api", Ready: false,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  "ImagePullBackOff",
				Message: `Back-off pulling image "registry.example.com/api:v3-typo"`,
			}},
			Image: "registry.example.com/api:v3-typo",
		}}
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	}
	return p
}

func configMap(keys ...string) *corev1.ConfigMap {
	data := map[string]string{}
	for _, k := range keys {
		data[k] = "v"
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "app-config"}, Data: data}
}

func service() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "api"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80}},
		},
	}
}

func slice(pods ...string) *discoveryv1.EndpointSlice {
	s := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: "api-abc12",
			Labels: map[string]string{discoveryv1.LabelServiceName: "api"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	for _, p := range pods {
		ready := true
		s.Endpoints = append(s.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: p},
		})
	}
	return s
}

func serviceAccount() *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "default"}}
}

func node() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		}},
	}
}

func ptr[T any](v T) *T { return &v }

// brokenObjects is the fixture the golden test and the contract test
// share: one pod healthy but referencing a ConfigMap key that does
// not exist, one pod in ImagePullBackOff, and the Deployment's
// rollout stuck at 1/2 — every bundle section has something to say.
func brokenObjects() []runtime.Object {
	return []runtime.Object{
		deployment(1, 2, 1),
		replicaSet(),
		apiPod("api-"+hash+"-aaaaa", false),
		apiPod("api-"+hash+"-bbbbb", true),
		configMap("other.key"), // log.level is missing
		service(),
		slice("api-" + hash + "-aaaaa"),
		serviceAccount(),
		node(),
	}
}

func brokenLogs() *fakeLogs {
	return &fakeLogs{
		streams: map[string]string{
			"prod/api-" + hash + "-aaaaa/api": strings.Join([]string{
				`2026-07-01T08:00:00.000000000Z INFO handled request path=/api/cart/123 status=200 dur=15ms`,
				`2026-07-01T08:00:01.000000000Z INFO handled request path=/api/cart/456 status=200 dur=9ms`,
				`2026-07-01T08:00:02.000000000Z ERROR config key log.level missing, using default`,
			}, "\n") + "\n",
		},
		errs: map[string]error{
			"prod/api-" + hash + "-bbbbb/api": fmt.Errorf(`container "api" in pod "api-` + hash + `-bbbbb" is waiting to start: trying and failing to pull image`),
		},
	}
}

func TestRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("bundle")
	if !ok {
		t.Fatal("bundle is not registered in the default registry")
	}
	if c.MCPName != "k8s_triage_workload" {
		t.Errorf("MCP name = %q, want k8s_triage_workload", c.MCPName)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("registered command invalid: %v", err)
	}
}

// TestComposedGlossaryComplete asserts the composition sources are
// registered and their glossaries reached the bundle declaration —
// the contract Verify below depends on the union being complete.
func TestComposedGlossaryComplete(t *testing.T) {
	b, ok := checks.Lookup("bundle")
	if !ok {
		t.Fatal("bundle not registered")
	}
	declared := map[string]bool{}
	for _, f := range b.Output {
		declared[f.Name] = true
	}
	for _, name := range []string{"triage spec", "triage delta", "triage logs", "state edges"} {
		c, ok := checks.Lookup(name)
		if !ok {
			t.Fatalf("composition source %q not registered", name)
		}
		for _, f := range c.Output {
			if !declared[f.Name] {
				t.Errorf("field %q of %q missing from bundle's glossary", f.Name, name)
			}
		}
	}
}

func TestBrokenWorkloadGolden(t *testing.T) {
	res := checktest.Run(t, testCommand(brokenLogs(), brokenObjects()...), "--workload=Deployment/prod/api")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	golden := filepath.Join("testdata", "bundle-broken.golden")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(golden, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_GOLDEN=1 to create)", err)
	}
	if res.Stdout != string(want) {
		t.Errorf("golden mismatch:\ngot:\n%s\nwant:\n%s", res.Stdout, want)
	}
}

// TestBrokenWorkloadSections asserts every section is present and
// carries its expected diagnosis, independent of the golden bytes.
func TestBrokenWorkloadSections(t *testing.T) {
	res := checktest.Run(t, testCommand(brokenLogs(), brokenObjects()...), "--workload=Deployment/prod/api")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, want := range []string{
		"kind=bundle.target",
		"section=spec",
		"kind=spec.resource",
		"section=delta",
		"kind=pod.imagepull",
		"kind=workload.rollout",
		"section=edges",
		"kind=edge.missing_key",
		"section=radius",
		"kind=radius.neighbor",
		"section=logs",
		"kind=log.template",
		"kind=log.fetch_error",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("bundle output missing %q:\n%s", want, res.Stdout)
		}
	}
}

// TestHealthyWorkloadMinimal proves zero nominal state per section:
// a healthy workload's bundle is the head, the spec, the radius
// neighborhood, and nothing else — no delta, edge, or log-template
// findings.
func TestHealthyWorkloadMinimal(t *testing.T) {
	logs := &fakeLogs{streams: map[string]string{
		"prod/api-" + hash + "-aaaaa/api": "",
		"prod/api-" + hash + "-bbbbb/api": "",
	}}
	objs := []runtime.Object{
		deployment(2, 2, 2),
		replicaSet(),
		apiPod("api-"+hash+"-aaaaa", false),
		apiPod("api-"+hash+"-bbbbb", false),
		configMap("log.level"),
		service(),
		slice("api-"+hash+"-aaaaa", "api-"+hash+"-bbbbb"),
		serviceAccount(),
		node(),
	}
	res := checktest.Run(t, testCommand(logs, objs...), "--workload=Deployment/prod/api")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, section := range []string{"section=delta", "section=edges", "section=logs"} {
		if strings.Contains(res.Stdout, section) {
			t.Errorf("healthy workload must not emit %s findings:\n%s", section, res.Stdout)
		}
	}
	// The healthy bundle is explicit, not silent: the head, the spec,
	// and the radius neighborhood still orient the agent.
	for _, want := range []string{"kind=bundle.target", "section=spec", "section=radius"} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("healthy bundle still answers with %q:\n%s", want, res.Stdout)
		}
	}
}

// TestIncidentPayloadResolvesOwnerChain feeds the inject payload of
// the broken pod and expects the bundle to target the owning
// Deployment (Pod → ReplicaSet → Deployment).
func TestIncidentPayloadResolvesOwnerChain(t *testing.T) {
	incident := `{"kind":"k8s-event","reason":"BackOff","namespace":"prod",` +
		`"kind_of_object":"Pod","name":"api-` + hash + `-bbbbb",` +
		`"context":{"controller_ref":"ReplicaSet/api-` + hash + `"}}`
	res := checktest.Run(t, testCommand(brokenLogs(), brokenObjects()...), "--incident="+incident)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	first := strings.SplitN(res.Stdout, "\n", 2)[0]
	if !strings.Contains(first, "kind_of_object=Deployment") || !strings.Contains(first, "name=api") {
		t.Errorf("incident should resolve to the owning Deployment, head = %s", first)
	}
}

// TestIncidentPayloadObjectGoneFallsBackToController deletes the
// incident pod; the payload's controller_ref must carry the target.
func TestIncidentPayloadObjectGoneFallsBackToController(t *testing.T) {
	objs := []runtime.Object{
		deployment(1, 2, 1),
		replicaSet(),
		apiPod("api-"+hash+"-aaaaa", false),
		configMap("log.level"),
		service(),
		slice("api-" + hash + "-aaaaa"),
		serviceAccount(),
		node(),
	}
	logs := &fakeLogs{streams: map[string]string{"prod/api-" + hash + "-aaaaa/api": ""}}
	incident := `{"namespace":"prod","kind_of_object":"Pod","name":"api-` + hash + `-gone",` +
		`"context":{"controller_ref":"ReplicaSet/api-` + hash + `"}}`
	res := checktest.Run(t, testCommand(logs, objs...), "--incident="+incident)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	first := strings.SplitN(res.Stdout, "\n", 2)[0]
	if !strings.Contains(first, "kind_of_object=Deployment") {
		t.Errorf("controller_ref fallback should resolve to the Deployment, head = %s", first)
	}
}

func TestUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no target", nil, "no target"},
		{"both targets", []string{"--workload=Deployment/prod/api", "--incident={}"}, "not both"},
		{"bad incident json", []string{"--incident=not-json"}, "inject payload JSON"},
		{"incident missing fields", []string{"--incident={}"}, "needs namespace"},
		{"namespace contradiction", []string{"--workload=Deployment/prod/api", "--namespace=dev"}, "contradicts"},
		{"bad depth", []string{"--workload=Deployment/prod/api", "--depth=0"}, "--depth"},
		{"bad max-templates", []string{"--workload=Deployment/prod/api", "--max-templates=0"}, "--max-templates"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, testCommand(brokenLogs(), brokenObjects()...), tc.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit = %d, want %d (stderr %q)", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr %q should contain %q", res.Stderr, tc.want)
			}
			if res.Stdout != "" {
				t.Errorf("usage errors must keep stdout clean, got %q", res.Stdout)
			}
		})
	}
}

func TestUnknownWorkloadIsRuntimeError(t *testing.T) {
	res := checktest.Run(t, testCommand(brokenLogs(), brokenObjects()...), "--workload=Deployment/prod/nonesuch")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit = %d, want %d", res.Code, emit.ExitRuntime)
	}
	if !strings.Contains(res.Stderr, "not found") {
		t.Errorf("stderr = %q, want a not-found diagnosis", res.Stderr)
	}
}

func TestVerifyContract(t *testing.T) {
	checktest.VerifyContract(t, testCommand(brokenLogs(), brokenObjects()...), "--workload=Deployment/prod/api")
	healthyLogs := &fakeLogs{streams: map[string]string{
		"prod/api-" + hash + "-aaaaa/api": "",
		"prod/api-" + hash + "-bbbbb/api": "",
	}}
	objs := []runtime.Object{
		deployment(2, 2, 2), replicaSet(),
		apiPod("api-"+hash+"-aaaaa", false), apiPod("api-"+hash+"-bbbbb", false),
		configMap("log.level"), service(), slice("api-"+hash+"-aaaaa", "api-"+hash+"-bbbbb"), serviceAccount(), node(),
	}
	checktest.VerifyContract(t, testCommand(healthyLogs, objs...), "--workload=Deployment/prod/api")
}
