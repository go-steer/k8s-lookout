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

package checks_test

// §13 tests for `triage spec`: fake.Clientset + dynamic fake
// fixtures, golden files pinning the exact token-dense rendering,
// the credential tripwire (no SUPERSECRETVALUE_* marker may survive
// on stdout), the contract sweep, and the honest-unavailability
// check for --diff (M3).

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// leakMarker is the fixture-credential tripwire prefix (§13): every
// planted secret in this file carries it, and no output may.
const leakMarker = "SUPERSECRETVALUE"

func boolPtr(b bool) *bool    { return &b }
func int32Ptr(i int32) *int32 { return &i }

// specCmd builds a `triage spec` command over a typed fake. The
// dynamic client errors if reached — typed-path tests must not fall
// through to it.
func specCmd(objs ...runtime.Object) checks.Command {
	client := k8sfake.NewSimpleClientset(objs...)
	return checks.SpecCommand(checks.SpecDeps{
		Typed: func() (kubernetes.Interface, error) { return client, nil },
		Dynamic: func() (dynamic.Interface, error) {
			panic("typed-path test reached the dynamic client")
		},
	})
}

// specCmdDynamic builds the command with discovery resources and a
// dynamic fake serving the given custom-resource objects.
func specCmdDynamic(t *testing.T, resources []*metav1.APIResourceList,
	listKinds map[schema.GroupVersionResource]string, objs ...runtime.Object) checks.Command {
	t.Helper()
	client := k8sfake.NewSimpleClientset()
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = resources
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
	return checks.SpecCommand(checks.SpecDeps{
		Typed:   func() (kubernetes.Interface, error) { return client, nil },
		Dynamic: func() (dynamic.Interface, error) { return dyn, nil },
	})
}

func assertNoLeak(t *testing.T, stdout string) {
	t.Helper()
	if strings.Contains(stdout, leakMarker) {
		t.Errorf("SECRET LEAK: a %s marker reached stdout:\n%s", leakMarker, stdout)
	}
}

var specTransition = metav1.NewTime(time.Date(2026, 7, 24, 9, 30, 0, 0, time.UTC))

// fixturePod plants a credential in every pod position the renderer
// touches: literal env value under a credential name, an init
// container's token, plus healthy AND abnormal conditions.
func fixturePod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "payments-api-7d9c4b-x2n8p",
			Namespace: "prod",
			Labels:    map[string]string{"app": "payments"},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: "payments-api-7d9c4b", Controller: boolPtr(true)},
			},
		},
		Spec: corev1.PodSpec{
			NodeName:           "gke-prod-pool-1-8f2a",
			ServiceAccountName: "payments",
			Volumes: []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "payments-config"}}}},
				{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: "payments-tls"}}},
				{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
			InitContainers: []corev1.Container{{
				Name:  "migrate",
				Image: "registry.example.com/migrate:v3",
				Env:   []corev1.EnvVar{{Name: "MIGRATE_DB_TOKEN", Value: leakMarker + "_INIT"}},
			}},
			Containers: []corev1.Container{{
				Name:  "api",
				Image: "registry.example.com/payments:v1.4.2",
				Ports: []corev1.ContainerPort{{Name: "https", ContainerPort: 8443, Protocol: corev1.ProtocolTCP}},
				Env: []corev1.EnvVar{
					{Name: "DB_PASSWORD", Value: leakMarker + "_ENV"},
					{Name: "LOG_LEVEL", Value: "debug"},
					{Name: "API_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "payments-secrets"}, Key: "api-key"}}},
					{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP"}}},
				},
				EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "payments-env"}}}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
					Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/healthz", Port: intstr.FromInt32(8443)}},
					InitialDelaySeconds: 5,
					PeriodSeconds:       10,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.FromInt32(8443)}},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				// Healthy: must be elided (zero nominal state).
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: specTransition},
				// Abnormal: must surface as a warning finding.
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady",
					Message: "containers with unready status: [api]", LastTransitionTime: specTransition},
			},
		},
	}
}

func TestSpecPodGolden(t *testing.T) {
	cmd := specCmd(fixturePod())
	res := checktest.Run(t, cmd, "Pod/prod/payments-api-7d9c4b-x2n8p")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/spec-pod.golden", res.Stdout)
	assertNoLeak(t, res.Stdout)
	if !strings.Contains(res.Stdout, "DB_PASSWORD=[REDACTED]") {
		t.Errorf("credential env var should render as name + [REDACTED]:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "PodScheduled") {
		t.Errorf("healthy condition must be elided:\n%s", res.Stdout)
	}

	// The contract sweep in both formats, and the JSON stream is
	// leak-checked too.
	checktest.VerifyContract(t, cmd, "Pod/prod/payments-api-7d9c4b-x2n8p")
	assertNoLeak(t, checktest.Run(t, cmd, "--format=json", "Pod/prod/payments-api-7d9c4b-x2n8p").Stdout)
}

// TestSpecKindAliases: the documented short names and lowercased
// full kinds resolve to the same read.
func TestSpecKindAliases(t *testing.T) {
	want := checktest.Run(t, specCmd(fixturePod()), "Pod/prod/payments-api-7d9c4b-x2n8p").Stdout
	for _, ref := range []string{
		"po/prod/payments-api-7d9c4b-x2n8p",
		"pod/prod/payments-api-7d9c4b-x2n8p",
		"po/payments-api-7d9c4b-x2n8p", // 2-part + --namespace
	} {
		args := []string{ref}
		if strings.Count(ref, "/") == 1 {
			args = append(args, "--namespace=prod")
		}
		res := checktest.Run(t, specCmd(fixturePod()), args...)
		if res.Code != emit.ExitData {
			t.Fatalf("%s: exit = %d, stderr: %s", ref, res.Code, res.Stderr)
		}
		if res.Stdout != want {
			t.Errorf("%s: output diverges from canonical reference:\n%s", ref, res.Stdout)
		}
	}
}

func TestSpecDeploymentGolden(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "api", Namespace: "prod",
			Labels: map[string]string{"app": "payments"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(3),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "payments"}},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge:       &intstr.IntOrString{Type: intstr.String, StrVal: "25%"},
					MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
				},
			},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "api",
					Image: "registry.example.com/payments:v1.4.2",
					Env:   []corev1.EnvVar{{Name: "SIGNING_KEY", Value: leakMarker + "_TEMPLATE"}},
				}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			Conditions: []appsv1.DeploymentCondition{
				{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, LastTransitionTime: specTransition},
				{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse, Reason: "MinimumReplicasUnavailable",
					Message: "Deployment does not have minimum availability.", LastTransitionTime: specTransition},
			},
		},
	}
	res := checktest.Run(t, specCmd(dep), "deploy/prod/api")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/spec-deployment.golden", res.Stdout)
	assertNoLeak(t, res.Stdout)
	if strings.Contains(res.Stdout, "Progressing") {
		t.Errorf("healthy condition must be elided:\n%s", res.Stdout)
	}
	checktest.VerifyContract(t, specCmd(dep), "Deployment/prod/api")
}

func TestSpecServiceGolden(t *testing.T) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "prod",
			Labels: map[string]string{"app": "payments"}},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP, // default: elided
			Selector: map[string]string{"app": "payments"},
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP},
				{Name: "https", Port: 443, TargetPort: intstr.FromString("tls"), Protocol: corev1.ProtocolTCP},
			},
		},
	}
	res := checktest.Run(t, specCmd(svc), "svc/prod/payments")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/spec-service.golden", res.Stdout)
	if strings.Contains(res.Stdout, "ClusterIP") {
		t.Errorf("default service type must be elided:\n%s", res.Stdout)
	}
	checktest.VerifyContract(t, specCmd(svc), "Service/prod/payments")
}

// TestSpecSecretGolden is the sharpest tripwire: data KEYS render
// with sizes, VALUES never appear in any encoding.
func TestSpecSecretGolden(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "db-credentials", Namespace: "prod"},
		Type:       corev1.SecretTypeOpaque, // default: elided
		Data: map[string][]byte{
			"password": []byte(leakMarker + "_PW"),
			"username": []byte("admin"),
		},
	}
	res := checktest.Run(t, specCmd(secret), "Secret/prod/db-credentials")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/spec-secret.golden", res.Stdout)
	assertNoLeak(t, res.Stdout)
	// Neither raw nor base64 form may survive; "admin" is planted
	// as a value, not a key, so it must vanish too.
	for _, needle := range []string{"admin", "YWRtaW4"} {
		if strings.Contains(res.Stdout, needle) {
			t.Errorf("secret value %q reached stdout:\n%s", needle, res.Stdout)
		}
	}
	if !strings.Contains(res.Stdout, "password(19B)") {
		t.Errorf("secret keys should render with decoded sizes:\n%s", res.Stdout)
	}
	checktest.VerifyContract(t, specCmd(secret), "Secret/prod/db-credentials")
}

func TestSpecConfigMapKeysOnly(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-config", Namespace: "prod"},
		Data: map[string]string{
			"app.conf":  "listen :8080\nupstream db.prod.svc\n",
			"log.level": "debug",
		},
	}
	res := checktest.Run(t, specCmd(cm), "cm/prod/payments-config")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/spec-configmap.golden", res.Stdout)
	if strings.Contains(res.Stdout, "listen :8080") {
		t.Errorf("configmap values must not render (keys only):\n%s", res.Stdout)
	}
}

// TestSpecDynamicFallback: a kind outside the typed table resolves
// via discovery and reads through the dynamic client; its spec
// renders as flattened path=value pairs.
func TestSpecDynamicFallback(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
	cert := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata":   map[string]any{"name": "api-tls", "namespace": "prod"},
		"spec": map[string]any{
			"secretName": "api-tls",
			"dnsNames":   []any{"api.example.com", "www.api.example.com"},
		},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "False", "reason": "DoesNotExist",
			"message": "Issuing certificate as Secret does not exist",
		}}},
	}}
	resources := []*metav1.APIResourceList{{
		GroupVersion: "cert-manager.io/v1",
		APIResources: []metav1.APIResource{{Name: "certificates", Kind: "Certificate", Namespaced: true}},
	}}
	listKinds := map[schema.GroupVersionResource]string{gvr: "CertificateList"}

	cmd := specCmdDynamic(t, resources, listKinds, cert)
	res := checktest.Run(t, cmd, "Certificate/prod/api-tls")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/spec-certificate.golden", res.Stdout)
	checktest.VerifyContract(t, specCmdDynamic(t, resources, listKinds, cert), "Certificate/prod/api-tls")

	// Unknown kind: runtime error naming the kind, exit 1.
	res = checktest.Run(t, specCmdDynamic(t, resources, listKinds), "FooBar/prod/x")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, `unknown kind "FooBar"`) {
		t.Errorf("unknown kind: exit=%d stderr=%q", res.Code, res.Stderr)
	}
}

func TestSpecClusterScoped(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "gke-prod-pool-1-8f2a",
			Labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-b"}},
		Spec: corev1.NodeSpec{PodCIDR: "10.8.4.0/24"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			// Pressure polarity: True is the abnormal state.
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionTrue, Reason: "KubeletHasInsufficientMemory",
				Message: "kubelet has insufficient memory available", LastTransitionTime: specTransition},
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastTransitionTime: specTransition},
		}},
	}
	res := checktest.Run(t, specCmd(node), "no/gke-prod-pool-1-8f2a")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/spec-node.golden", res.Stdout)
	if !strings.Contains(res.Stdout, "MemoryPressure=True") {
		t.Errorf("pressure condition at True must surface:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "Ready=True") {
		t.Errorf("healthy Ready condition must be elided:\n%s", res.Stdout)
	}

	// Kind/ns/name on a cluster-scoped kind is a usage error.
	res = checktest.Run(t, specCmd(node), "Node/prod/gke-prod-pool-1-8f2a")
	if res.Code != emit.ExitUsage || !strings.Contains(res.Stderr, "cluster-scoped") {
		t.Errorf("namespaced ref to Node: exit=%d stderr=%q", res.Code, res.Stderr)
	}
}

func TestSpecNamespaceDefaulting(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "app-config", Namespace: "default"},
		Data:       map[string]string{"k": "v"},
	}
	// 2-part ref without --namespace falls back to "default".
	res := checktest.Run(t, specCmd(cm), "cm/app-config")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	// With --namespace pointing elsewhere, the read misses: the
	// scope namespace is honored, not silently ignored.
	res = checktest.Run(t, specCmd(cm), "cm/app-config", "--namespace=prod")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, "not found") {
		t.Errorf("scoped miss: exit=%d stderr=%q", res.Code, res.Stderr)
	}
}

// TestSpecWorkloadFlag: --workload=<Kind>/<ns>/<name> is equivalent
// to the 3-part positional.
func TestSpecWorkloadFlag(t *testing.T) {
	want := checktest.Run(t, specCmd(fixturePod()), "Pod/prod/payments-api-7d9c4b-x2n8p").Stdout
	res := checktest.Run(t, specCmd(fixturePod()), "--workload=Pod/prod/payments-api-7d9c4b-x2n8p")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if res.Stdout != want {
		t.Errorf("--workload output diverges from positional:\n%s", res.Stdout)
	}
}

func TestSpecTargetUsageErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"no target", nil, "no target"},
		{"both positional and --workload",
			[]string{"Pod/prod/x", "--workload=Pod/prod/x"}, "not both"},
		{"bare kind", []string{"Pod"}, "invalid resource reference"},
		{"too many segments", []string{"Pod/a/b/c"}, "invalid resource reference"},
		{"empty segment", []string{"Pod//x"}, "invalid resource reference"},
		{"-A rejected", []string{"Pod/prod/x", "-A"}, "exactly one resource"},
		{"--diff unimplemented", []string{"Pod/prod/x", "--diff"}, "not yet implemented (§6.6)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := checktest.Run(t, specCmd(fixturePod()), tt.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit = %d, want %d (stderr: %q)", res.Code, emit.ExitUsage, res.Stderr)
			}
			if res.Stdout != "" {
				t.Errorf("stdout must stay clean on usage errors, got %q", res.Stdout)
			}
			if !strings.Contains(res.Stderr, tt.wantErr) {
				t.Errorf("stderr = %q, want substring %q", res.Stderr, tt.wantErr)
			}
		})
	}
}

// TestSpecHelpGolden pins the generated --help — it documents the
// alias table and the M3 status of --diff, both agent-facing
// contracts.
func TestSpecHelpGolden(t *testing.T) {
	checktest.Golden(t, "testdata/spec-help.golden", specCmd().Help())
}

// TestSpecRegisteredInDefaultRegistry: the production command is
// mounted with the designed names.
func TestSpecRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("triage spec")
	if !ok {
		t.Fatal("triage spec is not in the default registry")
	}
	if c.MCPName != "k8s_resource_spec" {
		t.Errorf("MCPName = %q, want k8s_resource_spec", c.MCPName)
	}
	if c.Positional == nil {
		t.Error("triage spec must declare its positional argument")
	}
}
