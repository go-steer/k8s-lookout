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

package audit

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func aiDeps(objs ...runtime.Object) Deps {
	cs := fake.NewClientset(objs...)
	return Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
	}
}

func healthyAIDeployment(ns, name string) *appsv1.Deployment {
	replicas := int32(1)
	labels := map[string]string{"app": name}
	gpuQty := resource.MustParse("1")
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: "inference-runner",
					Containers: []corev1.Container{{
						Name:  "vllm",
						Image: "vllm/vllm-openai:v0.5.4",
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceName("nvidia.com/gpu"): gpuQty,
							},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "model-cache",
							MountPath: "/models",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "model-cache",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "models-pvc",
							},
						},
					}},
				},
			},
		},
	}
}

func TestAICommandContract(t *testing.T) {
	dep := healthyAIDeployment("ml", "vllm-llama")
	dep.Spec.Template.Spec.HostNetwork = true
	checktest.VerifyContract(t, AICommand(aiDeps(dep)), "-A")
}

func TestAINonAIWorkloadsSkipped(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					HostNetwork: true,
					Containers:  []corev1.Container{{Name: "web", Image: "nginx:latest"}},
				},
			},
		},
	}
	cmd := AICommand(aiDeps(dep))
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	if !strings.Contains(res.Stdout, "scanned=1 findings=0 ") || !strings.Contains(res.Stdout, "ai_workloads=0") {
		t.Fatalf("expected 0 findings and ai_workloads=0, got:\n%s", res.Stdout)
	}
}

func TestAIHealthyWorkloadClean(t *testing.T) {
	dep := healthyAIDeployment("ml", "vllm-llama")
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "vllm-svc"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "vllm-llama"},
			Ports:    []corev1.ServicePort{{Port: 8000, Protocol: corev1.ProtocolTCP}},
		},
	}
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "isolate-vllm"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "vllm-llama"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		},
	}
	cmd := AICommand(aiDeps(dep, svc, np))
	res := checktest.Run(t, cmd, "--namespace=ml")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	if !strings.Contains(res.Stdout, "scanned=1 findings=0 ") || !strings.Contains(res.Stdout, "ai_workloads=1") {
		t.Fatalf("expected 0 findings and ai_workloads=1, got:\n%s", res.Stdout)
	}
}

func TestAIServingPortClusterWideNoNetPol(t *testing.T) {
	dep := healthyAIDeployment("ml", "vllm-llama")
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "vllm-svc"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "vllm-llama"},
			Ports:    []corev1.ServicePort{{Port: 8000, Protocol: corev1.ProtocolTCP}},
		},
	}
	cmd := AICommand(aiDeps(dep, svc))
	res := checktest.Run(t, cmd, "--namespace=ml")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	for _, want := range []string{
		"kind=audit.ai_workload_exposure",
		"severity=warning",
		"reason=ServingPortClusterWideNoNetPol",
		"service=vllm-svc",
		"ports=8000/TCP",
		"findings=1",
		"ai_workloads=1",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("missing %q in stdout:\n%s", want, res.Stdout)
		}
	}
}

func TestAIHostNetworkWithAccelerator(t *testing.T) {
	dep := healthyAIDeployment("ml", "vllm-llama")
	dep.Spec.Template.Spec.HostNetwork = true

	cmd := AICommand(aiDeps(dep))
	res := checktest.Run(t, cmd, "--namespace=ml")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	for _, want := range []string{
		"kind=audit.ai_workload_exposure",
		"severity=warning",
		"reason=HostNetworkWithAccelerator",
		"accelerator=nvidia.com/gpu",
		"findings=1",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("missing %q in stdout:\n%s", want, res.Stdout)
		}
	}
}

func TestAISharedModelCacheHostPath(t *testing.T) {
	dep := healthyAIDeployment("ml", "vllm-llama")
	dep.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: "hf-cache",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/mnt/node-cache/huggingface"},
		},
	}}

	cmd := AICommand(aiDeps(dep))
	res := checktest.Run(t, cmd, "--namespace=ml")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	for _, want := range []string{
		"kind=audit.ai_workload_exposure",
		"severity=warning",
		"reason=SharedModelCacheHostPath",
		"volumes=hf-cache:/mnt/node-cache/huggingface",
		"findings=1",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("missing %q in stdout:\n%s", want, res.Stdout)
		}
	}
}

func TestAIDefaultSAWithAccelerator(t *testing.T) {
	dep := healthyAIDeployment("ml", "vllm-llama")
	dep.Spec.Template.Spec.ServiceAccountName = ""

	cmd := AICommand(aiDeps(dep))
	res := checktest.Run(t, cmd, "--namespace=ml")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	for _, want := range []string{
		"kind=audit.ai_workload_exposure",
		"severity=warning",
		"reason=DefaultSAWithAccelerator",
		"service_account=default",
		"accelerator=nvidia.com/gpu",
		"findings=1",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("missing %q in stdout:\n%s", want, res.Stdout)
		}
	}
}

func TestAIUnpinnedModelLoaderImage(t *testing.T) {
	dep := healthyAIDeployment("ml", "vllm-llama")
	dep.Spec.Template.Spec.InitContainers = []corev1.Container{{
		Name:  "model-downloader",
		Image: "huggingface/downloader:latest",
	}}

	cmd := AICommand(aiDeps(dep))
	res := checktest.Run(t, cmd, "--namespace=ml")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	for _, want := range []string{
		"kind=audit.ai_workload_exposure",
		"severity=info",
		"reason=UnpinnedModelLoaderImage",
		"containers=1",
		"container_names=model-downloader",
		"total_containers=2",
		"findings=1",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("missing %q in stdout:\n%s", want, res.Stdout)
		}
	}
}

func TestAIDetectionNodeSelectorAffinityAndRuntime(t *testing.T) {
	falseVal := false
	byNodeSelector := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "tpu-selector"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &falseVal,
					NodeSelector: map[string]string{
						"cloud.google.com/gke-tpu-accelerator": "tpu-v5p-slice",
					},
					Containers: []corev1.Container{{
						Name:  "worker",
						Image: "us-docker.pkg.dev/proj/jax-worker:v1.0.0",
					}},
				},
			},
		},
	}
	byAffinity := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "gpu-affinity"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &falseVal,
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
								NodeSelectorTerms: []corev1.NodeSelectorTerm{{
									MatchExpressions: []corev1.NodeSelectorRequirement{{
										Key:      "cloud.google.com/gke-accelerator",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"nvidia-l4"},
									}},
								}},
							},
						},
					},
					Containers: []corev1.Container{{
						Name:  "worker",
						Image: "us-docker.pkg.dev/proj/infer:v1.0.0",
					}},
				},
			},
		},
	}
	byRuntime := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "cpu-ollama"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &falseVal,
					Containers: []corev1.Container{{
						Name:    "server",
						Image:   "custom/runner:v1.2.0",
						Command: []string{"ollama", "serve"},
					}},
				},
			},
		},
	}

	cmd := AICommand(aiDeps(byNodeSelector, byAffinity, byRuntime))
	res := checktest.Run(t, cmd, "--namespace=ml")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Fatalf("Verify: %v\n%s", err, res.Stdout)
	}
	if !strings.Contains(res.Stdout, "scanned=3 findings=0 ") || !strings.Contains(res.Stdout, "ai_workloads=3") {
		t.Fatalf("expected scanned=3 findings=0 ai_workloads=3, got:\n%s", res.Stdout)
	}
}

func TestAIScopesAndFlags(t *testing.T) {
	prodDep := healthyAIDeployment("ml", "vllm-llama")
	sysDep := healthyAIDeployment("kube-system", "gpu-driver-test")
	sysDep.Spec.Template.Spec.HostNetwork = true

	cmd := AICommand(aiDeps(prodDep, sysDep))

	// --workload scope
	res := checktest.Run(t, cmd, "--workload=Deployment/ml/vllm-llama")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "scanned=1 findings=0 ") || !strings.Contains(res.Stdout, "ai_workloads=1") {
		t.Fatalf("unexpected --workload output:\n%s", res.Stdout)
	}

	// -A --skip-system-namespaces
	resSkip := checktest.Run(t, cmd, "-A", "--skip-system-namespaces")
	if resSkip.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0: %s", resSkip.Code, resSkip.Stderr)
	}
	if !strings.Contains(resSkip.Stdout, "scanned=1 findings=0 ") || !strings.Contains(resSkip.Stdout, "skipped_system_namespaces=1") {
		t.Fatalf("expected skipped_system_namespaces=1, got:\n%s", resSkip.Stdout)
	}

	// Missing scope is a usage error (exit 2)
	resNoScope := checktest.Run(t, cmd)
	if resNoScope.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want 2 on missing scope", resNoScope.Code)
	}
}
