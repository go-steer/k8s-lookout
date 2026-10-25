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

package audit_test

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func testDepsWithComputeClasses(classes []*unstructured.Unstructured, objs ...runtime.Object) audit.Deps {
	client := fake.NewClientset(objs...)
	scheme := runtime.NewScheme()
	gvr := schema.GroupVersionResource{Group: "cloud.google.com", Version: "v1", Resource: "computeclasses"}
	dynObjs := make([]runtime.Object, 0, len(classes))
	for _, c := range classes {
		dynObjs = append(dynObjs, c)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		gvr: "ComputeClassList",
	}, dynObjs...)
	return audit.Deps{
		Client:  func(context.Context) (kubernetes.Interface, error) { return client, nil },
		Dynamic: func(context.Context) (dynamic.Interface, error) { return dyn, nil },
	}
}

func makeComputeClassCR(name string, priorities []map[string]any, autoCreate bool) *unstructured.Unstructured {
	rawPrios := make([]any, 0, len(priorities))
	for _, p := range priorities {
		rawPrios = append(rawPrios, p)
	}
	spec := map[string]any{
		"priorities": rawPrios,
	}
	if autoCreate {
		spec["nodePoolAutoCreation"] = map[string]any{"enabled": true}
	}
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "cloud.google.com/v1",
			"kind":       "ComputeClass",
			"metadata": map[string]any{
				"name": name,
			},
			"spec": spec,
		},
	}
}

func TestComputeClassContract(t *testing.T) {
	cc := makeComputeClassCR("gold-tier", []map[string]any{
		{"machineFamily": "n2", "spot": true},
		{"machineFamily": "n2", "spot": false},
	}, false)

	spotTpl := template(lbl("spot-api"), container("api", true, true))
	spotTpl.Spec.NodeSelector = map[string]string{"cloud.google.com/gke-spot": "true"}

	missingCCTpl := template(lbl("missing-cc"), container("worker", true, true))
	missingCCTpl.Spec.NodeSelector = map[string]string{"cloud.google.com/compute-class": "nonexistent-cc"}

	gpuZoneTpl := template(lbl("gpu-job"), container("trainer", true, true))
	gpuZoneTpl.Spec.NodeSelector = map[string]string{
		"cloud.google.com/gke-accelerator": "nvidia-l4",
		"topology.kubernetes.io/zone":      "us-central1-a",
	}

	deps := testDepsWithComputeClasses(
		[]*unstructured.Unstructured{cc},
		deploy("prod", "spot-api", 3, spotTpl),
		svc("prod", "spot-api", lbl("spot-api"), port("http", 80, "http")),
		deploy("prod", "missing-cc", 2, missingCCTpl),
		deploy("prod", "gpu-job", 1, gpuZoneTpl),
		node("n-a1", map[string]string{
			"cloud.google.com/gke-nodepool": "pool-single-zone",
			"topology.kubernetes.io/zone":   "us-central1-a",
		}),
		node("n-a2", map[string]string{
			"cloud.google.com/gke-nodepool": "pool-single-zone",
			"topology.kubernetes.io/zone":   "us-central1-a",
		}),
		node("n-b1", map[string]string{
			"cloud.google.com/gke-nodepool":    "pool-gpu",
			"topology.kubernetes.io/zone":      "us-central1-b",
			"cloud.google.com/gke-accelerator": "nvidia-l4",
		}),
	)

	checktest.VerifyContract(t, audit.ComputeClassCommand(deps), "-A")
}

func TestComputeClassHealthyCluster(t *testing.T) {
	cc := makeComputeClassCR("resilient-cc", []map[string]any{
		{"machineFamily": "c3", "spot": true},
		{"machineFamily": "c3", "spot": false},
	}, true)

	tpl := template(lbl("api"), container("api", true, true))
	tpl.Spec.NodeSelector = map[string]string{"cloud.google.com/compute-class": "resilient-cc"}

	deps := testDepsWithComputeClasses(
		[]*unstructured.Unstructured{cc},
		deploy("prod", "api", 3, tpl),
		node("n1", map[string]string{
			"cloud.google.com/gke-nodepool":  "pool-main",
			"topology.kubernetes.io/zone":    "us-central1-a",
			"cloud.google.com/compute-class": "resilient-cc",
		}),
		node("n2", map[string]string{
			"cloud.google.com/gke-nodepool":  "pool-main",
			"topology.kubernetes.io/zone":    "us-central1-b",
			"cloud.google.com/compute-class": "resilient-cc",
		}),
	)

	res := checktest.Run(t, audit.ComputeClassCommand(deps), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "findings=0") {
		t.Errorf("expected findings=0 on healthy cluster, got:\n%s", res.Stdout)
	}
}

func TestSpotWithoutFallback(t *testing.T) {
	fallbackCC := makeComputeClassCR("spot-with-od-fallback", []map[string]any{
		{"machineFamily": "n2", "spot": true},
		{"machineFamily": "n2", "spot": false},
	}, true)

	directSpotTpl := template(lbl("checkout"), container("checkout", true, true))
	directSpotTpl.Spec.NodeSelector = map[string]string{"cloud.google.com/gke-spot": "true"}

	fallbackSpotTpl := template(lbl("catalog"), container("catalog", true, true))
	fallbackSpotTpl.Spec.NodeSelector = map[string]string{
		"cloud.google.com/compute-class": "spot-with-od-fallback",
	}

	deps := testDepsWithComputeClasses(
		[]*unstructured.Unstructured{fallbackCC},
		deploy("prod", "checkout", 3, directSpotTpl),
		svc("prod", "checkout", lbl("checkout"), port("http", 80, "http")),
		deploy("prod", "catalog", 3, fallbackSpotTpl),
	)

	out := checktest.AssertEnvelopeContract(t, audit.ComputeClassCommand(deps), "-A")
	res := checktest.Run(t, audit.ComputeClassCommand(deps), "-A")
	checktest.AssertHasFinding(t, out, checktest.FindingMatch{
		Kind:         "audit.compute_class_posture",
		Namespace:    "prod",
		KindOfObject: "Deployment",
		Name:         "checkout",
		Severity:     emit.SeverityWarning,
		Reason:       "SpotWithoutFallback",
	})
	rec := findRecord(t, res.Stdout, "audit.compute_class_posture", "Deployment", "prod", "checkout")
	if rec["replicas"] != "3" || rec["spot_selector"] != "cloud.google.com/gke-spot=true" || rec["services"] != "1" || rec["service_names"] != "checkout" {
		t.Errorf("unexpected SpotWithoutFallback details: %v", rec)
	}
	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Name: "catalog"})
}

func TestUnsatisfiableZoneWithAccelerator(t *testing.T) {
	gpuContainer := container("trainer", true, true)
	gpuContainer.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
		},
	}
	badTpl := template(lbl("trainer"), gpuContainer)
	badTpl.Spec.NodeSelector = map[string]string{
		"cloud.google.com/gke-accelerator": "nvidia-tesla-a100",
		"topology.kubernetes.io/zone":      "us-central1-a",
	}

	goodTpl := template(lbl("serving"), gpuContainer)
	goodTpl.Spec.NodeSelector = map[string]string{
		"cloud.google.com/gke-accelerator": "nvidia-tesla-a100",
		"topology.kubernetes.io/zone":      "us-central1-c",
	}

	deps := testDeps(
		deploy("ml", "trainer", 1, badTpl),
		deploy("ml", "serving", 1, goodTpl),
		node("cpu-a", map[string]string{
			"topology.kubernetes.io/zone": "us-central1-a",
		}),
		node("gpu-c", map[string]string{
			"topology.kubernetes.io/zone":      "us-central1-c",
			"cloud.google.com/gke-accelerator": "nvidia-tesla-a100",
		}),
	)

	out := checktest.AssertEnvelopeContract(t, audit.ComputeClassCommand(deps), "-A")
	res := checktest.Run(t, audit.ComputeClassCommand(deps), "-A")
	checktest.AssertHasFinding(t, out, checktest.FindingMatch{
		Kind:         "audit.compute_class_posture",
		Namespace:    "ml",
		KindOfObject: "Deployment",
		Name:         "trainer",
		Severity:     emit.SeverityWarning,
		Reason:       "UnsatisfiableZoneWithAccelerator",
	})
	rec := findRecord(t, res.Stdout, "audit.compute_class_posture", "Deployment", "ml", "trainer")
	if rec["accelerator"] != "nvidia-tesla-a100" || rec["zone"] != "us-central1-a" || rec["accelerator_zones"] != "us-central1-c" {
		t.Errorf("unexpected UnsatisfiableZoneWithAccelerator details: %v", rec)
	}
	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Name: "serving"})
}

func TestComputeClassMissingAndNoMatchingNodes(t *testing.T) {
	existingNoNodesCC := makeComputeClassCR("custom-empty", []map[string]any{
		{"machineFamily": "c3"},
	}, false)

	missingTpl := template(lbl("missing-app"), container("app", true, true))
	missingTpl.Spec.NodeSelector = map[string]string{"cloud.google.com/compute-class": "typo-class"}

	noNodesTpl := template(lbl("empty-app"), container("app", true, true))
	noNodesTpl.Spec.NodeSelector = map[string]string{"cloud.google.com/compute-class": "custom-empty"}

	builtInTpl := template(lbl("builtin-app"), container("app", true, true))
	builtInTpl.Spec.NodeSelector = map[string]string{"cloud.google.com/compute-class": "Balanced"}

	deps := testDepsWithComputeClasses(
		[]*unstructured.Unstructured{existingNoNodesCC},
		deploy("prod", "missing-app", 1, missingTpl),
		deploy("prod", "empty-app", 1, noNodesTpl),
		deploy("prod", "builtin-app", 1, builtInTpl),
		node("n1", map[string]string{"topology.kubernetes.io/zone": "us-central1-a"}),
	)

	out := checktest.AssertEnvelopeContract(t, audit.ComputeClassCommand(deps), "-A")
	res := checktest.Run(t, audit.ComputeClassCommand(deps), "-A")
	checktest.AssertHasFinding(t, out, checktest.FindingMatch{
		Kind:         "audit.compute_class_posture",
		Namespace:    "prod",
		KindOfObject: "Deployment",
		Name:         "missing-app",
		Severity:     emit.SeverityWarning,
		Reason:       "ComputeClassMissing",
	})
	missingRec := findRecord(t, res.Stdout, "audit.compute_class_posture", "Deployment", "prod", "missing-app")
	if missingRec["compute_class"] != "typo-class" {
		t.Errorf("unexpected ComputeClassMissing details: %v", missingRec)
	}

	checktest.AssertHasFinding(t, out, checktest.FindingMatch{
		Kind:         "audit.compute_class_posture",
		Namespace:    "prod",
		KindOfObject: "Deployment",
		Name:         "empty-app",
		Severity:     emit.SeverityInfo,
		Reason:       "ComputeClassNoMatchingNodes",
	})
	emptyRec := findRecord(t, res.Stdout, "audit.compute_class_posture", "Deployment", "prod", "empty-app")
	if emptyRec["compute_class"] != "custom-empty" {
		t.Errorf("unexpected ComputeClassNoMatchingNodes details: %v", emptyRec)
	}

	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Name: "builtin-app"})
}

func TestSingleZoneNodePool(t *testing.T) {
	spreadTpl := template(lbl("api"), container("api", true, true))
	spreadTpl.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.DoNotSchedule,
	}}
	spreadTpl.Spec.NodeSelector = map[string]string{"cloud.google.com/gke-nodepool": "pool-pinned"}

	deps := testDeps(
		deploy("prod", "api", 3, spreadTpl),
		node("n1", map[string]string{
			"cloud.google.com/gke-nodepool": "pool-pinned",
			"topology.kubernetes.io/zone":   "us-central1-a",
		}),
		node("n2", map[string]string{
			"cloud.google.com/gke-nodepool": "pool-single-info",
			"topology.kubernetes.io/zone":   "us-central1-a",
		}),
		node("n3", map[string]string{
			"cloud.google.com/gke-nodepool": "pool-single-info",
			"topology.kubernetes.io/zone":   "us-central1-a",
		}),
		node("n4", map[string]string{
			"cloud.google.com/gke-nodepool": "pool-multi",
			"topology.kubernetes.io/zone":   "us-central1-a",
		}),
		node("n5", map[string]string{
			"cloud.google.com/gke-nodepool": "pool-multi",
			"topology.kubernetes.io/zone":   "us-central1-b",
		}),
	)

	out := checktest.AssertEnvelopeContract(t, audit.ComputeClassCommand(deps), "-A")
	res := checktest.Run(t, audit.ComputeClassCommand(deps), "-A")
	checktest.AssertHasFinding(t, out, checktest.FindingMatch{
		Kind:         "audit.single_zone_nodepool",
		KindOfObject: "NodePool",
		Name:         "pool-pinned",
		Severity:     emit.SeverityWarning,
		Reason:       "SingleZoneNodePool",
	})
	pinnedRec := findRecord(t, res.Stdout, "audit.single_zone_nodepool", "NodePool", "", "pool-pinned")
	if pinnedRec["nodepool"] != "pool-pinned" || pinnedRec["zone"] != "us-central1-a" || pinnedRec["nodes"] != "1" || pinnedRec["cluster_zones"] != "us-central1-a,us-central1-b" || pinnedRec["affected_workloads"] != "1" || pinnedRec["affected_workload_names"] != "Deployment/prod/api" {
		t.Errorf("unexpected SingleZoneNodePool warning details: %v", pinnedRec)
	}

	checktest.AssertHasFinding(t, out, checktest.FindingMatch{
		Kind:         "audit.single_zone_nodepool",
		KindOfObject: "NodePool",
		Name:         "pool-single-info",
		Severity:     emit.SeverityInfo,
		Reason:       "SingleZoneNodePool",
	})
	infoRec := findRecord(t, res.Stdout, "audit.single_zone_nodepool", "NodePool", "", "pool-single-info")
	if infoRec["nodepool"] != "pool-single-info" || infoRec["zone"] != "us-central1-a" || infoRec["nodes"] != "2" || infoRec["cluster_zones"] != "us-central1-a,us-central1-b" {
		t.Errorf("unexpected SingleZoneNodePool info details: %v", infoRec)
	}

	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Name: "pool-multi"})
}
