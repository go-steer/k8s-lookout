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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func bareContainer(name string) corev1.Container {
	c := container(name, true, true)
	c.Resources = corev1.ResourceRequirements{}
	return c
}

func TestWorkloadsResourceRequestsAndLimits(t *testing.T) {
	objs := []runtime.Object{
		deploy("prod", "x", 3, spread(template(
			map[string]string{"app": "x"},
			bareContainer("app"),
			container("sidecar", true, true),
		))),
		pdb("prod", "x-pdb", matching(map[string]string{"app": "x"})),
	}
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	byReason := map[string]map[string]string{}
	for _, r := range recs {
		byReason[r["reason"]] = r
	}
	for _, tc := range []struct {
		reason   string
		kind     string
		severity string
		resName  string
	}{
		{"NoCPURequest", "audit.no_resource_requests", "warning", "cpu"},
		{"NoMemoryRequest", "audit.no_resource_requests", "warning", "memory"},
		{"NoMemoryLimit", "audit.no_resource_limits", "warning", "memory"},
		{"NoCPULimit", "audit.no_resource_limits", "info", "cpu"},
	} {
		r, ok := byReason[tc.reason]
		if !ok {
			t.Errorf("missing finding for reason %s in:\n%s", tc.reason, res.Stdout)
			continue
		}
		if r["kind"] != tc.kind || r["severity"] != tc.severity || r["resource"] != tc.resName {
			t.Errorf("%s: got kind=%q severity=%q resource=%q", tc.reason, r["kind"], r["severity"], r["resource"])
		}
		if r["containers"] != "1" || r["container_names"] != "app" || r["total_containers"] != "2" {
			t.Errorf("%s: got containers=%q container_names=%q total_containers=%q",
				tc.reason, r["containers"], r["container_names"], r["total_containers"])
		}
	}
}

func TestWorkloadsResourceRequestDefaultsFromLimit(t *testing.T) {
	c := bareContainer("app")
	c.Resources.Limits = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("500m"),
		corev1.ResourceMemory: resource.MustParse("256Mi"),
	}
	objs := []runtime.Object{
		deploy("prod", "x", 3, spread(template(map[string]string{"app": "x"}, c))),
		pdb("prod", "x-pdb", matching(map[string]string{"app": "x"})),
	}
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if recs := findingLines(t, res.Stdout); len(recs) != 0 {
		t.Errorf("limits imply requests in Kubernetes; want 0 findings, got:\n%s", res.Stdout)
	}
}

func TestWorkloadsResourceLimitRangeSuppressesDefaults(t *testing.T) {
	lr := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "defaults"},
		Spec: corev1.LimitRangeSpec{
			Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypeContainer,
				DefaultRequest: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("100m"),
				},
				Default: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("256Mi"),
					corev1.ResourceCPU:    resource.MustParse("500m"),
				},
			}},
		},
	}
	objs := []runtime.Object{
		deploy("prod", "x", 3, spread(template(map[string]string{"app": "x"}, bareContainer("app")))),
		pdb("prod", "x-pdb", matching(map[string]string{"app": "x"})),
		lr,
	}
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	// LimitRange sets default limits for both CPU and Memory, and in Kubernetes
	// LimitRanger also defaults requests from those default limits if DefaultRequest
	// is unset.
	if recs := findingLines(t, res.Stdout); len(recs) != 0 {
		t.Errorf("LimitRange defaults should suppress resource findings, got:\n%s", res.Stdout)
	}
}
