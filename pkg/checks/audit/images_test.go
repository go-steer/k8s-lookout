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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func imagesCluster() []runtime.Object {
	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod"}},
		// floating tags on a regular and an init container; one pinned by digest.
		deploy("prod", "front", 2, corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: lbl("front")},
			Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{{Name: "migrate", Image: "ghcr.io/acme/migrate:main", SecurityContext: restrictedSecurityContext()}},
				Containers: []corev1.Container{
					{Name: "nginx", Image: "nginx:latest", SecurityContext: restrictedSecurityContext()},
					{Name: "app", Image: "ghcr.io/acme/app:v1.4.2@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", SecurityContext: restrictedSecurityContext()},
				},
			},
		}),
		// untagged, with a registry port in the host (the tag is after the last '/').
		deploy("prod", "cache", 2, template(lbl("cache"), corev1.Container{Name: "redis", Image: "registry.internal:5000/redis", SecurityContext: restrictedSecurityContext()})),
		// pinned: a semver tag is mutable in principle but not a finding.
		deploy("prod", "api", 2, template(lbl("api"), corev1.Container{Name: "api", Image: "ghcr.io/acme/api:v9.2.0", SecurityContext: restrictedSecurityContext()})),
		// latest, but pinned by digest -> silent.
		deploy("prod", "tool", 1, template(lbl("tool"), corev1.Container{Name: "tool", Image: "ghcr.io/acme/tool:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", SecurityContext: restrictedSecurityContext()})),
	}
}

func TestImagesGolden(t *testing.T) {
	res := checktest.Run(t, audit.HardeningCommand(testDeps(imagesCluster()...)), "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/images.golden", res.Stdout)
}

func TestImagesClaims(t *testing.T) {
	out := checktest.AssertEnvelopeContract(t, audit.HardeningCommand(testDeps(imagesCluster()...)), "--namespace=prod")
	f := checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.image_floating_tag", Name: "front", Reason: "FloatingTag", Severity: "info"})
	if f.Fields["container_names"] != "migrate,nginx" || f.Fields["total_containers"] != "3" {
		t.Errorf("front: fields=%v", f.Fields)
	}
	checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.image_floating_tag", Name: "cache", Reason: "NoTag", Severity: "info"})
	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: "audit.image_floating_tag", Name: "api"})
	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: "audit.image_floating_tag", Name: "tool"})
}
