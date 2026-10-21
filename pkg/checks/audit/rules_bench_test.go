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
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
)

// The pilot's runtime comparison: the Go detectors and the CEL engine
// judging the same workloads for the same four kinds, listing excluded.
// Run with: go test ./pkg/checks/audit -run xxx -bench Pilot -benchmem

func pilotJudgers(b *testing.B) (goArm, celArm, convertOnly func() (int, error)) {
	b.Helper()
	objs := append(rolloutCluster(), imagesCluster()...)
	// The two fixtures share a Deployment name; drop the images copy.
	client := fake.NewClientset(dedupe(objs)...)
	goArm, celArm, convertOnly, err := audit.PilotJudgers(context.Background(), client, "prod")
	if err != nil {
		b.Fatal(err)
	}
	ng, _ := goArm()
	nc, err := celArm()
	if err != nil {
		b.Fatal(err)
	}
	if ng != nc || ng == 0 {
		b.Fatalf("arms disagree before timing: go=%d cel=%d", ng, nc)
	}
	return goArm, celArm, convertOnly
}

func BenchmarkPilotGo(b *testing.B) {
	goArm, _, _ := pilotJudgers(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		goArm()
	}
}

func BenchmarkPilotCEL(b *testing.B) {
	_, celArm, _ := pilotJudgers(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := celArm(); err != nil {
			b.Fatal(err)
		}
	}
}

// dedupe keeps the first object per kind/namespace/name.
func dedupe(objs []runtime.Object) []runtime.Object {
	seen := map[string]bool{}
	var out []runtime.Object
	for _, o := range objs {
		m, err := meta.Accessor(o)
		if err != nil {
			out = append(out, o)
			continue
		}
		key := o.GetObjectKind().GroupVersionKind().Kind + "/" + m.GetNamespace() + "/" + m.GetName()
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, o)
	}
	return out
}

func BenchmarkPilotCELConvertOnly(b *testing.B) {
	_, _, convertOnly := pilotJudgers(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := convertOnly(); err != nil {
			b.Fatal(err)
		}
	}
}
