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

package leeway

import (
	"os/exec"
	"strings"
	"testing"
)

// forbiddenDeps are import-path prefixes pkg/leeway must never reach, even
// transitively.
//
// This is NFR-10 and §2.2's "pure, no client-go", made falsifiable. The reason
// it checks the *transitive* closure rather than this package's own import
// block is that a direct-import check is trivially satisfiable by accident: one
// innocuous-looking helper package that itself imports a clientset would put a
// cluster connection inside pkg/leeway while every file here still looked
// clean.
var forbiddenDeps = []struct {
	prefix string
	why    string
}{
	{"k8s.io/client-go", "NFR-10: pkg/leeway must have no client-go dependency, so cmd/leeway stays thin and the engine stays testable without a cluster"},
	{"k8s.io/metrics", "the metrics client is a cluster reader; scoring takes numbers from its caller"},
	{"k8s.io/kubectl", "kubectl's libraries pull in the whole client stack"},
	{"github.com/go-steer/k8s-lookout/internal/watch", "layering: the watch sources depend on pkg/leeway, never the reverse"},
	{"github.com/go-steer/k8s-lookout/pkg/sources", "layering: sources depend on pkg/leeway, never the reverse"},
	{"github.com/go-steer/k8s-lookout/pkg/store", "the engine does not persist; §9 storage is the source's job"},
}

func TestPkgLeeway_ImportsNothingThatTouchesACluster(t *testing.T) {
	// Deliberately not skipped when `go` is missing. A guard that quietly
	// opts out is worse than no guard, because the package keeps advertising
	// a property nothing is checking.
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}

	for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		dep = strings.TrimSpace(dep)
		if dep == "" {
			continue
		}
		for _, f := range forbiddenDeps {
			if dep == f.prefix || strings.HasPrefix(dep, f.prefix+"/") {
				t.Errorf("pkg/leeway transitively imports %q\n  forbidden prefix: %s\n  why: %s", dep, f.prefix, f.why)
			}
		}
	}
}

// TestForbiddenDeps_GuardActuallyMatches proves the matcher works, so that a
// typo in a prefix cannot turn the guard above into a no-op that passes
// forever.
func TestForbiddenDeps_GuardActuallyMatches(t *testing.T) {
	cases := map[string]bool{
		"k8s.io/client-go":                   true,
		"k8s.io/client-go/kubernetes":        true,
		"k8s.io/client-go/informers/core/v1": true,
		"k8s.io/api/core/v1":                 false,
		"k8s.io/apimachinery/pkg/util/sets":  false,
		"k8s.io/client-gopher":               false, // prefix must respect path boundaries
		"math":                               false,
	}
	for dep, want := range cases {
		var got bool
		for _, f := range forbiddenDeps {
			if dep == f.prefix || strings.HasPrefix(dep, f.prefix+"/") {
				got = true
			}
		}
		if got != want {
			t.Errorf("matcher on %q = %v, want %v", dep, got, want)
		}
	}
}
