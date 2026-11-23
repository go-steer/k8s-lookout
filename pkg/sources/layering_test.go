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

package sources_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
)

const modulePath = "github.com/go-steer/k8s-lookout"

// layeringRules are the import-graph invariants of the repo: which
// packages may reach which. Everything here lives in one Go module, so
// nothing in the compiler stops `pkg/sources/rollout` importing
// `internal/watch` or `pkg/store` and welding the sources to the
// sentinel's runner or persistence layer.
//
// Each rule is checked against the TRANSITIVE closure, not the offending
// package's own import block.
var layeringRules = []struct {
	name      string
	pattern   string
	forbidden []string
	why       string
}{
	{
		name:      "pkg/... never reaches internal/...",
		pattern:   modulePath + "/pkg/...",
		forbidden: []string{modulePath + "/internal"},
		why: "the sources and engine in pkg/ are the embeddable half of this repo. " +
			"internal/ is the sentinel: its inject path, its flag surface, its runner. " +
			"The dependency runs one way: internal/watch wires up pkg/sources, never the reverse.",
	},
	{
		name:      "pkg/sources/... stays store-free",
		pattern:   modulePath + "/pkg/sources/...",
		forbidden: []string{modulePath + "/pkg/store"},
		why: "sources must run cleanly in store-less mode without importing SQLite persistence directly; " +
			"internal/watch owns any optional store wiring.",
	},
}

func TestLayering_TheImportGraphMatchesTheDesign(t *testing.T) {
	for _, rule := range layeringRules {
		t.Run(rule.name, func(t *testing.T) {
			for pkg, deps := range closures(t, rule.pattern) {
				for _, dep := range deps {
					for _, prefix := range rule.forbidden {
						if underPrefix(dep, prefix) {
							t.Errorf("%s transitively imports %s\n  forbidden prefix: %s\n  why: %s",
								pkg, dep, prefix, rule.why)
						}
					}
				}
			}
		})
	}
}

// TestStandaloneSource_CallerSuppliedFactoryAndNoStore verifies the API-shape
// half of the layering contract (#842): an informer-backed source constructs,
// syncs, and shuts down cleanly with a caller-supplied transforming informer
// factory and no store or internal/watch runner.
func TestStandaloneSource_CallerSuppliedFactoryAndNoStore(t *testing.T) {
	client := fake.NewSimpleClientset()
	factory := kube.NewTransformingFactory(client)

	src := rollout.New(client, rollout.DefaultConfig())
	src.WithFactory(factory)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pre-instantiate the shared informers and start the caller-owned factory
	// before launching Run, matching standalone shared-factory callers.
	_ = factory.Core().V1().Pods().Informer()
	_ = factory.Apps().V1().Deployments().Informer()
	_ = factory.Apps().V1().ReplicaSets().Informer()
	_ = factory.Apps().V1().StatefulSets().Informer()
	factory.Start(ctx.Done())

	errCh := make(chan error, 1)
	go func() {
		errCh <- src.Run(ctx, func(engine.Signal) {})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for !src.HasSynced() {
		if time.Now().After(deadline) {
			t.Fatal("standalone source did not sync within 3s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("standalone source Run returned error: %v", err)
	}
}

func closures(t *testing.T, pattern string) map[string][]string {
	t.Helper()
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}}\t{{join .Deps \" \"}}", pattern).CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pattern, err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	got := make(map[string][]string, len(lines))
	for _, line := range lines {
		pkg, deps, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok || pkg == "" {
			continue
		}
		got[pkg] = strings.Fields(deps)
	}
	if len(got) == 0 {
		t.Fatalf("go list %s matched no packages — the rule is guarding nothing", pattern)
	}
	return got
}

func underPrefix(dep, prefix string) bool {
	return dep == prefix || strings.HasPrefix(dep, prefix+"/")
}

func TestUnderPrefix_RespectsPathBoundaries(t *testing.T) {
	cases := []struct {
		dep, prefix string
		want        bool
	}{
		{modulePath + "/internal", modulePath + "/internal", true},
		{modulePath + "/internal/watch", modulePath + "/internal", true},
		{modulePath + "/internal/watch/sub", modulePath + "/internal", true},
		{modulePath + "/pkg/sources", modulePath + "/internal", false},
		{modulePath + "/internalise", modulePath + "/internal", false},
		{"k8s.io/client-go/kubernetes", "k8s.io/client-go", true},
		{"k8s.io/client-gopher", "k8s.io/client-go", false},
		{"k8s.io/api/core/v1", "k8s.io/client-go", false},
	}
	for _, c := range cases {
		if got := underPrefix(c.dep, c.prefix); got != c.want {
			t.Errorf("underPrefix(%q, %q) = %v, want %v", c.dep, c.prefix, got, c.want)
		}
	}
}

func TestLayeringRules_EveryRuleMatchesSomething(t *testing.T) {
	for _, rule := range layeringRules {
		if len(rule.forbidden) == 0 {
			t.Errorf("rule %q forbids nothing", rule.name)
		}
		if len(closures(t, rule.pattern)) == 0 {
			t.Errorf("rule %q matches no packages", rule.name)
		}
	}
}
