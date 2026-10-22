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

package kube

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFleetKubeconfig drops a kubeconfig with three contexts: two usable
// ones against different servers, and one pointing at a cluster the
// file never defines — the shape a stale entry leaves behind.
func writeFleetKubeconfig(t *testing.T) string {
	t.Helper()
	const cfg = `apiVersion: v1
kind: Config
current-context: prod-eu
clusters:
- name: eu
  cluster:
    server: https://eu.example.test:443
- name: us
  cluster:
    server: https://us.example.test:443
contexts:
- name: prod-eu
  context:
    cluster: eu
    user: alice
- name: prod-us
  context:
    cluster: us
    user: alice
- name: stale
  context:
    cluster: deleted
    user: alice
users:
- name: alice
  user:
    token: t0ken
`
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// TestListContexts: every context, sorted by name, each carrying the
// server its cluster entry names. Sorted because the order decides
// runner order, log order and (before #386) which snapshot won — a map
// iteration there would make a fleet's startup nondeterministic.
func TestListContexts(t *testing.T) {
	refs, err := ListContexts(writeFleetKubeconfig(t))
	if err != nil {
		t.Fatalf("ListContexts: %v", err)
	}
	want := []ContextRef{
		{Name: "prod-eu", Cluster: "eu", Server: "https://eu.example.test:443"},
		{Name: "prod-us", Cluster: "us", Server: "https://us.example.test:443"},
		// A context whose cluster the file does not define still
		// enumerates: it is a cluster the operator listed, and the
		// caller must be able to name it when it fails to resolve
		// rather than have it vanish.
		{Name: "stale", Cluster: "deleted", Server: ""},
	}
	if len(refs) != len(want) {
		t.Fatalf("got %d contexts, want %d: %+v", len(refs), len(want), refs)
	}
	for i := range want {
		if refs[i] != want[i] {
			t.Errorf("context[%d] = %+v, want %+v", i, refs[i], want[i])
		}
	}
}

func TestListContextsMissingFile(t *testing.T) {
	_, err := ListContexts(filepath.Join(t.TempDir(), "nope"))
	if err == nil {
		t.Fatal("ListContexts on a missing file returned no error")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %q does not name the file", err)
	}
}

// TestConfigForContext: each context resolves to its own server, and
// the current-context is irrelevant — a fleet names every cluster
// explicitly, so honoring current-context would silently collapse N
// runners onto one cluster.
func TestConfigForContext(t *testing.T) {
	path := writeFleetKubeconfig(t)
	for name, want := range map[string]string{
		"prod-eu": "https://eu.example.test:443",
		"prod-us": "https://us.example.test:443",
	} {
		cfg, err := ConfigForContext(path, name)
		if err != nil {
			t.Fatalf("ConfigForContext(%q): %v", name, err)
		}
		if cfg.Host != want {
			t.Errorf("context %q host = %q, want %q", name, cfg.Host, want)
		}
	}
}

// TestConfigForContextErrors: a context that cannot be resolved says
// which one, because the caller turns that into a named, skipped
// cluster (#388) and an operator has to know which one went.
func TestConfigForContextErrors(t *testing.T) {
	path := writeFleetKubeconfig(t)
	for _, tc := range []struct {
		name, context, want string
	}{
		{"undefined cluster", "stale", "stale"},
		{"no such context", "ghost", "ghost"},
		{"unnamed", "", "no context named"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ConfigForContext(path, tc.context)
			if err == nil {
				t.Fatalf("ConfigForContext(%q) returned no error", tc.context)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestConfigForContextIgnoresInCluster: ListContexts/ConfigForContext
// never fall back to the pod's own service account the way BuildConfig
// does. A caller enumerating contexts named its clusters; silently
// adding the cluster the sentinel happens to run in would watch
// something nobody listed.
func TestConfigForContextIgnoresInCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	cfg, err := ConfigForContext(writeFleetKubeconfig(t), "prod-us")
	if err != nil {
		t.Fatalf("ConfigForContext: %v", err)
	}
	if cfg.Host != "https://us.example.test:443" {
		t.Errorf("host = %q — in-cluster credentials leaked into a kubeconfig context", cfg.Host)
	}
}

func writeNamedKubeconfig(t *testing.T, dir, filename, currentCtx string, contexts ...string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("apiVersion: v1\nkind: Config\n")
	if currentCtx != "" {
		b.WriteString("current-context: " + currentCtx + "\n")
	}
	b.WriteString("clusters:\n")
	for _, c := range contexts {
		b.WriteString("- name: " + c + "\n  cluster:\n    server: https://" + c + ".example.test:443\n")
	}
	b.WriteString("contexts:\n")
	for _, c := range contexts {
		b.WriteString("- name: " + c + "\n  context:\n    cluster: " + c + "\n    user: u\n")
	}
	b.WriteString("users:\n- name: u\n  user:\n    token: t\n")
	p := filepath.Join(dir, filename)
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

func TestResolveFleetTargetsKubeconfigDirAndDisambiguation(t *testing.T) {
	dir := t.TempDir()
	// Out-of-order writes; discovery must sort lexicographically.
	p3 := writeNamedKubeconfig(t, dir, "03-west", "default", "default")
	p1 := writeNamedKubeconfig(t, dir, "01-east.yaml", "default", "default")
	p2 := writeNamedKubeconfig(t, dir, "02-central.kubeconfig", "prod-central", "prod-central")
	// Non-kubeconfig regular file and hidden file are ignored when extension is unknown.
	if err := os.WriteFile(filepath.Join(dir, "README.txt"), []byte("not a kubeconfig\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".hidden.yaml"), []byte("invalid: ["), 0o600); err != nil {
		t.Fatal(err)
	}

	targets, err := ResolveFleetTargets("", dir, "")
	if err != nil {
		t.Fatalf("ResolveFleetTargets: %v", err)
	}
	want := []FleetTarget{
		{Kubeconfig: p1, Context: "default", Label: "01-east.yaml:default"},
		{Kubeconfig: p2, Context: "prod-central", Label: "prod-central"},
		{Kubeconfig: p3, Context: "default", Label: "03-west:default"},
	}
	if len(targets) != len(want) {
		t.Fatalf("got %d targets, want %d: %+v", len(targets), len(want), targets)
	}
	for i := range want {
		if targets[i] != want[i] {
			t.Errorf("target[%d] = %+v, want %+v", i, targets[i], want[i])
		}
	}
}

func TestResolveFleetTargetsContextsWildcardAndExplicit(t *testing.T) {
	kc := writeFleetKubeconfig(t)

	allTargets, err := ResolveFleetTargets(kc, "", "*")
	if err != nil {
		t.Fatalf("ResolveFleetTargets(*): %v", err)
	}
	wantAll := []FleetTarget{
		{Kubeconfig: kc, Context: "prod-eu", Label: "prod-eu"},
		{Kubeconfig: kc, Context: "prod-us", Label: "prod-us"},
		{Kubeconfig: kc, Context: "stale", Label: "stale"},
	}
	if len(allTargets) != len(wantAll) {
		t.Fatalf("got %+v, want %+v", allTargets, wantAll)
	}
	for i := range wantAll {
		if allTargets[i] != wantAll[i] {
			t.Errorf("allTargets[%d] = %+v, want %+v", i, allTargets[i], wantAll[i])
		}
	}

	explicit, err := ResolveFleetTargets(kc, "", "prod-us,prod-eu,prod-us")
	if err != nil {
		t.Fatalf("ResolveFleetTargets(explicit): %v", err)
	}
	wantExplicit := []FleetTarget{
		{Kubeconfig: kc, Context: "prod-us", Label: "prod-us"},
		{Kubeconfig: kc, Context: "prod-eu", Label: "prod-eu"},
	}
	if len(explicit) != len(wantExplicit) {
		t.Fatalf("got %+v, want %+v", explicit, wantExplicit)
	}
	for i := range wantExplicit {
		if explicit[i] != wantExplicit[i] {
			t.Errorf("explicit[%d] = %+v, want %+v", i, explicit[i], wantExplicit[i])
		}
	}
}
