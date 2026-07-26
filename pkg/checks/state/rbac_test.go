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

package state

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// TestShippedClusterRoleCoversLoadCluster parses the SHIPPED sentinel
// ClusterRole and asserts it grants everything the enrichment read
// paths (DESIGN.md §7.6) need — the M4 drill's observation 2: the
// scoped-list fallback is one LoadCluster pass over the incident
// namespace, and the role was missing several of its kinds, so every
// enrichment on a --storm-less sentinel failed at resolve. Keying the
// assertion on LoadClusterListRequirements() (declared next to
// listCluster) keeps the role and the code in sync forever: adding a
// kind to the List pass fails this test until deploy/12 grants it.
func TestShippedClusterRoleCoversLoadCluster(t *testing.T) {
	role := loadWatcherClusterRole(t)

	for _, req := range LoadClusterListRequirements() {
		if !roleAllows(role, req.Group, req.Resource, "list") {
			t.Errorf("deploy/12-clusterrole-watcher.yaml does not grant list on %s/%s — enrichment's scoped-list fallback (state.LoadCluster) needs it (M4 observation 2)",
				groupLabel(req.Group), req.Resource)
		}
	}

	// The live path's single API read (internal/watch/enrich.go
	// workloadObject): one GET of the incident's resolved top owner,
	// which can be any workload kind. Pods are granted get already;
	// the apps/batch workload kinds must be too.
	liveGets := []ListRequirement{
		{"", "pods"},
		{"apps", "deployments"},
		{"apps", "replicasets"},
		{"apps", "statefulsets"},
		{"apps", "daemonsets"},
		{"batch", "jobs"},
		{"batch", "cronjobs"},
	}
	for _, req := range liveGets {
		if !roleAllows(role, req.Group, req.Resource, "get") {
			t.Errorf("deploy/12-clusterrole-watcher.yaml does not grant get on %s/%s — enrichment's live path GETs the incident's top owner (M4 observation 2)",
				groupLabel(req.Group), req.Resource)
		}
	}
}

// TestShippedClusterRoleStaysReadOnly guards the role's headline
// invariant while the grants above widen it: the sentinel only ever
// OBSERVES — no write verb, no secret get/watch (the expiry source's
// paged `list` is the one deliberate exception, documented in the
// role).
func TestShippedClusterRoleStaysReadOnly(t *testing.T) {
	role := loadWatcherClusterRole(t)
	readOnly := []string{"get", "list", "watch"}
	for _, rule := range role.Rules {
		for _, verb := range rule.Verbs {
			if !slices.Contains(readOnly, verb) {
				t.Errorf("rule %v grants %q — the sentinel role is read-only by design", rule.Resources, verb)
			}
		}
		if slices.Contains(rule.Resources, "secrets") {
			for _, verb := range rule.Verbs {
				if verb != "list" {
					t.Errorf("secrets rule grants %q — list is the only permitted secret access (§11 tradeoff note)", verb)
				}
			}
		}
	}
}

func loadWatcherClusterRole(t *testing.T) *rbacv1.ClusterRole {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "12-clusterrole-watcher.yaml"))
	if err != nil {
		t.Fatalf("reading shipped ClusterRole: %v", err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parsing deploy/12-clusterrole-watcher.yaml: %v", err)
	}
	if role.Kind != "ClusterRole" || role.Name != "k8s-event-watcher" {
		t.Fatalf("unexpected object %s/%s in deploy/12-clusterrole-watcher.yaml", role.Kind, role.Name)
	}
	return &role
}

// roleAllows reports whether any rule grants verb on group/resource.
func roleAllows(role *rbacv1.ClusterRole, group, resource, verb string) bool {
	for _, rule := range role.Rules {
		if !matchToken(rule.APIGroups, group) {
			continue
		}
		if !matchToken(rule.Resources, resource) {
			continue
		}
		if matchToken(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

func matchToken(haystack []string, want string) bool {
	return slices.Contains(haystack, want) || slices.Contains(haystack, rbacv1.APIGroupAll)
}

func groupLabel(group string) string {
	if group == "" {
		return "core"
	}
	return group
}
