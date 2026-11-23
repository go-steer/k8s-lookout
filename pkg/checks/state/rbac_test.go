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

// otherSourceBulkReads are the (group, resource) pairs the shipped
// ClusterRole grants list/watch on for readers OTHER than
// state.LoadCluster — the informer sources and the expiry source.
// TestShippedClusterRoleGrantsSubsetOfLoadCluster uses them to tell a
// legitimate non-LoadCluster grant from privilege creep. Adding a new
// cluster-wide list/watch grant to deploy/12 is a security-sensitive
// change, so it must be declared here (with the source it serves) or
// the test fails — the whole point of the allowlist.
var otherSourceBulkReads = []ListRequirement{
	{"", "events"},                                                      // k8s-events source (primary informer)
	{"policy", "poddisruptionbudgets"},                                  // object-state source
	{"autoscaling", "horizontalpodautoscalers"},                         // autoscaling source
	{"metrics.k8s.io", "pods"},                                          // saturation source (metrics API)
	{"admissionregistration.k8s.io", "validatingwebhookconfigurations"}, // expiry source (webhook CA bundles)
	{"admissionregistration.k8s.io", "mutatingwebhookconfigurations"},   // expiry source
	{"cert-manager.io", "certificates"},                                 // expiry source (discovery-gated)
	{"acme.cert-manager.io", "challenges"},                              // expiry source ACME stalls (#542, discovery-gated, optional)
	{"acme.cert-manager.io", "orders"},                                  // expiry source ACME stalls
	{"gateway.networking.k8s.io", "gateways"},                           // gateway source (discovery-gated)
	{"gateway.networking.k8s.io", "httproutes"},                         // gateway source
	// read-path volume checks (`health`, `state volumes`, `state storage`):
	// claim binding (spec.volumeName) and volume metadata only — neither object
	// holds data or credentials.
	{"", "persistentvolumeclaims"},
	{"", "persistentvolumes"},
	// `audit computeclass`: GKE ComputeClass priority list (machine families,
	// core and memory floors, spot/reservation flags); inert everywhere but GKE.
	{"cloud.google.com", "computeclasses"},
	// graph history, #507: with --store the watch topology graph records
	// NetworkPolicy→Pod Governs edges (internal/watch routingAccess). Pod
	// selectors only; no informer starts without a store.
	{"networking.k8s.io", "networkpolicies"},
	// state volumes, #616: lists storage.k8s.io/volumeattachments to join
	// Multi-Attach / FailedAttachVolume events to the holding node and pod.
	{"storage.k8s.io", "volumeattachments"},
}

// TestShippedClusterRoleGrantsSubsetOfLoadCluster is the least-privilege
// invariant after issue #192 made state.LoadCluster tolerant of
// per-resource Forbidden (partial bundles): the shipped role's
// cluster-wide list/watch grants must be a SUBSET of what the codebase
// actually reads — every LoadCluster requirement (LoadClusterListRequirements)
// plus the declared other-source reads (otherSourceBulkReads) — and
// nothing more. This replaces the old "the role MUST cover every
// requirement" assertion: a resource can now be dropped from an
// operator's copy of the role and enrichment/bundle degrade to a
// documented partial (proved by TestListClusterToleratesForbidden),
// so the guard flips from "grant all" to "grant no more than needed."
// The shipped file still happens to cover every requirement; the
// t.Logf below reports any it does not, so a deliberate narrowing is
// visible without failing the build.
func TestShippedClusterRoleGrantsSubsetOfLoadCluster(t *testing.T) {
	role := loadWatcherClusterRole(t)

	allowed := map[ListRequirement]bool{}
	for _, req := range LoadClusterListRequirements() {
		allowed[req] = true
	}
	for _, req := range otherSourceBulkReads {
		allowed[req] = true
	}

	// Subset / no-creep: every list-or-watch grant in the shipped role
	// must name a resource the codebase reads. A stray grant (or a new
	// source's grant added without a matching allowlist entry) fails
	// here — cluster-wide bulk-read grants are the privilege that
	// matters (issue #192: `list` on secrets returns full values).
	for _, rule := range role.Rules {
		if !grantsBulkRead(rule) {
			continue // get-only (pods/log, nodes/proxy) — not bulk read
		}
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				req := ListRequirement{Group: group, Resource: resource}
				if !allowed[req] {
					t.Errorf("deploy/12-clusterrole-watcher.yaml grants list/watch on %s/%s, which is neither a LoadCluster requirement nor a declared other-source read — privilege creep: wire it into state.LoadClusterListRequirements() or add it to otherSourceBulkReads with the source it serves",
						groupLabel(group), resource)
				}
			}
		}
	}

	// Visibility, not a failure: report any LoadCluster requirement the
	// shipped file does not grant. Missing ones degrade to a partial
	// bundle; this line makes a deliberate narrowing auditable.
	for _, req := range LoadClusterListRequirements() {
		if !roleAllows(role, req.Group, req.Resource, "list") {
			t.Logf("note: deploy/12-clusterrole-watcher.yaml does not grant list on %s/%s — bundles degrade to a partial (skipped=%s)",
				groupLabel(req.Group), req.Resource, req.String())
		}
	}

	// The live enrichment path (internal/watch/enrich.go workloadObject,
	// unchanged by #192) still GETs the incident's resolved top owner,
	// so the shipped role must grant get on every workload kind.
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

// grantsBulkRead reports whether a rule grants a cluster-wide bulk-read
// verb (list or watch) — the privilege the subset guard polices.
func grantsBulkRead(rule rbacv1.PolicyRule) bool {
	for _, v := range rule.Verbs {
		if v == "list" || v == "watch" || v == rbacv1.VerbAll {
			return true
		}
	}
	return false
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
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "base", "12-clusterrole-watcher.yaml"))
	if err != nil {
		t.Fatalf("reading shipped ClusterRole: %v", err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parsing deploy/base/12-clusterrole-watcher.yaml: %v", err)
	}
	if role.Kind != "ClusterRole" || role.Name != "lookout-watch" {
		t.Fatalf("unexpected object %s/%s in deploy/base/12-clusterrole-watcher.yaml", role.Kind, role.Name)
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

func TestShippedNamespacedRoleStaysReadOnly(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "components", "scope-namespaced", "role-watcher.yaml"))
	if err != nil {
		t.Fatalf("reading deploy/components/scope-namespaced/role-watcher.yaml: %v", err)
	}
	var role rbacv1.Role
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parsing deploy/components/scope-namespaced/role-watcher.yaml: %v", err)
	}
	readOnly := []string{"get", "list", "watch"}
	for _, rule := range role.Rules {
		for _, verb := range rule.Verbs {
			if !slices.Contains(readOnly, verb) {
				t.Errorf("namespaced Role rule %v grants non-read verb %q", rule.Resources, verb)
			}
		}
		if slices.Contains(rule.Resources, "secrets") {
			for _, verb := range rule.Verbs {
				if verb != "list" {
					t.Errorf("namespaced Role secrets rule grants %q — only list is permitted", verb)
				}
			}
		}
	}
}

func TestShippedNodesClusterRoleStaysMinimal(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "components", "scope-namespaced", "clusterrole-watcher-nodes.yaml"))
	if err != nil {
		t.Fatalf("reading deploy/components/scope-namespaced/clusterrole-watcher-nodes.yaml: %v", err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parsing deploy/components/scope-namespaced/clusterrole-watcher-nodes.yaml: %v", err)
	}
	readOnly := []string{"get", "list", "watch"}
	allowedResources := []string{"nodes", "persistentvolumes"}
	for _, rule := range role.Rules {
		for _, verb := range rule.Verbs {
			if !slices.Contains(readOnly, verb) {
				t.Errorf("nodes ClusterRole rule %v grants non-read verb %q", rule.Resources, verb)
			}
		}
		for _, res := range rule.Resources {
			if !slices.Contains(allowedResources, res) {
				t.Errorf("nodes ClusterRole grants unexpected cluster resource %q (expected only nodes, persistentvolumes)", res)
			}
		}
	}
}

func TestShippedCapacityRolePinsResourceName(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "base", "14-role-watcher-capacity.yaml"))
	if err != nil {
		t.Fatalf("reading deploy/base/14-role-watcher-capacity.yaml: %v", err)
	}
	var role rbacv1.Role
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parsing deploy/base/14-role-watcher-capacity.yaml: %v", err)
	}
	if role.Namespace != "kube-system" {
		t.Errorf("capacity Role namespace = %q, want kube-system", role.Namespace)
	}
	readOnly := []string{"get", "list"}
	for _, rule := range role.Rules {
		for _, verb := range rule.Verbs {
			if !slices.Contains(readOnly, verb) {
				t.Errorf("capacity Role rule %v grants unexpected verb %q", rule.Resources, verb)
			}
		}
		if slices.Contains(rule.Verbs, "get") && !slices.Equal(rule.ResourceNames, []string{"cluster-autoscaler-status"}) {
			t.Errorf("capacity Role get rule must pin resourceNames=[\"cluster-autoscaler-status\"], got %v", rule.ResourceNames)
		}
	}
}

// TestNoSecretsComponentPatchesMatchBaseMinusSecrets pins both strategic-merge
// RBAC patches in deploy/components/no-secrets/ (#852) to their respective
// source manifests minus `secrets` and `serviceaccounts`, and ensures the
// deprecated root compatibility copies in deploy/ stay byte-equal to
// deploy/base/.
func TestNoSecretsComponentPatchesMatchBaseMinusSecrets(t *testing.T) {
	stripSecretsAndSAs := func(rules []rbacv1.PolicyRule) []rbacv1.PolicyRule {
		var out []rbacv1.PolicyRule
		for _, r := range rules {
			if slices.Equal(r.APIGroups, []string{""}) &&
				(slices.Equal(r.Resources, []string{"secrets"}) || slices.Equal(r.Resources, []string{"serviceaccounts"})) {
				continue
			}
			out = append(out, r)
		}
		return out
	}

	baseCR := loadWatcherClusterRole(t)
	patchCRBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "components", "no-secrets", "patch-clusterrole.yaml"))
	if err != nil {
		t.Fatalf("reading no-secrets/patch-clusterrole.yaml: %v", err)
	}
	var patchCR rbacv1.ClusterRole
	if err := yaml.Unmarshal(patchCRBytes, &patchCR); err != nil {
		t.Fatalf("parsing no-secrets/patch-clusterrole.yaml: %v", err)
	}
	wantCR := stripSecretsAndSAs(baseCR.Rules)
	if len(patchCR.Rules) != len(wantCR) {
		t.Fatalf("no-secrets ClusterRole rules count = %d, want %d", len(patchCR.Rules), len(wantCR))
	}
	for i := range wantCR {
		if !slices.Equal(patchCR.Rules[i].APIGroups, wantCR[i].APIGroups) ||
			!slices.Equal(patchCR.Rules[i].Resources, wantCR[i].Resources) ||
			!slices.Equal(patchCR.Rules[i].Verbs, wantCR[i].Verbs) ||
			!slices.Equal(patchCR.Rules[i].ResourceNames, wantCR[i].ResourceNames) {
			t.Errorf("no-secrets ClusterRole rule[%d] = %+v, want %+v", i, patchCR.Rules[i], wantCR[i])
		}
	}

	nsRoleBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "components", "scope-namespaced", "role-watcher.yaml"))
	if err != nil {
		t.Fatalf("reading scope-namespaced/role-watcher.yaml: %v", err)
	}
	var nsRole rbacv1.Role
	if err := yaml.Unmarshal(nsRoleBytes, &nsRole); err != nil {
		t.Fatalf("parsing scope-namespaced/role-watcher.yaml: %v", err)
	}
	patchRoleBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "components", "no-secrets", "patch-role.yaml"))
	if err != nil {
		t.Fatalf("reading no-secrets/patch-role.yaml: %v", err)
	}
	var patchRole rbacv1.Role
	if err := yaml.Unmarshal(patchRoleBytes, &patchRole); err != nil {
		t.Fatalf("parsing no-secrets/patch-role.yaml: %v", err)
	}
	wantRole := stripSecretsAndSAs(nsRole.Rules)
	if len(patchRole.Rules) != len(wantRole) {
		t.Fatalf("no-secrets Role rules count = %d, want %d", len(patchRole.Rules), len(wantRole))
	}
	for i := range wantRole {
		if !slices.Equal(patchRole.Rules[i].APIGroups, wantRole[i].APIGroups) ||
			!slices.Equal(patchRole.Rules[i].Resources, wantRole[i].Resources) ||
			!slices.Equal(patchRole.Rules[i].Verbs, wantRole[i].Verbs) {
			t.Errorf("no-secrets Role rule[%d] = %+v, want %+v", i, patchRole.Rules[i], wantRole[i])
		}
	}

	for _, pair := range [][2]string{
		{"deploy/base/12-clusterrole-watcher.yaml", "deploy/12-clusterrole-watcher.yaml"},
		{"deploy/base/14-role-watcher-capacity.yaml", "deploy/14-role-watcher-capacity.yaml"},
		{"deploy/base/51-deployment-watcher.yaml", "deploy/51-deployment-watcher.yaml"},
		{"deploy/mcp/mcp.yaml", "deploy-mcp-gemini/mcp.yaml"},
		{"deploy/mcp/gateway.yaml", "deploy-mcp-gemini/gateway.yaml"},
	} {
		canonicalPath := filepath.Join("..", "..", "..", filepath.FromSlash(pair[0]))
		compatPath := filepath.Join("..", "..", "..", filepath.FromSlash(pair[1]))
		info, err := os.Lstat(compatPath)
		if err != nil {
			t.Fatalf("stat compatibility copy %s: %v", pair[1], err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			t.Errorf("%s is a symlink; keep a regular file so raw GitHub fetches and core.symlinks=false clones get valid YAML", pair[1])
		}
		canonical, err := os.ReadFile(canonicalPath)
		if err != nil {
			t.Fatalf("reading %s: %v", pair[0], err)
		}
		compat, err := os.ReadFile(compatPath)
		if err != nil {
			t.Fatalf("reading compatibility copy %s: %v", pair[1], err)
		}
		if string(canonical) != string(compat) {
			t.Errorf("%s drifted from %s; keep compatibility copy byte-identical", pair[1], pair[0])
		}
	}
}
