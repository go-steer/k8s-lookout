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
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// --- fixture builders ---

var (
	bootstrapLabels = map[string]string{"kubernetes.io/bootstrapping": "rbac-defaults"}
	reconcileLabels = map[string]string{"addonmanager.kubernetes.io/mode": "Reconcile"}
)

func rule(groups, resources, verbs []string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{APIGroups: groups, Resources: resources, Verbs: verbs}
}

func star() []string { return []string{"*"} }

// fullWildcard is the one resource rule cluster-admin consists of.
func fullWildcard() rbacv1.PolicyRule { return rule(star(), star(), star()) }

func clusterRole(name string, labels map[string]string, rules ...rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Rules: rules}
}

func aggregated(r *rbacv1.ClusterRole) *rbacv1.ClusterRole {
	r.AggregationRule = &rbacv1.AggregationRule{ClusterRoleSelectors: []metav1.LabelSelector{
		{MatchLabels: map[string]string{"aggregate-to-" + r.Name: "true"}},
	}}
	return r
}

func role(ns, name string, rules ...rbacv1.PolicyRule) *rbacv1.Role {
	return &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Rules: rules}
}

func clusterBinding(name string, labels map[string]string, roleName string, subjects ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
		Subjects:   subjects,
	}
}

func roleBinding(ns, name, refKind, refName string, subjects ...rbacv1.Subject) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: refKind, Name: refName},
		Subjects:   subjects,
	}
}

func user(name string) rbacv1.Subject  { return rbacv1.Subject{Kind: rbacv1.UserKind, Name: name} }
func group(name string) rbacv1.Subject { return rbacv1.Subject{Kind: rbacv1.GroupKind, Name: name} }
func sa(ns, name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: ns, Name: name}
}

// bootstrapRBAC is what every cluster ships: the reconciled defaults,
// which must produce no finding at all.
func bootstrapRBAC() []runtime.Object {
	return []runtime.Object{
		clusterRole("cluster-admin", bootstrapLabels, fullWildcard(),
			rbacv1.PolicyRule{NonResourceURLs: star(), Verbs: star()}),
		clusterBinding("cluster-admin", bootstrapLabels, "cluster-admin", group("system:masters")),
		aggregated(clusterRole("admin", bootstrapLabels, rule([]string{"apps"}, []string{"deployments"}, star()))),
		aggregated(clusterRole("view", bootstrapLabels, rule([]string{""}, []string{"pods"}, []string{"get", "list", "watch"}))),
	}
}

// rbacCluster is the shared fixture: one object per shape the command
// has to tell apart.
//
//	cluster-wide
//	  CRB oncall-admins        → cluster-admin, 2 subjects — warning
//	  CR  platform-superuser   full wildcard under another name, bound once
//	  CRB superuser-ci         → platform-superuser — warning, as cluster-admin
//	  CR  crd-reader           every resource in example.com, read verbs — info
//	  CRB crd-reader           → crd-reader
//	  CR  gke-addon            full wildcard, addon-manager Reconcile — platform
//	  CR  monitoring           aggregated, full wildcard copied in — aggregated
//	team-a
//	  RB  owners               → ClusterRole cluster-admin — info, in-namespace
//	  R   deployer             every verb on deployments, bound once — info
//	  RB  deployer             → Role deployer
//	  R   unused-root          full wildcard, bound by nothing — info
//	  R   reader               get/list pods — silent
//	  RB  viewers              → view — silent
//	  RB  nobody               → cluster-admin, no subjects — silent
//	  RB  dangling             → ClusterRole gone — edge.rbac_dangling's claim, silent
func rbacCluster() []runtime.Object {
	objs := bootstrapRBAC()
	return append(objs,
		clusterBinding("oncall-admins", nil, "cluster-admin", group("oncall"), user("alice@example.com")),
		clusterRole("platform-superuser", nil, fullWildcard()),
		clusterBinding("superuser-ci", nil, "platform-superuser", sa("ci", "deployer")),
		clusterRole("crd-reader", nil, rule([]string{"example.com"}, star(), []string{"get", "list", "watch"})),
		clusterBinding("crd-reader", nil, "crd-reader", sa("ops", "crd-operator")),
		clusterRole("gke-addon", reconcileLabels, fullWildcard()),
		aggregated(clusterRole("monitoring", nil, fullWildcard())),

		roleBinding("team-a", "owners", "ClusterRole", "cluster-admin", group("team-a-owners")),
		role("team-a", "deployer", rule([]string{"apps"}, []string{"deployments"}, star())),
		roleBinding("team-a", "deployer", "Role", "deployer", sa("team-a", "ci")),
		role("team-a", "unused-root", fullWildcard()),
		role("team-a", "reader", rule([]string{""}, []string{"pods"}, []string{"get", "list"})),
		roleBinding("team-a", "viewers", "ClusterRole", "view", group("team-a-devs")),
		roleBinding("team-a", "nobody", "ClusterRole", "cluster-admin"),
		roleBinding("team-a", "dangling", "ClusterRole", "deleted-role", user("bob")),
	)
}

func TestRBACContract(t *testing.T) {
	checktest.VerifyContract(t, audit.RBACCommand(testDeps(rbacCluster()...)), "-A")
}

func TestRBACGolden(t *testing.T) {
	res := checktest.Run(t, audit.RBACCommand(testDeps(rbacCluster()...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/rbac.golden", res.Stdout)
}

// findRecord returns the one finding about kind on Kind/namespace/name,
// failing the test when there is not exactly one.
func findRecord(t *testing.T, stdout, kind, objKind, ns, name string) map[string]string {
	t.Helper()
	var hits []map[string]string
	for _, r := range findingLines(t, stdout) {
		if r["kind"] == kind && r["kind_of_object"] == objKind && r["namespace"] == ns && r["name"] == name {
			hits = append(hits, r)
		}
	}
	if len(hits) != 1 {
		t.Fatalf("want exactly one %s on %s %s/%s, got %d:\n%s", kind, objKind, ns, name, len(hits), stdout)
	}
	return hits[0]
}

// Every cluster has the bootstrap cluster-admin binding to
// system:masters. The API server reconciles it back on every start, so
// no edit clears a finding against it — it must be counted, not
// reported, or the check fires on every cluster in the fleet.
func TestRBACBootstrapDefaultsAreSilent(t *testing.T) {
	res := checktest.Run(t, audit.RBACCommand(testDeps(bootstrapRBAC()...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.HasPrefix(res.Stdout, "scanned=4 findings=0 ") {
		t.Errorf("the bootstrap defaults are not a finding, got:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "platform_managed=4") {
		t.Errorf("the defaults are excluded, not skipped silently — want platform_managed=4:\n%s", res.Stdout)
	}
}

func TestRBACClusterAdminBinding(t *testing.T) {
	res := checktest.Run(t, audit.RBACCommand(testDeps(rbacCluster()...)), "-A")

	r := findRecord(t, res.Stdout, "audit.cluster_admin_binding", "ClusterRoleBinding", "", "oncall-admins")
	if r["severity"] != "warning" || r["reason"] != "ClusterAdminClusterWide" {
		t.Errorf("cluster-wide cluster-admin is a warning: %v", r)
	}
	if r["role"] != "ClusterRole/cluster-admin" || r["subjects"] != "2" {
		t.Errorf("role/subjects wrong: %v", r)
	}
	if r["subject_names"] != "Group:oncall,User:alice@example.com" {
		t.Errorf("subject_names = %q", r["subject_names"])
	}

	// Through a RoleBinding the same role is confined to one namespace.
	r = findRecord(t, res.Stdout, "audit.cluster_admin_binding", "RoleBinding", "team-a", "owners")
	if r["severity"] != "info" || r["reason"] != "ClusterAdminInNamespace" {
		t.Errorf("a RoleBinding confines cluster-admin to its namespace, want info: %v", r)
	}
}

// The claim is about what the role grants, not what it is called: a
// full-wildcard ClusterRole under another name is cluster-admin, and a
// check matching the string would read as an all-clear over it.
func TestRBACEquivalentRoleIsClusterAdmin(t *testing.T) {
	res := checktest.Run(t, audit.RBACCommand(testDeps(rbacCluster()...)), "-A")
	r := findRecord(t, res.Stdout, "audit.cluster_admin_binding", "ClusterRoleBinding", "", "superuser-ci")
	if r["severity"] != "warning" || r["role"] != "ClusterRole/platform-superuser" {
		t.Errorf("a binding to a full-wildcard role is a cluster-admin binding: %v", r)
	}
	if !strings.Contains(r["message"], "cluster-admin under another name") {
		t.Errorf("message should say why a differently-named role counts: %q", r["message"])
	}
	if r["subject_names"] != "ServiceAccount:ci/deployer" {
		t.Errorf("subject_names = %q", r["subject_names"])
	}

	w := findRecord(t, res.Stdout, "audit.wildcard_rbac", "ClusterRole", "", "platform-superuser")
	if w["severity"] != "warning" || w["reason"] != "FullWildcard" || w["bindings"] != "1" {
		t.Errorf("a bound full wildcard is a warning: %v", w)
	}
}

func TestRBACWildcardLadder(t *testing.T) {
	res := checktest.Run(t, audit.RBACCommand(testDeps(rbacCluster()...)), "-A")

	r := findRecord(t, res.Stdout, "audit.wildcard_rbac", "Role", "team-a", "deployer")
	if r["reason"] != "WildcardVerbs" || r["severity"] != "info" {
		t.Errorf("every verb on named resources is WildcardVerbs at info: %v", r)
	}
	if r["verbs"] != "*" || r["resources"] != "deployments" || r["api_groups"] != "apps" {
		t.Errorf("the offending rule should be rendered: %v", r)
	}

	r = findRecord(t, res.Stdout, "audit.wildcard_rbac", "ClusterRole", "", "crd-reader")
	if r["reason"] != "WildcardResources" || r["severity"] != "info" {
		t.Errorf("read verbs on every resource of a group is WildcardResources at info: %v", r)
	}

	// Unbound, a full wildcard grants nothing today: still in the
	// inventory, but not a warning.
	r = findRecord(t, res.Stdout, "audit.wildcard_rbac", "Role", "team-a", "unused-root")
	if r["reason"] != "FullWildcard" || r["severity"] != "info" || r["bindings"] != "0" {
		t.Errorf("an unbound full wildcard is info: %v", r)
	}
}

func TestRBACBroadestRuleWins(t *testing.T) {
	objs := []runtime.Object{
		role("prod", "mixed",
			rule([]string{""}, star(), []string{"get"}),
			rule([]string{"apps"}, []string{"deployments"}, star()),
			rule([]string{""}, []string{"pods"}, []string{"list"})),
	}
	res := checktest.Run(t, audit.RBACCommand(testDeps(objs...)), "-A")
	r := findRecord(t, res.Stdout, "audit.wildcard_rbac", "Role", "prod", "mixed")
	if r["reason"] != "WildcardVerbs" || r["wildcard_rules"] != "2" || r["rules"] != "3" {
		t.Errorf("one finding per role, under the broadest reason: %v", r)
	}
}

// Look-alikes that grant nothing to anyone, or nothing broad.
func TestRBACLookAlikesAreSilent(t *testing.T) {
	res := checktest.Run(t, audit.RBACCommand(testDeps(rbacCluster()...)), "-A")
	for _, r := range findingLines(t, res.Stdout) {
		switch r["name"] {
		case "reader", "viewers", "nobody", "dangling", "gke-addon", "monitoring", "admin", "view":
			t.Errorf("%s/%s should not be reported: %v", r["kind_of_object"], r["name"], r)
		}
	}
	if !strings.Contains(res.Stdout, "aggregated_roles=1") {
		t.Errorf("the user aggregated role is excluded and counted:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "platform_managed=5") {
		t.Errorf("bootstrap 4 + the Reconcile addon role = 5:\n%s", res.Stdout)
	}
	// An apiGroups-only wildcard over named resources is not the claim.
	only := []runtime.Object{role("prod", "any-group-deploys", rule(star(), []string{"deployments"}, []string{"get"}))}
	res = checktest.Run(t, audit.RBACCommand(testDeps(only...)), "-A")
	if !strings.HasPrefix(res.Stdout, "scanned=1 findings=0 ") {
		t.Errorf("apiGroups-only wildcard is out of scope:\n%s", res.Stdout)
	}
}

// --namespace judges that namespace's objects; cluster-scoped ones are
// -A's, though a RoleBinding to a ClusterRole is still resolved.
func TestRBACNamespaceScope(t *testing.T) {
	res := checktest.Run(t, audit.RBACCommand(testDeps(rbacCluster()...)), "--namespace=team-a")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, r := range findingLines(t, res.Stdout) {
		if r["namespace"] != "team-a" {
			t.Errorf("cluster-scoped subject under --namespace: %v", r)
		}
	}
	findRecord(t, res.Stdout, "audit.cluster_admin_binding", "RoleBinding", "team-a", "owners")
	if !strings.Contains(res.Stdout, "scanned=8 ") {
		t.Errorf("scanned counts team-a's 5 bindings and 3 roles, got:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "aggregated_roles=") {
		t.Errorf("ClusterRoles are not judged under --namespace:\n%s", res.Stdout)
	}
}

func TestRBACScopeErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"no scope": nil,
		"workload": {"--workload=Deployment/prod/web"},
	} {
		t.Run(name, func(t *testing.T) {
			res := checktest.Run(t, audit.RBACCommand(testDeps()), args...)
			if res.Code != emit.ExitUsage {
				t.Errorf("exit %d, want usage; stderr: %s", res.Code, res.Stderr)
			}
		})
	}
}
