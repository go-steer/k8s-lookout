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

package audit

import (
	"context"
	"fmt"
	"sort"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The RBAC over-permission kinds (#184): the compliance-audit slugs
// cluster-admin-binding and wildcard-rbac. One is about who holds the
// power, the other about which object grants it, so their subjects
// differ — a binding and a role.
const (
	kindClusterAdminBinding = "audit.cluster_admin_binding"
	kindWildcardRBAC        = "audit.wildcard_rbac"
)

const (
	reasonClusterAdminClusterWide = "ClusterAdminClusterWide"
	reasonClusterAdminInNamespace = "ClusterAdminInNamespace"
	reasonFullWildcard            = "FullWildcard"
	reasonWildcardVerbs           = "WildcardVerbs"
	reasonWildcardResources       = "WildcardResources"
)

// The two labels that mark an RBAC object as the platform's rather than
// the operator's. Both mean the object is RECONCILED: the API server
// re-creates its bootstrap policy on every start, and the addon manager
// rewrites a Reconcile-mode object back to its manifest within a
// minute. Either way an operator who edited or deleted it would find it
// back, so a finding against it would carry a remedy that cannot work.
const (
	labelBootstrapping      = "kubernetes.io/bootstrapping"
	valueBootstrapping      = "rbac-defaults"
	labelAddonManagerMode   = "addonmanager.kubernetes.io/mode"
	valueAddonManagerReconc = "Reconcile"
)

// RBACCommand builds `lookout audit rbac`: RBAC that works exactly as
// written and grants too much (#184).
//
// # What the two claims mean, precisely
//
// A role is FULL-WILDCARD when one of its resource rules names `*` in
// apiGroups, resources and verbs at once. That is the whole definition
// of `cluster-admin` — its one resource rule is exactly that — so
// audit.cluster_admin_binding judges a binding by the rules of the role
// it points at, not by the role's name: a ClusterRole called
// `platform-superuser` with the same rule is cluster-admin under
// another name, and a check that matched the string would read as an
// all-clear over it.
//
// audit.wildcard_rbac reports the role itself, once, under the
// broadest wildcard any of its rules uses: FullWildcard, then
// WildcardVerbs (every verb, including escalate, bind and impersonate
// where the resources allow them, on named resources), then
// WildcardResources (named verbs on every resource in the groups
// named, which in the core group includes Secrets). A wildcard only in
// apiGroups over named resources is not reported: it widens a grant
// to same-named resources in other groups, which is rarely where the
// power is, and a rule that fired on it would fire on most operator
// roles in existence. Non-resource URL rules are not judged either.
//
// # Why this is posture and not `state edges`
//
// `state edges` already walks these objects for edge.rbac_dangling —
// a binding naming a role that does not exist, which is broken now.
// These bindings work perfectly; they are the "too much power"
// complement, and putting them in `state edges` would hand posture
// findings to a consumer calling it for incident triage
// (docs/fleet-audit-detectors-design.md decision 4). The group is
// separate; the paged Lists are shared (state.ListRoleBindings and
// friends).
//
// # Severities
//
// A ClusterRoleBinding to a full-wildcard role is a warning: its
// subjects can do anything to anything, including grant themselves
// whatever the next audit asks them to give up. The same role through
// a RoleBinding is info — full control of one namespace, which is
// close to what the built-in `admin` role already grants and is how
// some platforms hand namespaces to their owners.
//
// On a role, only a BOUND full wildcard is a warning. An unbound role
// grants nothing until something binds it, and a WildcardVerbs or
// WildcardResources rule is how most operators are written; both are
// still reported, at info, so the inventory is complete and a reviewed
// exemption — not silence — is what retires one.
//
// # What is excluded, and why it is not a silent skip
//
// Objects the platform reconciles (the kubernetes.io/bootstrapping or
// addonmanager Reconcile labels above) are not subjects: every cluster
// has the bootstrap `cluster-admin` ClusterRoleBinding to
// system:masters, and a finding no edit can clear is wallpaper. They
// are counted in the platform_managed note. Aggregated ClusterRoles
// are not wildcard_rbac subjects either — their rules are copied in by
// a controller from the roles that match the aggregation selector, so
// the wildcard is reported once, on the role it came from — and are
// counted in aggregated_roles. Platform objects a provider ships
// without either label will be reported; the response is an exemption,
// as with audit.podsecurity_gaps.
func RBACCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:        "audit rbac",
		MCPName:     "k8s_audit_rbac",
		MCPProfiles: []string{"audit"},
		Summary:     "RBAC over-permission posture: bindings that grant cluster-admin, or a role whose rules are cluster-admin under another name, and Roles/ClusterRoles whose rules use `*` for verbs or resources. Judges a binding by the rules of the role it points at, not by the role's name. Objects the platform reconciles (the bootstrap defaults, addon-manager Reconcile objects) are counted, not reported. --namespace judges that namespace's Roles and RoleBindings; ClusterRoles and ClusterRoleBindings are judged under -A. scanned counts bindings and roles examined.",
		Kinds: []checks.KindField{
			checks.Kind(kindClusterAdminBinding, "the binding grants a full-wildcard role (cluster-admin, or one with the same rule) to its subjects; warning for a ClusterRoleBinding, info for a RoleBinding, which confines it to one namespace", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindWildcardRBAC, "the role has a rule using `*` for verbs or resources; warning for a full wildcard that something binds, info for an unbound one and for the narrower wildcards", emit.SeverityWarning, emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "role", Doc: "the role the binding points at, as Kind/name"},
			{Name: "subjects", Doc: "subjects the binding grants the role to"},
			{Name: "subject_names", Doc: "those subjects as Kind:name (ServiceAccounts as ServiceAccount:namespace/name), sorted and capped at 8"},
			{Name: "rules", Doc: "rules in the role, aggregated or not"},
			{Name: "wildcard_rules", Doc: "rules in the role that use `*` for verbs or resources"},
			{Name: "verbs", Doc: "the verbs of the broadest wildcard rule, comma-joined"},
			{Name: "resources", Doc: "the resources of the broadest wildcard rule, comma-joined"},
			{Name: "api_groups", Doc: "the apiGroups of the broadest wildcard rule, comma-joined, with the core group written as `core`"},
			{Name: "bindings", Doc: "RoleBindings and ClusterRoleBindings pointing at the role; 0 means it grants nothing today"},
			{Name: "platform_managed", Doc: "summary note: bindings and roles left out because the platform reconciles them (kubernetes.io/bootstrapping=rbac-defaults, or addonmanager.kubernetes.io/mode=Reconcile)"},
			{Name: "aggregated_roles", Doc: "summary note: aggregated ClusterRoles left out of audit.wildcard_rbac, whose rules come from the roles that are judged; omitted when there are none or ClusterRoles are out of scope"},
		},
		Examples: []string{
			"lookout audit rbac -A",
			"lookout audit rbac --namespace=prod",
			"lookout audit rbac -A --exemptions=exemptions.yaml --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runRBAC(ctx, deps, inv)
		},
	}
}

// rbacObjects is one pass's worth of RBAC. The cluster-scoped slices are
// always filled — a RoleBinding may point at a ClusterRole, so resolving
// namespaced grants needs them — but they are only JUDGED when
// clusterScope is set.
type rbacObjects struct {
	roleBindings        []rbacv1.RoleBinding
	roles               []rbacv1.Role
	clusterRoleBindings []rbacv1.ClusterRoleBinding
	clusterRoles        []rbacv1.ClusterRole
	clusterScope        bool
}

func runRBAC(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit rbac judges bindings and roles, not workloads, so it is scoped by namespace: use --namespace=%s or -A", inv.Scope.Workload.Namespace)
	}
	if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns> or -A")
	}
	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	objs := rbacObjects{clusterScope: inv.Scope.AllNamespaces}
	if err := state.ListRoleBindings(ctx, client, listNS, func(r *rbacv1.RoleBinding) { objs.roleBindings = append(objs.roleBindings, *r) }); err != nil {
		return 0, err
	}
	if err := state.ListRoles(ctx, client, listNS, func(r *rbacv1.Role) { objs.roles = append(objs.roles, *r) }); err != nil {
		return 0, err
	}
	if objs.clusterScope {
		if err := state.ListClusterRoleBindings(ctx, client, func(r *rbacv1.ClusterRoleBinding) {
			objs.clusterRoleBindings = append(objs.clusterRoleBindings, *r)
		}); err != nil {
			return 0, err
		}
	}
	if err := state.ListClusterRoles(ctx, client, func(r *rbacv1.ClusterRole) { objs.clusterRoles = append(objs.clusterRoles, *r) }); err != nil {
		return 0, err
	}

	findings, tally := judgeRBAC(objs)
	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("platform_managed", itoa(tally.platform)); err != nil {
		return 0, err
	}
	if tally.aggregated > 0 {
		if err := inv.Out.Note("aggregated_roles", itoa(tally.aggregated)); err != nil {
			return 0, err
		}
	}
	return tally.scanned, nil
}

// rbacTally is what judgeRBAC examined beside what it found: the
// denominators the summary line owes a reader.
type rbacTally struct {
	scanned    int
	platform   int
	aggregated int
}

// judgeRBAC makes both claims over one pass's objects. It is a pure
// function of them, like every judge in this group.
func judgeRBAC(objs rbacObjects) ([]emit.Finding, rbacTally) {
	var tally rbacTally
	var out []emit.Finding

	// Rules by role, and how many bindings point at each — a role's
	// finding needs the second, a binding's needs the first.
	roleRules := map[string][]rbacv1.PolicyRule{}
	for _, r := range objs.roles {
		roleRules[roleKey("Role", r.Namespace, r.Name)] = r.Rules
	}
	for _, r := range objs.clusterRoles {
		roleRules[roleKey("ClusterRole", "", r.Name)] = r.Rules
	}
	bound := map[string]int{}
	for _, b := range objs.roleBindings {
		bound[bindingRoleKey(b.RoleRef, b.Namespace)]++
	}
	for _, b := range objs.clusterRoleBindings {
		bound[bindingRoleKey(b.RoleRef, "")]++
	}

	for _, b := range objs.roleBindings {
		tally.scanned++
		if platformManaged(b.Labels) {
			tally.platform++
			continue
		}
		if f, ok := judgeBinding(b.ObjectMeta, "RoleBinding", b.RoleRef, b.Subjects, roleRules); ok {
			out = append(out, f)
		}
	}
	for _, b := range objs.clusterRoleBindings {
		tally.scanned++
		if platformManaged(b.Labels) {
			tally.platform++
			continue
		}
		if f, ok := judgeBinding(b.ObjectMeta, "ClusterRoleBinding", b.RoleRef, b.Subjects, roleRules); ok {
			out = append(out, f)
		}
	}
	for _, r := range objs.roles {
		tally.scanned++
		if platformManaged(r.Labels) {
			tally.platform++
			continue
		}
		if f, ok := judgeRole(r.ObjectMeta, "Role", r.Rules, bound[roleKey("Role", r.Namespace, r.Name)]); ok {
			out = append(out, f)
		}
	}
	if objs.clusterScope {
		for _, r := range objs.clusterRoles {
			tally.scanned++
			if platformManaged(r.Labels) {
				tally.platform++
				continue
			}
			if r.AggregationRule != nil {
				tally.aggregated++
				continue
			}
			if f, ok := judgeRole(r.ObjectMeta, "ClusterRole", r.Rules, bound[roleKey("ClusterRole", "", r.Name)]); ok {
				out = append(out, f)
			}
		}
	}
	return out, tally
}

func roleKey(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

// bindingRoleKey resolves a roleRef to the key of the role it names. A
// Role is always in the binding's own namespace; a ClusterRole has none.
func bindingRoleKey(ref rbacv1.RoleRef, bindingNS string) string {
	if ref.Kind == "ClusterRole" {
		return roleKey("ClusterRole", "", ref.Name)
	}
	return roleKey(ref.Kind, bindingNS, ref.Name)
}

func platformManaged(l map[string]string) bool {
	return l[labelBootstrapping] == valueBootstrapping || l[labelAddonManagerMode] == valueAddonManagerReconc
}

// judgeBinding reports a binding whose role holds a full-wildcard rule.
// A binding whose role does not exist is edge.rbac_dangling's claim, and
// one with no subjects grants nothing to anyone; neither is reported.
func judgeBinding(meta metav1.ObjectMeta, kind string, ref rbacv1.RoleRef, subjects []rbacv1.Subject, roleRules map[string][]rbacv1.PolicyRule) (emit.Finding, bool) {
	rules, ok := roleRules[bindingRoleKey(ref, meta.Namespace)]
	if !ok || len(subjects) == 0 || !anyRule(rules, isFullWildcard) {
		return emit.Finding{}, false
	}
	role := ref.Kind + "/" + ref.Name
	grant := "cluster-admin"
	if ref.Kind != "ClusterRole" || ref.Name != "cluster-admin" {
		grant = fmt.Sprintf("%s, whose rules grant every verb on every resource — cluster-admin under another name,", role)
	}
	severity, reason := emit.SeverityWarning, reasonClusterAdminClusterWide
	message := fmt.Sprintf("binds %s to %d %s cluster-wide: %s can do anything to any object in the cluster, including rewrite RBAC to keep that power",
		grant, len(subjects), plural(len(subjects), "subject"), pronounFor(len(subjects)))
	if kind == "RoleBinding" {
		severity, reason = emit.SeverityInfo, reasonClusterAdminInNamespace
		message = fmt.Sprintf("binds %s to %d %s in this namespace: full control of every object in it, including its Secrets and its RBAC — more than the built-in admin role, which is the one meant for namespace owners",
			grant, len(subjects), plural(len(subjects), "subject"))
	}
	f := emit.Finding{
		Kind:     kindClusterAdminBinding,
		Severity: severity,
		Reason:   reason,
		Message:  message,
		Details: []emit.Field{
			{Key: "role", Value: role},
			{Key: "subjects", Value: itoa(len(subjects))},
			{Key: "subject_names", Value: cappedList(subjectNames(subjects))},
		},
		Namespace:    meta.Namespace,
		KindOfObject: kind,
		Name:         meta.Name,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f, true
}

// pronounFor keeps "1 subject ... it can" and "3 subjects ... they can"
// agreeing.
func pronounFor(n int) string {
	if n == 1 {
		return "it"
	}
	return "they"
}

// judgeRole reports a role whose rules use `*` for verbs or resources,
// under the broadest reason any rule earns.
func judgeRole(meta metav1.ObjectMeta, kind string, rules []rbacv1.PolicyRule, bindings int) (emit.Finding, bool) {
	var (
		worst     = -1
		worstRule rbacv1.PolicyRule
		wildRules int
	)
	for _, r := range rules {
		rank := wildcardRank(r)
		if rank < 0 {
			continue
		}
		wildRules++
		if rank > worst {
			worst, worstRule = rank, r
		}
	}
	if worst < 0 {
		return emit.Finding{}, false
	}
	reason := reasonWildcardResources
	switch worst {
	case 2:
		reason = reasonFullWildcard
	case 1:
		reason = reasonWildcardVerbs
	}

	var what string
	switch reason {
	case reasonFullWildcard:
		what = "a rule granting every verb on every resource in every API group — the rule that defines cluster-admin"
	case reasonWildcardVerbs:
		what = fmt.Sprintf("a rule granting every verb on %s, which includes delete, and escalate, bind or impersonate wherever those resources accept them", strings.Join(worstRule.Resources, ","))
	default:
		what = fmt.Sprintf("a rule granting %s on every resource in %s, which covers resources added after the role was written", strings.Join(worstRule.Verbs, ","), groupPhrase(worstRule.APIGroups))
		if hasStar(worstRule.APIGroups) || hasGroup(worstRule.APIGroups, "") {
			what += ", Secrets among them"
		}
	}
	severity := emit.SeverityInfo
	boundPhrase := fmt.Sprintf("%d bindings grant it", bindings)
	if bindings == 1 {
		boundPhrase = "1 binding grants it"
	}
	if bindings == 0 {
		boundPhrase = "nothing binds it, so it grants nothing until something does"
	} else if reason == reasonFullWildcard {
		severity = emit.SeverityWarning
	}
	f := emit.Finding{
		Kind:     kindWildcardRBAC,
		Severity: severity,
		Reason:   reason,
		Message:  fmt.Sprintf("%s has %s; %s", kind, what, boundPhrase),
		Details: []emit.Field{
			{Key: "rules", Value: itoa(len(rules))},
			{Key: "wildcard_rules", Value: itoa(wildRules)},
			{Key: "verbs", Value: strings.Join(worstRule.Verbs, ",")},
			{Key: "resources", Value: strings.Join(worstRule.Resources, ",")},
			{Key: "api_groups", Value: strings.Join(groupNames(worstRule.APIGroups), ",")},
			{Key: "bindings", Value: itoa(bindings)},
		},
		Namespace:    meta.Namespace,
		KindOfObject: kind,
		Name:         meta.Name,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f, true
}

// wildcardRank places a rule on the reason ladder: 2 full wildcard, 1
// every verb, 0 every resource, -1 neither. A rule with no resources is
// a non-resource URL rule, which is not judged.
func wildcardRank(r rbacv1.PolicyRule) int {
	if len(r.Resources) == 0 {
		return -1
	}
	verbs, resources := hasStar(r.Verbs), hasStar(r.Resources)
	switch {
	case verbs && resources && hasStar(r.APIGroups):
		return 2
	case verbs:
		return 1
	case resources:
		return 0
	}
	return -1
}

func isFullWildcard(r rbacv1.PolicyRule) bool { return wildcardRank(r) == 2 }

func anyRule(rules []rbacv1.PolicyRule, pred func(rbacv1.PolicyRule) bool) bool {
	for _, r := range rules {
		if pred(r) {
			return true
		}
	}
	return false
}

func hasStar(items []string) bool {
	for _, s := range items {
		if s == rbacv1.ResourceAll {
			return true
		}
	}
	return false
}

func hasGroup(groups []string, want string) bool {
	for _, g := range groups {
		if g == want {
			return true
		}
	}
	return false
}

// groupNames renders apiGroups with the core group, whose name is the
// empty string, spelled out.
func groupNames(groups []string) []string {
	out := make([]string, 0, len(groups))
	for _, g := range groups {
		if g == "" {
			g = "core"
		}
		out = append(out, g)
	}
	return out
}

func groupPhrase(groups []string) string {
	if hasStar(groups) {
		return "every API group"
	}
	names := groupNames(groups)
	return plural(len(names), "API group") + " " + strings.Join(names, ",")
}

// subjectNames renders subjects the way an operator greps for them.
func subjectNames(subjects []rbacv1.Subject) []string {
	out := make([]string, 0, len(subjects))
	for _, s := range subjects {
		name := s.Name
		if s.Kind == rbacv1.ServiceAccountKind {
			name = s.Namespace + "/" + s.Name
		}
		out = append(out, s.Kind+":"+name)
	}
	sort.Strings(out)
	return out
}
