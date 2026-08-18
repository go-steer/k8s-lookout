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

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// kindNetpolMissing is the one condition this detector reports: traffic
// in a direction that no NetworkPolicy restricts (#185).
const kindNetpolMissing = "audit.netpol_missing"

// The reasons split that condition two ways — by direction, and by
// whether the namespace is trying at all. The distinction is the whole
// point of the check: a namespace with no policies has made a choice,
// while a workload that fell through the selectors of the policies its
// neighbours are covered by has almost certainly made a mistake.
const (
	reasonNoIngressPolicies         = "NoIngressPolicies"
	reasonNoEgressPolicies          = "NoEgressPolicies"
	reasonIngressPoliciesSelectNone = "IngressPoliciesSelectNothing"
	reasonEgressPoliciesSelectNone  = "EgressPoliciesSelectNothing"
	reasonUnselectedIngress         = "UnselectedIngress"
	reasonUnselectedEgress          = "UnselectedEgress"
)

// NetpolCommand builds `lookout audit netpol`: which workloads no
// NetworkPolicy restricts, in each direction (#185).
//
// # What "covered" means here, precisely
//
// A pod is isolated for a direction when some NetworkPolicy in its
// namespace selects it AND names that direction in policyTypes.
// Isolation is the whole of the Kubernetes model: an unisolated pod
// accepts from anywhere and dials anywhere, and no amount of policy on
// the OTHER pods changes that. So the question this check asks is
// exactly the question the API answers — is anything isolating this
// pod — and not the harder, less decidable question of whether the
// rules that follow are tight.
//
// That is also the stated limit. A policy whose rule is `ingress: [{}]`
// isolates the pod and then allows the whole cluster back in; this
// check counts it as coverage. Judging rule CONTENT is a different
// detector with a different failure mode (it has to model CIDRs,
// namespace selectors and ports), and reporting an allow-all rule as
// "no policy" would be wrong in the direction that matters — it would
// say a deliberate, reviewed policy does not exist.
//
// # Two subjects, because there are two different defects
//
// When NOTHING in the namespace is covered for a direction, one finding
// is emitted against the NAMESPACE. The alternative — one finding per
// workload — turns a single decision and a single remedy into as many
// records as the namespace has Deployments, which is the noise this
// group exists to avoid. Two reasons split that case, because the two
// are not the same defect: nobody wrote a policy, or somebody wrote one
// and its selector matches none of the pods it was meant to protect.
// The second is a typo with the blast radius of an outage, and unlike
// the first it is not something anyone chose.
//
// When SOME workloads are covered and others are not, each uncovered
// one gets its own finding. Here the per-workload subject is the right
// one: the gap is specific to that template's labels, and the fix is a
// selector, not a policy decision. This is the high-value half of the
// check — a namespace that believes it is locked down, with one
// workload that is not.
//
// # What is excluded, and why it is not a silent skip
//
// A hostNetwork pod uses the node's network namespace, so NetworkPolicy
// — which selects pods — cannot constrain it at all. Such templates are
// left out of the arithmetic rather than reported as uncovered, because
// no policy an operator could write would fix them. They are counted in
// host_network_workloads on the namespace finding, and `audit hardening`
// (#183) reports every one of them as audit.host_namespace with
// precisely this consequence spelled out in its message.
func NetpolCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:        "audit netpol",
		MCPName:     "k8s_audit_netpol",
		MCPProfiles: []string{"audit"},
		Summary:     "NetworkPolicy coverage posture: namespaces where nothing restricts ingress or egress at all, and individual workloads that fell through the selectors of the policies covering their neighbours. Coverage means isolation — some policy selects the pod and names the direction — not that the rules it then applies are tight. hostNetwork templates are excluded, since NetworkPolicy cannot constrain them. Scope with --namespace or -A; scanned counts pod templates examined.",
		Kinds: []checks.KindField{
			checks.Kind(kindNetpolMissing, "nothing restricts this direction for the subject — a namespace with no policy at all, or a workload the covering policies' selectors miss; info for the egress direction, where no policy is a defensible default", emit.SeverityWarning, emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "policies", Doc: "NetworkPolicies in the namespace naming this direction in policyTypes; 0 on a namespace-subject finding, and the number that failed to select the subject on a workload one"},
			{Name: "total_policies", Doc: "NetworkPolicies in the namespace in either direction, so an egress-only namespace does not read as an empty one"},
			{Name: "workloads", Doc: "pod templates in the namespace this claim covers, excluding hostNetwork ones; the finding does not fire at 0"},
			{Name: "host_network_workloads", Doc: "pod templates excluded because they use the node's network namespace, where NetworkPolicy does not apply; omitted at 0"},
			{Name: "covered_workloads", Doc: "pod templates in the namespace that ARE selected for this direction — the neighbours the subject fell out of step with"},
			{Name: "pod_labels", Doc: "the template's own labels, which are what the policies' selectors failed to match, sorted and capped at 8"},
			{Name: "namespaces", Doc: "summary note: namespaces examined — the denominator for the namespace-subject claims, which `scanned` (pod templates) does not cover"},
		},
		Examples: []string{
			"lookout audit netpol -A",
			"lookout audit netpol --namespace=prod",
			"lookout audit netpol -A --exemptions=exemptions.yaml --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runNetpol(ctx, deps, inv)
		},
	}
}

func runNetpol(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit netpol judges a workload against the policies of the whole namespace around it, so it is scoped by namespace: use --namespace=%s or -A", inv.Scope.Workload.Namespace)
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
	templates, err := listPodTemplates(ctx, client, listNS)
	if err != nil {
		return 0, err
	}
	namespaces, err := listNamespacesInScope(ctx, client, listNS)
	if err != nil {
		return 0, err
	}
	policies, err := listNetworkPolicies(ctx, client, listNS)
	if err != nil {
		return 0, err
	}

	var findings []emit.Finding
	for _, ns := range namespaces {
		findings = append(findings, judgeNetpolNamespace(ns.Name, templates, policies)...)
	}

	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("namespaces", itoa(len(namespaces))); err != nil {
		return 0, err
	}
	return len(templates), nil
}

// netpolDirection is one of the two halves of the NetworkPolicy model.
// Everything this check does, it does once per direction, so the two
// are a value rather than a duplicated code path.
type netpolDirection struct {
	policyType networkingv1.PolicyType
	// name is how the direction appears in messages, lowercase.
	name string
	// noneReason fires when the namespace has no policy at all in this
	// direction; selectNothingReason when it has some and they cover no
	// template in it; workloadReason when they cover some but not this
	// one.
	noneReason          string
	selectNothingReason string
	workloadReason      string
	// noneSeverity differs between the two directions, and the asymmetry
	// is deliberate — see netpolDirections. The other two reasons are
	// warnings in both directions, because a policy that selects nothing
	// and a workload outside the policies around it are mistakes rather
	// than postures, whichever way the traffic runs.
	noneSeverity string
	// consequence completes the namespace-subject message.
	consequence string
}

// netpolDirections is the pair, ingress first because that is the
// direction an attacker uses to arrive.
//
// The severities are asymmetric on purpose. Unrestricted INGRESS means
// every pod in the cluster can open a connection to these, which is the
// lateral-movement path that makes one compromised pod into a
// cluster-wide problem — a warning. Unrestricted EGRESS is reported at
// info: egress policy is far less widely adopted, it breaks DNS and
// webhook callouts when applied carelessly, and a whole fleet that has
// deliberately not adopted it should not read as a fleet of warnings.
// The claim is still made, because a namespace that IS locked down
// inbound and wide open outbound is worth knowing about, and because
// the alternative — omitting it — would be the unverifiable coverage
// this group exists to eliminate. To retire it for a cluster that has
// chosen not to do egress policy, write a reviewed exemption.
var netpolDirections = []netpolDirection{
	{
		policyType:          networkingv1.PolicyTypeIngress,
		name:                "ingress",
		noneReason:          reasonNoIngressPolicies,
		selectNothingReason: reasonIngressPoliciesSelectNone,
		workloadReason:      reasonUnselectedIngress,
		noneSeverity:        emit.SeverityWarning,
		consequence:         "every pod in the cluster can open a connection to any of them, so one compromised pod anywhere reaches all of these directly",
	},
	{
		policyType:          networkingv1.PolicyTypeEgress,
		name:                "egress",
		noneReason:          reasonNoEgressPolicies,
		selectNothingReason: reasonEgressPoliciesSelectNone,
		workloadReason:      reasonUnselectedEgress,
		noneSeverity:        emit.SeverityInfo,
		consequence:         "any of them can dial anything the cluster network reaches, including every other namespace and whatever the node can route to",
	},
}

func listNetworkPolicies(ctx context.Context, client kubernetes.Interface, ns string) ([]networkingv1.NetworkPolicy, error) {
	var out []networkingv1.NetworkPolicy
	err := listPages("networkpolicies", func(o metav1.ListOptions) ([]networkingv1.NetworkPolicy, string, error) {
		l, err := client.NetworkingV1().NetworkPolicies(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(p *networkingv1.NetworkPolicy) {
		out = append(out, *p)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// coversDirection reports whether a policy isolates the pods it selects
// in the given direction.
//
// policyTypes is defaulted by the API server, but the default is worth
// implementing rather than assuming: an unset policyTypes means Ingress
// always, and Egress only if the policy actually has egress rules. A
// policy read from a manifest, a dry-run, or an older stored object can
// arrive here with the field empty, and treating that as "covers
// nothing" would report a locked-down namespace as wide open.
func coversDirection(p networkingv1.NetworkPolicy, want networkingv1.PolicyType) bool {
	if len(p.Spec.PolicyTypes) == 0 {
		if want == networkingv1.PolicyTypeIngress {
			return true
		}
		return len(p.Spec.Egress) > 0
	}
	for _, t := range p.Spec.PolicyTypes {
		if t == want {
			return true
		}
	}
	return false
}

// judgeNetpolNamespace makes both directional claims about one
// namespace and the templates in it.
func judgeNetpolNamespace(namespace string, templates []podTemplate, policies []networkingv1.NetworkPolicy) []emit.Finding {
	var subjects []podTemplate
	hostNetwork := 0
	for _, t := range templates {
		if t.namespace != namespace {
			continue
		}
		// NetworkPolicy selects pods; a pod on the node's network stack
		// is not one it can reach. Excluded from the arithmetic rather
		// than reported as uncovered — no policy would fix it.
		if t.spec.HostNetwork {
			hostNetwork++
			continue
		}
		subjects = append(subjects, t)
	}
	if len(subjects) == 0 {
		// Nothing to protect. An empty namespace with no policies is not
		// a posture defect, it is an empty namespace.
		return nil
	}

	var inNamespace []networkingv1.NetworkPolicy
	for _, p := range policies {
		if p.Namespace == namespace {
			inNamespace = append(inNamespace, p)
		}
	}

	var out []emit.Finding
	for _, d := range netpolDirections {
		out = append(out, judgeNetpolDirection(namespace, d, subjects, inNamespace, hostNetwork)...)
	}
	return out
}

func judgeNetpolDirection(namespace string, d netpolDirection, subjects []podTemplate, inNamespace []networkingv1.NetworkPolicy, hostNetwork int) []emit.Finding {
	var selectors []labels.Selector
	for _, p := range inNamespace {
		if !coversDirection(p, d.policyType) {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(&p.Spec.PodSelector)
		if err != nil {
			// An unparseable selector is the API server's problem, not
			// this check's; skipping it can only widen the reported gap,
			// which is the safe direction for a posture claim.
			continue
		}
		selectors = append(selectors, sel)
	}

	var uncovered []podTemplate
	for _, t := range subjects {
		set := labels.Set(t.labels)
		covered := false
		for _, sel := range selectors {
			if sel.Matches(set) {
				covered = true
				break
			}
		}
		if !covered {
			uncovered = append(uncovered, t)
		}
	}
	covered := len(subjects) - len(uncovered)

	if covered == 0 {
		// Nothing in the namespace is restricted in this direction, so
		// there is one fact and one remedy: report it once, against the
		// namespace.
		return []emit.Finding{netpolNamespaceFinding(namespace, d, netpolNamespaceCounts{
			workloads:     len(subjects),
			directional:   len(selectors),
			totalPolicies: len(inNamespace),
			hostNetwork:   hostNetwork,
		})}
	}

	out := make([]emit.Finding, 0, len(uncovered))
	for _, t := range uncovered {
		out = append(out, netpolWorkloadFinding(t, d, len(selectors), len(inNamespace), covered))
	}
	return out
}

// netpolNamespaceCounts are the denominators behind a namespace-subject
// finding, grouped because four bare ints in a call are four chances to
// pass them in the wrong order.
type netpolNamespaceCounts struct {
	workloads     int
	directional   int
	totalPolicies int
	hostNetwork   int
}

// netpolNamespaceFinding reports a direction in which nothing in the
// namespace is restricted. The subject is the Namespace because that is
// where the one object that would fix it goes.
func netpolNamespaceFinding(namespace string, d netpolDirection, c netpolNamespaceCounts) emit.Finding {
	severity, reason := d.noneSeverity, d.noneReason
	message := fmt.Sprintf("no NetworkPolicy restricts %s in this namespace, covering %d %s: %s",
		d.name, c.workloads, plural(c.workloads, "workload"), d.consequence)
	if c.directional > 0 {
		// The policies exist and match nothing. Not a posture choice in
		// either direction — someone wrote the object believing it
		// applied, so this is a warning even for egress.
		severity, reason = emit.SeverityWarning, d.selectNothingReason
		message = fmt.Sprintf("%d %s %s in this namespace %s none of its %d %s: the namespace reads as policed and is not, since %s",
			c.directional, d.name, plural(c.directional, "NetworkPolicy"), matchVerb(c.directional),
			c.workloads, plural(c.workloads, "workload"), d.consequence)
	}
	details := []emit.Field{
		{Key: "policies", Value: itoa(c.directional)},
		{Key: "total_policies", Value: itoa(c.totalPolicies)},
		{Key: "workloads", Value: itoa(c.workloads)},
	}
	if c.hostNetwork > 0 {
		details = append(details, emit.Field{Key: "host_network_workloads", Value: itoa(c.hostNetwork)})
	}
	f := emit.Finding{
		Kind:         kindNetpolMissing,
		Severity:     severity,
		Reason:       reason,
		Message:      message,
		Details:      details,
		Namespace:    namespace,
		KindOfObject: "Namespace",
		Name:         namespace,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f
}

// netpolWorkloadFinding reports a template that fell through the
// selectors of the policies its neighbours are covered by. The subject
// is the workload: the namespace made a decision and this one object is
// outside it, so the object is what has to change.
func netpolWorkloadFinding(t podTemplate, d netpolDirection, directional, totalPolicies, covered int) emit.Finding {
	f := emit.Finding{
		Kind:     kindNetpolMissing,
		Severity: emit.SeverityWarning,
		Reason:   d.workloadReason,
		Message: fmt.Sprintf("no %s NetworkPolicy selects this pod template: the namespace has %d of them, covering %d other %s, so this workload is the hole in a namespace that is otherwise policed",
			d.name, directional, covered, plural(covered, "workload")),
		Details: []emit.Field{
			{Key: "policies", Value: itoa(directional)},
			{Key: "total_policies", Value: itoa(totalPolicies)},
			{Key: "covered_workloads", Value: itoa(covered)},
			{Key: "pod_labels", Value: cappedList(labelPairs(t.labels))},
		},
		Namespace:    t.namespace,
		KindOfObject: t.kind,
		Name:         t.name,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, t.kind)
	return f
}

// matchVerb keeps "1 NetworkPolicy ... selects" and "2 NetworkPolicies
// ... select" agreeing with the count in front of them.
func matchVerb(n int) string {
	if n == 1 {
		return "selects"
	}
	return "select"
}

// labelPairs renders a label set as sorted key=value tokens, which is
// what an operator compares against the selector that missed them.
func labelPairs(set map[string]string) []string {
	if len(set) == 0 {
		return []string{"(none)"}
	}
	out := make([]string, 0, len(set))
	for k, v := range set {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
