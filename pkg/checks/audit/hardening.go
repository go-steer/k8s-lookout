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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The workload-hardening posture kinds (issue #183). Each names one
// way a pod template dissolves the boundary between the container and
// the node it lands on, or one way the namespace around it declines to
// enforce that boundary.
//
// Container-security kinds live in container_security.go, default-SA
// automount in sa_automount.go, and image floating tags in images.go.
const kindPodSecurity = "audit.podsecurity_gaps"

const (
	reasonNoPodSecurityEnforce  = "NoPodSecurityEnforce"
	reasonPodSecurityPrivileged = "PodSecurityEnforcePrivileged"
)

// HardeningCommand builds `lookout audit hardening`: the workload half
// of the compliance audit (#183), asking of every pod template in
// scope how much of the node it can reach, and of every namespace
// whether anything would stop a new one asking for more.
//
// # Why these five claims are one command
//
// They share a subject population — the pod templates in scope — and
// two of them are only decidable once that population is known. The
// default-ServiceAccount claim in particular is not "the default SA
// automounts its token", which is true in essentially every namespace
// of every cluster and would be wallpaper; it is "the default SA
// automounts its token AND something in this namespace actually runs
// as it". Resolving the absence against real usage is what turns the
// slug into a claim, the same move the node List makes for
// audit.rigid_scheduling.
//
// # What counts as a workload here
//
// Every pod-template owner, not just the three that `audit workloads`
// judges: a privileged CronJob is as privileged as a privileged
// Deployment, and a hardening report that silently skipped Jobs and
// hand-rolled Pods would reintroduce the unverifiable coverage this
// group exists to eliminate. Templates reachable from another listed
// owner are judged once, at the owner — a Job created by a CronJob and
// a Pod created by anything both carry an ownerReference, and are
// skipped in favour of the object an operator would actually edit.
//
// Init containers are judged alongside regular ones. An init container
// with privileged: true holds root on the node for as long as it runs,
// which is all the time it needs.
//
// # Two limits worth stating
//
// **Pod Security Admission has a cluster-level default** that lives in
// the API server's AdmissionConfiguration, not in any object this (or
// any) client can read. A cluster that sets `enforce: baseline` there
// is enforced on every unlabelled namespace, and audit.podsecurity_gaps
// will still report those namespaces. Unlike the node-count claims,
// this error is one-directional the OTHER way — it over-reports — so
// on such a cluster the honest response is a reviewed cluster-wide
// exemption entry, not a silent skip.
//
// **`privileged` is not the only way to be privileged.** A container
// adding CAP_SYS_ADMIN (or ALL) is root on the node by another door,
// and a check matching only `privileged: true` reads as an all-clear
// over it. Only those two capabilities are matched: NET_ADMIN and
// SYS_PTRACE are widely and legitimately used by service meshes and
// debug sidecars, and a check that fires on every mesh-injected pod is
// one people learn to skip.
func HardeningCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:        "audit hardening",
		MCPName:     "k8s_audit_hardening",
		MCPProfiles: []string{"audit"},
		Summary:     "Workload security posture: containers running privileged or holding node-root capabilities, pods sharing the host network/PID/IPC namespaces, hostPath mounts, default-ServiceAccount tokens that something actually uses, and namespaces with no Pod Security Admission enforcement. Judges every pod-template owner in scope — Deployments, StatefulSets, DaemonSets, CronJobs, unowned Jobs and unowned Pods — plus the namespaces around them. Scope with --namespace or -A; scanned counts pod templates examined, the namespaces note counts namespaces.",
		Flags: append([]emit.FlagSpec{
			skipSystemNamespacesFlag(),
			perWorkloadFlag(),
		}, PolicyFlags()...),
		Kinds: []checks.KindField{
			checks.Kind(kindPrivileged, "a container runs privileged or holds a node-root capability (ALL, SYS_ADMIN): a container escape is a node compromise", emit.SeverityWarning),
			checks.Kind(kindHostNamespace, "the pod shares the node's network, PID, or IPC namespace", emit.SeverityWarning),
			checks.Kind(kindHostPath, "the pod mounts a host path; warning when it is writable, info when read-only", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindDefaultSAMount, "the pod runs as the namespace's default ServiceAccount with its token automounted, and something in the pod can use it", emit.SeverityWarning),
			checks.Kind(kindPodSecurity, "the namespace enforces no Pod Security Admission level, so none of the above is prevented", emit.SeverityWarning),
			checks.Kind(kindImageFloatingTag, "a container image reference names no particular bytes — a tag that by convention moves (latest, main, master, dev, nightly, stable, edge) or no tag at all, with no digest: the next pull may run different code with no deploy and no record", emit.SeverityInfo),
			checks.UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "containers", Doc: "containers implicated by the finding — those running privileged, or holding a node-root capability"},
			{Name: "container_names", Doc: "their names, capped at 8 with a +N more tail"},
			{Name: "total_containers", Doc: "containers in the pod template, init containers included, so `containers` reads as a fraction"},
			{Name: "capabilities", Doc: "the node-root capabilities added by those containers (ALL, SYS_ADMIN), sorted and deduplicated"},
			{Name: "host_paths", Doc: "hostPath volumes the template mounts; a declared but unmounted hostPath volume grants no access and is not counted"},
			{Name: "host_path_names", Doc: "the paths on the node, sorted and capped at 8"},
			{Name: "service_account", Doc: "the ServiceAccount the finding is about — always `default`, the one every pod gets when its template names none"},
			{Name: "mounting_workloads", Doc: "workloads in the namespace running as the default ServiceAccount without disabling automount at the pod level; the finding does not fire at 0"},
			{Name: "mounting_workload_names", Doc: "their Kind/name, sorted and capped at 8"},
			{Name: "pss_enforce", Doc: "the namespace's pod-security.kubernetes.io/enforce label, omitted when unset"},
			{Name: "pss_warn", Doc: "its /warn label, omitted when unset — set without /enforce means the namespace is in dry-run"},
			{Name: "pss_audit", Doc: "its /audit label, omitted when unset — same dry-run meaning"},
			{Name: "workloads", Doc: "pod templates this pass judged in the namespace, so an unenforced namespace with nothing in it reads differently from a busy one"},
			{Name: "namespaces", Doc: "summary note: namespaces examined — the denominator for every namespace-subject claim, which `scanned` (pod templates) does not cover"},
			skippedSystemNamespacesField(),
		}, checks.UnreadFields()...),
		Examples: []string{
			"lookout audit hardening -A",
			"lookout audit hardening --namespace=prod",
			"lookout audit hardening -A --exemptions=exemptions.yaml --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runHardening(ctx, deps, inv)
		},
	}
}

func runHardening(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit hardening reports namespace-subject claims (Pod Security Admission, the default ServiceAccount) alongside workload ones, so it is scoped by namespace: use --namespace=%s or -A", inv.Scope.Workload.Namespace)
	}
	if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns> or -A")
	}
	pol, err := bindPolicy(inv)
	if err != nil {
		return 0, err
	}
	skipSys := inv.Flags.Bool("skip-system-namespaces")
	perWorkload := inv.Flags.Bool("per-workload")

	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ctx, snap := ensureSnapshot(ctx, client, listNS)
	ix, err := listHardeningIndex(ctx, client, listNS)
	if err != nil {
		return 0, err
	}
	ix.perWorkload = perWorkload
	if customTags := pol.StringSliceParam(kindImageFloatingTag, "floating_tags", nil); len(customTags) > 0 {
		ix.floatingTags = make(map[string]bool, len(customTags))
		for _, tag := range customTags {
			ix.floatingTags[tag] = true
		}
	}
	if skipSys {
		filteredT := ix.templates[:0]
		for _, t := range ix.templates {
			if pol.IsSystemNamespace(t.namespace) {
				snap.recordSkippedNS(t.namespace)
				delete(ix.templatesByNS, t.namespace)
				delete(ix.defaultSAUsers, t.namespace)
				delete(ix.defaultSAs, t.namespace)
				continue
			}
			filteredT = append(filteredT, t)
		}
		ix.templates = filteredT

		filteredNS := ix.namespaces[:0]
		for _, ns := range ix.namespaces {
			if pol.IsSystemNamespace(ns.Name) {
				snap.recordSkippedNS(ns.Name)
				delete(ix.defaultSAs, ns.Name)
				continue
			}
			filteredNS = append(filteredNS, ns)
		}
		ix.namespaces = filteredNS

		for nsName := range ix.defaultSAs {
			if pol.IsSystemNamespace(nsName) {
				snap.recordSkippedNS(nsName)
				delete(ix.defaultSAs, nsName)
			}
		}
	}

	// A custom role refusing one of these reads (#584) costs the
	// claims that need it, each named here; the rest still answer.
	var refusals []emit.Finding
	for _, r := range ix.refused {
		refusals = append(refusals, checks.RefusedFinding(r.Refusal, r.kind+" pod templates not audited"))
	}
	if ix.saRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*ix.saRefused,
			kindDefaultSAMount+" not judged: it reads each namespace's default ServiceAccount"))
	}
	if ix.nsRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*ix.nsRefused,
			kindPodSecurity+" and "+kindDefaultSAMount+" not judged: they are claims about the Namespace objects"))
	}
	for _, f := range refusals {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}

	var findings []emit.Finding
	for _, t := range ix.templates {
		findings = append(findings, ix.judgeTemplate(t)...)
	}
	for _, ns := range ix.namespaces {
		findings = append(findings, ix.judgeNamespace(ns)...)
	}

	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if ix.nsRefused == nil {
		if err := inv.Out.Note("namespaces", itoa(len(ix.namespaces))); err != nil {
			return 0, err
		}
	}
	if skipSys {
		if err := inv.Out.Note("skipped_system_namespaces", itoa(snap.skippedNSCount())); err != nil {
			return 0, err
		}
	}
	return len(ix.templates), nil
}

// hardeningIndex holds one pass's worth of listed objects: the pod
// templates to judge, the namespaces around them, and each namespace's
// default ServiceAccount.
type hardeningIndex struct {
	templates  []podTemplate
	namespaces []*corev1.Namespace
	// defaultSAs is keyed by namespace. A missing entry means the
	// namespace's default ServiceAccount was not listed — the token
	// claim then makes no assertion rather than guessing at the
	// object's automount field.
	defaultSAs map[string]*corev1.ServiceAccount
	// templatesByNS counts judged templates per namespace, so a
	// namespace-subject finding can say how much rides on it.
	templatesByNS map[string]int
	// defaultSAUsers lists, per namespace, the workloads that would
	// actually receive the default ServiceAccount's token.
	defaultSAUsers map[string][]string
	// refused, saRefused and nsRefused record the reads RBAC refused
	// (#584): pod-template kinds, ServiceAccounts, Namespaces.
	refused      []kindRefusal
	saRefused    *checks.Refusal
	nsRefused    *checks.Refusal
	perWorkload  bool
	floatingTags map[string]bool
}

func listHardeningIndex(ctx context.Context, client kubernetes.Interface, ns string) (*hardeningIndex, error) {
	ix := &hardeningIndex{
		defaultSAs:     map[string]*corev1.ServiceAccount{},
		templatesByNS:  map[string]int{},
		defaultSAUsers: map[string][]string{},
	}
	templates, refused, err := listPodTemplates(ctx, client, ns, true)
	if err != nil {
		return nil, err
	}
	ix.templates, ix.refused = templates, refused

	snap := snapshotFor(ctx, client, ns)
	if sas, r, err := snap.ServiceAccounts(ctx); err != nil {
		return nil, err
	} else if r != nil {
		// No default ServiceAccount listed means the token claim makes
		// no assertion (defaultSAs above), so a refusal only skips it.
		ix.saRefused = r
	} else {
		for i := range sas {
			sa := &sas[i]
			if sa.Name == defaultServiceAccount {
				ix.defaultSAs[sa.Namespace] = sa
			}
		}
	}
	if ix.namespaces, err = listNamespacesInScope(ctx, client, ns); err != nil {
		r, ok := checks.ForbiddenRefusal(err)
		if !ok {
			return nil, err
		}
		ix.nsRefused = &r
	}

	for _, t := range ix.templates {
		ix.templatesByNS[t.namespace]++
		if mountsDefaultToken(t.spec) {
			ix.defaultSAUsers[t.namespace] = append(ix.defaultSAUsers[t.namespace], t.kind+"/"+t.name)
		}
	}
	return ix, nil
}

// judgeTemplate applies every pod-template-subject claim to one
// template and stamps the shared subject and posture fingerprint.
func (ix *hardeningIndex) judgeTemplate(t podTemplate) []emit.Finding {
	out := append(privilegeFindings(t.spec), hostNamespaceFindings(t.spec)...)
	out = append(out, hostPathFindings(t.spec)...)
	out = append(out, imageTagFindingsWithTags(t.spec, ix.floatingTags)...)
	for i := range out {
		out[i].Namespace = t.namespace
		out[i].KindOfObject = t.kind
		out[i].Name = t.name
		out[i].Fingerprint = engine.PostureFingerprint(out[i].Kind, out[i].Reason, t.kind)
	}
	return out
}

// judgeNamespace applies the two namespace-population claims. Their
// subjects differ — one is the ServiceAccount, one is the Namespace —
// because those are the two objects an operator edits to fix them.
func (ix *hardeningIndex) judgeNamespace(ns *corev1.Namespace) []emit.Finding {
	var out []emit.Finding
	out = append(out, ix.defaultSAFindings(ns.Name)...)
	if f, ok := ix.podSecurityFinding(ns); ok {
		out = append(out, f)
	}
	return out
}

// The Pod Security Admission namespace labels. Only enforce carries
// admission consequences; warn and audit are reported as evidence that
// a namespace was set up for PSA and then left in dry-run.
const (
	psaEnforceLabel = "pod-security.kubernetes.io/enforce"
	psaWarnLabel    = "pod-security.kubernetes.io/warn"
	psaAuditLabel   = "pod-security.kubernetes.io/audit"
	// psaPrivileged is the level that enforces nothing. Labelling a
	// namespace with it is a deliberate act, which is why it reads
	// differently from an unlabelled namespace even though admission
	// behaves identically.
	psaPrivileged = "privileged"
)

func (ix *hardeningIndex) podSecurityFinding(ns *corev1.Namespace) (emit.Finding, bool) {
	enforce := ns.Labels[psaEnforceLabel]
	if enforce != "" && enforce != psaPrivileged {
		return emit.Finding{}, false
	}
	details := []emit.Field{}
	reason, message := reasonNoPodSecurityEnforce,
		"no pod-security.kubernetes.io/enforce label: nothing at admission stops a pod in this namespace asking for privileged, the host namespaces, or a hostPath — the workload claims above are the only thing that will notice"
	if enforce == psaPrivileged {
		reason, message = reasonPodSecurityPrivileged,
			"pod-security.kubernetes.io/enforce=privileged: Pod Security Admission is configured here and set to permit everything, so the namespace is opted out deliberately rather than by omission"
		details = append(details, emit.Field{Key: "pss_enforce", Value: enforce})
	}
	for _, l := range []struct{ key, label string }{
		{"pss_warn", psaWarnLabel},
		{"pss_audit", psaAuditLabel},
	} {
		if v := ns.Labels[l.label]; v != "" {
			details = append(details, emit.Field{Key: l.key, Value: v})
		}
	}
	details = append(details, emit.Field{Key: "workloads", Value: itoa(ix.templatesByNS[ns.Name])})

	f := emit.Finding{
		Kind:         kindPodSecurity,
		Severity:     emit.SeverityWarning,
		Reason:       reason,
		Message:      message,
		Details:      details,
		Namespace:    ns.Name,
		KindOfObject: "Namespace",
		Name:         ns.Name,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f, true
}
