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
const (
	kindPrivileged     = "audit.privileged_container"
	kindHostNamespace  = "audit.host_namespace"
	kindHostPath       = "audit.hostpath_mount"
	kindDefaultSAMount = "audit.default_sa_automount"
	kindPodSecurity    = "audit.podsecurity_gaps"
)

// The reasons. Two kinds carry more than one because the condition has
// distinct causes with distinct remedies, exactly as
// audit.rigid_scheduling does: the cause is what an operator acts on,
// so it is what the fingerprint separates.
const (
	reasonPrivileged            = "PrivilegedContainer"
	reasonDangerousCapability   = "DangerousCapability"
	reasonHostNetwork           = "HostNetwork"
	reasonHostPID               = "HostPID"
	reasonHostIPC               = "HostIPC"
	reasonWritableHostPath      = "WritableHostPath"
	reasonReadOnlyHostPath      = "ReadOnlyHostPath"
	reasonDefaultSAAutomount    = "DefaultServiceAccountAutomount"
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
		Name:    "audit hardening",
		MCPName: "k8s_audit_hardening",
		Summary: "Workload security posture: containers running privileged or holding node-root capabilities, pods sharing the host network/PID/IPC namespaces, hostPath mounts, default-ServiceAccount tokens that something actually uses, and namespaces with no Pod Security Admission enforcement. Judges every pod-template owner in scope — Deployments, StatefulSets, DaemonSets, CronJobs, unowned Jobs and unowned Pods — plus the namespaces around them. Scope with --namespace or -A; scanned counts pod templates examined, the namespaces note counts namespaces.",
		Kinds: []checks.KindField{
			checks.Kind(kindPrivileged, "a container runs privileged or holds a node-root capability (ALL, SYS_ADMIN): a container escape is a node compromise", emit.SeverityWarning),
			checks.Kind(kindHostNamespace, "the pod shares the node's network, PID, or IPC namespace", emit.SeverityWarning),
			checks.Kind(kindHostPath, "the pod mounts a host path; warning when it is writable, info when read-only", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindDefaultSAMount, "the pod runs as the namespace's default ServiceAccount with its token automounted, and something in the pod can use it", emit.SeverityWarning),
			checks.Kind(kindPodSecurity, "the namespace enforces no Pod Security Admission level, so none of the above is prevented", emit.SeverityWarning),
		},
		Output: []checks.OutputField{
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
		},
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
	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ix, err := listHardeningIndex(ctx, client, listNS)
	if err != nil {
		return 0, err
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
	if err := inv.Out.Note("namespaces", itoa(len(ix.namespaces))); err != nil {
		return 0, err
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
}

func listHardeningIndex(ctx context.Context, client kubernetes.Interface, ns string) (*hardeningIndex, error) {
	ix := &hardeningIndex{
		defaultSAs:     map[string]*corev1.ServiceAccount{},
		templatesByNS:  map[string]int{},
		defaultSAUsers: map[string][]string{},
	}
	templates, err := listPodTemplates(ctx, client, ns)
	if err != nil {
		return nil, err
	}
	ix.templates = templates

	if err := listPages("serviceaccounts", func(o metav1.ListOptions) ([]corev1.ServiceAccount, string, error) {
		l, err := client.CoreV1().ServiceAccounts(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(sa *corev1.ServiceAccount) {
		if sa.Name == defaultServiceAccount {
			ix.defaultSAs[sa.Namespace] = sa
		}
	}); err != nil {
		return nil, err
	}
	if ix.namespaces, err = listNamespacesInScope(ctx, client, ns); err != nil {
		return nil, err
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
	for i := range out {
		out[i].Namespace = t.namespace
		out[i].KindOfObject = t.kind
		out[i].Name = t.name
		out[i].Fingerprint = engine.PostureFingerprint(out[i].Kind, out[i].Reason, t.kind)
	}
	return out
}

// allContainers returns init containers followed by regular ones. Both
// run with the securityContext they declare, and an init container
// holds whatever it asks for until it exits.
func allContainers(spec corev1.PodSpec) []corev1.Container {
	out := make([]corev1.Container, 0, len(spec.InitContainers)+len(spec.Containers))
	out = append(out, spec.InitContainers...)
	return append(out, spec.Containers...)
}

// nodeRootCapabilities are the two additions equivalent to
// privileged: true for this check's purposes. SYS_ADMIN is the
// capability the kernel uses as a catch-all for "can reconfigure the
// machine"; ALL is every capability including it.
var nodeRootCapabilities = map[string]bool{"ALL": true, "SYS_ADMIN": true}

// privilegeFindings judges the two ways a container asks for the node.
// They are separate claims because the remedies differ: one is a
// boolean to unset, the other a capability to drop.
func privilegeFindings(spec corev1.PodSpec) []emit.Finding {
	cs := allContainers(spec)
	if len(cs) == 0 {
		return nil
	}
	var privileged, capable []string
	caps := map[string]bool{}
	for _, c := range cs {
		sc := c.SecurityContext
		if sc == nil {
			continue
		}
		if sc.Privileged != nil && *sc.Privileged {
			privileged = append(privileged, c.Name)
		}
		if sc.Capabilities == nil {
			continue
		}
		var found []string
		for _, add := range sc.Capabilities.Add {
			if name := canonicalCapability(string(add)); nodeRootCapabilities[name] {
				found = append(found, name)
			}
		}
		if len(found) > 0 {
			capable = append(capable, c.Name)
			for _, name := range found {
				caps[name] = true
			}
		}
	}

	var out []emit.Finding
	if len(privileged) > 0 {
		out = append(out, emit.Finding{
			Kind:     kindPrivileged,
			Severity: emit.SeverityWarning,
			Reason:   reasonPrivileged,
			Message: fmt.Sprintf("securityContext.privileged on %d of %d %s: every device on the node is visible and writable, and a process that escapes the container is root on the host",
				len(privileged), len(cs), plural(len(cs), "container")),
			Details: []emit.Field{
				{Key: "containers", Value: itoa(len(privileged))},
				{Key: "container_names", Value: cappedList(privileged)},
				{Key: "total_containers", Value: itoa(len(cs))},
			},
		})
	}
	if len(capable) > 0 {
		out = append(out, emit.Finding{
			Kind:     kindPrivileged,
			Severity: emit.SeverityWarning,
			Reason:   reasonDangerousCapability,
			Message: fmt.Sprintf("capabilities.add %s on %d of %d %s: not privileged: true, and equivalent to it — a check reading only that flag reports this template as clean",
				sortedKeys(caps), len(capable), len(cs), plural(len(cs), "container")),
			Details: []emit.Field{
				{Key: "containers", Value: itoa(len(capable))},
				{Key: "container_names", Value: cappedList(capable)},
				{Key: "total_containers", Value: itoa(len(cs))},
				{Key: "capabilities", Value: sortedKeys(caps)},
			},
		})
	}
	return out
}

// canonicalCapability normalizes a capability name to the bare
// uppercase form the API uses ("SYS_ADMIN"), tolerating the CAP_
// prefix people write out of kernel-header habit.
func canonicalCapability(name string) string {
	return strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(name)), "CAP_")
}

func sortedKeys(set map[string]bool) string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// hostNamespaceFindings judges the three host namespaces a pod can
// join. One finding each, not one finding listing all three: they are
// asked for separately, granted separately and removed separately, and
// a rollup counting "how many workloads see every process on the node"
// should not have to parse a list to answer.
func hostNamespaceFindings(spec corev1.PodSpec) []emit.Finding {
	var out []emit.Finding
	if spec.HostNetwork {
		out = append(out, emit.Finding{
			Kind:     kindHostNamespace,
			Severity: emit.SeverityWarning,
			Reason:   reasonHostNetwork,
			Message:  "hostNetwork: the pod binds ports directly on the node, reaches anything the node can reach including the metadata server and the kubelet, and NetworkPolicy — which selects pods, not host interfaces — cannot constrain it",
		})
	}
	if spec.HostPID {
		out = append(out, emit.Finding{
			Kind:     kindHostNamespace,
			Severity: emit.SeverityWarning,
			Reason:   reasonHostPID,
			Message:  "hostPID: every process on the node is visible in /proc, including other containers' command lines and anything they were passed as an argument",
		})
	}
	if spec.HostIPC {
		out = append(out, emit.Finding{
			Kind:     kindHostNamespace,
			Severity: emit.SeverityWarning,
			Reason:   reasonHostIPC,
			Message:  "hostIPC: the pod shares the node's SysV IPC and POSIX message queues with every other process on it",
		})
	}
	return out
}

// hostPathFindings judges the template's hostPath volumes, split by
// whether anything can write through them.
//
// Only MOUNTED volumes are judged: a hostPath volume declared and
// never mounted grants no access to anything, and reporting it would
// be true and useless. A volume counts as writable when any one of its
// mounts is, since one writable mount is all it takes.
func hostPathFindings(spec corev1.PodSpec) []emit.Finding {
	hostPaths := map[string]string{} // volume name -> path on the node
	for _, v := range spec.Volumes {
		if v.HostPath != nil {
			hostPaths[v.Name] = v.HostPath.Path
		}
	}
	if len(hostPaths) == 0 {
		return nil
	}
	writable, readOnly := map[string]bool{}, map[string]bool{}
	for _, c := range allContainers(spec) {
		for _, m := range c.VolumeMounts {
			path, ok := hostPaths[m.Name]
			if !ok {
				continue
			}
			if m.ReadOnly {
				readOnly[path] = true
			} else {
				writable[path] = true
			}
		}
	}
	// A path mounted read-only somewhere and writable elsewhere is
	// writable; the weaker mount does not constrain the stronger one.
	for path := range writable {
		delete(readOnly, path)
	}

	var out []emit.Finding
	if len(writable) > 0 {
		out = append(out, hostPathFinding(emit.SeverityWarning, reasonWritableHostPath,
			"mounted read-write: anything the container writes lands on the node's filesystem, and a path like /var/run, /etc or / is a container escape rather than a mount",
			writable))
	}
	if len(readOnly) > 0 {
		// Info, not warning: a read-only hostPath still reads node state
		// the container was never given, but it cannot modify the node,
		// and the read-only forms in practice (a CA bundle, /etc/localtime)
		// are ordinary. The absence is worth reporting and is not, on its
		// own, a defect.
		out = append(out, hostPathFinding(emit.SeverityInfo, reasonReadOnlyHostPath,
			"mounted read-only: the node's filesystem is readable from inside the container, though not writable",
			readOnly))
	}
	return out
}

func hostPathFinding(severity, reason, consequence string, paths map[string]bool) emit.Finding {
	names := make([]string, 0, len(paths))
	for p := range paths {
		names = append(names, p)
	}
	sort.Strings(names)
	return emit.Finding{
		Kind:     kindHostPath,
		Severity: severity,
		Reason:   reason,
		Message:  fmt.Sprintf("%d hostPath %s %s", len(names), plural(len(names), "volume"), consequence),
		Details: []emit.Field{
			{Key: "host_paths", Value: itoa(len(names))},
			{Key: "host_path_names", Value: cappedList(names)},
		},
	}
}

// defaultServiceAccount is the ServiceAccount every namespace has and
// every pod naming none is given.
const defaultServiceAccount = "default"

// mountsDefaultToken reports whether this template would actually
// receive the default ServiceAccount's token: it runs as the default
// SA, and it does not turn the mount off at the pod level. The
// SA-level field is checked separately — pod-level wins where both are
// set, which is why the two halves cannot be collapsed.
func mountsDefaultToken(spec corev1.PodSpec) bool {
	if n := spec.ServiceAccountName; n != "" && n != defaultServiceAccount {
		return false
	}
	return spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken
}

// judgeNamespace applies the two namespace-population claims. Their
// subjects differ — one is the ServiceAccount, one is the Namespace —
// because those are the two objects an operator edits to fix them.
func (ix *hardeningIndex) judgeNamespace(ns *corev1.Namespace) []emit.Finding {
	var out []emit.Finding
	if f, ok := ix.defaultSAFinding(ns.Name); ok {
		out = append(out, f)
	}
	if f, ok := ix.podSecurityFinding(ns); ok {
		out = append(out, f)
	}
	return out
}

// defaultSAFinding judges the default ServiceAccount's token mount.
//
// It fires only when the token is BOTH offered and taken. Every
// namespace in every cluster has a default ServiceAccount that
// automounts, so the offer alone is not a finding — it is the resting
// state of Kubernetes, and a check that reported it would fire once
// per namespace forever.
func (ix *hardeningIndex) defaultSAFinding(namespace string) (emit.Finding, bool) {
	sa, ok := ix.defaultSAs[namespace]
	if !ok {
		return emit.Finding{}, false
	}
	if sa.AutomountServiceAccountToken != nil && !*sa.AutomountServiceAccountToken {
		return emit.Finding{}, false
	}
	users := ix.defaultSAUsers[namespace]
	if len(users) == 0 {
		return emit.Finding{}, false
	}
	sort.Strings(users)
	f := emit.Finding{
		Kind:     kindDefaultSAMount,
		Severity: emit.SeverityWarning,
		Reason:   reasonDefaultSAAutomount,
		Message: fmt.Sprintf("%d %s run as the default ServiceAccount and receive its token: an API credential is sitting in a container that never asked for one, and it is the same credential for everything in the namespace",
			len(users), plural(len(users), "workload")),
		Details: []emit.Field{
			{Key: "service_account", Value: defaultServiceAccount},
			{Key: "mounting_workloads", Value: itoa(len(users))},
			{Key: "mounting_workload_names", Value: cappedList(users)},
		},
		Namespace:    namespace,
		KindOfObject: "ServiceAccount",
		Name:         defaultServiceAccount,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f, true
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
