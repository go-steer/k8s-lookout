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

// Package health implements `lookout health` (DESIGN.md §5): the
// "are there issues with this cluster?" scorecard. One composed pass
// over ten check categories — control-plane latency, node
// conditions, crash loops, aged Pending, rollout stalls, PVC/storage
// health, system add-ons, ResourceQuotas, cert expiry, webhook
// health — each reporting healthy, degraded, or unavailable. The
// scorecard always answers: every category emits exactly one
// `health.category` finding, healthy included, followed by the
// detailed findings of the degraded categories.
//
// Composition, not new checks: the delta-backed categories delegate
// to pkg/checks/delta's scan; the webhooks category delegates to
// `state webhooks`' exported core (state.CheckWebhooks); storage and
// certs are the two lightweight checks that have no standalone
// command yet. Control-plane latency delegates to the shipped
// `perf probe` packs (perf.ControlPlaneProbe, the apiserver pack)
// when the provider serves Metrics, and reports the honest
// unavailable reason through the pkg/cloud boundary only when the
// capability is genuinely absent (no provider / no metrics
// capability / metric positively missing from the workspace — never
// silence, §2). The §9.4 merge with open triage-status records rides
// --store (M4 exit shape).
package health

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/checks/perf"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func init() {
	checks.Register(New(Deps{}))
}

// Deps injects the cluster and cloud seams. The zero value is the
// production wiring.
type Deps struct {
	// Client yields the Kubernetes client. Nil means kube.BuildClient
	// with default config resolution.
	Client kube.ClientSource
	// Provider yields the cloud provider. Nil means cloud.New with
	// default detection. With a Metrics-capable provider the
	// control-plane category runs the perf apiserver pack; on
	// NoProvider (vanilla builds) it reports unavailable, never
	// silence (§2).
	Provider func(ctx context.Context) (cloud.Provider, error)
	// Now is the scan clock. Nil means time.Now.
	Now func() time.Time
}

func (d Deps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.BuildClient(kube.OptionsFrom(ctx))
}

func (d Deps) provider(ctx context.Context) (cloud.Provider, error) {
	if d.Provider != nil {
		return d.Provider(ctx)
	}
	return cloud.New(ctx, cloud.Config{})
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Category statuses.
const (
	statusHealthy     = "healthy"
	statusDegraded    = "degraded"
	statusUnavailable = "unavailable"
)

// categoryOrder is the fixed scorecard order (§5 health row).
var categoryOrder = []string{
	"control-plane",
	"nodes",
	"crashloops",
	"pending",
	"rollouts",
	"storage",
	"addons",
	"quota",
	"certs",
	"webhooks",
}

// New builds the `health` command around deps.
func New(deps Deps) checks.Command {
	return checks.Command{
		Name:    "health",
		MCPName: "k8s_cluster_health",
		Summary: "\"Any issues with this cluster?\" in one call: a ten-category scorecard (control-plane, nodes, crash loops, pending, rollouts, storage, add-ons, quotas, certs, webhooks) — every category answers healthy|degraded|unavailable, degraded ones with details. With --store, findings merge the sentinel's open triage-status records (§9.4): a scan mid-incident reports the diagnosis and the agent's severity judgment, not a fresh unknown.",
		Flags: []emit.FlagSpec{
			{Name: "top", Type: emit.FlagInt, Default: "3",
				Help: "how many findings to name inline on a degraded category's scorecard line"},
			{Name: "cert-warn", Type: emit.FlagDuration, Default: "720h",
				Help: "report TLS certificates expiring within this window (certs category)"},
			{Name: "store", Type: emit.FlagString, Default: "",
				Help: "path to a sentinel's SQLite store (its --store file); merges open §9.4 triage-status records so findings carry triage_* fields and severity reflects the agent's override"},
		},
		Kinds: append([]checks.KindField{
			checks.Kind("health.category", "one scorecard line: how this category answered — healthy, degraded, or unavailable. The scorecard always answers, so healthy is explicit rather than silent; the line carries the worst severity found inside the category", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind("pvc.pending", "a PersistentVolumeClaim is not bound; pods mounting it cannot start", emit.SeverityWarning),
			checks.Kind("pvc.lost", "a PersistentVolumeClaim's bound volume is lost", emit.SeverityCritical),
			checks.Kind("cert.expired", "a TLS secret's certificate has expired", emit.SeverityCritical),
			checks.Kind("cert.expiring", "a TLS secret's certificate expires within --cert-warn", emit.SeverityWarning),
			checks.Kind("cert.invalid", "a TLS secret's tls.crt does not contain a parseable X.509 certificate", emit.SeverityWarning),
		}, delegatedKinds()...),
		Output: append([]checks.OutputField{
			{Name: "category", Doc: "scorecard category the finding belongs to (on health.category: which category this line scores)"},
			{Name: "status", Doc: "category status: healthy|degraded|unavailable (the scorecard always answers — healthy is explicit)"},
			{Name: "total", Doc: "findings in a degraded category"},
			{Name: "top", Doc: "worst findings of a degraded category inline, as kind[ namespace/name]; capped by --top"},
			{Name: "subject", Doc: "TLS certificate subject (CN when set); never key material"},
			{Name: "not_after", Doc: "TLS certificate NotAfter, RFC 3339"},
			{Name: "days_left", Doc: "whole days until NotAfter (negative = expired)"},
			{Name: "phase", Doc: "PersistentVolumeClaim phase on storage findings (Pending or Lost)"},
			{Name: "webhook", Doc: "admission webhook as <configuration>/<webhook name>"},
			{Name: "service", Doc: "service backend a webhook points at, as <namespace>/<name>"},
			{Name: "backend", Doc: "why a webhook backend is dead: service missing, no ready endpoints, or port <p> not on service"},
			{Name: "gates", Doc: "namespaces a webhook gates, from namespaceSelector: all namespaces, or <matched>/<total> namespaces with up to 5 names"},
			{Name: "rules", Doc: "compact operations/resources summary of a webhook's rules, e.g. \"CREATE,UPDATE pods,deployments.apps\""},
			{Name: "object_selector", Doc: "a webhook's objectSelector, when one is set"},
			{Name: "timeout", Doc: "webhook timeoutSeconds as <n>s (nil defaults to the API's 10s)"},
			{Name: memory.DetailTriageStatus, Doc: "triage state from the matched §9.4 record (investigating|triaged|actioned|escalated) — present only with --store on merged findings"},
			{Name: memory.DetailTriageRootCause, Doc: "the incident agent's root-cause hypothesis, from the matched triage-status record"},
			{Name: memory.DetailTriageAction, Doc: "the incident agent's paper trail (PRs opened, escalations), from the matched triage-status record"},
			{Name: memory.DetailTriageSession, Doc: "incident session that wrote the matched triage-status record"},
			{Name: memory.DetailTriageAge, Doc: "how long ago the matched triage-status record was last updated"},
		}, delegatedOutput()...),
		Examples: []string{
			"lookout health",
			"lookout health --format=json --top=5",
			"lookout health --namespace=prod",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return run(ctx, deps, inv)
		},
	}
}

// perfFields is the subset of `perf probe`'s glossary the
// control-plane category's delegated apiserver-pack findings can
// carry — the other packs' fields (priority_level, code, trend) and
// the standalone command's unavailable-marker keys never appear on a
// health scorecard and stay out of its glossary.
var perfFields = map[string]bool{
	"pack": true, "metric": true, "verb": true, "resource": true,
	"observed": true, "latest": true, "threshold": true, "window": true,
}

// delegatedOutput pulls the delegated commands' glossaries — `triage
// delta` for the delta-backed categories and `perf probe` (apiserver
// pack fields only) for the control-plane category — minus the keys
// health declares itself and any overlap between the two.
func delegatedOutput() []checks.OutputField {
	seen := map[string]bool{
		"category": true, "status": true, "total": true, "top": true,
		"subject": true, "not_after": true, "days_left": true,
		"phase": true, "webhook": true, "service": true,
		"backend": true, "gates": true, "rules": true,
		"object_selector": true, "timeout": true,
	}
	var out []checks.OutputField
	for _, name := range []string{"triage delta", "perf probe"} {
		c, ok := checks.Lookup(name)
		if !ok {
			continue // isolated test registry; contract test asserts presence
		}
		for _, f := range c.Output {
			if seen[f.Name] || (name == "perf probe" && !perfFields[f.Name]) {
				continue
			}
			seen[f.Name] = true
			out = append(out, f)
		}
	}
	return out
}

// perfKinds is the subset of `perf probe`'s ledger the control-plane
// category can reach: it runs the apiserver pack only, so the apf,
// etcd and startup claims never appear on a scorecard and stay out of
// this command's vocabulary.
var perfKinds = map[string]bool{
	"perf.apiserver_p99": true, "perf.pack_unavailable": true,
}

// delegatedKinds pulls the delegated commands' kind ledgers on the
// same principle as delegatedOutput: `triage delta` for the
// delta-backed categories, `state webhooks` for the webhooks one, and
// `perf probe` (apiserver pack only) for control-plane. Health emits
// those findings verbatim, so it must declare exactly what they do.
func delegatedKinds() []checks.KindField {
	seen := map[string]bool{
		"health.category": true,
		"pvc.pending":     true, "pvc.lost": true,
		"cert.expired": true, "cert.expiring": true, "cert.invalid": true,
	}
	var out []checks.KindField
	for _, name := range []string{"triage delta", "state webhooks", "perf probe"} {
		c, ok := checks.Lookup(name)
		if !ok {
			continue // isolated test registry; contract test asserts presence
		}
		for _, k := range c.Kinds {
			if seen[k.Name] || (name == "perf probe" && !perfKinds[k.Name]) {
				continue
			}
			seen[k.Name] = true
			out = append(out, k)
		}
	}
	return out
}

// scorecard accumulates per-category findings and unavailability.
type scorecard struct {
	findings    map[string][]emit.Finding
	unavailable map[string]string // category → reason
}

func (s *scorecard) add(category string, f emit.Finding) {
	s.findings[category] = append(s.findings[category], f)
}

// run is the CheckFunc.
func run(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	top := inv.Flags.Int("top")
	certWarn := inv.Flags.Duration("cert-warn")
	if top < 1 {
		return 0, emit.UsageErrorf("--top must be at least 1, got %d", top)
	}
	if certWarn <= 0 {
		return 0, emit.UsageErrorf("--cert-warn must be positive, got %s", certWarn)
	}
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("--workload does not apply: health scores the cluster (use bundle for one workload)")
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	now := deps.now()
	ns := inv.Scope.Namespace // "" (default and -A) = whole cluster

	card := &scorecard{findings: map[string][]emit.Finding{}, unavailable: map[string]string{}}
	scanned := 0

	// Delta-backed categories: nodes, crashloops, pending, rollouts,
	// addons, quota — one delta pass, findings bucketed by kind. The
	// pdb class stays with `triage delta`: PDB gridlock is disruption
	// *readiness*, not one of the §5 health categories.
	dScanned, dFindings, err := delta.ScanCluster(ctx, client, ns, now, delta.Config{},
		"pods", "nodes", "system", "quota")
	if err != nil {
		return 0, err
	}
	scanned += dScanned
	for _, f := range dFindings {
		card.add(deltaCategory(f.Kind), f)
	}
	if ns != "" {
		card.unavailable["nodes"] = "Nodes are cluster-scoped; run without --namespace"
		if ns != metav1.NamespaceSystem {
			card.unavailable["addons"] = "system add-ons live in kube-system; run without --namespace"
		}
	}

	// storage: PVCs not Bound.
	n, err := checkStorage(ctx, client, ns, now, card)
	if err != nil {
		return 0, err
	}
	scanned += n

	// certs: every kubernetes.io/tls Secret in scope.
	n, err = checkCerts(ctx, client, ns, now, certWarn, card)
	if err != nil {
		return 0, err
	}
	scanned += n

	// webhooks: delegated to `state webhooks`' exported core — the
	// full audit (dead backends × failurePolicy, blast-radius scope,
	// timeout risk, CA-bundle expiry), not a subset. Scanned counts
	// the configurations, matching the lister's contract.
	if ns == "" {
		in, n, err := state.LoadWebhookInputs(ctx, client)
		if err != nil {
			return 0, err
		}
		scanned += n
		for _, f := range state.CheckWebhooks(in, certWarn, now) {
			card.add("webhooks", f)
		}
	} else {
		card.unavailable["webhooks"] = "webhook configurations are cluster-scoped; run without --namespace"
	}

	// control-plane: the §5 "control-plane latency (perf probe
	// packs)" category, delegated to the shipped perf apiserver pack
	// (p99 by verb/resource — the cheapest meaningful control-plane
	// read) when the provider serves Metrics. Breaches degrade the
	// category in `perf probe`'s exact finding shape; clean queries
	// score healthy. The honest unavailable reason survives ONLY when
	// the capability is genuinely absent (§2): no provider / no
	// metrics capability, or the metric positively missing from the
	// workspace (the pack_unavailable case — on GKE, control-plane
	// metrics not enabled). A real backend failure fails the scan
	// like any other category's read error.
	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	if backend, ok := provider.Metrics(); ok {
		findings, n, absent, err := perf.ControlPlaneProbe(ctx, backend, now)
		if err != nil {
			return 0, err
		}
		if absent != "" {
			card.unavailable["control-plane"] = fmt.Sprintf(
				"metric %s is not in the project's metrics workspace — enable GKE control-plane metrics for the API server (perf probe --pack=apiserver)", absent)
		} else {
			scanned += n
			for _, f := range findings {
				card.add("control-plane", f)
			}
		}
	} else {
		card.unavailable["control-plane"] = "requires cloud provider metrics; " +
			cloud.Unavailable(provider, cloud.CapabilityMetrics).Reason
	}

	// Memory merge (§9.4, the M4 exit shape): with --store, join
	// every finding against the sentinel's OPEN triage-status
	// records — matched findings gain the triage_* fields and their
	// severity reflects the agent's judgment, so the scorecard's
	// worst-severity and ordering below see triaged reality too.
	// Unmatched findings (and runs without --store) are unchanged.
	if storePath := inv.Flags.String("store"); storePath != "" {
		st, err := store.OpenRead(storePath)
		if err != nil {
			return 0, err
		}
		defer func() { _ = st.Close() }()
		records, err := st.TriageStatuses(ctx, memory.TriageQuery{OpenOnly: true})
		if err != nil {
			return 0, err
		}
		joiner := memory.NewJoiner(records, now)
		for _, fs := range card.findings {
			for i := range fs {
				joiner.Annotate(&fs[i])
			}
		}
	}

	// Deterministic, critical-first detail order for the categories
	// collected from paged Lists (delta pre-sorts its own the same
	// way).
	for _, cat := range []string{"storage", "certs"} {
		fs := card.findings[cat]
		sort.Slice(fs, func(i, j int) bool {
			ri, rj := severityRank(fs[i].Severity), severityRank(fs[j].Severity)
			if ri != rj {
				return ri < rj
			}
			if fs[i].Namespace != fs[j].Namespace {
				return fs[i].Namespace < fs[j].Namespace
			}
			if fs[i].Name != fs[j].Name {
				return fs[i].Name < fs[j].Name
			}
			return fs[i].Kind < fs[j].Kind
		})
	}

	// Scorecard block first — every category answers — then the
	// degraded categories' details, in the same fixed order.
	for _, cat := range categoryOrder {
		if err := inv.Out.Emit(categoryFinding(cat, card, top)); err != nil {
			return 0, err
		}
	}
	for _, cat := range categoryOrder {
		for _, f := range card.findings[cat] {
			f.Details = append([]emit.Field{{Key: "category", Value: cat}}, f.Details...)
			// §8 push/pull dedup key (docs/signal-schema-v1.md): a
			// category detail describes a symptom class the sentinel
			// could also push, so it carries the scan fingerprint the
			// push path would stamp — how "the sentinel paged on this
			// 20 minutes ago" and "the scan still sees it" merge into
			// one finding instead of two. Scorecard lines have no
			// incident-class identity and stay fingerprint-free.
			if f.Reason != "" && f.KindOfObject != "" {
				f.Fingerprint = engine.ScanFingerprint(f.Reason, f.KindOfObject, "")
			}
			if err := inv.Out.Emit(f); err != nil {
				return 0, err
			}
		}
	}
	return scanned, nil
}

// deltaCategory buckets a delta finding kind into its scorecard
// category.
func deltaCategory(kind string) string {
	switch {
	case kind == "pod.pending":
		return "pending"
	case strings.HasPrefix(kind, "node."):
		return "nodes"
	case strings.HasPrefix(kind, "addon."):
		return "addons"
	case strings.HasPrefix(kind, "quota."):
		return "quota"
	case strings.HasPrefix(kind, "workload.") || strings.HasPrefix(kind, "job."):
		return "rollouts"
	default: // pod.crashloop, pod.imagepull, pod.oomkilled, restarts, waiting, notready, failed
		return "crashloops"
	}
}

// categoryFinding renders one scorecard line.
func categoryFinding(cat string, card *scorecard, top int) emit.Finding {
	f := emit.Finding{
		Kind:     "health.category",
		Severity: emit.SeverityInfo,
		Details:  []emit.Field{{Key: "category", Value: cat}},
	}
	status := func(s string) {
		f.Details = append(f.Details, emit.Field{Key: "status", Value: s})
	}
	if reason, ok := card.unavailable[cat]; ok {
		status(statusUnavailable)
		f.Reason = "Unavailable"
		f.Message = reason
		return f
	}
	fs := card.findings[cat]
	if len(fs) == 0 {
		status(statusHealthy)
		return f
	}
	status(statusDegraded)
	f.Severity = worstSeverity(fs)
	f.Details = append(f.Details, emit.Field{Key: "total", Value: strconv.Itoa(len(fs))})
	names := make([]string, 0, top)
	for i := 0; i < len(fs) && i < top; i++ {
		name := fs[i].Kind
		if fs[i].Name != "" {
			subject := fs[i].Name
			if fs[i].Namespace != "" {
				subject = fs[i].Namespace + "/" + subject
			}
			name += " " + subject
		}
		names = append(names, name)
	}
	f.Details = append(f.Details, emit.Field{Key: "top", Value: strings.Join(names, "; ")})
	return f
}

// severityRank orders critical < warning < info; unknown sinks last.
func severityRank(sev string) int {
	switch sev {
	case emit.SeverityCritical:
		return 0
	case emit.SeverityWarning:
		return 1
	case emit.SeverityInfo:
		return 2
	}
	return 3
}

func worstSeverity(fs []emit.Finding) string {
	worst := emit.SeverityInfo
	for _, f := range fs {
		if f.Severity == emit.SeverityCritical {
			return emit.SeverityCritical
		}
		if f.Severity == emit.SeverityWarning {
			worst = emit.SeverityWarning
		}
	}
	return worst
}

// pageLimit matches the other one-shot commands' paged Lists.
const pageLimit = 500

func listPages[T any](what string, list func(metav1.ListOptions) ([]T, string, error), each func(*T)) error {
	opts := metav1.ListOptions{Limit: pageLimit}
	for {
		items, cont, err := list(opts)
		if err != nil {
			return fmt.Errorf("listing %s: %w", what, err)
		}
		for i := range items {
			each(&items[i])
		}
		if cont == "" {
			return nil
		}
		opts.Continue = cont
	}
}

// checkStorage flags PVCs not Bound: Pending (workloads mounting it
// cannot start) and Lost (the bound volume is gone).
func checkStorage(ctx context.Context, client kubernetes.Interface, ns string, now time.Time, card *scorecard) (int, error) {
	count := 0
	err := listPages("persistentvolumeclaims", func(o metav1.ListOptions) ([]corev1.PersistentVolumeClaim, string, error) {
		l, err := client.CoreV1().PersistentVolumeClaims(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(pvc *corev1.PersistentVolumeClaim) {
		count++
		f := emit.Finding{
			Namespace:    pvc.Namespace,
			KindOfObject: "PersistentVolumeClaim",
			Name:         pvc.Name,
			Details: []emit.Field{
				{Key: "phase", Value: string(pvc.Status.Phase)},
				{Key: "age", Value: now.Sub(pvc.CreationTimestamp.Time).Truncate(time.Second).String()},
			},
		}
		switch pvc.Status.Phase {
		case corev1.ClaimPending:
			f.Kind = "pvc.pending"
			f.Severity = emit.SeverityWarning
			f.Reason = "PVCPending"
			f.Message = "claim is not bound; pods mounting it cannot start"
		case corev1.ClaimLost:
			f.Kind = "pvc.lost"
			f.Severity = emit.SeverityCritical
			f.Reason = "PVCLost"
			f.Message = "bound volume is lost"
		default:
			return // Bound is nominal
		}
		card.add("storage", f)
	})
	return count, err
}

// checkCerts parses tls.crt of every kubernetes.io/tls Secret in
// scope — expiry and parseability only; no key material is read and
// none can reach a finding. Only TLS-type secrets count as scanned.
func checkCerts(ctx context.Context, client kubernetes.Interface, ns string, now time.Time, warn time.Duration, card *scorecard) (int, error) {
	count := 0
	now = now.UTC()
	err := listPages("secrets", func(o metav1.ListOptions) ([]corev1.Secret, string, error) {
		l, err := client.CoreV1().Secrets(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(s *corev1.Secret) {
		if s.Type != corev1.SecretTypeTLS {
			return
		}
		count++
		base := emit.Finding{
			Namespace:    s.Namespace,
			KindOfObject: "Secret",
			Name:         s.Name,
		}
		block, _ := pem.Decode(s.Data[corev1.TLSCertKey])
		var cert *x509.Certificate
		if block != nil && block.Type == "CERTIFICATE" {
			cert, _ = x509.ParseCertificate(block.Bytes)
		}
		if cert == nil {
			f := base
			f.Kind = "cert.invalid"
			f.Severity = emit.SeverityWarning
			f.Reason = "InvalidCertificate"
			f.Message = "tls.crt does not contain a parseable X.509 certificate"
			card.add("certs", f)
			return
		}
		subject := cert.Subject.CommonName
		if subject == "" {
			subject = cert.Subject.String()
		}
		days := int(math.Floor(cert.NotAfter.Sub(now).Hours() / 24))
		details := []emit.Field{
			{Key: "subject", Value: subject},
			{Key: "not_after", Value: cert.NotAfter.UTC().Format(time.RFC3339)},
			{Key: "days_left", Value: strconv.Itoa(days)},
		}
		switch {
		case cert.NotAfter.Before(now):
			f := base
			f.Kind = "cert.expired"
			f.Severity = emit.SeverityCritical
			f.Reason = "CertificateExpired"
			f.Message = fmt.Sprintf("certificate expired %dd ago", -days)
			f.Details = details
			card.add("certs", f)
		case cert.NotAfter.Sub(now) <= warn:
			f := base
			f.Kind = "cert.expiring"
			f.Severity = emit.SeverityWarning
			f.Reason = "CertificateExpiringSoon"
			f.Message = fmt.Sprintf("certificate expires in %dd", days)
			f.Details = details
			card.add("certs", f)
		}
	})
	return count, err
}
