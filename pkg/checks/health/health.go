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
// to pkg/checks/delta's scan; storage, certs, and webhooks are the
// three lightweight checks that have no standalone command yet
// (webhooks is the minimal service-backend subset of M5's `state
// webhooks`). Control-plane latency needs cloud provider metrics and
// reports unavailable until M4 (§14), through the pkg/cloud
// provider boundary. M1 is LIVE CHECKS ONLY: the merge with open
// sentinel findings (§9.1) and triage-status records (§9.4) — a
// scan mid-incident reporting triaged reality — lands in M4.
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

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
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
	// default detection (NoProvider on vanilla builds — the
	// control-plane category then reports unavailable, never
	// silence, §2).
	Provider func(ctx context.Context) (cloud.Provider, error)
	// Now is the scan clock. Nil means time.Now.
	Now func() time.Time
}

func (d Deps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.BuildClient(kube.Options{})
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
		Summary: "\"Any issues with this cluster?\" in one call: a ten-category scorecard (control-plane, nodes, crash loops, pending, rollouts, storage, add-ons, quotas, certs, webhooks) — every category answers healthy|degraded|unavailable, degraded ones with details. Live checks only until M4, when open sentinel findings and triage-status records merge in.",
		Flags: []emit.FlagSpec{
			{Name: "top", Type: emit.FlagInt, Default: "3",
				Help: "how many findings to name inline on a degraded category's scorecard line"},
			{Name: "cert-warn", Type: emit.FlagDuration, Default: "720h",
				Help: "report TLS certificates expiring within this window (certs category)"},
		},
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
		}, deltaOutput()...),
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

// deltaOutput pulls the delta command's glossary for the delegated
// categories, minus the keys health declares itself.
func deltaOutput() []checks.OutputField {
	seen := map[string]bool{
		"category": true, "status": true, "total": true, "top": true,
		"subject": true, "not_after": true, "days_left": true,
		"phase": true, "webhook": true, "service": true,
	}
	c, ok := checks.Lookup("triage delta")
	if !ok {
		return nil // isolated test registry; contract test asserts presence
	}
	var out []checks.OutputField
	for _, f := range c.Output {
		if seen[f.Name] {
			continue
		}
		seen[f.Name] = true
		out = append(out, f)
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

	// webhooks: cluster-scoped configurations with a service backend
	// that does not resolve.
	if ns == "" {
		n, err = checkWebhooks(ctx, client, card)
		if err != nil {
			return 0, err
		}
		scanned += n
	} else {
		card.unavailable["webhooks"] = "webhook configurations are cluster-scoped; run without --namespace"
	}

	// control-plane: latency packs need cloud provider metrics; the
	// query packs themselves land in M4 (§14). Explicit unavailable
	// through the provider boundary, never silence (§2).
	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	reason := "requires cloud provider metrics (M4)"
	if _, ok := provider.Metrics(); !ok {
		reason += "; " + cloud.Unavailable(provider, cloud.CapabilityMetrics).Reason
	}
	card.unavailable["control-plane"] = reason

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

// checkWebhooks is the minimal service-backend subset of M5's
// `state webhooks` (§5): a validating/mutating webhook whose
// ClientConfig names a Service that does not exist will fail (or
// silently pass, per failurePolicy) every admission it matches —
// cluster-wide breakage invisible from any workload's status.
// URL-backed webhooks are skipped: an external endpoint cannot be
// verified from a List pass.
func checkWebhooks(ctx context.Context, client kubernetes.Interface, card *scorecard) (int, error) {
	// Auxiliary input, deliberately not counted as scanned: services
	// are listed only to resolve webhook backends.
	services := map[string]bool{}
	err := listPages("services", func(o metav1.ListOptions) ([]corev1.Service, string, error) {
		l, err := client.CoreV1().Services(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(s *corev1.Service) { services[s.Namespace+"/"+s.Name] = true })
	if err != nil {
		return 0, err
	}

	count := 0
	check := func(configKind, configName, webhookName string, svc *admissionv1.ServiceReference) {
		if svc == nil { // URL-backed
			return
		}
		if services[svc.Namespace+"/"+svc.Name] {
			return
		}
		card.add("webhooks", emit.Finding{
			Kind:         "webhook.backend_missing",
			Severity:     emit.SeverityCritical,
			KindOfObject: configKind,
			Name:         configName,
			Reason:       "BackendServiceMissing",
			Message:      fmt.Sprintf("webhook backend service %s/%s not found", svc.Namespace, svc.Name),
			Details: []emit.Field{
				{Key: "webhook", Value: configName + "/" + webhookName},
				{Key: "service", Value: svc.Namespace + "/" + svc.Name},
			},
		})
	}

	err = listPages("validatingwebhookconfigurations", func(o metav1.ListOptions) ([]admissionv1.ValidatingWebhookConfiguration, string, error) {
		l, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(c *admissionv1.ValidatingWebhookConfiguration) {
		count++
		for i := range c.Webhooks {
			check("ValidatingWebhookConfiguration", c.Name, c.Webhooks[i].Name, c.Webhooks[i].ClientConfig.Service)
		}
	})
	if err != nil {
		return 0, err
	}
	err = listPages("mutatingwebhookconfigurations", func(o metav1.ListOptions) ([]admissionv1.MutatingWebhookConfiguration, string, error) {
		l, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(c *admissionv1.MutatingWebhookConfiguration) {
		count++
		for i := range c.Webhooks {
			check("MutatingWebhookConfiguration", c.Name, c.Webhooks[i].Name, c.Webhooks[i].ClientConfig.Service)
		}
	})
	if err != nil {
		return 0, err
	}

	// Deterministic order regardless of list interleaving.
	fs := card.findings["webhooks"]
	sort.Slice(fs, func(i, j int) bool {
		if fs[i].Name != fs[j].Name {
			return fs[i].Name < fs[j].Name
		}
		return detailValue(fs[i], "webhook") < detailValue(fs[j], "webhook")
	})
	return count, nil
}

func detailValue(f emit.Finding, key string) string {
	for _, d := range f.Details {
		if d.Key == key {
			return d.Value
		}
	}
	return ""
}
