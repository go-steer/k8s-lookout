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

// `state webhooks` (DESIGN.md §5): the full admission-webhook audit.
// A webhook with a dead backend either rejects every admission it
// matches (failurePolicy Fail — cluster-impacting) or silently stops
// enforcing its policy (Ignore); both are invisible from any
// workload's status. The check crosses backend health with the
// failure policy, sizes the blast radius (which namespaces and
// operations the webhook gates), flags timeout stall risk, and
// parses CA-bundle expiry.
//
// Finding kinds and severities:
//
//	webhook.failing_closed  critical  failurePolicy Fail (the v1 default) + dead backend: matching admissions are rejected
//	webhook.dead_backend    warning   failurePolicy Ignore + dead backend: the policy is silently not enforced
//	webhook.slow_risk       info      timeoutSeconds ≥ 10 with failurePolicy Fail on a live backend: a hung backend stalls matching admissions
//	webhook.ca_expired      critical  clientConfig.caBundle's freshest certificate is expired: TLS to the backend fails (behaves like a dead backend)
//	webhook.ca_expiring     warning   caBundle certificate expires within --cert-warn
//
// Healthy webhooks emit nothing (§4.2 zero nominal state). The core
// runs as a pure function (CheckWebhooks) over listed inputs
// (LoadWebhookInputs) so `health`'s webhooks category can delegate
// here without a second command runner.

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
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(WebhooksCommand(Deps{}))
}

// WebhooksCommand builds the `lookout state webhooks` command (§5
// tool matrix row: absorbs v2's webhook-inspector). This is the FULL
// admission-webhook check; `health`'s webhooks category delegates to
// the same core.
func WebhooksCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "state webhooks",
		MCPName: "k8s_admission_webhooks",
		Summary: "When creates/updates hang or fail cluster-wide with \"failed calling webhook\", or before relying on a policy engine: audit every admission webhook — dead backends × failurePolicy (Fail + dead backend rejects every matching admission), the namespace/rule blast radius, timeout stall risk, CA-bundle expiry. The full check; health's webhooks category delegates here.",
		Flags: []emit.FlagSpec{
			{Name: "cert-warn", Type: emit.FlagDuration, Default: "720h",
				Help: "report webhook CA bundles expiring within this window"},
		},
		Kinds: []checks.KindField{
			checks.Kind("webhook.failing_closed", "the webhook has no working backend and failurePolicy=Fail: every gated write is rejected cluster-wide", emit.SeverityCritical),
			checks.Kind("webhook.dead_backend", "the webhook's service backend is missing, has no ready endpoints, or does not serve the named port", emit.SeverityWarning),
			checks.Kind("webhook.slow_risk", "the webhook's timeout is long enough to slow every gated write if the backend degrades", emit.SeverityInfo),
			checks.Kind("webhook.ca_expired", "the webhook's caBundle has expired: the API server cannot verify it", emit.SeverityCritical),
			checks.Kind("webhook.ca_expiring", "the webhook's caBundle expires within --cert-warn", emit.SeverityWarning),
		},
		Output: []checks.OutputField{
			{Name: "webhook", Doc: "admission webhook as <configuration>/<webhook name>"},
			{Name: "service", Doc: "service backend the webhook points at, as <namespace>/<name>"},
			{Name: "backend", Doc: "why the backend is dead: service missing, no ready endpoints, or port <p> not on service"},
			{Name: "gates", Doc: "namespaces the webhook gates, from namespaceSelector: all namespaces, or <matched>/<total> namespaces with up to 5 names"},
			{Name: "rules", Doc: "compact operations/resources summary of the webhook's rules, e.g. \"CREATE,UPDATE pods,deployments.apps\""},
			{Name: "object_selector", Doc: "the webhook's objectSelector, when one is set"},
			{Name: "timeout", Doc: "webhook timeoutSeconds as <n>s (nil defaults to the API's 10s)"},
			{Name: "subject", Doc: "CA-bundle certificate subject (CN when set); never key material"},
			{Name: "not_after", Doc: "CA-bundle certificate NotAfter, RFC 3339"},
			{Name: "days_left", Doc: "whole days until NotAfter (negative = expired)"},
		},
		Examples: []string{
			"lookout state webhooks",
			"lookout state webhooks --format=json --cert-warn=336h",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runWebhooks(ctx, deps, inv)
		},
	}
}

func runWebhooks(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("state webhooks audits every admission webhook, not one workload: --workload does not apply")
	}
	if inv.Scope.Namespace != "" || inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("state webhooks reads cluster-scoped webhook configurations: --namespace/-A do not apply")
	}
	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	in, scanned, err := LoadWebhookInputs(ctx, client)
	if err != nil {
		return 0, err
	}
	for _, f := range CheckWebhooks(in, inv.Flags.Duration("cert-warn"), deps.now()) {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	return scanned, nil
}

// WebhookInputs are the listed objects the webhook checks run over.
type WebhookInputs struct {
	Validating []*admissionv1.ValidatingWebhookConfiguration
	Mutating   []*admissionv1.MutatingWebhookConfiguration
	Services   map[string]*corev1.Service              // ns/name
	Slices     map[string][]*discoveryv1.EndpointSlice // ns/svcName via discoveryv1.LabelServiceName
	Namespaces []*corev1.Namespace
}

// LoadWebhookInputs runs the paged Lists behind `state webhooks`.
// The returned count is the number of webhook configurations
// (Validating + Mutating) — that is what the command reports as
// scanned; services, endpointslices, and namespaces are auxiliary
// inputs for backend and scope resolution, deliberately not counted
// (matching `health`'s webhooks-category semantics).
func LoadWebhookInputs(ctx context.Context, client kubernetes.Interface) (WebhookInputs, int, error) {
	in := WebhookInputs{
		Services: map[string]*corev1.Service{},
		Slices:   map[string][]*discoveryv1.EndpointSlice{},
	}
	scanned := 0
	steps := []func() error{
		func() error {
			return listPages("validatingwebhookconfigurations", func(o metav1.ListOptions) ([]admissionv1.ValidatingWebhookConfiguration, string, error) {
				l, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *admissionv1.ValidatingWebhookConfiguration) {
				in.Validating = append(in.Validating, c)
				scanned++
			})
		},
		func() error {
			return listPages("mutatingwebhookconfigurations", func(o metav1.ListOptions) ([]admissionv1.MutatingWebhookConfiguration, string, error) {
				l, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *admissionv1.MutatingWebhookConfiguration) {
				in.Mutating = append(in.Mutating, c)
				scanned++
			})
		},
		func() error {
			return listPages("services", func(o metav1.ListOptions) ([]corev1.Service, string, error) {
				l, err := client.CoreV1().Services(metav1.NamespaceAll).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *corev1.Service) { in.Services[key(s.Namespace, s.Name)] = s })
		},
		func() error {
			return listPages("endpointslices", func(o metav1.ListOptions) ([]discoveryv1.EndpointSlice, string, error) {
				l, err := client.DiscoveryV1().EndpointSlices(metav1.NamespaceAll).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *discoveryv1.EndpointSlice) {
				if svc := s.Labels[discoveryv1.LabelServiceName]; svc != "" {
					k := key(s.Namespace, svc)
					in.Slices[k] = append(in.Slices[k], s)
				}
			})
		},
		func() error {
			return listPages("namespaces", func(o metav1.ListOptions) ([]corev1.Namespace, string, error) {
				l, err := client.CoreV1().Namespaces().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(n *corev1.Namespace) { in.Namespaces = append(in.Namespaces, n) })
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return WebhookInputs{}, 0, err
		}
	}
	return in, scanned, nil
}

// webhookEntry is the kind-agnostic view of one entry in a
// configuration's .Webhooks — ValidatingWebhook and MutatingWebhook
// share every field the checks read.
type webhookEntry struct {
	configKind  string // ValidatingWebhookConfiguration | MutatingWebhookConfiguration
	configName  string
	name        string
	client      admissionv1.WebhookClientConfig
	policy      *admissionv1.FailurePolicyType
	nsSelector  *metav1.LabelSelector
	objSelector *metav1.LabelSelector
	rules       []admissionv1.RuleWithOperations
	timeout     *int32
}

// CheckWebhooks runs the full §5 `state webhooks` analysis over the
// listed inputs; findings are sorted (config name, webhook name,
// kind) and healthy webhooks are silent. Pure function — `health`'s
// webhooks category delegates here.
func CheckWebhooks(in WebhookInputs, certWarn time.Duration, now time.Time) []emit.Finding {
	var entries []webhookEntry
	for _, c := range in.Validating {
		for i := range c.Webhooks {
			w := &c.Webhooks[i]
			entries = append(entries, webhookEntry{
				configKind: "ValidatingWebhookConfiguration", configName: c.Name,
				name: w.Name, client: w.ClientConfig, policy: w.FailurePolicy,
				nsSelector: w.NamespaceSelector, objSelector: w.ObjectSelector,
				rules: w.Rules, timeout: w.TimeoutSeconds,
			})
		}
	}
	for _, c := range in.Mutating {
		for i := range c.Webhooks {
			w := &c.Webhooks[i]
			entries = append(entries, webhookEntry{
				configKind: "MutatingWebhookConfiguration", configName: c.Name,
				name: w.Name, client: w.ClientConfig, policy: w.FailurePolicy,
				nsSelector: w.NamespaceSelector, objSelector: w.ObjectSelector,
				rules: w.Rules, timeout: w.TimeoutSeconds,
			})
		}
	}

	var findings []emit.Finding
	for _, e := range entries {
		findings = append(findings, checkWebhookEntry(in, e, certWarn, now)...)
	}
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Name != findings[j].Name {
			return findings[i].Name < findings[j].Name
		}
		wi, wj := findingDetail(findings[i], "webhook"), findingDetail(findings[j], "webhook")
		if wi != wj {
			return wi < wj
		}
		return findings[i].Kind < findings[j].Kind
	})
	return findings
}

// failsClosed reports the effective failure policy. In
// admissionregistration.k8s.io/v1 an unset failurePolicy defaults to
// Fail ("Allowed values are Ignore or Fail. Defaults to Fail." —
// k8s.io/api admissionregistration/v1 types; the old Ignore default
// was v1beta1 only, and this check reads v1).
func failsClosed(p *admissionv1.FailurePolicyType) bool {
	return p == nil || *p == admissionv1.Fail
}

// effectiveTimeout returns the webhook's timeout: an unset
// timeoutSeconds defaults to 10 ("Default to 10 seconds." — v1 API).
func effectiveTimeout(t *int32) int32 {
	if t == nil {
		return 10
	}
	return *t
}

func checkWebhookEntry(in WebhookInputs, e webhookEntry, certWarn time.Duration, now time.Time) []emit.Finding {
	var findings []emit.Finding
	whName := e.configName + "/" + e.name

	// Backend health. URL-backed webhooks skip the backend checks —
	// an external endpoint cannot be verified from a List pass — but
	// keep the timeout and CA-bundle checks below, which read only
	// the webhook's own spec.
	svcDetail := ""
	dead := "" // reason detail; empty = alive (or URL-backed: not provably dead)
	deadMsg := ""
	if svc := e.client.Service; svc != nil {
		svcDetail = svc.Namespace + "/" + svc.Name
		backendSvc := in.Services[key(svc.Namespace, svc.Name)]
		port := int32(443) // ServiceReference default ("Default to 443" — v1 API)
		if svc.Port != nil {
			port = *svc.Port
		}
		switch {
		case backendSvc == nil:
			dead = "service missing"
			deadMsg = fmt.Sprintf("backend service %s not found", svcDetail)
		case !anyReadyEndpoint(in.Slices[key(svc.Namespace, svc.Name)]):
			dead = "no ready endpoints"
			deadMsg = fmt.Sprintf("backend service %s has no ready endpoints", svcDetail)
		case !servicePortExists(backendSvc, port):
			dead = fmt.Sprintf("port %d not on service", port)
			deadMsg = fmt.Sprintf("backend service %s does not expose port %d", svcDetail, port)
		}
	}

	baseDetails := func() []emit.Field {
		d := []emit.Field{{Key: "webhook", Value: whName}}
		if svcDetail != "" {
			d = append(d, emit.Field{Key: "service", Value: svcDetail})
		}
		return d
	}
	// scopeDetails is the compact blast-radius block on dead-backend
	// findings: which namespaces the webhook gates, which
	// operations/resources it matches, and the objectSelector when
	// one narrows it further.
	scopeDetails := func(d []emit.Field) []emit.Field {
		d = append(d,
			emit.Field{Key: "gates", Value: renderGates(e.nsSelector, in.Namespaces)})
		if r := renderRules(e.rules); r != "" {
			d = append(d, emit.Field{Key: "rules", Value: r})
		}
		if s := renderSelector(e.objSelector); s != "" {
			d = append(d, emit.Field{Key: "object_selector", Value: s})
		}
		return d
	}

	if dead != "" {
		if failsClosed(e.policy) {
			findings = append(findings, emit.Finding{
				Kind:         "webhook.failing_closed",
				Severity:     emit.SeverityCritical,
				KindOfObject: e.configKind,
				Name:         e.configName,
				Reason:       "FailingClosed",
				Message:      deadMsg + " — matching admissions are REJECTED (failurePolicy Fail)",
				Details: scopeDetails(append(baseDetails(),
					emit.Field{Key: "backend", Value: dead})),
			})
		} else {
			findings = append(findings, emit.Finding{
				Kind:         "webhook.dead_backend",
				Severity:     emit.SeverityWarning,
				KindOfObject: e.configKind,
				Name:         e.configName,
				Reason:       "DeadBackend",
				Message:      deadMsg + " — webhook silently passes everything; the policy it enforces is NOT running (failurePolicy Ignore)",
				Details: scopeDetails(append(baseDetails(),
					emit.Field{Key: "backend", Value: dead})),
			})
		}
	} else if timeout := effectiveTimeout(e.timeout); timeout >= 10 && failsClosed(e.policy) {
		// Alive backend (or URL-backed, not provably dead): a slow or
		// hung backend stalls every matching admission for up to the
		// timeout before failing closed. A dead backend is already
		// covered by failing_closed above — never both.
		findings = append(findings, emit.Finding{
			Kind:         "webhook.slow_risk",
			Severity:     emit.SeverityInfo,
			KindOfObject: e.configKind,
			Name:         e.configName,
			Reason:       "SlowWebhookRisk",
			Message:      fmt.Sprintf("timeoutSeconds %d with failurePolicy Fail: a slow or hung backend stalls every matching admission for up to %ds before rejecting it", timeout, timeout),
			Details: append(baseDetails(),
				emit.Field{Key: "timeout", Value: fmt.Sprintf("%ds", timeout)}),
		})
	}

	// CA-bundle expiry. An empty or unparseable caBundle is SKIPPED
	// silently: cluster trust bundles and injected CAs (cert-manager
	// ca-injector, service-ca) are common, so a missing bundle is not
	// provably broken. Only a bundle that parses and has provably
	// expired (or soon-expiring) certificates is reported.
	if cert := freshestCABundleCert(e.client.CABundle); cert != nil {
		subject := cert.Subject.CommonName
		if subject == "" {
			subject = cert.Subject.String()
		}
		days := int(math.Floor(cert.NotAfter.Sub(now).Hours() / 24))
		certDetails := func() []emit.Field {
			return append(baseDetails(),
				emit.Field{Key: "subject", Value: subject},
				emit.Field{Key: "not_after", Value: cert.NotAfter.UTC().Format(time.RFC3339)},
				emit.Field{Key: "days_left", Value: strconv.Itoa(days)})
		}
		switch {
		case cert.NotAfter.Before(now):
			// TLS to the backend cannot be verified — behaves like a
			// dead backend.
			msg := fmt.Sprintf("CA bundle expired %dd ago — TLS to the backend fails", -days)
			if failsClosed(e.policy) {
				msg += "; matching admissions are REJECTED (failurePolicy Fail)"
			}
			findings = append(findings, emit.Finding{
				Kind:         "webhook.ca_expired",
				Severity:     emit.SeverityCritical,
				KindOfObject: e.configKind,
				Name:         e.configName,
				Reason:       "CABundleExpired",
				Message:      msg,
				Details:      certDetails(),
			})
		case cert.NotAfter.Sub(now) <= certWarn:
			findings = append(findings, emit.Finding{
				Kind:         "webhook.ca_expiring",
				Severity:     emit.SeverityWarning,
				KindOfObject: e.configKind,
				Name:         e.configName,
				Reason:       "CABundleExpiringSoon",
				Message:      fmt.Sprintf("CA bundle expires in %dd", days),
				Details:      certDetails(),
			})
		}
	}
	return findings
}

// anyReadyEndpoint reports whether any endpoint across the service's
// slices is Ready. A nil Ready condition counts as ready, per the
// EndpointSlice API convention ("a nil value indicates an unknown
// state[, and] should be interpreted as true").
func anyReadyEndpoint(slices []*discoveryv1.EndpointSlice) bool {
	for _, sl := range slices {
		for i := range sl.Endpoints {
			r := sl.Endpoints[i].Conditions.Ready
			if r == nil || *r {
				return true
			}
		}
	}
	return false
}

// servicePortExists reports whether the service exposes port among
// spec.ports[].port.
func servicePortExists(svc *corev1.Service, port int32) bool {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Port == port {
			return true
		}
	}
	return false
}

// gatesNameCap bounds the namespace name list on the gates detail.
const gatesNameCap = 5

// renderGates evaluates the webhook's namespaceSelector against the
// listed namespaces: a nil (or empty) selector gates all namespaces.
// (Nuances like the API server's implicit kube-system exemption for
// some managed offerings are deployment-specific and deliberately
// not modeled.)
func renderGates(nsSel *metav1.LabelSelector, namespaces []*corev1.Namespace) string {
	if nsSel == nil {
		return "all namespaces"
	}
	sel, err := metav1.LabelSelectorAsSelector(nsSel)
	if err != nil || sel.Empty() {
		// An empty selector matches everything; an unparseable one is
		// rejected by the API server, so treat both as "all".
		return "all namespaces"
	}
	var matched []string
	for _, ns := range namespaces {
		if sel.Matches(labels.Set(ns.Labels)) {
			matched = append(matched, ns.Name)
		}
	}
	sort.Strings(matched)
	out := fmt.Sprintf("%d/%d namespaces", len(matched), len(namespaces))
	if len(matched) == 0 {
		return out
	}
	names := matched
	extra := 0
	if len(names) > gatesNameCap {
		extra = len(names) - gatesNameCap
		names = names[:gatesNameCap]
	}
	out += ": " + strings.Join(names, ", ")
	if extra > 0 {
		out += fmt.Sprintf(", +%d more", extra)
	}
	return out
}

// renderRules compacts the webhook's rules into one
// "OPS resources" summary, e.g. "CREATE,UPDATE pods,deployments.apps":
// operations and resources are deduped across rules, resources render
// as <resource>.<group> (core group bare, "*" stays *), and the
// resource list is capped like the gates namespace list.
func renderRules(rules []admissionv1.RuleWithOperations) string {
	ops := map[string]bool{}
	res := map[string]bool{}
	for i := range rules {
		r := &rules[i]
		for _, op := range r.Operations {
			ops[string(op)] = true
		}
		groups := r.APIGroups
		if len(groups) == 0 {
			groups = []string{""}
		}
		for _, resource := range r.Resources {
			for _, g := range groups {
				switch {
				case resource == "*" && (g == "*" || g == ""):
					res["*"] = true
				case g == "":
					res[resource] = true
				default:
					res[resource+"."+g] = true
				}
			}
		}
	}
	if len(ops) == 0 && len(res) == 0 {
		return ""
	}
	opList := sortedKeys(ops)
	if ops["*"] {
		opList = []string{"*"}
	}
	resList := sortedKeys(res)
	if res["*"] {
		resList = []string{"*"}
	}
	extra := 0
	if len(resList) > gatesNameCap {
		extra = len(resList) - gatesNameCap
		resList = resList[:gatesNameCap]
	}
	out := strings.Join(opList, ",")
	if len(resList) > 0 {
		if out != "" {
			out += " "
		}
		out += strings.Join(resList, ",")
		if extra > 0 {
			out += fmt.Sprintf(",+%d more", extra)
		}
	}
	return out
}

// renderSelector renders a label selector compactly; "" when unset
// or empty (an empty objectSelector matches everything and adds no
// information).
func renderSelector(ls *metav1.LabelSelector) string {
	if ls == nil {
		return ""
	}
	sel, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil || sel.Empty() {
		return ""
	}
	return sel.String()
}

// freshestCABundleCert parses every CERTIFICATE block of a caBundle
// PEM and returns the one with the latest NotAfter — a bundle is
// only provably broken when its freshest CA is expired (clients
// verify against any certificate in the bundle). Nil when the bundle
// is empty or nothing parses (skip silently; see checkWebhookEntry).
func freshestCABundleCert(bundle []byte) *x509.Certificate {
	var best *x509.Certificate
	rest := bundle
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return best
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		if best == nil || cert.NotAfter.After(best.NotAfter) {
			best = cert
		}
	}
}

// findingDetail returns one Details value by key ("" when absent).
func findingDetail(f emit.Finding, key string) string {
	for _, d := range f.Details {
		if d.Key == key {
			return d.Value
		}
	}
	return ""
}
