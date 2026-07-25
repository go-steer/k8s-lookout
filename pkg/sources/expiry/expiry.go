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

// Package expiry is the expiry signal source (DESIGN.md §7.2 row 6):
// leading COUNTDOWNS — TLS secret certificates, webhook CA bundles,
// ServiceAccount token expiries where detectable, and cert-manager
// Certificate status — "cert expires in 72 h and last renewal failed".
//
// Scan model — periodic paged LISTs, deliberately NOT a Secret
// informer: an informer would cache every Secret in scope in memory —
// the single most sensitive object class in the cluster held resident
// in the sentinel's heap, and by far the heaviest cache for a signal
// whose useful resolution is measured in hours. Certificates expire on
// a countdown, not on an edge: a --expiry-interval (default 1h) paged
// LIST (field-selected to the relevant Secret types, page size capped)
// observes every crossing in time, holds at most one page of Secrets
// transiently, and retains nothing but (identity, notAfter, subject,
// issuer) per object. The cost is detection latency bounded by the
// scan interval, which is negligible against 14-day / 72-hour
// thresholds.
//
// Signal contract: one kind, expiry.warning (§7.3), fired once per
// (object, threshold crossing) — warning when notAfter enters
// --expiry-warn (default 336h/14d), escalated to critical when it
// enters CriticalWindow (72h, design-fixed) or the object is already
// expired, and — for cert-manager Certificates only, where renewal
// state is observable — critical whenever the last renewal FAILED,
// regardless of window (the design's example is exactly this pairing).
// Each scan re-checks; an object that stays inside a threshold does
// not re-fire (the per-object level latch), and a renewed cert
// (notAfter moved back out of the warn window) resets the latch so a
// future crossing fires fresh. Renewal is also the §7.4 clearance
// predicate (ClearanceObserver). The latch is in-memory: a sentinel
// restart re-fires the current level once and the engine's persisted
// dedup absorbs the repeat — same posture as object-state's
// progress_deadline countdown.
//
// cert-manager is discovery-gated: when the cert-manager.io/v1
// Certificate CRD is absent the scan skips it with ONE startup log
// line naming what was skipped and why — never silently (a cluster
// where certificates rotate through cert-manager and the sentinel
// quietly ignores renewal state would be lying about its coverage).
// TLS Secrets owned by a cert-manager Certificate are attributed to
// the Certificate (which carries renewal state) and skipped as plain
// secrets, so one renewal failure is one incident, not two.
package expiry

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "expiry"

// kindPrefix namespaces this source's signal kinds (§7.3).
const kindPrefix = "expiry."

// KindWarning is the one kind this source emits (§7.3
// `expiry.warning`): an expiry countdown crossed a threshold.
// Severity carries the which-threshold distinction (warning at
// --expiry-warn, critical at CriticalWindow / expired /
// renewal-failed); the kind is stable across both.
const KindWarning = kindPrefix + "warning"

// CriticalWindow is the design-fixed critical threshold: "cert
// expires in 72 h" (§7.2). Deliberately not a flag — the warning
// threshold is deployment policy (--expiry-warn), the pager-grade
// threshold is the design's.
const CriticalWindow = 72 * time.Hour

// ConfidenceBasis is the §8 forecast confidence_basis for every
// signal this source emits: the ETA is the certificate's own notAfter
// — not a model, a fact.
const ConfidenceBasis = "certificate-notAfter"

// reasonOf derives the dedup/fingerprint reason from a kind.
func reasonOf(kind string) string { return strings.TrimPrefix(kind, kindPrefix) }

// certManagerGV/certManagerGVR locate the discovery-gated Certificate
// CRs.
var (
	certManagerGV  = schema.GroupVersion{Group: "cert-manager.io", Version: "v1"}
	certManagerGVR = certManagerGV.WithResource("certificates")
)

// Config are the source's thresholds.
type Config struct {
	// Interval between scans (--expiry-interval). Default 1h.
	Interval time.Duration
	// WarnWindow is the warning threshold (--expiry-warn).
	// Default 336h (14 days). Must be >= CriticalWindow.
	WarnWindow time.Duration
	// Namespaces scopes the namespaced LISTs (secrets, service
	// accounts, Certificates) — the --expiry-namespaces flag. Empty =
	// all namespaces. Webhook configurations are cluster-scoped and
	// unaffected.
	Namespaces []string
	// PageSize bounds each LIST page. Default 200.
	PageSize int64
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{
		Interval:   time.Hour,
		WarnWindow: 336 * time.Hour,
		PageSize:   200,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Interval <= 0 {
		c.Interval = d.Interval
	}
	if c.WarnWindow < CriticalWindow {
		c.WarnWindow = d.WarnWindow
	}
	if c.PageSize <= 0 {
		c.PageSize = d.PageSize
	}
	return c
}

// level is the per-object threshold latch position.
type level int

const (
	levelNone level = iota
	levelWarn
	levelCritical
)

func (l level) severity() engine.Severity {
	if l == levelCritical {
		return engine.SeverityCritical
	}
	return engine.SeverityWarning
}

// objEntry is the per-object countdown memory the scan retains — the
// ONLY state kept between scans. No certificate, key, or token bytes.
type objEntry struct {
	kind      string // KindOfObject of the signal
	namespace string
	name      string
	notAfter  time.Time
	fired     level
	// renewedAt is when a fired countdown was observed reset (notAfter
	// moved back out of the warn window) — the §7.4 StableSince.
	renewedAt time.Time
	// seenGen marks the entry as present in the latest scan; entries
	// missing from a scan are deleted (object gone).
	seenGen uint64
}

// finding is one scanned object's current expiry facts, produced by
// the per-target scanners and judged centrally.
type finding struct {
	kind      string // KindOfObject
	namespace string
	name      string
	uid       string
	notAfter  time.Time
	subject   string
	issuer    string
	// detail is extra evidence: which webhook, which ServiceAccount,
	// cert-manager renewal state.
	detail string
	// renewalFailed escalates to critical regardless of window
	// (cert-manager only — the one target with observable renewal
	// state).
	renewalFailed bool
}

// Source implements sources.Source for the expiry row of §7.2.
type Source struct {
	client kubernetes.Interface
	// dyn reads cert-manager Certificates unstructured. Nil disables
	// the cert-manager target (logged at startup).
	dyn dynamic.Interface
	cfg Config

	mu    sync.Mutex
	emit  func(engine.Signal)
	state map[string]*objEntry // keyed by object UID (or synthetic key)
	// synced flips after the first completed scan; the clearance
	// observer declines to judge before that.
	synced bool
	gen    uint64

	// certManager reports whether the Certificate CRD was discovered.
	certManager bool

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// New constructs the source. dyn may be nil (cert-manager scanning
// disabled, logged loudly at startup). Zero-valued cfg fields take
// the shipped defaults.
func New(client kubernetes.Interface, dyn dynamic.Interface, cfg Config) *Source {
	return &Source{
		client: client,
		dyn:    dyn,
		cfg:    cfg.normalize(),
		state:  make(map[string]*objEntry),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: webhook configurations are
// cluster-scoped, so the source needs cluster RBAC (§11).
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// RequiredAccess implements sources.AccessDeclarer (§11).
//
// The secrets grant is the sensitive one — see the §11 note on
// deploy/12-clusterrole-watcher.yaml: this is the first read of
// Secret VALUES the sentinel performs (tls.crt to parse notAfter; the
// token JWT payload for its exp claim). Nothing beyond
// (identity, notAfter, subject CN, issuer CN, exp) survives the scan;
// no secret byte can reach a Signal. Scope it with
// --expiry-namespaces where cluster-wide secret list is not
// acceptable — the declared requirement narrows with the flag, so the
// §11 probe verifies exactly what will be read.
//
// cert-manager Certificates are deliberately NOT declared here: the
// CRD is discovery-gated at runtime, and a static requirement would
// fail startup on every cluster without cert-manager. When the CRD
// exists but the list is forbidden, the scan fails loudly instead
// (same §11 posture, enforced at first scan).
func (s *Source) RequiredAccess() []sources.Requirement {
	namespaces := s.cfg.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}
	var reqs []sources.Requirement
	for _, ns := range namespaces {
		reqs = append(reqs,
			sources.Requirement{Resource: "secrets", Verb: "list", Namespace: ns},
			sources.Requirement{Resource: "serviceaccounts", Verb: "list", Namespace: ns},
		)
	}
	for _, res := range []string{"validatingwebhookconfigurations", "mutatingwebhookconfigurations"} {
		reqs = append(reqs, sources.Requirement{Group: "admissionregistration.k8s.io", Resource: res, Verb: "list"})
	}
	return reqs
}

// ClearanceObserver returns the §7.4 clearance predicate: an expiry
// incident clears when the certificate was renewed (notAfter moved
// back out of the warn window — the per-object latch reset) or the
// object is gone.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return clearance{s} }

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Source) logPrintf(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// Run implements sources.Source: discovery-gates cert-manager, scans
// once immediately (a sentinel that waits a full interval before its
// first look is blind at exactly the moment it was deployed to look),
// then rescans every Interval. The FIRST scan failing is fatal —
// startup honesty, same §11 posture as the RBAC probe; later scan
// errors are logged and retried next interval (a transient API error
// must not kill a resident countdown).
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	s.discoverCertManager()

	if err := s.scan(ctx); err != nil {
		return fmt.Errorf("expiry: initial scan: %w", err)
	}

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.scan(ctx); err != nil {
				s.logPrintf("expiry: scan failed (will retry in %s): %v", s.cfg.Interval, err)
			}
		}
	}
}

// discoverCertManager gates the Certificate target on the CRD's
// presence. Absence is logged ONCE, loudly — never a silent skip.
func (s *Source) discoverCertManager() {
	resources, err := s.client.Discovery().ServerResourcesForGroupVersion(certManagerGV.String())
	found := false
	if err == nil && resources != nil {
		for _, r := range resources.APIResources {
			if r.Name == certManagerGVR.Resource {
				found = true
				break
			}
		}
	}
	switch {
	case !found:
		s.logPrintf("expiry: cert-manager CRD (%s %s) not found — Certificate renewal-state scanning disabled; TLS secrets and webhook CA bundles are still scanned", certManagerGV, certManagerGVR.Resource)
	case s.dyn == nil:
		s.logPrintf("expiry: cert-manager CRD present but no dynamic client — Certificate renewal-state scanning disabled")
	default:
		s.certManager = true
		s.logPrintf("expiry: cert-manager detected — Certificate renewal state included in the scan")
	}
}

// namespaces returns the configured scan scope ("" = all).
func (s *Source) namespaces() []string {
	if len(s.cfg.Namespaces) == 0 {
		return []string{""}
	}
	return s.cfg.Namespaces
}

// scan runs one full pass over every target and judges the findings
// against the per-object threshold latches.
func (s *Source) scan(ctx context.Context) error {
	now := s.clock()
	var findings []finding

	// cert-manager first: the Certificate → Secret attribution set
	// must exist before plain TLS secrets are scanned.
	managedSecrets := map[string]bool{} // "ns/name" of secrets owned by a Certificate
	if s.certManager {
		fs, managed, err := s.scanCertificates(ctx)
		if err != nil {
			return err
		}
		findings = append(findings, fs...)
		managedSecrets = managed
	}

	fs, err := s.scanSecrets(ctx, managedSecrets)
	if err != nil {
		return err
	}
	findings = append(findings, fs...)

	fs, err = s.scanWebhooks(ctx)
	if err != nil {
		return err
	}
	findings = append(findings, fs...)

	s.judge(findings, now)
	return nil
}

// judge applies the threshold latch to every finding, emits the
// crossings, and retires state for objects gone from this scan.
func (s *Source) judge(findings []finding, now time.Time) {
	var out []engine.Signal
	s.mu.Lock()
	s.gen++
	gen := s.gen
	for _, f := range findings {
		lvl := s.levelOf(f, now)
		st, ok := s.state[f.uid]
		if !ok {
			st = &objEntry{}
			s.state[f.uid] = st
		}
		st.kind, st.namespace, st.name = f.kind, f.namespace, f.name
		st.notAfter = f.notAfter
		st.seenGen = gen
		switch {
		case lvl > st.fired:
			st.fired = lvl
			out = append(out, s.signal(f, lvl, now))
		case lvl < st.fired:
			// Renewed (or escalation cause gone): reset the latch so a
			// future crossing fires fresh. Fully out of the warn window
			// is the §7.4 "cert renewed" clearance.
			st.fired = lvl
			if lvl == levelNone {
				st.renewedAt = now
			}
		}
	}
	for uid, st := range s.state {
		if st.seenGen != gen {
			delete(s.state, uid) // object gone — clearance reports object_deleted
		}
	}
	s.synced = true
	emit := s.emit
	s.mu.Unlock()
	if emit == nil {
		return // unit tests drive scan/judge directly
	}
	for _, sig := range out {
		emit(sig)
	}
}

// levelOf computes a finding's threshold level (the §7.2 countdown
// classes; see the package comment for the renewal-failed rule).
func (s *Source) levelOf(f finding, now time.Time) level {
	left := f.notAfter.Sub(now)
	switch {
	case f.renewalFailed:
		return levelCritical
	case left <= CriticalWindow: // includes already-expired
		return levelCritical
	case left <= s.cfg.WarnWindow:
		return levelWarn
	default:
		return levelNone
	}
}

// signal composes one expiry.warning Signal with the §8 Forecast: the
// ETA is the certificate's notAfter — a fact, not a model.
func (s *Source) signal(f finding, lvl level, now time.Time) engine.Signal {
	daysLeft := int(f.notAfter.Sub(now).Hours() / 24)
	var b strings.Builder
	if f.notAfter.Before(now) {
		fmt.Fprintf(&b, "certificate EXPIRED %s ago (notAfter %s)", now.Sub(f.notAfter).Truncate(time.Hour), f.notAfter.UTC().Format(time.RFC3339))
	} else {
		fmt.Fprintf(&b, "certificate expires in %s (notAfter %s)", f.notAfter.Sub(now).Truncate(time.Hour), f.notAfter.UTC().Format(time.RFC3339))
	}
	fmt.Fprintf(&b, "; subject=%s issuer=%s days_left=%d", orDash(f.subject), orDash(f.issuer), daysLeft)
	if f.detail != "" {
		fmt.Fprintf(&b, "; %s", f.detail)
	}
	return engine.Signal{
		Kind:     KindWarning,
		Source:   engine.SourceSentinel,
		Severity: lvl.severity(),
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: f.uid, Reason: reasonOf(KindWarning)},
			Namespace:    f.namespace,
			KindOfObject: f.kind,
			Name:         f.name,
			Message:      b.String(),
			FirstSeen:    now,
			LastSeen:     now,
			Count:        1,
		},
		Forecast: &engine.Forecast{ETA: f.notAfter, ConfidenceBasis: ConfidenceBasis},
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// ---- Target: Secrets (kubernetes.io/tls + service-account tokens) ----

// scanSecrets pages through the two relevant Secret types per scoped
// namespace. Secrets owned by a cert-manager Certificate are skipped
// (attributed to the Certificate, which carries renewal state).
func (s *Source) scanSecrets(ctx context.Context, managed map[string]bool) ([]finding, error) {
	var out []finding
	for _, ns := range s.namespaces() {
		// TLS secrets: tls.crt notAfter.
		err := s.eachSecretPage(ctx, ns, string(corev1.SecretTypeTLS), func(sec *corev1.Secret) {
			if managed[sec.Namespace+"/"+sec.Name] {
				return
			}
			cert := earliestCert(sec.Data[corev1.TLSCertKey])
			if cert == nil {
				return // unparseable — `state edges` reports edge.cert_invalid on the read path
			}
			out = append(out, finding{
				kind:      "Secret",
				namespace: sec.Namespace,
				name:      sec.Name,
				uid:       string(sec.UID),
				notAfter:  cert.NotAfter,
				subject:   subjectOf(cert),
				issuer:    issuerOf(cert),
				detail:    "source=tls-secret",
			})
		})
		if err != nil {
			return nil, err
		}
		// ServiceAccount token secrets: "SA key ages where detectable"
		// — a bound token's JWT carries an exp claim; legacy
		// non-expiring tokens have none and are skipped.
		sas, err := s.serviceAccountNames(ctx, ns)
		if err != nil {
			return nil, err
		}
		err = s.eachSecretPage(ctx, ns, string(corev1.SecretTypeServiceAccountToken), func(sec *corev1.Secret) {
			exp, ok := jwtExpiry(sec.Data[corev1.ServiceAccountTokenKey])
			if !ok {
				return // legacy long-lived token: no exp claim, no countdown
			}
			saName := sec.Annotations[corev1.ServiceAccountNameKey]
			detail := "source=serviceaccount-token"
			if saName != "" {
				detail += " serviceaccount=" + saName
				if !sas[saName] {
					detail += " (serviceaccount missing)"
				}
			}
			out = append(out, finding{
				kind:      "Secret",
				namespace: sec.Namespace,
				name:      sec.Name,
				uid:       string(sec.UID),
				notAfter:  exp,
				detail:    detail,
			})
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// eachSecretPage LISTs secrets of one type in pages, holding at most
// one page in memory (the no-informer rationale in the package
// comment).
func (s *Source) eachSecretPage(ctx context.Context, ns, secretType string, fn func(*corev1.Secret)) error {
	opts := metav1.ListOptions{
		FieldSelector: "type=" + secretType,
		Limit:         s.cfg.PageSize,
	}
	for {
		list, err := s.client.CoreV1().Secrets(ns).List(ctx, opts)
		if err != nil {
			return fmt.Errorf("list %s secrets (namespace %q): %w", secretType, ns, err)
		}
		for i := range list.Items {
			fn(&list.Items[i])
		}
		if list.Continue == "" {
			return nil
		}
		opts.Continue = list.Continue
	}
}

// serviceAccountNames lists the ServiceAccounts in scope so token
// secrets can be attributed (and orphans flagged) in evidence.
func (s *Source) serviceAccountNames(ctx context.Context, ns string) (map[string]bool, error) {
	out := map[string]bool{}
	opts := metav1.ListOptions{Limit: s.cfg.PageSize}
	for {
		list, err := s.client.CoreV1().ServiceAccounts(ns).List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("list serviceaccounts (namespace %q): %w", ns, err)
		}
		for i := range list.Items {
			out[list.Items[i].Name] = true
		}
		if list.Continue == "" {
			return out, nil
		}
		opts.Continue = list.Continue
	}
}

// ---- Target: webhook CA bundles ----

// scanWebhooks checks every Validating/MutatingWebhookConfiguration's
// caBundle certificates; the earliest-expiring cert in a
// configuration is its countdown (one signal per configuration — the
// dedup key is the configuration's UID).
func (s *Source) scanWebhooks(ctx context.Context) ([]finding, error) {
	var out []finding
	vwcs, err := s.client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list validatingwebhookconfigurations: %w", err)
	}
	for i := range vwcs.Items {
		c := &vwcs.Items[i]
		var earliest *x509.Certificate
		hook := ""
		for _, w := range c.Webhooks {
			if cert := earliestCert(w.ClientConfig.CABundle); cert != nil && (earliest == nil || cert.NotAfter.Before(earliest.NotAfter)) {
				earliest, hook = cert, w.Name
			}
		}
		if earliest != nil {
			out = append(out, webhookFinding("ValidatingWebhookConfiguration", c.Name, string(c.UID), hook, earliest))
		}
	}
	mwcs, err := s.client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list mutatingwebhookconfigurations: %w", err)
	}
	for i := range mwcs.Items {
		c := &mwcs.Items[i]
		var earliest *x509.Certificate
		hook := ""
		for _, w := range c.Webhooks {
			if cert := earliestCert(w.ClientConfig.CABundle); cert != nil && (earliest == nil || cert.NotAfter.Before(earliest.NotAfter)) {
				earliest, hook = cert, w.Name
			}
		}
		if earliest != nil {
			out = append(out, webhookFinding("MutatingWebhookConfiguration", c.Name, string(c.UID), hook, earliest))
		}
	}
	return out, nil
}

func webhookFinding(kind, name, uid, hook string, cert *x509.Certificate) finding {
	return finding{
		kind:     kind,
		name:     name,
		uid:      uid,
		notAfter: cert.NotAfter,
		subject:  subjectOf(cert),
		issuer:   issuerOf(cert),
		detail:   "source=webhook-cabundle webhook=" + hook,
	}
}

// ---- Target: cert-manager Certificates (discovery-gated) ----

// scanCertificates reads Certificate CRs unstructured: status.notAfter
// for the countdown, the Ready condition + status.lastFailureTime for
// renewal state. Returns the set of Secrets attributed to
// Certificates so scanSecrets skips them. A forbidden list here IS
// fatal — the CRD exists, so silent skipping would be a coverage lie
// (§11).
func (s *Source) scanCertificates(ctx context.Context) ([]finding, map[string]bool, error) {
	var out []finding
	managed := map[string]bool{}
	for _, ns := range s.namespaces() {
		var ri dynamic.ResourceInterface = s.dyn.Resource(certManagerGVR)
		if ns != "" {
			ri = s.dyn.Resource(certManagerGVR).Namespace(ns)
		}
		opts := metav1.ListOptions{Limit: s.cfg.PageSize}
		for {
			list, err := ri.List(ctx, opts)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return out, managed, nil // CRD deleted between discovery and scan
				}
				return nil, nil, fmt.Errorf("list certificates.cert-manager.io (namespace %q): %w", ns, err)
			}
			for i := range list.Items {
				if f, secret, ok := certificateFinding(&list.Items[i]); ok {
					out = append(out, f)
					if secret != "" {
						managed[f.namespace+"/"+secret] = true
					}
				}
			}
			if list.GetContinue() == "" {
				break
			}
			opts.Continue = list.GetContinue()
		}
	}
	return out, managed, nil
}

// certificateFinding extracts one Certificate CR's countdown facts.
// ok=false when the CR has no usable notAfter yet (never issued and
// never failed — nothing to count down from).
func certificateFinding(u *unstructured.Unstructured) (f finding, secretName string, ok bool) {
	f = finding{
		kind:      "Certificate",
		namespace: u.GetNamespace(),
		name:      u.GetName(),
		uid:       string(u.GetUID()),
	}
	secretName, _, _ = unstructured.NestedString(u.Object, "spec", "secretName")

	notAfterStr, _, _ := unstructured.NestedString(u.Object, "status", "notAfter")
	if t, err := time.Parse(time.RFC3339, notAfterStr); err == nil {
		f.notAfter = t
	}

	ready := ""
	readyDetail := ""
	if conds, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); found {
		for _, c := range conds {
			m, isMap := c.(map[string]any)
			if !isMap {
				continue
			}
			if t, _ := m["type"].(string); t == "Ready" {
				ready, _ = m["status"].(string)
				reason, _ := m["reason"].(string)
				msg, _ := m["message"].(string)
				readyDetail = strings.TrimSpace(reason + " " + msg)
			}
		}
	}
	lastFailure, _, _ := unstructured.NestedString(u.Object, "status", "lastFailureTime")
	f.renewalFailed = lastFailure != "" || ready == "False"

	f.detail = "source=cert-manager renewal="
	switch {
	case f.renewalFailed:
		f.detail += "FAILED"
		if lastFailure != "" {
			f.detail += " last_failure=" + lastFailure
		}
		if readyDetail != "" {
			f.detail += " ready_condition=" + strings.ReplaceAll(readyDetail, " ", "_")
		}
	case ready == "True":
		f.detail += "ok"
	default:
		f.detail += "unknown"
	}
	if secretName != "" {
		f.detail += " secret=" + secretName
	}

	if f.notAfter.IsZero() && !f.renewalFailed {
		return f, secretName, false
	}
	if f.notAfter.IsZero() {
		// Renewal failed with no issued cert: fire on the failure with
		// the epoch as a degenerate "already due" countdown.
		f.notAfter = time.Unix(0, 0).UTC()
	}
	return f, secretName, true
}

// ---- Cert / token parsing (no secret byte survives these) ----

// earliestCert parses every CERTIFICATE block in pemBytes and returns
// the one expiring first (a bundle's countdown is its weakest link).
// Nil when nothing parses. Mirrors the x509 handling of
// pkg/checks/state's cert checks — those helpers are package-internal
// to the read path, so the few lines are restated here rather than
// exporting an internal of an unrelated package.
func earliestCert(pemBytes []byte) *x509.Certificate {
	var earliest *x509.Certificate
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return earliest
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		if earliest == nil || cert.NotAfter.Before(earliest.NotAfter) {
			earliest = cert
		}
	}
}

func subjectOf(cert *x509.Certificate) string {
	if cn := cert.Subject.CommonName; cn != "" {
		return cn
	}
	return cert.Subject.String()
}

func issuerOf(cert *x509.Certificate) string {
	if cn := cert.Issuer.CommonName; cn != "" {
		return cn
	}
	return cert.Issuer.String()
}

// jwtExpiry extracts the exp claim from a JWT without validating it —
// the token is the sentinel's own cluster's, and only the timestamp
// leaves this function.
func jwtExpiry(token []byte) (time.Time, bool) {
	parts := strings.Split(string(token), ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0).UTC(), true
}

// ---- §7.4 clearance ----

// clearance implements engine.ClearanceObserver: an expiry incident
// clears when the object's countdown latch is back at levelNone (the
// cert was renewed past the warn window) or the object is gone.
type clearance struct{ s *Source }

func (c clearance) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	if inc.Key.Reason != reasonOf(KindWarning) {
		return engine.Clearance{}, false
	}
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.synced {
		return engine.Clearance{}, false // no scan yet — cannot judge
	}
	st, ok := s.state[inc.Key.UID]
	if !ok {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	if st.fired == levelNone {
		return engine.Clearance{
			Cleared:     true,
			StableSince: st.renewedAt,
			Resolution:  engine.ResolutionRecovered,
		}, true
	}
	return engine.Clearance{}, true // still inside a threshold
}
