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

package expiry

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// testNow is the fixed clock every test measures thresholds from.
var testNow = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

// certPEM generates a self-signed certificate expiring at notAfter.
// Fixture certs are generated in-test per §13 — no golden key
// material in the repo.
func certPEM(t *testing.T, cn, issuerCN string, notAfter time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    notAfter.Add(-24 * 365 * time.Hour),
		NotAfter:     notAfter,
	}
	// The issuer comes from the PARENT's subject in CreateCertificate;
	// a distinct parent template stamps the wanted issuer CN.
	parent := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: issuerCN},
		NotBefore:    tmpl.NotBefore,
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func tlsSecret(uid, ns, name string, crt []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: ns, Name: name},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: crt},
	}
}

// saTokenSecret builds a service-account token Secret whose token is
// an (unsigned) JWT carrying exp.
func saTokenSecret(t *testing.T, uid, ns, name, sa string, exp time.Time) *corev1.Secret {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"exp": exp.Unix()})
	if err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + ".sig"
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(uid), Namespace: ns, Name: name,
			Annotations: map[string]string{corev1.ServiceAccountNameKey: sa},
		},
		Type: corev1.SecretTypeServiceAccountToken,
		Data: map[string][]byte{corev1.ServiceAccountTokenKey: []byte(token)},
	}
}

// collector gathers emitted signals thread-safely.
type collector struct {
	mu   sync.Mutex
	sigs []engine.Signal
}

func (c *collector) emit(sig engine.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sigs = append(c.sigs, sig)
}

func (c *collector) all() []engine.Signal {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]engine.Signal, len(c.sigs))
	copy(out, c.sigs)
	return out
}

// newTestSource builds a source over fake clients with a settable
// clock, emit wired to a collector, no cert-manager unless enabled by
// the test.
func newTestSource(t *testing.T, cfg Config, objs ...runtime.Object) (*Source, *collector, *time.Time) {
	t.Helper()
	s := New(fake.NewSimpleClientset(objs...), nil, cfg)
	col := &collector{}
	now := testNow
	clock := &now
	s.now = func() time.Time { return *clock }
	s.emit = col.emit
	s.logf = t.Logf
	return s, col, clock
}

// scan runs one scan and fails the test on error.
func scan(t *testing.T, s *Source) {
	t.Helper()
	if err := s.scan(context.Background()); err != nil {
		t.Fatalf("scan: %v", err)
	}
}

// TestTLSSecret_ThresholdsAndSingleFire walks one certificate through
// the countdown: outside the window (silent) → inside 14d (warning,
// once) → inside 72h (critical, once) — with the §8 Forecast on every
// signal.
func TestTLSSecret_ThresholdsAndSingleFire(t *testing.T) {
	t.Parallel()
	notAfter := testNow.Add(20 * 24 * time.Hour) // 20d out
	s, col, clock := newTestSource(t, Config{},
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api.example.com", "corp-ca", notAfter)))

	scan(t, s) // 20d left: outside the 14d warn window
	if got := col.all(); len(got) != 0 {
		t.Fatalf("fired outside every threshold: %+v", got)
	}

	*clock = testNow.Add(10 * 24 * time.Hour) // 10d left → warning
	scan(t, s)
	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %d, want 1 warning", len(sigs))
	}
	sig := sigs[0]
	if sig.Kind != KindWarning || sig.Severity != engine.SeverityWarning {
		t.Errorf("got %q/%q, want expiry.warning at warning", sig.Kind, sig.Severity)
	}
	if sig.Key.UID != "sec-1" || sig.KindOfObject != "Secret" || sig.Namespace != "prod" || sig.Name != "api-tls" {
		t.Errorf("object identity wrong: %+v", sig.TriageEvent)
	}
	if sig.Forecast == nil || !sig.Forecast.ETA.Equal(notAfter) || sig.Forecast.ConfidenceBasis != ConfidenceBasis {
		t.Errorf("Forecast = %+v, want ETA=notAfter basis=%q", sig.Forecast, ConfidenceBasis)
	}
	for _, want := range []string{"subject=api.example.com", "issuer=corp-ca", "days_left=10", "source=tls-secret"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("Message %q missing %q", sig.Message, want)
		}
	}

	// Same level next scan: latched, no re-fire.
	*clock = clock.Add(time.Hour)
	scan(t, s)
	if len(col.all()) != 1 {
		t.Fatalf("re-fired inside the same threshold: %d signals", len(col.all()))
	}

	// Crossing 72h escalates once.
	*clock = testNow.Add(20*24*time.Hour - 48*time.Hour) // 48h left
	scan(t, s)
	sigs = col.all()
	if len(sigs) != 2 {
		t.Fatalf("signals = %d, want warning + critical", len(sigs))
	}
	if sigs[1].Severity != engine.SeverityCritical {
		t.Errorf("Severity = %q, want critical at 48h left", sigs[1].Severity)
	}
	*clock = clock.Add(time.Hour)
	scan(t, s)
	if len(col.all()) != 2 {
		t.Fatal("critical re-fired inside the same threshold")
	}
}

// TestTLSSecret_ExpiredIsCritical: already past notAfter fires
// critical immediately with EXPIRED evidence.
func TestTLSSecret_ExpiredIsCritical(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{},
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(-24*time.Hour))))
	scan(t, s)
	sigs := col.all()
	if len(sigs) != 1 || sigs[0].Severity != engine.SeverityCritical {
		t.Fatalf("signals = %+v, want one critical", sigs)
	}
	if !strings.Contains(sigs[0].Message, "EXPIRED") {
		t.Errorf("Message %q missing EXPIRED", sigs[0].Message)
	}
}

// TestRenewedClears: a renewal (notAfter pushed back out of the warn
// window) resets the latch, the §7.4 observer reports cleared, and a
// later re-crossing fires fresh.
func TestRenewedClears(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(48*time.Hour))))
	s := New(client, nil, Config{})
	col := &collector{}
	now := testNow
	s.now = func() time.Time { return now }
	s.emit = col.emit
	s.logf = t.Logf

	inc := engine.Incident{
		Key: engine.EventKey{UID: "sec-1", Reason: "warning"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Secret", Name: "api-tls"},
	}
	// Before any scan the observer declines to judge.
	if _, ok := s.ClearanceObserver().Clearance(inc); ok {
		t.Fatal("observer judged before the first scan")
	}

	scan(t, s)
	if len(col.all()) != 1 {
		t.Fatalf("setup fire missing: %+v", col.all())
	}
	if verdict, ok := s.ClearanceObserver().Clearance(inc); !ok || verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want judged + NOT cleared inside the window", verdict, ok)
	}

	// Renew: replace the secret's cert with one 90 days out.
	renewed := tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(90*24*time.Hour)))
	if _, err := client.CoreV1().Secrets("prod").Update(context.Background(), renewed, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	now = now.Add(time.Hour)
	scan(t, s)
	if len(col.all()) != 1 {
		t.Fatalf("renewal emitted a signal: %+v", col.all())
	}
	verdict, ok := s.ClearanceObserver().Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered after renewal", verdict, ok)
	}
	if !verdict.StableSince.Equal(now) {
		t.Errorf("StableSince = %v, want the renewal-observed scan time %v", verdict.StableSince, now)
	}

	// Re-crossing much later fires fresh (the latch was reset).
	now = testNow.Add(85 * 24 * time.Hour) // 5d before the renewed notAfter
	scan(t, s)
	if len(col.all()) != 2 {
		t.Fatalf("re-crossing after renewal did not fire: %+v", col.all())
	}
}

// TestObjectGone_ClearsAsDeleted: a secret missing from the next scan
// closes its incident as object_deleted.
func TestObjectGone_ClearsAsDeleted(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(48*time.Hour))))
	s := New(client, nil, Config{})
	col := &collector{}
	s.now = func() time.Time { return testNow }
	s.emit = col.emit
	s.logf = t.Logf
	scan(t, s)
	if len(col.all()) != 1 {
		t.Fatalf("setup fire missing: %+v", col.all())
	}
	if err := client.CoreV1().Secrets("prod").Delete(context.Background(), "api-tls", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete secret: %v", err)
	}
	scan(t, s)
	verdict, ok := s.ClearanceObserver().Clearance(engine.Incident{
		Key: engine.EventKey{UID: "sec-1", Reason: "warning"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Secret", Name: "api-tls"},
	})
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("verdict = %+v ok=%v, want cleared/object_deleted", verdict, ok)
	}
}

// TestWebhookCABundles: both webhook configuration kinds are scanned;
// the earliest-expiring caBundle cert is the configuration's
// countdown and the evidence names the webhook.
func TestWebhookCABundles(t *testing.T) {
	t.Parallel()
	near := certPEM(t, "webhook-ca", "webhook-ca", testNow.Add(5*24*time.Hour))
	far := certPEM(t, "other-ca", "other-ca", testNow.Add(300*24*time.Hour))
	vwc := &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{UID: "vwc-1", Name: "policy-gate"},
		Webhooks: []admissionv1.ValidatingWebhook{
			{Name: "far.example.com", ClientConfig: admissionv1.WebhookClientConfig{CABundle: far}},
			{Name: "near.example.com", ClientConfig: admissionv1.WebhookClientConfig{CABundle: near}},
		},
	}
	mwc := &admissionv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{UID: "mwc-1", Name: "sidecar-injector"},
		Webhooks: []admissionv1.MutatingWebhook{
			{Name: "inject.example.com", ClientConfig: admissionv1.WebhookClientConfig{CABundle: near}},
		},
	}
	s, col, _ := newTestSource(t, Config{}, vwc, mwc)
	scan(t, s)

	sigs := col.all()
	if len(sigs) != 2 {
		t.Fatalf("signals = %d, want one per configuration: %+v", len(sigs), sigs)
	}
	byUID := map[string]engine.Signal{}
	for _, sig := range sigs {
		byUID[sig.Key.UID] = sig
	}
	v, ok := byUID["vwc-1"]
	if !ok || v.KindOfObject != "ValidatingWebhookConfiguration" || v.Name != "policy-gate" {
		t.Fatalf("validating signal wrong: %+v", v)
	}
	if !strings.Contains(v.Message, "webhook=near.example.com") {
		t.Errorf("evidence must name the earliest-expiring webhook: %q", v.Message)
	}
	m, ok := byUID["mwc-1"]
	if !ok || m.KindOfObject != "MutatingWebhookConfiguration" {
		t.Fatalf("mutating signal wrong: %+v", m)
	}
	for _, sig := range sigs {
		if sig.Severity != engine.SeverityWarning { // 5d left: warn, not critical
			t.Errorf("Severity = %q, want warning", sig.Severity)
		}
	}
}

// TestSATokenExpiry: a bound token's JWT exp is a countdown; legacy
// tokens without exp are skipped silently ("where detectable").
func TestSATokenExpiry(t *testing.T) {
	t.Parallel()
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "deployer"}}
	bound := saTokenSecret(t, "tok-1", "prod", "deployer-token", "deployer", testNow.Add(72*time.Hour))
	legacy := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{UID: "tok-2", Namespace: "prod", Name: "legacy-token"},
		Type:       corev1.SecretTypeServiceAccountToken,
		Data:       map[string][]byte{corev1.ServiceAccountTokenKey: []byte("opaque-legacy-token")},
	}
	s, col, _ := newTestSource(t, Config{}, sa, bound, legacy)
	scan(t, s)

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %d, want just the bound token: %+v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.Key.UID != "tok-1" || sig.Severity != engine.SeverityCritical {
		t.Fatalf("got %+v, want tok-1 critical (72h)", sig)
	}
	for _, want := range []string{"source=serviceaccount-token", "serviceaccount=deployer"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("Message %q missing %q", sig.Message, want)
		}
	}
	if strings.Contains(sig.Message, "opaque-legacy-token") || strings.Contains(sig.Message, "eyJ") {
		t.Errorf("token material leaked into evidence: %q", sig.Message)
	}
}

// ---- cert-manager ----

// certManagerListKinds maps the Certificate GVR for the dynamic fake.
var certManagerListKinds = map[schema.GroupVersionResource]string{
	certManagerGVR: "CertificateList",
}

func certificateCR(uid, ns, name, secretName string, notAfter time.Time, ready bool, lastFailure string) *unstructured.Unstructured {
	status := map[string]any{
		"conditions": []any{
			map[string]any{"type": "Ready", "status": map[bool]string{true: "True", false: "False"}[ready], "reason": "Renewal", "message": "renewal attempt"},
		},
	}
	if !notAfter.IsZero() {
		status["notAfter"] = notAfter.UTC().Format(time.RFC3339)
	}
	if lastFailure != "" {
		status["lastFailureTime"] = lastFailure
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata":   map[string]any{"uid": uid, "namespace": ns, "name": name},
		"spec":       map[string]any{"secretName": secretName},
		"status":     status,
	}}
}

// withCertManager installs the CRD into fake discovery and a dynamic
// fake carrying the given Certificates.
func withCertManager(t *testing.T, s *Source, client *fake.Clientset, certs ...runtime.Object) {
	t.Helper()
	client.Resources = []*metav1.APIResourceList{{
		GroupVersion: certManagerGV.String(),
		APIResources: []metav1.APIResource{{Name: "certificates", Kind: "Certificate", Namespaced: true}},
	}}
	scheme := runtime.NewScheme()
	s.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, certManagerListKinds, certs...)
	s.discoverCertManager()
	if !s.certManager {
		t.Fatal("cert-manager not discovered despite CRD present")
	}
}

// TestCertManager_RenewalFailedEscalatesRegardlessOfWindow: the
// design's example — renewal failed makes the Certificate critical
// even when notAfter is far outside every window.
func TestCertManager_RenewalFailedEscalates(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset()
	s := New(client, nil, Config{})
	col := &collector{}
	s.now = func() time.Time { return testNow }
	s.emit = col.emit
	s.logf = t.Logf
	withCertManager(t, s, client,
		certificateCR("cert-1", "prod", "api-cert", "api-tls", testNow.Add(60*24*time.Hour), false, "2026-07-23T00:00:00Z"))

	scan(t, s)
	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %d, want 1: %+v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.KindOfObject != "Certificate" || sig.Severity != engine.SeverityCritical {
		t.Fatalf("got %q/%q, want Certificate critical despite 60d left", sig.KindOfObject, sig.Severity)
	}
	for _, want := range []string{"renewal=FAILED", "last_failure=2026-07-23T00:00:00Z", "source=cert-manager"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("Message %q missing %q", sig.Message, want)
		}
	}
}

// TestCertManager_HealthyOutsideWindowSilent + managed-secret
// attribution: the Certificate covers its Secret, so the plain TLS
// scan skips it (one renewal failure must be one incident, not two).
func TestCertManager_ManagedSecretAttribution(t *testing.T) {
	t.Parallel()
	// The Secret's cert is INSIDE the warn window; the owning
	// Certificate is healthy with the renewed notAfter outside it.
	// Attribution means: no signal at all.
	client := fake.NewSimpleClientset(
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(5*24*time.Hour))))
	s := New(client, nil, Config{})
	col := &collector{}
	s.now = func() time.Time { return testNow }
	s.emit = col.emit
	s.logf = t.Logf
	withCertManager(t, s, client,
		certificateCR("cert-1", "prod", "api-cert", "api-tls", testNow.Add(60*24*time.Hour), true, ""))

	scan(t, s)
	if sigs := col.all(); len(sigs) != 0 {
		t.Fatalf("managed secret double-fired: %+v", sigs)
	}
}

// TestCertManager_AbsentSkipsWithLogLine: no CRD → one loud startup
// line, everything else still scanned — never a silent skip, never a
// crash.
func TestCertManager_AbsentSkipsWithLogLine(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{},
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(48*time.Hour))))
	var logged []string
	s.logf = func(format string, args ...any) { logged = append(logged, fmt.Sprintf(format, args...)) }

	s.discoverCertManager()
	if s.certManager {
		t.Fatal("cert-manager discovered on a cluster without the CRD")
	}
	found := false
	for _, line := range logged {
		if strings.Contains(line, "cert-manager") && strings.Contains(line, "disabled") {
			found = true
		}
	}
	if !found {
		t.Fatalf("absent CRD must be logged loudly, got %v", logged)
	}

	scan(t, s)
	if len(col.all()) != 1 {
		t.Fatalf("TLS scan must still run without cert-manager: %+v", col.all())
	}
}

// TestNamespaceScoping: --expiry-namespaces bounds what is read AND
// what is declared to the §11 probe.
func TestNamespaceScoping(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{Namespaces: []string{"prod"}},
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(48*time.Hour))),
		tlsSecret("sec-2", "dev", "dev-tls", certPEM(t, "dev", "ca", testNow.Add(48*time.Hour))))
	scan(t, s)
	sigs := col.all()
	if len(sigs) != 1 || sigs[0].Key.UID != "sec-1" {
		t.Fatalf("scoped scan read outside prod: %+v", sigs)
	}

	for _, req := range s.RequiredAccess() {
		if (req.Resource == "secrets" || req.Resource == "serviceaccounts") && req.Namespace != "prod" {
			t.Errorf("namespaced requirement not scoped: %v", req)
		}
	}
}

// TestRequiredAccess_DeclaresTheSensitiveRead pins the §11
// declaration — the secrets list is the sentinel's first secret-value
// read and MUST be declared, never assumed.
func TestRequiredAccess_DeclaresTheSensitiveRead(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), nil, Config{})
	want := map[string]bool{
		"list secrets cluster-wide":         true,
		"list serviceaccounts cluster-wide": true,
		"list validatingwebhookconfigurations.admissionregistration.k8s.io cluster-wide": true,
		"list mutatingwebhookconfigurations.admissionregistration.k8s.io cluster-wide":   true,
	}
	got := map[string]bool{}
	for _, req := range s.RequiredAccess() {
		got[req.String()] = true
	}
	for k := range want {
		if !got[k] {
			t.Errorf("missing declared requirement %q", k)
		}
	}
	if len(got) != len(want) {
		t.Errorf("declared %v, want exactly %v", got, want)
	}

	// And the probe fails loudly when denied (§11).
	_, err := sources.Probe(context.Background(), denyReviewer{}, s)
	if err == nil || !strings.Contains(err.Error(), Name) {
		t.Fatalf("Probe = %v, want loud failure naming the source", err)
	}
}

type denyReviewer struct{}

func (denyReviewer) Allowed(context.Context, sources.Requirement) (sources.Decision, error) {
	return sources.Decision{}, nil
}

// TestRun_InitialScanImmediate: Run scans before the first tick and a
// first-scan failure is fatal (startup honesty).
func TestRun_InitialScanImmediate(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{Interval: time.Hour},
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(48*time.Hour))))
	col := &collector{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, col.emit) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(col.all()) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(col.all()) != 1 {
		t.Fatalf("initial scan did not fire: %+v", col.all())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on clean shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestEarliestCert_PicksWeakestLink: multi-cert bundles count from
// the earliest notAfter; garbage blocks are skipped.
func TestEarliestCert_PicksWeakestLink(t *testing.T) {
	t.Parallel()
	near := testNow.Add(24 * time.Hour)
	bundle := append([]byte("junk before\n"), certPEM(t, "far", "ca", testNow.Add(1000*time.Hour))...)
	bundle = append(bundle, certPEM(t, "near", "ca", near)...)
	cert := earliestCert(bundle)
	if cert == nil || !cert.NotAfter.Equal(near) {
		t.Fatalf("earliestCert = %+v, want notAfter %v", cert, near)
	}
	if earliestCert([]byte("not pem at all")) != nil {
		t.Error("garbage parsed as a certificate")
	}
}

// TestKindInventoryFrozen: wire contract (§7.3).
func TestKindInventoryFrozen(t *testing.T) {
	t.Parallel()
	if KindWarning != "expiry.warning" {
		t.Errorf("KindWarning = %q", KindWarning)
	}
	if Name != "expiry" {
		t.Errorf("Name = %q", Name)
	}
	if CriticalWindow != 72*time.Hour {
		t.Errorf("CriticalWindow = %v, want the design-fixed 72h", CriticalWindow)
	}
	if ConfidenceBasis != "certificate-notAfter" {
		t.Errorf("ConfidenceBasis = %q", ConfidenceBasis)
	}
}
