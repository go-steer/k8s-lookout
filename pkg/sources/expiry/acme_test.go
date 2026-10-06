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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// acmeListKinds maps every GVR the ACME tests read for the dynamic
// fake (Certificates too: a stall re-judges its Certificate).
var acmeListKinds = map[schema.GroupVersionResource]string{
	certManagerGVR: "CertificateList",
	challengeGVR:   "ChallengeList",
	orderGVR:       "OrderList",
}

// challengeCR builds a Challenge as cert-manager writes it: owned by
// its Order, carrying the Certificate-name annotation copied down
// from the CertificateRequest.
func challengeCR(uid, ns, name, order, cert, dnsName, state, reason string, created time.Time) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": acmeGV.String(),
		"kind":       "Challenge",
		"metadata": map[string]any{
			"uid": uid, "namespace": ns, "name": name,
			"creationTimestamp": created.UTC().Format(time.RFC3339),
		},
		"spec": map[string]any{"dnsName": dnsName, "type": "DNS-01"},
		"status": map[string]any{
			"state": state, "reason": reason, "presented": true, "processing": true,
		},
	}}
	if cert != "" {
		u.SetAnnotations(map[string]string{certificateNameAnnotation: cert})
	}
	if order != "" {
		u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: acmeGV.String(), Kind: "Order", Name: order, UID: "order-uid"}})
	}
	return u
}

// orderCR builds an Order owned by a CertificateRequest.
func orderCR(uid, ns, name, cert, state, reason string, created time.Time, dnsNames ...string) *unstructured.Unstructured {
	names := make([]any, 0, len(dnsNames))
	for _, n := range dnsNames {
		names = append(names, n)
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": acmeGV.String(),
		"kind":       "Order",
		"metadata": map[string]any{
			"uid": uid, "namespace": ns, "name": name,
			"creationTimestamp": created.UTC().Format(time.RFC3339),
		},
		"spec":   map[string]any{"dnsNames": names},
		"status": map[string]any{"state": state, "reason": reason},
	}}
	if cert != "" {
		u.SetAnnotations(map[string]string{certificateNameAnnotation: cert})
	}
	u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "cert-manager.io/v1", Kind: "CertificateRequest", Name: cert + "-1", UID: "cr-uid"}})
	return u
}

// pendingCert is a Certificate mid-first-issuance: no notAfter yet,
// Ready=False — what cert-manager reports while a Challenge is stuck.
func pendingCert(uid, ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata":   map[string]any{"uid": uid, "namespace": ns, "name": name},
		"spec":       map[string]any{"secretName": name + "-tls"},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "False", "reason": "DoesNotExist", "message": "Issuing certificate as Secret does not exist"},
		}},
	}}
}

// acmeDiscovery serves the cert-manager and ACME groups from fake
// discovery.
func acmeDiscovery(client *fake.Clientset) {
	client.Resources = []*metav1.APIResourceList{
		{
			GroupVersion: certManagerGV.String(),
			APIResources: []metav1.APIResource{{Name: "certificates", Kind: "Certificate", Namespaced: true}},
		},
		{
			GroupVersion: acmeGV.String(),
			APIResources: []metav1.APIResource{
				{Name: "challenges", Kind: "Challenge", Namespaced: true},
				{Name: "orders", Kind: "Order", Namespaced: true},
			},
		},
	}
}

// newACMESource builds a source with cert-manager discovered over a
// dynamic fake seeded with objs, the ACME dimension armed (handlers
// are driven directly), and a settable clock.
func newACMESource(t *testing.T, objs ...runtime.Object) (*Source, *collector, *time.Time) {
	t.Helper()
	client := fake.NewSimpleClientset()
	acmeDiscovery(client)
	s := New(client, nil, Config{})
	s.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), acmeListKinds, objs...)
	col := &collector{}
	now := testNow
	clock := &now
	s.now = func() time.Time { return *clock }
	s.emit = col.emit
	s.logf = t.Logf
	s.discoverCertManager()
	if !s.certManager {
		t.Fatal("cert-manager not discovered despite CRD present")
	}
	s.acmeArmed = true
	return s, col, clock
}

func kindsOf(sigs []engine.Signal) []string {
	out := make([]string, 0, len(sigs))
	for _, s := range sigs {
		out = append(out, s.Kind+"/"+s.KindOfObject)
	}
	return out
}

// TestACME_ChallengePendingPastGraceFiresAfterItsCertificate is the
// headline: a Challenge stuck pending past the grace fires once, with
// its reason, DNS name and owning Certificate — and the Certificate's
// own expiry.warning is emitted FIRST, so the session a stall
// reattaches to exists by the time the stall reaches the watchboard.
func TestACME_ChallengePendingPastGraceFiresAfterItsCertificate(t *testing.T) {
	t.Parallel()
	ch := challengeCR("ch-1", "shop", "web-cert-1-123-456", "web-cert-1-123", "web-cert",
		"shop.unowned.example", "pending", "Waiting for DNS-01 challenge propagation: NS ns1.example. returned REFUSED",
		testNow.Add(-20*time.Minute))
	s, col, _ := newACMESource(t, pendingCert("cert-1", "shop", "web-cert"), ch)
	s.onACME(ch, "Challenge")

	s.sweepACME(context.Background(), testNow)
	sigs := col.all()
	if got := kindsOf(sigs); len(got) != 2 || got[0] != KindWarning+"/Certificate" || got[1] != KindChallengeStuck+"/Challenge" {
		t.Fatalf("signals = %v, want the Certificate's expiry.warning then expiry.challenge_stuck", got)
	}
	sig := sigs[1]
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("severity = %q, want warning (watchboard → reattachment)", sig.Severity)
	}
	if sig.ControllerRef != "Certificate/web-cert" {
		t.Errorf("ControllerRef = %q, want Certificate/web-cert (the §7.7 reattachment key)", sig.ControllerRef)
	}
	if sig.Key.UID != "ch-1" || sig.Key.Reason != "challenge_stuck" || sig.Namespace != "shop" {
		t.Errorf("identity = %+v ns=%q", sig.Key, sig.Namespace)
	}
	for _, want := range []string{"dns_name=shop.unowned.example", "type=DNS-01", "state=pending", `reason="Waiting for DNS-01 challenge propagation`, "certificate=web-cert", "order=web-cert-1-123"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("Message %q missing %q", sig.Message, want)
		}
	}

	// Latched: the next sweep says nothing new.
	s.sweepACME(context.Background(), testNow.Add(time.Minute))
	if n := len(col.all()); n != 2 {
		t.Fatalf("re-fired on the next sweep: %v", kindsOf(col.all()))
	}
}

// TestACME_ChallengeWithinGraceStaysQuiet: a fresh pending Challenge
// is normal issuance, not a stall — until the grace elapses.
func TestACME_ChallengeWithinGraceStaysQuiet(t *testing.T) {
	t.Parallel()
	ch := challengeCR("ch-1", "shop", "c", "o", "web-cert", "shop.example", "pending", "", testNow.Add(-5*time.Minute))
	s, col, _ := newACMESource(t, ch)
	s.onACME(ch, "Challenge")

	s.sweepACME(context.Background(), testNow)
	if sigs := col.all(); len(sigs) != 0 {
		t.Fatalf("fired inside the grace window: %v", kindsOf(sigs))
	}
	s.sweepACME(context.Background(), testNow.Add(11*time.Minute))
	if got := kindsOf(col.all()); len(got) != 1 || got[0] != KindChallengeStuck+"/Challenge" {
		t.Fatalf("after the grace = %v, want one challenge_stuck", got)
	}
}

// TestACME_TerminalChallengeFiresOnObservation: invalid has nothing
// left to wait for — no grace.
func TestACME_TerminalChallengeFiresOnObservation(t *testing.T) {
	t.Parallel()
	ch := challengeCR("ch-1", "shop", "c", "o", "web-cert", "shop.example", "invalid",
		"Error accepting authorization: acme: authorization error for shop.example: 403 urn:ietf:params:acme:error:unauthorized", testNow.Add(-time.Minute))
	s, col, _ := newACMESource(t, ch)
	s.onACME(ch, "Challenge")
	s.sweepACME(context.Background(), testNow)
	sigs := col.all()
	if len(sigs) != 1 || sigs[0].Kind != KindChallengeStuck {
		t.Fatalf("signals = %v, want one challenge_stuck", kindsOf(sigs))
	}
	if !strings.Contains(sigs[0].Message, "failed: state=invalid") || !strings.Contains(sigs[0].Message, "urn:ietf:params:acme:error:unauthorized") {
		t.Errorf("Message %q missing the terminal state and ACME reason", sigs[0].Message)
	}
}

// TestACME_ValidResolvesDeletedResolves: the §7.4 closed loop — a
// Challenge that validated is recovered, one cert-manager cleaned up
// is object_deleted.
func TestACME_ValidResolvesDeletedResolves(t *testing.T) {
	t.Parallel()
	stuck := challengeCR("ch-1", "shop", "c", "o", "web-cert", "shop.example", "pending", "", testNow.Add(-30*time.Minute))
	s, col, _ := newACMESource(t, stuck)
	s.onACME(stuck, "Challenge")
	s.sweepACME(context.Background(), testNow)
	if len(col.all()) != 1 {
		t.Fatalf("setup: %v", kindsOf(col.all()))
	}
	obs := s.ClearanceObserver()
	inc := engine.Incident{Key: engine.EventKey{UID: "ch-1", Reason: "challenge_stuck"}}
	if c, ok := obs.Clearance(inc); !ok || c.Cleared {
		t.Fatalf("still pending: Clearance = %+v, %v; want judged, not cleared", c, ok)
	}

	valid := challengeCR("ch-1", "shop", "c", "o", "web-cert", "shop.example", "valid", "", testNow.Add(-30*time.Minute))
	s.onACME(valid, "Challenge")
	s.sweepACME(context.Background(), testNow.Add(time.Minute))
	c, ok := obs.Clearance(inc)
	if !ok || !c.Cleared || c.Resolution != engine.ResolutionRecovered {
		t.Fatalf("valid: Clearance = %+v, %v; want recovered", c, ok)
	}
	if !c.StableSince.Equal(testNow.Add(time.Minute)) {
		t.Errorf("StableSince = %v, want the sweep that saw it recover", c.StableSince)
	}

	s.onACMEDelete(valid)
	c, ok = obs.Clearance(inc)
	if !ok || !c.Cleared || c.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("deleted: Clearance = %+v, %v; want object_deleted", c, ok)
	}
	if n := len(col.all()); n != 1 {
		t.Errorf("recovery emitted signals: %v", kindsOf(col.all()))
	}
}

// TestACME_ClearanceDeclinesBeforeArmedAndForeignReasons.
func TestACME_ClearanceDeclinesBeforeArmed(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), nil, Config{})
	inc := engine.Incident{Key: engine.EventKey{UID: "ch-1", Reason: "challenge_stuck"}}
	if _, ok := s.ClearanceObserver().Clearance(inc); ok {
		t.Error("judged an ACME incident before the informers synced")
	}
}

// TestACME_OrderRules: a terminal Order fires on observation; a
// pending Order fires only when no Challenge exists for it past the
// grace — a pending Order whose Challenges exist is theirs to report.
func TestACME_OrderRules(t *testing.T) {
	t.Parallel()
	old := testNow.Add(-time.Hour)
	failed := orderCR("o-1", "shop", "failed-order", "a-cert", "errored",
		"Failed to create Order: 429 urn:ietf:params:acme:error:rateLimited", old, "a.example")
	orphan := orderCR("o-2", "shop", "orphan-order", "b-cert", "pending", "", old, "b.example", "www.b.example")
	covered := orderCR("o-3", "shop", "covered-order", "c-cert", "pending", "", old, "c.example")
	ch := challengeCR("ch-3", "shop", "covered-ch", "covered-order", "c-cert", "c.example", "pending", "", testNow.Add(-time.Minute))
	s, col, _ := newACMESource(t)
	for _, o := range []*unstructured.Unstructured{failed, orphan, covered} {
		s.onACME(o, "Order")
	}
	s.onACME(ch, "Challenge")

	s.sweepACME(context.Background(), testNow)
	sigs := col.all()
	byName := map[string]engine.Signal{}
	for _, sig := range sigs {
		byName[sig.Name] = sig
	}
	if len(sigs) != 2 {
		t.Fatalf("signals = %v, want failed-order + orphan-order only", kindsOf(sigs))
	}
	if sig, ok := byName["failed-order"]; !ok || sig.Kind != KindOrderFailed || !strings.Contains(sig.Message, "rateLimited") || sig.ControllerRef != "Certificate/a-cert" {
		t.Errorf("failed-order = %+v", sig)
	}
	if sig, ok := byName["orphan-order"]; !ok || !strings.Contains(sig.Message, "no Challenge created") || !strings.Contains(sig.Message, "dns_names=b.example,www.b.example") {
		t.Errorf("orphan-order = %+v", sig)
	}
	if _, ok := byName["covered-order"]; ok {
		t.Error("a pending Order with a live Challenge fired — one stall reported twice")
	}

	// The orphan's Challenge appears: its incident clears.
	s.onACME(challengeCR("ch-2", "shop", "orphan-ch", "orphan-order", "b-cert", "b.example", "pending", "", testNow), "Challenge")
	c, ok := s.ClearanceObserver().Clearance(engine.Incident{Key: engine.EventKey{UID: "o-2", Reason: "order_failed"}})
	if !ok || !c.Cleared {
		t.Errorf("orphan with a Challenge now: Clearance = %+v, %v; want cleared", c, ok)
	}
}

// TestACME_ChallengeCertificateFallsBackToOrder: a Challenge without
// the annotation borrows its Order's.
func TestACME_ChallengeCertificateFallsBackToOrder(t *testing.T) {
	t.Parallel()
	o := orderCR("o-1", "shop", "the-order", "web-cert", "pending", "", testNow.Add(-time.Hour), "shop.example")
	ch := challengeCR("ch-1", "shop", "c", "the-order", "", "shop.example", "pending", "", testNow.Add(-time.Hour))
	s, col, _ := newACMESource(t)
	s.onACME(o, "Order")
	s.onACME(ch, "Challenge")
	s.sweepACME(context.Background(), testNow)
	sigs := col.all()
	if len(sigs) != 1 || sigs[0].ControllerRef != "Certificate/web-cert" {
		t.Fatalf("signals = %+v, want one challenge_stuck naming Certificate/web-cert", sigs)
	}
}

// TestACME_AbsentCRDAutoSkips: no acme.cert-manager.io group → the
// dimension is off with one log line, never an error, and the rest of
// the source runs.
func TestACME_AbsentCRDAutoSkips(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{},
		tlsSecret("sec-1", "prod", "api-tls", certPEM(t, "api", "ca", testNow.Add(48*time.Hour))))
	var mu sync.Mutex
	var logged []string
	s.logf = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		logged = append(logged, fmt.Sprintf(format, args...))
	}
	if s.startACME(context.Background()) {
		t.Fatal("ACME dimension started on a cluster without the CRDs")
	}
	found := false
	for _, line := range logged {
		if strings.Contains(line, "ACME") && strings.Contains(line, "not found") && strings.Contains(line, "disabled") {
			found = true
		}
	}
	if !found {
		t.Fatalf("absent ACME CRDs must be logged, got %v", logged)
	}
	scan(t, s)
	if len(col.all()) != 1 {
		t.Fatalf("countdowns must still run: %v", kindsOf(col.all()))
	}
}

// TestACME_ForbiddenListDisablesLoudly: CRDs present, grant missing →
// one line naming the grant; the source does not fail.
func TestACME_ForbiddenListDisablesLoudly(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset()
	acmeDiscovery(client)
	s := New(client, nil, Config{})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), acmeListKinds)
	dyn.PrependReactor("list", "challenges", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(challengeGVR.GroupResource(), "", fmt.Errorf("RBAC: denied"))
	})
	s.dyn = dyn
	var logged []string
	s.logf = func(format string, args ...any) { logged = append(logged, fmt.Sprintf(format, args...)) }
	if s.startACME(context.Background()) {
		t.Fatal("ACME dimension started without the list grant")
	}
	if len(logged) == 0 || !strings.Contains(strings.Join(logged, "\n"), "forbidden") || !strings.Contains(strings.Join(logged, "\n"), "acme.cert-manager.io") {
		t.Fatalf("forbidden list must name the missing grant, got %v", logged)
	}
}

// TestACME_InformerEndToEnd drives the real informer path: discovery,
// the grant check, the initial LIST, then a sweep.
func TestACME_InformerEndToEnd(t *testing.T) {
	t.Parallel()
	ch := challengeCR("ch-1", "shop", "c", "o", "web-cert", "shop.example", "pending", "self check failed", testNow.Add(-time.Hour))
	client := fake.NewSimpleClientset()
	acmeDiscovery(client)
	s := New(client, nil, Config{})
	s.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), acmeListKinds, ch)
	col := &collector{}
	s.now = func() time.Time { return testNow }
	s.emit = col.emit
	s.logf = t.Logf
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if !s.startACME(ctx) {
		t.Fatal("ACME dimension did not start with CRDs, grant and a dynamic client")
	}
	s.sweepACME(ctx, testNow)
	if got := kindsOf(col.all()); len(got) != 1 || got[0] != KindChallengeStuck+"/Challenge" {
		t.Fatalf("signals = %v, want one challenge_stuck", got)
	}
}

// TestACME_RequiredAccessOptional: the ACME grants are declared, as
// Optional — denied, the probe returns a note and no error.
func TestACME_RequiredAccessOptional(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), nil, Config{Namespaces: []string{"shop"}})
	var acme []sources.Requirement
	for _, req := range s.RequiredAccess() {
		if req.Group == acmeGV.Group {
			acme = append(acme, req)
		}
	}
	if len(acme) != 4 {
		t.Fatalf("ACME requirements = %v, want list+watch on challenges and orders", acme)
	}
	for _, req := range acme {
		if !req.Optional || req.Namespace != "shop" {
			t.Errorf("requirement %v: want Optional and scoped to --expiry-namespaces", req)
		}
	}
	notes, err := sources.Probe(context.Background(), denyACMEReviewer{}, s)
	if err != nil {
		t.Fatalf("Probe = %v, want the ACME denial to degrade, not fail", err)
	}
	if len(notes) != 4 || !strings.Contains(notes[0], "dimension disabled") {
		t.Errorf("notes = %v, want one per denied ACME requirement", notes)
	}
}

// denyACMEReviewer allows everything but the ACME group.
type denyACMEReviewer struct{}

func (denyACMEReviewer) Allowed(_ context.Context, req sources.Requirement) (sources.Decision, error) {
	return sources.Decision{Allowed: req.Group != acmeGV.Group}, nil
}

// TestACME_KindInventoryFrozen: wire contract (§7.3).
func TestACME_KindInventoryFrozen(t *testing.T) {
	t.Parallel()
	if KindChallengeStuck != "expiry.challenge_stuck" || KindOrderFailed != "expiry.order_failed" {
		t.Errorf("kinds = %q, %q", KindChallengeStuck, KindOrderFailed)
	}
}
