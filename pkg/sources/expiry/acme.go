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

// ACME issuance stalls (issue #542): cert-manager Challenges stuck
// pending or failed, and Orders that failed, surfaced with their own
// reason and DNS names.
//
// Why here and not a new source. The expiry source already owns the
// cert-manager surface (Certificate renewal state, discovery-gated on
// the CRD), and the one thing these kinds are FOR is to say why a
// Certificate is not being issued — the session they belong in is the
// one the Certificate's expiry.warning opened. Living in the same
// source is what lets a stall pull its Certificate's judgment forward
// (rejudgeCertificates below) instead of waiting out --expiry-interval,
// so the Certificate's session exists by the time the stall reaches
// the watchboard and reattaches there.
//
// Why an informer when the rest of this source polls. The no-informer
// rationale in the package comment is about Secrets: the most
// sensitive object class, and the heaviest cache. Challenges and
// Orders are neither — a handful of small, short-lived CRs per
// issuance — and a stall wants minute resolution, not the hourly
// countdown cadence. So they ride a dynamic informer (the gateway
// source's pattern) with a grace sweep on its own ticker.
//
// The firing rules:
//
//   - expiry.challenge_stuck — a Challenge whose status.state is
//     pending/processing (or not yet set) past Config.ACMEGrace, timed
//     from its creationTimestamp; or one in a terminal failure state
//     (invalid/errored/expired), which fires on observation — a
//     terminal state has nothing left to wait for.
//   - expiry.order_failed — an Order in a terminal failure state
//     (invalid/errored/expired), or one still pending past the grace
//     with NO Challenge created for it (cert-manager could not build
//     one — typically no solver matches the DNS name). A pending Order
//     whose Challenges exist is not judged: the Challenges carry the
//     cause, and firing both would be one stall reported twice.
//
// Both are WARNING: they route to the watchboard, which is where §7.7
// ancestor reattachment runs. Each signal names its owning
// Certificate in ControllerRef ("Certificate/<name>"), resolved from
// the cert-manager.io/certificate-name annotation cert-manager copies
// from the CertificateRequest onto its Order and Challenges (falling
// back to the owning Order's annotation for a Challenge). The
// dispatcher's reattachment stage keys a Certificate's incident and a
// stall naming that Certificate on the same signal-declared ancestor,
// so the stall lands in the Certificate's session as a
// kind=family.member followup rather than in a digest.
//
// Discovery-gated and grant-optional. Absent CRDs skip the dimension
// with one log line (never an error: cert-manager is optional
// software). The list/watch grants are declared Optional — a
// deployment applying an older ClusterRole still starts, says which
// grant is missing, and runs everything else.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Signal kinds for ACME issuance stalls. APPEND-ONLY, like every
// §7.3 kind: playbooks and fleet consumers match on them.
const (
	// KindChallengeStuck: an ACME Challenge sat pending past the grace
	// window, or reached a terminal failure state.
	KindChallengeStuck = kindPrefix + "challenge_stuck"
	// KindOrderFailed: an ACME Order reached a terminal failure state,
	// or stayed pending past the grace window with no Challenge.
	KindOrderFailed = kindPrefix + "order_failed"
)

// certificateNameAnnotation is the annotation cert-manager stamps on a
// CertificateRequest naming its Certificate; the ACME issuer copies a
// request's annotations onto its Order, and the Order's onto each
// Challenge.
const certificateNameAnnotation = "cert-manager.io/certificate-name"

// maxACMEReason bounds the ACME server's reason text carried in a
// message — it is free text from outside the cluster.
const maxACMEReason = 512

// acmeSyncTimeout bounds the wait for the ACME informers' initial
// LIST. A dimension that cannot sync is reported and disabled; it must
// not hold the rest of the source hostage.
const acmeSyncTimeout = 2 * time.Minute

var (
	acmeGV       = schema.GroupVersion{Group: "acme.cert-manager.io", Version: "v1"}
	challengeGVR = acmeGV.WithResource("challenges")
	orderGVR     = acmeGV.WithResource("orders")
)

// acmeEntry is the per-object memory for one Challenge or Order.
// Healthy objects are kept too: an Order's "no Challenge exists"
// judgment counts the Challenges that name it.
type acmeEntry struct {
	kindOf      string // "Challenge" | "Order"
	namespace   string
	name        string
	uid         string
	certificate string // owning Certificate ("" = unresolved)
	order       string // Challenge only: the owning Order's name
	created     time.Time

	// state is status.state as observed ("" = not yet set).
	state string
	// failing: the object currently meets its kind's failure predicate
	// (grace aside). For an Order in a non-terminal state this is
	// decided at sweep time, because it depends on the Challenge set.
	failing bool
	// terminal: a failure state with nothing left to wait for — fires
	// without the grace window.
	terminal bool
	// pendingOrder marks an Order still pending; it fails only while no
	// Challenge names it.
	pendingOrder bool
	// detail is the human evidence: DNS names, type, reason.
	detail string

	fired       bool
	recoveredAt time.Time
}

// signalKind is the kind this entry fires as.
func (e *acmeEntry) signalKind() string {
	if e.kindOf == "Order" {
		return KindOrderFailed
	}
	return KindChallengeStuck
}

// acmeTerminalFailure reports whether a cert-manager ACME state is a
// terminal failure (cert-manager's acme State enum).
func acmeTerminalFailure(state string) bool {
	switch state {
	case "invalid", "errored", "expired":
		return true
	}
	return false
}

// acmeInProgress reports whether a state is still in flight.
func acmeInProgress(state string) bool {
	switch state {
	case "", "pending", "processing":
		return true
	}
	return false
}

// acmeServed reports which of challenges/orders the
// acme.cert-manager.io/v1 group serves. A discovery error (group
// absent) reads as neither.
func (s *Source) acmeServed() (challenges, orders bool) {
	resources, err := s.client.Discovery().ServerResourcesForGroupVersion(acmeGV.String())
	if err != nil || resources == nil {
		return false, false
	}
	for _, r := range resources.APIResources {
		switch r.Name {
		case challengeGVR.Resource:
			challenges = true
		case orderGVR.Resource:
			orders = true
		}
	}
	return challenges, orders
}

// acmeRequirements are the Optional §11 grants the ACME dimension
// reads through, per scoped namespace.
func (s *Source) acmeRequirements() []sources.Requirement {
	var reqs []sources.Requirement
	for _, ns := range s.namespaces() {
		for _, res := range []string{challengeGVR.Resource, orderGVR.Resource} {
			for _, verb := range []string{"list", "watch"} {
				reqs = append(reqs, sources.Requirement{
					Group: acmeGV.Group, Resource: res, Verb: verb, Namespace: ns, Optional: true,
				})
			}
		}
	}
	return reqs
}

// startACME gates the ACME dimension (CRDs served, dynamic client
// present, list allowed), starts its informers and waits for their
// initial sync. It returns false — with one log line saying what is
// not watched and why — whenever the dimension cannot run; that is
// never an error for the source as a whole.
func (s *Source) startACME(ctx context.Context) bool {
	watchCh, watchOrd := s.acmeServed()
	if !watchCh && !watchOrd {
		s.logPrintf("expiry: cert-manager ACME CRDs (%s challenges, orders) not found — Challenge/Order stall detection disabled", acmeGV)
		return false
	}
	if s.dyn == nil {
		s.logPrintf("expiry: cert-manager ACME CRDs present but no dynamic client — Challenge/Order stall detection disabled")
		return false
	}
	type target struct {
		gvr    schema.GroupVersionResource
		kindOf string
	}
	var targets []target
	if watchCh {
		targets = append(targets, target{challengeGVR, "Challenge"})
	} else {
		s.logPrintf("expiry: %s not served — Challenge stall detection disabled (Orders still watched)", challengeGVR)
	}
	if watchOrd {
		targets = append(targets, target{orderGVR, "Order"})
	} else {
		s.logPrintf("expiry: %s not served — Order failure detection disabled (Challenges still watched)", orderGVR)
	}
	// A cheap LIST per target and namespace answers the grant question
	// before an informer would retry a 403 forever (§11: never a
	// silently empty watch).
	for _, t := range targets {
		for _, ns := range s.namespaces() {
			var err error
			if ns == "" {
				_, err = s.dyn.Resource(t.gvr).List(ctx, metav1.ListOptions{Limit: 1})
			} else {
				_, err = s.dyn.Resource(t.gvr).Namespace(ns).List(ctx, metav1.ListOptions{Limit: 1})
			}
			switch {
			case err == nil:
			case apierrors.IsForbidden(err):
				s.logPrintf("expiry: list %s.%s forbidden (namespace %q) — Challenge/Order stall detection disabled; grant list+watch on challenges,orders in %s (deploy/12-clusterrole-watcher.yaml)",
					t.gvr.Resource, acmeGV.Group, ns, acmeGV.Group)
				return false
			default:
				s.logPrintf("expiry: list %s.%s (namespace %q) failed (%v) — Challenge/Order stall detection disabled",
					t.gvr.Resource, acmeGV.Group, ns, err)
				return false
			}
		}
	}

	var synced []cache.InformerSynced
	for _, ns := range s.namespaces() {
		factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.dyn, 0, ns, nil)
		for _, t := range targets {
			kindOf := t.kindOf
			h, err := factory.ForResource(t.gvr).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    func(obj any) { s.onACME(obj, kindOf) },
				UpdateFunc: func(_, obj any) { s.onACME(obj, kindOf) },
				DeleteFunc: func(obj any) { s.onACMEDelete(obj) },
			})
			if err != nil {
				s.logPrintf("expiry: register %s handler: %v — Challenge/Order stall detection disabled", t.gvr.Resource, err)
				return false
			}
			synced = append(synced, h.HasSynced)
		}
		factory.Start(ctx.Done())
		go func() {
			<-ctx.Done()
			factory.Shutdown()
		}()
	}
	syncCtx, cancel := context.WithTimeout(ctx, acmeSyncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), synced...) {
		if ctx.Err() == nil {
			s.logPrintf("expiry: ACME informers did not sync within %s — Challenge/Order stall detection disabled", acmeSyncTimeout)
		}
		return false
	}
	s.mu.Lock()
	s.acmeArmed = true
	s.mu.Unlock()
	s.logPrintf("expiry: cert-manager ACME detected — Challenges stuck past %s and failed Orders included", s.cfg.ACMEGrace)
	return true
}

// onACME records one Challenge/Order's current facts.
func (s *Source) onACME(obj any, kindOf string) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	now := s.clock()
	fresh := evaluateACME(u, kindOf, now)

	s.mu.Lock()
	defer s.mu.Unlock()
	cur, seen := s.acme[u.GetUID()]
	if seen {
		fresh.fired = cur.fired
		fresh.recoveredAt = cur.recoveredAt
		if fresh.certificate == "" {
			fresh.certificate = cur.certificate
		}
	}
	s.acme[u.GetUID()] = fresh
}

// onACMEDelete drops an object's state. A bound incident on a deleted
// object resolves through the clearance predicate's object-gone
// branch — cert-manager deletes an Order's Challenges once the Order
// is final, so a stuck Challenge that finally validated usually
// resolves this way.
func (s *Source) onACMEDelete(obj any) {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = t.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	s.mu.Lock()
	delete(s.acme, u.GetUID())
	s.mu.Unlock()
}

// evaluateACME extracts one Challenge/Order's facts.
func evaluateACME(u *unstructured.Unstructured, kindOf string, now time.Time) *acmeEntry {
	e := &acmeEntry{
		kindOf:      kindOf,
		namespace:   u.GetNamespace(),
		name:        u.GetName(),
		uid:         string(u.GetUID()),
		certificate: u.GetAnnotations()[certificateNameAnnotation],
		created:     u.GetCreationTimestamp().Time,
	}
	if e.created.IsZero() {
		e.created = now
	}
	e.state, _, _ = unstructured.NestedString(u.Object, "status", "state")
	reason, _, _ := unstructured.NestedString(u.Object, "status", "reason")
	reason = strings.Join(strings.Fields(reason), " ")
	if len(reason) > maxACMEReason {
		reason = reason[:maxACMEReason] + "…"
	}
	deleting := u.GetDeletionTimestamp() != nil

	var b strings.Builder
	switch kindOf {
	case "Challenge":
		for _, ref := range u.GetOwnerReferences() {
			if ref.Kind == "Order" {
				e.order = ref.Name
			}
		}
		dnsName, _, _ := unstructured.NestedString(u.Object, "spec", "dnsName")
		typ, _, _ := unstructured.NestedString(u.Object, "spec", "type")
		wildcard, _, _ := unstructured.NestedBool(u.Object, "spec", "wildcard")
		presented, _, _ := unstructured.NestedBool(u.Object, "status", "presented")
		fmt.Fprintf(&b, "dns_name=%s type=%s", orDash(dnsName), orDash(typ))
		if wildcard {
			b.WriteString(" wildcard=true")
		}
		fmt.Fprintf(&b, " presented=%t", presented)
		// A Challenge being deleted is cert-manager cleaning up after
		// its Order went final — not a stall.
		e.failing = !deleting && (acmeInProgress(e.state) || acmeTerminalFailure(e.state))
		e.terminal = acmeTerminalFailure(e.state)
	case "Order":
		names, _, _ := unstructured.NestedStringSlice(u.Object, "spec", "dnsNames")
		if cn, _, _ := unstructured.NestedString(u.Object, "spec", "commonName"); cn != "" && len(names) == 0 {
			names = []string{cn}
		}
		fmt.Fprintf(&b, "dns_names=%s", orDash(strings.Join(names, ",")))
		for _, ref := range u.GetOwnerReferences() {
			if ref.Kind == "CertificateRequest" {
				fmt.Fprintf(&b, " certificaterequest=%s", ref.Name)
			}
		}
		e.terminal = !deleting && acmeTerminalFailure(e.state)
		e.failing = e.terminal
		e.pendingOrder = !deleting && acmeInProgress(e.state)
	}
	if reason != "" {
		fmt.Fprintf(&b, " reason=%q", reason)
	}
	e.detail = b.String()
	return e
}

// challengesByOrderLocked counts the live Challenges naming each
// Order ("ns/name"). Called under s.mu.
func (s *Source) challengesByOrderLocked() map[string]int {
	out := map[string]int{}
	for _, e := range s.acme {
		if e.kindOf == "Challenge" && e.order != "" {
			out[e.namespace+"/"+e.order]++
		}
	}
	return out
}

// ordersByNameLocked indexes Orders by "ns/name", for a Challenge's
// Certificate fallback. Called under s.mu.
func (s *Source) ordersByNameLocked() map[string]*acmeEntry {
	out := map[string]*acmeEntry{}
	for _, e := range s.acme {
		if e.kindOf == "Order" {
			out[e.namespace+"/"+e.name] = e
		}
	}
	return out
}

// failingNowLocked is the entry's failure predicate, grace aside. A
// pending Order fails only while no Challenge names it.
func failingNowLocked(e *acmeEntry, challenges map[string]int) bool {
	if e.kindOf == "Order" && e.pendingOrder {
		return challenges[e.namespace+"/"+e.name] == 0
	}
	return e.failing
}

// acmeFireableLocked returns the entries that cross their firing rule
// now, latching each, and records recoveries for entries no longer
// failing. Called under s.mu.
func (s *Source) acmeFireableLocked(now time.Time) []*acmeEntry {
	if !s.acmeArmed {
		return nil
	}
	challenges := s.challengesByOrderLocked()
	orders := s.ordersByNameLocked()
	uids := make([]types.UID, 0, len(s.acme))
	for uid := range s.acme {
		uids = append(uids, uid)
	}
	sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
	var out []*acmeEntry
	for _, uid := range uids {
		e := s.acme[uid]
		if e.kindOf == "Challenge" && e.certificate == "" && e.order != "" {
			if o := orders[e.namespace+"/"+e.order]; o != nil {
				e.certificate = o.certificate
			}
		}
		if !failingNowLocked(e, challenges) {
			if e.fired {
				e.fired = false
				e.recoveredAt = now
			}
			continue
		}
		if e.fired {
			continue
		}
		if !e.terminal && now.Sub(e.created) < s.cfg.ACMEGrace {
			continue // still inside the grace window
		}
		e.fired = true
		e.recoveredAt = time.Time{}
		cp := *e
		out = append(out, &cp)
	}
	return out
}

// sweepACME is the ACME ticker body: find the crossings, bring their
// Certificates' judgment up to date first, then emit the stalls.
func (s *Source) sweepACME(ctx context.Context, now time.Time) {
	s.mu.Lock()
	fire := s.acmeFireableLocked(now)
	s.mu.Unlock()
	if len(fire) == 0 {
		return
	}
	s.rejudgeCertificates(ctx, fire, now)
	sigs := make([]engine.Signal, 0, len(fire))
	for _, e := range fire {
		sigs = append(sigs, s.acmeSignal(e, now))
	}
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit == nil {
		return
	}
	for _, sig := range sigs {
		emit(sig)
	}
}

// rejudgeCertificates re-reads the Certificates in each stall's
// namespace and runs them through the threshold latch WITHOUT
// retiring anything else — a stalled issuance is exactly a Certificate
// that is Ready=False, and its expiry.warning must open the session
// BEFORE the stall reaches the watchboard, or there is nothing to
// reattach to until the next --expiry-interval scan. Already-latched
// Certificates do not re-fire. Best effort: a failed read is logged
// and the stall is still emitted.
func (s *Source) rejudgeCertificates(ctx context.Context, fire []*acmeEntry, now time.Time) {
	if !s.certManager {
		return
	}
	namespaces := map[string]bool{}
	for _, e := range fire {
		if e.certificate != "" {
			namespaces[e.namespace] = true
		}
	}
	if len(namespaces) == 0 {
		return
	}
	nsList := make([]string, 0, len(namespaces))
	for ns := range namespaces {
		nsList = append(nsList, ns)
	}
	sort.Strings(nsList)
	var findings []finding
	for _, ns := range nsList {
		fs, _, err := s.listCertificates(ctx, ns)
		if err != nil {
			s.logPrintf("expiry: re-read Certificates in %q for an ACME stall failed (%v) — the stall is still reported", ns, err)
			continue
		}
		findings = append(findings, fs...)
	}
	s.judgeWith(findings, now, false)
}

// acmeSignal composes one stall Signal.
func (s *Source) acmeSignal(e *acmeEntry, now time.Time) engine.Signal {
	kind := e.signalKind()
	var b strings.Builder
	state := e.state
	if state == "" {
		state = "unset"
	}
	switch {
	case e.terminal:
		fmt.Fprintf(&b, "ACME %s %s failed: state=%s; %s", e.kindOf, e.name, state, e.detail)
	case e.kindOf == "Order":
		fmt.Fprintf(&b, "ACME Order %s stuck state=%s with no Challenge created for %s past the %s grace window (cert-manager could not build one — check the issuer's solvers match these names); %s",
			e.name, state, now.Sub(e.created).Truncate(time.Second), s.cfg.ACMEGrace, e.detail)
	default:
		fmt.Fprintf(&b, "ACME Challenge %s stuck state=%s for %s past the %s grace window; %s",
			e.name, state, now.Sub(e.created).Truncate(time.Second), s.cfg.ACMEGrace, e.detail)
	}
	if e.certificate != "" {
		fmt.Fprintf(&b, "; certificate=%s", e.certificate)
	}
	if e.order != "" {
		fmt.Fprintf(&b, " order=%s", e.order)
	}
	sig := engine.Signal{
		Kind:     kind,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: e.uid, Reason: reasonOf(kind)},
			Namespace:    e.namespace,
			KindOfObject: e.kindOf,
			Name:         e.name,
			Message:      b.String(),
			FirstSeen:    e.created,
			LastSeen:     now,
			Count:        1,
		},
	}
	if e.certificate != "" {
		// The §7.7 reattachment key: the dispatcher joins this stall to
		// the live incident on the same Certificate.
		sig.ControllerRef = "Certificate/" + e.certificate
	}
	return sig
}

// acmeClearance is the §7.4 predicate for the ACME kinds: cleared when
// the object no longer meets its failure rule (a Challenge validated,
// an Order's Challenges appeared or it went valid) or it is gone.
func (s *Source) acmeClearance(inc engine.Incident) (engine.Clearance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.acmeArmed {
		return engine.Clearance{}, false // never synced — cannot judge
	}
	e, ok := s.acme[types.UID(inc.Key.UID)]
	if !ok {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	if failingNowLocked(e, s.challengesByOrderLocked()) {
		return engine.Clearance{}, true // still stalled
	}
	return engine.Clearance{
		Cleared:     true,
		StableSince: e.recoveredAt, // zero = recovered as of this observation
		Resolution:  engine.ResolutionRecovered,
	}, true
}
