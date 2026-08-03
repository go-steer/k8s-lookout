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

// Package gateway is the Gateway API health signal source (post-M5
// roadmap C.5, issue #168): the Gateway-API sibling of the ingress
// source (#135). GKE guidance steers most users to the Gateway API,
// not the classic ingress-gce Ingress — yet Gateway programming
// failures never surface as the ingress-gce `Sync`/`Translate` events
// that source keys on. They surface as STATUS CONDITIONS on the
// `Gateway` and `HTTPRoute` objects: `Programmed`, `Accepted`, and
// `ResolvedRefs` reading `False`. This source is the only in-cluster
// evidence that a Gateway-fronted load balancer is not being
// programmed, or that a route config was rejected.
//
// Pairing with the ingress source: GKE Gateway uses container-native
// LB with the SAME NEGs and NEG controller as Ingress, so the ingress
// source's `ingress.neg_failed` (a Service-scoped NEG-controller
// failure) already fires for Gateway-backed Services — no duplication.
// This source adds only the Gateway-native half the NEG signal cannot
// see: the frontend/data-plane programming and route-attachment
// conditions.
//
// Client model — dynamic/unstructured, no new dependency. The
// Gateway API types (`sigs.k8s.io/gateway-api`) are deliberately NOT a
// go.mod dependency: like the expiry source reading cert-manager
// Certificates, this source watches the CRs unstructured through a
// dynamic informer (pkg/sources/expiry precedent), and gates on the
// CRD's presence via discovery (pkg/checks/spec.go precedent). A
// cluster without the Gateway API installed never enables the source
// under --sources=auto (the discovery gate in internal/watch/auto.go),
// and an explicit --sources=…,gateway on such a cluster fails loudly
// at startup (§11) rather than watching an empty stream.
//
// Firing rule — sustained failure, not transient provisioning. A
// GKE Gateway legitimately sits at `Programmed=False` for MINUTES
// while the load balancer provisions; firing on every fresh Gateway
// would be pure noise. A condition counts as a failure only when ALL
// of:
//
//   - status == False for `Programmed` / `Accepted` / `ResolvedRefs`
//     (Conflicted and other condition types are out of scope for v1);
//   - reason != "Pending" — the Gateway API's standard in-progress
//     reason, distinguishing "still working" from a real failure
//     (Invalid, NoResources, AddressNotAssigned, NoMatchingParent,
//     BackendNotFound, …);
//   - the condition's observedGeneration (when present) matches the
//     object's metadata.generation — the controller has caught up to
//     the current spec, so this is not a stale condition mid-reconcile;
//   - the failure has been sustained for at least Config.Grace, timed
//     from the condition's own lastTransitionTime — so a long-broken
//     Gateway fires immediately on observation while a mid-provisioning
//     one waits out the grace window.
//
// Level, not edge: unlike object-state's transition discipline, this
// source reports a currently-broken STATE (the expiry-countdown
// stance). A pre-existing sustained failure present at the initial
// LIST is reported once armed — an ongoing outage is worth saying even
// if the sentinel did not witness its onset — and the engine's
// persisted dedup absorbs the restart replay.
//
// Kinds (§7.3, APPEND-ONLY), both WARNING — the watchboard posture:
// programming failures repeat on every controller requeue while
// broken (the digest batches them), and false criticals erode trust:
//
//   - gateway.programming_failed — a Gateway (top-level or any
//     listener) reports `Programmed=False`: the controller accepted the
//     spec but could not program the load balancer / data plane. The
//     Gateway-API analog of ingress.sync_failed, and the piece the NEG
//     half does not cover.
//   - gateway.route_rejected — the config/routes were rejected: an
//     HTTPRoute parent reports `Accepted=False` or `ResolvedRefs=False`,
//     or a Gateway/listener reports `Accepted=False`/`ResolvedRefs=False`
//     — the config never became routable (bad backendRef, no matching
//     parent, unsupported value). The analog of ingress.translate_failed.
//
// One incident per (object, kind): a Gateway whose two listeners are
// both unprogrammed is ONE gateway.programming_failed on that Gateway
// (the message names the listeners); a Gateway that is both
// unprogrammed and unaccepted is two incidents (distinct reasons).
//
// §7.4 clearance: a ClearanceObserver resolves an incident when the
// failing condition returns (object healthy) or the object is gone —
// the same closed loop as expiry/object-state.
package gateway

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "gateway"

// kindPrefix namespaces this source's signal kinds (§7.3).
const kindPrefix = "gateway."

// Signal kinds emitted by this source. APPEND-ONLY: kinds are part of
// the signal schema playbooks and fleet consumers match on — never
// rename or reuse one. The dedup/fingerprint reason for each is the
// kind suffix.
const (
	// KindProgrammingFailed: a Gateway (top-level or any listener)
	// reports Programmed=False, sustained — the controller could not
	// program the load balancer / data plane. Analog of
	// ingress.sync_failed.
	KindProgrammingFailed = kindPrefix + "programming_failed"
	// KindRouteRejected: a routing/config attachment was rejected,
	// sustained — an HTTPRoute parent Accepted=False/ResolvedRefs=False,
	// or a Gateway/listener Accepted=False/ResolvedRefs=False. The
	// config never became routable. Analog of ingress.translate_failed.
	KindRouteRejected = kindPrefix + "route_rejected"
)

// GroupVersion / resources of the Gateway API GA surface this source
// watches. v1 is what GKE Gateway serves at GA; v1beta1/v1alpha2 are
// out of scope for v1 of this source.
var (
	gatewayGV    = schema.GroupVersion{Group: "gateway.networking.k8s.io", Version: "v1"}
	gatewayGVR   = gatewayGV.WithResource("gateways")
	httprouteGVR = gatewayGV.WithResource("httproutes")
)

// pendingReason is the Gateway API's standard in-progress reason: a
// condition False FOR THIS reason is the controller still working, not
// a failure. Gating it out is what keeps a mid-provisioning Gateway
// from firing before its grace window even elapses.
const pendingReason = "Pending"

// reasonOf derives the dedup/fingerprint reason from a kind.
func reasonOf(kind string) string { return strings.TrimPrefix(kind, kindPrefix) }

// Config are the source's thresholds. Zero values take DefaultConfig.
type Config struct {
	// Grace is how long a failing condition must be sustained (timed
	// from its lastTransitionTime) before it fires — the window that
	// absorbs normal LB provisioning latency. Default 5m.
	Grace time.Duration
	// TickInterval drives the sweep that re-evaluates grace crossings
	// (a condition that went False and then produced no further
	// informer updates still fires when its grace elapses) and prunes
	// TTL-expired state. Default 30s.
	TickInterval time.Duration
	// StateTTL bounds the per-object memory: entries not refreshed by
	// any informer activity within this window are dropped (safety net
	// behind DeleteFunc). Default 24h.
	StateTTL time.Duration
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{
		Grace:        5 * time.Minute,
		TickInterval: 30 * time.Second,
		StateTTL:     24 * time.Hour,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Grace <= 0 {
		c.Grace = d.Grace
	}
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	if c.StateTTL <= 0 {
		c.StateTTL = d.StateTTL
	}
	return c
}

// failure is one object's currently-failing condition family for a
// single kind (all listeners/parents contributing to that kind folded
// together — one incident per object+kind).
type failure struct {
	// since is the earliest lastTransitionTime among the contributing
	// conditions: the failure has held at least since here. Grace is
	// measured from it.
	since time.Time
	// detail is the human evidence (which listeners/parents, the
	// reasons and messages), aggregated and deterministic.
	detail string
	// fired latches one emission per failure episode; cleared when the
	// kind stops failing.
	fired bool
}

// objState is the per-object memory: the failing kinds now, plus the
// recovery instants used by the §7.4 clearance predicate.
type objState struct {
	kindOf    string // "Gateway" | "HTTPRoute"
	namespace string
	name      string
	uid       string
	// failing maps kind → the active failure for it (absent = healthy).
	failing map[string]*failure
	// recovered maps kind → when it was last observed to stop failing,
	// the clearance StableSince. Retained after failing empties so a
	// bound incident can resolve.
	recovered map[string]time.Time
	lastSeen  time.Time
}

// Source implements sources.Source for the gateway row of §7.2.
type Source struct {
	client kubernetes.Interface
	dyn    dynamic.Interface
	cfg    Config

	mu   sync.Mutex
	emit func(engine.Signal)
	// armed flips true after every informer cache syncs. Handlers
	// always record state; they emit only when armed, so the initial
	// LIST rebuilds memory before firing (a level signal still fires
	// for a pre-existing sustained failure once armed — see the package
	// comment).
	armed bool
	// synced flips with armed; the clearance observer declines to judge
	// before it.
	synced bool
	state  map[types.UID]*objState

	// watchGateways / watchRoutes record which CRDs discovery found;
	// Run watches each present resource and logs any absent one.
	watchGateways bool
	watchRoutes   bool

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// New constructs the source. dyn is the dynamic client the informers
// read through; client supplies discovery for the CRD gate. Zero-valued
// cfg fields take the shipped defaults.
func New(client kubernetes.Interface, dyn dynamic.Interface, cfg Config) *Source {
	return &Source{
		client: client,
		dyn:    dyn,
		cfg:    cfg.normalize(),
		state:  make(map[types.UID]*objState),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: Gateways and HTTPRoutes live in any
// namespace, so the informers watch cluster-wide and the source needs
// cluster RBAC (§11).
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// RequiredAccess implements sources.AccessDeclarer (§11): list+watch on
// the two Gateway API resources. Matches deploy/12-clusterrole-watcher.yaml.
// Like the expiry source's cert-manager grant, these rules are inert on
// clusters without the Gateway API CRDs (RBAC for absent groups is
// legal and ignored), so the SSAR probe passes wherever the ClusterRole
// is applied — the CRD-presence gate in auto.go is what actually
// decides auto-enable.
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, res := range []string{gatewayGVR.Resource, httprouteGVR.Resource} {
		for _, verb := range []string{"list", "watch"} {
			reqs = append(reqs, sources.Requirement{Group: gatewayGV.Group, Resource: res, Verb: verb})
		}
	}
	return reqs
}

// ClearanceObserver returns the §7.4 clearance predicate.
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

// GatewayAPIServed reports whether the Gateway API v1 group serves at
// least one of the resources this source watches. It is the discovery
// gate internal/watch/auto.go gives resolveSourcesAuto so a cluster
// without the Gateway API installed skips the source instead of
// enabling a watch that would fail loudly at startup.
func GatewayAPIServed(client kubernetes.Interface) bool {
	gw, hr := discoverResources(client)
	return gw || hr
}

// discoverResources asks discovery which of gateways/httproutes the
// gateway.networking.k8s.io/v1 group serves. A discovery error (group
// absent) reads as "neither served".
func discoverResources(client kubernetes.Interface) (gateways, httproutes bool) {
	resources, err := client.Discovery().ServerResourcesForGroupVersion(gatewayGV.String())
	if err != nil || resources == nil {
		return false, false
	}
	for _, r := range resources.APIResources {
		switch r.Name {
		case gatewayGVR.Resource:
			gateways = true
		case httprouteGVR.Resource:
			httproutes = true
		}
	}
	return gateways, httproutes
}

// arm enables emission — called once all informer caches sync.
func (s *Source) arm() {
	s.mu.Lock()
	s.armed = true
	s.synced = true
	s.mu.Unlock()
}

// send delivers signals to the pipeline (never called under s.mu).
func (s *Source) send(sigs []engine.Signal) {
	if len(sigs) == 0 {
		return
	}
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit == nil {
		return // unit tests drive handlers directly
	}
	for _, sig := range sigs {
		emit(sig)
	}
}

// Run implements sources.Source: discovery-gates the Gateway API,
// starts a dynamic informer per served resource, arms after the caches
// sync, then drives the grace-crossing sweep until ctx is cancelled.
// Neither CRD served is fatal — the source was named (explicitly, or
// resolved by auto's discovery gate which would have skipped it if
// absent), so an empty watch would be a coverage lie (§11).
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	s.watchGateways, s.watchRoutes = discoverResources(s.client)
	if !s.watchGateways && !s.watchRoutes {
		return fmt.Errorf("gateway: Gateway API not found (no %s or %s served) — enable it (a GKE Gateway class or the upstream CRDs) or drop %q from --sources",
			gatewayGVR, httprouteGVR, Name)
	}
	if !s.watchRoutes {
		s.logPrintf("gateway: %s not served — watching Gateways only (HTTPRoute route-rejection signals disabled)", httprouteGVR)
	}
	if !s.watchGateways {
		s.logPrintf("gateway: %s not served — watching HTTPRoutes only (Gateway programming signals disabled)", gatewayGVR)
	}

	factory := dynamicinformer.NewDynamicSharedInformerFactory(s.dyn, 0)
	var synced []cache.InformerSynced
	if s.watchGateways {
		inf := factory.ForResource(gatewayGVR).Informer()
		h, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { s.onObject(obj, "Gateway") },
			UpdateFunc: func(_, obj any) { s.onObject(obj, "Gateway") },
			DeleteFunc: func(obj any) { s.onDelete(obj) },
		})
		if err != nil {
			return fmt.Errorf("gateway: register Gateway handler: %w", err)
		}
		synced = append(synced, h.HasSynced)
	}
	if s.watchRoutes {
		inf := factory.ForResource(httprouteGVR).Informer()
		h, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { s.onObject(obj, "HTTPRoute") },
			UpdateFunc: func(_, obj any) { s.onObject(obj, "HTTPRoute") },
			DeleteFunc: func(obj any) { s.onDelete(obj) },
		})
		if err != nil {
			return fmt.Errorf("gateway: register HTTPRoute handler: %w", err)
		}
		synced = append(synced, h.HasSynced)
	}

	factory.Start(ctx.Done())
	defer factory.Shutdown()

	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return fmt.Errorf("gateway: cache sync failed (informer stopped before initial list completed)")
	}
	s.arm()
	// Evaluate immediately after arming: a failure already sustained
	// past its grace window at the initial LIST fires now, not one tick
	// later.
	s.send(s.sweep(s.clock()))

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.send(s.sweep(s.clock()))
		}
	}
}

// tombstoneObj unwraps cache.DeletedFinalStateUnknown tombstones.
func tombstoneObj(obj any) any {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return t.Obj
	}
	return obj
}

// onObject records an object's current failing conditions, then fires
// any that are already past their grace window.
func (s *Source) onObject(obj any, kindOf string) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	now := s.clock()
	failing := s.evaluate(u, kindOf, now)

	s.mu.Lock()
	st, seen := s.state[u.GetUID()]
	if !seen {
		st = &objState{
			kindOf:    kindOf,
			namespace: u.GetNamespace(),
			name:      u.GetName(),
			uid:       string(u.GetUID()),
			failing:   make(map[string]*failure),
			recovered: make(map[string]time.Time),
		}
		s.state[u.GetUID()] = st
	}
	st.namespace, st.name, st.lastSeen = u.GetNamespace(), u.GetName(), now
	// Merge the freshly observed failures into the latch-bearing state:
	// keep the fired latch across observations of the SAME ongoing
	// failure, adopt refreshed detail/since, drop kinds no longer
	// failing (recording the recovery instant for clearance).
	for kind, f := range failing {
		if cur, ok := st.failing[kind]; ok {
			cur.since, cur.detail = f.since, f.detail
		} else {
			st.failing[kind] = f
			delete(st.recovered, kind) // failing afresh
		}
	}
	for kind := range st.failing {
		if _, still := failing[kind]; !still {
			delete(st.failing, kind)
			st.recovered[kind] = now
		}
	}
	out := s.fireableLocked(now)
	s.mu.Unlock()
	s.send(out)
}

// onDelete drops an object's state. A bound incident on a deleted
// object resolves via the clearance predicate's object-gone branch.
func (s *Source) onDelete(obj any) {
	u, ok := tombstoneObj(obj).(*unstructured.Unstructured)
	if !ok {
		return
	}
	s.mu.Lock()
	delete(s.state, u.GetUID())
	s.mu.Unlock()
}

// sweep is the ticker body: fires grace crossings for still-failing
// kinds and prunes TTL-expired state. Returns the signals to emit.
func (s *Source) sweep(now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.fireableLocked(now)
	cutoff := now.Add(-s.cfg.StateTTL)
	for uid, st := range s.state {
		if st.lastSeen.Before(cutoff) {
			delete(s.state, uid)
		}
	}
	return out
}

// fireableLocked emits one signal per (object, kind) whose failure is
// armed, unfired, and sustained past Config.Grace. Called under s.mu.
func (s *Source) fireableLocked(now time.Time) []engine.Signal {
	if !s.armed {
		return nil
	}
	var out []engine.Signal
	// Deterministic order (object UID) so a multi-kind object emits in
	// a stable sequence — tests and digests read the same every run.
	uids := make([]types.UID, 0, len(s.state))
	for uid := range s.state {
		uids = append(uids, uid)
	}
	sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
	for _, uid := range uids {
		st := s.state[uid]
		for _, kind := range []string{KindProgrammingFailed, KindRouteRejected} {
			f := st.failing[kind]
			if f == nil || f.fired {
				continue
			}
			if now.Sub(f.since) < s.cfg.Grace {
				continue // still inside the grace window
			}
			f.fired = true
			out = append(out, s.signal(kind, st, f, now))
		}
	}
	return out
}

// signal composes one Signal for a fired failure.
func (s *Source) signal(kind string, st *objState, f *failure, now time.Time) engine.Signal {
	held := now.Sub(f.since).Truncate(time.Second)
	msg := fmt.Sprintf("%s %s: %s (sustained %s past the %s grace window)",
		st.kindOf, st.name, f.detail, held, s.cfg.Grace)
	return engine.Signal{
		Kind:     kind,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: st.uid, Reason: reasonOf(kind)},
			Namespace:    st.namespace,
			KindOfObject: st.kindOf,
			Name:         st.name,
			Message:      msg,
			FirstSeen:    f.since,
			LastSeen:     now,
			Count:        1,
		},
	}
}

// ---- condition extraction (unstructured) ----

// evaluate reads an object's status conditions and returns the failing
// kinds folded per (object, kind). now supplies the fallback onset when
// a condition carries no parseable lastTransitionTime.
func (s *Source) evaluate(u *unstructured.Unstructured, kindOf string, now time.Time) map[string]*failure {
	gen := u.GetGeneration()
	out := map[string]*failure{}

	add := func(kind, detail string, since time.Time) {
		f, ok := out[kind]
		if !ok {
			out[kind] = &failure{since: since, detail: detail}
			return
		}
		if since.Before(f.since) {
			f.since = since // earliest onset across contributors
		}
		f.detail += "; " + detail
	}

	switch kindOf {
	case "Gateway":
		// Top-level Gateway conditions.
		for _, c := range conditionsAt(u.Object, "status", "conditions") {
			if kind, detail, since, ok := failingCondition(c, gen, "", now); ok {
				add(kind, detail, since)
			}
		}
		// Per-listener conditions.
		for _, l := range nestedSlice(u.Object, "status", "listeners") {
			lm, ok := l.(map[string]any)
			if !ok {
				continue
			}
			lname, _, _ := unstructured.NestedString(lm, "name")
			scope := "listener"
			if lname != "" {
				scope = "listener/" + lname
			}
			for _, c := range mapsOf(lm["conditions"]) {
				if kind, detail, since, ok := failingCondition(c, gen, scope, now); ok {
					add(kind, detail, since)
				}
			}
		}
	case "HTTPRoute":
		// Per-parent (status.parents[].conditions).
		for i, p := range nestedSlice(u.Object, "status", "parents") {
			pm, ok := p.(map[string]any)
			if !ok {
				continue
			}
			for _, c := range mapsOf(pm["conditions"]) {
				if kind, detail, since, ok := failingCondition(c, gen, fmt.Sprintf("parent[%d]", i), now); ok {
					add(kind, detail, since)
				}
			}
		}
	}
	return out
}

// failingCondition decides whether one condition map is a sustained-
// eligible failure and, if so, returns the kind it maps to, a human
// detail, and the onset instant. scope prefixes the detail (listener /
// parent identity); empty for a top-level Gateway condition.
func failingCondition(c map[string]any, gen int64, scope string, now time.Time) (kind, detail string, since time.Time, ok bool) {
	ctype, _ := c["type"].(string)
	switch ctype {
	case "Programmed":
		kind = KindProgrammingFailed
	case "Accepted", "ResolvedRefs":
		kind = KindRouteRejected
	default:
		return "", "", time.Time{}, false
	}
	if status, _ := c["status"].(string); status != "False" {
		return "", "", time.Time{}, false
	}
	reason, _ := c["reason"].(string)
	if reason == pendingReason {
		return "", "", time.Time{}, false // controller still working, not a failure
	}
	// Skip conditions the controller has not yet re-evaluated for the
	// current spec (a stale condition describing a previous generation).
	if og, present := condInt64(c, "observedGeneration"); present && og != 0 && og < gen {
		return "", "", time.Time{}, false
	}
	since = now
	if lt, _ := c["lastTransitionTime"].(string); lt != "" {
		if t, err := time.Parse(time.RFC3339, lt); err == nil {
			since = t
		}
	}
	message, _ := c["message"].(string)
	detail = ctype + "=False"
	if reason != "" {
		detail += " (" + reason + ")"
	}
	if scope != "" {
		detail = scope + " " + detail
	}
	if message != "" {
		detail += ": " + message
	}
	return kind, detail, since, true
}

// conditionsAt returns the condition maps at a nested slice path.
func conditionsAt(obj map[string]any, path ...string) []map[string]any {
	return toMaps(nestedSlice(obj, path...))
}

// nestedSlice is a nil-safe unstructured slice read.
func nestedSlice(obj map[string]any, path ...string) []any {
	s, _, _ := unstructured.NestedSlice(obj, path...)
	return s
}

// mapsOf coerces an any that should be a []any of maps.
func mapsOf(v any) []map[string]any {
	s, ok := v.([]any)
	if !ok {
		return nil
	}
	return toMaps(s)
}

func toMaps(s []any) []map[string]any {
	out := make([]map[string]any, 0, len(s))
	for _, e := range s {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// condInt64 reads an integer field that unstructured may hold as
// float64 (JSON) or int64. present=false when the field is absent.
func condInt64(c map[string]any, key string) (val int64, present bool) {
	switch n := c[key].(type) {
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

// ---- §7.4 clearance ----

// clearance implements engine.ClearanceObserver: a gateway incident
// clears when the object's failing condition for that reason is gone
// (healthy) or the object itself is gone.
type clearance struct{ s *Source }

func (c clearance) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	reason := inc.Key.Reason
	if reason != reasonOf(KindProgrammingFailed) && reason != reasonOf(KindRouteRejected) {
		return engine.Clearance{}, false
	}
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.synced {
		return engine.Clearance{}, false // no sync yet — cannot judge
	}
	st, ok := s.state[types.UID(inc.Key.UID)]
	if !ok {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	kind := kindPrefix + reason
	if _, still := st.failing[kind]; still {
		return engine.Clearance{}, true // symptom still present
	}
	return engine.Clearance{
		Cleared:     true,
		StableSince: st.recovered[kind], // zero = absent as of this observation
		Resolution:  engine.ResolutionRecovered,
	}, true
}
