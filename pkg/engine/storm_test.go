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

package engine

import (
	"fmt"
	"testing"
	"time"
)

// fakeResolver is a scripted AncestorResolver (§13: scripted signal
// streams, no informers).
type fakeResolver struct {
	byObject map[ObjectRef][]Ancestor
}

func (f *fakeResolver) Ancestors(ref ObjectRef) []Ancestor { return f.byObject[ref] }

var (
	ancNode = Ancestor{Kind: "Node", Name: "gke-a"}
	ancRS   = Ancestor{Kind: "ReplicaSet", Namespace: "shop", Name: "pay-7b9d"}
	ancNS   = Ancestor{Kind: "Namespace", Name: "shop"}
	ancCM   = Ancestor{Kind: "ConfigMap", Namespace: "shop", Name: "db-config"}
)

// podSignal fabricates a critical pod incident with a stamped
// fingerprint, the shape signals have when they reach the correlator
// (after the pipeline stamp, dedup verdict = new).
func podSignal(i int, ns string) Signal {
	name := fmt.Sprintf("pay-%d", i)
	return Signal{
		Kind:        KindK8sEvent,
		Source:      SourceSentinel,
		Severity:    SeverityCritical,
		Fingerprint: Fingerprint(KindK8sEvent, "CrashLoopBackOff", "Pod", ""),
		TriageEvent: TriageEvent{
			Key:          EventKey{UID: fmt.Sprintf("uid-%d", i), Reason: "CrashLoopBackOff"},
			Namespace:    ns,
			KindOfObject: "Pod",
			Name:         name,
			FirstSeen:    time.Date(2026, 7, 24, 10, 0, i, 0, time.UTC),
			LastSeen:     time.Date(2026, 7, 24, 10, 0, i, 0, time.UTC),
		},
	}
}

func (s Signal) ref() ObjectRef {
	return ObjectRef{Kind: s.KindOfObject, Namespace: s.Namespace, Name: s.Name}
}

// newTestCorrelator builds a correlator over a resolver that maps
// every provided signal to the given candidate list, with a settable
// fake clock.
func newTestCorrelator(t *testing.T, window time.Duration, min int, res *fakeResolver) (*StormCorrelator, *time.Time) {
	t.Helper()
	c, err := NewStormCorrelator(window, min, res)
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	now := time.Date(2026, 7, 24, 10, 1, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	return c, &now
}

func TestStormCorrelator_ConstructorValidation(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{}
	if _, err := NewStormCorrelator(0, 3, res); err == nil {
		t.Error("window=0 must be rejected")
	}
	if _, err := NewStormCorrelator(time.Minute, 1, res); err == nil {
		t.Error("min=1 must be rejected (a storm of one is an incident)")
	}
	if _, err := NewStormCorrelator(time.Minute, 3, nil); err == nil {
		t.Error("nil resolver must be rejected")
	}
}

// TestStorm_FormsAtThreshold is the §13 core scenario: N pod failures
// on one node → the Nth forms exactly ONE storm; earlier ones proceed
// per-incident and become superseded members.
func TestStorm_FormsAtThreshold(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	sigs := []Signal{podSignal(1, "shop"), podSignal(2, "web"), podSignal(3, "api")}
	for _, s := range sigs {
		res.byObject[s.ref()] = []Ancestor{ancNode, ancNS}
	}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)

	if v := c.Observe(sigs[0]); v.Kind != StormNone {
		t.Fatalf("incident 1: verdict %v, want StormNone", v.Kind)
	}
	c.NoteMemberSession(sigs[0].Key, "sess-1")
	if v := c.Observe(sigs[1]); v.Kind != StormNone {
		t.Fatalf("incident 2: verdict %v, want StormNone", v.Kind)
	}
	c.NoteMemberSession(sigs[1].Key, "sess-2")

	v := c.Observe(sigs[2])
	if v.Kind != StormFormed {
		t.Fatalf("incident 3: verdict %v, want StormFormed", v.Kind)
	}
	st := v.Storm
	if st.Ancestor != ancNode {
		t.Errorf("storm ancestor = %+v, want the Node (priority over namespace)", st.Ancestor)
	}
	if st.ID != "Node//gke-a" {
		t.Errorf("storm ID = %q", st.ID)
	}
	if st.AffectedCount != 3 || st.NamespaceCount != 3 {
		t.Errorf("affected=%d namespaces=%d, want 3/3", st.AffectedCount, st.NamespaceCount)
	}
	wantFP := Fingerprint(KindStorm, "CrashLoopBackOff", "Node", "")
	if st.Fingerprint != wantFP {
		t.Errorf("storm fingerprint = %q, want %q", st.Fingerprint, wantFP)
	}
	if st.Severity != SeverityCritical {
		t.Errorf("severity = %q, want max member (critical)", st.Severity)
	}
	if len(v.Members) != 3 {
		t.Fatalf("members = %d, want 3", len(v.Members))
	}
	// Arrival order, with the pre-storm sessions recorded and the
	// trigger suppressed (no session).
	if v.Members[0].SessionID != "sess-1" || v.Members[1].SessionID != "sess-2" || v.Members[2].SessionID != "" {
		t.Errorf("member sessions = %q/%q/%q, want sess-1/sess-2/(empty)",
			v.Members[0].SessionID, v.Members[1].SessionID, v.Members[2].SessionID)
	}
	if len(st.Representatives) != 3 || len(st.MemberFingerprints) != 3 {
		t.Errorf("reps=%d fingerprints=%d, want 3/3", len(st.Representatives), len(st.MemberFingerprints))
	}
	if st.FirstSeen != sigs[0].FirstSeen {
		t.Errorf("storm FirstSeen = %v, want the first member's %v", st.FirstSeen, sigs[0].FirstSeen)
	}
	if c.ActiveStorms() != 1 {
		t.Errorf("ActiveStorms = %d, want 1", c.ActiveStorms())
	}
}

// TestStorm_LateArrivalAttaches pins §7.5 window semantics: incidents
// arriving AFTER formation attach as members while the storm is
// unresolved — they never seed a second storm.
func TestStorm_LateArrivalAttaches(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 1; i <= 5; i++ {
		s := podSignal(i, "shop")
		res.byObject[s.ref()] = []Ancestor{ancNode}
		sigs = append(sigs, s)
	}
	c, now := newTestCorrelator(t, time.Minute, 3, res)
	for _, s := range sigs[:3] {
		c.Observe(s)
	}
	// Even well past the formation window: attachment is gated on the
	// storm being open, not on the 60s window.
	*now = now.Add(5 * time.Minute)
	v := c.Observe(sigs[3])
	if v.Kind != StormAttached {
		t.Fatalf("late arrival: verdict %v, want StormAttached", v.Kind)
	}
	if v.Storm.AffectedCount != 4 {
		t.Errorf("affected = %d, want 4", v.Storm.AffectedCount)
	}
	if v2 := c.Observe(sigs[4]); v2.Kind != StormAttached || v2.Storm.AffectedCount != 5 {
		t.Errorf("second late arrival: %v affected=%d, want attached/5", v2.Kind, v2.Storm.AffectedCount)
	}
	if c.ActiveStorms() != 1 {
		t.Errorf("ActiveStorms = %d, want exactly 1", c.ActiveStorms())
	}
	// Representatives stay capped at the first three.
	if got := len(c.storms["Node//gke-a"].info().Representatives); got != 3 {
		t.Errorf("representatives = %d, want cap 3", got)
	}
}

// TestStorm_BelowThresholdProceedsIndividually: fewer than --storm-min
// correlated incidents keep today's per-incident behavior.
func TestStorm_BelowThreshold(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	s1, s2 := podSignal(1, "shop"), podSignal(2, "shop")
	res.byObject[s1.ref()] = []Ancestor{ancNode}
	res.byObject[s2.ref()] = []Ancestor{ancNode}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)
	if v := c.Observe(s1); v.Kind != StormNone {
		t.Errorf("verdict %v, want StormNone", v.Kind)
	}
	if v := c.Observe(s2); v.Kind != StormNone {
		t.Errorf("verdict %v, want StormNone", v.Kind)
	}
	if c.ActiveStorms() != 0 {
		t.Errorf("ActiveStorms = %d, want 0", c.ActiveStorms())
	}
}

// TestStorm_WindowExpiry: incidents older than the window do not
// count toward formation.
func TestStorm_WindowExpiry(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 1; i <= 3; i++ {
		s := podSignal(i, "shop")
		res.byObject[s.ref()] = []Ancestor{ancNode}
		sigs = append(sigs, s)
	}
	c, now := newTestCorrelator(t, time.Minute, 3, res)
	c.Observe(sigs[0])
	c.Observe(sigs[1])
	*now = now.Add(2 * time.Minute) // both age out
	if v := c.Observe(sigs[2]); v.Kind != StormNone {
		t.Errorf("stale window entries must not correlate: verdict %v", v.Kind)
	}
}

// TestStorm_KeyPriority: the storm forms on the best-priority key the
// TRIGGERING incident carries that reaches threshold — a shared owner
// beats the namespace; the namespace only groups when nothing better
// does.
func TestStorm_KeyPriority(t *testing.T) {
	t.Parallel()
	t.Run("namespace when owners differ", func(t *testing.T) {
		t.Parallel()
		res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
		s1, s2, s3 := podSignal(1, "shop"), podSignal(2, "shop"), podSignal(3, "shop")
		res.byObject[s1.ref()] = []Ancestor{ancRS, ancNS}
		res.byObject[s2.ref()] = []Ancestor{ancRS, ancNS}
		res.byObject[s3.ref()] = []Ancestor{{Kind: "ReplicaSet", Namespace: "shop", Name: "other-rs"}, ancNS}
		c, _ := newTestCorrelator(t, time.Minute, 3, res)
		c.Observe(s1)
		c.Observe(s2)
		v := c.Observe(s3)
		if v.Kind != StormFormed || v.Storm.Ancestor != ancNS {
			t.Errorf("verdict %v ancestor %+v, want formed on the Namespace", v.Kind, v.Storm.Ancestor)
		}
	})
	t.Run("owner beats namespace", func(t *testing.T) {
		t.Parallel()
		res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
		s1, s2, s3 := podSignal(1, "shop"), podSignal(2, "shop"), podSignal(3, "shop")
		for _, s := range []Signal{s1, s2, s3} {
			res.byObject[s.ref()] = []Ancestor{ancRS, ancCM, ancNS}
		}
		c, _ := newTestCorrelator(t, time.Minute, 3, res)
		c.Observe(s1)
		c.Observe(s2)
		v := c.Observe(s3)
		if v.Kind != StormFormed || v.Storm.Ancestor != ancRS {
			t.Errorf("verdict %v ancestor %+v, want formed on the ReplicaSet", v.Kind, v.Storm.Ancestor)
		}
	})
	t.Run("shared config beats namespace", func(t *testing.T) {
		t.Parallel()
		res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
		s1, s2, s3 := podSignal(1, "shop"), podSignal(2, "shop"), podSignal(3, "shop")
		res.byObject[s1.ref()] = []Ancestor{{Kind: "ReplicaSet", Namespace: "shop", Name: "rs-a"}, ancCM, ancNS}
		res.byObject[s2.ref()] = []Ancestor{{Kind: "ReplicaSet", Namespace: "shop", Name: "rs-b"}, ancCM, ancNS}
		res.byObject[s3.ref()] = []Ancestor{{Kind: "ReplicaSet", Namespace: "shop", Name: "rs-c"}, ancCM, ancNS}
		c, _ := newTestCorrelator(t, time.Minute, 3, res)
		c.Observe(s1)
		c.Observe(s2)
		v := c.Observe(s3)
		if v.Kind != StormFormed || v.Storm.Ancestor != ancCM {
			t.Errorf("verdict %v ancestor %+v, want formed on the shared ConfigMap", v.Kind, v.Storm.Ancestor)
		}
	})
}

// TestStorm_SeverityEscalator pins the documented §7.5 rule: severity
// is the max member severity; at >= 10 members warning is promoted to
// critical (only that promotion).
func TestStorm_SeverityEscalator(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 1; i <= 10; i++ {
		s := podSignal(i, "shop")
		s.Severity = SeverityWarning
		res.byObject[s.ref()] = []Ancestor{ancNode}
		sigs = append(sigs, s)
	}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)
	var v StormVerdict
	for i, s := range sigs {
		v = c.Observe(s)
		if i == 2 && v.Storm.Severity != SeverityWarning {
			t.Errorf("3 warning members: severity %q, want warning", v.Storm.Severity)
		}
	}
	if v.Storm.AffectedCount != 10 {
		t.Fatalf("affected = %d, want 10", v.Storm.AffectedCount)
	}
	if v.Storm.Severity != SeverityCritical {
		t.Errorf("10 warning members: severity %q, want critical (size escalator)", v.Storm.Severity)
	}
}

// TestStorm_RecoveryAllMembersClear: member clearance feeds the
// storm's recovery — only when ALL members clear is the storm
// resolved (and closed, so the next burst forms a fresh storm).
func TestStorm_RecoveryAllMembersClear(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 1; i <= 3; i++ {
		s := podSignal(i, "shop")
		res.byObject[s.ref()] = []Ancestor{ancNode}
		sigs = append(sigs, s)
	}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)
	for _, s := range sigs {
		c.Observe(s)
	}

	if _, done, ok := c.MemberResolved(sigs[0].Key); !ok || done {
		t.Fatalf("member 1 resolved: done=%v ok=%v, want pending member", done, ok)
	}
	if _, done, _ := c.MemberResolved(sigs[1].Key); done {
		t.Fatal("2/3 members cleared must not resolve the storm")
	}
	// One member reverts (§7.4 resolved.reverted): the bar rises back.
	c.MemberReverted(sigs[1].Key)
	if _, done, _ := c.MemberResolved(sigs[2].Key); done {
		t.Fatal("a reverted member must keep the storm open")
	}
	info, done, ok := c.MemberResolved(sigs[1].Key)
	if !ok || !done {
		t.Fatalf("last member cleared: done=%v ok=%v, want storm resolved", done, ok)
	}
	if info.AffectedCount != 3 {
		t.Errorf("final snapshot affected = %d, want 3", info.AffectedCount)
	}
	if c.ActiveStorms() != 0 {
		t.Errorf("resolved storm must close: ActiveStorms = %d", c.ActiveStorms())
	}
	// Post-resolution, the same keys correlate into a NEW storm.
	if _, _, ok := c.MemberResolved(sigs[0].Key); ok {
		t.Error("closed storm must forget its members")
	}
	for i, s := range sigs {
		v := c.Observe(s)
		if i < 2 && v.Kind != StormNone {
			t.Errorf("post-resolution incident %d: verdict %v, want StormNone", i+1, v.Kind)
		}
		if i == 2 && v.Kind != StormFormed {
			t.Errorf("post-resolution burst must form a fresh storm, got %v", v.Kind)
		}
	}
}

// TestStorm_UnresolvableObjectNeverCorrelates: no candidates (index
// not ready / object unknown) → per-incident, always.
func TestStorm_UnresolvableObject(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	c, _ := newTestCorrelator(t, time.Minute, 2, res)
	for i := 1; i <= 4; i++ {
		if v := c.Observe(podSignal(i, "shop")); v.Kind != StormNone {
			t.Fatalf("unresolvable incident %d: verdict %v, want StormNone", i, v.Kind)
		}
	}
}

// TestStorm_RefireIsOneIncident: a dedup retry-safety re-fire of the
// same key replaces its window entry instead of double-counting it.
func TestStorm_RefireIsOneIncident(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	s1, s2 := podSignal(1, "shop"), podSignal(2, "shop")
	res.byObject[s1.ref()] = []Ancestor{ancNode}
	res.byObject[s2.ref()] = []Ancestor{ancNode}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)
	c.Observe(s1)
	c.Observe(s1) // re-fire, same key
	if v := c.Observe(s2); v.Kind != StormNone {
		t.Errorf("2 distinct incidents (one re-fired) must not reach min=3: verdict %v", v.Kind)
	}
}

// TestDedup_AttachToStorm covers the extended binding model: the
// rebind routes followups to the storm session, records the storm
// fingerprint, and supplies the identity ref for entries that never
// went through BindIncident (the suppressed trigger).
func TestDedup_AttachToStorm(t *testing.T) {
	t.Parallel()
	cache, _ := NewDedupCache(5*time.Minute, "")
	key := EventKey{UID: "uid-1", Reason: "CrashLoopBackOff"}
	cache.Observe(key, time.Now())

	ref := IncidentRef{Namespace: "shop", KindOfObject: "Pod", Name: "pay-1", Fingerprint: "sha256:member"}
	cache.AttachToStorm(key, "sess-storm", "sha256:storm", ref)

	if sid, ok := cache.LookupSession(key); !ok || sid != "sess-storm" {
		t.Errorf("LookupSession = (%q, %v), want (sess-storm, true)", sid, ok)
	}
	bound, ok := cache.LookupBinding(key)
	if !ok || bound.Ref.Name != "pay-1" {
		t.Fatalf("LookupBinding = (%+v, %v), want the supplied ref", bound, ok)
	}
	// A pre-existing (richer) ref is kept on rebind.
	richer := IncidentRef{Namespace: "shop", KindOfObject: "Pod", Name: "pay-1", ControllerRef: "ReplicaSet/pay-7b9d"}
	key2 := EventKey{UID: "uid-2", Reason: "CrashLoopBackOff"}
	cache.Observe(key2, time.Now())
	cache.BindIncident(key2, "sess-1", richer)
	cache.AttachToStorm(key2, "sess-storm", "sha256:storm", IncidentRef{Name: "poorer"})
	bound2, _ := cache.LookupBinding(key2)
	if bound2.SessionID != "sess-storm" || bound2.Ref.ControllerRef != "ReplicaSet/pay-7b9d" {
		t.Errorf("rebind must swap the session and keep the richer ref: %+v", bound2)
	}
	// The storm marker survives Snapshot/restore (version-tolerant
	// like Incident).
	path := t.TempDir() + "/dedup.json"
	persisted, _ := NewDedupCache(5*time.Minute, path)
	persisted.Observe(key, time.Now())
	persisted.AttachToStorm(key, "sess-storm", "sha256:storm", ref)
	if err := persisted.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored, err := NewDedupCache(5*time.Minute, path)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if sid, ok := restored.LookupSession(key); !ok || sid != "sess-storm" {
		t.Errorf("restored binding = (%q, %v), want (sess-storm, true)", sid, ok)
	}
	if restored.entries[key].Storm != "sha256:storm" {
		t.Errorf("restored storm marker = %q, want sha256:storm", restored.entries[key].Storm)
	}
}

// pullPodSignal fabricates one pod's retryable image-pull failure,
// already classified (the shape the correlator sees post-dispatch
// stamp). Each pod belongs to a DIFFERENT workload in a DIFFERENT
// namespace — the shape of a registry-wide incident, and the shape
// that topology-only correlation cannot group.
func pullPodSignal(i int, host string) Signal {
	ns := fmt.Sprintf("team-%d", i)
	return Signal{
		Kind:        KindK8sEvent,
		Source:      SourceSentinel,
		Severity:    SeverityCritical,
		PullClass:   PullClassRetryable,
		Fingerprint: Fingerprint(KindK8sEvent, "ImagePullBackOff", "Pod", ""),
		TriageEvent: TriageEvent{
			Key:          EventKey{UID: fmt.Sprintf("pull-uid-%d", i), Reason: "BackOff"},
			Message:      fmt.Sprintf("Back-off pulling image %q", host+"/proj/app:v1"),
			Namespace:    ns,
			KindOfObject: "Pod",
			Name:         fmt.Sprintf("app-%d", i),
			FirstSeen:    time.Date(2026, 8, 12, 10, 0, i, 0, time.UTC),
			LastSeen:     time.Date(2026, 8, 12, 10, 0, i, 0, time.UTC),
		},
	}
}

// TestStorm_RegistryKeyGroupsCrossWorkloadPullFailures is issue #213's
// blast-radius half: a per-region registry quota hits every pod
// pulling from that host, across workloads and namespaces. Topology
// gives those pods no common ancestor, so without the synthetic
// registry key they fan out into N per-incident sessions. With it,
// they are ONE storm.
func TestStorm_RegistryKeyGroupsCrossWorkloadPullFailures(t *testing.T) {
	t.Parallel()
	const host = "us-east1-artifactregistry.gcr.io"
	// Each pod resolves to its OWN namespace ancestor and nothing
	// shared — topology alone cannot correlate them.
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	sigs := []Signal{pullPodSignal(1, host), pullPodSignal(2, host), pullPodSignal(3, host)}
	for _, s := range sigs {
		res.byObject[s.ref()] = []Ancestor{{Kind: "Namespace", Name: s.Namespace}}
	}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)

	if v := c.Observe(sigs[0]); v.Kind != StormNone {
		t.Fatalf("incident 1: verdict %v, want StormNone", v.Kind)
	}
	if v := c.Observe(sigs[1]); v.Kind != StormNone {
		t.Fatalf("incident 2: verdict %v, want StormNone", v.Kind)
	}
	v := c.Observe(sigs[2])
	if v.Kind != StormFormed {
		t.Fatalf("incident 3: verdict %v, want StormFormed (the registry is the shared ancestor)", v.Kind)
	}
	want := Ancestor{Kind: AncestorKindRegistry, Name: host}
	if v.Storm.Ancestor != want {
		t.Errorf("storm ancestor = %+v, want %+v", v.Storm.Ancestor, want)
	}
	if v.Storm.ID != "Registry//"+host {
		t.Errorf("storm ID = %q, want %q", v.Storm.ID, "Registry//"+host)
	}
	if len(v.Members) != 3 {
		t.Errorf("storm members = %d, want 3", len(v.Members))
	}
}

// TestStorm_RegistryKeyOnlyForRetryable: two workloads with two
// different BAD TAGS on one registry are two incidents, not a storm.
// The registry key is scoped to the retryable class precisely so that
// terminal failures keep grouping by topology as they always have.
func TestStorm_RegistryKeyOnlyForRetryable(t *testing.T) {
	t.Parallel()
	const host = "us-east1-artifactregistry.gcr.io"
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 1; i <= 3; i++ {
		s := pullPodSignal(i, host)
		s.PullClass = PullClassTerminal
		res.byObject[s.ref()] = []Ancestor{{Kind: "Namespace", Name: s.Namespace}}
		sigs = append(sigs, s)
	}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)
	for i, s := range sigs {
		if v := c.Observe(s); v.Kind != StormNone {
			t.Fatalf("terminal incident %d: verdict %v, want StormNone (no registry grouping)", i+1, v.Kind)
		}
	}
}

// TestStorm_RegistryKeyOutranksTopology: a registry incident spans
// workloads, so its key must be checked BEFORE the owner chain —
// otherwise three pods of one Deployment plus three of another become
// two storms instead of one.
func TestStorm_RegistryKeyOutranksTopology(t *testing.T) {
	t.Parallel()
	const host = "gcr.io"
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	shared := Ancestor{Kind: "ReplicaSet", Namespace: "shop", Name: "pay-7b9d"}
	sigs := []Signal{pullPodSignal(1, host), pullPodSignal(2, host), pullPodSignal(3, host)}
	for _, s := range sigs {
		res.byObject[s.ref()] = []Ancestor{shared}
	}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)
	c.Observe(sigs[0])
	c.Observe(sigs[1])
	v := c.Observe(sigs[2])
	if v.Kind != StormFormed {
		t.Fatalf("verdict %v, want StormFormed", v.Kind)
	}
	if v.Storm.Ancestor.Kind != AncestorKindRegistry {
		t.Errorf("storm ancestor = %+v, want the Registry key to outrank the ReplicaSet", v.Storm.Ancestor)
	}
}

// TestStorm_RegistryKeyNeedsAnImageRef: a retryable class with no
// parsable image reference in the message contributes no registry
// key, and must not accidentally group under an empty host.
func TestStorm_RegistryKeyNeedsAnImageRef(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 1; i <= 3; i++ {
		s := pullPodSignal(i, "gcr.io")
		s.Message = "Error: ErrImagePull" // no quoted image ref
		res.byObject[s.ref()] = []Ancestor{{Kind: "Namespace", Name: s.Namespace}}
		sigs = append(sigs, s)
	}
	c, _ := newTestCorrelator(t, time.Minute, 3, res)
	for i, s := range sigs {
		if v := c.Observe(s); v.Kind != StormNone {
			t.Fatalf("incident %d: verdict %v, want StormNone (no registry host to group on)", i+1, v.Kind)
		}
	}
}
