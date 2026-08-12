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

package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// scriptedResolver is the §13 scripted-topology stand-in: every pod
// object resolves to the same node + namespace blast-radius keys.
type scriptedResolver struct {
	byObject map[engine.ObjectRef][]engine.Ancestor
}

func (r *scriptedResolver) Ancestors(ref engine.ObjectRef) []engine.Ancestor {
	return r.byObject[ref]
}

// stormPodSignal fabricates the i-th pod incident of a node-failure
// burst, deterministic for the wire pins.
func stormPodSignal(i int, ns string) engine.Signal {
	ts := time.Date(2026, 7, 24, 10, 0, i, 0, time.UTC)
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: fmt.Sprintf("uid-%d", i), Reason: "CrashLoopBackOff"},
			Namespace:    ns,
			KindOfObject: "Pod",
			Name:         fmt.Sprintf("pay-%d", i),
			Message:      "Back-off restarting failed container",
			Count:        1,
			FirstSeen:    ts,
			LastSeen:     ts,
			Node:         "gke-a",
		},
	}
}

// newStormDispatcher wires a per-incident dispatcher with storm
// correlation over a scripted resolver mapping the first n pod
// incidents (namespaces cycling shop/web/api) to the node gke-a key.
func newStormDispatcher(t *testing.T, base string, n int) (*dispatcher, []engine.Signal) {
	t.Helper()
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	res := &scriptedResolver{byObject: map[engine.ObjectRef][]engine.Ancestor{}}
	namespaces := []string{"shop", "web", "api"}
	var sigs []engine.Signal
	for i := 1; i <= n; i++ {
		sig := stormPodSignal(i, namespaces[(i-1)%len(namespaces)])
		res.byObject[engine.ObjectRef{Kind: "Pod", Namespace: sig.Namespace, Name: sig.Name}] = []engine.Ancestor{
			{Kind: "Node", Name: "gke-a"},
			{Kind: "Namespace", Name: sig.Namespace},
		}
		sigs = append(sigs, sig)
	}
	correlator, err := engine.NewStormCorrelator(engine.DefaultStormWindow, engine.DefaultStormMin, res)
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	return &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  "prod-us-central1",
		mode:     "per-incident",
		storm:    correlator,
	}, sigs
}

// messageOf unwraps the inject envelope.
func messageOf(t *testing.T, body string) string {
	t.Helper()
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("not an inject envelope: %v (body=%q)", err, body)
	}
	return envelope.Message
}

// TestStormDispatch_NodeFailureOneStorm is the §13/§14 M2 exit drill
// at dispatcher scale: N pod failures on one node → exactly ONE
// kind=storm inject in ONE storm session; the pre-storm members'
// sessions get supersede pointers; late arrivals attach as followups;
// no member opens a session after the storm forms.
func TestStormDispatch_NodeFailureOneStorm(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 5)
	ctx := context.Background()

	for _, sig := range sigs {
		d.DispatchSignal(ctx, sig)
	}

	// Sessions: sess-1, sess-2 per-incident (before the storm), then
	// sess-3 for the storm. Incidents 3-5 open NO sessions of their own.
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 3 {
		t.Errorf("session creates = %v, want 3 (two per-incident + one storm)", got)
	}
	var stormInjects, memberInjects, supersededInjects, eventInjects int
	for _, in := range *injects {
		msg := messageOf(t, in.Body)
		switch {
		case strings.Contains(msg, `"kind":"storm"`):
			stormInjects++
			if in.SessionID != "sess-3" {
				t.Errorf("storm inject landed in %q, want sess-3", in.SessionID)
			}
		case strings.Contains(msg, `"kind":"storm.member_superseded"`):
			supersededInjects++
			if in.SessionID != "sess-1" && in.SessionID != "sess-2" {
				t.Errorf("supersede landed in %q, want a pre-storm member session", in.SessionID)
			}
		case strings.Contains(msg, `"kind":"storm.member"`):
			memberInjects++
			if in.SessionID != "sess-3" {
				t.Errorf("member attach landed in %q, want the storm session", in.SessionID)
			}
		case strings.Contains(msg, `"kind":"k8s-event"`):
			eventInjects++
		}
	}
	if stormInjects != 1 {
		t.Errorf("storm injects = %d, want exactly 1", stormInjects)
	}
	if eventInjects != 2 || supersededInjects != 2 || memberInjects != 2 {
		t.Errorf("event/superseded/attached = %d/%d/%d, want 2/2/2", eventInjects, supersededInjects, memberInjects)
	}
	// Every member's dedup binding now routes to the storm session.
	for _, sig := range sigs {
		if sid, ok := d.dedup.LookupSession(sig.Key); !ok || sid != "sess-3" {
			t.Errorf("member %s binding = (%q, %v), want (sess-3, true)", sig.Name, sid, ok)
		}
	}
	if got := testutil.ToFloat64(d.metrics.stormsFormed); got != 1 {
		t.Errorf("storms_formed = %v, want 1", got)
	}
	for kind, want := range map[string]float64{"suppressed": 1, "superseded": 2, "attached": 2} {
		if got := testutil.ToFloat64(d.metrics.stormMembers.WithLabelValues(kind)); got != want {
			t.Errorf("storm_members{%s} = %v, want %v", kind, got, want)
		}
	}
}

// TestStormDispatch_BelowThresholdIndividual: fewer than --storm-min
// correlated incidents behave exactly as without correlation.
func TestStormDispatch_BelowThresholdIndividual(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 2)
	ctx := context.Background()
	for _, sig := range sigs {
		d.DispatchSignal(ctx, sig)
	}
	if got := testutil.ToFloat64(d.metrics.sessionCreates.WithLabelValues("ok")); got != 2 {
		t.Errorf("session creates = %v, want 2 per-incident", got)
	}
	for _, in := range *injects {
		if strings.Contains(messageOf(t, in.Body), `"kind":"storm"`) {
			t.Error("no storm may form below the threshold")
		}
	}
}

// TestStormDispatch_MembersTracked: storm members are handed to the
// recovery tracker bound to the STORM session, so §7.4 outcomes route
// there.
func TestStormDispatch_MembersTracked(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 4)
	var emitted []engine.Signal
	d.tracker = engine.NewRecoveryTracker(5*time.Minute, func(sig engine.Signal) { emitted = append(emitted, sig) })
	ctx := context.Background()
	for _, sig := range sigs {
		d.DispatchSignal(ctx, sig)
	}
	if d.tracker.Len() != 4 {
		t.Errorf("tracker.Len = %d, want all 4 members tracked", d.tracker.Len())
	}
	_ = emitted
}

// TestStormRecovery_AllMembersClear: member kind=resolved outcomes
// route to the storm session (rebound dedup binding); the LAST one
// resolves the storm, injecting the storm's own outcome record.
func TestStormRecovery_AllMembersClear(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 3)
	ctx := context.Background()
	for _, sig := range sigs {
		d.DispatchSignal(ctx, sig)
	}
	before := len(*injects)

	for i, sig := range sigs {
		res := resolvedSignalFor(sig, engine.KindResolved)
		res.Fingerprint = "sha256:member"
		d.DispatchSignal(ctx, res)
		wantDone := i == len(sigs)-1
		if got := testutil.ToFloat64(d.metrics.stormsResolved); (got == 1) != wantDone {
			t.Errorf("after member %d resolved: storms_resolved = %v, want done=%v", i+1, got, wantDone)
		}
	}
	post := (*injects)[before:]
	// 3 member resolved followups + 1 storm resolved record, all in
	// the storm session.
	if len(post) != 4 {
		t.Fatalf("post-storm injects = %d, want 4 (3 member outcomes + storm outcome)", len(post))
	}
	for _, in := range post {
		if in.SessionID != "sess-3" {
			t.Errorf("outcome landed in %q, want the storm session sess-3", in.SessionID)
		}
	}
	var final inject.ResolvedPayload
	if err := json.Unmarshal([]byte(messageOf(t, post[3].Body)), &final); err != nil {
		t.Fatalf("unmarshal storm outcome: %v", err)
	}
	if final.Kind != inject.KindResolved || final.Reason != "storm" {
		t.Errorf("storm outcome kind/reason = %q/%q, want resolved/storm", final.Kind, final.Reason)
	}
	if final.KindOfObject != "Node" || final.Name != "gke-a" {
		t.Errorf("storm outcome object = %s/%s, want Node/gke-a", final.KindOfObject, final.Name)
	}
	if final.UID != "storm:Node//gke-a" {
		t.Errorf("storm outcome uid = %q", final.UID)
	}
	// Resolved storm is closed: the next burst forms a NEW storm
	// (fresh session), not an attachment to the old one. Fresh UIDs
	// (replacement pods) so dedup sees new incidents; the resolver
	// keys on object identity, which is unchanged.
	for _, sig := range sigs {
		sig.Key.UID = "replacement-" + sig.Key.UID
		d.DispatchSignal(ctx, sig)
	}
	if got := testutil.ToFloat64(d.metrics.stormsFormed); got != 2 {
		t.Errorf("storms_formed after second burst = %v, want 2", got)
	}
}

// TestStormFormed_ExactWireShape pins the §7.5 kind=storm payload
// byte-for-byte, plus the kind=storm.member_superseded pointer left
// in a pre-storm member session and the kind=storm.member late-attach
// followup. SCHEMA-STABLE (fleet consumers + playbooks parse
// structurally): treat
// a failing pin as a breaking schema change, never as a test to
// update.
func TestStormFormed_ExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 4)
	ctx := context.Background()
	for _, sig := range sigs {
		d.DispatchSignal(ctx, sig)
	}
	// Inject order: sess-1 event, sess-2 event, sess-3 storm,
	// sess-1 superseded, sess-2 superseded, sess-3 member attach.
	if len(*injects) != 6 {
		t.Fatalf("injects = %d, want 6", len(*injects))
	}

	const memberFP = "sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b" // Fingerprint(k8s-event, CrashLoopBackOff, Pod, "")
	const stormFP = "sha256:8fdc3aab7c6444c4a8c4baba5ddcac72d6db1310fd108a9fd6cc09c28e939264"  // Fingerprint(storm, CrashLoopBackOff, Node, "")

	wantStorm := `{"kind":"storm","fingerprint":"` + stormFP + `","severity":"critical","cluster":"prod-us-central1","ancestor_kind":"Node","ancestor_name":"gke-a","reason":"CrashLoopBackOff","message":"Node gke-a: 3 incidents across 3 namespace(s) share this blast-radius key; 3 representative incident(s) attached; member sessions are suppressed and route here","affected_count":3,"namespaces_count":3,"first_seen":"2026-07-24T10:00:01Z","last_seen":"2026-07-24T10:00:03Z","representative_incidents":[{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"shop","kind_of_object":"Pod","name":"pay-1","uid":"uid-1","session_id":"sess-1"},{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"web","kind_of_object":"Pod","name":"pay-2","uid":"uid-2","session_id":"sess-2"},{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"api","kind_of_object":"Pod","name":"pay-3","uid":"uid-3"}],"member_fingerprints":["` + memberFP + `","` + memberFP + `","` + memberFP + `"],"context":{"node":"gke-a"}}`
	if got := messageOf(t, (*injects)[2].Body); got != wantStorm {
		t.Errorf("storm payload drifted from the schema-stable wire shape:\n got: %s\nwant: %s", got, wantStorm)
	}

	wantSuperseded := `{"kind":"storm.member_superseded","storm_fingerprint":"` + stormFP + `","storm_session_id":"sess-3","ancestor_kind":"Node","ancestor_name":"gke-a","cluster":"prod-us-central1","message":"this incident was folded into the Node gke-a storm (3 incidents); further followups and the outcome record route to the storm session","incident":{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"shop","kind_of_object":"Pod","name":"pay-1","uid":"uid-1","session_id":"sess-1"}}`
	if got := messageOf(t, (*injects)[3].Body); got != wantSuperseded {
		t.Errorf("superseded payload drifted from the schema-stable wire shape:\n got: %s\nwant: %s", got, wantSuperseded)
	}

	wantMember := `{"kind":"storm.member","storm_fingerprint":"` + stormFP + `","storm_session_id":"sess-3","ancestor_kind":"Node","ancestor_name":"gke-a","cluster":"prod-us-central1","message":"late-arriving incident attached to the Node gke-a storm (now 4 incidents)","incident":{"fingerprint":"` + memberFP + `","reason":"CrashLoopBackOff","namespace":"shop","kind_of_object":"Pod","name":"pay-4","uid":"uid-4"}}`
	if got := messageOf(t, (*injects)[5].Body); got != wantMember {
		t.Errorf("member payload drifted from the schema-stable wire shape:\n got: %s\nwant: %s", got, wantMember)
	}
}

// TestStormKindConstantsAlignedWithWireContract pins engine's storm
// kinds to inject's, mirroring the k8s-event and resolved pins.
func TestStormKindConstantsAlignedWithWireContract(t *testing.T) {
	t.Parallel()
	pairs := [][2]string{
		{engine.KindStorm, inject.KindStorm},
		{engine.KindStormMember, inject.KindStormMember},
		{engine.KindStormMemberSuperseded, inject.KindStormMemberSuperseded},
	}
	for _, p := range pairs {
		if p[0] != p[1] {
			t.Errorf("engine kind %q != inject kind %q", p[0], p[1])
		}
	}
}

// denyingReviewer denies one requirement's resource, allows the rest.
type denyingReviewer struct{ denyResource string }

func (r denyingReviewer) Allowed(_ context.Context, req sources.Requirement) (sources.Decision, error) {
	return sources.Decision{Allowed: req.Resource != r.denyResource}, nil
}

// TestProbeGraphAccess is the §11 loud-failure check for the graph
// feed's informers: --storm with a missing grant refuses to start,
// naming the permission.
func TestProbeGraphAccess(t *testing.T) {
	t.Parallel()
	if err := probeGraphAccess(context.Background(), denyingReviewer{}); err != nil {
		t.Errorf("all-allowed probe failed: %v", err)
	}
	err := probeGraphAccess(context.Background(), denyingReviewer{denyResource: "replicasets"})
	if err == nil {
		t.Fatal("missing replicasets grant must fail startup")
	}
	if !strings.Contains(err.Error(), "replicasets") || !strings.Contains(err.Error(), "--storm") {
		t.Errorf("error must name the grant and the flag: %v", err)
	}
}

// TestStormFlags pins the --storm flag surface. DEFAULT CHANGED
// DELIBERATELY on 2026-07-27 under the zero-deployed-users policy
// (one post-M0 pin change, recorded in the CHANGELOG): --storm is now
// a string mode defaulting to auto (probe the graph grants at
// startup, resolve on/off loudly — auto_test.go covers resolution);
// it was a default-false bool through 0.8.0. --storm=on keeps the
// old opt-in fatality; bounds stay validated in every mode.
func TestStormFlags(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.storm != stormAuto {
		t.Errorf("default --storm = %q, want auto (the 2026-07-27 default change)", f.storm)
	}
	if f.stormWindow != 60*time.Second {
		t.Errorf("default storm-window = %v, want 60s", f.stormWindow)
	}
	if f.stormMin != 3 {
		t.Errorf("default storm-min = %d, want 3", f.stormMin)
	}
	if f.stormEnabled() {
		t.Error("stormEnabled must be false before auto resolution runs")
	}

	on, err := parseFlags([]string{"--dry-run", "--storm=on", "--storm-window=30s", "--storm-min=5"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := on.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !on.stormEnabled() || on.stormWindow != 30*time.Second || on.stormMin != 5 {
		t.Errorf("storm flags not honored: %+v", on)
	}

	zero, _ := parseFlags([]string{"--dry-run", "--storm=on", "--storm-window=0"})
	if err := zero.validate(); err != nil {
		t.Fatalf("validate --storm-window=0: %v (0 disables, must not be rejected)", err)
	}
	if zero.stormEnabled() {
		t.Error("--storm-window=0 must disable correlation even with --storm=on")
	}

	bad, _ := parseFlags([]string{"--dry-run", "--storm-min=1"})
	if err := bad.validate(); err == nil {
		t.Error("--storm-min=1 must be rejected")
	}
	neg, _ := parseFlags([]string{"--dry-run", "--storm-window=-5s"})
	if err := neg.validate(); err == nil {
		t.Error("negative --storm-window must be rejected")
	}
}
