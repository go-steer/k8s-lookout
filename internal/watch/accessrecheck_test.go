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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// revokingReviewer denies one requirement (by String()) and allows the
// rest, with the denial switchable so a test can hand the grant back.
type revokingReviewer struct {
	mu     sync.Mutex
	denied map[string]bool
}

func (r *revokingReviewer) Allowed(_ context.Context, req sources.Requirement) (sources.Decision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return sources.Decision{Allowed: !r.denied[req.String()], Reason: "the ClusterRoleBinding was narrowed"}, nil
}

func (r *revokingReviewer) set(req sources.Requirement, denied bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.denied[req.String()] = denied
}

// declaringSource is a source that only exists to declare access —
// its Run blocks, because the re-check never touches it.
type declaringSource struct {
	name string
	reqs []sources.Requirement
}

func (s *declaringSource) Name() string         { return s.name }
func (s *declaringSource) Scope() sources.Scope { return sources.ScopeCluster }
func (s *declaringSource) Run(ctx context.Context, _ func(engine.Signal)) error {
	<-ctx.Done()
	return nil
}
func (s *declaringSource) RequiredAccess() []sources.Requirement { return s.reqs }

// awaitSignal waits for one dispatched signal.
func awaitSignal(t *testing.T, got <-chan engine.Signal) engine.Signal {
	t.Helper()
	select {
	case sig := <-got:
		return sig
	case <-time.After(5 * time.Second):
		t.Fatal("no signal dispatched — a revocation nobody hears about is the silent coverage gap #385 is about")
		return engine.Signal{}
	}
}

// TestAccessRecheck_ARevokedRequiredGrantEndsTheRunner is the whole
// point of #385 joined to #383: the sentinel notices the grant is
// gone, says so on the wire, and then stops pretending to watch a
// cluster it cannot read — terminally, so the supervisor does not
// restart it into the same refusal every ten seconds.
func TestAccessRecheck_ARevokedRequiredGrantEndsTheRunner(t *testing.T) {
	t.Parallel()
	req := sources.Requirement{Resource: "events", Verb: "watch"}
	rv := &revokingReviewer{denied: map[string]bool{req.String(): true}}
	src := &declaringSource{name: "k8s-events", reqs: []sources.Requirement{req}}
	m := newMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan engine.Signal, 4)

	var rc accessRecheck
	rc.start(ctx, cancel, time.Millisecond, rv, []sources.Source{src}, m, func(sig engine.Signal) { got <- sig })

	sig := awaitSignal(t, got)
	if sig.Kind != sources.KindAccessRevoked || sig.Key.Reason != sources.ReasonAccessRevoked {
		t.Errorf("signal kind/reason = %q/%q, want %q/%q", sig.Kind, sig.Key.Reason, sources.KindAccessRevoked, sources.ReasonAccessRevoked)
	}
	if sig.Severity != engine.SeverityCritical {
		t.Errorf("severity = %q, want critical — a required grant is gone and this cluster is about to stop being watched", sig.Severity)
	}
	if sig.KindOfObject != "Grant" || sig.Name != "k8s-events" {
		t.Errorf("object identity = %s/%s, want Grant/k8s-events", sig.KindOfObject, sig.Name)
	}
	for _, want := range []string{"events", "watch", "the ClusterRoleBinding was narrowed"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message %q does not name %q", sig.Message, want)
		}
	}

	// The runner's ctx is cancelled, and the error left behind is the
	// one #383 classifies as terminal.
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the runner ctx was not cancelled — a source that cannot read kept the runner alive, watching nothing")
	}
	var err error
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if err = rc.failure(); err != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err == nil {
		t.Fatal("no terminal error recorded")
	}
	if !errors.Is(err, sources.ErrAccessDenied) {
		t.Errorf("error %v does not satisfy errors.Is(err, sources.ErrAccessDenied) — the supervisor would restart this cluster forever", err)
	}
	if reason := classifyExit(err); reason != reasonAccessDenied {
		t.Errorf("classifyExit = %q, want %q", reason, reasonAccessDenied)
	}
	if got := testutil.ToFloat64(m.sourceDenied.WithLabelValues("k8s-events", "events", "true")); got != 1 {
		t.Errorf("lookout_source_denied{source=k8s-events,resource=events,required=true} = %v, want 1", got)
	}
}

// TestAccessRecheck_AnOptionalGrantDegradesAndKeepsRunning: the #145
// case (saturation's nodes/proxy read, platform-denied on Autopilot)
// found at runtime instead of at startup. It is reported, but one dark
// dimension is not a blind sentinel and must not stop the runner.
func TestAccessRecheck_AnOptionalGrantDegradesAndKeepsRunning(t *testing.T) {
	t.Parallel()
	req := sources.Requirement{Resource: "nodes", Subresource: "proxy", Verb: "get", Optional: true}
	rv := &revokingReviewer{denied: map[string]bool{req.String(): true}}
	src := &declaringSource{name: "saturation", reqs: []sources.Requirement{req}}
	m := newMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan engine.Signal, 4)

	var rc accessRecheck
	rc.start(ctx, cancel, time.Millisecond, rv, []sources.Source{src}, m, func(sig engine.Signal) { got <- sig })

	sig := awaitSignal(t, got)
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("severity = %q, want warning — the source keeps running with one dimension disabled", sig.Severity)
	}
	if got := testutil.ToFloat64(m.sourceDenied.WithLabelValues("saturation", "nodes/proxy", "false")); got != 1 {
		t.Errorf("lookout_source_denied{...,required=false} = %v, want 1", got)
	}
	// Give the loop several more sweeps: none of them may end the
	// runner, and none of them may re-report a standing denial.
	time.Sleep(50 * time.Millisecond)
	if err := rc.failure(); err != nil {
		t.Errorf("an OPTIONAL denial ended the runner: %v", err)
	}
	if ctx.Err() != nil {
		t.Error("an OPTIONAL denial cancelled the runner ctx")
	}
	if len(got) != 0 {
		t.Errorf("a standing optional denial re-reported %d more times", len(got))
	}

	// The grant comes back: the gauge must retract, because the series
	// answers "is coverage missing now", not "was it ever".
	rv.set(req, false)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if testutil.ToFloat64(m.sourceDenied.WithLabelValues("saturation", "nodes/proxy", "false")) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Error("lookout_source_denied stayed at 1 after the grant was restored")
}

// TestAccessRecheck_DisabledAndEmpty: --access-recheck=0 and a source
// set that declares nothing both mean "no watchdog". Neither may
// cancel anything or record a failure.
func TestAccessRecheck_DisabledAndEmpty(t *testing.T) {
	t.Parallel()
	req := sources.Requirement{Resource: "events", Verb: "watch"}
	rv := &revokingReviewer{denied: map[string]bool{req.String(): true}}

	for _, tc := range []struct {
		name     string
		interval time.Duration
		srcs     []sources.Source
	}{
		{"disabled", 0, []sources.Source{&declaringSource{name: "k8s-events", reqs: []sources.Requirement{req}}}},
		{"nothing declared", time.Millisecond, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var rc accessRecheck
			rc.start(ctx, cancel, tc.interval, rv, tc.srcs, newMetrics(), func(engine.Signal) {
				t.Error("dispatched a signal with the re-check off")
			})
			time.Sleep(20 * time.Millisecond)
			if err := rc.failure(); err != nil {
				t.Errorf("failure() = %v, want nil", err)
			}
			if ctx.Err() != nil {
				t.Error("the runner ctx was cancelled with the re-check off")
			}
		})
	}
}

// TestAccessRecheck_FirstFailureWins mirrors the feed-failure path: a
// second revocation is a consequence of the same lost binding, and the
// first is the one worth reporting.
func TestAccessRecheck_FirstFailureWins(t *testing.T) {
	t.Parallel()
	var rc accessRecheck
	first := errors.New("first")
	rc.fatal(first)
	rc.fatal(errors.New("second"))
	if rc.failure() != first {
		t.Errorf("failure() = %v, want %v", rc.failure(), first)
	}
}

// TestRequirementResource pins the metric's resource label: enough to
// name the RBAC rule, without the verb and scope that would multiply
// the series per source for no operator benefit.
func TestRequirementResource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		req  sources.Requirement
		want string
	}{
		{sources.Requirement{Resource: "events", Verb: "watch"}, "events"},
		{sources.Requirement{Resource: "nodes", Subresource: "proxy", Verb: "get"}, "nodes/proxy"},
		{sources.Requirement{Resource: "gateways", Group: "gateway.networking.k8s.io", Verb: "list"}, "gateways.gateway.networking.k8s.io"},
		{sources.Requirement{Resource: "configmaps", Verb: "get", Name: "cluster-autoscaler-status", Namespace: "kube-system"}, "configmaps"},
	} {
		if got := requirementResource(tc.req); got != tc.want {
			t.Errorf("requirementResource(%v) = %q, want %q", tc.req, got, tc.want)
		}
	}
}

// TestAccessRevokedSignal_UIDIsStable: the UID is the dedup identity,
// so the same lost permission must hash to the same incident every
// time — a re-report after a restart folds into the open incident
// instead of opening a second one.
func TestAccessRevokedSignal_UIDIsStable(t *testing.T) {
	t.Parallel()
	rev := sources.Revocation{
		Source:      "k8s-events",
		Requirement: sources.Requirement{Resource: "events", Verb: "watch"},
		Scope:       sources.ScopeCluster,
	}
	a := accessRevokedSignal(rev, time.Now())
	b := accessRevokedSignal(rev, time.Now().Add(time.Hour))
	if a.Key.UID != b.Key.UID {
		t.Errorf("UID drifted between reports: %q vs %q", a.Key.UID, b.Key.UID)
	}
	other := rev
	other.Requirement = sources.Requirement{Resource: "events", Verb: "list"}
	if accessRevokedSignal(other, time.Now()).Key.UID == a.Key.UID {
		t.Error("two different lost permissions share a UID — one would suppress the other")
	}
}
