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

// #599 repeats against a sink that misbehaves, and a lower-class angle
// on the same key: neither may make an open incident go dark or split
// it into a second session.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// faultyDaemon is the routing fake daemon with a per-inject status
// override: fail(sid) returns the status to answer for the next inject
// into sid (0 = accept). Failed injects are not captured.
type faultyDaemon struct {
	mu      sync.Mutex
	fail    func(sid string) int
	injects []routedInject
}

func newFaultyDaemon(t *testing.T) (string, *faultyDaemon) {
	t.Helper()
	fd := &faultyDaemon{fail: func(string) int { return 0 }}
	sessions := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fd.mu.Lock()
		defer fd.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			sessions++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"app":"core-agent","user":"alice","sessionID":"sess-%d","url":"http://x"}`, sessions)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/inject"):
			sid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sessions/"), "/inject")
			body, _ := io.ReadAll(r.Body)
			if code := fd.fail(sid); code != 0 {
				w.WriteHeader(code)
				return
			}
			fd.injects = append(fd.injects, routedInject{SessionID: sid, Body: string(body)})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, fd
}

func (fd *faultyDaemon) setFail(f func(sid string) int) {
	fd.mu.Lock()
	fd.fail = f
	fd.mu.Unlock()
}

func (fd *faultyDaemon) captured() []routedInject {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	return append([]routedInject(nil), fd.injects...)
}

// TestDispatchRepeat_LowerClassAngleKeepsBinding is the review repro:
// with production routing, a capacity.pending warning on the stuck
// pod's key just after a lapse must not unbind the open incident, and
// the next FailedScheduling must not open a second session.
func TestDispatchRepeat_LowerClassAngleKeepsBinding(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	r.emit(5*time.Minute+5*time.Second, sig) // reminder 1 into sess-1
	pending := sig
	pending.Kind = "capacity.pending"
	pending.Key.Reason = "pending"
	pending.Severity = engine.SeverityWarning
	pending.Message = "pod pending: no node group can fit it"
	r.emit(5*time.Minute+2*time.Second, pending)
	r.emit(3*time.Second, sig)
	if got := testutil.ToFloat64(r.d.metrics.sessionCreates.WithLabelValues("ok")); got != 1 {
		t.Fatalf("session creates = %v, want 1 (a warning angle must not split the incident)", got)
	}
	for _, in := range *injects {
		if in.SessionID != "sess-1" {
			t.Errorf("inject routed to %q, want only sess-1", in.SessionID)
		}
	}
}

// TestDispatchRepeat_SessionGoneReopens: the agent lost the session
// (inject 404). The repeat must not go dark: the incident is unbound
// and reopened at once in a new session with the full open payload,
// and its schedule restarts there.
func TestDispatchRepeat_SessionGoneReopens(t *testing.T) {
	t.Parallel()
	base, fd := newFaultyDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	fd.setFail(func(sid string) int {
		if sid == "sess-1" {
			return http.StatusNotFound
		}
		return 0
	})
	r.emit(5*time.Minute+5*time.Second, sig) // reminder 1 → 404 → reopen
	if got := testutil.ToFloat64(r.d.metrics.sessionCreates.WithLabelValues("ok")); got != 2 {
		t.Fatalf("session creates = %v, want 2 (reopened after the 404)", got)
	}
	got := fd.captured()
	last := got[len(got)-1]
	if p := payloadFields(t, last.Body); last.SessionID != "sess-2" || p["kind"] != engine.KindK8sEvent || p["repeat_count"] != nil {
		t.Fatalf("after the 404: %s %v, want a full k8s-event open in sess-2", last.SessionID, p)
	}
	r.emit(5*time.Minute+5*time.Second, sig)
	got = fd.captured()
	last = got[len(got)-1]
	if p := payloadFields(t, last.Body); last.SessionID != "sess-2" || p["repeat_count"] != float64(1) {
		t.Fatalf("next lapse: %s %v, want reminder 1 in sess-2", last.SessionID, p)
	}
}

// goneSink is an in-process inject.Sink whose Append into a session
// marked gone returns exactly *inject.StatusError{Code: 404} — the
// error the ax sink (and its part-3 reminder hold) uses to say the
// task or session no longer exists.
type goneSink struct {
	mu      sync.Mutex
	opens   int
	gone    map[string]bool
	appends []routedInject
}

func (s *goneSink) OpenIncident(_ context.Context, payload any) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opens++
	sid := fmt.Sprintf("task/sess-%d", s.opens)
	b, _ := json.Marshal(payload)
	s.appends = append(s.appends, routedInject{SessionID: sid, Body: string(b)})
	return sid, nil
}

func (s *goneSink) Append(_ context.Context, id string, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gone[id] {
		return &inject.StatusError{Op: "ax sink: inject", Code: http.StatusNotFound}
	}
	b, _ := json.Marshal(payload)
	s.appends = append(s.appends, routedInject{SessionID: id, Body: string(b)})
	return nil
}

// TestDispatchRepeat_SinkStatusError404Reopens: the un-bind path keys
// on errors.As(*inject.StatusError) with Code 404, not on message
// text — a sink returning exactly that error gets the incident
// reopened in a new session.
func TestDispatchRepeat_SinkStatusError404Reopens(t *testing.T) {
	t.Parallel()
	sink := &goneSink{gone: map[string]bool{}}
	d := newRecoveryDispatcher(t, "http://unused")
	d.injector = sink
	r := wireRepeat(t, d)
	sig := unschedulableSignal()
	r.emit(0, sig)
	sink.mu.Lock()
	sink.gone["task/sess-1"] = true
	sink.mu.Unlock()
	r.emit(5*time.Minute+5*time.Second, sig)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.opens != 2 {
		t.Fatalf("opens = %d, want 2 (reopened after the StatusError 404)", sink.opens)
	}
	last := sink.appends[len(sink.appends)-1]
	var p map[string]any
	if err := json.Unmarshal([]byte(last.Body), &p); err != nil {
		t.Fatal(err)
	}
	if last.SessionID != "task/sess-2" || p["kind"] != engine.KindK8sEvent {
		t.Fatalf("after the 404: %s %v, want the k8s-event open in task/sess-2", last.SessionID, p)
	}
	if sid, ok := d.dedup.LookupSession(sig.CanonicalKey()); !ok || sid != "task/sess-2" {
		t.Fatalf("binding = %q %v, want task/sess-2", sid, ok)
	}
}

// TestDispatchRepeat_WebhookGoneReopens: the webhook sink surfaces a
// receiver's 404 on /incidents/<id>/events as *inject.StatusError, so a
// receiver that dropped the incident gets it reopened (POST /incidents)
// instead of a re-send into the dead id forever.
func TestDispatchRepeat_WebhookGoneReopens(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var paths []string
	opens := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		paths = append(paths, r.URL.Path)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/incidents":
			opens++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"id":"inc-%d"}`, opens)
		case r.URL.Path == "/incidents/inc-1/events":
			http.Error(w, "no such incident", http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/events"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	r := wireRepeat(t, newWebhookDispatcher(t, srv.URL, ""))
	sig := unschedulableSignal()
	r.emit(0, sig)
	r.emit(5*time.Minute+5*time.Second, sig) // reminder → 404 → reopen
	mu.Lock()
	gotOpens := opens
	mu.Unlock()
	if gotOpens != 2 {
		t.Fatalf("webhook opens = %d (paths %v), want 2: the 404 must reopen", gotOpens, paths)
	}
	if sid, ok := r.d.dedup.LookupSession(sig.CanonicalKey()); !ok || sid != "inc-2" {
		t.Fatalf("binding = %q %v, want inc-2", sid, ok)
	}
	r.emit(5*time.Minute+5*time.Second, sig)
	mu.Lock()
	defer mu.Unlock()
	if last := paths[len(paths)-1]; last != "/incidents/inc-2/events" {
		t.Fatalf("next reminder went to %q, want /incidents/inc-2/events", last)
	}
}

// TestDispatchRepeat_FailedReminderThenNewShapeSendsChange: reminder 1
// fails with a 5xx and the next occurrence brings a new shape; the
// re-send must announce the change, not a stale reminder 1.
func TestDispatchRepeat_FailedReminderThenNewShapeSendsChange(t *testing.T) {
	t.Parallel()
	base, fd := newFaultyDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	fd.setFail(func(string) int { return http.StatusBadGateway })
	r.emit(5*time.Minute+5*time.Second, sig) // reminder 1 → 502
	fd.setFail(func(string) int { return 0 })
	sig.Message = "0/6 nodes are available: 6 Insufficient memory."
	r.emit(time.Minute, sig)
	got := fd.captured()
	if len(got) != 2 {
		t.Fatalf("injects = %d, want the open + one re-send", len(got))
	}
	p := payloadFields(t, got[1].Body)
	if p["kind"] != engine.KindK8sEventFollowup || p["repeat_count"] != nil {
		t.Fatalf("re-send = %v, want a change followup (repeat_count omitted)", p)
	}
}

// TestDispatchRepeat_FailedReminderIsResent: a transient delivery error
// does not let the schedule skip the reminder; the next occurrence
// re-sends it.
func TestDispatchRepeat_FailedReminderIsResent(t *testing.T) {
	t.Parallel()
	base, fd := newFaultyDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	r.emit(0, sig)
	fd.setFail(func(string) int { return http.StatusServiceUnavailable })
	r.emit(5*time.Minute+5*time.Second, sig) // reminder 1 → 503
	fd.setFail(func(string) int { return 0 })
	r.emit(time.Minute, sig)
	got := fd.captured()
	if len(got) != 2 {
		t.Fatalf("injects = %d, want the open + the re-sent reminder", len(got))
	}
	if p := payloadFields(t, got[1].Body); p["repeat_count"] != float64(1) || got[1].SessionID != "sess-1" {
		t.Fatalf("re-send = %s %v, want reminder 1 into sess-1", got[1].SessionID, p)
	}
}

// TestDispatchRepeat_PartialOpenResendsOpen: the open created the
// session but its initial delivery failed. The agent's first message
// must be the full open payload, never reminder 1 ("nothing changed")
// for an incident it has never seen.
func TestDispatchRepeat_PartialOpenResendsOpen(t *testing.T) {
	t.Parallel()
	base, fd := newFaultyDaemon(t)
	r := wireRepeat(t, newRecoveryDispatcher(t, base))
	sig := unschedulableSignal()
	fd.setFail(func(string) int { return http.StatusInternalServerError })
	r.emit(0, sig) // session created, delivery failed: bound anyway
	fd.setFail(func(string) int { return 0 })
	r.emit(time.Minute, sig)
	got := fd.captured()
	if len(got) != 1 {
		t.Fatalf("injects = %d, want the re-sent open", len(got))
	}
	p := payloadFields(t, got[0].Body)
	if got[0].SessionID != "sess-1" || p["kind"] != engine.KindK8sEvent || p["repeat_count"] != nil {
		t.Fatalf("first delivered message = %s %v, want the full k8s-event open into sess-1", got[0].SessionID, p)
	}
	if got := testutil.ToFloat64(r.d.metrics.repeatFollowups.WithLabelValues("open")); got != 1 {
		t.Errorf("repeat_followups{type=open} = %v, want 1", got)
	}
	// And the schedule carries on from the re-send: the next lapse is
	// reminder 1.
	r.emit(5*time.Minute+5*time.Second, sig)
	got = fd.captured()
	if p := payloadFields(t, got[len(got)-1].Body); p["repeat_count"] != float64(1) {
		t.Fatalf("lapse after the re-sent open = %v, want reminder 1", p)
	}
}
