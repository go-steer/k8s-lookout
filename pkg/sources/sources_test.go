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

package sources

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// fakeSource is a scriptable Source for registry/runner tests. Its
// run func defaults to "emit nothing, block until cancelled".
type fakeSource struct {
	name  string
	scope Scope
	run   func(ctx context.Context, emit func(Signal)) error
	reqs  []Requirement
}

func (f *fakeSource) Name() string { return f.name }
func (f *fakeSource) Scope() Scope { return f.scope }
func (f *fakeSource) Run(ctx context.Context, emit func(Signal)) error {
	if f.run != nil {
		return f.run(ctx, emit)
	}
	<-ctx.Done()
	return nil
}
func (f *fakeSource) RequiredAccess() []Requirement { return f.reqs }

// syncingSource is a fakeSource that also reports a sync barrier.
type syncingSource struct {
	fakeSource
	synced bool
}

func (s *syncingSource) HasSynced() bool { return s.synced }

// TestAllSynced covers the readiness predicate behind /readyz: the
// poll-driven sources have no barrier and must not hold the sentinel
// un-ready, while a single unsynced informer source must.
func TestAllSynced(t *testing.T) {
	t.Parallel()
	pollDriven := &fakeSource{name: "expiry"}
	ready := &syncingSource{fakeSource: fakeSource{name: "k8s-events"}, synced: true}
	listing := &syncingSource{fakeSource: fakeSource{name: "objectstate"}}

	if ok, who := AllSynced([]Source{pollDriven}); !ok || who != "" {
		t.Errorf("a source with no barrier must be ready by definition; got %v %q", ok, who)
	}
	if ok, who := AllSynced([]Source{pollDriven, ready}); !ok || who != "" {
		t.Errorf("all barriers crossed: got %v %q", ok, who)
	}
	if ok, who := AllSynced([]Source{pollDriven, ready, listing}); ok || who != "objectstate" {
		t.Errorf("one source still listing: got %v %q, want false \"objectstate\"", ok, who)
	}
	if ok, who := AllSynced(nil); !ok || who != "" {
		t.Errorf("no sources: got %v %q, want vacuously true", ok, who)
	}
}

func TestRegistry_RegisterLookupAll(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	a := &fakeSource{name: "alpha", scope: ScopeCluster}
	b := &fakeSource{name: "beta", scope: ScopeNamespace}
	if err := r.Register(a); err != nil {
		t.Fatalf("Register(alpha): %v", err)
	}
	if err := r.Register(b); err != nil {
		t.Fatalf("Register(beta): %v", err)
	}
	got, ok := r.Lookup("alpha")
	if !ok || got != Source(a) {
		t.Errorf("Lookup(alpha) = %v, %v", got, ok)
	}
	if _, ok := r.Lookup("gamma"); ok {
		t.Error("Lookup(gamma) should miss")
	}
	all := r.All()
	if len(all) != 2 || all[0].Name() != "alpha" || all[1].Name() != "beta" {
		t.Errorf("All() should preserve registration order; got %v", all)
	}
}

func TestRegistry_RejectsDuplicateAndEmptyNames(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	if err := r.Register(&fakeSource{name: "k8s-events"}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := r.Register(&fakeSource{name: "k8s-events"})
	if err == nil || !strings.Contains(err.Error(), "k8s-events") {
		t.Errorf("duplicate Register should fail naming the source; got %v", err)
	}
	if err := r.Register(&fakeSource{name: ""}); err == nil {
		t.Error("empty-name Register should fail")
	}
}

func TestScope_String(t *testing.T) {
	t.Parallel()
	cases := map[Scope]string{
		ScopeNamespace: "namespace",
		ScopeCluster:   "cluster",
		ScopeProject:   "project",
		Scope(42):      "Scope(42)",
	}
	for scope, want := range cases {
		if got := scope.String(); got != want {
			t.Errorf("Scope(%d).String() = %q, want %q", int(scope), got, want)
		}
	}
}

func TestRunAll_DeliversSignalsAndStopsOnCancel(t *testing.T) {
	t.Parallel()
	emitted := &fakeSource{name: "emitter", run: func(ctx context.Context, emit func(Signal)) error {
		emit(Signal{Kind: engine.KindK8sEvent, TriageEvent: engine.TriageEvent{
			Key: engine.EventKey{UID: "u1", Reason: "CrashLoopBackOff"},
		}})
		<-ctx.Done()
		return nil
	}}

	var (
		mu   sync.Mutex
		got  []Signal
		seen = make(chan struct{}, 1)
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunAll(ctx, []Source{emitted}, func(sig Signal) {
			mu.Lock()
			got = append(got, sig)
			mu.Unlock()
			select {
			case seen <- struct{}{}:
			default:
			}
		})
	}()
	select {
	case <-seen:
	case <-time.After(3 * time.Second):
		t.Fatal("no signal emitted within timeout")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RunAll after clean cancel: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].Key.UID != "u1" {
		t.Errorf("emitted signals = %+v", got)
	}
}

func TestRunAll_SourceFailureCancelsSiblingsAndNamesSource(t *testing.T) {
	t.Parallel()
	boom := errors.New("informer exploded")
	failing := &fakeSource{name: "failing", run: func(context.Context, func(Signal)) error {
		return boom
	}}
	// The sibling blocks on ctx — RunAll must cancel it (no hang)
	// when the failing source errors.
	sibling := &fakeSource{name: "sibling"}

	err := RunAll(context.Background(), []Source{failing, sibling}, func(Signal) {})
	if err == nil {
		t.Fatal("RunAll should surface the source failure")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error should wrap the source's error; got %v", err)
	}
	if !strings.Contains(err.Error(), `source "failing"`) {
		t.Errorf("error should name the failing source; got %v", err)
	}
}
