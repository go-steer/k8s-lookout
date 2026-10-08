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

// Keyed-open dispatcher tests (issue #590): a sink with the
// inject.KeyedOpener capability (the ax sink) is opened with lookout's
// own incident identity, never asked to guess it from the payload.

package watch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// keyedSink records which verbs the dispatcher used and with what key.
type keyedSink struct {
	mu         sync.Mutex
	opens      []inject.IncidentKey
	creates    []inject.IncidentKey
	plainOpens int
	plainNew   int
	appends    []string
}

func (s *keyedSink) OpenIncidentKeyed(_ context.Context, key inject.IncidentKey, _ any) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opens = append(s.opens, key)
	return fmt.Sprintf("task/open-%d", len(s.opens)), nil
}

func (s *keyedSink) CreateSessionKeyed(_ context.Context, key inject.IncidentKey) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates = append(s.creates, key)
	return fmt.Sprintf("task/board-%d", len(s.creates)), nil
}

func (s *keyedSink) OpenIncident(context.Context, any) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plainOpens++
	return "plain", nil
}

func (s *keyedSink) CreateSession(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plainNew++
	return "plain", nil
}

func (s *keyedSink) Append(_ context.Context, id string, _ any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appends = append(s.appends, id)
	return nil
}

func newKeyedDispatcher(sink inject.Sink) *dispatcher {
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	return &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:    dedup,
		injector: sink,
		metrics:  newMetrics(),
		cluster:  "prod-us-central1",
		mode:     "per-incident",
	}
}

// The open carries the cluster and the canonical dedup key. kubelet's
// generic BackOff reason with a crash-loop message is the same incident
// as CrashLoopBackOff, so both name the same key.
func TestKeyedDispatch_OpenCarriesTheCanonicalIncidentKey(t *testing.T) {
	t.Parallel()
	sink := &keyedSink{}
	d := newKeyedDispatcher(sink)

	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	d.DispatchSignal(context.Background(), sig)

	if sink.plainOpens != 0 {
		t.Errorf("plain OpenIncident called %d times; a KeyedOpener must be opened by key", sink.plainOpens)
	}
	if len(sink.opens) != 1 {
		t.Fatalf("keyed opens = %v, want 1", sink.opens)
	}
	want := inject.IncidentKey{Cluster: "prod-us-central1", Kind: inject.IncidentKeyIncident, ID: "abc-123/CrashLoopBackOff"}
	if sink.opens[0] != want {
		t.Errorf("key = %+v, want %+v", sink.opens[0], want)
	}
}

// A storm's key is its ancestor, not its (class-level) fingerprint.
func TestKeyedDispatch_StormKeyIsTheAncestor(t *testing.T) {
	t.Parallel()
	d := newKeyedDispatcher(&keyedSink{})
	got := d.stormKey(engine.Ancestor{Kind: "Node", Name: "gke-pool-1-abcd"})
	want := inject.IncidentKey{Cluster: "prod-us-central1", Kind: inject.IncidentKeyStorm, ID: "Node//gke-pool-1-abcd"}
	if got != want {
		t.Errorf("storm key = %+v, want %+v", got, want)
	}
}

// The watchboard opens its sessions by the cluster's watchboard key,
// keeping the SessionOpener wire order (empty session, then the digest).
func TestKeyedDispatch_WatchboardOpensByClusterKey(t *testing.T) {
	t.Parallel()
	sink := &keyedSink{}
	d := newKeyedDispatcher(sink)
	board := newWatchboard(watchboardConfig{
		injector:      sink,
		metrics:       d.metrics,
		cluster:       d.cluster,
		batch:         100,
		flushInterval: time.Minute,
		rotateAfter:   200,
	})
	board.bind = d.bindWatchboardIncident
	ctx := context.Background()
	board.Add(ctx, warningSignal(1), 1)
	board.FlushNow(ctx)

	if sink.plainNew != 0 || sink.plainOpens != 0 {
		t.Errorf("plain verbs used (CreateSession=%d, OpenIncident=%d); want the keyed ones", sink.plainNew, sink.plainOpens)
	}
	want := inject.IncidentKey{Cluster: "prod-us-central1", Kind: inject.IncidentKeyWatchboard}
	if len(sink.creates) != 1 || sink.creates[0] != want {
		t.Fatalf("CreateSessionKeyed keys = %+v, want [%+v]", sink.creates, want)
	}
	if len(sink.appends) != 1 || sink.appends[0] != "task/board-1" {
		t.Errorf("digest appends = %v, want one into task/board-1", sink.appends)
	}
}
