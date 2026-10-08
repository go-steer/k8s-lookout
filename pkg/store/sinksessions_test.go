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

package store

import (
	"context"
	"testing"
	"time"
)

func TestSinkSessions_RoundTripPerCluster(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	if _, ok, err := s.SinkSession(ctx, "prod", "incident/prod/u1/CrashLoopBackOff"); ok || err != nil {
		t.Fatalf("empty store: ok=%v err=%v, want not found", ok, err)
	}
	if err := s.PutSinkSession(ctx, "prod", "incident/prod/u1/CrashLoopBackOff", "lookout-a/attach-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSinkSession(ctx, "prod", "incident/prod/u1/CrashLoopBackOff", "lookout-a/attach-2"); err != nil {
		t.Fatal(err)
	}
	id, ok, err := s.SinkSession(ctx, "prod", "incident/prod/u1/CrashLoopBackOff")
	if err != nil || !ok || id != "lookout-a/attach-2" {
		t.Errorf("got (%q, %v, %v), want the latest id", id, ok, err)
	}
	if _, ok, _ := s.SinkSession(ctx, "staging", "incident/prod/u1/CrashLoopBackOff"); ok {
		t.Error("sessions must not leak across clusters")
	}
}

func TestSinkSessions_NilStoreIsANoOp(t *testing.T) {
	var s *Store
	ctx := context.Background()
	if err := s.PutSinkSession(ctx, "prod", "k", "id"); err != nil {
		t.Errorf("nil store put: %v", err)
	}
	if _, ok, err := s.SinkSession(ctx, "prod", "k"); ok || err != nil {
		t.Errorf("nil store get: ok=%v err=%v", ok, err)
	}
}

// A session nobody reopened within the retention window is pruned.
func TestSinkSessions_PrunedByTTL(t *testing.T) {
	s, clock := openTest(t, WithTTL(24*time.Hour))
	ctx := context.Background()
	if err := s.PutSinkSession(ctx, "prod", "old", "t/attach-1"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(48 * time.Hour)
	if err := s.PutSinkSession(ctx, "prod", "fresh", "t/attach-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PruneOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.SinkSession(ctx, "prod", "old"); ok {
		t.Error("stale session survived the TTL prune")
	}
	if _, ok, _ := s.SinkSession(ctx, "prod", "fresh"); !ok {
		t.Error("fresh session was pruned")
	}
}
