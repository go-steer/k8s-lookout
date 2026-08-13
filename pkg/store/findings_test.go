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
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/findings"
)

// testCluster is the cluster every fixture row belongs to; the
// accessors are cluster-scoped, so it has to match.
const testCluster = "prod-east"

func testFindingState(key string) findings.State {
	return findings.State{
		SubjectKey:   key,
		Fingerprint:  "sha256:crash",
		Cluster:      testCluster,
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "payment-backend",
		Reason:       "CrashLoopBackOff",
		Severity:     "critical",
		FirstSeen:    t0.Add(-2 * time.Hour),
		LastSeen:     t0,
	}
}

// TestFindingState_RoundTrip: a state set survives the swap with its
// timestamps intact — FirstSeen especially, which is the "broken for
// two hours" number the diff carries forward rather than recomputes.
func TestFindingState_RoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	want := testFindingState("prod-east/prod/Pod/payment-backend/CrashLoopBackOff")
	if err := s.ReplaceFindingStates(ctx, testCluster, []findings.State{want}); err != nil {
		t.Fatalf("ReplaceFindingStates: %v", err)
	}
	got, err := s.FindingStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FindingStates returned %d, want 1", len(got))
	}
	g := got[0]
	if g.SubjectKey != want.SubjectKey || g.Fingerprint != want.Fingerprint ||
		g.Cluster != want.Cluster || g.Namespace != want.Namespace ||
		g.KindOfObject != want.KindOfObject || g.Name != want.Name ||
		g.Reason != want.Reason || g.Severity != want.Severity {
		t.Errorf("identity round-trip: %+v", g)
	}
	if !g.FirstSeen.Equal(want.FirstSeen) || !g.LastSeen.Equal(want.LastSeen) {
		t.Errorf("stamps = first %s last %s, want %s / %s", g.FirstSeen, g.LastSeen, want.FirstSeen, want.LastSeen)
	}
	if !g.AckUntil.IsZero() || g.AckBy != "" {
		t.Errorf("unacked row came back acked: until %s by %q", g.AckUntil, g.AckBy)
	}
}

// TestFindingState_ReplaceIsAWholeSetSwap: the write is the complete
// new state, not a merge. A subject the differ resolved must be GONE
// after the swap — if it lingered, the next run would keep reporting
// it ongoing forever.
func TestFindingState_ReplaceIsAWholeSetSwap(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	first := []findings.State{
		testFindingState("a/ns/Pod/one/CrashLoopBackOff"),
		testFindingState("b/ns/Pod/two/ImagePullBackOff"),
	}
	if err := s.ReplaceFindingStates(ctx, testCluster, first); err != nil {
		t.Fatalf("ReplaceFindingStates: %v", err)
	}

	// Second run: "two" resolved, "three" appeared.
	second := []findings.State{
		testFindingState("a/ns/Pod/one/CrashLoopBackOff"),
		testFindingState("c/ns/Pod/three/NodeNotReady"),
	}
	if err := s.ReplaceFindingStates(ctx, testCluster, second); err != nil {
		t.Fatalf("ReplaceFindingStates (2nd): %v", err)
	}

	got, err := s.FindingStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	var keys []string
	for _, st := range got {
		keys = append(keys, st.SubjectKey)
	}
	if len(keys) != 2 || keys[0] != "a/ns/Pod/one/CrashLoopBackOff" || keys[1] != "c/ns/Pod/three/NodeNotReady" {
		t.Errorf("after swap keys = %v, want the second set in key order", keys)
	}
}

// TestFindingState_ReplaceWithEmptyClears: everything recovered ⇒ no
// open subjects. The empty set has to be writable, or a fully-healthy
// run could never clear the table.
func TestFindingState_ReplaceWithEmptyClears(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	if err := s.ReplaceFindingStates(ctx, testCluster, []findings.State{testFindingState("a/ns/Pod/one/Crash")}); err != nil {
		t.Fatalf("ReplaceFindingStates: %v", err)
	}
	if err := s.ReplaceFindingStates(ctx, testCluster, nil); err != nil {
		t.Fatalf("ReplaceFindingStates(nil): %v", err)
	}
	got, err := s.FindingStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("FindingStates after empty swap = %d rows, want 0", len(got))
	}
}

// TestFindingState_ClustersAreIsolated: two clusters can share one
// store file. Without the cluster scope on both the read and the
// swap, diffing cluster B would wipe cluster A's rows and the next A
// run would report every one of them resolved — a fleet-wide false
// all-clear.
func TestFindingState_ClustersAreIsolated(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	east := testFindingState("prod-east/prod/Pod/one/Crash")
	west := testFindingState("prod-west/prod/Pod/two/Crash")
	west.Cluster = "prod-west"

	if err := s.ReplaceFindingStates(ctx, "prod-east", []findings.State{east}); err != nil {
		t.Fatalf("ReplaceFindingStates(east): %v", err)
	}
	if err := s.ReplaceFindingStates(ctx, "prod-west", []findings.State{west}); err != nil {
		t.Fatalf("ReplaceFindingStates(west): %v", err)
	}

	for cluster, wantKey := range map[string]string{
		"prod-east": east.SubjectKey,
		"prod-west": west.SubjectKey,
	} {
		got, err := s.FindingStates(ctx, cluster)
		if err != nil {
			t.Fatalf("FindingStates(%s): %v", cluster, err)
		}
		if len(got) != 1 || got[0].SubjectKey != wantKey {
			t.Errorf("FindingStates(%s) = %+v, want just %s", cluster, got, wantKey)
		}
	}

	// A healthy run on west must not resolve east either.
	if err := s.ReplaceFindingStates(ctx, "prod-west", nil); err != nil {
		t.Fatalf("ReplaceFindingStates(west, nil): %v", err)
	}
	got, err := s.FindingStates(ctx, "prod-east")
	if err != nil {
		t.Fatalf("FindingStates(east): %v", err)
	}
	if len(got) != 1 {
		t.Errorf("clearing west left east with %d rows, want 1", len(got))
	}
}

// TestFindingState_ReplaceRejectsForeignRows: a row whose cluster does
// not match the swap's scope would be written where the next read
// cannot see it. Refuse rather than write it into a blind spot.
func TestFindingState_ReplaceRejectsForeignRows(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	err := s.ReplaceFindingStates(ctx, "prod-west", []findings.State{testFindingState("prod-east/prod/Pod/one/Crash")})
	if err == nil {
		t.Fatal("swapping a foreign cluster's row should error")
	}
	if !strings.Contains(err.Error(), "prod-east") || !strings.Contains(err.Error(), "prod-west") {
		t.Errorf("error does not name both clusters: %v", err)
	}
}

// TestFindingState_AckRoundTrip: an ack lands on the row, survives a
// read, and is visible to findings.State.Acked at the boundary it was
// written with.
func TestFindingState_AckRoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	key := "prod-east/prod/Pod/payment-backend/CrashLoopBackOff"
	if err := s.ReplaceFindingStates(ctx, testCluster, []findings.State{testFindingState(key)}); err != nil {
		t.Fatalf("ReplaceFindingStates: %v", err)
	}
	until := t0.Add(4 * time.Hour)
	acked, err := s.AckFinding(ctx, key, "gari", until)
	if err != nil {
		t.Fatalf("AckFinding: %v", err)
	}
	if !acked.AckUntil.Equal(until) || acked.AckBy != "gari" {
		t.Errorf("AckFinding returned until %s by %q, want %s / gari", acked.AckUntil, acked.AckBy, until)
	}
	if !acked.Acked(t0) {
		t.Error("fresh ack is not open at t0")
	}
	if acked.Acked(until.Add(time.Second)) {
		t.Error("ack still open past its expiry")
	}

	got, err := s.FindingStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	if len(got) != 1 || !got[0].AckUntil.Equal(until) || got[0].AckBy != "gari" {
		t.Errorf("ack did not survive the read: %+v", got)
	}
}

// TestFindingState_AckSurvivesAReplace: acks are taken between runs,
// so the next diff's swap must carry them — findings.Diff copies
// AckUntil/AckBy into Next, and the store must not drop them.
func TestFindingState_AckSurvivesAReplace(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	key := "prod-east/prod/Pod/payment-backend/CrashLoopBackOff"
	if err := s.ReplaceFindingStates(ctx, testCluster, []findings.State{testFindingState(key)}); err != nil {
		t.Fatalf("ReplaceFindingStates: %v", err)
	}
	until := t0.Add(4 * time.Hour)
	if _, err := s.AckFinding(ctx, key, "gari", until); err != nil {
		t.Fatalf("AckFinding: %v", err)
	}
	prev, err := s.FindingStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}

	// A diff run one hour later, inside the window.
	now := t0.Add(time.Hour)
	res := findings.Diff(prev, []findings.Observation{{
		SubjectKey:   key,
		Fingerprint:  "sha256:crash",
		Cluster:      "prod-east",
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "payment-backend",
		Reason:       "CrashLoopBackOff",
		Severity:     "critical",
	}}, now)
	if len(res.Changes) != 1 || res.Changes[0].Transition != findings.TransitionSuppressed {
		t.Fatalf("changes = %+v, want one suppressed", res.Changes)
	}
	if err := s.ReplaceFindingStates(ctx, testCluster, res.Next); err != nil {
		t.Fatalf("ReplaceFindingStates (2nd): %v", err)
	}
	got, err := s.FindingStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	if len(got) != 1 || !got[0].AckUntil.Equal(until) || got[0].AckBy != "gari" {
		t.Errorf("ack lost across the swap: %+v", got)
	}
}

// TestFindingState_AckUnknownSubject: acking a key that is not open
// is an error, not a row-creating upsert — see AckFinding's doc.
func TestFindingState_AckUnknownSubject(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	if _, err := s.AckFinding(ctx, "nope/ns/Pod/x/Crash", "gari", t0.Add(time.Hour)); err == nil {
		t.Error("acking an unknown subject should error")
	}
	if _, err := s.UnackFinding(ctx, "nope/ns/Pod/x/Crash"); err == nil {
		t.Error("unacking an unknown subject should error")
	}
	got, err := s.FindingStates(ctx, testCluster)
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("a failed ack created %d rows, want 0", len(got))
	}
}

// TestFindingState_AckValidation: an empty key or a zero expiry is
// rejected before touching the database — an "ack" with no expiry is
// the standing override that §9.4 owns, not this table.
func TestFindingState_AckValidation(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	if _, err := s.AckFinding(ctx, "", "gari", t0.Add(time.Hour)); err == nil {
		t.Error("empty subject key should error")
	}
	if _, err := s.AckFinding(ctx, "a/ns/Pod/x/Crash", "gari", time.Time{}); err == nil {
		t.Error("zero expiry should error")
	}
}

// TestFindingState_Unack: clearing a window returns the row with no
// ack, and the subject goes back to being diffed normally.
func TestFindingState_Unack(t *testing.T) {
	t.Parallel()
	s, _ := openTest(t)
	ctx := context.Background()

	key := "prod-east/prod/Pod/payment-backend/CrashLoopBackOff"
	if err := s.ReplaceFindingStates(ctx, testCluster, []findings.State{testFindingState(key)}); err != nil {
		t.Fatalf("ReplaceFindingStates: %v", err)
	}
	if _, err := s.AckFinding(ctx, key, "gari", t0.Add(4*time.Hour)); err != nil {
		t.Fatalf("AckFinding: %v", err)
	}
	cleared, err := s.UnackFinding(ctx, key)
	if err != nil {
		t.Fatalf("UnackFinding: %v", err)
	}
	if !cleared.AckUntil.IsZero() || cleared.AckBy != "" {
		t.Errorf("UnackFinding left an ack: until %s by %q", cleared.AckUntil, cleared.AckBy)
	}
	if cleared.Acked(t0) {
		t.Error("cleared row still reports as acked")
	}
	// Idempotent on an already-clear row.
	if _, err := s.UnackFinding(ctx, key); err != nil {
		t.Errorf("second UnackFinding: %v", err)
	}
}

// TestFindingState_NilAndReadOnly: a nil store reads nothing and
// refuses writes; an OpenRead handle serves the diff's read half but
// refuses to persist — a read-only consumer can still SEE transitions,
// it just cannot advance the state.
func TestFindingState_NilAndReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var nilStore *Store
	if got, err := nilStore.FindingStates(ctx, testCluster); err != nil || got != nil {
		t.Errorf("nil store FindingStates = %v, %v; want nil, nil", got, err)
	}
	if err := nilStore.ReplaceFindingStates(ctx, testCluster, nil); err == nil {
		t.Error("nil store ReplaceFindingStates should error")
	}
	if _, err := nilStore.AckFinding(ctx, "k", "gari", t0); err == nil {
		t.Error("nil store AckFinding should error")
	}
	if _, err := nilStore.UnackFinding(ctx, "k"); err == nil {
		t.Error("nil store UnackFinding should error")
	}

	path := filepath.Join(t.TempDir(), "lookout.db")
	w, err := Open(path, WithLogf(t.Logf), WithClock((&testClock{now: t0}).Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	key := "prod-east/prod/Pod/payment-backend/CrashLoopBackOff"
	if err := w.ReplaceFindingStates(ctx, testCluster, []findings.State{testFindingState(key)}); err != nil {
		t.Fatalf("ReplaceFindingStates: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	defer func() { _ = r.Close() }()
	if got, err := r.FindingStates(ctx, testCluster); err != nil || len(got) != 1 {
		t.Errorf("read-only FindingStates = %d, %v; want 1, nil", len(got), err)
	}
	if err := r.ReplaceFindingStates(ctx, testCluster, nil); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only ReplaceFindingStates = %v, want ErrReadOnlyStore", err)
	}
	if _, err := r.AckFinding(ctx, key, "gari", t0.Add(time.Hour)); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only AckFinding = %v, want ErrReadOnlyStore", err)
	}
	if _, err := r.UnackFinding(ctx, key); !errors.Is(err, ErrReadOnlyStore) {
		t.Errorf("read-only UnackFinding = %v, want ErrReadOnlyStore", err)
	}
}

// TestFindingState_PreV6Store: a store written before migration v6
// answers "no state" on reads — never "no such table" — so the first
// diff against an upgraded file reports everything new instead of
// failing. Writes say plainly that the schema is too old.
func TestFindingState_PreV6Store(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create version table: %v", err)
	}
	for _, m := range migrations[:findingStateSchemaVersion-1] {
		if _, err := db.Exec(m); err != nil {
			t.Fatalf("apply old migration: %v", err)
		}
	}
	if _, err := db.Exec(`INSERT INTO schema_version (version) VALUES (?)`, findingStateSchemaVersion-1); err != nil {
		t.Fatalf("set version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	ctx := context.Background()
	r, err := OpenRead(path, WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	if got, err := r.FindingStates(ctx, testCluster); err != nil || got != nil {
		t.Errorf("pre-v6 FindingStates = %v, %v; want nil, nil", got, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A read-write open migrates the file forward, so the table is
	// there and writes work: the upgrade path costs the operator
	// nothing but one all-`new` run.
	w, err := Open(path, WithLogf(t.Logf), WithClock((&testClock{now: t0}).Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()
	if err := w.ReplaceFindingStates(ctx, testCluster, []findings.State{testFindingState("a/ns/Pod/one/Crash")}); err != nil {
		t.Errorf("ReplaceFindingStates after upgrade: %v", err)
	}
}
