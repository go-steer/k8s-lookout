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
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// historyFixture is a store + graph wired the way the sentinel wires
// them: the graph's OnChange feeds RecordGraphChange, and one fake
// clock stamps both the change records and the snapshot rows.
type historyFixture struct {
	t     *testing.T
	store *Store
	graph *graph.Graph
	now   time.Time
	path  string
}

func newHistoryFixture(t *testing.T) *historyFixture {
	t.Helper()
	f := &historyFixture{
		t:    t,
		now:  time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		path: filepath.Join(t.TempDir(), "lookout.db"),
	}
	s, err := Open(f.path, WithClock(func() time.Time { return f.now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	f.store = s
	f.graph = graph.New(graph.Options{
		SwapInterval: -1,
		OnChange:     s.RecordGraphChange,
		Now:          func() time.Time { return f.now },
	})
	return f
}

func (f *historyFixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func (f *historyFixture) seed(objs ...any) {
	f.t.Helper()
	if err := f.graph.Writer().FromObjects(slices.Values(objs)); err != nil {
		f.t.Fatal(err)
	}
}

func (f *historyFixture) apply(op graph.Op, obj any) *graph.Snapshot {
	f.t.Helper()
	w := f.graph.Writer()
	if err := w.Apply(graph.Delta{Op: op, Object: obj}); err != nil {
		f.t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		f.t.Fatal(err)
	}
	f.store.Flush()
	return f.snap()
}

func (f *historyFixture) snap() *graph.Snapshot {
	f.t.Helper()
	s, err := f.graph.Snapshot()
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}

func (f *historyFixture) putSnapshot() *graph.Snapshot {
	f.t.Helper()
	s := f.snap()
	if err := f.store.PutGraphSnapshot(context.Background(), s); err != nil {
		f.t.Fatal(err)
	}
	return s
}

// assertSameTopology compares a GraphAt result with a live snapshot
// through the public surface: generation, node/edge counts, and a
// per-identity spot check.
func assertSameTopology(t *testing.T, got, want *graph.Snapshot) {
	t.Helper()
	if got.Generation() != want.Generation() {
		t.Errorf("generation: got %d, want %d", got.Generation(), want.Generation())
	}
	if got.NumNodes() != want.NumNodes() || got.NumEdges() != want.NumEdges() {
		t.Errorf("topology size: got %d nodes/%d edges, want %d/%d",
			got.NumNodes(), got.NumEdges(), want.NumNodes(), want.NumEdges())
	}
}

func histPod(name, node string, image string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: name, UID: types.UID("uid-" + name)},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "app", Image: image}},
		},
	}
}

func histNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// TestGraphAt_NearestSnapshotPlusReplay is the §6.6 resolution
// contract, exact boundaries included:
//
//	t0 S1 · t1 c1 · t2 c2 · t3 S2 · t4 c3
//
//	GraphAt(t0)      == S1        (snapshot's own instant)
//	GraphAt(t1-1ns)  == S1        (change at t1 excluded)
//	GraphAt(t1)      == S1+c1     (boundary inclusive)
//	GraphAt(t2)      == S1+c1+c2  (S2 is later — not eligible)
//	GraphAt(t3)      == S2        (no replay needed)
//	GraphAt(t4)      == S2+c3
//	GraphAt(<t0)     == ErrNoHistory
func TestGraphAt_NearestSnapshotPlusReplay(t *testing.T) {
	t.Parallel()
	f := newHistoryFixture(t)
	ctx := context.Background()
	f.seed(histNode("n1"), histPod("pay-1", "n1", "img:v1"))

	t0 := f.now
	s1 := f.putSnapshot()

	f.advance(time.Minute)
	t1 := f.now
	afterC1 := f.apply(graph.OpAdd, histPod("pay-2", "n1", "img:v1"))

	f.advance(time.Minute)
	t2 := f.now
	afterC2 := f.apply(graph.OpDelete, histPod("pay-1", "n1", "img:v1"))

	f.advance(time.Minute)
	t3 := f.now
	s2 := f.putSnapshot()

	f.advance(time.Minute)
	t4 := f.now
	afterC3 := f.apply(graph.OpAdd, histPod("pay-3", "n1", "img:v2"))

	for _, tc := range []struct {
		name string
		at   time.Time
		want *graph.Snapshot
	}{
		{"snapshot instant", t0, s1},
		{"just before first change", t1.Add(-time.Nanosecond), s1},
		{"change boundary inclusive", t1, afterC1},
		{"mid-log", t2, afterC2},
		{"second snapshot instant", t3, s2},
		{"replay after second snapshot", t4, afterC3},
		{"far future", t4.Add(24 * time.Hour), afterC3},
	} {
		got, err := f.store.GraphAt(ctx, tc.at)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		t.Run(tc.name, func(t *testing.T) { assertSameTopology(t, got, tc.want) })
	}

	// The replayed mid-log state must answer identity queries: pay-1
	// deleted, pay-2 present.
	got, err := f.store.GraphAt(ctx, t2)
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := got.Lookup(graph.KindPod, "shop", "pay-2"); !ok {
		t.Error("pay-2 missing from replayed graph")
	} else if ref, _ := got.Resolve(id); !ref.Observed {
		t.Errorf("pay-2 not observed: %+v", ref)
	}
	if id, ok := got.Lookup(graph.KindPod, "shop", "pay-1"); ok {
		if ref, _ := got.Resolve(id); ref.Observed {
			t.Errorf("deleted pay-1 still observed at t2: %+v", ref)
		}
	}

	if _, err := f.store.GraphAt(ctx, t0.Add(-time.Second)); !errors.Is(err, ErrNoHistory) {
		t.Errorf("GraphAt before first snapshot: got %v, want ErrNoHistory", err)
	}
}

// TestGraphAt_DisabledAndPreHistoryStores: nil store and stores
// without the v2 tables answer ErrNoHistory / nothing, never an
// sqlite error.
func TestGraphAt_DisabledAndPreHistoryStores(t *testing.T) {
	t.Parallel()
	var nilStore *Store
	if _, err := nilStore.GraphAt(context.Background(), time.Now()); !errors.Is(err, ErrNoHistory) {
		t.Errorf("nil store: got %v, want ErrNoHistory", err)
	}
	nilStore.RecordGraphChange(graph.ChangeRecord{}) // must not panic
	if err := nilStore.PutGraphSnapshot(context.Background(), nil); err != nil {
		t.Errorf("nil store PutGraphSnapshot: %v", err)
	}
	if rows, err := nilStore.GraphChanges(context.Background(), time.Time{}, time.Now()); err != nil || rows != nil {
		t.Errorf("nil store GraphChanges: %v, %v", rows, err)
	}
}

// TestGraphChanges_TriageFeed: the delta log rows come back in
// chronological order with identity, op, and the FieldChanges
// summary intact — the `triage changes` contract.
func TestGraphChanges_TriageFeed(t *testing.T) {
	t.Parallel()
	f := newHistoryFixture(t)
	f.seed(histNode("n1"), histPod("pay-1", "n1", "img:v1"))
	start := f.now

	f.advance(time.Minute)
	f.apply(graph.OpUpdate, histPod("pay-1", "n1", "img:v2"))
	f.advance(time.Minute)
	f.apply(graph.OpDelete, histPod("pay-1", "n1", "img:v2"))

	rows, err := f.store.GraphChanges(context.Background(), start, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %+v", rows)
	}
	upd, del := rows[0], rows[1]
	if upd.Op != "update" || upd.Kind != "Pod" || upd.Namespace != "shop" || upd.Name != "pay-1" {
		t.Errorf("update row: %+v", upd)
	}
	if len(upd.FieldChanges) != 1 || upd.FieldChanges[0].Path != "container/app/image" ||
		upd.FieldChanges[0].From != "img:v1" || upd.FieldChanges[0].To != "img:v2" {
		t.Errorf("update field changes: %+v", upd.FieldChanges)
	}
	if del.Op != "delete" || len(del.FieldChanges) != 0 {
		t.Errorf("delete row: %+v", del)
	}
	if !upd.At.Before(del.At) {
		t.Errorf("rows out of order: %v then %v", upd.At, del.At)
	}

	// Window edges: from is exclusive, to inclusive.
	if rows, _ := f.store.GraphChanges(context.Background(), upd.At, del.At); len(rows) != 1 || rows[0].Op != "delete" {
		t.Errorf("window (updAt, delAt]: %+v", rows)
	}
}

// TestPrune_KeepsNewestSnapshot: TTL expiry removes old snapshots
// and change rows but ALWAYS keeps the newest snapshot, even when it
// is itself past TTL.
func TestPrune_KeepsNewestSnapshot(t *testing.T) {
	t.Parallel()
	f := newHistoryFixture(t)
	ctx := context.Background()
	f.seed(histNode("n1"))
	f.putSnapshot() // old snapshot

	f.advance(time.Hour)
	f.apply(graph.OpAdd, histPod("pay-1", "n1", "img:v1"))
	f.advance(time.Hour)
	newest := f.putSnapshot()

	// Jump far past TTL: everything above is expired.
	f.advance(DefaultTTL + time.Hour)
	stats, err := f.store.PruneOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.TTLRows == 0 {
		t.Fatal("prune removed nothing")
	}

	var snapshots, changes int
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM graph_snapshots`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM graph_changes`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if snapshots != 1 {
		t.Errorf("want exactly the newest snapshot kept, got %d", snapshots)
	}
	if changes != 0 {
		t.Errorf("want change log emptied by TTL, got %d rows", changes)
	}

	// The surviving snapshot is the newest one and still resolves.
	got, err := f.store.GraphAt(ctx, f.now)
	if err != nil {
		t.Fatal(err)
	}
	assertSameTopology(t, got, newest)
}

// TestPrune_SizeBoundCoversHistoryTables: with occurrences empty,
// the size bound falls through to graph_changes (oldest first) and
// then snapshots — still keeping the newest snapshot.
func TestPrune_SizeBoundCoversHistoryTables(t *testing.T) {
	t.Parallel()
	f := newHistoryFixture(t)
	ctx := context.Background()
	f.seed(histNode("n1"))
	f.putSnapshot()
	for i := range 200 {
		f.advance(time.Second)
		f.apply(graph.OpAdd, histPod(fmt.Sprintf("pay-%d", i), "n1", "img:v1"))
	}
	f.advance(time.Second)
	f.putSnapshot()

	f.store.max = 1 // force the size path
	if _, err := f.store.PruneOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var snapshots, changes int
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM graph_snapshots`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM graph_changes`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes != 0 {
		t.Errorf("size prune left %d change rows under a 1-byte budget", changes)
	}
	if snapshots != 1 {
		t.Errorf("size prune must keep exactly the newest snapshot, got %d", snapshots)
	}
}

// TestOpenRead_ServesGraphAt: the one-shot CLI path — a read-only
// open of a sentinel's store serves GraphAt/GraphChanges, refuses
// writes, and closes cleanly.
func TestOpenRead_ServesGraphAt(t *testing.T) {
	t.Parallel()
	f := newHistoryFixture(t)
	ctx := context.Background()
	f.seed(histNode("n1"), histPod("pay-1", "n1", "img:v1"))
	want := f.putSnapshot()
	at := f.now
	f.store.Flush()

	ro, err := OpenRead(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()

	got, err := ro.GraphAt(ctx, at)
	if err != nil {
		t.Fatal(err)
	}
	assertSameTopology(t, got, want)

	if err := ro.PutGraphSnapshot(ctx, want); err == nil {
		t.Error("read-only PutGraphSnapshot must refuse")
	}
	ro.RecordGraphChange(graph.ChangeRecord{Op: graph.OpAdd, Kind: graph.KindPod}) // no-op, no panic
	ro.Flush()                                                                     // no-op, no deadlock
	if _, err := ro.PruneOnce(ctx); err != nil {
		t.Errorf("read-only PruneOnce must no-op: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Errorf("double close: %v", err)
	}
}

// TestOpenRead_MissingFile: a wrong --store path is a clear error,
// not a fresh empty database.
func TestOpenRead_MissingFile(t *testing.T) {
	t.Parallel()
	if _, err := OpenRead(filepath.Join(t.TempDir(), "nope.db")); err == nil {
		t.Fatal("OpenRead of a missing file must fail")
	}
	if _, err := OpenRead(""); err == nil {
		t.Fatal("OpenRead of an empty path must fail")
	}
}

// TestGraphSnapshot_RowMetadata pins the bookkeeping columns:
// resource_version carries the graph generation, format_version the
// codec version, size_bytes the blob length.
func TestGraphSnapshot_RowMetadata(t *testing.T) {
	t.Parallel()
	f := newHistoryFixture(t)
	f.seed(histNode("n1"))
	snap := f.putSnapshot()

	var rv, fv, size int64
	var epoch string
	var data []byte
	if err := f.store.db.QueryRow(
		`SELECT resource_version, format_version, size_bytes, epoch, data FROM graph_snapshots`).
		Scan(&rv, &fv, &size, &epoch, &data); err != nil {
		t.Fatal(err)
	}
	if uint64(rv) != snap.Generation() {
		t.Errorf("resource_version: got %d, want generation %d", rv, snap.Generation())
	}
	if fv != graph.SnapshotFormatVersion {
		t.Errorf("format_version: got %d, want %d", fv, graph.SnapshotFormatVersion)
	}
	if size != int64(len(data)) || size == 0 {
		t.Errorf("size_bytes %d != len(data) %d", size, len(data))
	}
	if epoch == "" || epoch != f.store.epoch {
		t.Errorf("epoch: got %q, want the writer process's id %q ('' is reserved for pre-migration rows)", epoch, f.store.epoch)
	}
}

// TestGraphAt_AcrossSentinelRestart is the M3 drill observation 1
// regression: epoch A writes a snapshot and changes, the sentinel
// "restarts" (store reopened, FRESH graph — new interner, generation
// counter back at 1), epoch B writes its own snapshot and changes.
// GraphAt must resolve inside each epoch without ever replaying one
// epoch's delta log onto the other's snapshots, and a t in the gap
// between epochs resolves to the prior epoch's last state (the
// documented boundary semantics).
func TestGraphAt_AcrossSentinelRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "lookout.db")
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	// ---- Epoch A ----
	stA, err := Open(path, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	gA := graph.New(graph.Options{SwapInterval: -1, OnChange: stA.RecordGraphChange, Now: clock})
	wA := gA.Writer()
	if err := wA.FromObjects(slices.Values([]any{histNode("n1"), histPod("a-1", "n1", "img:v1")})); err != nil {
		t.Fatal(err)
	}
	t0 := now
	snapA, err := gA.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := stA.PutGraphSnapshot(ctx, snapA); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	t1 := now // epoch A change: pod a-2 appears (generation 2 > B's snapshot generation)
	if err := wA.Apply(graph.Delta{Op: graph.OpAdd, Object: histPod("a-2", "n1", "img:v1")}); err != nil {
		t.Fatal(err)
	}
	if err := wA.Flush(); err != nil {
		t.Fatal(err)
	}
	stA.Flush()
	if err := stA.Close(); err != nil {
		t.Fatal(err)
	}

	// ---- The restart gap: nothing observes the cluster here. ----
	now = now.Add(time.Minute)
	tGap := now

	// ---- Epoch B: fresh process, fresh graph, DIFFERENT interning
	// order and object set, generation reset — the shape that made
	// pre-epoch replay fail with "bad change effect". ----
	now = now.Add(time.Minute)
	stB, err := Open(path, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stB.Close() }()
	gB := graph.New(graph.Options{SwapInterval: -1, OnChange: stB.RecordGraphChange, Now: clock})
	wB := gB.Writer()
	if err := wB.FromObjects(slices.Values([]any{histPod("b-1", "n1", "img:v2"), histNode("n1")})); err != nil {
		t.Fatal(err)
	}
	t3 := now
	snapB, err := gB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := stB.PutGraphSnapshot(ctx, snapB); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	t4 := now
	if err := wB.Apply(graph.Delta{Op: graph.OpAdd, Object: histPod("b-2", "n1", "img:v2")}); err != nil {
		t.Fatal(err)
	}
	if err := wB.Flush(); err != nil {
		t.Fatal(err)
	}
	stB.Flush()

	lookup := func(s *graph.Snapshot, name string) bool {
		id, ok := s.Lookup(graph.KindPod, "shop", name)
		if !ok {
			return false
		}
		ref, _ := s.Resolve(id)
		return ref.Observed
	}
	for _, tc := range []struct {
		name    string
		at      time.Time
		present []string
		absent  []string
	}{
		{"epoch A baseline", t0, []string{"a-1"}, []string{"a-2", "b-1", "b-2"}},
		{"epoch A + replay", t1, []string{"a-1", "a-2"}, []string{"b-1", "b-2"}},
		{"gap resolves to prior epoch's last state", tGap, []string{"a-1", "a-2"}, []string{"b-1", "b-2"}},
		{"epoch B baseline", t3, []string{"b-1"}, []string{"a-1", "a-2", "b-2"}},
		{"epoch B + replay", t4, []string{"b-1", "b-2"}, []string{"a-1", "a-2"}},
	} {
		got, err := stB.GraphAt(ctx, tc.at)
		if err != nil {
			t.Fatalf("%s: GraphAt(%s): %v (pre-epoch stores failed here with 'bad change effect')", tc.name, tc.at, err)
		}
		for _, name := range tc.present {
			if !lookup(got, name) {
				t.Errorf("%s: pod %s must be present", tc.name, name)
			}
		}
		for _, name := range tc.absent {
			if lookup(got, name) {
				t.Errorf("%s: pod %s from the other epoch leaked in", tc.name, name)
			}
		}
	}

	// The two processes really wrote two distinct, non-empty epochs.
	var epochs int
	if err := stB.db.QueryRow(`SELECT COUNT(DISTINCT epoch) FROM graph_snapshots WHERE epoch != ''`).Scan(&epochs); err != nil {
		t.Fatal(err)
	}
	if epochs != 2 {
		t.Errorf("want 2 distinct non-empty snapshot epochs, got %d", epochs)
	}
}

// TestGraphAt_LegacyEpochBackfill pins the migration's backfill
// semantics: rows carrying the pre-migration default epoch ” are ONE
// epoch, and GraphAt resolves them exactly as before the epoch
// columns existed.
func TestGraphAt_LegacyEpochBackfill(t *testing.T) {
	t.Parallel()
	f := newHistoryFixture(t)
	ctx := context.Background()
	f.seed(histNode("n1"), histPod("pay-1", "n1", "img:v1"))
	f.putSnapshot()
	f.advance(time.Minute)
	want := f.apply(graph.OpAdd, histPod("pay-2", "n1", "img:v1"))

	// Blank the epoch stamps: this is what rows written before
	// migration v5 look like after ALTER TABLE's DEFAULT '' backfill.
	if _, err := f.store.db.Exec(`UPDATE graph_snapshots SET epoch = ''`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.Exec(`UPDATE graph_changes SET epoch = ''`); err != nil {
		t.Fatal(err)
	}
	got, err := f.store.GraphAt(ctx, f.now)
	if err != nil {
		t.Fatal(err)
	}
	assertSameTopology(t, got, want)
	if id, ok := got.Lookup(graph.KindPod, "shop", "pay-2"); !ok {
		t.Error("pay-2 missing: legacy '' rows must replay as one epoch")
	} else if ref, _ := got.Resolve(id); !ref.Observed {
		t.Errorf("pay-2 not observed: %+v", ref)
	}
}

// TestGraphAt_PreEpochSchemaReadOnly: a store from a sentinel older
// than migration v5 (graph-history tables, no epoch columns) opened
// read-only takes the single-epoch legacy query path.
func TestGraphAt_PreEpochSchemaReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations[:graphHistorySchemaVersion] {
		if _, err := db.Exec(m); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_version (version) VALUES (?)`, graphHistorySchemaVersion); err != nil {
		t.Fatal(err)
	}

	// One snapshot + one change, produced by the real graph writer and
	// inserted through the OLD column set.
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	var recs []graph.ChangeRecord
	g := graph.New(graph.Options{
		SwapInterval: -1,
		OnChange:     func(r graph.ChangeRecord) { recs = append(recs, r) },
		Now:          func() time.Time { return now },
	})
	w := g.Writer()
	if err := w.FromObjects(slices.Values([]any{histNode("n1"), histPod("pay-1", "n1", "img:v1")})); err != nil {
		t.Fatal(err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	enc, err := snap.Encode()
	if err != nil {
		t.Fatal(err)
	}
	t0 := now
	if _, err := db.Exec(`INSERT INTO graph_snapshots (taken_at, resource_version, format_version, size_bytes, data)
		VALUES (?,?,?,?,?)`, t0.UnixNano(), int64(snap.Generation()), graph.SnapshotFormatVersion, len(enc), enc); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := w.Apply(graph.Delta{Op: graph.OpAdd, Object: histPod("pay-2", "n1", "img:v1")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("want 1 change record, got %d", len(recs))
	}
	if _, err := db.Exec(`INSERT INTO graph_changes (at, generation, op, kind, namespace, name, uid, changes, effect)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		recs[0].At.UnixNano(), int64(recs[0].Generation), recs[0].Op.String(), recs[0].Kind.String(),
		recs[0].Namespace, recs[0].Name, recs[0].UID, "[]", recs[0].Effect); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := OpenRead(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if ro.schemaVersion >= graphEpochSchemaVersion {
		t.Fatalf("fixture is not pre-epoch (schema v%d)", ro.schemaVersion)
	}
	got, err := ro.GraphAt(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := got.Lookup(graph.KindPod, "shop", "pay-2"); !ok {
		t.Error("pay-2 missing from pre-epoch store resolution")
	} else if ref, _ := got.Resolve(id); !ref.Observed {
		t.Errorf("pay-2 not observed: %+v", ref)
	}
}

// TestGraphAt_PreservesWatches (M3 drill observation 2): the sentinel
// feed's partial watched set survives the store round-trip — both off
// the bare snapshot and through change replay — so history-mode
// radius keeps the #46 unknown-vs-missing honesty.
func TestGraphAt_PreservesWatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "lookout.db")
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	st, err := Open(path, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	g := graph.New(graph.Options{
		SwapInterval: -1,
		OnChange:     st.RecordGraphChange,
		Now:          clock,
		WatchedKinds: []graph.NodeKind{graph.KindPod, graph.KindNode, graph.KindReplicaSet},
	})
	w := g.Writer()
	if err := w.FromObjects(slices.Values([]any{histNode("n1"), histPod("pay-1", "n1", "img:v1")})); err != nil {
		t.Fatal(err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutGraphSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	tSnap := now
	now = now.Add(time.Minute)
	if err := w.Apply(graph.Delta{Op: graph.OpAdd, Object: histPod("pay-2", "n1", "img:v1")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	st.Flush()

	for name, at := range map[string]time.Time{"snapshot verbatim": tSnap, "after replay": now} {
		got, err := st.GraphAt(ctx, at)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, k := range []graph.NodeKind{graph.KindPod, graph.KindNode, graph.KindReplicaSet} {
			if !got.Watches(k) {
				t.Errorf("%s: watched kind %v lost through the store", name, k)
			}
		}
		for _, k := range []graph.NodeKind{graph.KindDeployment, graph.KindConfigMap, graph.KindSecret} {
			if got.Watches(k) {
				t.Errorf("%s: unwatched kind %v reads watched — history answers regress to pre-#46 mislabeling", name, k)
			}
		}
	}
}
