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
	"log"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// graphSnapshotter is the slice of *store.Store the snapshot loop
// needs; an interface so the loop's timing behavior is testable
// without SQLite.
type graphSnapshotter interface {
	PutGraphSnapshot(ctx context.Context, snap *graph.Snapshot) error
}

var _ graphSnapshotter = (*store.Store)(nil)

// graphHistoryBootstrapPoll bounds how quickly the FIRST snapshot
// lands after the graph feed finishes initial sync: change records
// logged before any snapshot exists are unreplayable ("blast radius
// at onset" needs a baseline), so the loop polls fast until the
// baseline is stored, then settles onto the configured interval.
const graphHistoryBootstrapPoll = 10 * time.Second

// runGraphHistoryLoop persists periodic §6.6 topology snapshots.
// Cadence is --graph-snapshot-interval (default 5m); a generation
// unchanged since the last stored snapshot is skipped (identical
// topology, and with no interceding change records GraphAt resolves
// those times off the previous snapshot anyway). Failures are loud
// and retried next tick — a missed snapshot widens the replay
// window, never breaks it.
func runGraphHistoryLoop(ctx context.Context, snapshot func() (*graph.Snapshot, error), st graphSnapshotter, interval time.Duration) {
	poll := graphHistoryBootstrapPoll
	if poll > interval {
		poll = interval
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	var lastStored uint64
	bootstrapped := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		snap, err := snapshot()
		if err != nil {
			continue // initial sync still running (graph.ErrNotReady)
		}
		if bootstrapped && snap.Generation() == lastStored {
			continue
		}
		if err := st.PutGraphSnapshot(ctx, snap); err != nil {
			if ctx.Err() == nil {
				log.Printf("graph history: snapshot failed (generation %d, retrying next tick): %v", snap.Generation(), err)
			}
			continue
		}
		lastStored = snap.Generation()
		if !bootstrapped {
			bootstrapped = true
			t.Reset(interval)
			log.Printf("graph history: baseline snapshot stored (generation %d, %d nodes, %d edges) — --at queries answerable from here on",
				snap.Generation(), snap.NumNodes(), snap.NumEdges())
		}
	}
}
