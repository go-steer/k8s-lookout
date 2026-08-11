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

// Package watch implements `lookout watch`, the resident per-cluster
// sentinel. It watches Kubernetes Events via a client-go informer,
// filters to a configured allow-list of Event.Reason values, dedupes
// duplicates within a rolling window, and POSTs matched events to a
// core-agent daemon's per-incident session endpoint.
package watch

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Main is the `lookout watch` entry point; argv is the argument list
// after the subcommand name. It follows the standalone binary's 0/1
// exit-code convention and prints errors with a "lookout watch:"
// stderr prefix.
func Main(argv []string) int {
	if err := realMain(argv); err != nil {
		fmt.Fprintln(os.Stderr, "lookout watch:", err)
		return 1
	}
	return 0
}

// runRecoveryLoop drives the tracker on an interval and keeps the
// recovery_tracking gauge current. Exits when ctx is cancelled.
func runRecoveryLoop(ctx context.Context, tracker *engine.RecoveryTracker, m *metrics) {
	t := time.NewTicker(recoveryTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tracker.Tick()
			m.recoveryTracking.Set(float64(tracker.Len()))
		}
	}
}

// runSnapshotLoop persists the dedup cache to disk on an interval
// so a sidecar crash doesn't lose more than interval seconds of
// state. Exits when ctx is cancelled.
func runSnapshotLoop(ctx context.Context, cache *engine.DedupCache, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := cache.Snapshot(); err != nil {
				log.Printf("dedup snapshot: %v", err)
			}
		}
	}
}
