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

	"github.com/go-steer/k8s-lookout/pkg/memory/distill"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// runDistillLoop drives the §9.2 distiller on the --distill-interval
// cadence: one pass shortly after startup (the store may already
// hold a week of evidence from previous runs — facts should not
// wait six hours for it), then one per interval. The sentinel-store
// binding (rather than core-agent's shared Memory interface, which
// v2.7.0 does not ship) is documented in pkg/memory.
//
// A pass is pure derivation over the occurrence window: failures are
// logged and counted (distill_errors_total), never fatal — the next
// pass re-derives everything.
func runDistillLoop(ctx context.Context, st *store.Store, interval time.Duration, m *metrics) {
	// Startup delay keeps the first pass out of the sentinel's
	// informer-sync window; one minute is invisible against a 6h
	// cadence and keeps tests able to override via interval.
	const startupDelay = time.Minute
	first := time.NewTimer(min(startupDelay, interval))
	defer first.Stop()
	select {
	case <-ctx.Done():
		return
	case <-first.C:
	}
	runDistillPass(ctx, st, m)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runDistillPass(ctx, st, m)
		}
	}
}

func runDistillPass(ctx context.Context, st *store.Store, m *metrics) {
	stats, err := distill.Run(ctx, st, st, distill.Config{})
	if err != nil {
		if ctx.Err() != nil {
			return // shutdown race, not a distiller failure
		}
		m.distillErrors.Inc()
		log.Printf("distill: pass failed (facts stay stale until the next pass): %v", err)
		return
	}
	for class, n := range stats.Written {
		m.memoryFacts.WithLabelValues(class).Add(float64(n))
	}
	log.Printf("distill: pass complete (scanned=%d occurrences, wrote %d fact(s) across %d class(es))",
		stats.Scanned, stats.Total(), len(stats.Written))
}
