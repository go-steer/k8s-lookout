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

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// deferSiblingResolved handles recovery coalescing for sibling-folded
// per-incident sessions (§7.7, issue #641): when multiple pods of a
// workload folded into one session, an individual pod clearing while
// another sibling pod in the same group is still open must NOT send a
// premature kind=resolved into the shared session. Instead, individual
// member keys flip to resolved as they clear, and the single coalesced
// kind=resolved inject goes out when the last open member clears.
func (d *dispatcher) deferSiblingResolved(ctx context.Context, sig engine.Signal, sid string) bool {
	if sig.Kind != engine.KindResolved || d.mode != "per-incident" {
		return false
	}
	rootKey, groupKeys, isGroup := d.dedup.SiblingGroup(sig.Key)
	if !isGroup {
		return false
	}
	if d.triage != nil {
		d.triage.resolve(ctx, sig)
	}
	d.dedup.MarkResolved(sig.Key)
	d.metrics.activeIncidents.Set(float64(d.dedup.ActiveBoundCount()))
	if d.tracker != nil {
		for _, k := range groupKeys {
			if d.tracker.Open(k) {
				d.store.Record(sig, store.Outcome{Route: store.RouteResolved, SessionID: sid})
				return true
			}
		}
	}
	d.dedup.MarkResolved(rootKey)
	d.metrics.activeIncidents.Set(float64(d.dedup.ActiveBoundCount()))
	return false
}
