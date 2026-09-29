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

package topologydrift

import (
	"sync/atomic"
	"time"
)

// eventResource is which informer delivered a watch event.
type eventResource int

const (
	eventPod eventResource = iota
	eventNode
	eventDeployment
	eventStatefulSet
	numEventResources
)

// label is the `resource` value, shared with last_event_timestamp so the two
// series join.
func (r eventResource) label() string {
	switch r {
	case eventPod:
		return resourcePod
	case eventNode:
		return resourceNode
	case eventDeployment:
		return resourceDeployment
	default:
		return resourceStatefulSet
	}
}

// Watch-event outcomes. Inert is the §6.3 early return: the event changed
// nothing leeway indexes, which for a pod is almost always status churn.
const (
	outcomeInert   = "inert"
	outcomeApplied = "applied"
)

// watchEvents counts every event the six informer handlers are delivered, by
// resource and by whether it moved the index (#491).
//
// It is the denominator §6.6.1's cost model is written in. Without it the
// kwok ladder can only divide CPU by evaluations, and that overstates the
// per-event cost by however many events the coalescer folded into each one.
//
// Plain atomics read at scrape time rather than a synchronous OTel counter,
// for two reasons. The informer handlers are registered before the
// instruments are declared, so a synchronous counter would drop the initial
// sync, and the initial sync is a large share of what the ladder measures.
// And this is the hottest path in the subsystem: one atomic add costs less
// than building an attribute set on every event.
type watchEvents struct {
	n [numEventResources][2]atomic.Int64
}

// note counts one delivered event.
func (w *watchEvents) note(r eventResource, applied bool) {
	i := 0
	if applied {
		i = 1
	}
	w.n[r][i].Add(1)
}

// each yields every resource × outcome total, zeros included, so all eight
// series exist from the first scrape.
func (w *watchEvents) each(yield func(resource, outcome string, n int64)) {
	for r := eventResource(0); r < numEventResources; r++ {
		yield(r.label(), outcomeInert, w.n[r][0].Load())
		yield(r.label(), outcomeApplied, w.n[r][1].Load())
	}
}

// lastEvents is the instant of the last pod and node event, per resource, as
// unix seconds (#499). It feeds last_event_timestamp the same way watchEvents
// feeds the counter, and for the same two reasons: the stamps of the initial
// sync, delivered before the instruments exist, now survive; and a
// synchronous gauge Record with a per-call attribute set was ~490 ns and 4
// allocations on every event, 80% of an inert pod update's cost.
type lastEvents struct {
	unix [numEventResources]atomic.Int64
}

// stamp records an event for r at at.
func (l *lastEvents) stamp(r eventResource, at time.Time) {
	l.unix[r].Store(at.Unix())
}

// each yields the resources that have seen an event. A resource that has not
// is withheld rather than reported as 1970: a zero timestamp reads as an
// informer silent for fifty years, which is precisely the alert this gauge
// exists to drive.
func (l *lastEvents) each(yield func(resource string, unix int64)) {
	for r := eventResource(0); r < numEventResources; r++ {
		if v := l.unix[r].Load(); v != 0 {
			yield(r.label(), v)
		}
	}
}
