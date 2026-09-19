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
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// scaleLog remembers when each workload's declared size last changed. It is
// the recent-scale row of docs/leeway-design.md §7.6: a subject whose replica
// count moved a minute ago has a distribution the scheduler is still filling
// in, and judging its balance against the plan is judging a half-finished
// sentence.
//
// Kubernetes records no timestamp for a spec.replicas change — the resource
// version moves and `metadata.generation` increments, but neither says when,
// and generation also increments for template edits. So the change has to be
// *watched*, which means the log only knows about changes that happened while
// this process was running. A workload first seen at startup is therefore
// dated zero, never now, for the same reason a node first seen already
// cordoned is (§7.6): guessing "now" would relax every subject in the cluster
// for a whole settle window after every restart, and a suppression that
// switches itself on at startup is worse than one that under-reports.
//
// Entries are kept for as long as the workload exists rather than expiring
// with the settle window, because the replica count in the entry is the
// baseline the *next* change is measured against — dropping it would make
// that change look like another first sighting.
type scaleLog struct {
	mu sync.Mutex
	at map[leeway.SubjectRef]scaleEntry
}

// scaleEntry is one workload's declared size and when it was last seen to
// change. A zero changedAt means "never observed changing", which covers both
// a workload that has held its size since this process started and one it
// inherited at startup.
type scaleEntry struct {
	replicas  int32
	changedAt time.Time
}

func newScaleLog() *scaleLog {
	return &scaleLog{at: make(map[leeway.SubjectRef]scaleEntry)}
}

// Observe records a workload's current declared size, stamping now when it
// differs from the size already on file. The first sighting stamps nothing.
func (l *scaleLog) Observe(sub leeway.SubjectRef, replicas int32, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev, known := l.at[sub]
	switch {
	case !known:
		l.at[sub] = scaleEntry{replicas: replicas}
	case prev.replicas != replicas:
		l.at[sub] = scaleEntry{replicas: replicas, changedAt: now}
	}
	// Same size as last time: leave the entry alone. Re-stamping on every
	// resync would turn the informer's own 30-second relist into a permanent
	// scale event.
}

// Forget drops a workload's entry. A name that comes back is a new workload
// as far as this log is concerned, and gets a first sighting rather than
// inheriting the old one's size — which is right, because it really is a new
// object and its replica count was never compared against anything.
func (l *scaleLog) Forget(sub leeway.SubjectRef) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.at, sub)
}

// ScaledAt returns when this subject's replica count was last seen to change,
// or the zero time. An unknown subject — a DaemonSet, a Job, a Custom
// subject, or a Deployment in a cluster where the apps informers never
// started — reads zero, which §7.6 treats as "not recently scaled" rather
// than as an error.
func (l *scaleLog) ScaledAt(sub leeway.SubjectRef) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.at[sub].changedAt
}

// Len reports how many workloads are on file.
func (l *scaleLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.at)
}
