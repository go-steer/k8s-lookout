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

import "github.com/go-steer/k8s-lookout/pkg/engine"

// ClearanceObserver exposes the source to the §7.4 recovery tracker.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

// Clearance implements engine.ClearanceObserver for leeway findings (DESIGN
// §7.4: every source that can observe a symptom can observe its absence).
//
// The symptom is an open §8.2 episode. A finding is cleared when the machine no
// longer holds the subject-axis in a firing phase — which is precisely the
// state `Route` would decline to emit from — and the resolution is
// object_deleted when the subject has left the pod index entirely.
//
// **This is not a second resolve dwell.** §8.2 already holds a finding
// outstanding for `Dwell.Resolve` after the breach clears, and PhaseResolving
// reports Firing, so this observer keeps saying "symptomatic" for the whole of
// it. The recovery tracker's own `--recovery-stable-for` then runs on top. The
// two windows compose rather than compete: §8.2 decides when the *drift* is
// over, the tracker decides when it has been over long enough to tell anybody.
//
// ok=false for incidents this source did not raise, and for a process whose
// caches have not synced — an empty mirror makes every subject look deleted,
// which is the one wrong answer that is also irreversible.
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	sub, key, ok := parseFindingUID(inc.Key.UID)
	if !ok {
		return engine.Clearance{}, false
	}
	if !s.HasSynced() {
		return engine.Clearance{}, false
	}

	if !s.state.Tracked(sub) {
		// No objects left under this subject. StableSince stays zero: the
		// deletion is the fact, and dating it from a sweep that merely noticed
		// would be inventing a timestamp.
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}

	st, live := s.alerts.StateOf(sub, key)
	switch {
	case live && st.Phase.Firing():
		return engine.Clearance{}, true
	case live:
		// Pending: the episode dropped under threshold and then breached again
		// inside the resolve dwell, or this is a fresh episode on a subject
		// whose previous finding is still tracked. Either way the drift is
		// back, and the incident is not clear.
		return engine.Clearance{}, true
	}
	// The episode ran to a resolve and left the machine, taking its ClearSince
	// with it, so there is nothing left to date the absence from and
	// StableSince stays zero — "absent as of this observation only", which
	// makes the tracker count its own window from here. That is not a loss:
	// §8.2's thirty-minute resolve dwell has already elapsed by the time this
	// branch is reachable, so the drift has been gone far longer than any
	// stability window the tracker would have imposed.
	return engine.Clearance{Cleared: true, Resolution: engine.ResolutionRecovered}, true
}
