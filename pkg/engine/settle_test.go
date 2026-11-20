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

package engine

import (
	"testing"
	"time"
)

func TestFilter_Settle_DefaultDisabledPassesUnchanged(t *testing.T) {
	t.Parallel()
	// Default FilterConfig has settle == 0 (opt-in): CrashLoopBackOff at count=3
	// with a 5s span passes immediately.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	t0 := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	sig := makeEvent("BackOff", "default", 3)
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(5 * time.Second)
	if ok, gate := f.Decide(sig); !ok || gate != "" {
		t.Fatalf("default settle=0 Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestFilter_Settle_FamilyAndAllowlist(t *testing.T) {
	t.Parallel()
	// Include Failed in allowedReasons so the allowlisted Failed startup-race
	// messages reach the settle gate.
	reasons := append([]string(nil), defaultReasons...)
	reasons = append(reasons, "Failed")
	f := NewFilter(NewFilterConfig(reasons, nil, nil, 0, 0, 0).WithSettle(2 * time.Minute))
	t0 := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	settledCases := []struct {
		name    string
		reason  string
		message string
		count   int
	}{
		{
			name:    "kubelet BackOff restarting failed container",
			reason:  "BackOff",
			message: "Back-off restarting failed container app in pod app-0",
			count:   3,
		},
		{
			name:    "explicit CrashLoopBackOff",
			reason:  "CrashLoopBackOff",
			message: "container app is in CrashLoopBackOff",
			count:   3,
		},
		{
			name:    "Failed allowlist host IP unknown",
			reason:  "Failed",
			message: "Error: host IP unknown; known addresses: []",
			count:   1,
		},
		{
			name:    "Failed allowlist etc-hosts missing",
			reason:  "Failed",
			message: "Error: open /var/lib/kubelet/pods/abc/etc-hosts: no such file or directory",
			count:   1,
		},
	}

	for _, tc := range settledCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sig := makeEvent(tc.reason, "default", tc.count)
			sig.Message = tc.message
			sig.FirstSeen = t0
			sig.LastSeen = t0.Add(119 * time.Second)
			if ok, gate := f.Decide(sig); ok || gate != GateSettle {
				t.Errorf("below settle (119s): Decide = (%v, %q), want (false, %q)", ok, gate, GateSettle)
			}

			// Exact boundary (>= settle) passes.
			sig.LastSeen = t0.Add(2 * time.Minute)
			if ok, gate := f.Decide(sig); !ok || gate != "" {
				t.Errorf("at settle (120s): Decide = (%v, %q), want (true, \"\")", ok, gate)
			}

			// Zero timestamps fail open.
			zeroFirst := sig
			zeroFirst.FirstSeen = time.Time{}
			zeroFirst.LastSeen = t0.Add(10 * time.Second)
			if ok, gate := f.Decide(zeroFirst); !ok || gate != "" {
				t.Errorf("zero FirstSeen: Decide = (%v, %q), want fail-open (true, \"\")", ok, gate)
			}
			zeroLast := sig
			zeroLast.FirstSeen = t0
			zeroLast.LastSeen = time.Time{}
			if ok, gate := f.Decide(zeroLast); !ok || gate != "" {
				t.Errorf("zero LastSeen: Decide = (%v, %q), want fail-open (true, \"\")", ok, gate)
			}
		})
	}
}

func TestFilter_Settle_CountGateRunsBeforeSettle(t *testing.T) {
	t.Parallel()
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 3, 0).WithSettle(2 * time.Minute))
	t0 := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	sig := makeEvent("BackOff", "default", 2)
	sig.Message = "Back-off restarting failed container worker in pod worker-0"
	sig.FirstSeen = t0
	sig.LastSeen = t0.Add(10 * time.Second)
	if ok, gate := f.Decide(sig); ok || gate != GateCrashLoopDebounce {
		t.Fatalf("count=2 < min=3 Decide = (%v, %q), want (false, %q)", ok, gate, GateCrashLoopDebounce)
	}
}

func TestFilter_Settle_ExemptReasonsNeverSettled(t *testing.T) {
	t.Parallel()
	reasons := append([]string(nil), defaultReasons...)
	reasons = append(reasons, "Failed")
	f := NewFilter(NewFilterConfig(reasons, nil, nil, 3, 3, 3).WithSettle(2 * time.Minute))
	t0 := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	exemptCases := []struct {
		name    string
		reason  string
		message string
		count   int
	}{
		{name: "FailedMount", reason: "FailedMount", message: "MountVolume.SetUp failed", count: 1},
		{name: "Unhealthy", reason: "Unhealthy", message: "Readiness probe failed", count: 3},
		{name: "ImagePullBackOff", reason: "ImagePullBackOff", message: "Back-off pulling image", count: 1},
		{name: "ErrImagePull", reason: "ErrImagePull", message: "manifest unknown", count: 1},
		{name: "BackOff image pull", reason: "BackOff", message: "Back-off pulling image \"bad:tag\"", count: 1},
		{name: "FailedScheduling", reason: "FailedScheduling", message: "0/3 nodes are available", count: 1},
		{name: "OOMKilled", reason: "OOMKilled", message: "container OOMKilled", count: 1},
		{name: "Evicted", reason: "Evicted", message: "The node was low on resource", count: 1},
		{name: "NodeNotReady", reason: "NodeNotReady", message: "Node is not ready", count: 1},
		{name: "NetworkNotReady", reason: "NetworkNotReady", message: "network is not ready", count: 1},
		{name: "non-allowlisted Failed", reason: "Failed", message: "Error: CreateContainerConfigError: secret not found", count: 1},
		{name: "partial prefix Failed without etc-hosts suffix", reason: "Failed", message: "Error: open /etc/passwd: permission denied", count: 1},
	}

	for _, tc := range exemptCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sig := makeEvent(tc.reason, "default", tc.count)
			sig.Message = tc.message
			sig.FirstSeen = t0
			sig.LastSeen = t0 // span == 0, would fail GateSettle if not exempt
			if ok, gate := f.Decide(sig); !ok || gate != "" {
				t.Errorf("exempt %s: Decide = (%v, %q), want (true, \"\")", tc.name, ok, gate)
			}
		})
	}

	// Non-k8s-event source-namespaced signal is never caught by GateSettle.
	srcSig := Signal{
		Kind:     "objectstate.restart_burst",
		Source:   SourceSentinel,
		Severity: SeverityCritical,
		TriageEvent: TriageEvent{
			Key:       EventKey{UID: "u-src", Reason: "CrashLoopBackOff"},
			Namespace: "default",
			Count:     5,
			FirstSeen: t0,
			LastSeen:  t0,
		},
	}
	if ok, gate := f.Decide(srcSig); !ok || gate != "" {
		t.Errorf("source-namespaced signal Decide = (%v, %q), want (true, \"\")", ok, gate)
	}
}

func TestDedupCache_LastRetiredTS_ReadOnlyDoesNotMutateTombstone(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	c, err := NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	c.SetClock(func() time.Time { return now })

	key := EventKey{UID: "pod-1", Reason: "BackOff"}
	// Unseen key -> false.
	if ts, ok := c.LastRetiredTS(key); ok || !ts.IsZero() {
		t.Fatalf("unseen key LastRetiredTS = (%v, %v), want (zero, false)", ts, ok)
	}

	// Open incident, bind, and resolve.
	tEvent := now
	res := c.ObserveSignal(key, Observation{LastTS: tEvent, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true})
	if res.Kind != DedupNewIncident {
		t.Fatalf("open = %v, want DedupNewIncident", res.Kind)
	}
	c.BindSession(key, "sess-1")
	// Still open -> LastRetiredTS must return false.
	if _, ok := c.LastRetiredTS(key); ok {
		t.Fatal("open incident unexpectedly reported by LastRetiredTS")
	}

	now = now.Add(2 * time.Minute)
	c.MarkResolved(key)
	// Resolved live entry (before sweep into tombstone) -> returns tEvent.
	if ts, ok := c.LastRetiredTS(key); !ok || !ts.Equal(tEvent) {
		t.Fatalf("resolved live entry LastRetiredTS = (%v, %v), want (%v, true)", ts, ok, tEvent)
	}

	// Advance past dedup window and sweep so entry retires into #816 tombstone.
	now = now.Add(6 * time.Minute)
	c.Sweep()
	if ts, ok := c.LastRetiredTS(key); !ok || !ts.Equal(tEvent) {
		t.Fatalf("retired tombstone LastRetiredTS = (%v, %v), want (%v, true)", ts, ok, tEvent)
	}

	// Calling LastRetiredTS must NOT delete or mutate the #816 replay tombstone:
	// an unchanged relist at tEvent must still be suppressed as Replay!
	replay := c.ObserveSignal(key, Observation{LastTS: tEvent, Kind: KindK8sEvent, Severity: SeverityCritical, Repeatable: true})
	if replay.Kind != DedupDuplicate || !replay.Replay {
		t.Fatalf("after LastRetiredTS, unchanged relist = %+v, want Replay=true", replay)
	}
}
