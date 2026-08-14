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

import "testing"

// TestFingerprint_PinnedVectors pins example fingerprints to their
// exact values.
//
// FROZEN CONTRACT — these vectors are a cross-cluster wire contract,
// not an implementation detail. Fingerprints are persisted in
// incident records and joined on by fleet consumers across clusters running
// DIFFERENT lookout versions; if this test fails, the change breaks
// every fleet rollup mid-upgrade. Fix the code to restore the pinned
// values — never update the vectors. (Every hash is independently
// derivable: `printf 'kind\x00reasonClass\x00objectClass\x00zone' |
// sha256sum`.)
func TestFingerprint_PinnedVectors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                                 string
		kind, reasonClass, objectClass, zone string
		want                                 string
	}{
		{
			name: "k8s-event crashloop with zone",
			kind: "k8s-event", reasonClass: "CrashLoopBackOff", objectClass: "Pod", zone: "us-east1-b",
			want: "sha256:82fbc19ad7e86147ed4e0cb80bcab970bc75195e8db75df9d3bec4f66fe2153d",
		},
		{
			name: "k8s-event crashloop without zone",
			kind: "k8s-event", reasonClass: "CrashLoopBackOff", objectClass: "Pod", zone: "",
			want: "sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b",
		},
		{
			name: "k8s-event image pull (canonical reason family)",
			kind: "k8s-event", reasonClass: "ImagePullBackOff", objectClass: "Pod", zone: "us-east1-b",
			want: "sha256:0a3458d04f73d63991f71bf30c0fcb00d79c6db571d7a3f76031b309d74cce8b",
		},
		{
			name: "k8s-event failed mount",
			kind: "k8s-event", reasonClass: "FailedMount", objectClass: "Pod", zone: "europe-west1-c",
			want: "sha256:9a0eae4b560cb8b88c9c742ffe3a0cb2740b3f480c780669ea29222cd98acea0",
		},
		{
			name: "source-namespaced kind (capacity.stockout)",
			kind: "capacity.stockout", reasonClass: "GCE_STOCKOUT", objectClass: "Node", zone: "us-east1-b",
			want: "sha256:0b58818db890b3e877ba0e455d8da7a8381f61d5a5733a033c46e42d34885850",
		},
		{
			name: "all fields empty (separators alone)",
			kind: "", reasonClass: "", objectClass: "", zone: "",
			want: "sha256:709e80c88487a2411e1ee4dfb9f22a861492d20c4765150c0c794abd70f8147c",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Fingerprint(tc.kind, tc.reasonClass, tc.objectClass, tc.zone)
			if got != tc.want {
				t.Errorf("Fingerprint(%q, %q, %q, %q) = %s, want %s — this is a FROZEN cross-cluster contract; fix the code, do not repin",
					tc.kind, tc.reasonClass, tc.objectClass, tc.zone, got, tc.want)
			}
		})
	}
}

// TestFingerprint_ClassNotInstance verifies the load-bearing design
// property (§8): the fingerprint identifies the failure CLASS, so it
// must be identical for different objects and different clusters, and
// must differ across kinds, reason classes, object classes, and zones.
func TestFingerprint_ClassNotInstance(t *testing.T) {
	t.Parallel()
	base := Fingerprint("k8s-event", "CrashLoopBackOff", "Pod", "us-east1-b")

	// No object name or UID is an input — two crash-looping pods in
	// two clusters of the same zone carry the same fingerprint by
	// construction. Deterministic across calls.
	if again := Fingerprint("k8s-event", "CrashLoopBackOff", "Pod", "us-east1-b"); again != base {
		t.Errorf("fingerprint not deterministic: %s vs %s", base, again)
	}

	// Each input dimension changes the hash.
	for name, other := range map[string]string{
		"kind":         Fingerprint("k8s-event-followup", "CrashLoopBackOff", "Pod", "us-east1-b"),
		"reason-class": Fingerprint("k8s-event", "OOMKilled", "Pod", "us-east1-b"),
		"object-class": Fingerprint("k8s-event", "CrashLoopBackOff", "Node", "us-east1-b"),
		"zone":         Fingerprint("k8s-event", "CrashLoopBackOff", "Pod", "us-west1-a"),
	} {
		if other == base {
			t.Errorf("changing %s did not change the fingerprint", name)
		}
	}

	// The NUL separator makes field boundaries unambiguous: shifting
	// a character between adjacent fields must change the hash.
	if Fingerprint("k8s-eventC", "rashLoopBackOff", "Pod", "") == Fingerprint("k8s-event", "CrashLoopBackOff", "Pod", "") {
		t.Error("field boundaries are ambiguous — separator failed")
	}
}

// TestPostureFingerprint_PinnedVectors pins the posture recipe
// (issue #182). Same frozen-contract rules as the vectors above: the
// #189 fleet rollup joins on these across clusters, so a change here
// is a wire break, not a test update.
func TestPostureFingerprint_PinnedVectors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                      string
		kind, reason, objectClass string
		want                      string
	}{
		{
			name: "workload posture",
			kind: "audit.no_pdb", reason: "NoPodDisruptionBudget", objectClass: "Deployment",
			want: "sha256:7fe356f9a64f75392625206434494ed78e8fd020d5d35459c93fe96db64390ec",
		},
		{
			name: "cluster-scoped posture (no object class)",
			kind: "audit.exemption_expired", reason: "ExemptionExpired", objectClass: "",
			want: "sha256:434e960e0f889a0bdacfb627694794044c0e90071520bcd15ab34592ce8f5c8b",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := PostureFingerprint(tc.kind, tc.reason, tc.objectClass)
			if got != tc.want {
				t.Errorf("PostureFingerprint(%q, %q, %q) = %s, want %s — frozen cross-cluster contract; fix the code, do not repin",
					tc.kind, tc.reason, tc.objectClass, got, tc.want)
			}
		})
	}
}

// TestPostureFingerprint_Recipe pins the three properties that
// distinguish the posture recipe from the scan one. Each is a decision
// recorded in PostureFingerprint's doc comment; a change to any of them
// resplits the fleet rollup.
func TestPostureFingerprint_Recipe(t *testing.T) {
	t.Parallel()

	// Zone is empty by construction: a posture class is a property of
	// a spec, identical wherever the workload lands.
	if got, want := PostureFingerprint("audit.no_pdb", "NoPodDisruptionBudget", "Deployment"),
		Fingerprint("audit.no_pdb", "NoPodDisruptionBudget", "Deployment", ""); got != want {
		t.Errorf("posture recipe must stamp an empty zone: %s vs %s", got, want)
	}

	// The kind is the detector's own, NOT the reactive "k8s-event"
	// kind the scan recipe borrows.
	if PostureFingerprint("audit.no_pdb", "NoPodDisruptionBudget", "Deployment") ==
		Fingerprint(KindK8sEvent, "NoPodDisruptionBudget", "Deployment", "") {
		t.Error("posture findings must not fingerprint under the reactive k8s-event kind")
	}

	// The reason is NOT canonicalized: a posture reason that collides
	// with an event-reason table key must survive intact rather than
	// being silently rewritten into another class. "pending" is a live
	// CanonicalReason mapping, which makes it the sharpest probe.
	if CanonicalReason("pending") == "pending" {
		t.Skip("canonicalization table no longer remaps \"pending\"; pick another colliding key")
	}
	if PostureFingerprint("audit.x", "pending", "Deployment") ==
		Fingerprint("audit.x", CanonicalReason("pending"), "Deployment", "") {
		t.Error("posture reasons must pass through uncanonicalized")
	}

	// Each input still separates classes.
	base := PostureFingerprint("audit.no_pdb", "NoPodDisruptionBudget", "Deployment")
	for name, other := range map[string]string{
		"kind":         PostureFingerprint("audit.singleton", "NoPodDisruptionBudget", "Deployment"),
		"reason":       PostureFingerprint("audit.no_pdb", "PDBBlocksEviction", "Deployment"),
		"object-class": PostureFingerprint("audit.no_pdb", "NoPodDisruptionBudget", "StatefulSet"),
	} {
		if other == base {
			t.Errorf("changing %s did not change the posture fingerprint", name)
		}
	}
}

// TestScanFingerprint_Unchanged guards the frozen scan recipe against
// being "unified" with the posture one. ScanFingerprint's output is
// joined on by every open §9.4 triage-status record.
func TestScanFingerprint_Unchanged(t *testing.T) {
	t.Parallel()
	got := ScanFingerprint("CrashLoopBackOff", "Pod", "us-east1-b")
	const want = "sha256:82fbc19ad7e86147ed4e0cb80bcab970bc75195e8db75df9d3bec4f66fe2153d"
	if got != want {
		t.Errorf("ScanFingerprint = %s, want %s — the scan recipe is frozen; adding the posture recipe must not touch it", got, want)
	}
}

// TestFingerprint_ReasonClassUsesCanonicalReason documents the
// call-site contract: callers pass the reason through CanonicalReason
// first, so both kubelet variants of one failure family produce the
// SAME fingerprint (mirroring dedup's family collapse).
func TestFingerprint_ReasonClassUsesCanonicalReason(t *testing.T) {
	t.Parallel()
	a := Fingerprint("k8s-event", CanonicalReason("ErrImagePull"), "Pod", "us-east1-b")
	b := Fingerprint("k8s-event", CanonicalReason("ImagePullBackOff"), "Pod", "us-east1-b")
	if a != b {
		t.Errorf("ErrImagePull and ImagePullBackOff must share a fingerprint via CanonicalReason: %s vs %s", a, b)
	}
}
