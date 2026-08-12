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
	"fmt"
	"strings"
	"testing"
	"time"
)

// artifactRegistry429 is the REAL message that motivated issue #213,
// verbatim from a GKE cluster: kubelet aggregating a GET and a HEAD
// against Artifact Registry, both refused by the per-region
// per-minute request quota. The quota window rolls and the pull
// succeeds — nobody needs to be paged for it.
const artifactRegistry429 = `Failed to pull image "us-east1-artifactregistry.gcr.io/gke-release/gke-release/gke-distroless/bash:gke_distroless_20260615.00_p0@sha256:b98f4278dc5123fbad96c54ba8301c512d32f4716528ab30faea15108c68c52e": [failed to pull and unpack image "us-east1-artifactregistry.gcr.io/gke-release/gke-release/gke-distroless/bash@sha256:b98f4278dc5123fbad96c54ba8301c512d32f4716528ab30faea15108c68c52e": failed to copy: httpReadSeeker: failed open: unexpected status from GET request to https://us-east1-artifactregistry.gcr.io/v2/gke-release/gke-release/gke-distroless/bash/manifests/sha256:4e2ffa754250cb4d4d68fa5c2dd7a592c78edbbedc3a3b9428810c4bdf0c438b: 429 Too Many Requests
toomanyrequests: Quota exceeded for quota metric 'Requests per project per region' and limit 'Requests per project per region per minute per region' of service 'artifactregistry.googleapis.com' for consumer 'project_number:235545413903'., failed to pull and unpack image "us-east1-artifactregistry.gcr.io/gke-release/gke-release/gke-distroless/bash@sha256:b98f4278dc5123fbad96c54ba8301c512d32f4716528ab30faea15108c68c52e": failed to resolve reference "us-east1-artifactregistry.gcr.io/gke-release/gke-release/gke-distroless/bash@sha256:b98f4278dc5123fbad96c54ba8301c512d32f4716528ab30faea15108c68c52e": unexpected status from HEAD request to https://us-east1-artifactregistry.gcr.io/v2/gke-release/gke-release/gke-distroless/bash/manifests/sha256:b98f4278dc5123fbad96c54ba8301c512d32f4716528ab30faea15108c68c52e: 429 Too Many Requests]`

// gkeMissingRepo and gkeUnreachableRegistry are verbatim from a GKE
// v1.36.2-gke.2064000 (containerd) cluster, contributed on issue #216
// while the classifier was being ported elsewhere. They pin the two
// ends of the classification against a real emitter: a repository that
// does not exist is terminal — note that it lands on the generic `not
// found` marker, the specific "repository does not exist" wording
// never appears — and a registry that will not answer is retryable.
const gkeMissingRepo = `Failed to pull image "us-docker.pkg.dev/PROJECT/does-not-exist/nope:v1": failed to pull and unpack image "us-docker.pkg.dev/PROJECT/does-not-exist/nope:v1": failed to resolve reference "us-docker.pkg.dev/PROJECT/does-not-exist/nope:v1": unexpected status from HEAD request to https://us-docker.pkg.dev/v2/PROJECT/does-not-exist/nope/manifests/v1: 404 Not Found`

const gkeUnreachableRegistry = `Failed to pull image "10.255.255.1:5000/app/nope:v1": failed to pull and unpack image "10.255.255.1:5000/app/nope:v1": failed to resolve reference "10.255.255.1:5000/app/nope:v1": failed to do request: Head "https://10.255.255.1:5000/v2/app/nope/manifests/v1": dial tcp 10.255.255.1:5000: i/o timeout`

// TestClassifyPullFailure pins the classifier against the real
// containerd/registry error strings. Like the CanonicalReasonForEvent
// table in dedup_test.go, this is a DELIBERATE dependency on wording
// that is not API — this table is where a rewording becomes visible
// instead of silently changing which incidents we suppress.
func TestClassifyPullFailure(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		message string
		want    PullClass
	}{
		// Retryable: kubelet's retry cycle is the correct response.
		{"artifact registry 429 (issue #213)", artifactRegistry429, PullClassRetryable},
		{"docker hub rate limit", `Failed to pull image "nginx:1.25": toomanyrequests: You have reached your pull rate limit.`, PullClassRetryable},
		{"registry 503", `Failed to pull image "reg.example.com/app:v1": unexpected status: 503 Service Unavailable`, PullClassRetryable},
		{"dial timeout", `Failed to pull image "reg.example.com/app:v1": dial tcp 10.0.0.1:443: i/o timeout`, PullClassRetryable},
		{"tls handshake timeout", `Failed to pull image "reg.example.com/app:v1": net/http: TLS handshake timeout`, PullClassRetryable},
		{"connection reset", `Failed to pull image "reg.example.com/app:v1": read tcp: connection reset by peer`, PullClassRetryable},
		{"truncated layer", `Failed to pull image "reg.example.com/app:v1": failed to copy: unexpected EOF`, PullClassRetryable},
		{"context deadline", `Failed to pull image "reg.example.com/app:v1": context deadline exceeded`, PullClassRetryable},
		{"unreachable registry (GKE 1.36, issue #216)", gkeUnreachableRegistry, PullClassRetryable},

		// Terminal: retrying cannot help. These keep firing on event #1.
		{"bad tag / manifest unknown", `Failed to pull image "nginx:nope": manifest unknown`, PullClassTerminal},
		{"bad tag / NotFound rpc", `Failed to pull image "nginx:invalid-tag-for-testing": rpc error: code = NotFound desc = failed to pull and unpack image`, PullClassTerminal},
		{"private repo denied", `Failed to pull image "gcr.io/other/app:v1": pull access denied, repository does not exist or may require authorization`, PullClassTerminal},
		{"unauthorized", `Failed to pull image "reg.example.com/app:v1": unauthorized: authentication required`, PullClassTerminal},
		{"malformed reference", `Failed to pull image "NOT A REF": invalid reference format`, PullClassTerminal},
		{"never-pull policy", "Container image \"nginx:1.25\" is not present with pull policy of Never: ErrImageNeverPull", PullClassTerminal},
		{"disk full is node-terminal, not transient", `Failed to pull image "nginx:1.25": write /var/lib/containerd/x: no space left on device`, PullClassTerminal},
		{"missing repository (GKE 1.36, issue #216)", gkeMissingRepo, PullClassTerminal},

		// Terminal wins ties: an aggregated message naming a bad tag
		// AND a timeout must not be suppressed as merely transient.
		{"mixed markers resolve terminal", `Failed to pull image "nginx:nope": [dial tcp: i/o timeout, manifest unknown]`, PullClassTerminal},

		// Unknown: no cause in the text. Fires like it always has.
		{"bare back-off carries no cause", `Back-off pulling image "nginx:1.25"`, PullClassUnknown},
		{"sync-result error carries no cause", "Error: ErrImagePull", PullClassUnknown},
		{"empty message", "", PullClassUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyPullFailure(tc.message); got != tc.want {
				t.Errorf("ClassifyPullFailure(%.60q…) = %v, want %v", tc.message, got, tc.want)
			}
		})
	}
}

// TestClassifyPullFailure_ReferenceTextDoesNotVote is issue #216: the
// image reference is operator-chosen text that kubelet echoes back
// several times, so before the fix a digest, a tag or a repository
// path could decide the class on its own. The two directions are not
// equally bad — a false terminal fires an incident that was going to
// clear (noise), while a false RETRYABLE holds a real failure for
// three events (a miss) — so both are pinned here.
func TestClassifyPullFailure_ReferenceTextDoesNotVote(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		message string
		want    PullClass
	}{
		// Digits that are not a status code. A sha256 digest contains
		// "429" about 1.5% of the time, and the real Artifact Registry
		// quota error quotes a project number in the same message.
		{"digest containing 429", `Failed to pull image "us-docker.pkg.dev/p/r/app@sha256:b98f4290c1d2e3f4a5b6c7d8e9f0112233445566778899aabbccddeeff00112233": rpc error: code = Unknown desc = failed to commit snapshot`, PullClassUnknown},
		{"tag containing 429", `Failed to pull image "reg.example.com/team/app:v429": rpc error: code = Unknown desc = failed to commit snapshot`, PullClassUnknown},
		{"project number containing 429", `Failed to pull image "us-docker.pkg.dev/p/r/app:v1": rpc error: code = Unknown desc = unrecognized failure for consumer 'project_number:429551413903'`, PullClassUnknown},

		// Words in the repository path. `denied` reads as a permission
		// failure and would have fired this timeout immediately —
		// worse, terminal signals get no Registry storm ancestor, so a
		// registry-wide outage would not have correlated either.
		{"repository path containing denied", `Failed to pull image "reg.example.com/denied-team/app:v1": failed to resolve reference "reg.example.com/denied-team/app:v1": failed to do request: Head "https://reg.example.com/v2/denied-team/app/manifests/v1": dial tcp 10.0.0.1:443: i/o timeout`, PullClassRetryable},
		{"repository path containing unauthorized", `Failed to pull image "reg.example.com/unauthorized-probe/app:v1": read tcp: connection reset by peer`, PullClassRetryable},

		// Stripping the reference must not disarm a genuine failure
		// whose repository path happens to say the same thing.
		{"genuine denial in a denied-named repo", `Failed to pull image "reg.example.com/denied-team/app:v1": denied: requested access to the resource is denied`, PullClassTerminal},
		{"genuine 429 keeps its phrase", `Failed to pull image "reg.example.com/app@sha256:aaaa": unexpected status from HEAD request: 429 Too Many Requests`, PullClassRetryable},

		// Accepted regression from dropping the bare "429" marker: a
		// status code with no reason phrase is no longer recognized.
		// Unknown fires immediately, which is the safe direction.
		{"bare 429 with no reason phrase", `Failed to pull image "gcr.io/p/app:v1": unexpected status from HEAD request to https://gcr.io/v2/p/app/manifests/v1: 429`, PullClassUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyPullFailure(tc.message); got != tc.want {
				t.Errorf("ClassifyPullFailure(%.80q…) = %v, want %v", tc.message, got, tc.want)
			}
		})
	}
}

// TestPullRetryableMarkersArePhrases guards the #216 rule directly: a
// marker made only of digits matches any message that happens to
// contain those digits, and the retryable list is the one where a
// false positive costs a suppressed incident.
func TestPullRetryableMarkersArePhrases(t *testing.T) {
	t.Parallel()
	for _, marker := range pullRetryableMarkers {
		if strings.IndexFunc(marker, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
			t.Errorf("retryable marker %q is bare digits — pair it with the reason phrase (issue #216)", marker)
		}
	}
}

func TestRegistryHost(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		message string
		want    string
	}{
		{"artifact registry, digest ref", artifactRegistry429, "us-east1-artifactregistry.gcr.io"},
		{"back-off message carries the ref too", `Back-off pulling image "gcr.io/proj/app:v1"`, "gcr.io"},
		{"host with port", `Failed to pull image "localhost:5000/app:v1": i/o timeout`, "localhost:5000"},
		{"bare localhost", `Failed to pull image "localhost/app:v1": i/o timeout`, "localhost"},
		{"docker hub official image", `Failed to pull image "nginx:1.25": toomanyrequests`, dockerHubHost},
		{"docker hub user repo", `Failed to pull image "someuser/app:v1": toomanyrequests`, dockerHubHost},
		{"no image reference in message", "Error: ErrImagePull", ""},
		{"unterminated quote", `Failed to pull image "nginx`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RegistryHost(tc.message); got != tc.want {
				t.Errorf("RegistryHost = %q, want %q", got, tc.want)
			}
		})
	}
}

// pullSignal builds a k8s-event signal for one pod's pull failure.
func pullSignal(uid, reason, message string) Signal {
	return Signal{
		Kind:   KindK8sEvent,
		Source: SourceSentinel,
		TriageEvent: TriageEvent{
			Key:          EventKey{UID: uid, Reason: reason},
			Message:      message,
			Namespace:    "kube-system",
			KindOfObject: "Pod",
			Name:         "fluentbit-gke-big",
		},
	}
}

// TestPullClassMemo_CarriesCauseToCauselessBackOff is the memo's whole
// reason for existing. kubelet splits an image-pull incident across
// two events: `Failed` carries the cause and is NOT in the shipped
// --reason allow-list; `BackOff` is allow-listed and carries no cause
// at all. Classifying each message in isolation would hold the 429 on
// the Failed event and then fire on the causeless BackOff one seconds
// later — gating nothing. The memo joins them.
func TestPullClassMemo_CarriesCauseToCauselessBackOff(t *testing.T) {
	t.Parallel()
	m := NewPullClassMemo()

	if got := m.Resolve(pullSignal("pod-1", "Failed", artifactRegistry429)); got != PullClassRetryable {
		t.Fatalf("cause-bearing Failed event: class = %v, want PullClassRetryable", got)
	}
	// The follow-on back-off for the SAME pod says only "still
	// happening" — it must inherit the cause we already learned.
	if got := m.Resolve(pullSignal("pod-1", "BackOff", `Back-off pulling image "us-east1-artifactregistry.gcr.io/gke-release/x:v1"`)); got != PullClassRetryable {
		t.Errorf("causeless BackOff for the same pod: class = %v, want the inherited PullClassRetryable", got)
	}
	// A DIFFERENT pod has no evidence of its own: unknown, fires.
	if got := m.Resolve(pullSignal("pod-2", "BackOff", `Back-off pulling image "us-east1-artifactregistry.gcr.io/gke-release/x:v1"`)); got != PullClassUnknown {
		t.Errorf("causeless BackOff for an unseen pod: class = %v, want PullClassUnknown", got)
	}
}

// TestPullClassMemo_TerminalCauseAlsoCarries: the carry-forward is not
// a suppression mechanism, it is a cause lookup. A bad tag learned
// from the Failed event must reach the back-off event as TERMINAL, so
// the gate downstream lets it fire immediately.
func TestPullClassMemo_TerminalCauseAlsoCarries(t *testing.T) {
	t.Parallel()
	m := NewPullClassMemo()
	if got := m.Resolve(pullSignal("pod-1", "Failed", `Failed to pull image "nginx:nope": manifest unknown`)); got != PullClassTerminal {
		t.Fatalf("bad tag: class = %v, want PullClassTerminal", got)
	}
	if got := m.Resolve(pullSignal("pod-1", "BackOff", `Back-off pulling image "nginx:nope"`)); got != PullClassTerminal {
		t.Errorf("back-off after a bad tag: class = %v, want PullClassTerminal (must still fire fast)", got)
	}
}

// TestPullClassMemo_FreshCauseSupersedes: a pod that starts failing
// for a new reason is judged on the new evidence, not the old.
func TestPullClassMemo_FreshCauseSupersedes(t *testing.T) {
	t.Parallel()
	m := NewPullClassMemo()
	m.Resolve(pullSignal("pod-1", "Failed", artifactRegistry429))
	if got := m.Resolve(pullSignal("pod-1", "Failed", `Failed to pull image "nginx:nope": manifest unknown`)); got != PullClassTerminal {
		t.Fatalf("new cause: class = %v, want PullClassTerminal", got)
	}
	if got := m.Resolve(pullSignal("pod-1", "BackOff", `Back-off pulling image "nginx:nope"`)); got != PullClassTerminal {
		t.Errorf("back-off inherits the NEWEST cause: class = %v, want PullClassTerminal", got)
	}
}

// TestPullClassMemo_Expires: stale evidence must not judge a much
// later failure.
func TestPullClassMemo_Expires(t *testing.T) {
	t.Parallel()
	m := NewPullClassMemo()
	now := time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	m.Resolve(pullSignal("pod-1", "Failed", artifactRegistry429))
	now = now.Add(defaultPullMemoTTL + time.Second)
	if got := m.Resolve(pullSignal("pod-1", "BackOff", `Back-off pulling image "gcr.io/x/y:v1"`)); got != PullClassUnknown {
		t.Errorf("expired cause: class = %v, want PullClassUnknown", got)
	}
	if m.Len() != 0 {
		t.Errorf("expired entry should be dropped on lookup; Len = %d", m.Len())
	}
}

// TestPullClassMemo_Bounded: a cluster churning through pods must not
// grow the memo without limit.
func TestPullClassMemo_Bounded(t *testing.T) {
	t.Parallel()
	m := NewPullClassMemo()
	now := time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }
	for i := 0; i < maxPullMemoEntries+50; i++ {
		now = now.Add(time.Millisecond)
		m.Resolve(pullSignal(fmt.Sprintf("pod-%d", i), "Failed", artifactRegistry429))
	}
	if m.Len() > maxPullMemoEntries {
		t.Errorf("memo grew past its bound: Len = %d, max = %d", m.Len(), maxPullMemoEntries)
	}
}

// TestPullClassMemo_NonPullSignalsUntouched: the memo must not
// classify (or remember) anything outside the image-pull family.
func TestPullClassMemo_NonPullSignalsUntouched(t *testing.T) {
	t.Parallel()
	m := NewPullClassMemo()
	crash := pullSignal("pod-1", "BackOff", "Back-off restarting failed container server in pod web-1")
	if got := m.Resolve(crash); got != PullClassNA {
		t.Errorf("crash-loop BackOff: class = %v, want PullClassNA", got)
	}
	if got := m.Resolve(pullSignal("pod-1", "OOMKilled", "")); got != PullClassNA {
		t.Errorf("OOMKilled: class = %v, want PullClassNA", got)
	}
	if m.Len() != 0 {
		t.Errorf("non-pull signals must not populate the memo; Len = %d", m.Len())
	}
}

// TestPullClassMemo_NilIsMessageOnly: a nil memo (unit tests that
// predate the classifier, or any caller that does not want
// carry-forward) still classifies, just without inheritance.
func TestPullClassMemo_NilIsMessageOnly(t *testing.T) {
	t.Parallel()
	var m *PullClassMemo
	if got := m.Resolve(pullSignal("pod-1", "Failed", artifactRegistry429)); got != PullClassRetryable {
		t.Errorf("nil memo: class = %v, want PullClassRetryable", got)
	}
	if got := m.Resolve(pullSignal("pod-1", "BackOff", `Back-off pulling image "gcr.io/x/y:v1"`)); got != PullClassUnknown {
		t.Errorf("nil memo must not carry forward: class = %v, want PullClassUnknown", got)
	}
}
