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
	"testing"
	"time"
)

// This file pins the end-to-end behaviour of issue #225 across the two
// stages that have to cooperate to get it right: PullClassMemo (which
// decides whether a failure is registry-side) and StormCorrelator
// (which decides whether N failures are one incident). Neither stage
// can fix the reported symptom alone, so neither stage's own tests can
// prove it fixed.

const arHost = "us-east1-artifactregistry.gcr.io"

// gkeRegistryStorm is the observed incident, verbatim in shape from a
// GKE cluster whose region-wide Artifact Registry quota rolled over:
// SEVEN objects across two namespaces failing on one registry, of
// which only TWO ever said why. kubelet emits the cause-bearing
// `Failed` event once per object and then falls back to the causeless
// `Back-off pulling image "…"` — and for five of these objects the
// sentinel only ever saw the causeless form.
//
// Before #225 this arrived as seven root-cause investigations. The two
// cause-bearing events were the only ones carrying a registry
// blast-radius key, and two is below DefaultStormMin.
var gkeRegistryStorm = []struct {
	uid, ns, name string
	reason        string
	severity      Severity
	message       string
}{
	{"uid-gcsfuse", "kube-system", "gcsfusecsi-node", "Failed", SeverityWarning,
		`Failed to pull image "` + arHost + `/gke-release/gke-release/gcs-fuse-csi-driver:v1.22.21-gke.1@sha256:8ed8c7bce5b9a9f057600e32676b47985946edaaa09de57380414834e30590fe": failed to pull and unpack image "` + arHost + `/gke-release/gke-release/gcs-fuse-csi-driver@sha256:8ed8c7bce5b9a9f057600e32676b47985946edaaa09de57380414834e30590fe": unexpected status from HEAD request to https://` + arHost + `/v2/gke-release/gke-release/gcs-fuse-csi-driver/manifests/sha256:8ed8c7bce5b9a9f057600e32676b47985946edaaa09de57380414834e30590fe: 429 Too Many Requests`},
	{"uid-metadata", "kube-system", "gke-metadata-server", "Failed", SeverityWarning,
		`Failed to pull image "` + arHost + `/gke-release/gke-release/gke-metadata-server:gke_metadata_server_20260309.00_p0@sha256:2fd008f6a58022a2880e918c0f484e9f2680d55ef42176d43a3ba89efaa155ef": failed to resolve reference "` + arHost + `/gke-release/gke-release/gke-metadata-server@sha256:2fd008f6a58022a2880e918c0f484e9f2680d55ef42176d43a3ba89efaa155ef": unexpected status from HEAD request to https://` + arHost + `/v2/gke-release/gke-release/gke-metadata-server/manifests/sha256:2fd008f6a58022a2880e918c0f484e9f2680d55ef42176d43a3ba89efaa155ef: 429 Too Many Requests`},

	// From here on: causeless. Every one of these fired its own
	// session before #225.
	{"uid-fluentbit", "kube-system", "fluentbit-gke-big", "BackOff", SeverityInfo,
		`Back-off pulling image "` + arHost + `/gke-release/gke-release/gke-distroless/bash:gke_distroless_20260615.00_p0@sha256:b98f4278dc5123fbad96c54ba8301c512d32f4716528ab30faea15108c68c52e"`},
	{"uid-collector", "gke-gmp-system", "collector", "BackOff", SeverityInfo,
		`Back-off pulling image "` + arHost + `/gke-release/gke-release/gke-distroless/bash:gke_distroless_20260601.00_p0@sha256:3a4553b24025243918522d0deafeaf325410aae5f682723cdc32e0cbf786a6cd"`},
	{"uid-nodelocaldns", "kube-system", "node-local-dns", "BackOff", SeverityInfo,
		`Back-off pulling image "` + arHost + `/gke-release/gke-release/k8s-dns-node-cache:1.26.8-gke.5@sha256:b466bc19f519bf5eb7ee278a57d33f9fdf253db8dd4e78677bd7b0373afcfea7"`},
	{"uid-parallelstore", "kube-system", "parallelstore-csi-node", "BackOff", SeverityInfo,
		`Back-off pulling image "` + arHost + `/gke-release/gke-release/parallelstore-csi-driver:v0.2.11-gke.2@sha256:e9fb22926fc1281317817c3201452a39bc5314d548b7c5f8bfe736d40839113b"`},
	{"uid-pdcsi", "kube-system", "pdcsi-node", "BackOff", SeverityInfo,
		`Back-off pulling image "` + arHost + `/gke-release/gke-release/gke-metrics-collector:20250311_2300_RC0@sha256:e6713a8266629d9e103f7b2c7715bf510170fe28ebe2dc341327fa5e6a0facde"`},
}

// TestRegistryStorm_CauselessBackOffsJoinTheStorm replays the observed
// incident through the real memo and the real correlator.
//
// The scripted topology deliberately gives the seven objects NOTHING
// in common below the registry: each runs on its own node under its
// own DaemonSet, and they do not even share a namespace. The namespace
// candidate is present precisely so that a pass would be meaningless
// if the registry key were not winning on its own merits — six of the
// seven could group on kube-system, and the assertion on the storm's
// ancestor rejects that grouping.
func TestRegistryStorm_CauselessBackOffsJoinTheStorm(t *testing.T) {
	t.Parallel()

	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	for i, e := range gkeRegistryStorm {
		res.byObject[ObjectRef{Kind: "Pod", Namespace: e.ns, Name: e.name}] = []Ancestor{
			{Kind: "Node", Name: fmt.Sprintf("gke-node-%d", i)},
			{Kind: "DaemonSet", Namespace: e.ns, Name: e.name},
			{Kind: "Namespace", Name: e.ns},
		}
	}
	c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, res)
	memo := NewPullClassMemo()
	memo.now = func() time.Time { return *now }

	var formed, attached, alone int
	var storm StormInfo
	for i, e := range gkeRegistryStorm {
		// One event per second: well inside DefaultStormWindow, which
		// is the situation a quota rollover actually produces.
		*now = now.Add(time.Second)
		sig := Signal{
			Kind:        KindK8sEvent,
			Source:      SourceSentinel,
			Severity:    e.severity,
			Fingerprint: Fingerprint(KindK8sEvent, "ImagePullBackOff", "Pod", ""),
			TriageEvent: TriageEvent{
				Key:          EventKey{UID: e.uid, Reason: e.reason},
				Message:      e.message,
				Namespace:    e.ns,
				KindOfObject: "Pod",
				Name:         e.name,
				FirstSeen:    *now,
				LastSeen:     *now,
			},
		}
		sig.PullClass = memo.Resolve(sig)
		if sig.PullClass != PullClassRetryable {
			t.Errorf("event %d (%s): PullClass = %v, want PullClassRetryable — "+
				"every one of these is the same registry fault", i, e.name, sig.PullClass)
		}
		switch v := c.Observe(sig); v.Kind {
		case StormFormed:
			formed++
			storm = v.Storm
		case StormAttached:
			attached++
			storm = v.Storm
		default:
			alone++
		}
	}

	if formed != 1 {
		t.Errorf("storms formed = %d, want exactly 1 — one registry fault is one incident", formed)
	}
	if alone != DefaultStormMin-1 {
		// The first min-1 incidents necessarily precede formation
		// (§7.5): they are windowed, not lost, and the dispatcher
		// supersedes their sessions into the storm.
		t.Errorf("per-incident verdicts = %d, want %d (the pre-formation window only)", alone, DefaultStormMin-1)
	}
	if attached != len(gkeRegistryStorm)-DefaultStormMin {
		t.Errorf("attached = %d, want %d", attached, len(gkeRegistryStorm)-DefaultStormMin)
	}
	if want := (Ancestor{Kind: AncestorKindRegistry, Name: arHost}); storm.Ancestor != want {
		t.Errorf("storm ancestor = %+v, want %+v — the registry must outrank the namespace", storm.Ancestor, want)
	}
	if storm.AffectedCount != len(gkeRegistryStorm) {
		t.Errorf("AffectedCount = %d, want %d", storm.AffectedCount, len(gkeRegistryStorm))
	}
	if storm.NamespaceCount != 2 {
		t.Errorf("NamespaceCount = %d, want 2 (kube-system + gke-gmp-system)", storm.NamespaceCount)
	}
	if c.ActiveStorms() != 1 {
		t.Errorf("ActiveStorms = %d, want 1", c.ActiveStorms())
	}
}

// TestRegistryStorm_FormsWithNoTopologyAtAll is the faithful repro of
// the reported cluster, where the seven incidents did not correlate on
// ANYTHING — not even the namespace six of them shared. That points at
// the resolver returning no candidates for these objects (an unready
// or non-matching topology index; graphfeed.Ancestors yields nil when
// Snapshot.Lookup misses), which is a separate defect.
//
// The registry key must survive it regardless: it is prepended in
// Observe from the signal's own message, so it does not depend on the
// object being resolvable in the graph. That independence is load
// bearing and this test exists to keep it — a future refactor that
// sources the registry key from the topology instead would pass every
// other test in this file and silently regress this cluster back to
// seven sessions.
func TestRegistryStorm_FormsWithNoTopologyAtAll(t *testing.T) {
	t.Parallel()

	// Resolver knows nothing about anything.
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, res)
	memo := NewPullClassMemo()
	memo.now = func() time.Time { return *now }

	var storm StormInfo
	var formed int
	for _, e := range gkeRegistryStorm {
		*now = now.Add(time.Second)
		sig := Signal{
			Kind:        KindK8sEvent,
			Source:      SourceSentinel,
			Severity:    e.severity,
			Fingerprint: Fingerprint(KindK8sEvent, "ImagePullBackOff", "Pod", ""),
			TriageEvent: TriageEvent{
				Key:          EventKey{UID: e.uid, Reason: e.reason},
				Message:      e.message,
				Namespace:    e.ns,
				KindOfObject: "Pod",
				Name:         e.name,
				FirstSeen:    *now,
				LastSeen:     *now,
			},
		}
		sig.PullClass = memo.Resolve(sig)
		switch v := c.Observe(sig); v.Kind {
		case StormFormed:
			formed++
			storm = v.Storm
		case StormAttached:
			storm = v.Storm
		}
	}
	if formed != 1 {
		t.Fatalf("storms formed = %d, want 1 even with an empty topology index", formed)
	}
	if storm.AffectedCount != len(gkeRegistryStorm) {
		t.Errorf("AffectedCount = %d, want %d", storm.AffectedCount, len(gkeRegistryStorm))
	}
}

// TestRegistryStorm_MemoIsLoadBearing documents WHY the memo change was
// needed, by pinning the fact the fix rests on: five of the seven
// messages name no cause at all. Classifying each message in isolation
// — the pre-#225 behaviour — leaves only two registry-keyed incidents,
// one short of DefaultStormMin.
func TestRegistryStorm_MemoIsLoadBearing(t *testing.T) {
	t.Parallel()
	var causeBearing int
	for _, e := range gkeRegistryStorm {
		if ClassifyPullFailure(e.message) == PullClassRetryable {
			causeBearing++
		}
	}
	if causeBearing != 2 {
		t.Fatalf("cause-bearing messages = %d, want 2 (the corpus changed; revisit the premise)", causeBearing)
	}
	if causeBearing >= DefaultStormMin {
		t.Errorf("premise broken: %d cause-bearing events would reach DefaultStormMin=%d "+
			"on their own, so the host scope would not be load-bearing", causeBearing, DefaultStormMin)
	}
}
