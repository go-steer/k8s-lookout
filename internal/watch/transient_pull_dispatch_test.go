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

// End-to-end drills for issue #213 at the DISPATCHER level, which is
// the only level where the feature can be proven. The unit tests in
// pkg/engine show that the classifier reads a 429 correctly and that
// the filter holds a signal already stamped retryable; neither shows
// that a real kubelet event stream gets stamped at all.
//
// That gap is the whole difficulty. kubelet splits one image-pull
// incident across two events with different reasons:
//
//	reason=Failed   "Failed to pull image …: 429 Too Many Requests"   ← the CAUSE
//	reason=BackOff  "Back-off pulling image …"                        ← no cause
//
// and `Failed` is NOT in the shipped --reason allow-list while
// `BackOff` is. Classifying each message where it stands would hold
// the 429 on an event the allow-list was going to drop anyway, then
// fire on the causeless back-off ten seconds later — a gate that
// suppresses nothing. These drills run the real two-event sequence
// through DispatchSignal with SHIPPED defaults.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// ar429Message is the real Artifact Registry per-region quota refusal
// that motivated the issue, trimmed to the parts that matter (the
// full string is pinned verbatim in pkg/engine/pullfailure_test.go).
const ar429Message = `Failed to pull image "us-east1-artifactregistry.gcr.io/gke-release/gke-distroless/bash@sha256:b98f42": ` +
	`failed to copy: httpReadSeeker: failed open: unexpected status from GET request to ` +
	`https://us-east1-artifactregistry.gcr.io/v2/gke-release/gke-distroless/bash/manifests/sha256:4e2ffa: 429 Too Many Requests` +
	"\n" + `toomanyrequests: Quota exceeded for quota metric 'Requests per project per region' of service 'artifactregistry.googleapis.com'`

const arBackOffMessage = `Back-off pulling image "us-east1-artifactregistry.gcr.io/gke-release/gke-distroless/bash@sha256:b98f42"`

// newPullDispatcher is a per-incident dispatcher with the SHIPPED
// filter defaults (nil reasons = defaultReasons; 0s = the shipped
// thresholds, so --imagepull-transient-min-count is 3) and a live
// pull-class memo, exactly as wiring.go builds it.
func newPullDispatcher(t *testing.T, base string) *dispatcher {
	t.Helper()
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	return &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 0, 0)),
		dedup:     dedup,
		pullClass: engine.NewPullClassMemo(),
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "prod-us-central1",
		mode:      "per-incident",
	}
}

// pullEvent fabricates one kubelet pull event for the fluentbit pod.
func pullEvent(reason, message string, count int, at time.Time) engine.Signal {
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "pod-fluentbit-uid", Reason: reason},
			Namespace:    "kube-system",
			KindOfObject: "Pod",
			Name:         "fluentbit-gke-big",
			Message:      message,
			Count:        count,
			FirstSeen:    at,
			LastSeen:     at,
			Type:         "Warning",
		},
	}
}

// TestDispatch_TransientPull_HeldUntilThreshold is the issue's
// headline scenario: a registry rate limit must not open a session
// while kubelet is still working through its retry cycle.
func TestDispatch_TransientPull_HeldUntilThreshold(t *testing.T) {
	base, injects := newRoutingFakeDaemon(t)
	d := newPullDispatcher(t, base)
	ctx := context.Background()
	at := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	// The cause-bearing event. Dropped by the --reason allow-list
	// (Failed is not in it) but still seen by the pipeline, which is
	// what teaches the memo why this pod is failing.
	d.DispatchSignal(ctx, pullEvent("Failed", ar429Message, 1, at))
	// The allow-listed, causeless back-off events that follow.
	d.DispatchSignal(ctx, pullEvent("BackOff", arBackOffMessage, 1, at.Add(10*time.Second)))
	d.DispatchSignal(ctx, pullEvent("BackOff", arBackOffMessage, 2, at.Add(30*time.Second)))

	if len(*injects) != 0 {
		t.Fatalf("a transient 429 opened %d session(s) before the debounce threshold; want 0:\n%+v", len(*injects), *injects)
	}
	if got := testutil.ToFloat64(d.metrics.eventsFiltered.WithLabelValues(engine.GatePullTransient)); got != 2 {
		t.Errorf("imagepull_transient_debounce counter = %v, want 2 (the two held back-offs)", got)
	}

	// Still failing at count=3: the registry is not recovering on its
	// own, so this is a real incident and must fire.
	d.DispatchSignal(ctx, pullEvent("BackOff", arBackOffMessage, 3, at.Add(70*time.Second)))
	if len(*injects) != 1 {
		t.Fatalf("sustained 429 at count=3 produced %d injects, want 1", len(*injects))
	}
}

// TestDispatch_TransientPull_SelfHealsSilently: the payoff. A rate
// limit that clears inside the retry cycle never reaches an operator.
func TestDispatch_TransientPull_SelfHealsSilently(t *testing.T) {
	base, injects := newRoutingFakeDaemon(t)
	d := newPullDispatcher(t, base)
	ctx := context.Background()
	at := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	d.DispatchSignal(ctx, pullEvent("Failed", ar429Message, 1, at))
	d.DispatchSignal(ctx, pullEvent("BackOff", arBackOffMessage, 1, at.Add(10*time.Second)))
	// …the quota window rolls, the pull succeeds, kubelet stops
	// reporting. Nothing further arrives.

	if len(*injects) != 0 {
		t.Errorf("a self-healing 429 alerted anyway: %d inject(s)\n%+v", len(*injects), *injects)
	}
	if d.dedup.Len() != 0 {
		t.Errorf("held signals must not create dedup entries; Len = %d", d.dedup.Len())
	}
}

// TestDispatch_BadTag_StillFiresOnFirstEvent is the guardrail on the
// whole feature, and the promise #197 made: a persistent failure is
// not delayed. Note the bad tag's cause ALSO arrives on the
// allow-list-rejected `Failed` event — so this passes only because
// the memo carries terminal causes forward just as eagerly as
// retryable ones.
func TestDispatch_BadTag_StillFiresOnFirstEvent(t *testing.T) {
	base, injects := newRoutingFakeDaemon(t)
	d := newPullDispatcher(t, base)
	ctx := context.Background()
	at := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	d.DispatchSignal(ctx, pullEvent("Failed",
		`Failed to pull image "gcr.io/proj/app:nope": rpc error: code = NotFound desc = manifest unknown`, 1, at))
	d.DispatchSignal(ctx, pullEvent("BackOff",
		`Back-off pulling image "gcr.io/proj/app:nope"`, 1, at.Add(10*time.Second)))

	if len(*injects) != 1 {
		t.Fatalf("a bad tag produced %d injects, want 1 on the first allow-listed event", len(*injects))
	}
}

// TestDispatch_UnclassifiedPull_StillFiresOnFirstEvent: when no event
// ever named a cause, behavior is exactly what it was before #213.
// The gate can only ever delay a failure it positively recognizes.
func TestDispatch_UnclassifiedPull_StillFiresOnFirstEvent(t *testing.T) {
	base, injects := newRoutingFakeDaemon(t)
	d := newPullDispatcher(t, base)
	ctx := context.Background()
	at := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	// Only the causeless back-off — no Failed event was ever emitted.
	d.DispatchSignal(ctx, pullEvent("BackOff", arBackOffMessage, 1, at))

	if len(*injects) != 1 {
		t.Fatalf("an unclassified pull failure produced %d injects, want 1 (pre-#213 behavior)", len(*injects))
	}
}

// TestDispatch_CrashLoopDebounceUnchanged pins #197's gate against
// regression from #213's plumbing: the crash-loop family is counted
// on its own threshold and is not touched by the pull class.
func TestDispatch_CrashLoopDebounceUnchanged(t *testing.T) {
	base, injects := newRoutingFakeDaemon(t)
	d := newPullDispatcher(t, base)
	ctx := context.Background()
	at := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	msg := "Back-off restarting failed container server in pod web-1"

	d.DispatchSignal(ctx, pullEvent("BackOff", msg, 1, at))
	d.DispatchSignal(ctx, pullEvent("BackOff", msg, 2, at.Add(10*time.Second)))
	if len(*injects) != 0 {
		t.Fatalf("crash-loop below --backoff-min-count opened %d session(s), want 0", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.eventsFiltered.WithLabelValues(engine.GateCrashLoopDebounce)); got != 2 {
		t.Errorf("crashloop_debounce counter = %v, want 2", got)
	}
	d.DispatchSignal(ctx, pullEvent("BackOff", msg, 3, at.Add(20*time.Second)))
	if len(*injects) != 1 {
		t.Errorf("crash-loop at count=3 produced %d injects, want 1", len(*injects))
	}
}
