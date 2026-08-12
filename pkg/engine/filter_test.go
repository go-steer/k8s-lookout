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

func makeEvent(reason, namespace string, count int) Signal {
	return Signal{
		Kind:     KindK8sEvent,
		Source:   SourceSentinel,
		Severity: SeverityCritical,
		TriageEvent: TriageEvent{
			Key:       EventKey{UID: "u1", Reason: reason},
			Namespace: namespace,
			Count:     count,
		},
	}
}

func TestFilter_Accept_DefaultReasons(t *testing.T) {
	t.Parallel()
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	// Every default reason should accept a plain event (count=1
	// suffices), except the count-debounced families: Unhealthy
	// (probe flap) and the crash-loop family (BackOff /
	// CrashLoopBackOff), which need count>=3.
	for _, reason := range defaultReasons {
		count := 1
		switch reason {
		case "Unhealthy", "BackOff", "CrashLoopBackOff":
			count = 3 // meet the debounce threshold
		}
		if !f.Accept(makeEvent(reason, "default", count)) {
			t.Errorf("default reason %s rejected", reason)
		}
	}
	if f.Accept(makeEvent("SomeRandomReason", "default", 1)) {
		t.Error("non-default reason should be rejected")
	}
}

func TestFilter_Accept_CustomAllowList(t *testing.T) {
	t.Parallel()
	f := NewFilter(NewFilterConfig([]string{"CustomReason"}, nil, nil, 0, 0, 0))
	if !f.Accept(makeEvent("CustomReason", "default", 1)) {
		t.Error("custom-listed reason should accept")
	}
	// The shipped defaults are NOT included when a custom list
	// is supplied — the operator's list is the complete set.
	if f.Accept(makeEvent("CrashLoopBackOff", "default", 1)) {
		t.Error("non-custom reason should reject when custom list is set")
	}
}

func TestFilter_Accept_ExcludedNamespaceWins(t *testing.T) {
	t.Parallel()
	// Exclude takes precedence over include (operator can express
	// "everything except kube-system" without listing every
	// included namespace).
	// OOMKilled is the vehicle: a real failure that is NOT count-gated,
	// so these assertions isolate the namespace logic from the
	// crash-loop/Unhealthy count debounce.
	f := NewFilter(NewFilterConfig(nil, []string{"default", "kube-system"}, []string{"kube-system"}, 0, 0, 0))
	if f.Accept(makeEvent("OOMKilled", "kube-system", 1)) {
		t.Error("excluded namespace should reject even when listed as allowed")
	}
	if !f.Accept(makeEvent("OOMKilled", "default", 1)) {
		t.Error("allowed namespace (not excluded) should accept")
	}
}

func TestFilter_Accept_AllowNamespacesLimitsScope(t *testing.T) {
	t.Parallel()
	f := NewFilter(NewFilterConfig(nil, []string{"prod"}, nil, 0, 0, 0))
	// OOMKilled: real failure, not count-gated (see above).
	if !f.Accept(makeEvent("OOMKilled", "prod", 1)) {
		t.Error("prod namespace should accept when allow-listed")
	}
	if f.Accept(makeEvent("OOMKilled", "dev", 1)) {
		t.Error("dev namespace should reject when only prod is allowed")
	}
}

func TestFilter_Accept_UnhealthyRequiresMinCount(t *testing.T) {
	t.Parallel()
	// Default unhealthy-min-count is 3.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	if f.Accept(makeEvent("Unhealthy", "default", 1)) {
		t.Error("Unhealthy count=1 should reject (below threshold 3)")
	}
	if f.Accept(makeEvent("Unhealthy", "default", 2)) {
		t.Error("Unhealthy count=2 should reject")
	}
	if !f.Accept(makeEvent("Unhealthy", "default", 3)) {
		t.Error("Unhealthy count=3 should accept (meets threshold)")
	}
	if !f.Accept(makeEvent("Unhealthy", "default", 100)) {
		t.Error("Unhealthy count=100 should accept")
	}
}

func TestFilter_Accept_UnhealthyThresholdOverridable(t *testing.T) {
	t.Parallel()
	// Custom threshold of 10 — probe-flap tolerance turned up.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 10, 0, 0))
	if f.Accept(makeEvent("Unhealthy", "default", 5)) {
		t.Error("Unhealthy count=5 should reject with threshold 10")
	}
	if !f.Accept(makeEvent("Unhealthy", "default", 10)) {
		t.Error("Unhealthy count=10 should accept with threshold 10")
	}
}

func TestFilter_Accept_UnhealthyThresholdDoesntAffectOtherReasons(t *testing.T) {
	t.Parallel()
	// The unhealthy count-threshold is Unhealthy-specific; a reason
	// outside the debounced families (OOMKilled) fires on count=1 no
	// matter how high the Unhealthy threshold is set.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 100, 0, 0))
	if !f.Accept(makeEvent("OOMKilled", "default", 1)) {
		t.Error("OOMKilled count=1 should always accept")
	}
}

// makeEventMsg is makeEvent with a kubelet message, so the tests can
// exercise the message-aware canonical-family disambiguation that the
// crash-loop debounce keys on.
func makeEventMsg(reason, message, namespace string, count int) Signal {
	s := makeEvent(reason, namespace, count)
	s.Message = message
	return s
}

func TestFilter_Accept_BackoffRequiresMinCount(t *testing.T) {
	t.Parallel()
	// Default backoff-min-count is 3: a transient startup blip
	// (first BackOff) is suppressed; a genuine crash loop climbs
	// Event.Count past the threshold and fires.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	if f.Accept(makeEvent("CrashLoopBackOff", "default", 1)) {
		t.Error("CrashLoopBackOff count=1 should reject (below threshold 3)")
	}
	if f.Accept(makeEvent("CrashLoopBackOff", "default", 2)) {
		t.Error("CrashLoopBackOff count=2 should reject")
	}
	if !f.Accept(makeEvent("CrashLoopBackOff", "default", 3)) {
		t.Error("CrashLoopBackOff count=3 should accept (meets threshold)")
	}
	// A raw kubelet "BackOff" crash-loop event (the repeating cycle,
	// message `Back-off restarting failed container …`) canonicalizes
	// to CrashLoopBackOff and is gated the same way.
	if f.Accept(makeEventMsg("BackOff", "Back-off restarting failed container app", "default", 1)) {
		t.Error("raw BackOff crash-loop count=1 should reject (canonical CrashLoopBackOff, below threshold)")
	}
	if !f.Accept(makeEventMsg("BackOff", "Back-off restarting failed container app", "default", 3)) {
		t.Error("raw BackOff crash-loop count=3 should accept")
	}
}

func TestFilter_Accept_BackoffThresholdOverridable(t *testing.T) {
	t.Parallel()
	// A threshold of 1 disables the debounce — fire on the first event.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 1, 0))
	if !f.Accept(makeEvent("CrashLoopBackOff", "default", 1)) {
		t.Error("CrashLoopBackOff count=1 should accept with threshold 1")
	}
	// Turned up to 10.
	f = NewFilter(NewFilterConfig(nil, nil, nil, 0, 10, 0))
	if f.Accept(makeEvent("CrashLoopBackOff", "default", 5)) {
		t.Error("CrashLoopBackOff count=5 should reject with threshold 10")
	}
	if !f.Accept(makeEvent("CrashLoopBackOff", "default", 10)) {
		t.Error("CrashLoopBackOff count=10 should accept with threshold 10")
	}
}

func TestFilter_Accept_BackoffGateSparesImagePullFamily(t *testing.T) {
	t.Parallel()
	// The crash-loop debounce is crash-loop ONLY. An image-pull
	// failure with no classified cause (PullClass unset — the zero
	// value here) is persistent-until-proven-otherwise and fires
	// fast: ImagePullBackOff, ErrImagePull, and a raw kubelet
	// "BackOff" whose message names an image pull (`Back-off pulling
	// image …`, canonical ImagePullBackOff) all fire on count=1.
	// Issue #213's gate narrows this only for PullClassRetryable —
	// see TestFilter_Accept_TransientPullDebounce below.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	if !f.Accept(makeEvent("ImagePullBackOff", "default", 1)) {
		t.Error("ImagePullBackOff count=1 should fire (image-pull family is not debounced)")
	}
	if !f.Accept(makeEvent("ErrImagePull", "default", 1)) {
		t.Error("ErrImagePull count=1 should fire (image-pull family is not debounced)")
	}
	if !f.Accept(makeEventMsg("BackOff", `Back-off pulling image "nope:bad"`, "default", 1)) {
		t.Error("raw BackOff image-pull count=1 should fire (canonical ImagePullBackOff, not debounced)")
	}
}

// makePullEvent builds an image-pull-family event with an explicit
// cause class — the shape the dispatcher hands the filter after
// PullClassMemo.Resolve has stamped it.
func makePullEvent(reason, message string, class PullClass, count int) Signal {
	sig := makeEventMsg(reason, message, "kube-system", count)
	sig.PullClass = class
	return sig
}

// TestFilter_Accept_TransientPullDebounce is issue #213's core
// assertion: a registry rate limit waits, a bad tag does not.
func TestFilter_Accept_TransientPullDebounce(t *testing.T) {
	t.Parallel()
	// "Failed" is not in the shipped allow-list, so use the reasons
	// that are; the gate keys on the CANONICAL family either way.
	f := NewFilter(NewFilterConfig([]string{"BackOff", "ImagePullBackOff", "ErrImagePull"}, nil, nil, 0, 0, 0))

	backoffMsg := `Back-off pulling image "us-east1-artifactregistry.gcr.io/gke-release/x:v1"`
	for count := 1; count < 3; count++ {
		if f.Accept(makePullEvent("BackOff", backoffMsg, PullClassRetryable, count)) {
			t.Errorf("retryable pull failure at count=%d should be held (threshold 3)", count)
		}
	}
	if !f.Accept(makePullEvent("BackOff", backoffMsg, PullClassRetryable, 3)) {
		t.Error("retryable pull failure at count=3 should fire — the registry is not recovering")
	}

	// Terminal and unknown causes are untouched by the gate: a bad
	// tag still fires on the very first event.
	if !f.Accept(makePullEvent("ImagePullBackOff", `Back-off pulling image "nginx:nope"`, PullClassTerminal, 1)) {
		t.Error("terminal pull failure at count=1 must still fire fast (a bad tag is persistent)")
	}
	if !f.Accept(makePullEvent("ImagePullBackOff", "Error: ErrImagePull", PullClassUnknown, 1)) {
		t.Error("unrecognized pull failure at count=1 must still fire — the gate only suppresses what it recognizes")
	}
}

// TestFilter_Accept_TransientPullThresholdOverridable: 1 restores
// firing on the first event, the documented escape hatch.
func TestFilter_Accept_TransientPullThresholdOverridable(t *testing.T) {
	t.Parallel()
	msg := `Back-off pulling image "gcr.io/x/y:v1"`
	off := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 1))
	if !off.Accept(makePullEvent("ImagePullBackOff", msg, PullClassRetryable, 1)) {
		t.Error("--imagepull-transient-min-count=1 should fire on the first event")
	}
	strict := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 10))
	if strict.Accept(makePullEvent("ImagePullBackOff", msg, PullClassRetryable, 9)) {
		t.Error("count=9 below a threshold of 10 should be held")
	}
	if !strict.Accept(makePullEvent("ImagePullBackOff", msg, PullClassRetryable, 10)) {
		t.Error("count=10 at a threshold of 10 should fire")
	}
}

// TestFilter_Accept_TransientPullGateSparesOtherFamilies: a retryable
// class stamped on a non-pull signal (which the memo never does, but
// defense in depth) must not gate anything else.
func TestFilter_Accept_TransientPullGateSparesOtherFamilies(t *testing.T) {
	t.Parallel()
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	oom := makeEvent("OOMKilled", "kube-system", 1)
	oom.PullClass = PullClassRetryable
	if !f.Accept(oom) {
		t.Error("OOMKilled must not be gated by the image-pull debounce")
	}
}

// TestFilter_Decide_GateNames pins the gate labels Accept's caller
// records as a metric. Cheap, and it keeps the label set from
// drifting silently away from the metric's documented Help string.
func TestFilter_Decide_GateNames(t *testing.T) {
	t.Parallel()
	f := NewFilter(NewFilterConfig(nil, nil, []string{"skipped"}, 0, 0, 0))
	cases := []struct {
		name string
		sig  Signal
		want string
	}{
		{"accepted", makeEvent("OOMKilled", "default", 1), ""},
		{"reason not allowed", makeEvent("SomeRandomReason", "default", 1), GateReasonNotAllowed},
		{"namespace excluded", makeEvent("OOMKilled", "skipped", 1), GateNamespaceExcluded},
		{"unhealthy debounce", makeEvent("Unhealthy", "default", 1), GateUnhealthyDebounce},
		{"crashloop debounce", makeEvent("CrashLoopBackOff", "default", 1), GateCrashLoopDebounce},
		{"transient pull debounce",
			makePullEvent("ImagePullBackOff", `Back-off pulling image "gcr.io/x/y:v1"`, PullClassRetryable, 1),
			GatePullTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ok, gate := f.Decide(tc.sig)
			if ok != (tc.want == "") {
				t.Fatalf("Decide accepted = %v, want %v", ok, tc.want == "")
			}
			if gate != tc.want {
				t.Errorf("gate = %q, want %q", gate, tc.want)
			}
			// Accept must never disagree with Decide.
			if got := f.Accept(tc.sig); got != ok {
				t.Errorf("Accept = %v but Decide = %v", got, ok)
			}
		})
	}
}

// makeSourceSignal builds a source-namespaced-kind signal (an
// object-state style transition signal).
func makeSourceSignal(kind, reason, namespace string) Signal {
	return Signal{
		Kind:     kind,
		Source:   SourceSentinel,
		Severity: SeverityWarning,
		TriageEvent: TriageEvent{
			Key:       EventKey{UID: "u1", Reason: reason},
			Namespace: namespace,
			Count:     1,
		},
	}
}

func TestFilter_Accept_SourceNamespacedKindsSkipReasonAllowList(t *testing.T) {
	t.Parallel()
	// The --reason allow-list is the operator control over the
	// open-ended Event.Reason space; source-namespaced kinds are a
	// curated set whose operator control is --sources. The default
	// filter (shipped reason list) must pass them.
	f := NewFilter(NewFilterConfig(nil, nil, nil, 0, 0, 0))
	if !f.Accept(makeSourceSignal("objectstate.pdb_gridlocked", "pdb_gridlocked", "prod")) {
		t.Error("source-namespaced kind rejected by the k8s-event reason allow-list")
	}
	// Even a custom --reason list doesn't gate them.
	f = NewFilter(NewFilterConfig([]string{"CrashLoopBackOff"}, nil, nil, 0, 0, 0))
	if !f.Accept(makeSourceSignal("objectstate.endpoints_empty", "endpoints_empty", "prod")) {
		t.Error("custom --reason list must not gate source-namespaced kinds")
	}
}

func TestFilter_Accept_NamespaceRulesApplyToSourceNamespacedKinds(t *testing.T) {
	t.Parallel()
	// Exclude wins for every source's signals.
	f := NewFilter(NewFilterConfig(nil, nil, []string{"kube-system"}, 0, 0, 0))
	if f.Accept(makeSourceSignal("objectstate.restart_burst", "restart_burst", "kube-system")) {
		t.Error("excluded namespace must reject source-namespaced kinds too")
	}
	// Allow-list scopes namespaced signals of any kind...
	f = NewFilter(NewFilterConfig(nil, []string{"prod"}, nil, 0, 0, 0))
	if f.Accept(makeSourceSignal("objectstate.restart_burst", "restart_burst", "dev")) {
		t.Error("namespace allow-list must scope source-namespaced kinds")
	}
	if !f.Accept(makeSourceSignal("objectstate.restart_burst", "restart_burst", "prod")) {
		t.Error("allow-listed namespace must accept")
	}
	// ...but a CLUSTER-scoped signal (empty namespace — e.g. a node
	// transition) passes: the allow-list scopes workload attention
	// and a node serves every namespace.
	if !f.Accept(makeSourceSignal("objectstate.node_notready", "node_notready", "")) {
		t.Error("cluster-scoped source signal must pass a namespace allow-list")
	}
}

func TestFilter_Accept_EmptyNamespaceK8sEventKeepsFrozenBehavior(t *testing.T) {
	t.Parallel()
	// FROZEN M0 behavior: a kind=k8s-event with an empty namespace is
	// still rejected by a set namespace allow-list. The cluster-scope
	// exemption above is for source-namespaced kinds only.
	f := NewFilter(NewFilterConfig(nil, []string{"prod"}, nil, 0, 0, 0))
	if f.Accept(makeEvent("CrashLoopBackOff", "", 1)) {
		t.Error("empty-namespace k8s-event must keep being rejected by a namespace allow-list (frozen behavior)")
	}
}
