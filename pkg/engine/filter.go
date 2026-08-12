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

// defaultReasons is the shipped set of Event.Reason values that
// trigger investigations. Chosen to cover the top-frequency real
// failures per docs/k8s-event-agent-design.md §"Event filter
// allow-list". Operators can override via --reason.
var defaultReasons = []string{
	"CrashLoopBackOff",
	"ImagePullBackOff",
	"ErrImagePull",
	"OOMKilled",
	"FailedMount",
	"FailedScheduling",
	"BackOff",
	"Unhealthy",
	"NetworkNotReady",
	"NodeNotReady",
	"Evicted",
}

// FilterConfig captures the sidecar's per-event decision logic.
// Constructed from CLI flags in main.go; injected into the filter
// so tests can override each knob independently.
type FilterConfig struct {
	// allowedReasons is the set of Event.Reason values that pass
	// the filter. Case-sensitive match against Event.Reason
	// (k8s uses CamelCase; case-insensitivity would only hide
	// operator typos in configs).
	allowedReasons map[string]struct{}
	// allowedNamespaces, when non-empty, restricts firing to
	// events from these namespaces. Empty = all namespaces.
	// Applied AFTER excludedNamespaces (exclude wins).
	allowedNamespaces map[string]struct{}
	// excludedNamespaces suppresses events from these namespaces
	// even if they'd otherwise pass. Applied before
	// allowedNamespaces so operators can express "all except
	// kube-system" without listing every included namespace.
	excludedNamespaces map[string]struct{}
	// unhealthyMinCount is the special case for the Unhealthy
	// reason: probes flap constantly and firing on every one
	// would drown the sidecar. Require the event's own Count
	// (k8s Event.Count, which repeats-per-source) to reach this
	// value before we pass it. Default 3.
	unhealthyMinCount int
	// backoffMinCount is the leading-edge debounce for the
	// crash-loop family (canonical CrashLoopBackOff — i.e. kubelet's
	// repeating BackOff `Back-off restarting failed container …`
	// cycle, and CrashLoopBackOff itself). A transient startup blip
	// (a pod losing a scale-up race that self-heals in ~2m) emits a
	// first BackOff and would open a noise session before the
	// recovery tracker can resolve it; a genuine crash loop climbs
	// Event.Count past this threshold within seconds. Require the
	// event's own Count to reach this value before we pass it.
	// Default 3 (issue #197). The image-pull family
	// (ImagePullBackOff/ErrImagePull, incl. BackOff on a bad image)
	// is deliberately NOT gated HERE: a bad tag is persistent and
	// should fire fast — which is why the check keys on the
	// message-aware CANONICAL reason, not the raw one. The retryable
	// half of that family has its own gate below.
	backoffMinCount int
	// imagePullTransientMinCount is the leading-edge debounce for the
	// RETRYABLE half of the image-pull family (issue #213). The
	// family-wide exclusion above is right about bad tags and wrong
	// about registries: an Artifact Registry per-region request quota
	// answers 429 for the rest of the minute and then serves the pull,
	// so the incident is over before anyone reads the alert. Require
	// the event's own Count to reach this value before firing, but
	// ONLY when the failure classifies PullClassRetryable — terminal
	// and unrecognized causes still fire on event #1, so this can only
	// ever delay a failure we positively recognize as self-clearing.
	// Default 3; kubelet's pull backoff (10s/20s/40s/80s, capped 300s)
	// puts that at roughly 1-2 minutes of CONTINUED failure, which a
	// rolling quota window does not reach and a real registry outage
	// does.
	imagePullTransientMinCount int
}

// NewFilterConfig builds a FilterConfig from CLI-shaped inputs.
// Empty slices default to the shipped values; non-positive counts
// default to their shipped defaults (0 means "use the default", so
// callers that don't care pass 0 and get the shipped debounce).
func NewFilterConfig(reasons []string, allowNamespaces, excludeNamespaces []string, unhealthyMinCount, backoffMinCount, imagePullTransientMinCount int) FilterConfig {
	if len(reasons) == 0 {
		reasons = defaultReasons
	}
	if unhealthyMinCount <= 0 {
		unhealthyMinCount = 3
	}
	if backoffMinCount <= 0 {
		backoffMinCount = 3
	}
	if imagePullTransientMinCount <= 0 {
		imagePullTransientMinCount = 3
	}
	fc := FilterConfig{
		allowedReasons:             stringSet(reasons),
		allowedNamespaces:          stringSet(allowNamespaces),
		excludedNamespaces:         stringSet(excludeNamespaces),
		unhealthyMinCount:          unhealthyMinCount,
		backoffMinCount:            backoffMinCount,
		imagePullTransientMinCount: imagePullTransientMinCount,
	}
	return fc
}

// stringSet converts a []string to a set for O(1) membership tests.
func stringSet(xs []string) map[string]struct{} {
	if len(xs) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		if x == "" {
			continue
		}
		out[x] = struct{}{}
	}
	return out
}

// Filter decides whether a signal should proceed to dedup + inject.
// Pure function — same input, same output; no I/O.
type Filter struct {
	cfg FilterConfig
}

func NewFilter(cfg FilterConfig) *Filter {
	return &Filter{cfg: cfg}
}

// Gate names the rule that rejected a signal — the label Decide hands
// back so operators can see WHICH rule is eating their events, not
// just that something did. A closed, low-cardinality set: safe as a
// Prometheus label value.
const (
	GateReasonNotAllowed    = "reason_not_allowed"
	GateNamespaceExcluded   = "namespace_excluded"
	GateNamespaceNotAllowed = "namespace_not_allowed"
	GateUnhealthyDebounce   = "unhealthy_debounce"
	GateCrashLoopDebounce   = "crashloop_debounce"
	GatePullTransient       = "imagepull_transient_debounce"
)

// Accept returns true if the signal passes every filter rule. The
// decision order is deliberate:
//
//  1. Reason must be in the allow-list (or the allow-list is empty
//     meaning "everything" — but that's not a shipped default).
//     k8s-event kinds ONLY: `--reason` is the allow-list over the
//     open-ended Event.Reason space. Source-namespaced kinds
//     (`objectstate.*`, `rollout.*`, …) are a curated, finite set —
//     the operator control for them is source enablement
//     (`--sources`, §7.2), so the event allow-list does not apply.
//  2. Namespace must not be in excluded (exclude wins). Applies to
//     every source's signals uniformly.
//  3. Namespace must be in allowed (or allowed is empty = all).
//     Cluster-scoped signals from source-namespaced kinds (e.g. an
//     objectstate node signal, Namespace == "") pass: a namespace
//     allow-list scopes workload attention, and a node serves every
//     namespace. k8s-event kinds keep the frozen M0 behavior exactly
//     (an empty-namespace event is rejected by a set allow-list).
//  4. Leading-edge count debounce (k8s-event kinds): the Unhealthy
//     probe-flap gate, the crash-loop-family gate, and the transient
//     image-pull gate each require the event's repeat count to reach
//     their threshold. Keyed on the message-aware CANONICAL reason so
//     a generic kubelet "BackOff" lands in the right family. Within
//     the image-pull family the gate narrows again, on Signal.PullClass
//     (issue #213): only a RETRYABLE cause — a registry rate limit, a
//     5xx, a timeout — is debounced. A bad tag is persistent, classifies
//     terminal, and still fires on the first event.
func (f *Filter) Accept(sig Signal) bool {
	ok, _ := f.Decide(sig)
	return ok
}

// Decide is Accept plus the name of the rule that rejected the signal
// (one of the Gate* constants; empty when accepted). Same rules, same
// order — Accept delegates here, so the two can never disagree. The
// gate name exists for the metric: a debounce that silently eats
// events an operator expected is indistinguishable from a broken
// watcher unless the suppression is countable per rule.
func (f *Filter) Decide(sig Signal) (bool, string) {
	eventKind := isK8sEventKind(sig.Kind)
	if eventKind && f.cfg.allowedReasons != nil {
		if _, ok := f.cfg.allowedReasons[sig.Key.Reason]; !ok {
			return false, GateReasonNotAllowed
		}
	}
	if len(f.cfg.excludedNamespaces) > 0 {
		if _, excluded := f.cfg.excludedNamespaces[sig.Namespace]; excluded {
			return false, GateNamespaceExcluded
		}
	}
	if len(f.cfg.allowedNamespaces) > 0 {
		if _, allowed := f.cfg.allowedNamespaces[sig.Namespace]; !allowed {
			if eventKind || sig.Namespace != "" {
				return false, GateNamespaceNotAllowed
			}
		}
	}
	if eventKind {
		switch CanonicalReasonForEvent(sig.Key.Reason, sig.Message) {
		case "Unhealthy":
			if sig.Count < f.cfg.unhealthyMinCount {
				return false, GateUnhealthyDebounce
			}
		case "CrashLoopBackOff":
			if sig.Count < f.cfg.backoffMinCount {
				return false, GateCrashLoopDebounce
			}
		case "ImagePullBackOff":
			if sig.PullClass == PullClassRetryable && sig.Count < f.cfg.imagePullTransientMinCount {
				return false, GatePullTransient
			}
		}
	}
	return true, ""
}

// isK8sEventKind reports whether the signal is the frozen k8s-event
// wire kind (or its followup, or a legacy caller that set no kind) —
// the kinds the `--reason` allow-list and the Unhealthy threshold
// were designed for.
func isK8sEventKind(kind string) bool {
	switch kind {
	case KindK8sEvent, KindK8sEventFollowup, "":
		return true
	}
	return false
}
