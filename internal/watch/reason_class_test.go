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

// Issue #574: kubelet emits `reason: BackOff` for a crash loop AND for
// an image-pull retry, and lookout's classification of the two
// (engine.CanonicalReasonForEvent) never left the process, so a
// consumer routing on `reason` sent a crash loop to its generic
// BackOff handler. These tests pin `reason_class` on the wire beside
// the untouched `reason`.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
)

// reasonClassDispatcher is a shared-mode dispatcher whose filter lets
// the generic kubelet reasons through, capturing every inject.
func reasonClassDispatcher(t *testing.T, maxBytes int) (*dispatcher, *[]string) {
	t.Helper()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "t", AssertedCaller: "a@b"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	return &dispatcher{
		filter:         engine.NewFilter(engine.NewFilterConfig([]string{"BackOff", "Failed", "FailedScheduling"}, nil, nil, 0, 1, 0)),
		dedup:          dedup,
		injector:       inj,
		metrics:        newMetrics(),
		cluster:        "prod-east",
		mode:           "shared",
		targetSid:      "sess-shared",
		injectMaxBytes: maxBytes,
	}, injects
}

func reasonClassEvent(uid, reason, message string) engine.Signal {
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: reason},
			Namespace:    "triage-demo",
			KindOfObject: "Pod",
			Name:         "checkout-784cfb56df-4fvfc",
			Message:      message,
			Count:        1,
			Type:         "Warning",
		},
	}
}

// TestDispatchSignal_ReasonClassOnTheWire drives the real dispatcher
// for each kubelet shape and reads the payload back off the wire:
// reason stays what the cluster said, reason_class is the family —
// set even when the two are equal, so a consumer never needs a
// fallback.
func TestDispatchSignal_ReasonClassOnTheWire(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, reason, message, want string
	}{
		{"crash-loop BackOff", "BackOff",
			"Back-off restarting failed container checkout in pod checkout-784cfb56df-4fvfc_triage-demo(0a1b)",
			"CrashLoopBackOff"},
		{"image-pull BackOff", "BackOff",
			`Back-off pulling image "us-docker.pkg.dev/team/app:v9"`,
			"ImagePullBackOff"},
		{"Failed to pull image", "Failed",
			`Failed to pull image "us-docker.pkg.dev/team/app:v9": manifest unknown`,
			"ImagePullBackOff"},
		{"plain reason equals itself", "FailedScheduling",
			"0/3 nodes are available: 3 Insufficient cpu.",
			"FailedScheduling"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, injects := reasonClassDispatcher(t, 0)
			d.DispatchSignal(context.Background(), reasonClassEvent("uid-"+string(rune('a'+i)), tc.reason, tc.message))
			if len(*injects) != 1 {
				t.Fatalf("expected 1 inject; got %d", len(*injects))
			}
			body := messageOf(t, (*injects)[0])
			var p inject.Payload
			if err := json.Unmarshal([]byte(body), &p); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			if p.Reason != tc.reason {
				t.Errorf("reason = %q, want the cluster's own %q (never rewritten)", p.Reason, tc.reason)
			}
			if p.ReasonClass != tc.want {
				t.Errorf("reason_class = %q, want %q", p.ReasonClass, tc.want)
			}
			// The raw reason keeps its frozen position; reason_class is
			// appended LAST (§Evolution: additions go at the end).
			if want := `{"kind":"k8s-event","reason":"` + tc.reason + `",`; !strings.HasPrefix(body, want) {
				t.Errorf("wire bytes do not start with %s:\n%s", want, body)
			}
			if want := `"type":"Warning","reason_class":"` + tc.want + `"}`; !strings.HasSuffix(body, want) {
				t.Errorf("wire bytes do not end with %s:\n%s", want, body)
			}
		})
	}
}

// TestDispatchSignal_ReasonClassSurvivesSanitizerAndFit: reason_class
// is routing identity. It is computed from the RAW message (a fixed
// family name never carries message text, so it needs no mask), the
// message itself is still masked, and the --inject-max-bytes fit —
// which sheds enrichment and truncates message — never touches it.
func TestDispatchSignal_ReasonClassSurvivesSanitizerAndFit(t *testing.T) {
	t.Parallel()
	const secret = "failed dialing postgres://svc:hunter2@db:5432"
	msg := "Back-off restarting failed container app: " + secret + " " + strings.Repeat("x", 20_000)
	if emit.MaskString(msg) == msg {
		t.Fatal("test fixture bug: MaskString does not change the planted message")
	}
	d, injects := reasonClassDispatcher(t, inject.MaxInjectBytes)
	d.DispatchSignal(context.Background(), reasonClassEvent("uid-fit", "BackOff", msg))
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	body := messageOf(t, (*injects)[0])
	var p inject.Payload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if strings.Contains(p.Message, "hunter2") {
		t.Errorf("secret reached the wire unmasked: %q", p.Message)
	}
	if len(p.Message) >= len(msg) {
		t.Fatalf("test setup: message was not truncated by the fit (len %d)", len(p.Message))
	}
	if p.Reason != "BackOff" || p.ReasonClass != "CrashLoopBackOff" {
		t.Errorf("fit/sanitizer disturbed reason identity: reason=%q reason_class=%q", p.Reason, p.ReasonClass)
	}
}

// TestIncidentPayload_ReasonClassSourceKinds pins the decision for
// source-namespaced kinds: reason_class only when a canonical mapping
// applies (the reason joins an existing family), omitted otherwise so
// those payloads stay byte-identical to before.
func TestIncidentPayload_ReasonClassSourceKinds(t *testing.T) {
	t.Parallel()
	mapped := engine.Signal{Kind: capacity.KindPending, TriageEvent: engine.TriageEvent{
		Key: engine.EventKey{UID: "u1", Reason: "pending"}, KindOfObject: "Pod", Name: "p"}}
	if got := incidentPayload(mapped, engine.DedupResult{Count: 1}).ReasonClass; got != "FailedScheduling" {
		t.Errorf("capacity.pending reason_class = %q, want FailedScheduling", got)
	}
	unmapped := engine.Signal{Kind: rollout.KindStall, TriageEvent: engine.TriageEvent{
		Key: engine.EventKey{UID: "u2", Reason: rollout.ReasonStall}, KindOfObject: "Deployment", Name: "web"}}
	p := incidentPayload(unmapped, engine.DedupResult{Count: 1})
	if p.ReasonClass != "" {
		t.Errorf("rollout stall reason_class = %q, want empty (no mapping applies)", p.ReasonClass)
	}
	b, _ := json.Marshal(p)
	if strings.Contains(string(b), "reason_class") {
		t.Errorf("omitempty broken: reason_class on the wire for an unmapped source kind: %s", b)
	}
}
