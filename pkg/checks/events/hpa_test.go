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

package events

import (
	"fmt"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func hpaFixture(ns, name, uid, targetKind, targetName string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				Kind: targetKind, Name: targetName, APIVersion: "apps/v1",
			},
		},
	}
}

func rescaleEvent(ns, evName, hpaName, hpaUID string, size int, count int32, first, last time.Duration) *corev1.Event {
	msg := fmt.Sprintf("New size: %d; reason: cpu resource utilization (percentage of request) above target", size)
	ev := event(ns, evName, objRef(hpaKind, ns, hpaName, hpaUID),
		corev1.EventTypeNormal, "SuccessfulRescale", msg, count, first, last)
	ev.Source = corev1.EventSource{Component: "horizontal-pod-autoscaler"}
	return ev
}

// oscillation is the golden fixture: 6→3→7→3, three direction-
// bracketed moves = 2 flips within minutes of each other.
func oscillation(ns, hpaName, hpaUID string) []runtime.Object {
	return []runtime.Object{
		rescaleEvent(ns, "ev-hpa-1", hpaName, hpaUID, 6, 1, 25*time.Minute, 25*time.Minute),
		rescaleEvent(ns, "ev-hpa-2", hpaName, hpaUID, 3, 1, 20*time.Minute, 20*time.Minute),
		rescaleEvent(ns, "ev-hpa-3", hpaName, hpaUID, 7, 1, 15*time.Minute, 15*time.Minute),
		rescaleEvent(ns, "ev-hpa-4", hpaName, hpaUID, 3, 1, 10*time.Minute, 10*time.Minute),
	}
}

func thrashRecords(t *testing.T, recs []record) []record {
	t.Helper()
	var out []record
	for _, r := range recs {
		if r["kind"] == "event.hpa_thrash" {
			out = append(out, r)
		}
	}
	return out
}

// TestHPAThrash_WorkloadMode: the HPA targets the Deployment, so its
// events join the workload timeline AND the oscillation is analyzed;
// the finding carries the recovered replica sequence and the target.
func TestHPAThrash_WorkloadMode(t *testing.T) {
	objs := append(webTree(), hpaFixture("prod", "web-hpa", "h1", "Deployment", "web"))
	objs = append(objs, oscillation("prod", "web-hpa", "h1")...)
	recs, _ := runRecords(t, testCommand(objs...), "--workload=Deployment/prod/web")

	thrash := thrashRecords(t, recs)
	if len(thrash) != 1 {
		t.Fatalf("thrash findings = %d, want 1:\n%v", len(thrash), recs)
	}
	f := thrash[0]
	if f["severity"] != "warning" || f["reason"] != "HPAThrash" {
		t.Errorf("thrash envelope = %v", f)
	}
	if f["kind_of_object"] != hpaKind || f["name"] != "web-hpa" {
		t.Errorf("thrash subject = %s/%s, want HorizontalPodAutoscaler/web-hpa", f["kind_of_object"], f["name"])
	}
	if f["replicas"] != "6->3->7->3" {
		t.Errorf("replicas = %q, want 6->3->7->3", f["replicas"])
	}
	if f["flips"] != "2" {
		t.Errorf("flips = %s, want 2", f["flips"])
	}
	if f["window"] != "30m0s" {
		t.Errorf("window = %s, want the default 30m0s", f["window"])
	}
	if f["target"] != "Deployment/web" {
		t.Errorf("target = %q, want Deployment/web", f["target"])
	}

	// The SuccessfulRescale events themselves also appear as normal
	// timeline entries (the HPA is part of the workload's scope).
	var sawRescale bool
	for _, r := range recs {
		if r["reason"] == "SuccessfulRescale" && r["kind"] == "event.normal" {
			sawRescale = true
		}
	}
	if !sawRescale {
		t.Error("SuccessfulRescale timeline entries missing from the workload timeline")
	}
}

// TestHPAThrash_MonotonicRampDoesNotFire: a fast but one-directional
// scale-up has zero direction changes — no thrash, however many
// rescales.
func TestHPAThrash_MonotonicRampDoesNotFire(t *testing.T) {
	objs := append(webTree(), hpaFixture("prod", "web-hpa", "h1", "Deployment", "web"))
	objs = append(objs,
		rescaleEvent("prod", "ev-hpa-1", "web-hpa", "h1", 2, 1, 25*time.Minute, 25*time.Minute),
		rescaleEvent("prod", "ev-hpa-2", "web-hpa", "h1", 4, 1, 20*time.Minute, 20*time.Minute),
		rescaleEvent("prod", "ev-hpa-3", "web-hpa", "h1", 6, 1, 15*time.Minute, 15*time.Minute),
		rescaleEvent("prod", "ev-hpa-4", "web-hpa", "h1", 8, 1, 10*time.Minute, 10*time.Minute),
	)
	recs, _ := runRecords(t, testCommand(objs...), "--workload=Deployment/prod/web")
	if thrash := thrashRecords(t, recs); len(thrash) != 0 {
		t.Errorf("monotonic ramp fired thrash: %v", thrash)
	}
}

// TestHPAThrash_AggregatedEvents: k8s collapses a two-message loop
// into two Event objects with counts — the first/last envelope of
// each still recovers the oscillation.
func TestHPAThrash_AggregatedEvents(t *testing.T) {
	objs := append(webTree(), hpaFixture("prod", "web-hpa", "h1", "Deployment", "web"))
	objs = append(objs,
		rescaleEvent("prod", "ev-hpa-up", "web-hpa", "h1", 6, 3, 28*time.Minute, 8*time.Minute),
		rescaleEvent("prod", "ev-hpa-down", "web-hpa", "h1", 2, 3, 26*time.Minute, 6*time.Minute),
	)
	recs, _ := runRecords(t, testCommand(objs...), "--workload=Deployment/prod/web")
	thrash := thrashRecords(t, recs)
	if len(thrash) != 1 {
		t.Fatalf("thrash findings = %d, want 1 (aggregated-count envelope):\n%v", len(thrash), recs)
	}
	if thrash[0]["replicas"] != "6->2->6->2" {
		t.Errorf("replicas = %q, want 6->2->6->2", thrash[0]["replicas"])
	}
}

// TestHPAThrash_WindowBoundsFlips: the same oscillation spread wider
// than --hpa-window stops counting as thrash.
func TestHPAThrash_WindowBoundsFlips(t *testing.T) {
	objs := append(webTree(), hpaFixture("prod", "web-hpa", "h1", "Deployment", "web"))
	objs = append(objs, oscillation("prod", "web-hpa", "h1")...)
	// Flips happen at -15m and -10m: a 3m window can never hold both.
	recs, _ := runRecords(t, testCommand(objs...),
		"--workload=Deployment/prod/web", "--hpa-window=3m")
	if thrash := thrashRecords(t, recs); len(thrash) != 0 {
		t.Errorf("flips 5m apart fired inside a 3m window: %v", thrash)
	}
	// --hpa-flips above what the sequence contains: no fire.
	recs, _ = runRecords(t, testCommand(objs...),
		"--workload=Deployment/prod/web", "--hpa-flips=3")
	if thrash := thrashRecords(t, recs); len(thrash) != 0 {
		t.Errorf("2 flips fired with --hpa-flips=3: %v", thrash)
	}
}

// TestHPAThrash_OtherWorkloadsHPAIsExcluded: an oscillating HPA that
// targets a DIFFERENT workload stays out of this workload's timeline
// and analysis.
func TestHPAThrash_OtherWorkloadsHPAIsExcluded(t *testing.T) {
	objs := append(webTree(), hpaFixture("prod", "other-hpa", "h2", "Deployment", "other"))
	objs = append(objs, oscillation("prod", "other-hpa", "h2")...)
	recs, _ := runRecords(t, testCommand(objs...), "--workload=Deployment/prod/web")
	if len(recs) != 0 {
		t.Errorf("another workload's HPA leaked into the timeline: %v", recs)
	}
}

// TestHPAThrash_NamespaceMode: no workload, no HPA List — the
// analysis still runs off the event stream alone (no target detail,
// which needs the HPA object).
func TestHPAThrash_NamespaceMode(t *testing.T) {
	objs := oscillation("prod", "web-hpa", "h1")
	recs, _ := runRecords(t, testCommand(objs...), "--namespace=prod")
	thrash := thrashRecords(t, recs)
	if len(thrash) != 1 {
		t.Fatalf("thrash findings = %d, want 1:\n%v", len(thrash), recs)
	}
	if _, ok := thrash[0]["target"]; ok {
		t.Errorf("namespace mode cannot know the scale target, but emitted target=%q", thrash[0]["target"])
	}
}
