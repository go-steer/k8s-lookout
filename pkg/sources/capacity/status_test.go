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

package capacity

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// legacyStatus is the pre-1.30 text format, shaped after the blocks
// upstream CA writes (clusterstate/api/utils.go).
const legacyStatus = `Cluster-autoscaler status at 2026-07-26 10:00:00 +0000 UTC:
Cluster-wide:
  Health:      Healthy (ready=3 unready=0 notStarted=0 longNotStarted=0 registered=3 longUnregistered=0)
               LastProbeTime:      2026-07-26 10:00:00 +0000 UTC
  ScaleUp:     InProgress (ready=3 registered=3)
  ScaleDown:   NoCandidates (candidates=0)

NodeGroups:
  Name:        https://content.googleapis.com/compute/v1/projects/p/zones/us-east1-b/instanceGroups/mig-a
    Health:      Healthy (ready=1 unready=0 notStarted=0 longNotStarted=0 registered=1 longUnregistered=0 cloudProviderTarget=3 (minSize=0, maxSize=5))
                 LastProbeTime:      2026-07-26 10:00:00 +0000 UTC
    ScaleUp:     Backoff (ready=1 cloudProviderTarget=3)
    ScaleDown:   NoCandidates (candidates=0)
  Name:        https://content.googleapis.com/compute/v1/projects/p/zones/us-east1-c/instanceGroups/mig-b
    Health:      Healthy (ready=2 unready=0 notStarted=0 longNotStarted=0 registered=2 longUnregistered=0 cloudProviderTarget=2 (minSize=0, maxSize=5))
    ScaleUp:     NoActivity (ready=2 cloudProviderTarget=2)
`

// yamlStatusDoc is the CA ≥ 1.30 yaml format (upstream
// ClusterAutoscalerStatus api type).
const yamlStatusDoc = `time: "2026-07-26 10:00:00 +0000 UTC"
autoscalerStatus: Running
clusterWide:
  health:
    status: Healthy
nodeGroups:
- name: mig-a
  health:
    status: Unhealthy
    nodeCounts:
      registered:
        total: 1
        ready: 1
        notStarted: 0
      longUnregistered: 0
      unregistered: 0
    cloudProviderTarget: 3
    minSize: 0
    maxSize: 5
  scaleUp:
    status: Backoff
    backoffInfo:
      errorCode: QUOTA_EXCEEDED
      errorMessage: Instance creation failed due to quota
- name: mig-b
  health:
    status: Healthy
    nodeCounts:
      registered:
        total: 2
        ready: 2
    cloudProviderTarget: 2
  scaleUp:
    status: NoActivity
`

func TestParseStatus_LegacyText(t *testing.T) {
	t.Parallel()
	got, err := parseStatus(legacyStatus)
	if err != nil {
		t.Fatalf("parseStatus(legacy): %v", err)
	}
	want := []nodeGroupStatus{
		{Name: "mig-a", HealthStatus: "Healthy", Ready: 1, Registered: 1, Target: 3, Backoff: true},
		{Name: "mig-b", HealthStatus: "Healthy", Ready: 2, Registered: 2, Target: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("legacy parse =\n  %+v\nwant\n  %+v", got, want)
	}
}

func TestParseStatus_YAML(t *testing.T) {
	t.Parallel()
	got, err := parseStatus(yamlStatusDoc)
	if err != nil {
		t.Fatalf("parseStatus(yaml): %v", err)
	}
	want := []nodeGroupStatus{
		{Name: "mig-a", HealthStatus: "Unhealthy", Ready: 1, Registered: 1, Target: 3,
			Backoff: true, BackoffError: "QUOTA_EXCEEDED: Instance creation failed due to quota"},
		{Name: "mig-b", HealthStatus: "Healthy", Ready: 2, Registered: 2, Target: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("yaml parse =\n  %+v\nwant\n  %+v", got, want)
	}
}

func TestParseStatus_GarbageFailsLoudly(t *testing.T) {
	t.Parallel()
	if _, err := parseStatus(""); err == nil {
		t.Error("empty status parsed without error")
	}
	if _, err := parseStatus("not a status at all"); err == nil {
		t.Error("junk status parsed without error")
	}
}

// TestJudgeStatus_GapSustainedThenEscalated walks the episode
// lifecycle on the yaml fixture: a fresh gap is silent, a sustained
// gap fires (critical here — mig-a is in Backoff WITH an error), and
// a closed gap retires the episode so the next one fires fresh.
func TestJudgeStatus_GapSustainedThenEscalated(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	t0 := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	groups, err := parseStatus(yamlStatusDoc)
	if err != nil {
		t.Fatal(err)
	}

	s.judgeStatus(groups, t0)
	if len(got) != 0 {
		t.Fatalf("fresh gap fired immediately: %+v (GapSustain not honored)", got)
	}
	s.judgeStatus(groups, t0.Add(1*time.Minute))
	if len(got) != 0 {
		t.Fatalf("gap fired before GapSustain: %+v", got)
	}
	s.judgeStatus(groups, t0.Add(3*time.Minute))
	if len(got) != 1 {
		t.Fatalf("sustained gap fired %d signals, want 1", len(got))
	}
	sig := got[0]
	if sig.Kind != KindScaleUpGap || sig.Severity != engine.SeverityCritical {
		t.Errorf("gap signal = (%s, %s), want (capacity.scaleup_gap, critical — backoff with error)", sig.Kind, sig.Severity)
	}
	if sig.Name != "mig-a" || sig.Key.UID != "nodegroup:mig-a" || sig.KindOfObject != "NodeGroup" {
		t.Errorf("gap identity = %s/%s/%s, want NodeGroup mig-a", sig.KindOfObject, sig.Name, sig.Key.UID)
	}
	for _, want := range []string{"cloudProviderTarget=3", "ready=1", "QUOTA_EXCEEDED", "health=Unhealthy"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("gap message %q missing evidence %q", sig.Message, want)
		}
	}

	// Same status again: episode latch — no re-fire.
	s.judgeStatus(groups, t0.Add(4*time.Minute))
	if len(got) != 1 {
		t.Fatalf("episode re-fired: %d signals", len(got))
	}

	// Gap closes (target reached), then reopens: fires fresh.
	closed, _ := parseStatus(strings.Replace(yamlStatusDoc, "cloudProviderTarget: 3", "cloudProviderTarget: 1", 1))
	s.judgeStatus(closed, t0.Add(5*time.Minute))
	s.judgeStatus(groups, t0.Add(6*time.Minute))
	s.judgeStatus(groups, t0.Add(10*time.Minute))
	if len(got) != 2 {
		t.Fatalf("reopened gap fired %d total, want 2", len(got))
	}
}

// TestJudgeStatus_LegacyBackoffWithoutErrorIsWarning: the legacy text
// format carries no error detail, so even a Backoff nodegroup caps at
// warning ("critical if backoff WITH error", scope rule).
func TestJudgeStatus_LegacyBackoffWithoutErrorIsWarning(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	groups, err := parseStatus(legacyStatus)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	s.judgeStatus(groups, t0)
	s.judgeStatus(groups, t0.Add(3*time.Minute))
	if len(got) != 1 {
		t.Fatalf("fired %d signals, want 1 (only mig-a has a gap)", len(got))
	}
	if got[0].Severity != engine.SeverityWarning {
		t.Errorf("legacy backoff-no-error severity = %s, want warning", got[0].Severity)
	}
	if !strings.Contains(got[0].Message, "scale-up in backoff") {
		t.Errorf("message %q missing backoff evidence", got[0].Message)
	}
}

// TestPollStatus_MissingConfigMapLogsOnceAndIdles: no CA on the
// cluster is a legal deployment — one loud log, no error, no signal.
func TestPollStatus_MissingConfigMapLogsOnceAndIdles(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	var logs []string
	s.logf = func(format string, args ...any) { logs = append(logs, format) }
	s.emit = func(engine.Signal) { t.Error("missing ConfigMap must not emit") }

	now := time.Now()
	if err := s.pollStatus(context.Background(), now); err != nil {
		t.Fatalf("missing ConfigMap returned error: %v", err)
	}
	if err := s.pollStatus(context.Background(), now); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "not found") {
		t.Errorf("logs = %q, want exactly one not-found line", logs)
	}
}

// TestPollStatus_ReadsConfigMapEndToEnd: fake-clientset ConfigMap →
// parse → judge, both formats through the same poll path.
func TestPollStatus_ReadsConfigMapEndToEnd(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, doc string }{
		{"legacy", legacyStatus},
		{"yaml", yamlStatusDoc},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := fake.NewSimpleClientset(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster-autoscaler-status", Namespace: "kube-system"},
				Data:       map[string]string{"status": tc.doc},
			})
			s := New(client, cloud.NoProvider, Config{})
			var got []engine.Signal
			s.emit = func(sig engine.Signal) { got = append(got, sig) }

			t0 := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
			if err := s.pollStatus(context.Background(), t0); err != nil {
				t.Fatalf("pollStatus: %v", err)
			}
			if err := s.pollStatus(context.Background(), t0.Add(3*time.Minute)); err != nil {
				t.Fatalf("pollStatus: %v", err)
			}
			if len(got) != 1 || got[0].Kind != KindScaleUpGap {
				t.Fatalf("%s end-to-end fired %d signals (%v), want 1 scaleup_gap", tc.name, len(got), got)
			}
		})
	}
}
