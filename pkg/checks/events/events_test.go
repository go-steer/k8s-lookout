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
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// testNow anchors every fixture timestamp and the injected clock, so
// the --since cutoff math is deterministic.
var testNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

func ago(d time.Duration) metav1.Time { return metav1.Time{Time: testNow.Add(-d)} }

func testCommand(objs ...runtime.Object) checks.Command {
	client := fake.NewClientset(objs...)
	source := func(context.Context) (kubernetes.Interface, error) { return client, nil }
	return newCommand(source, func() time.Time { return testNow })
}

// --- fixtures ---------------------------------------------------------------

func deployment(ns, name, uid string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, UID: types.UID(uid),
	}}
}

func replicaSet(ns, name, uid, ownerDeploy string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, UID: types.UID(uid),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "Deployment", Name: ownerDeploy, Controller: ptr(true),
		}},
	}}
}

func pod(ns, name, uid, ownerRS string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, UID: types.UID(uid),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: ownerRS, Controller: ptr(true),
		}},
	}}
}

func ptr[T any](v T) *T { return &v }

// event builds a core/v1 Event fixture. first/last are ages before
// testNow.
func event(ns, name string, obj corev1.ObjectReference, evType, reason, message string, count int32, first, last time.Duration) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: ns, Name: name},
		InvolvedObject: obj,
		Type:           evType,
		Reason:         reason,
		Message:        message,
		Count:          count,
		FirstTimestamp: ago(first),
		LastTimestamp:  ago(last),
		Source:         corev1.EventSource{Component: "kubelet"},
	}
}

func objRef(kind, ns, name, uid string) corev1.ObjectReference {
	return corev1.ObjectReference{Kind: kind, Namespace: ns, Name: name, UID: types.UID(uid)}
}

// webTree is the owner-reference tree under test: Deployment web →
// ReplicaSet web-abc → pods web-abc-1/web-abc-2, plus an unrelated
// Deployment other with its own pod.
func webTree() []runtime.Object {
	return []runtime.Object{
		deployment("prod", "web", "d1"),
		replicaSet("prod", "web-abc", "r1", "web"),
		pod("prod", "web-abc-1", "p1", "web-abc"),
		pod("prod", "web-abc-2", "p2", "web-abc"),
		deployment("prod", "other", "d2"),
		replicaSet("prod", "other-def", "r2", "other"),
		pod("prod", "other-def-1", "q1", "other-def"),
	}
}

// webEvents is the standard event fixture over webTree: a reason
// family to collapse, a Normal deployment event, an out-of-tree
// event, and one older than the default --since.
func webEvents() []runtime.Object {
	return []runtime.Object{
		event("prod", "ev-pull-1", objRef("Pod", "prod", "web-abc-1", "p1"),
			corev1.EventTypeWarning, "ErrImagePull",
			"rpc error: code = NotFound desc = failed to pull image", 1, 30*time.Minute, 30*time.Minute),
		event("prod", "ev-pull-2", objRef("Pod", "prod", "web-abc-1", "p1"),
			corev1.EventTypeWarning, "ImagePullBackOff",
			"Back-off pulling image \"registry.example/web:v9\"", 4, 29*time.Minute, 5*time.Minute),
		event("prod", "ev-scale", objRef("Deployment", "prod", "web", "d1"),
			corev1.EventTypeNormal, "ScalingReplicaSet",
			"Scaled up replica set web-abc to 2", 1, 40*time.Minute, 40*time.Minute),
		event("prod", "ev-other", objRef("Pod", "prod", "other-def-1", "q1"),
			corev1.EventTypeWarning, "BackOff",
			"Back-off restarting failed container", 3, 10*time.Minute, 3*time.Minute),
		event("prod", "ev-stale", objRef("Pod", "prod", "web-abc-2", "p2"),
			corev1.EventTypeNormal, "Scheduled",
			"Successfully assigned prod/web-abc-2 to node-1", 1, 2*time.Hour, 2*time.Hour),
	}
}

// --- registration + contract ------------------------------------------------

func TestRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("triage events")
	if !ok {
		t.Fatal("triage events is not registered in the default registry")
	}
	if c.MCPName != "k8s_event_timeline" {
		t.Errorf("MCP name = %q, want k8s_event_timeline", c.MCPName)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("registered command invalid: %v", err)
	}
}

func TestContract(t *testing.T) {
	objs := append(webTree(), webEvents()...)
	checktest.VerifyContract(t, testCommand(objs...), "--namespace=prod")
	checktest.VerifyContract(t, testCommand(objs...), "--workload=Deployment/prod/web")
	checktest.VerifyContract(t, testCommand(objs...), "-A", "--since=3h")
}

// --- scoping ----------------------------------------------------------------

func TestNoTargetIsUsageError(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--workload=Deployment/prod/web", "-A"},
		{"--workload=Deployment/prod/web", "--namespace=staging"},
		{"--workload=Deployment/prod/web", "--hpa-window=0s"},
		{"--workload=Deployment/prod/web", "--hpa-flips=0"},
	} {
		res := checktest.Run(t, testCommand(webTree()...), args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("args %v: exit = %d, want %d (stderr: %s)", args, res.Code, emit.ExitUsage, res.Stderr)
		}
		if res.Stdout != "" {
			t.Errorf("args %v: usage error must keep stdout clean, got %q", args, res.Stdout)
		}
	}
}

func TestUnknownWorkloadIsRuntimeError(t *testing.T) {
	res := checktest.Run(t, testCommand(webTree()...), "--workload=Deployment/prod/nonesuch")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit = %d, want %d (stderr: %s)", res.Code, emit.ExitRuntime, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "not found") {
		t.Errorf("stderr should name the missing workload, got %q", res.Stderr)
	}
}

// --- timeline ---------------------------------------------------------------

// record is one parsed finding line.
type record map[string]string

func runRecords(t *testing.T, cmd checks.Command, args ...string) (recs []record, summary record) {
	t.Helper()
	res := checktest.Run(t, cmd, args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	for _, line := range lines[:len(lines)-1] {
		recs = append(recs, parseLogfmtLine(t, line))
	}
	return recs, parseLogfmtLine(t, lines[len(lines)-1])
}

func parseLogfmtLine(t *testing.T, line string) record {
	t.Helper()
	rec := record{}
	rest := line
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			t.Fatalf("bad logfmt line %q", line)
		}
		key := rest[:eq]
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			q, err := strconv.QuotedPrefix(rest)
			if err != nil {
				t.Fatalf("bad quoted value in %q: %v", line, err)
			}
			val, err = strconv.Unquote(q)
			if err != nil {
				t.Fatal(err)
			}
			rest = rest[len(q):]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		rec[key] = val
		rest = strings.TrimPrefix(rest, " ")
	}
	return rec
}

// TestWorkloadTimeline is the owner-tree + dedup + chronology test:
// out-of-tree and out-of-window events vanish, the ImagePull reason
// family collapses to one entry with summed counts, and entries
// order by newest activity.
func TestWorkloadTimeline(t *testing.T) {
	objs := append(webTree(), webEvents()...)
	recs, summary := runRecords(t, testCommand(objs...), "--workload=Deployment/prod/web")

	if summary["scanned"] != "5" {
		t.Errorf("scanned = %s, want 5 (every event listed in prod)", summary["scanned"])
	}
	if len(recs) != 2 {
		t.Fatalf("findings = %d, want 2 (tree-filtered, family-collapsed, since-filtered):\n%v", len(recs), recs)
	}
	// Chronological: the deployment scale event (-40m) precedes the
	// image-pull family (last activity -5m).
	first, second := recs[0], recs[1]
	if first["reason"] != "ScalingReplicaSet" || first["kind"] != "event.normal" || first["severity"] != "info" {
		t.Errorf("first entry = %v, want the Normal ScalingReplicaSet event", first)
	}
	if second["reason"] != "ImagePullBackOff" {
		t.Errorf("second entry reason = %s, want canonical ImagePullBackOff", second["reason"])
	}
	if second["kind"] != "event.warning" || second["severity"] != "warning" {
		t.Errorf("Warning events must map to event.warning/warning, got %v", second)
	}
	if second["count"] != "5" {
		t.Errorf("count = %s, want 5 (1 ErrImagePull + 4 ImagePullBackOff repeats)", second["count"])
	}
	if second["variants"] != "ErrImagePull,ImagePullBackOff" {
		t.Errorf("variants = %q, want the collapsed raw reasons", second["variants"])
	}
	if second["first_seen"] != ago(30*time.Minute).UTC().Format(time.RFC3339) {
		t.Errorf("first_seen = %s, want the family's oldest activity", second["first_seen"])
	}
	if second["last_seen"] != ago(5*time.Minute).UTC().Format(time.RFC3339) {
		t.Errorf("last_seen = %s, want the family's newest activity", second["last_seen"])
	}
	for _, r := range recs {
		if r["name"] == "other-def-1" {
			t.Errorf("out-of-tree event leaked into the workload timeline: %v", r)
		}
		if r["reason"] == "Scheduled" {
			t.Errorf("event older than --since leaked into the timeline: %v", r)
		}
	}
}

// TestPodTargetResolvesWholeTree: targeting a leaf pod climbs to the
// Deployment root and includes sibling objects' events.
func TestPodTargetResolvesWholeTree(t *testing.T) {
	objs := append(webTree(), webEvents()...)
	recs, _ := runRecords(t, testCommand(objs...), "--workload=Pod/prod/web-abc-2")
	if len(recs) != 2 {
		t.Fatalf("findings = %d, want 2 (the whole tree's timeline from a leaf pod)", len(recs))
	}
	if recs[0]["name"] != "web" || recs[1]["name"] != "web-abc-1" {
		t.Errorf("expected deployment + sibling-pod entries, got %v", recs)
	}
}

func TestNamespaceTimeline(t *testing.T) {
	objs := append(webEvents(),
		event("staging", "ev-stg", objRef("Pod", "staging", "canary-1", "s1"),
			corev1.EventTypeWarning, "OOMKilled", "container killed", 1, 8*time.Minute, 8*time.Minute))

	// --namespace=prod: no tree filter — the other-def-1 event now
	// shows; staging stays out.
	recs, summary := runRecords(t, testCommand(objs...), "--namespace=prod")
	if summary["scanned"] != "5" {
		t.Errorf("scanned = %s, want 5", summary["scanned"])
	}
	if len(recs) != 3 {
		t.Fatalf("findings = %d, want 3 (prod namespace timeline):\n%v", len(recs), recs)
	}
	for _, r := range recs {
		if r["namespace"] != "prod" {
			t.Errorf("namespace scope leaked: %v", r)
		}
	}
	// BackOff canonicalizes to CrashLoopBackOff even without a
	// CrashLoopBackOff variant present.
	if got := recs[len(recs)-1]["reason"]; got != "CrashLoopBackOff" {
		t.Errorf("newest entry reason = %s, want canonical CrashLoopBackOff for raw BackOff", got)
	}

	// -A: staging joins.
	recs, summary = runRecords(t, testCommand(objs...), "-A")
	if summary["scanned"] != "6" {
		t.Errorf("-A scanned = %s, want 6", summary["scanned"])
	}
	if len(recs) != 4 {
		t.Errorf("-A findings = %d, want 4:\n%v", len(recs), recs)
	}

	// --since tightens the window: only the image-pull family (last
	// activity 5m ago) and the BackOff (3m ago) survive a 6m window.
	recs, _ = runRecords(t, testCommand(objs...), "--namespace=prod", "--since=6m")
	if len(recs) != 2 {
		t.Errorf("--since=6m findings = %d, want 2 (only activity newer than 6m)", len(recs))
	}
}

// TestWorkloadTimelineGolden pins the full stdout byte-for-byte.
func TestWorkloadTimelineGolden(t *testing.T) {
	objs := append(webTree(), webEvents()...)
	objs = append(objs, hpaFixture("prod", "web-hpa", "h1", "Deployment", "web"))
	objs = append(objs, oscillation("prod", "web-hpa", "h1")...)
	res := checktest.Run(t, testCommand(objs...), "--workload=Deployment/prod/web")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	golden := filepath.Join("testdata", "events-workload.golden")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(golden, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_GOLDEN=1 to create)", err)
	}
	if res.Stdout != string(want) {
		t.Errorf("golden mismatch:\ngot:\n%s\nwant:\n%s", res.Stdout, want)
	}
}
