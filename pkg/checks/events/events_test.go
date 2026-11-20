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
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

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
	checktest.Golden(t, "testdata/events-workload.golden", res.Stdout)
}

// TestWorkloadTimelineUnderViewRole is #546: under the built-in `view`
// role Secrets and RBAC objects are forbidden. The owner tree is
// ownerReferences only, so the timeline never needed them — it must
// not even ask for them, and its payload must be byte-identical to the
// full-access golden (nothing degraded, so nothing to report).
func TestWorkloadTimelineUnderViewRole(t *testing.T) {
	objs := append(webTree(), webEvents()...)
	objs = append(objs, hpaFixture("prod", "web-hpa", "h1", "Deployment", "web"))
	objs = append(objs, oscillation("prod", "web-hpa", "h1")...)
	client := fake.NewClientset(objs...)
	var asked []string
	for _, gr := range []schema.GroupResource{
		{Resource: "secrets"},
		{Group: "rbac.authorization.k8s.io", Resource: "rolebindings"},
		{Group: "rbac.authorization.k8s.io", Resource: "roles"},
		{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"},
		{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"},
	} {
		client.PrependReactor("list", gr.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
			asked = append(asked, gr.String())
			return true, nil, apierrors.NewForbidden(gr, "", errors.New("denied by test"))
		})
	}
	cmd := newCommand(func(context.Context) (kubernetes.Interface, error) { return client, nil }, func() time.Time { return testNow })
	res := checktest.Run(t, cmd, "--workload=Deployment/prod/web")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	if len(asked) > 0 {
		t.Errorf("the owner-tree pass listed %v; it needs none of them", asked)
	}
	checktest.Golden(t, "testdata/events-workload.golden", res.Stdout)
}

// TestWorkloadTimelineReplicaSetsRefused: a custom role refusing
// ReplicaSets (#584) cuts the Deployment → ReplicaSet → Pod hop, so the
// pods' events drop out of the tree — and the timeline says so in one
// read.unavailable record instead of reading as a quiet workload.
func TestWorkloadTimelineReplicaSetsRefused(t *testing.T) {
	client := fake.NewClientset(append(webTree(), webEvents()...)...)
	checktest.RefuseRead(&client.Fake, schema.GroupResource{Group: "apps", Resource: "replicasets"})
	cmd := newCommand(func(context.Context) (kubernetes.Interface, error) { return client, nil }, func() time.Time { return testNow })
	res := checktest.Run(t, cmd, "--workload=Deployment/prod/web")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, want 0; stderr: %s", res.Code, res.Stderr)
	}
	want := `kind=read.unavailable severity=info reason=ListForbidden message="forbidden: list replicasets.apps — namespaced, this identity lacks it; grant list on replicasets (apps) via a ClusterRole or Role, as lookout's shipped ClusterRole does — events of replicasets.apps in the owner-reference tree, and of anything reached only through them, are not in the timeline" resource=replicasets.apps`
	if !strings.Contains(res.Stdout, want) {
		t.Errorf("missing the refusal record:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "reason=ScalingReplicaSet") {
		t.Errorf("the Deployment's own events were lost with the hop:\n%s", res.Stdout)
	}
	if err := checktest.Verify(cmd, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("contract: %v", err)
	}
}

// TestSeriesFallback verifies issue #833 on the read path (`triage events`):
// events.k8s.io/v1 series-only Events populate count and last_seen from
// Series.{Count,LastObservedTime} (so an Event whose EventTime predates
// --since still survives when Series.LastObservedTime is fresh), first
// occurrences with Series == nil report count=1, and legacy fields win when
// both are populated.
func TestSeriesFallback(t *testing.T) {
	objs := []runtime.Object{
		// 1. Series-only repeat whose EventTime (40m ago) is older than --since=15m
		//    but Series.LastObservedTime (2m ago) is inside the window.
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "prod", Name: "ev-series-repeat"},
			InvolvedObject: objRef("Pod", "prod", "hog-repeat", "u-repeat"),
			Type:           corev1.EventTypeWarning,
			Reason:         "FailedScheduling",
			Message:        "0/3 nodes are available: 3 Insufficient cpu.",
			EventTime:      metav1.MicroTime{Time: testNow.Add(-40 * time.Minute)},
			Series: &corev1.EventSeries{
				Count:            9,
				LastObservedTime: metav1.MicroTime{Time: testNow.Add(-2 * time.Minute)},
			},
			ReportingController: "default-scheduler",
		},
		// 2. Series-only first occurrence (EventTime set, Series == nil, Count == 0).
		&corev1.Event{
			ObjectMeta:          metav1.ObjectMeta{Namespace: "prod", Name: "ev-series-first"},
			InvolvedObject:      objRef("Pod", "prod", "hog-first", "u-first"),
			Type:                corev1.EventTypeWarning,
			Reason:              "FailedScheduling",
			Message:             "0/3 nodes are available: 3 Insufficient memory.",
			EventTime:           metav1.MicroTime{Time: testNow.Add(-5 * time.Minute)},
			ReportingController: "default-scheduler",
		},
		// 3. Both legacy and Series populated: legacy Count and LastTimestamp win.
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "prod", Name: "ev-both"},
			InvolvedObject: objRef("Pod", "prod", "hog-both", "u-both"),
			Type:           corev1.EventTypeWarning,
			Reason:         "FailedScheduling",
			Message:        "0/3 nodes are available: legacy wins.",
			Count:          7,
			FirstTimestamp: ago(10 * time.Minute),
			LastTimestamp:  ago(3 * time.Minute),
			EventTime:      metav1.MicroTime{Time: testNow.Add(-10 * time.Minute)},
			Series: &corev1.EventSeries{
				Count:            2,
				LastObservedTime: metav1.MicroTime{Time: testNow.Add(-1 * time.Minute)},
			},
			Source: corev1.EventSource{Component: "default-scheduler"},
		},
	}

	recs, summary := runRecords(t, testCommand(objs...), "--namespace=prod", "--since=15m", "--per-object")
	if summary["scanned"] != "3" {
		t.Errorf("scanned = %s, want 3", summary["scanned"])
	}
	if len(recs) != 3 {
		t.Fatalf("findings = %d, want 3 (series repeat must survive --since=15m):\n%v", len(recs), recs)
	}
	byName := make(map[string]record, len(recs))
	for _, r := range recs {
		byName[r["name"]] = r
	}

	rep := byName["hog-repeat"]
	if rep["count"] != "9" {
		t.Errorf("hog-repeat count = %q, want 9", rep["count"])
	}
	if rep["first_seen"] != ago(40*time.Minute).UTC().Format(time.RFC3339) {
		t.Errorf("hog-repeat first_seen = %q, want %s", rep["first_seen"], ago(40*time.Minute).UTC().Format(time.RFC3339))
	}
	if rep["last_seen"] != ago(2*time.Minute).UTC().Format(time.RFC3339) {
		t.Errorf("hog-repeat last_seen = %q, want %s", rep["last_seen"], ago(2*time.Minute).UTC().Format(time.RFC3339))
	}

	first := byName["hog-first"]
	if first["count"] != "1" {
		t.Errorf("hog-first count = %q, want 1", first["count"])
	}
	wantFirstTS := ago(5 * time.Minute).UTC().Format(time.RFC3339)
	if first["first_seen"] != wantFirstTS || first["last_seen"] != wantFirstTS {
		t.Errorf("hog-first first_seen/last_seen = %q/%q, want both %s", first["first_seen"], first["last_seen"], wantFirstTS)
	}

	both := byName["hog-both"]
	if both["count"] != "7" {
		t.Errorf("hog-both count = %q, want 7 (legacy wins)", both["count"])
	}
	if both["last_seen"] != ago(3*time.Minute).UTC().Format(time.RFC3339) {
		t.Errorf("hog-both last_seen = %q, want %s (legacy wins)", both["last_seen"], ago(3*time.Minute).UTC().Format(time.RFC3339))
	}

	checktest.VerifyContract(t, testCommand(objs...), "--namespace=prod", "--since=15m")
}
