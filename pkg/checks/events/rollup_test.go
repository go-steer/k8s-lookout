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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func TestPodOwner(t *testing.T) {
	for name, want := range map[string]string{
		"anetd-bxg2q": "anetd", // DaemonSet
		"metrics-server-v1.36.0-74b7966557-fbm8g": "metrics-server-v1.36.0", // Deployment: hash + suffix
		"web-7d9c4bf6d8-x2n8p":                    "web",                    // Deployment
		"postgres-0":                              "postgres",               // StatefulSet
		"backup-28812345-zq4wk":                   "backup",                 // CronJob's Job
		"kube-proxy":                              "",                       // no generated part
		"api-server":                              "",                       // 'server' is not a generated suffix
	} {
		if got := podOwner(name); got != want {
			t.Errorf("podOwner(%q) = %q, want %q", name, got, want)
		}
	}
}

// dsChurn is a node cycle's worth of events in kube-system: five pods
// of DaemonSet anetd each Pulled and Started, four of them warned
// NodeNotReady, and two pods of a Deployment Started.
func dsChurn() []runtime.Object {
	var objs []runtime.Object
	pods := []string{"anetd-bxg2q", "anetd-c4kzp", "anetd-d7wmn", "anetd-fh2ls", "anetd-gq9tv"}
	for i, p := range pods {
		ref := objRef("Pod", "kube-system", p, "uid-"+p)
		age := time.Duration(10+i) * time.Minute
		objs = append(objs,
			event("kube-system", p+".pulled", ref, corev1.EventTypeNormal, "Pulled", "Successfully pulled image anetd:1.2", 1, age, age),
			event("kube-system", p+".started", ref, corev1.EventTypeNormal, "Started", "Started container anetd", 1, age, age),
		)
		if i < 4 {
			objs = append(objs, event("kube-system", p+".nnr", ref, corev1.EventTypeWarning, "NodeNotReady", "Node is not ready", 1, age, age))
		}
	}
	for _, p := range []string{"web-7d9c4bf6d8-x2n8p", "web-7d9c4bf6d8-k5r2w"} {
		ref := objRef("Pod", "kube-system", p, "uid-"+p)
		objs = append(objs, event("kube-system", p+".started", ref, corev1.EventTypeNormal, "Started", "Started container web", 1, 5*time.Minute, 5*time.Minute))
	}
	return objs
}

// TestNamespaceTimelineRollsUpAnOwner: five anetd pods' Pulled and
// Started each become one entry naming the owner, counting the objects
// and naming the newest three; four NodeNotReady warnings stay per pod
// (below the warning threshold of five); the Deployment's two pods stay
// per pod (below three).
func TestNamespaceTimelineRollsUpAnOwner(t *testing.T) {
	recs, summary := runRecords(t, testCommand(dsChurn()...), "--namespace=kube-system")
	byName := map[string][]record{}
	for _, r := range recs {
		byName[r["name"]+"/"+r["reason"]] = append(byName[r["name"]+"/"+r["reason"]], r)
	}
	pulled := byName["anetd-*/Pulled"]
	if len(pulled) != 1 {
		t.Fatalf("want one rolled-up Pulled entry, got %d:\n%v", len(pulled), recs)
	}
	r := pulled[0]
	for k, want := range map[string]string{
		"kind": "event.normal", "kind_of_object": "Pod", "objects": "5", "count": "5",
		"examples": "anetd-bxg2q,anetd-c4kzp,anetd-d7wmn",
		"message":  "5 pods: Successfully pulled image anetd:1.2",
	} {
		if r[k] != want {
			t.Errorf("rolled-up %s = %q, want %q", k, r[k], want)
		}
	}
	if r["first_seen"] == r["last_seen"] {
		t.Errorf("first/last seen should span the members: %v", r)
	}
	if n := len(byName["anetd-*/NodeNotReady"]); n != 0 {
		t.Errorf("four warnings are below the threshold and stay per pod, got a rollup")
	}
	if got := countReason(recs, "NodeNotReady"); got != 4 {
		t.Errorf("want 4 per-pod NodeNotReady entries, got %d", got)
	}
	if len(byName["web-*/Started"]) != 0 || countPrefix(recs, "web-7d9c4bf6d8-") != 2 {
		t.Errorf("two pods of one owner stay per pod:\n%v", recs)
	}
	if summary["findings"] != fmt.Sprint(len(recs)) {
		t.Errorf("summary findings=%s, emitted %d", summary["findings"], len(recs))
	}
}

// TestFiveWarningsRollUpAndNameEveryObject: at the warning threshold a
// rolled-up entry still names all five objects.
func TestFiveWarningsRollUpAndNameEveryObject(t *testing.T) {
	objs := dsChurn()
	ref := objRef("Pod", "kube-system", "anetd-gq9tv", "uid-anetd-gq9tv")
	objs = append(objs, event("kube-system", "anetd-gq9tv.nnr", ref, corev1.EventTypeWarning, "NodeNotReady", "Node is not ready", 1, 14*time.Minute, 14*time.Minute))
	recs, _ := runRecords(t, testCommand(objs...), "--namespace=kube-system")
	for _, r := range recs {
		if r["name"] == "anetd-*" && r["reason"] == "NodeNotReady" {
			if r["objects"] != "5" || len(strings.Split(r["examples"], ",")) != 5 || r["severity"] != emit.SeverityWarning {
				t.Errorf("want 5 objects, all named, warning: %v", r)
			}
			return
		}
	}
	t.Fatalf("five warnings should roll up:\n%v", recs)
}

// TestPerObjectAndWorkloadTimelinesDoNotRollUp: --per-object restores
// one entry per object, and so does a workload timeline.
func TestPerObjectAndWorkloadTimelinesDoNotRollUp(t *testing.T) {
	recs, _ := runRecords(t, testCommand(dsChurn()...), "--namespace=kube-system", "--per-object")
	for _, r := range recs {
		if r["objects"] != "" {
			t.Fatalf("--per-object must not roll up: %v", r)
		}
	}
	if got := countReason(recs, "Pulled"); got != 5 {
		t.Errorf("want 5 per-pod Pulled entries, got %d", got)
	}
}

// TestNodeEventsRollUpByReason: Node events group by reason alone.
func TestNodeEventsRollUpByReason(t *testing.T) {
	var objs []runtime.Object
	for i, n := range []string{"gk3-pool-a-1", "gk3-pool-b-2", "gk3-pool-c-3"} {
		age := time.Duration(20+i) * time.Minute
		objs = append(objs, event("default", n+".reg", objRef("Node", "", n, "uid-"+n), corev1.EventTypeNormal, "RegisteredNode", "Node registered", 1, age, age))
	}
	recs, _ := runRecords(t, testCommand(objs...), "-A")
	if len(recs) != 1 || recs[0]["name"] != "*" || recs[0]["kind_of_object"] != "Node" || recs[0]["objects"] != "3" {
		t.Fatalf("want one Node rollup:\n%v", recs)
	}
}

func TestRolledUpTimelineContract(t *testing.T) {
	res := checktest.Run(t, testCommand(dsChurn()...), "--namespace=kube-system")
	if err := checktest.Verify(testCommand(), res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("contract: %v", err)
	}
}

func countReason(recs []record, reason string) int {
	n := 0
	for _, r := range recs {
		if r["reason"] == reason {
			n++
		}
	}
	return n
}

func countPrefix(recs []record, prefix string) int {
	n := 0
	for _, r := range recs {
		if strings.HasPrefix(r["name"], prefix) {
			n++
		}
	}
	return n
}
