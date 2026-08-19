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

package delta

import (
	"strconv"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("triage delta")
	if !ok {
		t.Fatal("triage delta is not registered in the default registry")
	}
	if c.MCPName != "k8s_triage_delta" {
		t.Errorf("MCP name = %q, want k8s_triage_delta", c.MCPName)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("registered command invalid: %v", err)
	}
}

// TestHealthyClusterEmitsNothing is the §1-principle-5 test: a
// cluster where every class has healthy members produces zero
// finding lines and only the explicit summary.
func TestHealthyClusterEmitsNothing(t *testing.T) {
	cmd := testCommand(
		healthyPod("prod", "web-0"),
		healthyDeployment("prod", "web", 1),
		systemDeployment("coredns", map[string]string{"k8s-app": "kube-dns"}, 2, 2),
		healthyNode("node-0"),
		pdb("prod", "web-pdb", 1, 3, 2, 3),
		quota("prod", "compute", map[string][2]string{"pods": {"10", "1"}}),
		healthyCronJob("prod", "backup"),
	)
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	// 1 pod + 2 deployments + 1 node + 1 pdb + 1 quota + 1 cronjob = 7.
	if want := "scanned=7 findings=0 elapsed=100ms\n"; res.Stdout != want {
		t.Errorf("stdout = %q, want %q (healthy objects must emit nothing)", res.Stdout, want)
	}
}

// finding is the (kind, name, severity) triple the per-class tests
// assert on.
type finding struct{ kind, name, severity string }

// runFindings executes the command and parses stdout into triples
// plus the summary counts.
func runFindings(t *testing.T, cmd checks.Command, args ...string) (fs []finding, scanned int) {
	t.Helper()
	res := checktest.Run(t, cmd, args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	for _, line := range lines[:len(lines)-1] {
		rec := parseLogfmtLine(t, line)
		fs = append(fs, finding{kind: rec["kind"], name: rec["name"], severity: rec["severity"]})
	}
	sum := parseLogfmtLine(t, lines[len(lines)-1])
	scanned, err := strconv.Atoi(sum["scanned"])
	if err != nil {
		t.Fatalf("summary line %q: %v", lines[len(lines)-1], err)
	}
	if got, _ := strconv.Atoi(sum["findings"]); got != len(fs) {
		t.Fatalf("summary findings=%d but %d finding lines", got, len(fs))
	}
	return fs, scanned
}

// parseLogfmtLine understands exactly what emit's encoder produces.
func parseLogfmtLine(t *testing.T, line string) map[string]string {
	t.Helper()
	out := map[string]string{}
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
			val, _ = strconv.Unquote(q)
			rest = rest[len(q):]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		out[key] = val
		rest = strings.TrimPrefix(rest, " ")
	}
	return out
}

func assertFindings(t *testing.T, got []finding, want []finding) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got %d findings, want %d\ngot: %v", len(got), len(want), got)
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing finding %+v\ngot: %v", w, got)
		}
	}
}

func TestPodsClass(t *testing.T) {
	cmd := testCommand(
		crashloopPod("prod", "crash-1"),
		imagePullPod("prod", "pull-1"),
		oomPod("prod", "oom-1"),
		restartsPod("prod", "restart-1", 7),
		restartsPod("prod", "calm-1", 4), // below the default threshold: silence
		pendingPod("prod", "pend-1", 30*60e9, true),
		pendingPod("prod", "pend-2", 30*60e9, false),
		pendingPod("prod", "pend-3", 60e9, true), // young Pending: silence
		notReadyPod("prod", "nr-1"),
		evictedPod("prod", "ev-1"),
		healthyPod("prod", "ok-1"),
		rolloutDeployment("prod", "web"),
		stalledDeployment("prod", "stalled"),
		healthyDeployment("prod", "fine", 2),
		rolloutStatefulSet("prod", "db"),
		rolloutDaemonSet("prod", "agent", 3, 1),
		failedJob("prod", "etl"),
	)
	got, scanned := runFindings(t, cmd, "--only=pods")
	assertFindings(t, got, []finding{
		{"pod.crashloop", "crash-1", "critical"},
		{"pod.imagepull", "pull-1", "critical"},
		{"pod.oomkilled", "oom-1", "warning"},
		{"pod.restarts", "restart-1", "warning"},
		{"pod.pending", "pend-1", "critical"},
		{"pod.pending", "pend-2", "warning"},
		{"pod.notready", "nr-1", "warning"},
		{"pod.failed", "ev-1", "warning"},
		{"workload.rollout", "web", "warning"},
		{"workload.stalled", "stalled", "critical"},
		{"workload.rollout", "db", "critical"},
		{"workload.rollout", "agent", "warning"},
		{"job.failed", "etl", "warning"},
	})
	// 11 pods + 3 deployments + 1 sts + 1 ds + 1 job.
	if scanned != 17 {
		t.Errorf("scanned = %d, want 17", scanned)
	}
}

func TestRestartThresholdFlag(t *testing.T) {
	cmd := testCommand(restartsPod("prod", "restart-1", 3))
	got, _ := runFindings(t, cmd, "--only=pods", "--restarts=3")
	assertFindings(t, got, []finding{{"pod.restarts", "restart-1", "warning"}})
}

// TestReplicaFailure covers the one abnormality with no pod to find.
// A quota or admission denial creates zero pods, so every pod-level
// check in this package is silent and the Deployment's own condition
// is the only evidence in the cluster.
func TestReplicaFailure(t *testing.T) {
	cmd := testCommand(
		replicaFailureDeployment("prod", "etl"),
		replicaFailureAndStalledDeployment("prod", "both"),
		clearedReplicaFailureDeployment("prod", "recovered", 2),
	)
	got, _ := runFindings(t, cmd, "--only=pods")
	// "both" reports replicafailure only: most specific wins, one
	// finding per workload. "recovered" is silent — the condition is
	// present but False.
	assertFindings(t, got, []finding{
		{"workload.replicafailure", "etl", "critical"},
		{"workload.replicafailure", "both", "critical"},
	})
}

// TestReplicaFailureCarriesTheAdmissionError checks the part that
// makes the finding actionable: the condition message is the whole
// answer, so it must survive to stdout.
func TestReplicaFailureCarriesTheAdmissionError(t *testing.T) {
	cmd := testCommand(replicaFailureDeployment("prod", "etl"))
	res := checktest.Run(t, cmd, "--only=pods")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	rec := parseLogfmtLine(t, strings.Split(res.Stdout, "\n")[0])
	if rec["reason"] != "FailedCreate" {
		t.Errorf("reason = %q, want FailedCreate", rec["reason"])
	}
	if !strings.Contains(rec["message"], "exceeded quota: compute") {
		t.Errorf("message = %q, want the admission error verbatim", rec["message"])
	}
	if rec["desired"] != "3" || rec["ready"] != "0" {
		t.Errorf("rollout details = desired %q ready %q, want 3 and 0", rec["desired"], rec["ready"])
	}
}

func TestNodesClass(t *testing.T) {
	cmd := testCommand(
		healthyNode("n-ok"),
		notReadyNode("n-bad"),
		pressureNode("n-disk"),
		npdNode("n-kernel", "KernelDeadlock", "DockerHung"),
		npdNode("n-flappy", "FrequentKubeletRestart", "FrequentKubeletRestart"),
		cordonedNode("n-cordon"),
		podOnNode("prod", "held-1", "n-cordon"),
		podOnNode("prod", "held-2", "n-cordon"),
		cordonedNode("n-drained"), // no pods behind it: silence
		preemptNode("n-spot", "cloud.google.com/impending-node-termination"),
		preemptNode("n-scale", "ToBeDeletedByClusterAutoscaler"),
		preemptNode("n-cand", "DeletionCandidateOfClusterAutoscaler"),
	)
	got, scanned := runFindings(t, cmd, "--only=nodes")
	assertFindings(t, got, []finding{
		{"node.notready", "n-bad", "critical"},
		{"node.pressure", "n-disk", "critical"},
		{"node.condition", "n-kernel", "critical"},
		{"node.condition", "n-flappy", "warning"},
		{"node.cordoned", "n-cordon", "warning"},
		{"node.preempt", "n-spot", "critical"},
		{"node.preempt", "n-scale", "warning"},
		{"node.preempt", "n-cand", "info"},
	})
	// The 10 nodes only — the auxiliary pod list is not "scanned".
	if scanned != 10 {
		t.Errorf("scanned = %d, want 10 nodes", scanned)
	}
	if last := got[len(got)-1]; last.severity != "info" {
		t.Errorf("info finding should sort last, got %+v", last)
	}
	if first := got[0]; first.severity != "critical" {
		t.Errorf("critical findings should sort first, got %+v", first)
	}
}

func TestSystemClass(t *testing.T) {
	cmd := testCommand(
		systemDeployment("coredns", map[string]string{"k8s-app": "kube-dns"}, 2, 0),
		systemDaemonSet("kube-proxy", map[string]string{"k8s-app": "kube-proxy"}, 5, 4),
		systemDaemonSet("pdcsi-node", map[string]string{"k8s-app": "gcp-compute-persistent-disk-csi-driver"}, 3, 3), // healthy
		systemDeployment("random-app", nil, 2, 0),                                                                   // degraded but not a known add-on
		rolloutDeployment("prod", "web"),                                                                            // out of class
	)
	got, scanned := runFindings(t, cmd, "--only=system")
	assertFindings(t, got, []finding{
		{"addon.degraded", "coredns", "critical"},
		{"addon.degraded", "kube-proxy", "warning"},
	})
	// kube-system Deployments (2) + DaemonSets (2) only.
	if scanned != 4 {
		t.Errorf("scanned = %d, want 4", scanned)
	}
}

// TestSystemAddonNotDoubleReported: with both the pods and system
// classes on, a degraded add-on yields addon.degraded only, not a
// second generic workload.rollout for the same object.
func TestSystemAddonNotDoubleReported(t *testing.T) {
	cmd := testCommand(systemDeployment("coredns", map[string]string{"k8s-app": "kube-dns"}, 2, 0))
	got, _ := runFindings(t, cmd)
	assertFindings(t, got, []finding{{"addon.degraded", "coredns", "critical"}})
}

func TestCSIDaemonSetDegradedByNameHeuristic(t *testing.T) {
	cmd := testCommand(systemDaemonSet("ebs-csi-node", nil, 4, 0))
	got, _ := runFindings(t, cmd, "--only=system")
	assertFindings(t, got, []finding{{"addon.degraded", "ebs-csi-node", "critical"}})
}

func TestPDBClass(t *testing.T) {
	cmd := testCommand(
		pdb("prod", "blocked", 0, 3, 3, 3),
		pdb("prod", "violated", 0, 1, 2, 3),
		pdb("prod", "roomy", 1, 3, 2, 3),
		pdb("prod", "podless", 0, 0, 1, 0),
	)
	got, scanned := runFindings(t, cmd, "--only=pdb")
	assertFindings(t, got, []finding{
		{"pdb.gridlocked", "blocked", "warning"},
		{"pdb.gridlocked", "violated", "critical"},
	})
	if scanned != 4 {
		t.Errorf("scanned = %d, want 4", scanned)
	}
}

func TestQuotaClass(t *testing.T) {
	cmd := testCommand(
		quota("prod", "exhausted", map[string][2]string{"pods": {"10", "10"}}),
		quota("prod", "near", map[string][2]string{"limits.cpu": {"10", "9"}}),
		quota("prod", "roomy", map[string][2]string{"requests.memory": {"10Gi", "2Gi"}}),
		quota("prod", "mixed", map[string][2]string{"pods": {"5", "5"}, "services": {"5", "1"}}),
	)
	got, scanned := runFindings(t, cmd, "--only=quota")
	assertFindings(t, got, []finding{
		{"quota.exhausted", "exhausted", "critical"},
		{"quota.near", "near", "warning"},
		{"quota.exhausted", "mixed", "critical"},
	})
	if scanned != 4 {
		t.Errorf("scanned = %d, want 4", scanned)
	}
}

func TestQuotaWarnFlag(t *testing.T) {
	cmd := testCommand(quota("prod", "half", map[string][2]string{"pods": {"10", "5"}}))
	got, _ := runFindings(t, cmd, "--only=quota", "--quota-warn=50")
	assertFindings(t, got, []finding{{"quota.near", "half", "warning"}})
}

// TestNamespaceScope: --namespace restricts every namespaced class
// and disables the cluster-scoped classes whose subjects the scope
// cannot claim (nodes always; system unless the scope is
// kube-system itself).
func TestNamespaceScope(t *testing.T) {
	objs := []runtime.Object{
		crashloopPod("prod", "crash-1"),
		crashloopPod("dev", "crash-2"),
		notReadyNode("n-bad"),
		systemDeployment("coredns", map[string]string{"k8s-app": "kube-dns"}, 2, 0),
	}

	got, scanned := runFindings(t, testCommand(objs...), "--namespace=prod")
	assertFindings(t, got, []finding{{"pod.crashloop", "crash-1", "critical"}})
	if scanned != 1 {
		t.Errorf("scanned = %d, want 1 (prod pod only)", scanned)
	}

	got, _ = runFindings(t, testCommand(objs...), "--namespace=kube-system")
	assertFindings(t, got, []finding{{"addon.degraded", "coredns", "critical"}})

	got, _ = runFindings(t, testCommand(objs...), "-A")
	assertFindings(t, got, []finding{
		{"pod.crashloop", "crash-1", "critical"},
		{"pod.crashloop", "crash-2", "critical"},
		{"node.notready", "n-bad", "critical"},
		{"addon.degraded", "coredns", "critical"},
	})
}

func TestUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"unknown only class", []string{"--only=bogus"}, "unknown class"},
		{"empty only", []string{"--only="}, "no classes selected"},
		{"workload unsupported", []string{"--workload=Deployment/prod/api"}, "--workload is not supported"},
		{"restarts zero", []string{"--restarts=0"}, "--restarts must be at least 1"},
		{"pending-age zero", []string{"--pending-age=0s"}, "--pending-age must be positive"},
		{"quota-warn out of range", []string{"--quota-warn=101"}, "--quota-warn must be a percentage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, testCommand(), tc.args...)
			// Bad flag values and unsupported scopes are the
			// operator's mistake: §4.2 exit 2, never 1.
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit = %d, want %d", res.Code, emit.ExitUsage)
			}
			if !strings.Contains(res.Stderr, tc.wantErr) {
				t.Errorf("stderr = %q, want it to contain %q", res.Stderr, tc.wantErr)
			}
			if res.Stdout != "" {
				t.Errorf("stdout = %q, want empty on error (no summary line)", res.Stdout)
			}
		})
	}
}

// TestContract runs the §13 round-trip in both formats over a
// cluster that exercises every finding class.
func TestContract(t *testing.T) {
	cmd := testCommand(mixedCluster()...)
	checktest.VerifyContract(t, cmd)
	checktest.VerifyContract(t, cmd, "--only=pods,quota")
	checktest.VerifyContract(t, cmd, "--only=nodes", "--restarts=2")
	checktest.VerifyContract(t, testCommand()) // empty cluster
}

// mixedCluster is the fixture for the golden test: at least one
// finding from every class, plus healthy objects that must stay
// silent.
func mixedCluster() []runtime.Object {
	return []runtime.Object{
		crashloopPod("prod", "api-0"),
		pendingPod("prod", "batch-1", 30*60e9, true),
		healthyPod("prod", "ok-1"),
		rolloutDeployment("prod", "web"),
		failedJob("prod", "etl"),
		healthyNode("node-0"),
		notReadyNode("node-1"),
		preemptNode("node-2", "cloud.google.com/impending-node-termination"),
		pdb("prod", "api-pdb", 0, 3, 3, 3),
		systemDeployment("coredns", map[string]string{"k8s-app": "kube-dns"}, 2, 0),
		quota("prod", "compute-quota", map[string][2]string{"limits.cpu": {"10", "9"}}),
	}
}

// TestGoldenMixedCluster pins the full logfmt stream byte-for-byte:
// severity ordering (critical first, then namespace/name), field
// order, quoting, ages, and the summary line.
func TestGoldenMixedCluster(t *testing.T) {
	res := checktest.Run(t, testCommand(mixedCluster()...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	want := strings.Join([]string{
		`kind=node.notready severity=critical kind_of_object=Node name=node-1 reason=NodeStatusUnknown message="Kubelet stopped posting node status." fingerprint=sha256:be601776f35d8a7fc0ac9193a373943343591d23b484fb0c8d51874d1f5d9016 age=15m0s`,
		`kind=node.preempt severity=critical kind_of_object=Node name=node-2 reason=PreemptionImminent fingerprint=sha256:9050d0355c5a728c7070cdb689c82165d545d5277e425e71ff848037aaf9a3fc taint=cloud.google.com/impending-node-termination pods=0`,
		`kind=addon.degraded severity=critical namespace=kube-system kind_of_object=Deployment name=coredns reason=AddonUnavailable fingerprint=sha256:117aeff7168fb83b202de87343f6ab48a534e464e05be128dbdabdd7bd6ba0b8 addon=dns desired=2 ready=0`,
		`kind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=api-0 reason=CrashLoopBackOff fingerprint=sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b container=app restarts=12 last_state=Error exit_code=1`,
		`kind=pod.pending severity=critical namespace=prod kind_of_object=Pod name=batch-1 reason=Unschedulable message="0/3 nodes are available: 3 Insufficient cpu." fingerprint=sha256:bfe155c0a308d61bd8cf9701619a8859d28b2c9d7c05e458fa9b58dcd2cc7eae age=30m0s`,
		`kind=pdb.gridlocked severity=warning namespace=prod kind_of_object=PodDisruptionBudget name=api-pdb reason=DisruptionsBlocked fingerprint=sha256:53d8a361b10c0011af21b07a5da885d2d76d3b1cc8027e3a8929a2db22429d0e healthy=3 required=3 pods=3`,
		`kind=quota.near severity=warning namespace=prod kind_of_object=ResourceQuota name=compute-quota reason=QuotaNearLimit fingerprint=sha256:12321d595a6773a7b80678b967e8b7968056c361954652cbf25a10b8e7cbdc12 resource=limits.cpu used=9 hard=10 pct=90`,
		`kind=job.failed severity=warning namespace=prod kind_of_object=Job name=etl reason=BackoffLimitExceeded message="Job has reached the specified backoff limit" fingerprint=sha256:45a13f2c2b0345dd176dbd913f958634ef940a94aa8e64dcb3b36a71bddf9d47 failed=4`,
		`kind=workload.rollout severity=warning namespace=prod kind_of_object=Deployment name=web reason=RolloutIncomplete fingerprint=sha256:f954cac01a76847aa7e59722fb0e7f2f85cfe25d1c8dfd610432742cb1b0fc43 desired=3 ready=1 updated=1 available=1`,
		`scanned=11 findings=9 elapsed=100ms`,
	}, "\n") + "\n"
	if res.Stdout != want {
		t.Errorf("golden mismatch\ngot:\n%s\nwant:\n%s", res.Stdout, want)
	}
}
