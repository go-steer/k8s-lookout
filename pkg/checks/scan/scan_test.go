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

package scan_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/scan"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// testClock is the fixed instant the drill-down's TLS math sees.
var testClock = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// stage builds a fake registered command for the test registry. The
// names must be real stage-1 names — scan resolves what it runs out of
// the registry, so a fake standing in for `triage delta` is registered
// under exactly that name.
func stage(name string, run emit.CheckFunc) checks.Command {
	return checks.Command{
		Name:    name,
		MCPName: "k8s_" + strings.ReplaceAll(name, " ", "_"),
		Summary: "test double for " + name,
		Run:     run,
	}
}

// emits returns a CheckFunc that emits the given findings and reports
// one object scanned per finding plus one.
func emits(findings ...emit.Finding) emit.CheckFunc {
	return func(_ context.Context, inv emit.Invocation) (int, error) {
		for _, f := range findings {
			if err := inv.Out.Emit(f); err != nil {
				return 0, err
			}
		}
		return len(findings) + 1, nil
	}
}

func crashloop(namespace, name string) emit.Finding {
	return emit.Finding{
		Kind: "pod.crashloop", Severity: emit.SeverityCritical,
		Namespace: namespace, KindOfObject: "Pod", Name: name,
		Reason: "CrashLoopBackOff", Message: "restarting",
	}
}

// newScan builds the command under test over a registry holding only
// the given stages, with the drill-down pointed at objs.
func newScan(t *testing.T, objs []runtime.Object, stages ...checks.Command) checks.Command {
	t.Helper()
	reg := checks.NewRegistry()
	for _, s := range stages {
		reg.Register(s)
	}
	client := fake.NewClientset(objs...)
	return scan.New(scan.Deps{
		Registry: reg,
		Client:   func(context.Context) (kubernetes.Interface, error) { return client, nil },
		Now:      func() time.Time { return testClock },
	})
}

// TestScan_Stage1_StampsTheCheckAndCountsIt: the whole premise of
// composing registered commands is that a scan finding is
// indistinguishable from the finding the check emits on its own,
// except that it says which check that was.
func TestScan_Stage1_StampsTheCheckAndCountsIt(t *testing.T) {
	c := newScan(t, nil,
		stage("triage delta", emits(crashloop("prod", "api-1"))),
		stage("state webhooks", emits()),
	)
	res := checktest.Run(t, c, "--max-drilldown=0")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "kind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=api-1 " +
		"reason=CrashLoopBackOff message=restarting check=\"triage delta\"\n" +
		"scanned=3 findings=1 elapsed=100ms checks=2 skipped=audit,cloud,perf drilldown=0\n"
	if res.Stdout != want {
		t.Errorf("stdout:\n%s\nwant:\n%s", res.Stdout, want)
	}
	if err := checktest.Verify(c, res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("output contract violated: %v", err)
	}
}

// TestScan_HealthyClusterIsSilent: §4.2 zero nominal state. A scan
// that found nothing still says what it ran, because "quiet" and
// "did not look" must not read the same.
func TestScan_HealthyClusterIsSilent(t *testing.T) {
	c := newScan(t, nil,
		stage("triage delta", emits()),
		stage("state webhooks", emits()),
		stage("stab drift", emits()),
	)
	res := checktest.Run(t, c)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "scanned=3 findings=0 elapsed=100ms checks=3 skipped=audit,cloud,perf drilldown=0\n"
	if res.Stdout != want {
		t.Errorf("stdout:\n%s\nwant:\n%s", res.Stdout, want)
	}
}

// TestScan_UsageErrorFromAStageIsCoverage: a check that declines the
// invocation has not failed — it has told us the scan is narrower than
// it looks, which is an info-severity coverage statement, not a fault.
func TestScan_UsageErrorFromAStageIsCoverage(t *testing.T) {
	declines := func(context.Context, emit.Invocation) (int, error) {
		return 0, emit.UsageErrorf("nothing to compare against: pass --exemptions")
	}
	c := newScan(t, nil,
		stage("triage delta", emits()),
		stage("state webhooks", declines),
	)
	res := checktest.Run(t, c)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "kind=scan.check_skipped severity=info reason=NotApplicable " +
		"message=\"nothing to compare against: pass --exemptions\" check=\"state webhooks\"\n" +
		"scanned=1 findings=1 elapsed=100ms checks=2 skipped=audit,cloud,perf drilldown=0\n"
	if res.Stdout != want {
		t.Errorf("stdout:\n%s\nwant:\n%s", res.Stdout, want)
	}
}

// TestScan_FailedStageDoesNotVoidTheScan: one broken check must not
// discard the other twelve's findings. Exiting 1 would emit no summary
// line at all, which §4.2 defines as a void result.
func TestScan_FailedStageDoesNotVoidTheScan(t *testing.T) {
	boom := func(context.Context, emit.Invocation) (int, error) {
		return 0, errors.New("listing webhooks: connection refused")
	}
	c := newScan(t, nil,
		stage("triage delta", emits(crashloop("prod", "api-1"))),
		stage("state webhooks", boom),
	)
	res := checktest.Run(t, c, "--max-drilldown=0")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, want := range []string{
		"kind=pod.crashloop",
		"kind=scan.check_failed severity=warning reason=CheckFailed " +
			"message=\"listing webhooks: connection refused\" check=\"state webhooks\"",
		"findings=2 elapsed=100ms checks=2",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
}

// TestScan_TimeoutIsReportedNotHidden: a partial scan that looked
// complete would be the worst possible failure mode — an agent would
// read findings=0 as "healthy" (§11, no coverage lies).
func TestScan_TimeoutIsReportedNotHidden(t *testing.T) {
	c := newScan(t, nil,
		stage("triage delta", emits()),
		stage("state webhooks", emits()),
	)
	res := checktest.Run(t, c, "--timeout=1ns")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "kind=scan.incomplete severity=warning reason=Timeout " +
		"message=\"the --timeout expired before every check ran; this scan is a partial view, " +
		"raise --timeout or narrow with --namespace\" not_run=\"triage delta,state webhooks\"\n" +
		"scanned=0 findings=1 elapsed=100ms checks=0 skipped=audit,cloud,perf drilldown=0\n"
	if res.Stdout != want {
		t.Errorf("stdout:\n%s\nwant:\n%s", res.Stdout, want)
	}
}

// TestScan_UnavailableStagesRollUpToTheSummary: `state wi` on a
// cluster with no cloud provider and `state gateway` where the Gateway
// API is not installed both emit one info finding and set the same
// `unavailable` note, so the last one would win. Scan overwrites it
// with the full list.
func TestScan_UnavailableStagesRollUpToTheSummary(t *testing.T) {
	unavailable := func(kind string) emit.CheckFunc {
		return func(_ context.Context, inv emit.Invocation) (int, error) {
			if err := inv.Out.Note("unavailable", "not served"); err != nil {
				return 0, err
			}
			return 0, inv.Out.Emit(emit.Finding{
				Kind: kind, Severity: emit.SeverityInfo,
				Reason: "NotServed", Message: "the API this check reads is not served by this cluster",
			})
		}
	}
	c := newScan(t, nil,
		stage("state gateway", unavailable("crd.unavailable")),
		stage("state wi", unavailable("cloud.unavailable")),
	)
	res := checktest.Run(t, c)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if want := `unavailable="state gateway,state wi"`; !strings.Contains(res.Stdout, want) {
		t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
	}
}

// TestScan_DrilldownRollsUpToTheController: twenty crashlooping pods
// of one Deployment are one dependency problem, not twenty. Without
// the roll-up the drill-down would run `state edges` once per pod and
// emit the same missing-ConfigMap finding twenty times.
func TestScan_DrilldownRollsUpToTheController(t *testing.T) {
	objs := ownedPods("prod", "api", 2)
	c := newScan(t, objs,
		stage("triage delta", emits(
			crashloop("prod", "api-rs-0"),
			crashloop("prod", "api-rs-1"),
		)),
	)
	res := checktest.Run(t, c)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if want := "drilldown=1"; !strings.Contains(res.Stdout, want) {
		t.Errorf("stdout missing %q — the two pods should roll up to their Deployment:\n%s", want, res.Stdout)
	}
}

// TestScan_DrilldownCapIsReported: dropping work silently would be a
// coverage lie; --max-drilldown says how much it dropped.
func TestScan_DrilldownCapIsReported(t *testing.T) {
	objs := append(ownedPods("prod", "api", 1), ownedPods("prod", "web", 1)...)
	c := newScan(t, objs,
		stage("triage delta", emits(
			crashloop("prod", "api-rs-0"),
			crashloop("prod", "web-rs-0"),
		)),
	)
	res := checktest.Run(t, c, "--max-drilldown=1")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, want := range []string{"drilldown=1", "truncated=1"} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
}

// TestScan_InfoFindingsDoNotTriggerADrilldown: the drill-down is for
// things that are wrong, not for everything a scan mentions.
func TestScan_InfoFindingsDoNotTriggerADrilldown(t *testing.T) {
	objs := ownedPods("prod", "api", 1)
	c := newScan(t, objs, stage("triage delta", emits(emit.Finding{
		Kind: "pod.info", Severity: emit.SeverityInfo,
		Namespace: "prod", KindOfObject: "Pod", Name: "api-rs-0",
		Reason: "Noted", Message: "nothing to see",
	})))
	res := checktest.Run(t, c)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if want := "drilldown=0"; !strings.Contains(res.Stdout, want) {
		t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
	}
}

// TestScan_IncludeAddsOptInGroups: the audit group is off by default
// because posture findings never self-clear, but it has to be one flag
// away — and the summary's skipped= note is how a reader learns that.
func TestScan_IncludeAddsOptInGroups(t *testing.T) {
	c := newScan(t, nil,
		stage("triage delta", emits()),
		stage("audit netpol", emits(emit.Finding{
			Kind: "audit.netpol_absent", Severity: emit.SeverityWarning,
			Namespace: "prod", KindOfObject: "Namespace", Name: "prod",
			Reason: "NoNetworkPolicy", Message: "no NetworkPolicy selects any pod in this namespace",
		})),
	)

	off := checktest.Run(t, c, "--max-drilldown=0")
	if strings.Contains(off.Stdout, "audit.netpol_absent") {
		t.Errorf("the audit group ran without --include:\n%s", off.Stdout)
	}
	if want := "checks=1 skipped=audit,cloud,perf"; !strings.Contains(off.Stdout, want) {
		t.Errorf("stdout missing %q:\n%s", want, off.Stdout)
	}

	on := checktest.Run(t, c, "--max-drilldown=0", "--include=audit")
	if !strings.Contains(on.Stdout, `kind=audit.netpol_absent`) {
		t.Errorf("--include=audit did not run the audit group:\n%s", on.Stdout)
	}
	if want := "checks=2 skipped=cloud,perf"; !strings.Contains(on.Stdout, want) {
		t.Errorf("stdout missing %q:\n%s", want, on.Stdout)
	}

	all := checktest.Run(t, c, "--max-drilldown=0", "--include=all,-perf")
	if want := "checks=2 skipped=perf"; !strings.Contains(all.Stdout, want) {
		t.Errorf("stdout missing %q:\n%s", want, all.Stdout)
	}
}

// TestScan_UsageErrors: exit 2, not 1 — a caller that passed a bad
// flag needs to know it was their argv, not the cluster.
func TestScan_UsageErrors(t *testing.T) {
	c := newScan(t, nil, stage("triage delta", emits()))
	for _, tc := range []struct{ name, arg, want string }{
		{"unknown group", "--include=bogus", `unknown group "bogus"`},
		{"workload target", "--workload=Deployment/prod/api", "--workload does not apply"},
		{"negative cap", "--max-drilldown=-1", "must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, c, tc.arg)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d; stderr: %s", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr %q does not mention %q", res.Stderr, tc.want)
			}
		})
	}
}

// TestScan_ContractInBothFormats runs the full §13 round-trip: every
// key scan emits, including the stamped check= and its own summary
// notes, must be declared in its glossary.
func TestScan_ContractInBothFormats(t *testing.T) {
	c := newScan(t, ownedPods("prod", "api", 1),
		stage("triage delta", emits(crashloop("prod", "api-rs-0"))),
		stage("state webhooks", emits()),
	)
	checktest.VerifyContract(t, c)
}

// --- fixtures ---

// ownedPods builds a Deployment → ReplicaSet → n Pods chain, the shape
// the drill-down roll-up has to walk.
func ownedPods(ns, name string, n int) []runtime.Object {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, UID: apitypes.UID(ns + "/" + name),
	}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name + "-rs", UID: apitypes.UID(ns + "/" + name + "-rs"),
		OwnerReferences: []metav1.OwnerReference{owns("Deployment", name, ns+"/"+name)},
	}}
	out := []runtime.Object{dep, rs}
	for i := range n {
		pod := name + "-rs-" + strconv.Itoa(i)
		out = append(out, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: pod, UID: apitypes.UID(ns + "/" + pod),
			OwnerReferences: []metav1.OwnerReference{owns("ReplicaSet", name+"-rs", ns+"/"+name+"-rs")},
		}})
	}
	return out
}

func owns(kind, name, uid string) metav1.OwnerReference {
	ctrl := true
	return metav1.OwnerReference{
		APIVersion: "apps/v1", Kind: kind, Name: name,
		UID: apitypes.UID(uid), Controller: &ctrl,
	}
}
