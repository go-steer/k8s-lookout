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

package findings

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

var t0 = time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

// fixedClock returns a Deps whose clock never moves, so ack windows
// and first_seen stamps are golden-testable.
func fixedClock(at time.Time) Deps { return Deps{Now: func() time.Time { return at }} }

// newStore creates a sentinel-shaped store file (migrations applied)
// and returns its path.
func newStore(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lookout.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}
	return path
}

// report renders a §4.2 logfmt report: finding lines plus the
// mandatory terminating summary, exactly as a pipe would deliver it.
func report(lines ...string) string {
	var b strings.Builder
	for _, l := range lines {
		b.WriteString(l)
		b.WriteString("\n")
	}
	b.WriteString("scanned=42 findings=")
	b.WriteString(strconv.Itoa(len(lines)))
	b.WriteString(" elapsed=100ms\n")
	return b.String()
}

const (
	crashPod = `kind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=payment-backend-7d9f8-x9k2l reason=CrashLoopBackOff message="back-off restarting failed container" fingerprint=sha256:crash`
	pullPod  = `kind=pod.imagepull severity=warning namespace=staging kind_of_object=Pod name=checkout-5f4b8-q4m7p reason=ImagePullBackOff message="pull access denied" fingerprint=sha256:pull`
	// The same crashing workload after a reschedule: a different
	// generated suffix, the same subject.
	crashPodRescheduled = `kind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=payment-backend-7d9f8-b2ndf reason=CrashLoopBackOff message="back-off restarting failed container" fingerprint=sha256:crash`
	// The pull failure, escalated.
	pullPodCritical = `kind=pod.imagepull severity=critical namespace=staging kind_of_object=Pod name=checkout-5f4b8-q4m7p reason=ImagePullBackOff message="pull access denied" fingerprint=sha256:pull`

	crashSubject = "prod-east/prod/Pod/payment-backend/CrashLoopBackOff"
	pullSubject  = "prod-east/staging/Pod/checkout/ImagePullBackOff"
)

func diffArgs(path string, extra ...string) []string {
	return append([]string{"--report=-", "--store=" + path, "--cluster=prod-east"}, extra...)
}

// TestDiffFirstRunIsAllNew: with no stored state every subject is
// new — the honest answer, and the one that makes a fresh store or an
// upgraded one behave predictably.
func TestDiffFirstRunIsAllNew(t *testing.T) {
	path := newStore(t)
	cmd := DiffCommand(fixedClock(t0))

	res := checktest.RunStdin(t, cmd, report(crashPod, pullPod), diffArgs(path)...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, want := range []string{
		"transition=new",
		"subject_key=" + crashSubject,
		"subject_key=" + pullSubject,
		"first_seen=2026-08-13T12:00:00Z",
		"scanned=2 findings=2",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("output missing %q:\n%s", want, res.Stdout)
		}
	}
	if strings.Contains(res.Stdout, "prev_severity=") {
		t.Errorf("a `new` transition carries a prev_severity:\n%s", res.Stdout)
	}
}

// TestDiffSecondRunClassifies is the whole point of the surface: a
// rescheduled pod stays ongoing (name normalization), a severity bump
// escalates, and a finding that stopped appearing resolves.
func TestDiffSecondRunClassifies(t *testing.T) {
	path := newStore(t)
	cmd := DiffCommand(fixedClock(t0))

	if res := checktest.RunStdin(t, cmd, report(crashPod, pullPod), diffArgs(path)...); res.Code != emit.ExitData {
		t.Fatalf("first run exit = %d, stderr: %s", res.Code, res.Stderr)
	}

	// Second run, 15 minutes later: the crasher was rescheduled under
	// a new generated suffix, the pull failure got worse, and a third
	// subject appeared.
	later := DiffCommand(fixedClock(t0.Add(15 * time.Minute)))
	nodeFinding := `kind=node.notready severity=warning kind_of_object=Node name=gke-pool-a-vmxq reason=NodeNotReady fingerprint=sha256:node`
	res := checktest.RunStdin(t, later, report(crashPodRescheduled, pullPodCritical, nodeFinding), diffArgs(path)...)
	if res.Code != emit.ExitData {
		t.Fatalf("second run exit = %d, stderr: %s", res.Code, res.Stderr)
	}

	got := transitionsBySubject(t, res.Stdout)
	want := map[string]string{
		crashSubject: "ongoing",
		pullSubject:  "escalated",
		"prod-east//Node/gke-pool-a-vmxq/NodeNotReady": "new",
	}
	for subject, class := range want {
		if got[subject] != class {
			t.Errorf("subject %s classified %q, want %q\n%s", subject, got[subject], class, res.Stdout)
		}
	}

	// The rescheduled pod kept the ORIGINAL first_seen: "broken since"
	// must not reset every time Kubernetes replaces the pod.
	if !strings.Contains(res.Stdout, "first_seen=2026-08-13T12:00:00Z") {
		t.Errorf("reschedule reset first_seen:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "prev_severity=warning") {
		t.Errorf("escalation did not report the previous severity:\n%s", res.Stdout)
	}

	// Third run: everything recovered.
	last := DiffCommand(fixedClock(t0.Add(30 * time.Minute)))
	res = checktest.RunStdin(t, last, report(), diffArgs(path)...)
	if res.Code != emit.ExitData {
		t.Fatalf("third run exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	got = transitionsBySubject(t, res.Stdout)
	if len(got) != 3 {
		t.Fatalf("empty report produced %d transitions, want 3 resolved:\n%s", len(got), res.Stdout)
	}
	for subject, class := range got {
		if class != "resolved" {
			t.Errorf("subject %s classified %q after disappearing, want resolved", subject, class)
		}
	}

	// And a fourth run over the same empty report reports NOTHING:
	// resolved rows are dropped, so recovery is announced exactly once.
	res = checktest.RunStdin(t, last, report(), diffArgs(path)...)
	if !strings.Contains(res.Stdout, "findings=0") {
		t.Errorf("recovery was re-announced on the next run:\n%s", res.Stdout)
	}
}

// TestDiffDryRunDoesNotAdvanceState: --dry-run is repeatable. Without
// it, running the same report twice is the difference between "two
// new" and "two ongoing", which is exactly the trap an operator
// previewing a report would fall into.
func TestDiffDryRunDoesNotAdvanceState(t *testing.T) {
	path := newStore(t)
	cmd := DiffCommand(fixedClock(t0))
	args := diffArgs(path, "--dry-run")

	first := checktest.RunStdin(t, cmd, report(crashPod), args...)
	second := checktest.RunStdin(t, cmd, report(crashPod), args...)
	if first.Stdout != second.Stdout {
		t.Errorf("--dry-run is not repeatable:\nfirst:\n%s\nsecond:\n%s", first.Stdout, second.Stdout)
	}
	if !strings.Contains(first.Stdout, "transition=new") {
		t.Errorf("--dry-run did not classify:\n%s", first.Stdout)
	}

	s, err := store.OpenRead(path)
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	rows, err := s.FindingStates(context.Background(), "prod-east")
	_ = s.Close()
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("--dry-run wrote %d state rows, want 0", len(rows))
	}
}

// TestDiffTransitionsFilter: the digest view. scanned still counts
// every subject considered, so "3 of 42 changed" is readable off the
// summary line alone.
func TestDiffTransitionsFilter(t *testing.T) {
	path := newStore(t)
	if res := checktest.RunStdin(t, DiffCommand(fixedClock(t0)), report(crashPod, pullPod), diffArgs(path)...); res.Code != emit.ExitData {
		t.Fatalf("seed run exit = %d, stderr: %s", res.Code, res.Stderr)
	}

	later := DiffCommand(fixedClock(t0.Add(15 * time.Minute)))
	res := checktest.RunStdin(t, later, report(crashPod, pullPodCritical),
		diffArgs(path, "--transitions=new,escalated,resolved")...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "scanned=2 findings=1") {
		t.Errorf("want scanned=2 findings=1 (two subjects considered, one emitted):\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "transition=ongoing") {
		t.Errorf("--transitions did not filter ongoing out:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "transition=escalated") {
		t.Errorf("--transitions dropped the escalation:\n%s", res.Stdout)
	}
}

// TestDiffAckSuppresses drives the ack path end to end through both
// commands: ack, see suppressed, let it expire, see the deferred
// escalation fire.
func TestDiffAckSuppresses(t *testing.T) {
	path := newStore(t)
	if res := checktest.RunStdin(t, DiffCommand(fixedClock(t0)), report(pullPod), diffArgs(path)...); res.Code != emit.ExitData {
		t.Fatalf("seed run exit = %d, stderr: %s", res.Code, res.Stderr)
	}

	ack := checktest.Run(t, AckCommand(fixedClock(t0)),
		pullSubject, "--store="+path, "--for=4h", "--by=gari")
	if ack.Code != emit.ExitData {
		t.Fatalf("ack exit = %d, stderr: %s", ack.Code, ack.Stderr)
	}
	for _, want := range []string{
		"kind=findings.ack",
		"subject_key=" + pullSubject,
		"ack_until=2026-08-13T16:00:00Z",
		"ack_by=gari",
	} {
		if !strings.Contains(ack.Stdout, want) {
			t.Errorf("ack output missing %q:\n%s", want, ack.Stdout)
		}
	}

	// Inside the window the finding is suppressed even though it got
	// worse: the ack outranks the escalation.
	inside := checktest.RunStdin(t, DiffCommand(fixedClock(t0.Add(time.Hour))),
		report(pullPodCritical), diffArgs(path)...)
	if got := transitionsBySubject(t, inside.Stdout)[pullSubject]; got != "suppressed" {
		t.Errorf("inside the ack window: %q, want suppressed\n%s", got, inside.Stdout)
	}
	if !strings.Contains(inside.Stdout, "ack_by=gari") {
		t.Errorf("suppressed record does not say who took it:\n%s", inside.Stdout)
	}

	// Past the expiry the deferred escalation fires: the ack paused
	// the classification, it did not swallow it.
	after := checktest.RunStdin(t, DiffCommand(fixedClock(t0.Add(5*time.Hour))),
		report(pullPodCritical), diffArgs(path)...)
	if got := transitionsBySubject(t, after.Stdout)[pullSubject]; got != "escalated" {
		t.Errorf("after the ack expired: %q, want escalated\n%s", got, after.Stdout)
	}
}

// TestAckClear ends a window early and puts the subject back in the
// normal classification path.
func TestAckClear(t *testing.T) {
	path := newStore(t)
	if res := checktest.RunStdin(t, DiffCommand(fixedClock(t0)), report(pullPod), diffArgs(path)...); res.Code != emit.ExitData {
		t.Fatalf("seed run exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if res := checktest.Run(t, AckCommand(fixedClock(t0)), pullSubject, "--store="+path, "--for=4h", "--by=gari"); res.Code != emit.ExitData {
		t.Fatalf("ack exit = %d, stderr: %s", res.Code, res.Stderr)
	}

	cleared := checktest.Run(t, AckCommand(fixedClock(t0)), pullSubject, "--store="+path, "--clear")
	if cleared.Code != emit.ExitData {
		t.Fatalf("clear exit = %d, stderr: %s", cleared.Code, cleared.Stderr)
	}
	if strings.Contains(cleared.Stdout, "ack_until=") || strings.Contains(cleared.Stdout, "ack_by=") {
		t.Errorf("--clear echoed a live ack:\n%s", cleared.Stdout)
	}

	res := checktest.RunStdin(t, DiffCommand(fixedClock(t0.Add(time.Hour))), report(pullPod), diffArgs(path)...)
	if got := transitionsBySubject(t, res.Stdout)[pullSubject]; got != "ongoing" {
		t.Errorf("after --clear: %q, want ongoing\n%s", got, res.Stdout)
	}
}

// TestDiffReadsAFile: --report accepts a path, not just stdin, so a
// captured report can be re-classified after the fact.
func TestDiffReadsAFile(t *testing.T) {
	path := newStore(t)
	reportPath := filepath.Join(t.TempDir(), "scan.logfmt")
	if err := os.WriteFile(reportPath, []byte(report(crashPod)), 0o600); err != nil {
		t.Fatalf("write report: %v", err)
	}
	res := checktest.Run(t, DiffCommand(fixedClock(t0)),
		"--report="+reportPath, "--store="+path, "--cluster=prod-east")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "transition=new") {
		t.Errorf("file report did not classify:\n%s", res.Stdout)
	}
}

// TestDiffRejectsAnUnparseableReport: a report the differ cannot
// fully read must FAIL, not partially classify. Dropping records
// silently would report the dropped subjects as `resolved` — the
// exact false recovery this surface exists to prevent.
func TestDiffRejectsAnUnparseableReport(t *testing.T) {
	path := newStore(t)
	res := checktest.RunStdin(t, DiffCommand(fixedClock(t0)),
		crashPod+"\nthis is not a record\nscanned=1 findings=1 elapsed=1ms\n", diffArgs(path)...)
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit = %d, want %d (stderr: %s)", res.Code, emit.ExitRuntime, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "line 2") {
		t.Errorf("error does not name the offending line: %q", res.Stderr)
	}
	if strings.Contains(res.Stdout, "findings=") {
		t.Errorf("a failed diff wrote a summary line: %q", res.Stdout)
	}

	s, err := store.OpenRead(path)
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	rows, err := s.FindingStates(context.Background(), "prod-east")
	_ = s.Close()
	if err != nil {
		t.Fatalf("FindingStates: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("a failed diff advanced state to %d rows, want 0", len(rows))
	}
}

// TestDiffAcceptsJSONReports: the upstream command's --format must
// not matter, since logfmt is the default and requiring JSON would
// make the obvious pipe silently parse nothing.
func TestDiffAcceptsJSONReports(t *testing.T) {
	path := newStore(t)
	jsonReport := `{"kind":"pod.crashloop","severity":"critical","namespace":"prod","kind_of_object":"Pod","name":"payment-backend-7d9f8-x9k2l","reason":"CrashLoopBackOff","fingerprint":"sha256:crash"}
{"scanned":42,"findings":1,"elapsed":"100ms"}
`
	res := checktest.RunStdin(t, DiffCommand(fixedClock(t0)), jsonReport, diffArgs(path)...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "subject_key="+crashSubject) {
		t.Errorf("JSON report did not classify:\n%s", res.Stdout)
	}
}

// TestUsageErrors pins the exit-2 misuse surface.
func TestUsageErrors(t *testing.T) {
	path := newStore(t)
	diffCases := []struct {
		name string
		args []string
		want string
	}{
		{"no store", []string{"--report=-"}, "§9.1"},
		{"empty report flag", []string{"--report=", "--store=" + path}, "--report is required"},
		{"bad transition", []string{"--report=-", "--store=" + path, "--transitions=flapping"},
			"new|ongoing|escalated|resolved|suppressed"},
	}
	for _, tc := range diffCases {
		res := checktest.RunStdin(t, DiffCommand(fixedClock(t0)), report(), tc.args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("diff %s: exit = %d, want %d (stderr: %s)", tc.name, res.Code, emit.ExitUsage, res.Stderr)
			continue
		}
		if !strings.Contains(res.Stderr, tc.want) {
			t.Errorf("diff %s: stderr %q missing %q", tc.name, res.Stderr, tc.want)
		}
	}

	ackCases := []struct {
		name string
		args []string
		want string
	}{
		{"no subject", []string{"--store=" + path}, "one subject key"},
		{"no store", []string{pullSubject}, "§9.1"},
		{"zero window", []string{pullSubject, "--store=" + path, "--for=0s"}, "--for must be positive"},
	}
	for _, tc := range ackCases {
		res := checktest.Run(t, AckCommand(fixedClock(t0)), tc.args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("ack %s: exit = %d, want %d (stderr: %s)", tc.name, res.Code, emit.ExitUsage, res.Stderr)
			continue
		}
		if !strings.Contains(res.Stderr, tc.want) {
			t.Errorf("ack %s: stderr %q missing %q", tc.name, res.Stderr, tc.want)
		}
	}
}

// TestAckUnknownSubject: acking something that is not open is a
// runtime error naming the key — a typo or a stale digest, either way
// worth saying out loud rather than accepting silently.
func TestAckUnknownSubject(t *testing.T) {
	path := newStore(t)
	res := checktest.Run(t, AckCommand(fixedClock(t0)), "prod-east/prod/Pod/ghost/Crash", "--store="+path)
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit = %d, want %d (stderr: %s)", res.Code, emit.ExitRuntime, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "prod-east/prod/Pod/ghost/Crash") {
		t.Errorf("error does not name the key: %q", res.Stderr)
	}
}

// TestContract runs the §13 output-contract scaffold over both
// commands in both formats: every emitted key is declared.
func TestContract(t *testing.T) {
	path := newStore(t)
	checktest.VerifyContractStdin(t, DiffCommand(fixedClock(t0)), report(crashPod, pullPod),
		"--report=-", "--store="+path, "--cluster=prod-east", "--dry-run")

	// Seed a subject, then verify the ack surface against it. --clear
	// keeps the run repeatable across the two formats.
	if res := checktest.RunStdin(t, DiffCommand(fixedClock(t0)), report(pullPod), diffArgs(path)...); res.Code != emit.ExitData {
		t.Fatalf("seed run exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.VerifyContract(t, AckCommand(fixedClock(t0)), pullSubject, "--store="+path, "--for=4h", "--by=gari")
	checktest.VerifyContract(t, AckCommand(fixedClock(t0)), pullSubject, "--store="+path, "--clear")
}

// transitionsBySubject indexes a logfmt transition stream by subject
// key, ignoring the summary line.
func transitionsBySubject(t *testing.T, stdout string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if line == "" || strings.HasPrefix(line, "scanned=") {
			continue
		}
		var subject, transition string
		for _, field := range strings.Fields(line) {
			switch {
			case strings.HasPrefix(field, "subject_key="):
				subject = strings.TrimPrefix(field, "subject_key=")
			case strings.HasPrefix(field, "transition="):
				transition = strings.TrimPrefix(field, "transition=")
			}
		}
		if subject == "" {
			t.Fatalf("transition line has no subject_key: %q", line)
		}
		out[subject] = transition
	}
	return out
}
