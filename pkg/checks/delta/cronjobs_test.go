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
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The clock is pinned at 12:00:00, so an @hourly CronJob last
// scheduled N whole hours ago has missed exactly N activations
// (11:00, 12:00, … counting the one at the pinned instant).
func TestCronJobsClass(t *testing.T) {
	suspended := cronJob("prod", "paused", ptr(6*time.Hour))
	suspended.Spec.Suspend = ptr(true)

	neverRan := cronJob("prod", "fresh", nil)
	neverRan.CreationTimestamp = ago(2 * time.Minute)

	cmd := testCommand(
		healthyCronJob("prod", "backup"),          // ran at the top of the hour
		cronJob("prod", "late", ptr(2*time.Hour)), // 2 missed: warning
		cronJob("prod", "wedged", ptr(9*time.Hour)),
		suspended,                     // posture, not an incident: silent here
		neverRan,                      // created inside the grace window: nothing due yet
		cronJob("prod", "stale", nil), // never ran, created 24h ago
	)
	got, scanned := runFindings(t, cmd, "--only=pods")
	assertFindings(t, got, []finding{
		{"cron.missed", "late", "warning"},
		{"cron.missed", "wedged", "critical"},
		{"cron.missed", "stale", "critical"},
	})
	if scanned != 6 {
		t.Errorf("scanned = %d, want 6 (suspended and healthy CronJobs are still assessed)", scanned)
	}
}

// A CronJob whose lastScheduleTime has already reached the activation
// its anchor points at is not missed — the anchor IS that run.
func TestCronJobRunningOnTimeIsSilent(t *testing.T) {
	for _, lastRun := range []time.Duration{0, 30 * time.Minute, 59 * time.Minute} {
		cmd := testCommand(cronJob("prod", "backup", ptr(lastRun)))
		got, _ := runFindings(t, cmd, "--only=pods")
		if len(got) != 0 {
			t.Errorf("last run %s ago: got %v, want no findings", lastRun, got)
		}
	}
}

// The grace window is the whole point of --cron-grace: an activation
// that came due seconds ago is scheduling latency, not a miss.
func TestCronGraceFlag(t *testing.T) {
	// Fires at :58; last run at 10:58, so the 11:58 activation came
	// due 2m before the pinned 12:00 clock.
	cj := cronJob("prod", "backup", ptr(time.Hour+2*time.Minute))
	cj.Spec.Schedule = "58 * * * *"

	got, _ := runFindings(t, testCommand(cj), "--only=pods")
	if len(got) != 0 {
		t.Errorf("inside the default 5m grace: got %v, want no findings", got)
	}

	got, _ = runFindings(t, testCommand(cj), "--only=pods", "--cron-grace=1m")
	assertFindings(t, got, []finding{{"cron.missed", "backup", "warning"}})
}

func TestCronGraceFlagRejectsNegative(t *testing.T) {
	res := checktest.Run(t, testCommand(), "--cron-grace=-1m")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit = %d, want a usage error; stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "--cron-grace") {
		t.Errorf("stderr = %q, want it to name --cron-grace", res.Stderr)
	}
}

// Past the controller's own 100-missed-starts limit the schedule does
// not recover on its own, and the finding has to say so — otherwise
// the reader waits for a run that will never come.
func TestCronJobPastControllerLimitSaysItIsAbandoned(t *testing.T) {
	// Every minute, last run a day ago: 1440 missed activations.
	cj := cronJob("prod", "minutely", ptr(24*time.Hour))
	cj.Spec.Schedule = "* * * * *"

	rec := oneFinding(t, cj, "--only=pods")
	if rec["reason"] != "ScheduleAbandoned" {
		t.Errorf("reason = %q, want ScheduleAbandoned", rec["reason"])
	}
	if !strings.Contains(rec["message"], "more than 100 activations have been missed") {
		t.Errorf("message = %q, want it to say the controller has given up", rec["message"])
	}
	if rec["missed_runs"] != "1440" {
		t.Errorf("missed_runs = %q, want 1440", rec["missed_runs"])
	}
}

// Beyond the walk cap the count is a floor, and the finding must not
// pretend otherwise.
func TestCronJobCappedCountIsReportedAsAFloor(t *testing.T) {
	// A year of minutely activations is far past cronsched's cap.
	cj := cronJob("prod", "ancient", ptr(365*24*time.Hour))
	cj.Spec.Schedule = "* * * * *"

	rec := oneFinding(t, cj, "--only=pods")
	if !strings.HasPrefix(rec["missed_runs"], "≥") {
		t.Errorf("missed_runs = %q, want a ≥-prefixed floor", rec["missed_runs"])
	}
	if rec["reason"] != "ScheduleAbandoned" {
		t.Errorf("reason = %q, want ScheduleAbandoned", rec["reason"])
	}
}

func TestCronJobDetails(t *testing.T) {
	cj := cronJob("prod", "backup", ptr(3*time.Hour))
	cj.Spec.TimeZone = ptr("UTC")
	cj.Status.Active = []corev1.ObjectReference{{Name: "backup-28001"}, {Name: "backup-28002"}}

	rec := oneFinding(t, cj, "--only=pods")
	for key, want := range map[string]string{
		"kind":          "cron.missed",
		"severity":      "critical",
		"reason":        "ScheduleMissed",
		"schedule":      "@hourly",
		"expected":      "2026-01-01T10:00:00Z",
		"missed_runs":   "3",
		"anchor":        "last_schedule",
		"time_zone":     "UTC",
		"last_schedule": "2026-01-01T09:00:00Z",
		"age":           "3h0m0s",
		"active_jobs":   "2",
	} {
		if rec[key] != want {
			t.Errorf("%s = %q, want %q", key, rec[key], want)
		}
	}
}

// A CronJob that has never run is judged from its creation stamp, and
// the finding says which anchor it used — the two mean different
// things to whoever reads it.
func TestCronJobNeverRanAnchorsOnCreation(t *testing.T) {
	rec := oneFinding(t, cronJob("prod", "stale", nil), "--only=pods")
	if rec["anchor"] != "creation" {
		t.Errorf("anchor = %q, want creation", rec["anchor"])
	}
	if rec["last_schedule"] != "never" {
		t.Errorf("last_schedule = %q, want never", rec["last_schedule"])
	}
}

// The API server validates spec.schedule at admission, so an
// unparseable one means our parser and the controller's disagree.
// §2 forbids silently skipping it.
func TestCronJobUnparseableScheduleIsReported(t *testing.T) {
	cj := cronJob("prod", "bogus", ptr(6*time.Hour))
	cj.Spec.Schedule = "not a schedule"

	rec := oneFinding(t, cj, "--only=pods")
	if rec["kind"] != "cron.unparseable" || rec["severity"] != "warning" {
		t.Errorf("got kind %q severity %q, want cron.unparseable warning", rec["kind"], rec["severity"])
	}
	if rec["schedule"] != "not a schedule" {
		t.Errorf("schedule = %q, want the offending spec verbatim", rec["schedule"])
	}
}

// spec.timeZone has to move the window, or a nightly job in another
// zone reads as missed for most of the day.
func TestCronJobTimeZoneShiftsTheWindow(t *testing.T) {
	// 03:00 in Tokyo is 18:00 UTC the day before, so on the UTC
	// timeline this schedule fires at 18:00. Anchored at midnight UTC
	// the next one is 18:00 today — still ahead of the 12:00 clock.
	cj := cronJob("prod", "nightly", ptr(12*time.Hour))
	cj.Spec.Schedule = "0 3 * * *"
	cj.Spec.TimeZone = ptr("Asia/Tokyo")

	got, _ := runFindings(t, testCommand(cj), "--only=pods")
	if len(got) != 0 {
		t.Errorf("got %v, want no findings (the Tokyo activation is still ahead)", got)
	}

	// The same schedule read as UTC came due at 03:00 today and is
	// nine hours overdue.
	utc := cronJob("prod", "nightly-utc", ptr(12*time.Hour))
	utc.Spec.Schedule = "0 3 * * *"
	got, _ = runFindings(t, testCommand(utc), "--only=pods")
	assertFindings(t, got, []finding{{"cron.missed", "nightly-utc", "warning"}})
}

// CronJobs belong to the pods class; --only must be able to exclude
// them like everything else.
func TestCronJobsRespectTheClassFilter(t *testing.T) {
	cmd := testCommand(cronJob("prod", "wedged", ptr(9*time.Hour)), healthyNode("node-0"))
	got, scanned := runFindings(t, cmd, "--only=nodes")
	if len(got) != 0 {
		t.Errorf("got %v, want no findings with --only=nodes", got)
	}
	if scanned != 1 {
		t.Errorf("scanned = %d, want 1 (the node only)", scanned)
	}
}

func TestCronJobNamespaceScope(t *testing.T) {
	cmd := testCommand(
		cronJob("prod", "wedged", ptr(9*time.Hour)),
		cronJob("dev", "also-wedged", ptr(9*time.Hour)),
	)
	got, _ := runFindings(t, cmd, "--only=pods", "--namespace=prod")
	assertFindings(t, got, []finding{{"cron.missed", "wedged", "critical"}})
}

// oneFinding runs the command over a single CronJob and returns the
// sole finding line, failing the test if there is not exactly one.
func oneFinding(t *testing.T, cj *batchv1.CronJob, args ...string) map[string]string {
	t.Helper()
	res := checktest.Run(t, testCommand(cj), args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) != 2 { // one finding + the summary
		t.Fatalf("want exactly one finding line, got:\n%s", res.Stdout)
	}
	return parseLogfmtLine(t, lines[0])
}

// ScanObjects is the seam `bundle` and the watch enrichment stage go
// through; the CronJob claim has to be reachable from there too.
func TestScanObjectsCoversCronJobs(t *testing.T) {
	fs := ScanObjects(testNow, Config{}, Objects{
		CronJobs: []batchv1.CronJob{*cronJob("prod", "wedged", ptr(9*time.Hour))},
	})
	if len(fs) != 1 || fs[0].Kind != "cron.missed" {
		t.Fatalf("ScanObjects = %+v, want one cron.missed", fs)
	}
}

// The zero Config must mean the flag defaults, not "no grace".
func TestConfigZeroCronGraceMeansTheFlagDefault(t *testing.T) {
	// Fires at :58, last run 10:58: the 11:58 activation came due 2m
	// ago — inside the 5m default, outside a 1m grace.
	cjp := cronJob("prod", "backup", ptr(time.Hour+2*time.Minute))
	cjp.Spec.Schedule = "58 * * * *"
	cj := *cjp
	if fs := ScanObjects(testNow, Config{}, Objects{CronJobs: []batchv1.CronJob{cj}}); len(fs) != 0 {
		t.Errorf("ScanObjects with a zero Config = %+v, want the 5m default grace to keep it quiet", fs)
	}
	if fs := ScanObjects(testNow, Config{CronGrace: time.Minute}, Objects{CronJobs: []batchv1.CronJob{cj}}); len(fs) != 1 {
		t.Errorf("ScanObjects with CronGrace=1m = %+v, want one finding", fs)
	}
}

// Every finding needs the §8 fingerprint so the push and pull paths
// dedup against each other rather than double-counting.
func TestCronFindingsCarryAnIdentity(t *testing.T) {
	rec := oneFinding(t, cronJob("prod", "wedged", ptr(9*time.Hour)), "--only=pods")
	if rec["fingerprint"] == "" {
		t.Error("no fingerprint on cron.missed")
	}
	if rec["kind_of_object"] != "CronJob" {
		t.Errorf("kind_of_object = %q, want CronJob", rec["kind_of_object"])
	}
}

// A schedule with no activations at all (February 30th) must not
// produce a finding — there is no window to have missed.
func TestCronJobImpossibleScheduleIsSilent(t *testing.T) {
	cj := cronJob("prod", "never", ptr(9*time.Hour))
	cj.Spec.Schedule = "0 0 30 2 *"

	got, _ := runFindings(t, testCommand(cj), "--only=pods")
	if len(got) != 0 {
		t.Errorf("got %v, want no findings for a schedule that never fires", got)
	}
}

// A zero lastScheduleTime (possible in hand-written or converted
// objects) must fall back to creation rather than reporting every
// activation since year 1.
func TestCronJobZeroLastScheduleFallsBackToCreation(t *testing.T) {
	cj := cronJob("prod", "odd", nil)
	cj.Status.LastScheduleTime = &metav1.Time{}

	rec := oneFinding(t, cj, "--only=pods")
	if rec["anchor"] != "creation" {
		t.Errorf("anchor = %q, want creation", rec["anchor"])
	}
	if rec["missed_runs"] != "24" {
		t.Errorf("missed_runs = %q, want 24 (a day of @hourly), not every hour since year 1", rec["missed_runs"])
	}
}
