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

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func TestParseAlertExcludes_ValidationMatrix(t *testing.T) {
	t.Parallel()

	validCases := []struct {
		name      string
		yaml      string
		wantCount int
	}{
		{
			name:      "empty explicit list",
			yaml:      "excludes: []\n",
			wantCount: 0,
		},
		{
			name: "full rule with trailing prefix and multi-cluster fields",
			yaml: `
excludes:
  - kind: Pod
    namespace: staging-canary
    reason: CrashLoopBackOff
    name: loadgen-*
    cluster: staging-us
    project: my-proj
    location: us-central1-a
`,
			wantCount: 1,
		},
		{
			name: "kind plus single narrowing field each",
			yaml: `
excludes:
  - kind: pod
    namespace: noisy-ns
  - kind: Deployment
    reason: ProgressDeadlineExceeded
  - kind: Pod
    name: exact-pod-0
`,
			wantCount: 3,
		},
	}
	for _, tc := range validCases {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			t.Parallel()
			rules, err := parseAlertExcludes([]byte(tc.yaml))
			if err != nil {
				t.Fatalf("parseAlertExcludes() unexpected error: %v", err)
			}
			if len(rules) != tc.wantCount {
				t.Fatalf("len(rules) = %d, want %d", len(rules), tc.wantCount)
			}
		})
	}

	invalidCases := []struct {
		name string
		yaml string
	}{
		{name: "empty file", yaml: ""},
		{name: "comments only", yaml: "# just a comment\n"},
		{name: "empty mapping missing excludes", yaml: "{}\n"},
		{name: "null excludes", yaml: "excludes: null\n"},
		{name: "bare list without wrapper", yaml: "- kind: Pod\n  namespace: foo\n"},
		{name: "unknown top-level field", yaml: "excludes: []\nextra: true\n"},
		{name: "unknown rule field labels", yaml: "excludes:\n  - kind: Pod\n    namespace: foo\n    labels:\n      app: web\n"},
		{name: "unknown rule field selector", yaml: "excludes:\n  - kind: Pod\n    namespace: foo\n    selector: app=web\n"},
		{name: "unknown rule field typo namespaces", yaml: "excludes:\n  - kind: Pod\n    namespaces: foo\n"},
		{name: "missing kind", yaml: "excludes:\n  - namespace: foo\n"},
		{name: "wildcard kind bare star", yaml: "excludes:\n  - kind: \"*\"\n    namespace: foo\n"},
		{name: "wildcard kind prefix", yaml: "excludes:\n  - kind: Pod*\n    namespace: foo\n"},
		{name: "kind-only rule rejected", yaml: "excludes:\n  - kind: Pod\n"},
		{name: "kind plus cluster only rejected", yaml: "excludes:\n  - kind: Pod\n    cluster: staging\n"},
		{name: "bare star name rejected", yaml: "excludes:\n  - kind: Pod\n    name: \"*\"\n"},
		{name: "mid-string star name rejected", yaml: "excludes:\n  - kind: Pod\n    name: \"load*gen\"\n"},
		{name: "multiple star name rejected", yaml: "excludes:\n  - kind: Pod\n    name: \"load*gen-*\"\n"},
		{name: "wildcard namespace rejected", yaml: "excludes:\n  - kind: Pod\n    namespace: \"staging-*\"\n"},
		{name: "wildcard reason rejected", yaml: "excludes:\n  - kind: Pod\n    reason: \"Crash*\"\n"},
		{name: "wildcard cluster rejected", yaml: "excludes:\n  - kind: Pod\n    namespace: foo\n    cluster: \"prod-*\"\n"},
		{name: "autoscaler verdict TriggeredScaleUp rejected", yaml: "excludes:\n  - kind: Pod\n    reason: TriggeredScaleUp\n"},
		{name: "autoscaler verdict NotTriggerScaleUp rejected", yaml: "excludes:\n  - kind: Pod\n    reason: NotTriggerScaleUp\n"},
		{name: "autoscaler verdict FailedScaleUp rejected", yaml: "excludes:\n  - kind: Pod\n    reason: FailedScaleUp\n"},
		{name: "autoscaler verdict lowercase rejected", yaml: "excludes:\n  - kind: Pod\n    reason: triggeredscaleup\n"},
		{name: "malformed YAML syntax", yaml: "excludes:\n  - [unclosed\n"},
	}
	for _, tc := range invalidCases {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseAlertExcludes([]byte(tc.yaml)); err == nil {
				t.Fatalf("parseAlertExcludes(%q) = nil, want error", tc.name)
			}
		})
	}
}

func TestAlertExcludeRule_Matching(t *testing.T) {
	t.Parallel()

	rules, err := parseAlertExcludes([]byte(`
excludes:
  - kind: pod
    namespace: staging-canary
    reason: crashloopbackoff
    name: loadgen-*
  - kind: Pod
    namespace: prod
    reason: BackOff
    name: exact-worker
    cluster: prod-us
    project: proj-1
    location: us-central1-a
  - kind: Pod
    namespace: regional-ns
    reason: ImagePullBackOff
    location: europe-west1
`))
	if err != nil {
		t.Fatalf("parseAlertExcludes: %v", err)
	}

	matchAny := func(sig engine.Signal) bool {
		for _, r := range rules {
			if r.Match(sig) {
				return true
			}
		}
		return false
	}

	// Rule 1: case-insensitive kind, canonical CrashLoopBackOff matches wire BackOff
	// with restarting-failed-container message, trailing "*" prefix matches.
	if !matchAny(engine.Signal{
		Kind: engine.KindK8sEvent,
		TriageEvent: engine.TriageEvent{
			KindOfObject: "Pod",
			Namespace:    "staging-canary",
			Name:         "loadgen-7c9d8f-x1",
			Key:          engine.EventKey{UID: "u1", Reason: "BackOff"},
			Message:      "Back-off restarting failed container worker in pod loadgen-7c9d8f-x1",
		},
	}) {
		t.Error("expected rule 1 to match wire BackOff with canonical CrashLoopBackOff")
	}

	// Rule 1 must NOT match wire BackOff for image pull (canonical ImagePullBackOff).
	if matchAny(engine.Signal{
		Kind: engine.KindK8sEvent,
		TriageEvent: engine.TriageEvent{
			KindOfObject: "Pod",
			Namespace:    "staging-canary",
			Name:         "loadgen-7c9d8f-x1",
			Key:          engine.EventKey{UID: "u2", Reason: "BackOff"},
			Message:      "Back-off pulling image \"bad:tag\"",
		},
	}) {
		t.Error("rule 1 unexpectedly matched image-pull BackOff")
	}

	// Rule 1 prefix must NOT match mid-string occurrence.
	if matchAny(engine.Signal{
		Kind: engine.KindK8sEvent,
		TriageEvent: engine.TriageEvent{
			KindOfObject: "Pod",
			Namespace:    "staging-canary",
			Name:         "my-loadgen-1",
			Key:          engine.EventKey{UID: "u3", Reason: "CrashLoopBackOff"},
		},
	}) {
		t.Error("rule 1 prefix unexpectedly matched non-prefix name")
	}

	// Rule 2: multi-cluster exact match on cluster/project/zone + exact name + wire BackOff.
	zonalSig := engine.Signal{
		Kind:    engine.KindK8sEvent,
		Cluster: "prod-us",
		Project: "proj-1",
		Region:  "us-central1",
		Zone:    "us-central1-a",
		TriageEvent: engine.TriageEvent{
			KindOfObject: "Pod",
			Namespace:    "prod",
			Name:         "exact-worker",
			Key:          engine.EventKey{UID: "u4", Reason: "BackOff"},
			Message:      "Back-off restarting failed container",
		},
	}
	if !matchAny(zonalSig) {
		t.Error("expected rule 2 to match zonal multi-cluster signal")
	}
	wrongCluster := zonalSig
	wrongCluster.Cluster = "prod-eu"
	if matchAny(wrongCluster) {
		t.Error("rule 2 unexpectedly matched wrong cluster")
	}
	wrongProject := zonalSig
	wrongProject.Project = "proj-2"
	if matchAny(wrongProject) {
		t.Error("rule 2 unexpectedly matched wrong project")
	}
	wrongZone := zonalSig
	wrongZone.Zone = "us-central1-b"
	if matchAny(wrongZone) {
		t.Error("rule 2 unexpectedly matched wrong zone")
	}
	wrongExactName := zonalSig
	wrongExactName.Name = "exact-worker-2"
	if matchAny(wrongExactName) {
		t.Error("rule 2 exact name unexpectedly matched suffix")
	}

	// Rule 3: location matches Signal.Region for regional clusters (Zone == "")
	// and canonical ImagePullBackOff matches wire ErrImagePull? No: wire BackOff
	// pulling image canonicalizes to ImagePullBackOff.
	regionalSig := engine.Signal{
		Kind:   engine.KindK8sEvent,
		Region: "europe-west1",
		TriageEvent: engine.TriageEvent{
			KindOfObject: "Pod",
			Namespace:    "regional-ns",
			Name:         "api-0",
			Key:          engine.EventKey{UID: "u5", Reason: "BackOff"},
			Message:      "Back-off pulling image \"gcr.io/app:bad\"",
		},
	}
	if !matchAny(regionalSig) {
		t.Error("expected rule 3 to match regional cluster via Region and canonical ImagePullBackOff")
	}
}

func TestAlertExcludesStartup_Exit2(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	validFile := filepath.Join(dir, "valid.yaml")
	if err := os.WriteFile(validFile, []byte("excludes:\n  - kind: Pod\n    namespace: staging\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	malformedFile := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(malformedFile, []byte("excludes:\n  - kind: Pod\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missingFile := filepath.Join(dir, "does-not-exist.yaml")

	cases := []struct {
		name string
		argv []string
	}{
		{
			name: "missing file",
			argv: []string{"--dry-run", "--alert-excludes=" + missingFile},
		},
		{
			name: "malformed file (kind-only rule)",
			argv: []string{"--dry-run", "--alert-excludes=" + malformedFile},
		},
		{
			name: "shared mode rejected",
			argv: []string{"--dry-run", "--mode=shared", "--target-session=sess-1", "--alert-excludes=" + validFile},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Main(tc.argv); got != emit.ExitUsage {
				t.Errorf("Main(%v) = %d, want ExitUsage (%d)", tc.argv, got, emit.ExitUsage)
			}
		})
	}
}

func writeAlertExcludesTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "alert-excludes.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write alert-excludes: %v", err)
	}
	return path
}

func loadAlertExcludeStoreForTest(t *testing.T, path string, m *metrics, logf func(string, ...any)) *alertExcludeStore {
	t.Helper()
	rules, raw, err := loadAlertExcludesFile(path)
	if err != nil {
		t.Fatalf("loadAlertExcludesFile: %v", err)
	}
	return newAlertExcludeStore(path, rules, raw, m, logf)
}

func TestAlertExcludesDispatch_DowngradesToInfoWithStore(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)
	d.filter = engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 3, 3, 3))
	path := writeAlertExcludesTemp(t, `
excludes:
  - kind: Pod
    namespace: checkout
    reason: CrashLoopBackOff
`)
	d.excludes = loadAlertExcludeStoreForTest(t, path, d.metrics, func(string, ...any) {})

	// Matching event: wire BackOff canonicalizes to CrashLoopBackOff.
	sig := crashLoopSignal()
	sig.Key.Reason = "BackOff"
	sig.Message = "Back-off restarting failed container checkout in pod checkout-svc-7b9d-x4kzq"
	sig.Count = 3
	d.DispatchSignal(context.Background(), sig)

	if len(*injects) != 0 {
		t.Fatalf("excluded event opened/injected %d times, want 0", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.alertExcluded); got != 1 {
		t.Errorf("lookout_watch_alert_excluded_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(d.metrics.eventsFiltered.WithLabelValues(engine.GateCrashLoopDebounce)); got != 0 {
		t.Errorf("events_filtered_total = %v, want 0", got)
	}
	if got := testutil.ToFloat64(d.metrics.infoDropped.WithLabelValues(engine.KindK8sEvent)); got != 1 {
		t.Errorf("lookout_info_dropped_total{kind=k8s-event} = %v, want 1", got)
	}
	byRoute := occurrencesByRoute(t, s)
	infoRows := byRoute[store.RouteInfoStored]
	if len(infoRows) != 1 {
		t.Fatalf("routes = %v, want 1 info-stored row", routes(byRoute))
	}
	if infoRows[0].Severity != engine.SeverityInfo {
		t.Errorf("stored row severity = %q, want %q", infoRows[0].Severity, engine.SeverityInfo)
	}

	// Below-threshold event gets rejected by filter BEFORE alert_excluded_total.
	lowCount := sig
	lowCount.Key.UID = "uid-low-count"
	lowCount.Count = 1
	d.DispatchSignal(context.Background(), lowCount)
	if got := testutil.ToFloat64(d.metrics.alertExcluded); got != 1 {
		t.Errorf("filter-rejected event incremented alert_excluded_total to %v, want 1", got)
	}
	if got := testutil.ToFloat64(d.metrics.eventsFiltered.WithLabelValues(engine.GateCrashLoopDebounce)); got != 1 {
		t.Errorf("events_filtered_total{crashloop_debounce} = %v, want 1", got)
	}

	// Non-matching pod still opens a critical per-incident session.
	other := crashLoopSignal()
	other.Namespace = "payments"
	other.Key.UID = "uid-payments-1"
	other.Count = 3
	d.DispatchSignal(context.Background(), other)
	if len(*injects) != 1 {
		t.Fatalf("non-matching pod injects = %d, want 1", len(*injects))
	}
}

func TestAlertExcludesDispatch_NoStoreLogsInfoDrop(t *testing.T) {
	// Not parallel: captures standard log output.
	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	d.store = nil
	path := writeAlertExcludesTemp(t, `
excludes:
  - kind: Pod
    namespace: checkout
    reason: CrashLoopBackOff
`)
	d.excludes = loadAlertExcludeStoreForTest(t, path, d.metrics, func(string, ...any) {})

	var buf bytes.Buffer
	origOut := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origOut)

	sig := crashLoopSignal()
	sig.Count = 3
	d.DispatchSignal(context.Background(), sig)

	if len(*injects) != 0 {
		t.Fatalf("excluded event injected %d times, want 0", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.alertExcluded); got != 1 {
		t.Errorf("lookout_watch_alert_excluded_total = %v, want 1", got)
	}
	if !strings.Contains(buf.String(), "info-drop ") {
		t.Errorf("expected info-drop log line when --store is unset, got:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), `--alert-excludes kind="Pod" namespace="checkout" reason="CrashLoopBackOff"`) {
		t.Errorf("expected info-drop log line to name the matched exclude rule, got:\n%s", buf.String())
	}
}

func TestAlertExcludesDispatch_OutranksEscalatedTriageRecord(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)
	d.filter = engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 3, 3, 3))
	path := writeAlertExcludesTemp(t, `
excludes:
  - kind: Pod
    namespace: checkout
    reason: CrashLoopBackOff
`)
	d.excludes = loadAlertExcludeStoreForTest(t, path, d.metrics, func(string, ...any) {})

	sig := crashLoopSignal()
	sig.Count = 3
	// Agent escalated this exact pod to critical.
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint: stampedFingerprint(sig),
		ResourceKey: memory.ResourceKey("Pod", "checkout", "checkout-svc-7b9d-x4kzq"),
		Session:     "sid-escalated",
		Status:      memory.StatusEscalated,
	})

	d.DispatchSignal(context.Background(), sig)

	if len(*injects) != 0 {
		t.Fatalf("alert-excludes failed to outrank status=escalated: got %d injects, want 0", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.alertExcluded); got != 1 {
		t.Errorf("lookout_watch_alert_excluded_total = %v, want 1", got)
	}
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInfoStored]) != 1 {
		t.Errorf("routes = %v, want 1 info-stored row", routes(byRoute))
	}
}

func TestAlertExcludesHotReload_ValidAndRefused(t *testing.T) {
	t.Parallel()
	path := writeAlertExcludesTemp(t, `
excludes:
  - kind: Pod
    namespace: staging
    reason: CrashLoopBackOff
`)
	m := newMetrics()
	var logMu sync.Mutex
	var logs []string
	logf := func(format string, args ...any) {
		logMu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		logMu.Unlock()
	}
	allLogs := func() string {
		logMu.Lock()
		defer logMu.Unlock()
		return strings.Join(logs, "\n")
	}

	st := loadAlertExcludeStoreForTest(t, path, m, logf)
	if got := testutil.ToFloat64(m.alertExcludeRules); got != 1 {
		t.Fatalf("initial alert_exclude_rules = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.alertExcludeRefused); got != 0 {
		t.Fatalf("initial alert_exclude_refused = %v, want 0", got)
	}
	if !strings.Contains(allLogs(), `rule [1/1] kind="Pod" namespace="staging" reason="CrashLoopBackOff"`) {
		t.Errorf("startup audit log missing per-rule line:\n%s", allLogs())
	}

	podA := engine.Signal{TriageEvent: engine.TriageEvent{KindOfObject: "Pod", Namespace: "staging", Name: "a", Key: engine.EventKey{Reason: "CrashLoopBackOff"}}}
	podB := engine.Signal{TriageEvent: engine.TriageEvent{KindOfObject: "Pod", Namespace: "canary", Name: "loadgen-1", Key: engine.EventKey{Reason: "Unhealthy"}}}
	if _, ok := st.Match(podA); !ok {
		t.Fatal("expected podA to match initial rules")
	}
	if _, ok := st.Match(podB); ok {
		t.Fatal("did not expect podB to match initial rules")
	}

	// Valid hot-reload via SIGHUP channel adds a second rule.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sighup := make(chan os.Signal, 1)
	done := make(chan struct{})
	go func() {
		st.RunReloader(ctx, time.Hour, sighup)
		close(done)
	}()

	updatedYAML := `
excludes:
  - kind: Pod
    namespace: staging
    reason: CrashLoopBackOff
  - kind: Pod
    namespace: canary
    name: loadgen-*
`
	if err := os.WriteFile(path, []byte(updatedYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	sighup <- syscall.SIGHUP

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st.Len() == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st.Len() != 2 {
		t.Fatalf("after SIGHUP reload len = %d, want 2", st.Len())
	}
	if got := testutil.ToFloat64(m.alertExcludeRules); got != 2 {
		t.Errorf("after valid reload alert_exclude_rules = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.alertExcludeRefused); got != 0 {
		t.Errorf("after valid reload alert_exclude_refused = %v, want 0", got)
	}
	if _, ok := st.Match(podB); !ok {
		t.Error("expected podB to match after valid hot reload")
	}
	if !strings.Contains(allLogs(), `rule [2/2] kind="Pod" namespace="canary" name="loadgen-*"`) {
		t.Errorf("reload audit log missing rule [2/2]:\n%s", allLogs())
	}

	// Malformed/truncated edit: prior valid rules stay in force and refused=1.
	if err := os.WriteFile(path, []byte("excludes:\n  - kind: Pod\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.Reload(false); err == nil {
		t.Fatal("Reload(malformed) = nil, want error")
	}
	if got := testutil.ToFloat64(m.alertExcludeRefused); got != 1 {
		t.Errorf("after malformed reload alert_exclude_refused = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.alertExcludeRules); got != 2 {
		t.Errorf("after malformed reload alert_exclude_rules = %v, want 2 (prior kept)", got)
	}
	if _, ok := st.Match(podB); !ok {
		t.Error("prior rule for podB was lost on refused reload")
	}
	if !strings.Contains(allLogs(), "REFUSED") {
		t.Errorf("expected loud REFUSED log line, got:\n%s", allLogs())
	}

	// Subsequent valid reload clears refused back to 0.
	if err := os.WriteFile(path, []byte("excludes: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.Reload(false); err != nil {
		t.Fatalf("Reload(valid empty): %v", err)
	}
	if got := testutil.ToFloat64(m.alertExcludeRefused); got != 0 {
		t.Errorf("after recovery reload alert_exclude_refused = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.alertExcludeRules); got != 0 {
		t.Errorf("after recovery reload alert_exclude_rules = %v, want 0", got)
	}
	if _, ok := st.Match(podB); ok {
		t.Error("podB still matched after reloading excludes: []")
	}

	cancel()
	<-done
}

func TestAlertExcludesHotReload_OpenIncidentAbsorbedUntilResolved(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)

	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	dedup, err := engine.NewDedupCache(5*time.Minute, "")
	if err != nil {
		t.Fatalf("NewDedupCache: %v", err)
	}
	dedup.SetClock(func() time.Time { return now })
	open := true
	dedup.SetOpenCheck(func(engine.EventKey) bool { return open })
	d.dedup = dedup

	path := writeAlertExcludesTemp(t, "excludes: []\n")
	d.excludes = loadAlertExcludeStoreForTest(t, path, d.metrics, func(string, ...any) {})

	// Step 1: Open an incident normally before any exclude rule exists.
	sig := crashLoopSignal()
	sig.Count = 3
	sig.FirstSeen = now
	sig.LastSeen = now
	d.DispatchSignal(context.Background(), sig)

	if len(*injects) != 1 {
		t.Fatalf("initial open injects = %d, want 1", len(*injects))
	}
	key := sig.CanonicalKey()
	sid, bound := d.dedup.LookupSession(key)
	if !bound || sid == "" {
		t.Fatalf("expected incident bound to a session, got (%q, %v)", sid, bound)
	}

	// Step 2: Hot-reload a rule excluding this pod while the incident is open.
	if err := os.WriteFile(path, []byte(`
excludes:
  - kind: Pod
    namespace: checkout
    reason: CrashLoopBackOff
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := d.excludes.Reload(false); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// Step 3: Sighting inside the active dedup window is absorbed (no new
	// session, no reminder, existing session binding intact).
	now = now.Add(2 * time.Minute)
	sig2 := sig
	sig2.Count = 4
	sig2.LastSeen = now
	d.DispatchSignal(context.Background(), sig2)

	if len(*injects) != 1 {
		t.Fatalf("in-window excluded sighting injected: got %d injects, want 1", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.alertExcluded); got != 1 {
		t.Errorf("in-window alert_excluded_total = %v, want 1", got)
	}
	if gotSid, ok := d.dedup.LookupSession(key); !ok || gotSid != sid {
		t.Errorf("in-window LookupSession = (%q, %v), want (%q, true)", gotSid, ok, sid)
	}

	// Step 4: Excluded sibling source kind (objectstate.restart_burst with
	// canonical CrashLoopBackOff) must NOT emit a CrossSourceJoin followup
	// into the open session.
	now = now.Add(30 * time.Second)
	sibling := sig
	sibling.Kind = "objectstate.restart_burst"
	sibling.Key.Reason = "CrashLoopBackOff"
	sibling.LastSeen = now
	d.DispatchSignal(context.Background(), sibling)
	if len(*injects) != 1 {
		t.Fatalf("excluded sibling kind emitted cross-source join followup: got %d injects, want 1", len(*injects))
	}

	// Step 5: Advance PAST the 5m dedup window while the incident is still
	// open: sighting stays an absorbed duplicate so reminders stop AND the
	// bound SessionID stays intact for RecoveryTracker.
	now = now.Add(10 * time.Minute)
	sig3 := sig
	sig3.Count = 5
	sig3.LastSeen = now
	d.DispatchSignal(context.Background(), sig3)

	if len(*injects) != 1 {
		t.Fatalf("post-lapse excluded sighting injected: got %d injects, want 1", len(*injects))
	}
	if gotSid, ok := d.dedup.LookupSession(key); !ok || gotSid != sid {
		t.Fatalf("post-lapse LookupSession = (%q, %v), want (%q, true) so RecoveryTracker can still resolve", gotSid, ok, sid)
	}

	// Step 6: RecoveryTracker emits KindResolved for the SAME key AFTER the
	// dedup window lapsed — must deliver to the bound session with zero
	// unknown_session drops.
	now = now.Add(1 * time.Minute)
	resolvedSig := engine.Signal{
		Kind:        engine.KindResolved,
		TriageEvent: sig.TriageEvent,
		Recovery: &engine.Recovery{
			Resolution:        engine.ResolutionRecovered,
			ResolvedAt:        now,
			ClearedAfter:      10 * time.Minute,
			ObservedStableFor: 1 * time.Minute,
		},
	}
	d.DispatchSignal(context.Background(), resolvedSig)
	open = false

	if len(*injects) != 2 {
		t.Fatalf("post-lapse resolved inject count = %d, want 2", len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.recoveryDrops.WithLabelValues("unknown_session")); got != 0 {
		t.Errorf("recovery_drops_total{cause=unknown_session} = %v, want 0", got)
	}

	// Step 7: Once resolved, a fresh recurrence after the dedup window routes
	// cleanly to RouteStore (info-stored in --store) without opening a session.
	now = now.Add(6 * time.Minute)
	sig4 := sig
	sig4.Count = 6
	sig4.LastSeen = now
	d.DispatchSignal(context.Background(), sig4)

	if len(*injects) != 2 {
		t.Fatalf("post-resolution excluded recurrence injected: got %d injects, want 2", len(*injects))
	}
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInfoStored]) != 1 {
		t.Errorf("routes = %v, want 1 info-stored row after resolution", routes(byRoute))
	}
}

func TestAlertExcludesFlagUsagePlaceholder(t *testing.T) {
	t.Parallel()
	fs, _ := newFlagSet()
	fl := fs.Lookup("alert-excludes")
	if fl == nil {
		t.Fatal("alert-excludes flag not registered")
	}
	name, _ := flag.UnquoteUsage(fl)
	if name != "file" {
		t.Errorf("flag usage placeholder = %q, want %q", name, "file")
	}
}

func TestAlertExcludesRunnerRestart_KeepsLastGoodHotReload(t *testing.T) {
	t.Parallel()
	path := writeAlertExcludesTemp(t, `
excludes:
  - kind: Pod
    namespace: v1-ns
`)
	f, err := parseFlags([]string{"--dry-run", "--alert-excludes=" + path})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	r := &runner{
		f:       f,
		metrics: newMetrics(),
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	disp1 := &dispatcher{mode: "per-incident", metrics: r.metrics}
	dedup1, _ := engine.NewDedupCache(5*time.Minute, "")
	if wait := r.wireDispatcherRoutingAndLoops(ctx1, f, nil, disp1, dedup1, nil); wait != nil {
		cancel1()
		wait()
	} else {
		cancel1()
	}

	// Hot-reload to v2 rules while running.
	if err := os.WriteFile(path, []byte("excludes:\n  - kind: Pod\n    namespace: v2-ns\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.excludes.Reload(false); err != nil {
		t.Fatalf("Reload v2: %v", err)
	}

	// Now corrupt the file before a simulated supervisor restart.
	if err := os.WriteFile(path, []byte("excludes:\n  - kind: Pod\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	disp2 := &dispatcher{mode: "per-incident", metrics: r.metrics}
	dedup2, _ := engine.NewDedupCache(5*time.Minute, "")
	if wait := r.wireDispatcherRoutingAndLoops(ctx2, f, nil, disp2, dedup2, nil); wait != nil {
		cancel2()
		wait()
	} else {
		cancel2()
	}

	// Must keep v2 (last valid hot-reload), NOT fall back to startup v1!
	v2Sig := engine.Signal{TriageEvent: engine.TriageEvent{KindOfObject: "Pod", Namespace: "v2-ns"}}
	v1Sig := engine.Signal{TriageEvent: engine.TriageEvent{KindOfObject: "Pod", Namespace: "v1-ns"}}
	if _, ok := disp2.excludes.Match(v2Sig); !ok {
		t.Error("after restart with broken file, last-good hot-reloaded v2 rule was lost")
	}
	if _, ok := disp2.excludes.Match(v1Sig); ok {
		t.Error("after restart with broken file, stale startup v1 rule resurrected")
	}
	if got := testutil.ToFloat64(r.metrics.alertExcludeRefused); got != 1 {
		t.Errorf("alert_exclude_refused after restart with broken file = %v, want 1", got)
	}
}
