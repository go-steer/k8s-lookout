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

package perf_test

// §13 conventions, mirroring the cloudcheck and `triage top`
// history suites: a capability-backed fake provider (cloud.NoProvider
// plus a working Metrics), canned per-metric series with exact
// threshold math, one golden per pack, VerifyContract in both
// formats, and both explicit degradation paths (§2 no-provider and
// the per-metric pack_unavailable) exercised.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/perf"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var fixedNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// fakeBackend serves canned series (or errors) per neutral metric
// name and records every query for shape assertions.
type fakeBackend struct {
	series  map[string][]cloud.Series
	errs    map[string]error
	queries []cloud.SeriesQuery
}

func (b *fakeBackend) QuerySeries(_ context.Context, q cloud.SeriesQuery) ([]cloud.Series, error) {
	b.queries = append(b.queries, q)
	if err := b.errs[q.Metric]; err != nil {
		return nil, err
	}
	return b.series[q.Metric], nil
}

// metricsProvider is NoProvider plus a working Metrics capability
// (the same embedding trick as top_test.go).
type metricsProvider struct {
	cloud.Provider
	backend cloud.MetricsBackend
}

func (p metricsProvider) Metrics() (cloud.MetricsBackend, bool) { return p.backend, true }

func testDeps(p cloud.Provider) perf.Deps {
	return perf.Deps{
		Provider: func(context.Context) (cloud.Provider, error) { return p, nil },
		Now:      func() time.Time { return fixedNow },
	}
}

func backedDeps(b *fakeBackend) perf.Deps {
	return testDeps(metricsProvider{Provider: cloud.NoProvider, backend: b})
}

// series builds one labeled series with points at 1-minute spacing
// ending at fixedNow (absolute times are irrelevant to the breach
// math; only ordering matters).
func series(metric string, labels map[string]string, vals ...float64) cloud.Series {
	s := cloud.Series{Metric: metric, Labels: labels}
	start := fixedNow.Add(-time.Duration(len(vals)) * time.Minute)
	for i, v := range vals {
		s.Points = append(s.Points, cloud.Point{Time: start.Add(time.Duration(i) * time.Minute), Value: v})
	}
	return s
}

// Shared logfmt parsing helpers (same shapes as the other command
// test suites).

func parseLine(t *testing.T, line string) map[string]string {
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
			end := len(rest)
			for i := 1; i < len(rest); i++ {
				if rest[i] == '"' && rest[i-1] != '\\' {
					end = i + 1
					break
				}
			}
			val = strings.ReplaceAll(rest[1:end-1], `\"`, `"`)
			rest = rest[end:]
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

func findingLines(t *testing.T, stdout string) []map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	var out []map[string]string
	for _, l := range lines[:len(lines)-1] {
		out = append(out, parseLine(t, l))
	}
	return out
}

func summaryLine(t *testing.T, stdout string) map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	return parseLine(t, lines[len(lines)-1])
}

// Pack fixtures with exact threshold math.

func apiserverBackend() *fakeBackend {
	return &fakeBackend{series: map[string][]cloud.Series{
		"apiserver_request_duration_seconds": {
			// max exactly warn (1.0): NOT breaching — warning is
			// strictly above warn.
			series("apiserver_request_duration_seconds", map[string]string{"verb": "GET", "resource": "configmaps"}, 0.4, 1.0),
			// max exactly crit (4.0): critical — critical is >=.
			series("apiserver_request_duration_seconds", map[string]string{"verb": "LIST", "resource": "pods"}, 0.75, 4.0, 2.25),
			// warn + epsilon: warning.
			series("apiserver_request_duration_seconds", map[string]string{"verb": "POST", "resource": "secrets"}, 1.01, 0.9),
			// WATCH is long-lived by design: excluded client-side,
			// however extreme its values.
			series("apiserver_request_duration_seconds", map[string]string{"verb": "WATCH", "resource": "pods"}, 3600),
		},
	}}
}

func apfBackend() *fakeBackend {
	return &fakeBackend{series: map[string][]cloud.Series{
		"apiserver_flowcontrol_current_inqueue_requests": {
			series("apiserver_flowcontrol_current_inqueue_requests", map[string]string{"priority_level": "workload-low"}, 12, 34, 8),
			series("apiserver_flowcontrol_current_inqueue_requests", map[string]string{"priority_level": "global-default"}, 2, 1),
			series("apiserver_flowcontrol_current_inqueue_requests", map[string]string{"priority_level": "leader-election"}, 150, 90),
		},
		// One merged 429-rate series (GroupBy []); max 0.4/s: above
		// the warn=0 "any sustained 429s" line, below critical 1/s.
		"apiserver_request_total": {
			series("apiserver_request_total", nil, 0.1, 0.4, 0.2),
		},
	}}
}

func etcdBackend() *fakeBackend {
	return &fakeBackend{series: map[string][]cloud.Series{
		"etcd_disk_wal_fsync_duration_seconds": {
			series("etcd_disk_wal_fsync_duration_seconds", nil, 0.008, 0.024),
		},
		"etcd_mvcc_db_total_size_in_bytes": {
			// 5.9GiB: past the 5.5GiB critical line (GKE quota ~6GiB).
			series("etcd_mvcc_db_total_size_in_bytes", nil, 4.2*(1<<30), 5.9*(1<<30)),
		},
	}}
}

func startupBackend() *fakeBackend {
	return &fakeBackend{series: map[string][]cloud.Series{
		// First-half mean 50, second-half mean 67 → trend +34%;
		// max 74 breaches warn=60 only.
		"pod/latencies/pod_first_ready": {
			series("pod/latencies/pod_first_ready", nil, 50, 50, 60, 74),
		},
	}}
}

func TestApiserverPack(t *testing.T) {
	backend := apiserverBackend()
	res := checktest.Run(t, perf.Command(backedDeps(backend)), "--pack=apiserver")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("findings = %d, want 2 (exact-warn stays silent, WATCH excluded): %v", len(recs), recs)
	}
	// Deterministic order: sorted by verb — LIST before POST.
	crit := recs[0]
	if crit["kind"] != "perf.apiserver_p99" || crit["severity"] != "critical" || crit["reason"] != "ApiserverLatencyHigh" {
		t.Errorf("critical finding = %v", crit)
	}
	if crit["verb"] != "LIST" || crit["resource"] != "pods" || crit["observed"] != "4s" || crit["threshold"] != "4s" || crit["latest"] != "2.25s" {
		t.Errorf("critical finding details = %v, want LIST/pods observed=4s threshold=4s latest=2.25s", crit)
	}
	if crit["pack"] != "apiserver" || crit["window"] != "1h0m0s" {
		t.Errorf("critical finding pack/window = %v", crit)
	}
	warn := recs[1]
	if warn["severity"] != "warning" || warn["verb"] != "POST" || warn["observed"] != "1.01s" || warn["threshold"] != "1s" {
		t.Errorf("warning finding = %v, want POST at 1.01s over the 1s warn line", warn)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "3" || sum["pack"] != "apiserver" || sum["window"] != "1h0m0s" {
		t.Errorf("summary = %v, want scanned=3 (post-exclusion) with pack/window notes", sum)
	}
	// The backend saw the §15 Q4 shape the pack declares.
	if len(backend.queries) != 1 {
		t.Fatalf("queries = %d, want 1", len(backend.queries))
	}
	q := backend.queries[0]
	if q.Percentile != 99 || len(q.GroupBy) != 2 || q.GroupBy[0] != "verb" || q.GroupBy[1] != "resource" || q.Step != 5*time.Minute || q.Rate {
		t.Errorf("query = %+v, want p99 grouped by verb/resource at 5m", q)
	}
	if !q.Window.End.Equal(fixedNow) || !q.Window.Start.Equal(fixedNow.Add(-time.Hour)) {
		t.Errorf("window = %+v, want [now-1h, now] on the injected clock", q.Window)
	}
}

func TestAPFPack(t *testing.T) {
	backend := apfBackend()
	res := checktest.Run(t, perf.Command(backedDeps(backend)), "--pack=apf")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 3 {
		t.Fatalf("findings = %d, want 3: %v", len(recs), recs)
	}
	// Saturation first (query order), sorted by priority_level:
	// leader-election (critical) before workload-low (warning);
	// global-default (max 2 <= warn 10) stays silent.
	if recs[0]["kind"] != "perf.apf_saturation" || recs[0]["severity"] != "critical" ||
		recs[0]["priority_level"] != "leader-election" || recs[0]["observed"] != "150" || recs[0]["threshold"] != "100" {
		t.Errorf("saturation critical = %v", recs[0])
	}
	if recs[1]["severity"] != "warning" || recs[1]["priority_level"] != "workload-low" || recs[1]["observed"] != "34" || recs[1]["threshold"] != "10" {
		t.Errorf("saturation warning = %v", recs[1])
	}
	// 429s: warn=0 means any sustained rate breaches; the matcher-
	// pinned code label survives the total-rate collapse.
	rej := recs[2]
	if rej["kind"] != "perf.apf_rejects" || rej["severity"] != "warning" || rej["code"] != "429" ||
		rej["observed"] != "0.4/s" || rej["threshold"] != "0/s" || rej["reason"] != "APFRequestsRejected" {
		t.Errorf("rejects finding = %v", rej)
	}
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "4" {
		t.Errorf("summary = %v, want scanned=4 (3 saturation series + 1 rate series)", sum)
	}
	// The 429 query carries the matcher + rate + total-collapse shape.
	q := backend.queries[1]
	if !q.Rate || q.Matchers["code"] != "429" || q.GroupBy == nil || len(q.GroupBy) != 0 {
		t.Errorf("rejects query = %+v, want Rate + code=429 + empty non-nil GroupBy", q)
	}
}

func TestEtcdPack(t *testing.T) {
	res := checktest.Run(t, perf.Command(backedDeps(etcdBackend())), "--pack=etcd")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("findings = %d, want 2: %v", len(recs), recs)
	}
	if recs[0]["kind"] != "perf.etcd_fsync" || recs[0]["severity"] != "warning" ||
		recs[0]["observed"] != "0.024s" || recs[0]["threshold"] != "0.01s" {
		t.Errorf("fsync finding = %v, want 0.024s over the 10ms SLO line", recs[0])
	}
	if recs[1]["kind"] != "perf.etcd_db_size" || recs[1]["severity"] != "critical" ||
		recs[1]["observed"] != "5.9GiB" || recs[1]["threshold"] != "5.5GiB" {
		t.Errorf("db-size finding = %v", recs[1])
	}
}

func TestStartupPackTrend(t *testing.T) {
	backend := startupBackend()
	res := checktest.Run(t, perf.Command(backedDeps(backend)), "--pack=startup")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("findings = %d, want 1: %v", len(recs), recs)
	}
	r := recs[0]
	if r["kind"] != "perf.startup_p95" || r["severity"] != "warning" || r["observed"] != "74s" || r["threshold"] != "60s" || r["latest"] != "74s" {
		t.Errorf("startup finding = %v", r)
	}
	// The §5 trend: halves (50,50) vs (60,74) → means 50 vs 67 → +34%.
	if r["trend"] != "+34%" {
		t.Errorf("trend = %q, want +34%%", r["trend"])
	}
	if r["window"] != "24h0m0s" {
		t.Errorf("window = %q, want the 24h pack default", r["window"])
	}
	q := backend.queries[0]
	if q.Percentile != 95 || !q.Window.Start.Equal(fixedNow.Add(-24*time.Hour)) {
		t.Errorf("query = %+v, want p95 over the 24h default window", q)
	}
}

// TestSinceOverridesPackWindow: --since re-anchors the window and
// the emitted window fields.
func TestSinceOverridesPackWindow(t *testing.T) {
	backend := startupBackend()
	res := checktest.Run(t, perf.Command(backedDeps(backend)), "--pack=startup", "--since=6h")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !backend.queries[0].Window.Start.Equal(fixedNow.Add(-6 * time.Hour)) {
		t.Errorf("window start = %v, want now-6h", backend.queries[0].Window.Start)
	}
	if sum := summaryLine(t, res.Stdout); sum["window"] != "6h0m0s" {
		t.Errorf("summary = %v, want window=6h0m0s", sum)
	}
}

// TestPackUnavailable: a metric positively absent from the workspace
// (wrapped cloud.ErrMetricAbsent) degrades to one explicit
// perf.pack_unavailable finding while the pack's other queries still
// run — never silence, never a whole-pack failure (§2, §5).
func TestPackUnavailable(t *testing.T) {
	backend := etcdBackend()
	backend.errs = map[string]error{
		"etcd_disk_wal_fsync_duration_seconds": errors.New("gke: metric etcd_disk_wal_fsync_duration_seconds is not in this project's workspace: " + cloud.ErrMetricAbsent.Error()),
	}
	// A plain error with matching text must NOT trigger the absence
	// path — only the wrapped sentinel does. Assert exit 1 first.
	res := checktest.Run(t, perf.Command(backedDeps(backend)), "--pack=etcd")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("unwrapped error: exit %d, want 1 (text matching must not count as absence)", res.Code)
	}

	backend = etcdBackend()
	backend.errs = map[string]error{
		"etcd_disk_wal_fsync_duration_seconds": errors.Join(errors.New("gke: control-plane metrics not enabled"), cloud.ErrMetricAbsent),
	}
	res = checktest.Run(t, perf.Command(backedDeps(backend)), "--pack=etcd")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("findings = %d, want pack_unavailable + the db-size finding: %v", len(recs), recs)
	}
	pu := recs[0]
	if pu["kind"] != "perf.pack_unavailable" || pu["severity"] != "warning" || pu["reason"] != "MetricAbsent" ||
		pu["pack"] != "etcd" || pu["metric"] != "etcd_disk_wal_fsync_duration_seconds" {
		t.Errorf("pack_unavailable = %v", pu)
	}
	if !strings.Contains(pu["message"], "enable GKE control-plane metrics") {
		t.Errorf("message %q must carry the remedy", pu["message"])
	}
	if recs[1]["kind"] != "perf.etcd_db_size" {
		t.Errorf("second finding = %v, want the db-size query to have still run", recs[1])
	}
	if sum := summaryLine(t, res.Stdout); sum["scanned"] != "1" {
		t.Errorf("summary = %v, want scanned=1 (only the db-size series was examined)", sum)
	}
}

// TestNoProvider: the §2 no-provider degradation — one explicit
// cloud.unavailable finding, the summary marker, exit 0, scanned=0.
func TestNoProvider(t *testing.T) {
	res := checktest.Run(t, perf.Command(testDeps(cloud.NoProvider)), "--pack=apiserver")
	if res.Code != emit.ExitData {
		t.Fatalf("unavailable path must exit 0 (explicit, not broken), got %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("want exactly the cloud.unavailable finding, got %d: %v", len(recs), recs)
	}
	r := recs[0]
	if r["kind"] != "cloud.unavailable" || r["reason"] != "CapabilityUnavailable" ||
		r["capability"] != string(cloud.CapabilityMetrics) || r["provider"] != cloud.NoProviderName ||
		!strings.Contains(r["message"], cloud.NoProviderReason) {
		t.Errorf("unavailable finding = %v", r)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["scanned"] != "0" || sum["unavailable"] != cloud.NoProviderReason {
		t.Errorf("summary = %v, want scanned=0 unavailable=%q", sum, cloud.NoProviderReason)
	}
}

// TestBackendRuntimeError: any non-absence backend failure is a
// runtime error — stderr diagnostics, exit 1, no summary.
func TestBackendRuntimeError(t *testing.T) {
	backend := apiserverBackend()
	backend.errs = map[string]error{"apiserver_request_duration_seconds": errors.New("monitoring 500")}
	res := checktest.Run(t, perf.Command(backedDeps(backend)), "--pack=apiserver")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want 1", res.Code)
	}
	if !strings.Contains(res.Stderr, "monitoring 500") {
		t.Errorf("stderr %q must surface the backend error", res.Stderr)
	}
}

func TestUsageErrors(t *testing.T) {
	cmd := perf.Command(backedDeps(apiserverBackend()))
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"missing pack", nil, "--pack is required"},
		{"unknown pack", []string{"--pack=nope"}, "unknown pack"},
		{"namespace", []string{"--pack=apiserver", "--namespace=prod"}, "do not apply"},
		{"all namespaces", []string{"--pack=apiserver", "-A"}, "do not apply"},
		{"workload", []string{"--pack=apiserver", "--workload=Deployment/prod/api"}, "do not apply"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, cmd, tc.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d (stderr %q)", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr %q, want it to mention %q", res.Stderr, tc.want)
			}
			if res.Stdout != "" {
				t.Errorf("usage error leaked stdout: %q", res.Stdout)
			}
			// The valid pack list must be in the pack-flag errors.
			if strings.Contains(tc.want, "pack") && !strings.Contains(res.Stderr, "apiserver|apf|etcd|startup") {
				t.Errorf("stderr %q, want the valid pack list", res.Stderr)
			}
		})
	}
}

func TestContract(t *testing.T) {
	// A healthy (breaching-findings) run per pack family and the
	// unavailable path, both formats.
	checktest.VerifyContract(t, perf.Command(backedDeps(apiserverBackend())), "--pack=apiserver")
	checktest.VerifyContract(t, perf.Command(backedDeps(startupBackend())), "--pack=startup")
	checktest.VerifyContract(t, perf.Command(testDeps(cloud.NoProvider)), "--pack=etcd")
}

func TestRegistered(t *testing.T) {
	c, ok := checks.Lookup("perf probe")
	if !ok {
		t.Fatal("perf probe is not in the default registry")
	}
	if c.MCPName != "k8s_perf_probe" {
		t.Errorf("MCP name = %q, want k8s_perf_probe", c.MCPName)
	}
}

// TestGolden pins one full stdout per pack (fake clock: elapsed is
// always 100ms), including the etcd pack's mixed
// pack_unavailable + finding shape.
func TestGolden(t *testing.T) {
	etcdMixed := etcdBackend()
	etcdMixed.errs = map[string]error{
		"etcd_disk_wal_fsync_duration_seconds": errors.Join(errors.New("gke: control-plane metrics not enabled"), cloud.ErrMetricAbsent),
	}
	cases := []struct {
		pack    string
		backend *fakeBackend
	}{
		{"apiserver", apiserverBackend()},
		{"apf", apfBackend()},
		{"etcd", etcdMixed},
		{"startup", startupBackend()},
	}
	for _, tc := range cases {
		t.Run(tc.pack, func(t *testing.T) {
			res := checktest.Run(t, perf.Command(backedDeps(tc.backend)), "--pack="+tc.pack)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}
			checktest.Golden(t, "testdata/"+tc.pack+".golden", res.Stdout)
		})
	}
}
