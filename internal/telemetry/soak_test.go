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

package telemetry

// The black-hole soak behind §14's Phase 8 exit criterion: the process
// survives a black-holed OTLP endpoint for 24 h with flat RSS.
//
// This is a Go test and not a shell harness for the same reason
// cmd/leeway's smoke test is: there is one copy of the setup, it
// compiles, and `go test` is the only thing anyone has to know. It is
// env-gated rather than tagged because §12.1 asks for something with
// "somewhere to run that is not a PR check" — a build tag hides it from
// `go vet` and from the compiler on every ordinary run, and a soak that
// stops compiling between soaks is a soak nobody runs twice.
//
// Run it with dev/tools/soak-otlp, which is a wrapper over:
//
//	LOOKOUT_SOAK=1 LOOKOUT_SOAK_DURATION=24h go test -run TestSoak -v \
//	  -timeout 25h ./internal/telemetry/

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// soakDefaults. The interval is far shorter than production's minute so
// a bounded run still sees dozens of failed exports; the series count is
// the order §8.4's cardinality controls cap a large cluster at.
const (
	defaultSoakDuration = 2 * time.Minute
	defaultSoakInterval = 10 * time.Second
	defaultSoakSeries   = 2000
)

func soakEnvDuration(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", key, v, err)
	}
	return d
}

func soakEnvInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", key, v, err)
	}
	return n
}

// residentBytes reads this process's RSS. Flat RSS is the exit
// criterion's own wording, and it is not the same claim as a flat Go
// heap: a heap that is returned to the runtime but not to the kernel
// still shows here. Linux-only; the soak runs on Linux.
func residentBytes() (int64, bool) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}

func mib(n int64) float64 { return float64(n) / (1 << 20) }

// TestSoak_ABlackHoledOTLPEndpoint runs the real OTLP HTTP exporter
// against a collector that accepts every connection and answers none.
//
// What it asserts, which is §12.1's export-pipeline row verbatim:
// rising drop counts, a flat heap, and an unaffected /metrics.
func TestSoak_ABlackHoledOTLPEndpoint(t *testing.T) {
	if strings.TrimSpace(os.Getenv("LOOKOUT_SOAK")) == "" {
		t.Skip("soak: set LOOKOUT_SOAK=1 to run (LOOKOUT_SOAK_DURATION, LOOKOUT_SOAK_PUSH_INTERVAL, LOOKOUT_SOAK_SERIES tune it); dev/tools/soak-otlp wraps this")
	}

	duration := soakEnvDuration(t, "LOOKOUT_SOAK_DURATION", defaultSoakDuration)
	interval := soakEnvDuration(t, "LOOKOUT_SOAK_PUSH_INTERVAL", defaultSoakInterval)
	series := soakEnvInt(t, "LOOKOUT_SOAK_SERIES", defaultSoakSeries)

	// The black hole: accept every connection, answer none.
	//
	// The handler must DRAIN THE BODY before it waits. net/http only
	// starts the background read that notices a client hanging up once
	// the request body has hit EOF, so a handler that blocks on
	// r.Context() without reading first never learns the exporter gave
	// up — the handlers then pile up one per attempt, which is growth in
	// the harness charged to the process under test, and is the exact
	// measurement error this soak exists to detect. (Observed: a 70s run
	// wedged in srv.Close with a handler per export still parked.)
	var accepted atomic.Int64
	teardown := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		accepted.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-teardown:
		}
	}))
	defer srv.Close()
	defer close(teardown)

	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", srv.URL)

	reg := prometheus.NewRegistry()
	mp, shutdown, err := SetupMetrics(context.Background(), MetricsOptions{
		Mode:         ModeOTLP,
		Registerer:   reg,
		Cluster:      "soak",
		PushInterval: interval,
	})
	if err != nil {
		t.Fatalf("SetupMetrics: %v", err)
	}

	meter := mp.Meter("soak")
	counter, err := meter.Int64Counter("soak.observations")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	gauge, err := meter.Float64Gauge("soak.level")
	if err != nil {
		t.Fatalf("Float64Gauge: %v", err)
	}

	// A fixed attribute set per series, built once: allocating them in
	// the loop would measure the load generator's garbage rather than
	// the export path's.
	sets := make([]metric.MeasurementOption, series)
	for i := range sets {
		sets[i] = metric.WithAttributes(
			attribute.String("domain", fmt.Sprintf("zone-%d", i%64)),
			attribute.String("subject", fmt.Sprintf("workload-%d", i)),
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var recorded atomic.Int64
	go func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				for i, set := range sets {
					counter.Add(ctx, 1, set)
					gauge.Record(ctx, float64(i%17), set)
				}
				recorded.Add(int64(len(sets)))
			}
		}
	}()

	type row struct {
		at       time.Duration
		heap     int64
		rss      int64
		failed   float64
		dropped  float64
		families int
	}
	var rows []row

	sampleOnce := func(at time.Duration) row {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		rss, _ := residentBytes()
		families, gerr := reg.Gather()
		if gerr != nil {
			t.Errorf("t=%s: /metrics degraded while the collector was black-holed: %v", at, gerr)
		}
		failed, _ := sample(t, reg, "lookout_otlp_exports_total", map[string]string{"outcome": "failed"})
		dropped, _ := sample(t, reg, "lookout_otlp_points_dropped_total", nil)
		return row{at, int64(ms.HeapInuse), rss, failed, dropped, len(families)}
	}

	start := time.Now()
	// Warm-up: the first sample is taken after one full export cycle, so
	// the baseline includes the exporter's steady-state allocations
	// rather than treating them as growth.
	warmup := interval + 5*time.Second
	if warmup > duration/4 {
		warmup = duration / 4
	}
	time.Sleep(warmup)
	base := sampleOnce(time.Since(start))
	rows = append(rows, base)

	samplePeriod := duration / 12
	if samplePeriod < interval {
		samplePeriod = interval
	}
	for time.Since(start) < duration {
		remaining := duration - time.Since(start)
		if remaining < samplePeriod {
			time.Sleep(remaining)
		} else {
			time.Sleep(samplePeriod)
		}
		rows = append(rows, sampleOnce(time.Since(start)))
	}

	cancel()
	last := rows[len(rows)-1]

	t.Logf("black-hole soak: duration=%s push-interval=%s series=%d endpoint=%s",
		duration, interval, series, srv.URL)
	t.Logf("%10s %12s %12s %14s %16s %10s", "elapsed", "heap_MiB", "rss_MiB", "exports_failed", "points_dropped", "families")
	for _, r := range rows {
		t.Logf("%10s %12.1f %12.1f %14.0f %16.0f %10d",
			r.at.Round(time.Second), mib(r.heap), mib(r.rss), r.failed, r.dropped, r.families)
	}
	t.Logf("recorded %d measurements; the collector accepted %d requests and answered none",
		recorded.Load(), accepted.Load())

	// Rising drop counts.
	if last.failed <= base.failed {
		t.Errorf("exports_total{failed} went %v → %v: a black-holed collector must show as failures, not silence",
			base.failed, last.failed)
	}
	if last.dropped <= base.dropped {
		t.Errorf("points_dropped_total went %v → %v: the dropped points are how an operator learns the dashboard is stale",
			base.dropped, last.dropped)
	}
	// The last-success gauge stays at zero: nothing ever landed.
	if got, _ := sample(t, reg, "lookout_otlp_export_last_success_timestamp_seconds", nil); got != 0 {
		t.Errorf("last_success_timestamp_seconds = %v, want 0 — nothing was ever accepted", got)
	}

	// Flat heap. Twice the warmed-up baseline is generous on purpose: the
	// failure this guards against is unbounded queueing, which on a run
	// of any length does not land near a constant factor.
	if last.heap > 2*base.heap {
		t.Errorf("heap grew %.1f MiB → %.1f MiB over %s, which is not flat",
			mib(base.heap), mib(last.heap), duration)
	}
	if base.rss > 0 && last.rss > 2*base.rss {
		t.Errorf("RSS grew %.1f MiB → %.1f MiB over %s, which is not flat",
			mib(base.rss), mib(last.rss), duration)
	}

	// And /metrics is unaffected: same series count as at the baseline,
	// still gathering, with the recorded counter intact.
	if last.families != base.families {
		t.Errorf("/metrics exposed %d families at the baseline and %d at the end", base.families, last.families)
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Minute)
	defer shutCancel()
	// Shutdown flushes once more into the black hole, so it reports the
	// failure. That is the contract, not a defect: a shutdown that
	// claimed success after losing its final flush would be worse.
	if err := shutdown(shutCtx); err == nil {
		t.Errorf("shutdown swallowed the final export failure")
	}
}
