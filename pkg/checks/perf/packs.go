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

package perf

// The pack definitions — DATA, not code (DESIGN.md §5: four v2
// binaries that were each "query Monitoring, threshold, emit" became
// one command with data-driven packs). Every query is expressed in
// the backend-neutral cloud.SeriesQuery vocabulary (§15 Q4: GroupBy /
// Percentile / Rate all have obvious PromQL equivalents), so packs
// never learn which metrics backend serves them.
//
// Breach semantics (shared by every query): a series breaches when
// its MAXIMUM aligned point value in the window crosses a threshold —
// critical at max >= crit, warning at max STRICTLY ABOVE warn (so a
// warn threshold of 0 means "any sustained value at all", the APF
// reject shape). Non-breaching series emit nothing (zero nominal
// state); the summary's scanned still counts them.

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// packQuery is one thresholded metrics query of a pack.
type packQuery struct {
	// findingKind / reason stamp the emitted finding.
	findingKind string
	reason      string
	// metric is the backend-neutral metric name (translated by the
	// provider's backend — pkg/cloud/gke/metrics.go on GKE).
	metric string
	// matchers are exact-match label constraints.
	matchers map[string]string
	// groupBy / percentile / rate mirror cloud.SeriesQuery: nil
	// groupBy = raw series; non-nil (even empty) aggregates.
	groupBy    []string
	percentile int
	rate       bool
	// step is the desired alignment period.
	step time.Duration
	// warn / crit are the breach thresholds, in the metric's
	// neutral unit; comparison is always "above" (see the breach
	// semantics in the package comment).
	warn, crit float64
	// format renders values (observed/latest/threshold) with the
	// query's unit, deterministically.
	format func(float64) string
	// what / why are the human halves of the finding message: what
	// was measured, and why crossing the threshold hurts.
	what, why string
	// exclude drops series CLIENT-SIDE by label value before
	// evaluation. Matchers are exact-match only (§15 Q4 keeps the
	// query shape lowest-common-denominator), so "everything except
	// these verbs" cannot be pushed into the backend — the exclusion
	// is check-side policy and recorded here in the pack data.
	exclude map[string][]string
	// trend adds the first-half vs second-half window comparison to
	// the finding (the startup pack's §5 "P95 trend").
	trend bool
}

// pack is one --pack value: a named set of queries over a default
// window.
type pack struct {
	doc    string
	window time.Duration
	// queries run in order; a query degrading with
	// cloud.ErrMetricAbsent does not stop the ones after it.
	queries []packQuery
}

// gib for threshold literals.
const gib = float64(1 << 30)

// packs is the whole `perf probe` surface. Threshold provenance is
// per query, in place.
var packs = map[string]pack{
	"apiserver": {
		doc:    "API server p99 request latency by verb/resource",
		window: time.Hour,
		queries: []packQuery{{
			findingKind: "perf.apiserver_p99",
			reason:      "ApiserverLatencyHigh",
			metric:      "apiserver_request_duration_seconds",
			groupBy:     []string{"verb", "resource"},
			percentile:  99,
			step:        5 * time.Minute,
			// Warn at 1s (interactive calls feel it), critical at 4s
			// (controller resyncs and kubectl start timing out well
			// before the default 60s client timeout compounds).
			warn:   1.0,
			crit:   4.0,
			format: fmtSeconds,
			what:   "API server p99 request latency",
			why:    "every controller, webhook, and kubectl call inherits this latency",
			// WATCH/CONNECT/PROXY are long-lived by design — their
			// "latency" is connection lifetime, not service time.
			exclude: map[string][]string{"verb": {"WATCH", "CONNECT", "PROXY"}},
		}},
	},
	"apf": {
		doc:    "API Priority and Fairness: queue saturation and 429 rejects",
		window: time.Hour,
		queries: []packQuery{
			{
				findingKind: "perf.apf_saturation",
				reason:      "APFQueueSaturated",
				metric:      "apiserver_flowcontrol_current_inqueue_requests",
				groupBy:     []string{"priority_level"},
				step:        time.Minute,
				// Sustained queue depth: 10 queued requests is early
				// pressure, 100 is a priority level shedding load.
				warn:   10,
				crit:   100,
				format: fmtCount,
				what:   "APF in-queue requests",
				why:    "requests queued at this priority level wait before being served; sustained depth precedes 429 rejects",
			},
			{
				findingKind: "perf.apf_rejects",
				reason:      "APFRequestsRejected",
				metric:      "apiserver_request_total",
				matchers:    map[string]string{"code": "429"},
				groupBy:     []string{}, // total reject rate, one series
				rate:        true,
				step:        5 * time.Minute,
				// warn 0 = ANY sustained 429s (they are APF rejects
				// by definition); critical from 1/s.
				warn:   0,
				crit:   1.0,
				format: fmtPerSec,
				what:   "API server 429 reject rate",
				why:    "any sustained 429s mean APF is shedding load — clients are being throttled and retrying",
			},
		},
	},
	"etcd": {
		doc:    "etcd WAL fsync p99 and database size",
		window: time.Hour,
		queries: []packQuery{
			{
				findingKind: "perf.etcd_fsync",
				reason:      "EtcdFsyncSlow",
				metric:      "etcd_disk_wal_fsync_duration_seconds",
				groupBy:     []string{},
				percentile:  99,
				step:        5 * time.Minute,
				// etcd's own SLO guidance: WAL fsync p99 under 10ms;
				// 100ms means every write (and therefore every
				// cluster mutation) is stalling on disk.
				warn:   0.01,
				crit:   0.1,
				format: fmtSeconds,
				what:   "etcd WAL fsync p99",
				why:    "etcd's SLO wants fsync p99 under 10ms — a slower disk stalls every write the control plane makes",
			},
			{
				findingKind: "perf.etcd_db_size",
				reason:      "EtcdDatabaseLarge",
				metric:      "etcd_mvcc_db_total_size_in_bytes",
				step:        5 * time.Minute,
				// GKE's etcd quota is ~6GiB: warn with headroom to
				// clean up (4GiB), critical when the quota is close
				// enough that the control plane going read-only is
				// imminent (5.5GiB).
				warn:   4 * gib,
				crit:   5.5 * gib,
				format: fmtGiB,
				what:   "etcd database size",
				why:    "GKE's etcd quota is ~6GiB — at the quota the control plane stops accepting writes",
			},
		},
	},
	"startup": {
		doc:    "pod startup (first-ready) p95 with window trend",
		window: 24 * time.Hour,
		queries: []packQuery{{
			findingKind: "perf.startup_p95",
			reason:      "PodStartupSlow",
			metric:      "pod/latencies/pod_first_ready",
			groupBy:     []string{},
			percentile:  95,
			step:        30 * time.Minute,
			// First-ready includes image pull: over a minute is
			// slow scale-up, five minutes means autoscaling cannot
			// react inside an incident.
			warn:   60,
			crit:   300,
			format: fmtSeconds,
			what:   "pod startup p95 (created to first ready)",
			why:    "slow image pulls, scheduling, or init containers delay every scale-up and rollout",
			trend:  true,
		}},
	},
}

// packOrder is the canonical listing order — the §5 matrix's
// `--pack=apiserver|apf|etcd|startup`. init() panics if it drifts
// from the packs map (a pack added to one but not the other is a
// compile-adjacent bug, same policy as checks.Register).
var packOrder = []string{"apiserver", "apf", "etcd", "startup"}

func init() {
	if len(packOrder) != len(packs) {
		panic("perf: packOrder and packs disagree")
	}
	for _, n := range packOrder {
		if _, ok := packs[n]; !ok {
			panic("perf: packOrder names unknown pack " + n)
		}
	}
}

// packNames returns the valid --pack values in canonical order, for
// usage errors and --help.
func packNames() string {
	return strings.Join(packOrder, "|")
}

// labelDetailKeys is the fixed emission order for group-label detail
// fields — every key here is declared in the command's Output.
var labelDetailKeys = []string{"verb", "resource", "priority_level", "code"}

// Value formatters: deterministic short strings (golden-testable),
// trailing zeros trimmed.

func trimFloat(v float64, decimals int) string {
	p := math.Pow10(decimals)
	return strconv.FormatFloat(math.Round(v*p)/p, 'f', -1, 64)
}

func fmtSeconds(v float64) string { return trimFloat(v, 3) + "s" }
func fmtCount(v float64) string   { return trimFloat(v, 1) }
func fmtPerSec(v float64) string  { return trimFloat(v, 2) + "/s" }
func fmtGiB(v float64) string     { return trimFloat(v/gib, 2) + "GiB" }
