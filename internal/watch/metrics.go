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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metrics bundles the sidecar's Prometheus counters + gauges. Kept
// as a struct so the wiring is testable — tests can construct a
// registry, wire metrics into it, and assert values without
// stringifying Prometheus output.
type metrics struct {
	registry            *prometheus.Registry
	eventsSeen          *prometheus.CounterVec
	eventsInjected      *prometheus.CounterVec
	eventsDedupSuppress *prometheus.CounterVec
	injectErrors        *prometheus.CounterVec
	sessionCreates      *prometheus.CounterVec
	activeIncidents     prometheus.Gauge
	recoveriesObserved  *prometheus.CounterVec
	recoveriesReverted  prometheus.Counter
	recoveryTracking    prometheus.Gauge
	recoveryDrops       *prometheus.CounterVec
	stormsFormed        prometheus.Counter
	stormsResolved      prometheus.Counter
	stormsActive        prometheus.Gauge
	stormMembers        *prometheus.CounterVec
	watchboardEntries   *prometheus.CounterVec
	watchboardDigests   prometheus.Counter
	watchboardRotations prometheus.Counter
	watchboardBuffered  prometheus.Gauge
	infoDropped         *prometheus.CounterVec
}

// newMetrics registers all sidecar metrics against a fresh registry
// and returns the bundle. Tests use this with an isolated registry;
// main.go passes the resulting handler to promhttp.
func newMetrics() *metrics {
	reg := prometheus.NewRegistry()
	m := &metrics{
		registry: reg,
		eventsSeen: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_events_seen_total",
			Help: "Total k8s events observed by the informer, before filter.",
		}, []string{"reason", "namespace"}),
		eventsInjected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_events_injected_total",
			Help: "Total events that survived filter + dedup and were POSTed to the daemon.",
		}, []string{"reason", "namespace"}),
		eventsDedupSuppress: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_events_deduped_total",
			Help: "Total events suppressed by the rolling-window dedup cache.",
		}, []string{"reason", "namespace"}),
		injectErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_inject_errors_total",
			Help: "Total inject (or session-create) attempts that returned a non-2xx response or transport error.",
		}, []string{"reason", "http_code"}),
		sessionCreates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_session_creates_total",
			Help: "Total POST /sessions attempts, labeled by outcome.",
		}, []string{"outcome"}),
		activeIncidents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "k8s_event_watcher_active_incidents",
			Help: "Current number of incidents in the sidecar's dedup cache.",
		}),
		recoveriesObserved: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_recoveries_observed_total",
			Help: "Total kind=resolved outcome records emitted (§7.4), by resolution (recovered|object_deleted).",
		}, []string{"resolution"}),
		recoveriesReverted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "k8s_event_watcher_recoveries_reverted_total",
			Help: "Total kind=resolved.reverted records emitted: symptom recurred within the revert window after a resolve.",
		}),
		recoveryTracking: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "k8s_event_watcher_recovery_tracking",
			Help: "Current number of bound incidents the recovery tracker is watching for clearance.",
		}),
		recoveryDrops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_recovery_drops_total",
			Help: "Total resolved signals dropped instead of injected, by cause (unknown_session: binding lost, e.g. restart without --dedup-persist).",
		}, []string{"cause"}),
		stormsFormed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "k8s_event_watcher_storms_formed_total",
			Help: "Total kind=storm incidents opened by blast-radius correlation (§7.5).",
		}),
		stormsResolved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "k8s_event_watcher_storms_resolved_total",
			Help: "Total storms resolved because every member incident cleared (§7.4 + §7.5).",
		}),
		stormsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "k8s_event_watcher_storms_active",
			Help: "Currently open (unresolved) storms.",
		}),
		stormMembers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_storm_members_total",
			Help: "Total incidents folded into storms, by how they joined (suppressed: per-incident session never opened; superseded: pre-storm session pointed at the storm; attached: late arrival).",
		}, []string{"kind"}),
		watchboardEntries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_watchboard_entries_total",
			Help: "Total warning-class signals buffered onto the shared watchboard digest (§7.7), by signal kind.",
		}, []string{"kind"}),
		watchboardDigests: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "k8s_event_watcher_watchboard_digests_total",
			Help: "Total kind=watchboard.digest injects flushed to the watchboard session.",
		}),
		watchboardRotations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "k8s_event_watcher_watchboard_rotations_total",
			Help: "Total size-based watchboard session rotations (§15 Q2): a fresh session opened after --watchboard-rotate digest injects.",
		}),
		watchboardBuffered: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "k8s_event_watcher_watchboard_buffered",
			Help: "Warning-class signals currently buffered awaiting the next watchboard digest flush.",
		}),
		infoDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "k8s_event_watcher_info_dropped_total",
			Help: "Total info-severity signals counted and dropped by §7.7 routing (stored-only class; the raw store lands in M3), by signal kind.",
		}, []string{"kind"}),
	}
	reg.MustRegister(
		m.eventsSeen,
		m.eventsInjected,
		m.eventsDedupSuppress,
		m.injectErrors,
		m.sessionCreates,
		m.activeIncidents,
		m.recoveriesObserved,
		m.recoveriesReverted,
		m.recoveryTracking,
		m.recoveryDrops,
		m.stormsFormed,
		m.stormsResolved,
		m.stormsActive,
		m.stormMembers,
		m.watchboardEntries,
		m.watchboardDigests,
		m.watchboardRotations,
		m.watchboardBuffered,
		m.infoDropped,
	)
	return m
}

// serveMetrics starts a small HTTP server exposing /metrics on addr.
// Blocks until ctx is cancelled; returns any listener error. Callers
// start it in a goroutine and use ctx cancellation for shutdown.
//
// When addr == "" the server is skipped entirely (metrics still get
// collected in-process; just not exposed). Useful for tests + tiny
// deployments that don't have a Prometheus scraper.
func serveMetrics(ctx context.Context, addr string, m *metrics) error {
	if addr == "" {
		<-ctx.Done()
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	// Simple liveness probe — no /metrics dependency, so K8s can
	// use it as a livenessProbe without conflating "prometheus is
	// scraping" with "the process is up."
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	// Bind synchronously so port-in-use fails fast; then serve
	// in a goroutine and let ctx cancellation drive shutdown.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
