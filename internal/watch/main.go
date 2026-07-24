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

// Command k8s-event-watcher is the v2.6 semi-autonomous-triage sidecar.
// It watches Kubernetes Events via a client-go informer, filters to a
// configured allow-list of Event.Reason values, dedupes duplicates
// within a rolling window, and POSTs matched events to a core-agent
// daemon's per-incident session endpoint. See
// docs/k8s-event-agent-design.md for the full design.
package watch

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-steer/core-agent/v2/pkg/telemetry"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
)

// flags is the CLI-shaped config, parsed once in main and threaded
// to the components. All fields match --flag-name in the design
// doc's "Sidecar CLI" section.
type flags struct {
	daemonURL         string
	tokenEnv          string
	mode              string
	targetSession     string
	owner             string
	reasons           string
	namespaces        string
	excludeNamespaces string
	dedupWindow       time.Duration
	dedupPersist      string
	unhealthyMinCount int
	inCluster         bool
	kubeconfig        string
	clusterName       string
	logLevel          string
	dryRun            bool
	metricsAddr       string
	snapshotInterval  time.Duration
	otelExporter      string
}

// parseFlags reads argv into flags. Returns nil on --help (main
// exits 0). Any other parse error surfaces as an error so main can
// report + exit 2 (POSIX convention for CLI misuse).
func parseFlags(args []string) (*flags, error) {
	fs := flag.NewFlagSet("k8s-event-watcher", flag.ContinueOnError)
	f := &flags{}

	// Required.
	fs.StringVar(&f.daemonURL, "daemon-url", "", "Base URL of the core-agent daemon (http://... or https://...). Required.")
	fs.StringVar(&f.tokenEnv, "token-env", "", "Env var name holding the bearer token for the daemon. Required.")

	// Session routing.
	fs.StringVar(&f.mode, "mode", "per-incident", "Session routing mode: per-incident (create per (uid,reason)) or shared (all to --target-session).")
	fs.StringVar(&f.targetSession, "target-session", "", "Required when --mode=shared: SessionID to post all injects to.")
	fs.StringVar(&f.owner, "owner", "", "X-Asserted-Caller value for POST /sessions in per-incident mode. Sidecar must be in daemon's proxy_identities.")

	// Event filtering.
	fs.StringVar(&f.reasons, "reason", "", "Comma-separated allow-list of Event.Reason values. Empty = shipped default set.")
	fs.StringVar(&f.namespaces, "namespace", "", "Comma-separated allow-list of namespaces. Empty = all namespaces.")
	fs.StringVar(&f.excludeNamespaces, "exclude-namespace", "", "Comma-separated deny-list of namespaces.")

	// Dedup.
	fs.DurationVar(&f.dedupWindow, "dedup-window", 5*time.Minute, "Rolling window for (uid,reason) dedup.")
	fs.StringVar(&f.dedupPersist, "dedup-persist", "", "Optional path to persist dedup cache across sidecar restart.")
	fs.IntVar(&f.unhealthyMinCount, "unhealthy-min-count", 3, "Require this many consecutive Unhealthy events before firing.")

	// Kubernetes client.
	fs.BoolVar(&f.inCluster, "in-cluster", false, "Use in-cluster service account credentials. Auto-detected inside a pod.")
	fs.StringVar(&f.kubeconfig, "kubeconfig", "", "Explicit kubeconfig path. Used outside a pod.")
	fs.StringVar(&f.clusterName, "cluster-name", "", "Human-readable cluster name included in every inject payload.")

	// Operational.
	fs.StringVar(&f.logLevel, "log-level", "info", "One of: debug, info, warn, error.")
	fs.BoolVar(&f.dryRun, "dry-run", false, "Print inject payloads to stdout without calling the daemon.")
	fs.StringVar(&f.metricsAddr, "metrics-addr", "", "Prometheus /metrics + /healthz listener address (host:port). Empty = disabled.")
	fs.DurationVar(&f.snapshotInterval, "snapshot-interval", 30*time.Second, "How often to persist the dedup cache when --dedup-persist is set. 0 = only on shutdown.")

	// OpenTelemetry — mirrors the daemon's config.otel.exporter shape.
	// When "otlp", honors standard OTEL_EXPORTER_OTLP_ENDPOINT env vars.
	// The W3C traceparent propagator is registered globally regardless
	// of this setting so outbound POSTs carry trace context to a
	// tracing-enabled daemon even when the watcher itself isn't
	// exporting spans locally (rare but useful during phased rollouts).
	fs.StringVar(&f.otelExporter, "otel-exporter", "none", "OpenTelemetry span exporter: none | console | otlp. See docs/otel.md.")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return f, nil
}

// validate checks flag combinations after parse. Called once from
// main so misconfig fails before any network / API touching.
func (f *flags) validate() error {
	if !f.dryRun && f.daemonURL == "" {
		return errors.New("--daemon-url is required (unless --dry-run)")
	}
	if !f.dryRun && f.tokenEnv == "" {
		return errors.New("--token-env is required (unless --dry-run)")
	}
	if strings.HasSuffix(f.daemonURL, "/") {
		return fmt.Errorf("--daemon-url must not end with '/' (got %q)", f.daemonURL)
	}
	switch f.mode {
	case "per-incident":
		if f.dryRun {
			return nil
		}
		if f.owner == "" {
			return errors.New("--owner is required in per-incident mode (must match a proxy identity in the daemon's users.json)")
		}
	case "shared":
		if f.targetSession == "" {
			return errors.New("--target-session is required in shared mode")
		}
	default:
		return fmt.Errorf("--mode must be per-incident or shared (got %q)", f.mode)
	}
	if f.dedupWindow <= 0 {
		return errors.New("--dedup-window must be > 0")
	}
	if f.snapshotInterval < 0 {
		return errors.New("--snapshot-interval must be >= 0")
	}
	return nil
}

// splitCSV parses a comma-separated flag value. Empty strings after
// split are dropped; whitespace around values trimmed.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// dispatcher is the pipeline that ties filter → dedup → injector +
// metrics for one signal. Sources (pkg/sources) feed it through
// DispatchSignal; storm correlation, severity routing, and
// enrichment slot in here as they land (§7.1).
type dispatcher struct {
	filter    *engine.Filter
	dedup     *engine.DedupCache
	injector  *inject.Injector
	metrics   *metrics
	cluster   string
	mode      string // "per-incident" or "shared"
	targetSid string // for shared mode
	dryRun    bool
	// injectLock serializes per-(app, sid) session creation +
	// injects so two rapid-fire events for the same key don't
	// both call CreateSession. Coarse-grained; a per-key map of
	// mutexes would let concurrent keys parallelize but this
	// path is nowhere near a bottleneck.
	injectLock sync.Mutex
}

// Dispatch is the TriageEvent-shaped entry point, kept from M0: it
// wraps the event in the kind=k8s-event Signal the k8s-events source
// would emit and forwards to DispatchSignal. The M0 contract tests
// (wire shape, dispatcher logs) pin this path; keep it until every
// caller speaks Signal.
func (d *dispatcher) Dispatch(ctx context.Context, ev engine.TriageEvent) {
	d.DispatchSignal(ctx, engine.Signal{
		Kind:        engine.KindK8sEvent,
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityCritical,
		TriageEvent: ev,
	})
}

// DispatchSignal is the pipeline entry point for signals emitted by
// sources (§7.1: filter → dedup → inject; the correlation / routing /
// enrichment stages land in later M2 changes).
//
// For Kind=k8s-event the inject payload is the frozen
// pkg/inject.Payload, byte-identical to the M0 watcher's — the
// Signal-only fields (severity, fingerprint, source, zone) are
// carried in-process but not serialized for that kind (§8: existing
// fields keep their exact names; new fields ship with new kinds).
func (d *dispatcher) DispatchSignal(ctx context.Context, sig engine.Signal) {
	// Stamp deployment identity + derived fields sources leave
	// blank: sources don't know which cluster they run in (§7.2),
	// and the fingerprint (§8) needs the canonicalized reason-class
	// + zone, so it is computed here, once, for every source.
	if sig.Cluster == "" {
		sig.Cluster = d.cluster
	}
	if sig.Source == "" {
		sig.Source = engine.SourceSentinel
	}
	if sig.Fingerprint == "" {
		sig.Fingerprint = engine.Fingerprint(sig.Kind, engine.CanonicalReason(sig.Key.Reason), sig.KindOfObject, sig.Zone)
	}
	d.metrics.eventsSeen.WithLabelValues(sig.Key.Reason, sig.Namespace).Inc()
	if !d.filter.Accept(sig) {
		return
	}
	result := d.dedup.Observe(sig.Key, sig.LastSeen)
	d.metrics.activeIncidents.Set(float64(d.dedup.Len()))
	if result.Kind == engine.DedupDuplicate {
		d.metrics.eventsDedupSuppress.WithLabelValues(sig.Key.Reason, sig.Namespace).Inc()
		// Info-level log: the operator asked "is the watcher seeing
		// events?" and today the answer was "yes when things break,
		// silent when things work" — this line makes suppressed
		// duplicates visible so the operator can distinguish
		// "watcher missed the event" from "watcher saw it and
		// correctly deduped". Bound is the dedup window (set via
		// --dedup-window, default 5m); result.Count is the running
		// hit count for this key within the current window.
		log.Printf("dedup %s pod=%s/%s (count=%d, window active)",
			sig.Key.Reason, sig.Namespace, sig.Name, result.Count)
		return
	}
	// New incident: create or reuse a session, then inject.
	d.injectLock.Lock()
	defer d.injectLock.Unlock()
	sid := d.targetSid
	if d.mode == "per-incident" && !d.dryRun {
		newSid, err := d.injector.CreateSession(ctx)
		if err != nil {
			log.Printf("dispatcher: create session for %s/%s: %v", sig.Namespace, sig.Name, err)
			d.metrics.sessionCreates.WithLabelValues("error").Inc()
			d.metrics.injectErrors.WithLabelValues(sig.Key.Reason, "session_create").Inc()
			return
		}
		sid = newSid
		d.metrics.sessionCreates.WithLabelValues("ok").Inc()
		d.dedup.BindSession(sig.Key, sid)
	}
	payload := inject.Payload{
		Kind:         sig.Kind,
		Reason:       sig.Key.Reason,
		Namespace:    sig.Namespace,
		KindOfObject: sig.KindOfObject,
		Name:         sig.Name,
		Container:    sig.Container,
		UID:          sig.Key.UID,
		Message:      sig.Message,
		Count:        result.Count,
		FirstSeen:    sig.FirstSeen,
		LastSeen:     sig.LastSeen,
		Cluster:      sig.Cluster,
		Context: inject.PayloadContext{
			ControllerRef: sig.ControllerRef,
			Node:          sig.Node,
			Labels:        sig.Labels,
		},
	}
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
		d.metrics.eventsInjected.WithLabelValues(sig.Key.Reason, sig.Namespace).Inc()
		log.Printf("would-fire %s pod=%s/%s (sid=%s, mode=%s, dry-run)",
			sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
		return
	}
	if err := d.injector.Inject(ctx, sid, payload); err != nil {
		log.Printf("dispatcher: inject for %s/%s (sid=%s): %v", sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(sig.Key.Reason, "inject").Inc()
		return
	}
	d.metrics.eventsInjected.WithLabelValues(sig.Key.Reason, sig.Namespace).Inc()
	// Info-level log: the successful-inject case was silent before
	// #212 — operators had to correlate client-go informer warnings
	// with daemon session-list dumps to infer whether the watcher
	// was firing at all. Making success visible turns "is the
	// sidecar working?" into a grep. sid is traceable in the daemon's
	// own logs / /sessions API so cross-container reconstruction of
	// an incident is a single traceID-style filter.
	log.Printf("fire %s pod=%s/%s → sid=%s (mode=%s)",
		sig.Key.Reason, sig.Namespace, sig.Name, sid, d.mode)
}

// Main is the `lookout watch` entry point; argv is the argument list
// after the subcommand name. Everything below it is the k8s-event-watcher
// moved verbatim from core-agent — flags, behavior, and exit codes are
// preserved exactly (M0 contract: existing deployments swap images with
// zero config change), including the standalone binary's 0/1 exit-code
// convention and its "k8s-event-watcher:" stderr prefix.
func Main(argv []string) int {
	if err := realMain(argv); err != nil {
		fmt.Fprintln(os.Stderr, "k8s-event-watcher:", err)
		return 1
	}
	return 0
}

func realMain(argv []string) error {
	f, err := parseFlags(argv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}

	// OpenTelemetry init. Registers the W3C traceparent propagator
	// globally (so otelhttp-wrapped outbound POSTs carry trace
	// context to the daemon) and, when --otel-exporter=console|otlp
	// is set, wires the exporter so this watcher's own spans (fire /
	// dedup / metrics-server) get shipped too. See #217.
	otelCtx := context.Background()
	otelShutdown, err := telemetry.Setup(otelCtx, f.otelExporter)
	if err != nil {
		return fmt.Errorf("telemetry setup: %w", err)
	}
	defer func() { _ = otelShutdown(context.Background()) }()

	// Resolve bearer token from env (unless dry-run).
	var token string
	if !f.dryRun {
		token = os.Getenv(f.tokenEnv)
		if token == "" {
			return fmt.Errorf("bearer token env var %s is empty", f.tokenEnv)
		}
	}

	// Build components.
	filterCfg := engine.NewFilterConfig(splitCSV(f.reasons), splitCSV(f.namespaces), splitCSV(f.excludeNamespaces), f.unhealthyMinCount)
	filter := engine.NewFilter(filterCfg)

	dedup, err := engine.NewDedupCache(f.dedupWindow, f.dedupPersist)
	if err != nil {
		return fmt.Errorf("dedup cache: %w", err)
	}

	m := newMetrics()

	var inj *inject.Injector
	if !f.dryRun {
		inj, err = inject.NewInjector(inject.Config{
			DaemonURL:      f.daemonURL,
			BearerToken:    token,
			AssertedCaller: f.owner,
		})
		if err != nil {
			return fmt.Errorf("injector: %w", err)
		}
	}

	disp := &dispatcher{
		filter:    filter,
		dedup:     dedup,
		injector:  inj,
		metrics:   m,
		cluster:   f.clusterName,
		mode:      f.mode,
		targetSid: f.targetSession,
		dryRun:    f.dryRun,
	}

	// Root ctx cancelled on SIGINT / SIGTERM for graceful shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start the metrics HTTP server (blocks on ctx in-goroutine).
	go func() {
		if err := serveMetrics(ctx, f.metricsAddr, m); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()

	// Start the periodic dedup-snapshot ticker if configured.
	if f.dedupPersist != "" && f.snapshotInterval > 0 {
		go runSnapshotLoop(ctx, dedup, f.snapshotInterval)
	}

	// Build the kube client (skip in dry-run to avoid needing a
	// real cluster for CI / local exploratory runs).
	if f.dryRun {
		log.Printf("k8s-event-watcher: --dry-run: skipping kube client; would watch cluster %q", f.clusterName)
		<-ctx.Done()
		if err := dedup.Snapshot(); err != nil {
			log.Printf("dedup snapshot on shutdown: %v", err)
		}
		return nil
	}
	client, err := kube.BuildClient(kube.Options{InCluster: f.inCluster, Kubeconfig: f.kubeconfig})
	if err != nil {
		return err
	}

	// Signal sources (§7.2). One source today — the M0 event watcher
	// refactored into pkg/sources/k8sevents — but registered, probed,
	// and run through the generic interface the rest of M2 plugs into.
	// The flag surface deliberately gains nothing here: source
	// enablement config lands with the second source.
	registry := sources.NewRegistry()
	if err := registry.Register(k8sevents.New(client, 0)); err != nil {
		return err
	}

	// §11: verify each source's declared RBAC before watching
	// anything — a deployment whose ServiceAccount can't support a
	// source fails loudly here, naming the source and the missing
	// permission, never a silently empty watch.
	if err := sources.Probe(ctx, sources.NewAccessReviewer(client), registry.All()...); err != nil {
		return err
	}

	log.Printf("k8s-event-watcher: starting on cluster %q → daemon %s (mode=%s, owner=%s)",
		f.clusterName, f.daemonURL, f.mode, f.owner)
	err = sources.RunAll(ctx, registry.All(), func(sig engine.Signal) {
		disp.DispatchSignal(ctx, sig)
	})
	// Final snapshot on shutdown so any un-persisted dedup state
	// isn't lost. Best-effort — failure is logged, not fatal.
	if snapErr := dedup.Snapshot(); snapErr != nil {
		log.Printf("dedup snapshot on shutdown: %v", snapErr)
	}
	return err
}

// runSnapshotLoop persists the dedup cache to disk on an interval
// so a sidecar crash doesn't lose more than interval seconds of
// state. Exits when ctx is cancelled.
func runSnapshotLoop(ctx context.Context, cache *engine.DedupCache, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := cache.Snapshot(); err != nil {
				log.Printf("dedup snapshot: %v", err)
			}
		}
	}
}
