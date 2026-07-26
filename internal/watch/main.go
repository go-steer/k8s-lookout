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
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/go-steer/core-agent/v2/pkg/telemetry"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/degradation"
	"github.com/go-steer/k8s-lookout/pkg/sources/expiry"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// flags is the CLI-shaped config, parsed once in main and threaded
// to the components. All fields match --flag-name in the design
// doc's "Sidecar CLI" section.
type flags struct {
	daemonURL             string
	tokenEnv              string
	mode                  string
	targetSession         string
	owner                 string
	reasons               string
	namespaces            string
	excludeNamespaces     string
	sources               string
	rolloutObserve        time.Duration
	saturationInterval    time.Duration
	saturationWindow      time.Duration
	saturationWarn        time.Duration
	degradationWindow     time.Duration
	degradationDrop       float64
	expiryInterval        time.Duration
	expiryWarn            time.Duration
	expiryNamespaces      string
	dedupWindow           time.Duration
	dedupPersist          string
	unhealthyMinCount     int
	recoveryStableFor     time.Duration
	storm                 bool
	stormWindow           time.Duration
	stormMin              int
	severity              severityFlag
	watchboardBatch       int
	watchboardFlush       time.Duration
	watchboardRotate      int
	enrich                string
	enrichCap             int
	enrichLogLines        int
	enrichTimeout         time.Duration
	store                 string
	storeTTL              time.Duration
	storeMaxMB            int
	graphSnapshotInterval time.Duration
	// severityOverrides is the parsed --severity map, populated by
	// validate().
	severityOverrides map[string]engine.Severity
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

	// Signal sources (§7.2: sources are individually enabled).
	// ADDITIVE flag: the default is exactly the M0 surface, so
	// existing deployments keep byte-identical behavior without
	// touching their config.
	fs.StringVar(&f.sources, "sources", k8sevents.Name, "Comma-separated signal sources to enable: k8s-events, object-state, rollout, saturation, degradation, expiry. Default preserves the M0 watcher surface (k8s-events only).")

	// Rollout source thresholds (§7.2 row 3). ADDITIVE flag; only
	// meaningful with --sources=...,rollout.
	fs.DurationVar(&f.rolloutObserve, "rollout-observe", 3*time.Minute, "How long a new revision must make zero ready-count progress (while the old revision stays healthy) before rollout.stall fires. Fired well before progressDeadlineSeconds.")

	// Saturation source knobs (§7.2 row 4). ADDITIVE flags; only
	// meaningful with --sources=...,saturation.
	fs.DurationVar(&f.saturationInterval, "saturation-interval", 30*time.Second, "Sampling interval for the saturation source (metrics.k8s.io + kubelet volume stats).")
	fs.DurationVar(&f.saturationWindow, "saturation-window", 90*time.Minute, "Regression window for saturation forecasts; a forecast needs samples spanning at least half of it (the §8 linear-<window>-window confidence basis).")
	fs.DurationVar(&f.saturationWarn, "saturation-warn", 60*time.Minute, "Forecast ETA below which saturation.forecast fires at severity warning (critical fires below 15m); clearance requires the ETA to recede beyond 2x this threshold.")

	// Degradation source thresholds (§7.2 row 5). ADDITIVE flags; only
	// meaningful with --sources=…,degradation.
	fs.DurationVar(&f.degradationWindow, "degradation-window", 15*time.Minute, "Trend window for the degradation source's ready-ratio series and probe-flap counting. Must be > 0.")
	fs.Float64Var(&f.degradationDrop, "degradation-drop", 0.3, "Minimum ready-ratio decline from window start (with >= 2 distinct downward steps) that fires degradation.capacity. Must be in (0, 1].")

	// Expiry source thresholds (§7.2 row 6). ADDITIVE flags; only
	// meaningful with --sources=…,expiry. The critical threshold (72h)
	// is design-fixed, not a flag.
	fs.DurationVar(&f.expiryInterval, "expiry-interval", time.Hour, "Interval between expiry scans (periodic paged LISTs — deliberately no Secret informer). Must be > 0.")
	fs.DurationVar(&f.expiryWarn, "expiry-warn", 336*time.Hour, "Warning threshold for expiry.warning: certificates with notAfter inside this window fire at warning severity (critical at the design-fixed 72h). Must be >= 72h.")
	fs.StringVar(&f.expiryNamespaces, "expiry-namespaces", "", "Comma-separated namespaces the expiry scan LISTs secrets/serviceaccounts/Certificates in. Empty = all namespaces. Scopes the sensitive secrets-list grant (§11) — the startup RBAC probe verifies exactly this scope.")

	// Dedup.
	fs.DurationVar(&f.dedupWindow, "dedup-window", 5*time.Minute, "Rolling window for (uid,reason) dedup.")
	fs.StringVar(&f.dedupPersist, "dedup-persist", "", "Optional path to persist dedup cache across sidecar restart.")
	fs.IntVar(&f.unhealthyMinCount, "unhealthy-min-count", 3, "Require this many consecutive Unhealthy events before firing.")

	// Recovery injects (§7.4).
	fs.DurationVar(&f.recoveryStableFor, "recovery-stable-for", 5*time.Minute, "How long a cleared symptom must stay clear before kind=resolved is injected into the incident's session; recurrence within this window after a resolve fires kind=resolved.reverted. 0 disables recovery tracking.")

	// Storm correlation (§7.5). ADDITIVE and default-OFF in this
	// change: the topology-graph informers need RBAC (pods, nodes,
	// apps/replicasets list+watch) that deployments running the M0
	// ClusterRole may lack, and — unlike the recovery observer —
	// correlation is opted into explicitly, so a missing grant is a
	// loud startup error rather than a silent degrade. Flipping the
	// default is an M2-exit decision.
	fs.BoolVar(&f.storm, "storm", false, "Enable storm correlation (§7.5): group new incidents sharing a blast-radius key (nearest common topology ancestor) into one kind=storm session. Requires pods/nodes/replicasets list+watch RBAC for the graph informers (verified loudly at startup).")
	fs.DurationVar(&f.stormWindow, "storm-window", engine.DefaultStormWindow, "Second-level correlation window for storm formation. 0 disables correlation even with --storm.")
	fs.IntVar(&f.stormMin, "storm-min", engine.DefaultStormMin, "Minimum incidents sharing a blast-radius key within --storm-window to form a storm. Must be >= 2.")

	// Severity routing (§7.7). ADDITIVE flags: with no --severity
	// overrides the routes follow the source-stamped defaults, and the
	// M0 surface (k8s-events only, all critical) behaves identically.
	// The watchboard flags only matter in per-incident mode — in
	// --mode=shared ALL severities keep routing to --target-session
	// (frozen contract) and the watchboard machinery is disabled.
	fs.Var(&f.severity, "severity", "Per-kind severity override(s): kind=level[,kind=level...] with level one of critical|warning|info. Repeatable and additive; overrides the source-stamped §7.7 default for that kind. Each kind may appear at most once.")
	fs.IntVar(&f.watchboardBatch, "watchboard-batch", 5, "Buffered warning-class signals that trigger a watchboard digest flush (per-incident mode; §7.7). Must be >= 1.")
	fs.DurationVar(&f.watchboardFlush, "watchboard-flush", 60*time.Second, "Maximum age of a buffered warning before the watchboard digest flushes regardless of batch size. Must be > 0.")
	fs.IntVar(&f.watchboardRotate, "watchboard-rotate", 200, "Digest injects per watchboard session before size-based rotation (§15 Q2) opens a fresh session. Must be >= 1.")

	// Enrichment (§7.6). ADDITIVE flags with the design's normative
	// defaults: critical incidents get the in-process bundle attached
	// to their initial inject; the enrichment field is omitempty, so
	// deployments that set --enrich=off (or run --mode=shared) keep
	// byte-identical payloads.
	fs.StringVar(&f.enrich, "enrich", "critical", "Which severities get §7.6 enrichment on their per-incident session's initial inject: critical (default), warning (critical+warning), or off.")
	fs.IntVar(&f.enrichCap, "enrich-cap", 16384, "Byte budget for the attached enrichment bundle (§15 Q3: fixed budget, revisited with M2 telemetry). Truncation happens at section boundaries; dropped sections become overflow trailers naming the lookout command that reproduces them.")
	fs.IntVar(&f.enrichLogLines, "enrich-log-lines", 200, "Log tail per container stream distilled into the enrichment bundle's logs section. Must be >= 1.")
	fs.DurationVar(&f.enrichTimeout, "enrich-timeout", 5*time.Second, "Hard wall-clock budget for one enrichment run; on expiry the inject fires with whatever sections completed plus enrichment_error trailers. Must be > 0.")

	// Occurrence store (§9.1). ADDITIVE flags, default OFF: with no
	// --store the sentinel behaves exactly as before (info-severity
	// signals are counted and dropped). The path is ALWAYS explicit —
	// there is no default location (and never one under $HOME; the
	// store belongs on the same volume as --dedup-persist).
	fs.StringVar(&f.store, "store", "", "Path to the sentinel-local SQLite occurrence store (§9.1), e.g. /var/lib/lookout/lookout.db — put it on the --dedup-persist volume. Every emitted signal is recorded with its routing outcome; info-severity signals are persisted instead of dropped. Empty (default) disables the store.")
	fs.DurationVar(&f.storeTTL, "store-ttl", 720*time.Hour, "Retention for stored occurrences (§9.1 default 30 days); the prune loop deletes older rows. Must be > 0.")
	fs.IntVar(&f.storeMaxMB, "store-max-mb", 512, "Size bound for the occurrence store in MiB; when exceeded, the oldest occurrences are pruned first (loudly). Must be >= 1.")

	// Graph history (§6.6). ADDITIVE flag: history turns on only when
	// BOTH --store and the graph feed (--storm) are active — without a
	// resident graph there is nothing to snapshot, and without a store
	// there is nowhere to put it. Point-in-time queries (--at) and
	// `triage changes` read what this writes.
	fs.DurationVar(&f.graphSnapshotInterval, "graph-snapshot-interval", 5*time.Minute, "How often to persist a compressed topology snapshot to --store (§6.6; the per-delta change log is written continuously). Effective only with --store AND --storm (the graph feed). Must be > 0.")

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
	// Sources are validated before the mode switch's dry-run early
	// return: a typo'd source name is a config error in every mode.
	names := splitCSV(f.sources)
	if len(names) == 0 {
		return fmt.Errorf("--sources must enable at least one source (known: %s)", strings.Join(knownSources, ", "))
	}
	for _, name := range names {
		if !slices.Contains(knownSources, name) {
			return fmt.Errorf("--sources: unknown source %q (known: %s)", name, strings.Join(knownSources, ", "))
		}
	}
	// Rollout / saturation bounds (§7.2 rows 3–4): config errors in
	// every mode, like --sources itself, even when the sources are
	// disabled — a nonsensical value is a typo worth failing.
	if f.rolloutObserve <= 0 {
		return errors.New("--rollout-observe must be > 0")
	}
	if f.saturationInterval <= 0 {
		return errors.New("--saturation-interval must be > 0")
	}
	if f.saturationWindow <= f.saturationInterval {
		return errors.New("--saturation-window must be > --saturation-interval (the regression needs a window of samples)")
	}
	if f.saturationWarn <= 0 {
		return errors.New("--saturation-warn must be > 0")
	}
	// Degradation / expiry thresholds (§7.2 rows 5–6): config errors in
	// every mode, like the storm bounds, even when the sources are
	// disabled — a nonsensical value is a typo worth failing.
	if f.degradationWindow <= 0 {
		return errors.New("--degradation-window must be > 0")
	}
	if f.degradationDrop <= 0 || f.degradationDrop > 1 {
		return errors.New("--degradation-drop must be in (0, 1]")
	}
	if f.expiryInterval <= 0 {
		return errors.New("--expiry-interval must be > 0")
	}
	if f.expiryWarn < expiry.CriticalWindow {
		return fmt.Errorf("--expiry-warn must be >= the design-fixed critical threshold (%s)", expiry.CriticalWindow)
	}
	// Storm bounds are validated before the mode switch's dry-run
	// early return, like --sources: a nonsensical value is a config
	// error in every mode.
	if f.stormWindow < 0 {
		return errors.New("--storm-window must be >= 0 (0 disables storm correlation)")
	}
	if f.stormMin < 2 {
		return errors.New("--storm-min must be >= 2 (a storm of one is an incident)")
	}
	// Severity routing (§7.7): overrides and watchboard bounds are
	// config errors in every mode, like --sources and the storm
	// bounds. (The watchboard itself only runs in per-incident mode.)
	overrides, err := engine.ParseSeverityOverrides(f.severity.values)
	if err != nil {
		return fmt.Errorf("--severity: %w", err)
	}
	f.severityOverrides = overrides
	if f.watchboardBatch < 1 {
		return errors.New("--watchboard-batch must be >= 1")
	}
	if f.watchboardFlush <= 0 {
		return errors.New("--watchboard-flush must be > 0")
	}
	if f.watchboardRotate < 1 {
		return errors.New("--watchboard-rotate must be >= 1")
	}
	// Enrichment (§7.6): config errors in every mode, like the storm
	// and watchboard bounds. (The stage itself only runs in
	// per-incident mode.)
	if !enrichPolicies[f.enrich] {
		return fmt.Errorf("--enrich must be critical, warning, or off (got %q)", f.enrich)
	}
	if f.enrichCap < 1 {
		return errors.New("--enrich-cap must be >= 1")
	}
	if f.enrichLogLines < 1 {
		return errors.New("--enrich-log-lines must be >= 1")
	}
	if f.enrichTimeout <= 0 {
		return errors.New("--enrich-timeout must be > 0")
	}
	// Occurrence store (§9.1): bounds are config errors in every mode,
	// like the storm / watchboard / enrichment bounds, even when
	// --store is unset — a nonsensical value is a typo worth failing.
	if f.storeTTL <= 0 {
		return errors.New("--store-ttl must be > 0")
	}
	if f.storeMaxMB < 1 {
		return errors.New("--store-max-mb must be >= 1")
	}
	if f.graphSnapshotInterval <= 0 {
		return errors.New("--graph-snapshot-interval must be > 0")
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
	if f.recoveryStableFor < 0 {
		return errors.New("--recovery-stable-for must be >= 0 (0 disables recovery tracking)")
	}
	if f.snapshotInterval < 0 {
		return errors.New("--snapshot-interval must be >= 0")
	}
	return nil
}

// stormEnabled reports whether §7.5 storm correlation is on: the
// explicit --storm opt-in AND a non-zero correlation window.
func (f *flags) stormEnabled() bool { return f.storm && f.stormWindow > 0 }

// knownSources are the --sources names, in the §7.2 table order.
var knownSources = []string{k8sevents.Name, objectstate.Name, rollout.Name, saturation.Name, degradation.Name, expiry.Name}

// sourceEnabled reports whether --sources names the given source.
func (f *flags) sourceEnabled(name string) bool {
	return slices.Contains(splitCSV(f.sources), name)
}

// severityFlag is the repeatable --severity flag: each occurrence is
// kept verbatim and parsed (additively) by
// engine.ParseSeverityOverrides in validate().
type severityFlag struct {
	values []string
}

func (v *severityFlag) String() string { return strings.Join(v.values, ",") }

func (v *severityFlag) Set(s string) error {
	v.values = append(v.values, s)
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
	// tracker, when non-nil, is the §7.4 recovery tracker: every new
	// incident this dispatcher binds is handed to it for clearance
	// watching, and the resolved signals it emits come back through
	// DispatchSignal. Nil when recovery is disabled
	// (--recovery-stable-for=0, dry-run, or missing pods RBAC).
	tracker *engine.RecoveryTracker
	// routing, when non-nil, is the §7.7 severity-routing policy:
	// per-kind severity defaults come stamped by the sources; config
	// overrides them via --severity. Nil in unit tests that predate
	// severity routing — a nil policy skips the routing stage
	// entirely, preserving the pre-§7.7 pipeline byte-for-byte.
	routing *engine.RoutingPolicy
	// board, when non-nil, is the §7.7 shared watchboard the warning
	// class batches into. Only ever set in per-incident mode: in
	// --mode=shared ALL severities keep routing to --target-session
	// (the frozen shared-mode contract) and the watchboard machinery
	// is disabled.
	board *watchboard
	// storm, when non-nil, is the §7.5 correlation stage sitting
	// between dedup and session creation: new incidents pass through
	// it and may be folded into a kind=storm session instead of
	// opening their own. Nil when storm correlation is disabled
	// (--storm absent — the default).
	storm *engine.StormCorrelator
	// store, when non-nil, is the §9.1 raw-occurrence store: every
	// signal that survives the filter is recorded post-dedup with the
	// routing outcome it received. Nil when --store is unset — all
	// Store methods are nil-safe no-ops, so no call site branches.
	// The store is telemetry, never control flow: nothing in this
	// dispatcher reads it back.
	store *store.Store
	// enrich, when non-nil, is the §7.6 enrichment stage: new
	// per-incident (and storm) sessions get the in-process bundle
	// attached to their initial inject. Nil when disabled
	// (--enrich=off, --mode=shared, dry-run, or unit tests predating
	// §7.6 — a nil enricher keeps every payload byte-identical).
	enrich *enricher
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
	// Resolved / resolved.reverted (§7.4) are outcome records for an
	// EXISTING incident: they bypass filter + dedup (they are not new
	// incidents) and route as followups into the incident's bound
	// session. Their fingerprint is the original incident's — never
	// re-stamped here.
	if sig.Kind == engine.KindResolved || sig.Kind == engine.KindResolvedReverted {
		d.dispatchResolved(ctx, sig)
		return
	}
	if sig.Fingerprint == "" {
		sig.Fingerprint = engine.Fingerprint(sig.Kind, engine.CanonicalReason(sig.Key.Reason), sig.KindOfObject, sig.Zone)
	}
	// Effective severity (§7.7): the source-stamped per-kind default,
	// unless config overrides the kind via --severity. Stamped before
	// the storm stage so a storm's max-member severity honors the
	// override too. A nil policy (pre-§7.7 unit tests) leaves the
	// source's stamp untouched.
	if d.routing != nil {
		sig.Severity = d.routing.Classify(sig)
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
		// §9.1: suppressed duplicates are still emitted signals — the
		// store keeps them (with the session the incident is bound to)
		// so lookback sees the true occurrence rate, not the deduped one.
		d.store.Record(sig, store.Outcome{Route: store.RouteSuppressed, SessionID: result.SessionID})
		return
	}
	// Severity routing, info class (§7.7): stored only per §9.1 —
	// with --store set the signal is persisted (route=info-stored) and
	// surfaced by read-path queries and digests; without it, counted +
	// logged and dropped, never silently ignored. The info_dropped
	// metric counts the class in BOTH cases (frozen name; it is the
	// "did not inject" count, store or no store). Placed after dedup
	// (so it counts incident records, not raw event volume) and before
	// storm correlation (info neither opens nor joins sessions, storms
	// included). Shared mode skips routing entirely: ALL severities go
	// to --target-session.
	if d.mode == "per-incident" && d.routing != nil && engine.RouteFor(sig.Severity) == engine.RouteStore {
		d.metrics.infoDropped.WithLabelValues(sig.Kind).Inc()
		if d.store != nil {
			d.store.Record(sig, store.Outcome{Route: store.RouteInfoStored})
			log.Printf("info-store %s %s/%s (severity=info: stored-only class; persisted to --store)",
				sig.Kind, sig.Namespace, sig.Name)
		} else {
			log.Printf("info-drop %s %s/%s (severity=info: stored-only class; set --store to persist — counted and dropped)",
				sig.Kind, sig.Namespace, sig.Name)
		}
		return
	}
	// New incident: correlate, then create or reuse a session and
	// inject. The lock serializes storm formation with session
	// creation so two racing incidents cannot both open the storm.
	d.injectLock.Lock()
	defer d.injectLock.Unlock()
	// Storm correlation (§7.5, pipeline position: after dedup,
	// before severity routing): a new incident may form a storm,
	// attach to an open one, or fall through per-incident.
	if d.storm != nil {
		switch v := d.storm.Observe(sig); v.Kind {
		case engine.StormFormed:
			d.stormFormed(ctx, sig, v)
			return
		case engine.StormAttached:
			d.stormAttached(ctx, sig, v)
			return
		}
	}
	// Severity routing, warning class (§7.7): batch into the shared
	// watchboard's rolling digest instead of opening a per-incident
	// session. AFTER the storm stage on purpose: a correlated burst
	// always opens a storm session regardless of member severity —
	// §7.5's whole point is ONE aggregate incident an agent works,
	// which a digest entry is not — so storm formation/attachment
	// bypasses warning routing (the returns above). Only in
	// per-incident mode: shared mode routes ALL severities to
	// --target-session unchanged.
	if d.mode == "per-incident" && d.board != nil && engine.RouteFor(sig.Severity) == engine.RouteWatchboard {
		// Recorded without a session on purpose: the watchboard's
		// session is created lazily at flush time and rotates — the
		// digest inject, not this occurrence row, is the session-bound
		// artifact.
		d.store.Record(sig, store.Outcome{Route: store.RouteWatchboard})
		d.board.Add(ctx, sig, result.Count)
		return
	}
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
		// BindIncident = BindSession + the identity the recovery
		// tracker needs to survive a restart (rides on dedup-persist).
		d.dedup.BindIncident(sig.Key, sid, sig.IncidentRef())
		if d.storm != nil {
			// Remember the session for a possible later supersede:
			// if this incident becomes a founding storm member, the
			// storm inject points its session at the storm's.
			d.storm.NoteMemberSession(sig.Key, sid)
		}
	}
	// Hand the bound incident to the recovery tracker (§7.4) so the
	// fix-verify loop closes into this session. Shared mode tracks
	// too — the outcome routes to the shared session.
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       sig.Key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
	// Enrichment (§7.6): pre-warm the session by attaching the
	// in-process bundle to the INITIAL inject. Per-incident mode only
	// (shared mode has no session creation to warm and keeps its
	// frozen contract); severity-gated by --enrich; hard-capped by
	// --enrich-timeout. Runs under injectLock deliberately — critical
	// incidents are rare, the budget is seconds, and enriching after
	// unlock would let a followup for the same key race ahead of the
	// session's first message. Errors never block the inject: they
	// ride inside the bundle as enrichment_error trailers.
	if d.enrich != nil && d.mode == "per-incident" && d.enrich.enabledFor(sig.Severity) {
		if bundleStr := d.enrich.Incident(ctx, sig); bundleStr != "" {
			sig.Enrichment = &engine.Enrichment{Bundle: bundleStr}
		}
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
	if sig.Enrichment != nil {
		payload.Enrichment = &inject.PayloadEnrichment{Bundle: sig.Enrichment.Bundle}
	}
	// §8 forecast: trend/countdown sources only; omitempty keeps every
	// reactive payload byte-identical (frozen wire pins unchanged).
	if sig.Forecast != nil {
		payload.Forecast = &inject.PayloadForecast{ETA: sig.Forecast.ETA, ConfidenceBasis: sig.Forecast.ConfidenceBasis}
	}
	// §9.1: record the routing DECISION (route=injected, with the
	// session it targets), not delivery success — inject transport
	// errors stay the injector's telemetry (inject_errors_total).
	// The recorded signal carries no enrichment (the store strips it).
	d.store.Record(sig, store.Outcome{Route: store.RouteInjected, SessionID: sid})
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

// dispatchResolved routes a §7.4 outcome record into the incident's
// bound session as a followup. The dedup cache's binding — not
// tracker-local state — is authoritative: if the binding is unknown
// (sentinel restarted without --dedup-persist, or the entry was LRU-
// evicted), the record is logged, counted, and dropped; we never
// open a fresh session just to say something is fixed.
func (d *dispatcher) dispatchResolved(ctx context.Context, sig engine.Signal) {
	if sig.Recovery == nil {
		log.Printf("recovery: %s signal for %s/%s missing Recovery attachment — dropping (programming error)",
			sig.Kind, sig.Namespace, sig.Name)
		return
	}
	// Storm bookkeeping first (§7.5): member clearance feeds the
	// storm's recovery — the LAST member to clear resolves the storm.
	// Recorded before the member's own routing so a lost binding
	// (dropped outcome below) still keeps storm accounting correct.
	var stormFinal *engine.StormInfo
	if d.storm != nil {
		switch sig.Kind {
		case engine.KindResolved:
			if info, done, ok := d.storm.MemberResolved(sig.Key); ok && done {
				stormFinal = &info
			}
		case engine.KindResolvedReverted:
			d.storm.MemberReverted(sig.Key)
		}
		defer func() {
			if stormFinal != nil {
				d.stormResolved(ctx, sig, *stormFinal)
			}
		}()
	}
	sid := d.targetSid
	if d.mode == "per-incident" {
		bound, ok := d.dedup.LookupSession(sig.Key)
		if !ok {
			d.metrics.recoveryDrops.WithLabelValues("unknown_session").Inc()
			log.Printf("recovery: no bound session for %s %s/%s (uid=%s) — dropping %s (restart without --dedup-persist, or entry evicted)",
				sig.Key.Reason, sig.Namespace, sig.Name, sig.Key.UID, sig.Kind)
			// The OUTCOME still happened even though the inject has
			// nowhere to go — keep it (NULL session) so §7.4 stability
			// windows and recommendation history stay complete.
			d.store.Record(sig, store.Outcome{Route: store.RouteResolved})
			return
		}
		sid = bound
	}
	// §9.1: resolved / resolved.reverted rows are what the stability-
	// window and recommendation-history queries join against.
	d.store.Record(sig, store.Outcome{Route: store.RouteResolved, SessionID: sid})
	payload := resolvedPayload(sig)
	if d.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
		d.countResolved(sig)
		return
	}
	if err := d.injector.InjectResolved(ctx, sid, payload); err != nil {
		log.Printf("recovery: inject %s for %s/%s (sid=%s): %v", sig.Kind, sig.Namespace, sig.Name, sid, err)
		d.metrics.injectErrors.WithLabelValues(sig.Key.Reason, "inject").Inc()
		return
	}
	d.countResolved(sig)
	log.Printf("%s %s pod=%s/%s → sid=%s (resolution=%s, cleared_after=%s, stable_for=%s)",
		sig.Kind, sig.Key.Reason, sig.Namespace, sig.Name, sid,
		sig.Recovery.Resolution, sig.Recovery.ClearedAfter, sig.Recovery.ObservedStableFor)
}

func (d *dispatcher) countResolved(sig engine.Signal) {
	if sig.Kind == engine.KindResolvedReverted {
		d.metrics.recoveriesReverted.Inc()
		return
	}
	d.metrics.recoveriesObserved.WithLabelValues(string(sig.Recovery.Resolution)).Inc()
}

// resolvedPayload composes the §9.3 schema-stable outcome record from
// a resolved Signal. The frozen k8s-event payload is untouched — this
// is its own struct, serialized in the same inject envelope, pinned
// byte-exact by TestDispatchResolved_ExactWireShape.
func resolvedPayload(sig engine.Signal) inject.ResolvedPayload {
	rec := sig.Recovery
	p := inject.ResolvedPayload{
		Kind:              sig.Kind,
		Reason:            sig.Key.Reason,
		Namespace:         sig.Namespace,
		KindOfObject:      sig.KindOfObject,
		Name:              sig.Name,
		Container:         sig.Container,
		UID:               sig.Key.UID,
		Fingerprint:       sig.Fingerprint,
		Cluster:           sig.Cluster,
		FirstSeen:         sig.FirstSeen,
		ResolvedAt:        rec.ResolvedAt,
		ClearedAfter:      rec.ClearedAfter.String(),
		ObservedStableFor: rec.ObservedStableFor.String(),
		Resolution:        string(rec.Resolution),
		Context: inject.PayloadContext{
			ControllerRef: sig.ControllerRef,
			Node:          sig.Node,
			Labels:        sig.Labels,
		},
	}
	if sig.Kind == engine.KindResolvedReverted {
		p.RevertedAfter = rec.RevertedAfter.String()
	}
	return p
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

	// Occurrence store (§9.1): opt-in via --store. When unset, disp
	// keeps a nil *store.Store whose methods are all no-ops — the M2
	// behavior, byte for byte. Hooks wire the store's drop/prune/write
	// visibility into this process's Prometheus registry; the deferred
	// Close flushes the writer buffer on every return path (dry-run
	// included — the store works in dry-run: routing decisions are
	// real even when injects are printed).
	var occStore *store.Store
	if f.store != "" {
		occStore, err = store.Open(f.store,
			store.WithTTL(f.storeTTL),
			store.WithMaxBytes(int64(f.storeMaxMB)<<20),
			store.WithHooks(store.Hooks{
				OnWrite: func(route store.RouteOutcome) { m.storeRecords.WithLabelValues(string(route)).Inc() },
				OnDrop:  func(cause string) { m.storeDrops.WithLabelValues(cause).Inc() },
				OnPrune: func(cause string, rows int64) { m.storePruned.WithLabelValues(cause).Add(float64(rows)) },
			}),
		)
		if err != nil {
			return err
		}
		defer func() {
			if cerr := occStore.Close(); cerr != nil {
				log.Printf("store: close: %v", cerr)
			}
		}()
		disp.store = occStore
		log.Printf("store: enabled (path=%s, ttl=%s, max=%dMiB, prune every %s)",
			f.store, f.storeTTL, f.storeMaxMB, store.PruneInterval(f.storeTTL))
	}

	// Severity routing (§7.7): the policy (source defaults +
	// --severity overrides) is always on; the shared watchboard only
	// exists in per-incident mode — in --mode=shared ALL severities
	// keep routing to --target-session exactly as before (frozen
	// contract; the watchboard is the per-incident-mode answer to
	// warning noise).
	disp.routing = engine.NewRoutingPolicy(f.severityOverrides)
	if f.mode == "per-incident" {
		board := newWatchboard(watchboardConfig{
			injector:      inj,
			metrics:       m,
			cluster:       f.clusterName,
			dryRun:        f.dryRun,
			batch:         f.watchboardBatch,
			flushInterval: f.watchboardFlush,
			rotateAfter:   f.watchboardRotate,
		})
		board.bind = disp.bindWatchboardIncident
		disp.board = board
	} else {
		log.Printf("severity routing: --mode=shared — all severities route to --target-session (watchboard disabled)")
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

	// §9.1 TTL + size-bound prune loop (interval min(1h, ttl/24)).
	// Nil-safe: returns immediately when the store is disabled.
	go occStore.RunPrune(ctx)

	// Watchboard interval-flush loop (§7.7): flushes buffered
	// warnings at --watchboard-flush age, plus a final best-effort
	// flush on shutdown so a terminating sentinel keeps its buffer.
	if disp.board != nil {
		go disp.board.run(ctx)
		log.Printf("watchboard: enabled (batch=%d, flush=%s, rotate after %d digest injects — §15 Q2 size-based)",
			f.watchboardBatch, f.watchboardFlush, f.watchboardRotate)
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

	// Signal sources (§7.2), individually enabled via --sources
	// (default: k8s-events only — the frozen M0 surface). The dynamic
	// client exists only for the expiry source's discovery-gated
	// cert-manager reads, and the saturation source's metrics.k8s.io
	// dimension needs its own clientset — each built from the same
	// kube options, only when its source is enabled.
	var dyn dynamic.Interface
	if f.sourceEnabled(expiry.Name) {
		dyn, err = kube.BuildDynamicClient(kube.Options{InCluster: f.inCluster, Kubeconfig: f.kubeconfig})
		if err != nil {
			return err
		}
	}
	var metricsClient metricsv.Interface
	if f.sourceEnabled(saturation.Name) {
		restCfg, cfgErr := kube.BuildConfig(kube.Options{InCluster: f.inCluster, Kubeconfig: f.kubeconfig})
		if cfgErr != nil {
			return cfgErr
		}
		metricsClient, err = metricsv.NewForConfig(restCfg)
		if err != nil {
			return fmt.Errorf("metrics.k8s.io client: %w", err)
		}
	}
	bs, err := buildSources(f, client, dyn, metricsClient)
	if err != nil {
		return err
	}
	registry, objState := bs.registry, bs.objState

	// Storm correlation (§7.5): ONE shared informer factory serves
	// the sources and the graph (§6.3) — when enabled, the
	// object-state source registers on the same factory as the graph
	// feed, so pods/nodes are watched once.
	var sharedFactory informers.SharedInformerFactory
	var feed *graphFeed
	if f.stormEnabled() {
		sharedFactory = informers.NewSharedInformerFactory(client, 0)
		if objState != nil {
			objState.WithFactory(sharedFactory)
		}
		if bs.rollout != nil {
			// §6.3 again: the rollout source's pods/replicasets
			// informers ride the same factory as the graph feed.
			bs.rollout.WithFactory(sharedFactory)
		}
		if bs.degradation != nil {
			bs.degradation.WithFactory(sharedFactory)
		}
		// Graph history (§6.6): with a store configured, every applied
		// graph delta also logs a ChangeRecord through the store's
		// buffered writer. Without one, onChange stays nil and the
		// graph skips change tracking entirely.
		var onChange func(graph.ChangeRecord)
		if occStore != nil {
			onChange = occStore.RecordGraphChange
		}
		feed = newGraphFeed(sharedFactory, onChange)
	} else if f.storm {
		log.Printf("storm: disabled (--storm-window=0)")
	}

	// Enrichment (§7.6): per-incident mode only — shared mode routes
	// everything to --target-session and keeps its frozen payload
	// contract. When the graph feed runs, enrichment reuses its live
	// topology snapshot and the shared informer caches (§4.3 surface
	// 3); otherwise every run takes the scoped LoadCluster fallback.
	// No startup RBAC probe on purpose: enrichment is best-effort by
	// definition (§7.6 failure honesty) — a missing grant surfaces as
	// enrichment_error trailers + enrichment_failures_total, never as
	// a crash-loop or a silent degrade.
	if f.mode == "per-incident" && f.enrich != "off" {
		e := &enricher{
			client:   client,
			now:      time.Now,
			metrics:  m,
			policy:   f.enrich,
			cap:      f.enrichCap,
			logLines: f.enrichLogLines,
			timeout:  f.enrichTimeout,
		}
		path := "scoped-list"
		if feed != nil {
			e.snapshot = feed.snapshot
			podLister := sharedFactory.Core().V1().Pods().Lister()
			e.livePod = func(ns, name string) (*corev1.Pod, error) {
				return podLister.Pods(ns).Get(name)
			}
			path = "live-graph (scoped-list fallback)"
		}
		disp.enrich = e
		log.Printf("enrichment: enabled (severities=%s, cap=%dB, log-lines=%d, timeout=%s, read path: %s)",
			f.enrich, f.enrichCap, f.enrichLogLines, f.enrichTimeout, path)
	} else if f.enrich != "off" {
		log.Printf("enrichment: --mode=shared — disabled (the shared session's payload contract is frozen; enrichment warms per-incident sessions)")
	}

	// §11: verify each source's declared RBAC before watching
	// anything — a deployment whose ServiceAccount can't support a
	// source fails loudly here, naming the source and the missing
	// permission, never a silently empty watch.
	if err := sources.Probe(ctx, sources.NewAccessReviewer(client), registry.All()...); err != nil {
		return err
	}
	if feed != nil {
		// Same posture for the graph feed's informers: --storm is an
		// explicit opt-in, so a missing grant fails loudly at startup.
		if err := probeGraphAccess(ctx, sources.NewAccessReviewer(client)); err != nil {
			return err
		}
	}

	// Recovery injects (§7.4): watch bound incidents for symptom
	// clearance and inject the outcome into the same session. Sources
	// with their own clearance predicates register them ahead of the
	// generic pod observer (§7.4: each source that can observe a
	// symptom can observe its absence) — order matters for probe_flap,
	// where "pod is Ready" is true between the flaps the incident is
	// about.
	if f.recoveryStableFor > 0 {
		if err := setupRecovery(ctx, f, client, dedup, disp, m, bs); err != nil {
			return err
		}
	}

	// Arm the correlator and start the graph ingest loop. A feed
	// failure is fatal like a source failure (§7.2 posture: a
	// sentinel with a dead stage lies about its coverage) — it
	// cancels the process and its error surfaces after RunAll.
	var feedErr error
	var feedMu sync.Mutex
	if feed != nil {
		correlator, cerr := engine.NewStormCorrelator(f.stormWindow, f.stormMin, feed)
		if cerr != nil {
			return cerr
		}
		disp.storm = correlator
		go func() {
			if ferr := feed.Run(ctx); ferr != nil && ctx.Err() == nil {
				feedMu.Lock()
				feedErr = ferr
				feedMu.Unlock()
				cancel()
			}
		}()
		log.Printf("storm: correlation enabled (window=%s, min=%d)", f.stormWindow, f.stormMin)
		// Graph history (§6.6): periodic compressed snapshots into the
		// store, alongside the continuously logged deltas above. Only
		// when BOTH the graph feed and the store run — one-shot CLI
		// invocations read this history via --at + --store.
		if occStore != nil {
			go runGraphHistoryLoop(ctx, feed.snapshot, occStore, f.graphSnapshotInterval)
			log.Printf("graph history: enabled (snapshot every %s + per-delta change log → %s; serves --at point-in-time queries and triage changes)",
				f.graphSnapshotInterval, f.store)
		}
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
	if err == nil {
		feedMu.Lock()
		err = feedErr
		feedMu.Unlock()
	}
	return err
}

// recoveryTickInterval is how often the recovery tracker re-evaluates
// clearance predicates. Deliberately not a flag: any value well below
// --recovery-stable-for behaves identically, and 15s keeps worst-case
// resolution latency negligible against a 5m stability window.
const recoveryTickInterval = 15 * time.Second

// recoveryAccess is the RBAC the pod clearance observer needs beyond
// the M0 ClusterRole (which granted only pods get).
var recoveryAccess = []sources.Requirement{
	{Resource: "pods", Verb: "list"},
	{Resource: "pods", Verb: "watch"},
}

// builtSources is buildSources' result: the registry plus the typed
// handles the rest of startup needs — objectstate/rollout/degradation
// for shared-factory wiring (§6.3) and all of them for §7.4 clearance
// observers.
type builtSources struct {
	registry    *sources.Registry
	objState    *objectstate.Source
	rollout     *rollout.Source
	saturation  *saturation.Source
	degradation *degradation.Source
	expiry      *expiry.Source
}

// buildSources registers the sources named by --sources (§7.2:
// sources are individually enabled in config). Informer-backed
// sources are returned as typed handles so setupRecovery can reuse
// their informers/state as §7.4 clearance observers instead of
// starting duplicates, and so storm mode can move them onto the
// shared informer factory (§6.3). dyn may be nil unless the expiry
// source is enabled (it reads cert-manager Certificates unstructured;
// the caller builds it only when needed); metricsClient is only
// required (non-nil) when the saturation source is enabled — its
// metrics.k8s.io dimension rides a separate clientset.
func buildSources(f *flags, client kubernetes.Interface, dyn dynamic.Interface, metricsClient metricsv.Interface) (*builtSources, error) {
	bs := &builtSources{registry: sources.NewRegistry()}
	for _, name := range splitCSV(f.sources) {
		var src sources.Source
		switch name {
		case k8sevents.Name:
			src = k8sevents.New(client, 0)
		case objectstate.Name:
			bs.objState = objectstate.New(client, objectstate.DefaultConfig())
			src = bs.objState
		case rollout.Name:
			cfg := rollout.DefaultConfig()
			cfg.Observe = f.rolloutObserve
			bs.rollout = rollout.New(client, cfg)
			src = bs.rollout
		case saturation.Name:
			if metricsClient == nil {
				return nil, fmt.Errorf("--sources: %s requires a metrics.k8s.io client (programming error: buildSources called without one)", saturation.Name)
			}
			cfg := saturation.DefaultConfig()
			cfg.Interval = f.saturationInterval
			cfg.Window = f.saturationWindow
			cfg.WarnETA = f.saturationWarn
			bs.saturation = saturation.New(cfg,
				saturation.NewMetricsPodFetcher(metricsClient, client),
				saturation.NewKubeletVolumeFetcher(client))
			src = bs.saturation
		case degradation.Name:
			cfg := degradation.DefaultConfig()
			cfg.Window = f.degradationWindow
			cfg.Drop = f.degradationDrop
			bs.degradation = degradation.New(client, cfg)
			src = bs.degradation
		case expiry.Name:
			cfg := expiry.DefaultConfig()
			cfg.Interval = f.expiryInterval
			cfg.WarnWindow = f.expiryWarn
			cfg.Namespaces = splitCSV(f.expiryNamespaces)
			bs.expiry = expiry.New(client, dyn, cfg)
			src = bs.expiry
		default:
			// validate() rejects unknown names before we get here.
			return nil, fmt.Errorf("--sources: unknown source %q", name)
		}
		if err := bs.registry.Register(src); err != nil {
			return nil, err
		}
	}
	return bs, nil
}

// setupRecovery wires the §7.4 closed loop: pod clearance observer →
// RecoveryTracker → dispatcher, plus restart seeding from the dedup
// snapshot's persisted bindings.
//
// Observer selection: when the object-state source is enabled its pod
// informer doubles as the clearance observer (the source absorbed the
// minimal pod observer — one informer, same judging behavior, and its
// RBAC was already verified by the §11 source probe). Otherwise the
// standalone fallback observer keeps the default deployment's
// zero-config behavior identical to the shipped M2 PR.
//
// Fallback RBAC posture differs from the §11 source probe on purpose:
// an M0 deployment upgraded by image swap may still run the old
// ClusterRole without pods list/watch, and — unlike a signal source —
// a disabled recovery observer means a missing outcome record, never
// a missed incident. So insufficient RBAC disables recovery with a
// loud log naming the grant, instead of crash-looping existing
// deployments.
func setupRecovery(ctx context.Context, f *flags, client kubernetes.Interface, dedup *engine.DedupCache, disp *dispatcher, m *metrics, bs *builtSources) error {
	// Observer order is load-bearing (the tracker asks in order and
	// the FIRST claim wins): every source-specific observer precedes
	// any pod-scoped one, in source registration (§7.2) order. In
	// particular the saturation observer must outrank the generic
	// pod-readiness judge, because saturation.forecast incidents
	// carry KindOfObject=Pod and a readiness judge would wrongly
	// clear a leaking-but-Ready pod (same trap as degradation's
	// probe_flap, where "pod is Ready" is true between the flaps the
	// incident is about). Each observer judges only its own kinds, so
	// registration is safe regardless of which sources are enabled.
	var observers []engine.ClearanceObserver
	if bs.rollout != nil {
		observers = append(observers, bs.rollout.ClearanceObserver())
		log.Printf("recovery: rollout clearance observer registered (rollout completed or rolled back → cleared)")
	}
	if bs.saturation != nil {
		observers = append(observers, bs.saturation.ClearanceObserver())
		log.Printf("recovery: saturation clearance observer registered (forecast recede / non-positive-slope re-observation)")
	}
	if bs.degradation != nil {
		observers = append(observers, bs.degradation.ClearanceObserver())
		log.Printf("recovery: degradation clearance observer registered (ready-ratio recovered / flapping settled → cleared)")
	}
	if bs.expiry != nil {
		observers = append(observers, bs.expiry.ClearanceObserver())
		log.Printf("recovery: expiry clearance observer registered (certificate renewed → cleared)")
	}
	if bs.objState != nil {
		observers = append(observers, bs.objState.ClearanceObserver())
		log.Printf("recovery: clearance observer backed by the object-state source's pod + node informers")
	} else {
		reviewer := sources.NewAccessReviewer(client)
		podRBAC := true
		for _, req := range recoveryAccess {
			allowed, err := reviewer.Allowed(ctx, req)
			if err != nil {
				return fmt.Errorf("recovery: capability probe for %q failed: %w", req, err)
			}
			if !allowed {
				if len(observers) == 0 {
					log.Printf("recovery: DISABLED — ServiceAccount lacks %q; grant pods list/watch (see deploy/12-clusterrole-watcher.yaml) to enable §7.4 recovery injects", req)
					return nil
				}
				// The M2 fallback posture, source-aware: incidents
				// owned by source-specific observers still get
				// outcome records; only pod-scoped clearance is
				// missing.
				log.Printf("recovery: pod clearance DISABLED — ServiceAccount lacks %q; source-specific clearance observers stay active", req)
				podRBAC = false
				break
			}
		}
		if podRBAC {
			obs := newPodClearanceObserver(client)
			if err := obs.Start(ctx); err != nil {
				return err
			}
			observers = append(observers, obs)
		}
	}
	tracker := engine.NewRecoveryTracker(f.recoveryStableFor, func(sig engine.Signal) {
		disp.DispatchSignal(ctx, sig)
	})
	for _, o := range observers {
		tracker.AddObserver(o)
	}
	disp.tracker = tracker
	// Resume clearance watching for bindings restored from
	// --dedup-persist so the fix-verify loop survives a restart.
	if bindings := dedup.Bindings(); len(bindings) > 0 {
		for _, b := range bindings {
			tracker.Track(engine.Incident(b))
		}
		log.Printf("recovery: resumed tracking %d bound incident(s) from dedup snapshot", len(bindings))
	}
	go runRecoveryLoop(ctx, tracker, m)
	log.Printf("recovery: tracking enabled (stable-for=%s, tick=%s)", f.recoveryStableFor, recoveryTickInterval)
	return nil
}

// runRecoveryLoop drives the tracker on an interval and keeps the
// recovery_tracking gauge current. Exits when ctx is cancelled.
func runRecoveryLoop(ctx context.Context, tracker *engine.RecoveryTracker, m *metrics) {
	t := time.NewTicker(recoveryTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tracker.Tick()
			m.recoveryTracking.Set(float64(tracker.Len()))
		}
	}
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
