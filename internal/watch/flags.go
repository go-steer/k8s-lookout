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
	"errors"
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/autoscaling"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/degradation"
	"github.com/go-steer/k8s-lookout/pkg/sources/expiry"
	"github.com/go-steer/k8s-lookout/pkg/sources/gateway"
	"github.com/go-steer/k8s-lookout/pkg/sources/ingress"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/notifications"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/quota"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
	"github.com/go-steer/k8s-lookout/pkg/sources/tokenburn"
	"github.com/go-steer/k8s-lookout/pkg/sources/workload"
)

// The frozen `lookout watch` flag surface (M0 contract; §14 CLI
// stability): the flag-shaped config, parseFlags/newFlagSet, and
// validate — misconfig fails before any network / API touching.

// flags is the CLI-shaped config, parsed once in main and threaded
// to the components. All fields match --flag-name in the design
// doc's "Sidecar CLI" section.
type flags struct {
	daemonURL             string
	tokenEnv              string
	mode                  string
	targetSession         string
	owner                 string
	sink                  string
	sinkURL               string
	sinkTokenEnv          string
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
	capacityPoll          time.Duration
	pendingAge            time.Duration
	gatewayGrace          time.Duration
	quotaPoll             time.Duration
	quotaWindow           time.Duration
	quotaWarn             float64
	notificationsSub      string
	tokenPoll             time.Duration
	burnMultiple          float64
	burnETA               time.Duration
	tokenBudgetUSD        float64
	tokenEndpoint         string
	dedupWindow           time.Duration
	dedupPersist          string
	unhealthyMinCount     int
	backoffMinCount       int
	recoveryStableFor     time.Duration
	storm                 string
	stormWindow           time.Duration
	stormMin              int
	severity              severityFlag
	watchboardBatch       int
	watchboardFlush       time.Duration
	watchboardRotate      int
	enrich                string
	enrichCap             int
	injectMaxBytes        int
	enrichLogLines        int
	enrichTimeout         time.Duration
	enrichLists           string
	enrichListsPreflight  bool
	store                 string
	storeTTL              time.Duration
	storeMaxMB            int
	triageRegressFactor   int
	distillInterval       time.Duration
	graphSnapshotInterval time.Duration
	// severityOverrides is the parsed --severity map, populated by
	// validate().
	severityOverrides map[string]engine.Severity
	// sourcesAutoResolved records that --sources=auto expanded this
	// list: the auto summary already reported per-source degradation
	// lines, so realMain's Probe pass skips re-reporting them (#145
	// review finding 3 — one fact, one line).
	sourcesAutoResolved bool
	inCluster           bool
	kubeconfig          string
	clusterName         string
	project             string
	zone                string
	logLevel            string
	dryRun              bool
	metricsAddr         string
	snapshotInterval    time.Duration
	otelExporter        string
}

// parseFlags reads argv into flags. Returns nil on --help (main
// exits 0). Any other parse error surfaces as an error so main can
// report + exit 2 (POSIX convention for CLI misuse).
func parseFlags(args []string) (*flags, error) {
	fs, f := newFlagSet()
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return f, nil
}

// newFlagSet declares the full `lookout watch` flag surface onto a
// fresh FlagSet. Split from parseFlags so FlagInventory (flagdocs.go)
// can walk the SAME declarations the sentinel parses — the docs-site
// flag table is derived from this set, never maintained by hand.
func newFlagSet() (*flag.FlagSet, *flags) {
	fs := flag.NewFlagSet("lookout watch", flag.ContinueOnError)
	f := &flags{}

	// Required.
	fs.StringVar(&f.daemonURL, "daemon-url", "", "Base URL of the core-agent daemon (http://... or https://...). Required.")
	fs.StringVar(&f.tokenEnv, "token-env", "", "Env var name holding the bearer token for the daemon. Required.")

	// Session routing.
	fs.StringVar(&f.mode, "mode", "per-incident", "Session routing mode: per-incident (create per (uid,reason)) or shared (all to --target-session).")
	fs.StringVar(&f.targetSession, "target-session", "", "Required when --mode=shared: SessionID to post all injects to.")
	fs.StringVar(&f.owner, "owner", "", "X-Asserted-Caller value for POST /sessions in per-incident mode. Sidecar must be in daemon's proxy_identities.")

	// Agent sink (docs/agent-sink-design.md). The default is the
	// core-agent daemon client the sentinel has always spoken, so
	// deployments that omit --sink keep behaving identically.
	fs.StringVar(&f.sink, "sink", sinkCoreAgent, "Agent sink receiving incident payloads: core-agent (default: POST /sessions + /sessions/<sid>/inject against --daemon-url) or webhook (generic receiver: POST <sink-url>/incidents opens an incident with the schema-v1 payload JSON as the body; POST <sink-url>/incidents/<id>/events appends follow-ups).")
	fs.StringVar(&f.sinkURL, "sink-url", "", "Base URL of the generic webhook receiver (no trailing slash). Required with --sink=webhook. https is STRONGLY recommended: plain http is allowed (remote receivers are the point) but warns loudly at startup — incident payloads and the bearer token ride unencrypted.")
	fs.StringVar(&f.sinkTokenEnv, "sink-token-env", "", "Env var name holding the bearer token the webhook sink sends as Authorization: Bearer. Optional (unset = unauthenticated POSTs); only valid with --sink=webhook.")

	// Event filtering.
	fs.StringVar(&f.reasons, "reason", "", "Comma-separated allow-list of Event.Reason values. Empty = shipped default set.")
	fs.StringVar(&f.namespaces, "namespace", "", "Comma-separated allow-list of namespaces. Empty = all namespaces.")
	fs.StringVar(&f.excludeNamespaces, "exclude-namespace", "", "Comma-separated deny-list of namespaces.")

	// Signal sources (§7.2: sources are individually enabled).
	// DEFAULT CHANGED to auto (2026-07-27, zero-deployed-users policy;
	// the pre-auto default was k8s-events only): auto probes each
	// portable source's declared needs at startup and enables what the
	// deployment supports — see resolveSourcesAuto in auto.go. An
	// explicit list keeps the original §11 semantics exactly: every
	// named source's probe failure is fatal, and --sources=k8s-events
	// reproduces the old default byte-for-byte.
	fs.StringVar(&f.sources, "sources", autoValue, "Comma-separated signal sources to enable, or auto (the default): probe the portable sources' needs at startup — RBAC via SelfSubjectAccessReview, plus metrics.k8s.io presence for saturation — and enable what this deployment supports, skipping misses with one loud line each (k8s-events must pass; a sentinel that cannot watch events is misdeployed). Known sources: k8s-events, object-state, rollout, workload, autoscaling, saturation, degradation, expiry, capacity, ingress, gateway, quota, notifications, token-burn. quota (project tier), notifications (needs --notifications-subscription), and token-burn (core-agent cost stack) are never auto-enabled. An explicit list keeps §11 semantics: a named source's missing REQUIRED grant is fatal (optional dimensions — saturation's nodes/proxy PVC read — still degrade loudly instead, issue #145).")

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

	// Capacity source knobs (§7.2 row 7, §10.1). ADDITIVE flags; only
	// meaningful with --sources=…,capacity. The critical pending-age
	// escalation (15m) is design-fixed, not a flag.
	fs.DurationVar(&f.capacityPoll, "capacity-poll", 60*time.Second, "Poll interval for the capacity source's cluster-autoscaler-status ConfigMap read, provider scale-decision query, and pending-pod age sweep. Must be > 0.")
	fs.DurationVar(&f.pendingAge, "pending-age", 5*time.Minute, "How long a pod must be Pending+Unschedulable before capacity.pending-aged fires at warning (critical at the design-fixed 15m, or at this value when set higher). Must be > 0.")

	// Gateway source knobs (§7.2 gateway row, #168). ADDITIVE flag; only
	// meaningful with --sources=…,gateway. The grace window absorbs
	// normal load-balancer provisioning latency (a fresh Gateway sits at
	// Programmed=False for minutes) before a sustained status-condition
	// failure fires.
	fs.DurationVar(&f.gatewayGrace, "gateway-grace", 5*time.Minute, "How long a Gateway/HTTPRoute status condition (Programmed/Accepted/ResolvedRefs=False, reason != Pending) must be sustained — timed from its lastTransitionTime — before gateway.programming_failed / gateway.route_rejected fires. Absorbs normal LB provisioning latency. Must be > 0.")

	// Quota source knobs (§7.2 row 8, §10.2). ADDITIVE flags; only
	// meaningful with --sources=…,quota — which is a PER-PROJECT
	// opt-in: exactly one sentinel per GCP project enables it (§11
	// Project tier), and it requires a quota-capable cloud provider
	// (loud startup error otherwise). The severity thresholds
	// (warning ETA<7d or usage>=90%; critical ETA<48h or >=98%) are
	// design-fixed, not flags.
	fs.DurationVar(&f.quotaPoll, "quota-poll", 15*time.Minute, "Poll interval for the quota source's inventory read and per-watched-quota history query. Must be > 0.")

	fs.DurationVar(&f.quotaWindow, "quota-window", 7*24*time.Hour, "History window the quota usage slope is fitted over (the §8 linear-<window> confidence basis); a forecast needs usage points spanning at least half of it. Must be > 0.")
	fs.Float64Var(&f.quotaWarn, "quota-warn", 0.80, "Usage/limit ratio above which a quota is always watched (history fetched every poll) in addition to the top-10 nearest exhaustion. Must be in (0, 1).")

	// Notifications source (post-M5 #130). ADDITIVE flag; only
	// meaningful with --sources=...,notifications.
	fs.StringVar(&f.notificationsSub, "notifications-subscription", "", "Subscription the notifications source reads (GKE: a Pub/Sub subscription on the cluster's notificationConfig topic) — either projects/<p>/subscriptions/<name> or a bare name resolved against the provider project. Required when the notifications source is enabled.")

	// Token-burn source knobs (§7.2 row 9, §12). ADDITIVE flags; only
	// meaningful with --sources=…,token-burn. The cost stack rides
	// the SAME daemon the injector talks to (core-agent v2.7.0's GET
	// /sessions + GET /sessions/{app}/{sid}/usage), so the source
	// reuses --daemon-url/--token-env; --token-endpoint overrides the
	// base URL for split deployments. The sustain count (2 polls) and
	// regression window (15m) are design-fixed, not flags.
	fs.DurationVar(&f.tokenPoll, "token-poll", 60*time.Second, "Poll interval for the token-burn source's cost-stack reads (core-agent GET /sessions + per-session /usage). Must be > 0.")
	fs.Float64Var(&f.burnMultiple, "burn-multiple", 4, "Session token rate at or above this multiple of the cross-session trailing-median baseline (sustained 2 polls) fires token.burn at warning. Must be > 1.")
	fs.DurationVar(&f.burnETA, "burn-eta", 30*time.Minute, "Budget-exhaustion projection inside this window fires token.burn at critical (with the §8 linear forecast); clearance requires the ETA to recede beyond 2x this threshold. Must be > 0.")
	fs.Float64Var(&f.tokenBudgetUSD, "token-budget-usd", 0, "Per-session spend budget in USD for the token-burn source's critical trigger; 0 (default) = unknown, budget trigger disarmed. Lookout-side config because core-agent v2.7.0 does not expose its CostCeiling over the attach API (TODO(core-agent) in pkg/sources/tokenburn). Must be >= 0.")
	fs.StringVar(&f.tokenEndpoint, "token-endpoint", "", "Override base URL for the core-agent cost stack (default: --daemon-url — the §3 boundary rides the same daemon the injector talks to). No trailing slash.")

	// Dedup.
	fs.DurationVar(&f.dedupWindow, "dedup-window", 5*time.Minute, "Rolling window for (uid,reason) dedup.")
	fs.StringVar(&f.dedupPersist, "dedup-persist", "", "Optional path to persist dedup cache across sidecar restart.")
	fs.IntVar(&f.unhealthyMinCount, "unhealthy-min-count", 3, "Require this many consecutive Unhealthy events before firing.")
	fs.IntVar(&f.backoffMinCount, "backoff-min-count", 3, "Require the crash-loop family (canonical CrashLoopBackOff — kubelet's repeating BackOff cycle) to reach this Event.Count before firing, so a transient startup blip that self-heals does not open a noise session. Image-pull backoff is never gated (a bad tag is persistent). 1 fires on the first event.")

	// Recovery injects (§7.4).
	fs.DurationVar(&f.recoveryStableFor, "recovery-stable-for", 5*time.Minute, "How long a cleared symptom must stay clear before kind=resolved is injected into the incident's session; recurrence within this window after a resolve fires kind=resolved.reverted. 0 disables recovery tracking.")

	// Storm correlation (§7.5). DEFAULT CHANGED to auto (2026-07-27,
	// zero-deployed-users policy; the flag was a default-false bool
	// before): auto probes the graph informer grants at startup and
	// resolves on/off with a loud line either way — see
	// resolveStormAuto in auto.go. --storm=on keeps the original
	// explicit-opt-in semantics: a missing grant is a fatal startup
	// error. SYNTAX CHANGED with the type: bare `--storm` (valid bool
	// syntax) now errors; write --storm=on.
	fs.StringVar(&f.storm, "storm", stormAuto, "Storm correlation (§7.5): auto (the default — probe the graph informers' grants at startup: pods/nodes/replicasets list+watch; all present resolves on, a miss resolves off with one loud line naming the grant), on (fatal at startup when a grant is missing), or off. true/false are aliases for on/off; bare --storm is no longer valid syntax. When on, new incidents sharing a blast-radius key (nearest common topology ancestor) group into one kind=storm session.")
	fs.DurationVar(&f.stormWindow, "storm-window", engine.DefaultStormWindow, "Second-level correlation window for storm formation. 0 disables correlation even with --storm=on.")
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
	fs.IntVar(&f.enrichCap, "enrich-cap", 4096, "Byte budget for the attached enrichment bundle (§15: fixed budget). Kept under --inject-max-bytes so the bundle plus the rest of the payload clears the daemon's per-inject ceiling with headroom for the double-JSON envelope. Truncation happens at section boundaries; dropped sections become overflow trailers naming the lookout command that reproduces them.")
	fs.IntVar(&f.injectMaxBytes, "inject-max-bytes", inject.MaxInjectBytes, "Per-inject wire-body ceiling the dispatcher fits payloads to before POSTing (default matches the core-agent daemon's 8192-byte limit). An over-limit payload is shrunk least-signal-first — enrichment dropped, then message truncated — never identity, so the incident still routes; without this the daemon 400s the whole inject and a new incident lands as an empty session (issue #198).")
	fs.IntVar(&f.enrichLogLines, "enrich-log-lines", 200, "Log tail per container stream distilled into the enrichment bundle's logs section. Must be >= 1.")
	fs.DurationVar(&f.enrichTimeout, "enrich-timeout", 5*time.Second, "Hard wall-clock budget for one enrichment run; on expiry the inject fires with whatever sections completed plus enrichment_error trailers. Must be > 0.")
	fs.StringVar(&f.enrichLists, "enrich-lists", "all", "Which cluster resources the scoped-list enrichment fallback reads: 'all' (default), a comma-separated allowlist (pods,deployments), or subtractions (all,-secrets) to keep the watcher SA least-privilege. Denied or deselected lists degrade to a partial bundle with a skipped= note on the head, never a resolve failure.")
	fs.BoolVar(&f.enrichListsPreflight, "enrich-lists-preflight", false, "Before the scoped-list pass, SelfSubjectAccessReview each selected resource and drop the denied ones proactively (fewer 403s in the watcher log); falls back to reactive Forbidden-skip if SSAR is not permitted.")

	// Occurrence store (§9.1). ADDITIVE flags, default OFF: with no
	// --store the sentinel behaves exactly as before (info-severity
	// signals are counted and dropped). The path is ALWAYS explicit —
	// there is no default location (and never one under $HOME; the
	// store belongs on the same volume as --dedup-persist).
	fs.StringVar(&f.store, "store", "", "Path to the sentinel-local SQLite occurrence store (§9.1), e.g. /var/lib/lookout/lookout.db — put it on the --dedup-persist volume. Every emitted signal is recorded with its routing outcome; info-severity signals are persisted instead of dropped. Empty (default) disables the store.")
	fs.DurationVar(&f.storeTTL, "store-ttl", 720*time.Hour, "Retention for stored occurrences (§9.1 default 30 days); the prune loop deletes older rows. Must be > 0.")
	fs.IntVar(&f.storeMaxMB, "store-max-mb", 512, "Size bound for the occurrence store in MiB; when exceeded, the oldest occurrences are pruned first (loudly). Must be >= 1.")

	// §9.4 regression evidence (M4 observation 3). ADDITIVE flag;
	// only meaningful with --store (the triage-status records live
	// there). The threshold is a simple multiplier over the window
	// count at downgrade time — deliberately not a rate model.
	fs.IntVar(&f.triageRegressFactor, "triage-regress-factor", 3, "A downgraded incident (§9.4 severity_override) whose dedup-window count reaches this multiple of its count at downgrade time gets ONE kind=triage.regressed evidence followup into its bound session — never an automatic re-page (docs/triage-status-write-design.md). Must be >= 2; 0 disables.")

	// Distilled memories (§9.2). ADDITIVE flag: the distiller runs
	// only with --store — the occurrence window it reads AND the
	// in-tree memory backend it writes both live there (pkg/memory
	// documents why the sentinel store stands in for core-agent's
	// shared Memory interface until one exists to bind to).
	fs.DurationVar(&f.distillInterval, "distill-interval", 6*time.Hour, "How often the §9.2 distiller pass converts recurring occurrences into durable memory facts (requires --store; the pass reads the last 7d of occurrences). 0 disables distillation. Must be >= 0.")

	// Graph history (§6.6). ADDITIVE flag: history turns on only when
	// BOTH --store and the graph feed (--storm) are active — without a
	// resident graph there is nothing to snapshot, and without a store
	// there is nowhere to put it. Point-in-time queries (--at) and
	// `triage changes` read what this writes.
	fs.DurationVar(&f.graphSnapshotInterval, "graph-snapshot-interval", 5*time.Minute, "How often to persist a compressed topology snapshot to --store (§6.6; the per-delta change log is written continuously). Effective only with --store AND storm correlation on (the graph feed). Must be > 0.")

	// Kubernetes client.
	fs.BoolVar(&f.inCluster, "in-cluster", false, "Use in-cluster service account credentials. Auto-detected inside a pod.")
	fs.StringVar(&f.kubeconfig, "kubeconfig", "", "Explicit kubeconfig path. Used outside a pod.")
	fs.StringVar(&f.clusterName, "cluster-name", "", "Human-readable cluster name included in every inject payload.")

	// Deployment identity (§8). ADDITIVE flags: zone/project complete
	// the (fingerprint, cluster/project/zone) fleet-rollup join on
	// source-namespaced payloads, and zone participates in the
	// fingerprint hash. Precedence: explicit flag > cloud-provider
	// metadata (a provider implementing cloud.Identity, e.g. gke on
	// tagged builds) > empty. Empty zone keeps the pre-wiring
	// zone-less fingerprints byte-identical — deployments that set
	// nothing behave exactly as before.
	fs.StringVar(&f.project, "project", "", "Cloud project/account the cluster runs in, stamped into §8 payloads. Empty = detect from the cloud provider's metadata when a provider is compiled in; vanilla clusters can set it explicitly.")
	fs.StringVar(&f.zone, "zone", "", "Failure domain (zone, or region for regional clusters) stamped into §8 payloads and the signal fingerprint hash. Empty = detect from the cloud provider's metadata when a provider is compiled in; vanilla clusters can set it explicitly (e.g. from a topology label). Unset zones produce zone-less fingerprints — stable, but cross-cluster joins within a zone need it stamped.")

	// Operational.
	fs.StringVar(&f.logLevel, "log-level", "info", "One of: debug, info, warn, error.")
	fs.BoolVar(&f.dryRun, "dry-run", false, "Watch the cluster for real (informers, sources, filter/dedup/routing all run) but print inject payloads to stdout instead of calling the daemon/sink. Needs cluster access like a normal run.")
	fs.StringVar(&f.metricsAddr, "metrics-addr", "", "Prometheus /metrics + /healthz listener address (host:port). Empty = disabled.")
	fs.DurationVar(&f.snapshotInterval, "snapshot-interval", 30*time.Second, "How often to persist the dedup cache when --dedup-persist is set. 0 = only on shutdown.")

	// OpenTelemetry — mirrors the daemon's config.otel.exporter shape.
	// When "otlp", honors standard OTEL_EXPORTER_OTLP_ENDPOINT env vars.
	// The W3C traceparent propagator is registered globally regardless
	// of this setting so outbound POSTs carry trace context to a
	// tracing-enabled daemon even when the watcher itself isn't
	// exporting spans locally (rare but useful during phased rollouts).
	fs.StringVar(&f.otelExporter, "otel-exporter", "none", "OpenTelemetry span exporter: none | console | otlp. See docs/otel.md.")

	return fs, f
}

// sink* are the --sink values (docs/agent-sink-design.md).
const (
	sinkCoreAgent = "core-agent"
	sinkWebhook   = "webhook"
)

// validate checks flag combinations after parse. Called once from
// main so misconfig fails before any network / API touching.
func (f *flags) validate() error {
	// Agent sink (--sink): validated first — the daemon-flag
	// requirements below are sink-conditional. A typo'd sink name is a
	// config error in every mode, like --sources.
	switch f.sink {
	case sinkCoreAgent, sinkWebhook:
	default:
		return fmt.Errorf("--sink must be core-agent or webhook (got %q)", f.sink)
	}
	if f.sink == sinkWebhook {
		if !f.dryRun && f.sinkURL == "" {
			return errors.New("--sink-url is required with --sink=webhook (unless --dry-run)")
		}
		if strings.HasSuffix(f.sinkURL, "/") {
			return fmt.Errorf("--sink-url must not end with '/' (got %q)", f.sinkURL)
		}
		// core-agent session concepts make no sense against a generic
		// receiver: reject loudly instead of silently ignoring them.
		// --mode's default value is accepted (it is a no-op: the
		// webhook sink always opens one incident per signal).
		if f.mode != "per-incident" {
			return fmt.Errorf("--mode is a core-agent session concept: --sink=webhook always opens per-incident webhook incidents (got --mode=%s)", f.mode)
		}
		if f.targetSession != "" {
			return errors.New("--target-session is a core-agent session concept: not valid with --sink=webhook")
		}
		if f.owner != "" {
			return errors.New("--owner is a core-agent session concept (X-Asserted-Caller on POST /sessions): not valid with --sink=webhook")
		}
	} else {
		if f.sinkURL != "" {
			return errors.New("--sink-url is only valid with --sink=webhook")
		}
		if f.sinkTokenEnv != "" {
			return errors.New("--sink-token-env is only valid with --sink=webhook (the core-agent sink authenticates via --token-env)")
		}
	}
	if !f.dryRun && f.sink == sinkCoreAgent && f.daemonURL == "" {
		return errors.New("--daemon-url is required (unless --dry-run)")
	}
	if !f.dryRun && f.sink == sinkCoreAgent && f.tokenEnv == "" {
		return errors.New("--token-env is required (unless --dry-run)")
	}
	if strings.HasSuffix(f.daemonURL, "/") {
		return fmt.Errorf("--daemon-url must not end with '/' (got %q)", f.daemonURL)
	}
	// Sources are validated before the mode switch's dry-run early
	// return: a typo'd source name is a config error in every mode.
	// "auto" is the whole value or absent — mixing it with named
	// sources is ambiguous (is the name a floor, a pin, or a typo?)
	// and rejected loudly rather than guessed at.
	names := splitCSV(f.sources)
	if len(names) == 0 {
		return fmt.Errorf("--sources must enable at least one source (known: %s; or auto)", strings.Join(knownSources, ", "))
	}
	if slices.Contains(names, autoValue) {
		if len(names) != 1 {
			return fmt.Errorf("--sources: auto cannot be combined with named sources (got %q) — use auto alone, or pin an explicit list", f.sources)
		}
		f.sources = autoValue
	} else {
		for _, name := range names {
			if !slices.Contains(knownSources, name) {
				return fmt.Errorf("--sources: unknown source %q (known: %s; or auto)", name, strings.Join(knownSources, ", "))
			}
		}
	}
	// Storm mode (§7.5): normalize the bool-era aliases first so the
	// rest of startup switches on exactly three values.
	switch f.storm {
	case "true":
		f.storm = stormOn
	case "false":
		f.storm = stormOff
	case stormAuto, stormOn, stormOff:
	default:
		return fmt.Errorf("--storm must be auto, on, or off (got %q; true/false are aliases for on/off, and bare --storm is no longer valid — write --storm=on)", f.storm)
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
	// Capacity knobs (§7.2 row 7): config errors in every mode, like
	// the other source thresholds, even when the source is disabled.
	if f.capacityPoll <= 0 {
		return errors.New("--capacity-poll must be > 0")
	}
	if f.pendingAge <= 0 {
		return errors.New("--pending-age must be > 0")
	}
	// Gateway knob (§7.2 gateway row, #168): config error in every mode,
	// like the other source thresholds, even when the source is disabled.
	if f.gatewayGrace <= 0 {
		return errors.New("--gateway-grace must be > 0")
	}
	// Quota knobs (§7.2 row 8): config errors in every mode, like the
	// other source thresholds, even when the source is disabled.
	if f.quotaPoll <= 0 {
		return errors.New("--quota-poll must be > 0")
	}
	if f.quotaWindow <= 0 {
		return errors.New("--quota-window must be > 0")
	}
	if f.quotaWarn <= 0 || f.quotaWarn >= 1 {
		return errors.New("--quota-warn must be in (0, 1)")
	}
	// Token-burn knobs (§7.2 row 9, §12): config errors in every
	// mode, like the other source thresholds, even when the source
	// is disabled.
	if f.tokenPoll <= 0 {
		return errors.New("--token-poll must be > 0")
	}
	if f.burnMultiple <= 1 {
		return errors.New("--burn-multiple must be > 1 (a multiple at or below the baseline would fire on every session)")
	}
	if f.burnETA <= 0 {
		return errors.New("--burn-eta must be > 0")
	}
	if f.tokenBudgetUSD < 0 {
		return errors.New("--token-budget-usd must be >= 0 (0 = budget unknown)")
	}
	if strings.HasSuffix(f.tokenEndpoint, "/") {
		return fmt.Errorf("--token-endpoint must not end with '/' (got %q)", f.tokenEndpoint)
	}
	// The token-burn source needs a cost-stack endpoint: normally
	// --daemon-url (the §3 boundary rides the same daemon the
	// injector talks to), so this only bites --dry-run runs, which
	// skip --daemon-url — same loud-config posture as everything
	// above. With --sink=webhook the source never runs (it requires
	// the core-agent sink; buildSources idles it with a loud startup
	// message), so the endpoint requirement doesn't apply.
	if f.sink == sinkCoreAgent && f.sourceEnabled(tokenburn.Name) && f.tokenEndpoint == "" && f.daemonURL == "" {
		return fmt.Errorf("--sources: %s requires --daemon-url or --token-endpoint (the §12 cost stack is the core-agent daemon's attach API)", tokenburn.Name)
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
	// A floor well above any conceivable identity-only payload: below it
	// the fit would truncate every message to nothing (or fail to fit at
	// all), which is not a configuration anyone means to ask for.
	if f.injectMaxBytes < 1024 {
		return errors.New("--inject-max-bytes must be >= 1024")
	}
	if f.enrichLogLines < 1 {
		return errors.New("--enrich-log-lines must be >= 1")
	}
	if f.enrichTimeout <= 0 {
		return errors.New("--enrich-timeout must be > 0")
	}
	if _, err := state.ParseListSelection(f.enrichLists); err != nil {
		return fmt.Errorf("--enrich-lists: %w", err)
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
	if f.triageRegressFactor != 0 && f.triageRegressFactor < 2 {
		return errors.New("--triage-regress-factor must be >= 2 (a factor of 1 would fire on the first duplicate; 0 disables)")
	}
	if f.distillInterval < 0 {
		return errors.New("--distill-interval must be >= 0 (0 disables distillation)")
	}
	if f.graphSnapshotInterval <= 0 {
		return errors.New("--graph-snapshot-interval must be > 0")
	}
	switch f.mode {
	case "per-incident":
		if f.dryRun {
			return nil
		}
		if f.sink == sinkCoreAgent && f.owner == "" {
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

// stormEnabled reports whether §7.5 storm correlation is on:
// --storm resolved (or set) to on AND a non-zero correlation window.
// Meaningful only after resolveAutoDefaults has replaced a --storm=auto
// with its resolution — before that, auto reads as not-enabled.
func (f *flags) stormEnabled() bool { return f.storm == stormOn && f.stormWindow > 0 }

// sourcesAuto reports whether --sources is the auto sentinel value,
// i.e. startup must resolve the portable set before building sources.
func (f *flags) sourcesAuto() bool { return f.sources == autoValue }

// knownSources are the --sources names, in the §7.2 table order.
var knownSources = []string{k8sevents.Name, objectstate.Name, rollout.Name, workload.Name, autoscaling.Name, saturation.Name, degradation.Name, expiry.Name, capacity.Name, ingress.Name, gateway.Name, quota.Name, notifications.Name, tokenburn.Name}

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

// Get makes severityFlag a flag.Getter so FlagInventory can type the
// flag ([]string → "repeatable") without special-casing its name.
func (v *severityFlag) Get() any { return v.values }

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
