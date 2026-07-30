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
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/memory/distill"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/degradation"
	"github.com/go-steer/k8s-lookout/pkg/sources/expiry"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/notifications"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/quota"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
	"github.com/go-steer/k8s-lookout/pkg/sources/tokenburn"
	"github.com/go-steer/k8s-lookout/pkg/sources/workload"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// Composition root: realMain's construction phase — flags resolved,
// clients built, sources registered (§7.2), recovery wired (§7.4),
// and the dispatcher assembled — plus the pieces only startup uses.

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

	// Resolve the sink's bearer token from env (unless dry-run):
	// --token-env for the core-agent daemon (required), --sink-token-env
	// for the webhook receiver (optional — but a NAMED env var must be
	// non-empty, same loud posture).
	var token string
	if !f.dryRun && f.sink == sinkCoreAgent {
		token = os.Getenv(f.tokenEnv)
		if token == "" {
			return fmt.Errorf("bearer token env var %s is empty", f.tokenEnv)
		}
	}
	var sinkToken string
	if !f.dryRun && f.sink == sinkWebhook && f.sinkTokenEnv != "" {
		sinkToken = os.Getenv(f.sinkTokenEnv)
		if sinkToken == "" {
			return fmt.Errorf("bearer token env var %s is empty", f.sinkTokenEnv)
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

	// Agent sink selection (docs/agent-sink-design.md): the core-agent
	// daemon client by default — byte-identical wire to every release
	// before the Sink extraction — or the generic webhook sink.
	var inj inject.Sink
	if !f.dryRun {
		switch f.sink {
		case sinkWebhook:
			if strings.HasPrefix(f.sinkURL, "http://") {
				log.Printf("sink: webhook receiver %s uses plain http — incident payloads and the bearer token ride unencrypted; use https for anything beyond a trusted network", f.sinkURL)
			}
			ws, werr := inject.NewWebhookSink(inject.WebhookConfig{
				URL:         f.sinkURL,
				BearerToken: sinkToken,
			})
			if werr != nil {
				return fmt.Errorf("webhook sink: %w", werr)
			}
			inj = ws
		default:
			ci, cerr := inject.NewInjector(inject.Config{
				DaemonURL:      f.daemonURL,
				BearerToken:    token,
				AssertedCaller: f.owner,
			})
			if cerr != nil {
				return fmt.Errorf("injector: %w", cerr)
			}
			inj = ci
		}
	}
	// Which sink this process delivers to, as a scrapeable info gauge
	// (the frozen operation counters stay sink-agnostic — see
	// metrics.go on why the sink dimension is not a new label).
	m.sinkInfo.WithLabelValues(f.sink).Set(1)

	// §8 deployment identity: cluster is --cluster-name (M0);
	// zone/project resolve by precedence — explicit flag > provider
	// metadata > empty (never fatal; empty fields reproduce the
	// zone-less fingerprints deployments hashed before this wiring).
	idCtx, cancelID := context.WithTimeout(context.Background(), 15*time.Second)
	project, zone := resolveIdentity(idCtx, f)
	cancelID()
	if project != "" || zone != "" {
		log.Printf("identity: stamping project=%q zone=%q (precedence: explicit flag > provider metadata > empty; zone participates in the §8 fingerprint hash)", project, zone)
	}

	disp := &dispatcher{
		filter:    filter,
		dedup:     dedup,
		injector:  inj,
		metrics:   m,
		cluster:   f.clusterName,
		project:   project,
		zone:      zone,
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
		// §9.4 triage-status records ride the same store: severity
		// routing honors agent overrides (per-incident mode), and
		// recovery flips records to resolved in every mode.
		disp.triage = newTriageOverrides(occStore, m, f.triageRegressFactor)
		log.Printf("triage-status: enabled (open records refine routing every signal, cache refresh %s; recovery flips records to resolved; regression evidence at %dx the downgrade-time rate)",
			triageRefreshInterval, f.triageRegressFactor)
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

	// §9.2 distiller: the scheduled pass converting recurring
	// occurrences into durable memory facts. Requires --store (both
	// its input window and the in-tree memory backend live there);
	// without one, distillation is off and says so once.
	if occStore != nil && f.distillInterval > 0 {
		go runDistillLoop(ctx, occStore, f.distillInterval, m)
		log.Printf("distill: enabled (pass every %s over the last %s of occurrences → memory facts in %s)",
			f.distillInterval, distill.DefaultWindow, f.store)
	} else if f.distillInterval > 0 {
		log.Printf("distill: disabled (requires --store; distilled facts live in the sentinel store — see pkg/memory)")
	}

	// Watchboard interval-flush loop (§7.7): flushes buffered
	// warnings at --watchboard-flush age, plus a final best-effort
	// flush on shutdown so a terminating sentinel keeps its buffer.
	var waitBoard func()
	if disp.board != nil {
		waitBoard = startBoard(ctx, disp.board)
		log.Printf("watchboard: enabled (batch=%d, flush=%s, rotate after %d digest injects — §15 Q2 size-based)",
			f.watchboardBatch, f.watchboardFlush, f.watchboardRotate)
	}

	// Build the kube client. Dry-run runs the FULL watch pipeline —
	// informers, sources, filter/dedup/routing — against the real
	// cluster; only the sink deliveries are replaced by printing
	// payloads to stdout (the dispatcher's dryRun branches). The M0
	// behavior of skipping the kube client entirely made --dry-run a
	// flag-validation no-op that watched nothing — adopted the fix
	// from kube-agents' watcher: a dry run you cannot point at a
	// cluster and SEE payloads from is not a dry run.
	if f.dryRun {
		log.Printf("k8s-event-watcher: --dry-run: watching cluster %q; inject payloads print to stdout, no daemon/sink calls", f.clusterName)
	}
	client, err := kube.BuildClient(kube.Options{InCluster: f.inCluster, Kubeconfig: f.kubeconfig})
	if err != nil {
		return err
	}

	// Auto defaults (--sources=auto / --storm=auto): resolve BEFORE
	// any source-conditional client building below — the rest of
	// startup then sees a concrete source list and a concrete storm
	// on/off, exactly as if the operator had pinned them. Explicit
	// values skip this entirely and keep the frozen §11 semantics
	// (named source or --storm=on probe failure = fatal). Auto is the
	// ONLY mode that downgrades a miss to skip-with-loud-line.
	if err := resolveAutoDefaults(ctx, f, client); err != nil {
		return err
	}

	// Signal sources (§7.2), individually enabled via --sources
	// (auto resolves to the supported portable set above; an explicit
	// list is honored verbatim). The dynamic
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
	// The cloud provider (§2 boundary) exists for the capacity
	// source's provider scale-decision sub-source (§10.1 source 3)
	// and the quota source (§10.2). On a default (untagged) build or
	// off-cloud, cloud.New resolves to the NoProvider sentinel: the
	// capacity sub-source then reports itself unavailable explicitly
	// (never silently, §2), while the quota source REFUSES to start —
	// quota.New's loud §11 error — because a project-tier deployment
	// without a cloud makes no sense.
	var provider cloud.Provider
	if f.sourceEnabled(capacity.Name) || f.sourceEnabled(quota.Name) || f.sourceEnabled(notifications.Name) {
		provider, err = cloud.New(ctx, cloud.Config{Cluster: f.clusterName, NotificationsSubscription: f.notificationsSub})
		if err != nil {
			return fmt.Errorf("cloud provider: %w", err)
		}
		log.Printf("cloud: provider %q selected (%d compiled in)", provider.Name(), len(cloud.Registered()))
	}
	bs, err := buildSources(f, token, client, dyn, metricsClient, provider)
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
		if bs.workload != nil {
			bs.workload.WithFactory(sharedFactory)
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
	} else if f.storm == stormOn {
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
	probeNotes, err := sources.Probe(ctx, sources.NewAccessReviewer(client), registry.All()...)
	for _, note := range probeNotes {
		// Optional-requirement denials (#145): the source runs with
		// one dimension degraded — reported here, never silent.
		log.Printf("%s", note)
	}
	if err != nil {
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

	if f.sink == sinkWebhook {
		log.Printf("k8s-event-watcher: starting on cluster %q → webhook sink %s (POST /incidents + /incidents/<id>/events, schema-v1 payload bodies)",
			f.clusterName, f.sinkURL)
	} else {
		log.Printf("k8s-event-watcher: starting on cluster %q → daemon %s (mode=%s, owner=%s)",
			f.clusterName, f.daemonURL, f.mode, f.owner)
	}
	err = sources.RunAll(ctx, registry.All(), func(sig engine.Signal) {
		disp.DispatchSignal(ctx, sig)
	})
	// Final snapshot on shutdown so any un-persisted dedup state
	// isn't lost. Best-effort — failure is logged, not fatal.
	if snapErr := dedup.Snapshot(); snapErr != nil {
		log.Printf("dedup snapshot on shutdown: %v", snapErr)
	}
	// Block on the watchboard's final flush (§7.7, issue #108): run's
	// shutdown branch does a best-effort FlushNow when ctx cancels, and
	// this join keeps the process alive until it lands so buffered
	// warnings aren't dropped on SIGTERM. Nil-guarded: the board is
	// optional (per-incident mode without warning routing).
	if waitBoard != nil {
		waitBoard()
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
	workload    *workload.Source
	saturation  *saturation.Source
	degradation *degradation.Source
	expiry      *expiry.Source
	capacity    *capacity.Source
	quota       *quota.Source
	notes       *notifications.Source
	tokenBurn   *tokenburn.Source
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
// metrics.k8s.io dimension rides a separate clientset. provider is
// the §2 cloud boundary for the capacity source; nil is tolerated
// (treated as cloud.NoProvider — explicit unavailability, §2).
// daemonToken is the resolved --token-env bearer token, reused by the
// token-burn source's cost-stack client (§3: same daemon, same auth
// as the inject path); empty means authless.
func buildSources(f *flags, daemonToken string, client kubernetes.Interface, dyn dynamic.Interface, metricsClient metricsv.Interface, provider cloud.Provider) (*builtSources, error) {
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
		case workload.Name:
			bs.workload = workload.New(client, workload.DefaultConfig())
			src = bs.workload
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
		case capacity.Name:
			cfg := capacity.DefaultConfig()
			cfg.PollInterval = f.capacityPoll
			cfg.PendingAge = f.pendingAge
			bs.capacity = capacity.New(client, provider, cfg)
			src = bs.capacity
		case quota.Name:
			// §10.2/§11: the quota source is the Project-tier
			// deployment — quota.New fails LOUDLY (naming the source
			// and the missing capability) when the provider cannot
			// serve quota, and that error stops startup here; there
			// is no degraded quota mode.
			cfg := quota.DefaultConfig()
			cfg.Poll = f.quotaPoll
			cfg.Window = f.quotaWindow
			cfg.WarnPct = f.quotaWarn
			q, err := quota.New(provider, cfg)
			if err != nil {
				return nil, err
			}
			bs.quota = q
			src = bs.quota
		case notifications.Name:
			// Post-M5 #130: explicit-only project-tier source, the
			// quota posture — New fails LOUDLY (naming the source and
			// the missing capability/subscription) when the provider
			// cannot serve the stream; no degraded mode.
			n, err := notifications.New(provider, notifications.DefaultConfig())
			if err != nil {
				return nil, err
			}
			bs.notes = n
			src = bs.notes
		case tokenburn.Name:
			// The token-burn source requires the core-agent sink: its
			// §12 cost stack IS the core-agent daemon's attach API.
			// With --sink=webhook the sentinel idles the source with a
			// loud startup message instead of failing — the same
			// posture as distillation without --store.
			if f.sink == sinkWebhook {
				log.Printf("token-burn: disabled (--sink=webhook — the §12 cost stack is the core-agent daemon's attach API; the source requires --sink=core-agent)")
				continue
			}
			// §12: the cost stack rides the same daemon the
			// injector talks to; --token-endpoint overrides for
			// split deployments. validate() already required one of
			// the two, so this branch never sees both empty outside
			// a programming error.
			endpoint := f.tokenEndpoint
			if endpoint == "" {
				endpoint = f.daemonURL
			}
			if endpoint == "" {
				return nil, fmt.Errorf("--sources: %s requires --daemon-url or --token-endpoint", tokenburn.Name)
			}
			cfg := tokenburn.DefaultConfig()
			cfg.Poll = f.tokenPoll
			cfg.BurnMultiple = f.burnMultiple
			cfg.BurnETA = f.burnETA
			cfg.BudgetUSD = f.tokenBudgetUSD
			bs.tokenBurn = tokenburn.New(tokenburn.NewHTTPClient(endpoint, daemonToken), cfg)
			src = bs.tokenBurn
			log.Printf("token-burn: cost stack endpoint %s (core-agent v2.7.0 GET /sessions + per-session /usage; budget=%s)",
				endpoint, budgetDesc(f.tokenBudgetUSD))
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

// budgetDesc renders the --token-budget-usd startup-log value: the
// budget trigger's arming state must be visible at startup, not
// discovered from silence (§11 posture; the budget is lookout-side
// config until core-agent exposes its CostCeiling — see
// pkg/sources/tokenburn's TODO(core-agent)).
func budgetDesc(usd float64) string {
	if usd <= 0 {
		return "unknown — budget trigger disarmed, rate trigger only"
	}
	return fmt.Sprintf("$%.2f/session", usd)
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
	if bs.tokenBurn != nil {
		observers = append(observers, bs.tokenBurn.ClearanceObserver())
		log.Printf("recovery: token-burn clearance observer registered (spend receded / session ended → cleared)")
	}
	if bs.workload != nil {
		observers = append(observers, bs.workload.ClearanceObserver())
		log.Printf("recovery: workload clearance observer registered (sibling cron run succeeded / schedule resumed / object deleted → cleared)")
	}
	if bs.objState != nil {
		observers = append(observers, bs.objState.ClearanceObserver())
		log.Printf("recovery: clearance observer backed by the object-state source's pod + node informers")
	} else {
		reviewer := sources.NewAccessReviewer(client)
		podRBAC := true
		for _, req := range recoveryAccess {
			d, err := reviewer.Allowed(ctx, req)
			if err != nil {
				return fmt.Errorf("recovery: capability probe for %q failed: %w", req, err)
			}
			if !d.Allowed {
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
