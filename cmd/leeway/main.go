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

// Command leeway runs the placement subsystem — the topology-drift and
// compute-class sources of docs/leeway-design.md — as a standalone,
// metrics-only process.
//
// It is a deliberate exception to DESIGN.md §4.1's "one multicall
// binary", recorded there as such: an optional artifact, not a second
// user-facing CLI surface. §2.4 is what it exists for — clusters that do
// not run lookout, a deployment whose consumer is Grafana and a rotation
// rather than an agent, and isolating the subsystem under kwok for the
// scale runs. Keeping it building is also what keeps §2.4's four
// disciplines honest: the day a source reaches into the sentinel, this
// binary stops compiling.
//
// It must NOT run alongside `lookout watch` against the same cluster.
// Both build a Pod informer, and a second one on the largest stream in
// the cluster is exactly the duplicate watch §2.1 exists to avoid. Run
// one or the other; `lookout watch` already contains everything this
// does, and more.
//
// There is no inject path here and no store: emit writes a metric and a
// line on stderr, and the findings are the /metrics endpoint. Running
// store-less is supported by §9.1 — current state is rebuildable from
// the informers, so the cost of a restart is the in-flight dwell timers,
// not correctness.
//
// The flag surface is deliberately a fraction of `lookout watch`'s. The
// knobs here are the ones that change what is watched or what it costs;
// everything else takes the package defaults. An operator who needs the
// full surface wants the sentinel.
//
// Exit codes follow the §4.2 CLI contract: 0 clean shutdown, 1 runtime
// error, 2 usage error.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel/metric"

	"github.com/go-steer/k8s-lookout/internal/telemetry"
	"github.com/go-steer/k8s-lookout/internal/version"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/computeclass"
	"github.com/go-steer/k8s-lookout/pkg/sources/topologydrift"
)

// DefaultMetricsAddr matches the --metrics-addr the shipped sentinel
// manifests pass. The two binaries never share a pod, so reusing the
// port means a dashboard, a ServiceMonitor and a port-forward written
// for one work unchanged against the other.
const DefaultMetricsAddr = ":9090"

// options is the whole flag surface. Kept as one struct so the smoke
// test can drive realMain the way an operator drives the binary —
// through argv — rather than through a Go API no deployment has.
type options struct {
	kubeconfig  string
	kubeContext string
	inCluster   bool
	cluster     string

	metricsAddr  string
	otelExporter string

	sources string
	// sourcesNamed records whether --sources was passed rather than
	// defaulted. It is the same distinction the sentinel's auto mode
	// draws: a source the operator NAMED must run or the process must
	// fail, because they are relying on it; one that came from the
	// default is a candidate, and a cluster that cannot serve it is a
	// fact about the cluster rather than a misconfiguration.
	sourcesNamed bool

	topologyKeys      string
	topologyDefaults  string
	topologyPerDomain bool
	topologyTierC     bool
	computeClassTierC bool

	exitAfter   time.Duration
	showVersion bool
}

func main() { os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr)) }

// realMain is main with the process edges passed in. It returns the
// §4.2 exit code and never calls os.Exit itself.
func realMain(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("leeway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options

	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Empty uses the usual resolution (KUBECONFIG, then ~/.kube/config), or the in-cluster service account under --in-cluster.")
	fs.StringVar(&o.kubeContext, "context", "", "Context in the kubeconfig to use instead of its current-context. Never written back. Meaningless with --in-cluster.")
	fs.BoolVar(&o.inCluster, "in-cluster", false, "Force in-cluster service account credentials. The posture inside a Pod.")
	fs.StringVar(&o.cluster, "cluster-name", "", "Cluster identity, stamped as the `cluster` label on every exported series and as k8s.cluster.name on the OTLP resource. Set it whenever more than one of these processes reports to one backend, which is the moment two clusters become indistinguishable without it.")

	fs.StringVar(&o.metricsAddr, "metrics-addr", DefaultMetricsAddr, "Prometheus /metrics + /healthz + /readyz listener address (host:port). Port 0 binds a free port and the chosen one is logged. Empty disables the listener, which for this binary means it produces nothing observable at all — useful only with --otel-exporter=otlp.")
	fs.StringVar(&o.otelExporter, "otel-exporter", "none", "Push metrics over OTLP as well as serving them: `otlp` or `none`. The /metrics endpoint is unconditional either way; this only adds the push reader, configured by the standard OTEL_EXPORTER_OTLP_* environment.")

	fs.StringVar(&o.sources, "sources", strings.Join(defaultSources, ","), "Comma-separated sources to run. topology-drift is portable and reads only pods, nodes and replicasets; compute-class needs the GKE ComputeClass CRD and does nothing useful without it.")

	fs.StringVar(&o.topologyKeys, "topology-keys", strings.Join(defaultTopologyKeys(), ","), "Comma-separated node labels to treat as topology axes, in precedence order. The defaults are the two well-known labels; a cluster that partitions on a rack or cell label names it here.")
	fs.StringVar(&o.topologyDefaults, "topology-cluster-defaults", "", "Your cluster's kube-scheduler PodTopologySpread defaultConstraints, as `key=maxSkew[:DoNotSchedule|ScheduleAnyway]` comma-separated. Unset ASSUMES the upstream system defaults and caps every intent derived from them below critical; \"none\" asserts your cluster configures none. Same three states as the sentinel's flag of this name.")
	fs.BoolVar(&o.topologyPerDomain, "topology-per-domain-series", false, "Export the per-domain breakdown for EVERY tracked subject rather than only the drifting ones. Off by default because the count is multiplicative — see §8.4 and lookout_leeway_domain_series_withheld for what is being withheld and why.")
	fs.BoolVar(&o.topologyTierC, "topology-tier-c-signals", false, "Count Tier C topology-drift findings on the signals counter. Tier C is the tier where nobody declared anything, so a breach says \"this changed\" rather than \"this is wrong\"; it is metrics-only by default here as in the sentinel.")
	fs.BoolVar(&o.computeClassTierC, "compute-class-tier-c-signals", false, "Count Tier C compute-class findings (leeway.rank_tier_unused) on the signals counter. Metrics-only by default: an unused priority is frequently the intended configuration.")

	fs.DurationVar(&o.exitAfter, "exit-after", 0, "Shut down cleanly after this long, as though SIGTERM had arrived. 0 runs until signalled. This exists for bounded runs — the CI smoke test and the kwok scale harness — so neither has to kill the process to end it.")
	fs.BoolVar(&o.showVersion, "version", false, "Print the version and exit.")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: leeway [flags]\n\n"+
			"Runs the topology-drift and compute-class sources against one cluster and\n"+
			"exports their metrics. Metrics-only: there is no inject path and no store.\n\n"+
			"Do not run this against a cluster already running `lookout watch` — both\n"+
			"build a Pod informer, and the sentinel already does everything this does.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if o.showVersion {
		fmt.Fprintln(stdout, version.String("leeway"))
		return 0
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "sources" {
			o.sourcesNamed = true
		}
	})
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "leeway: unexpected argument %q (this binary takes flags only)\n", fs.Arg(0))
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, o, stderr); err != nil {
		fmt.Fprintf(stderr, "leeway: %v\n", err)
		return 1
	}
	return 0
}

// defaultSources is both sources: the binary's whole point is running
// the subsystem, and a compute-class source on a cluster with no
// ComputeClass CRD costs one discovery call and then sits idle.
var defaultSources = []string{topologydrift.Name, computeclass.Name}

// defaultTopologyKeys reads the axes off the source package rather than
// restating them, so this flag's help and the source's Config cannot
// disagree about what "the standard labels" are.
func defaultTopologyKeys() []string {
	out := make([]string, 0, len(topologydrift.DefaultTopologyKeys))
	for _, k := range topologydrift.DefaultTopologyKeys {
		out = append(out, string(k))
	}
	return out
}

func run(ctx context.Context, o options, stderr *os.File) error {
	if o.exitAfter > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.exitAfter)
		defer cancel()
	}

	// The cluster label is wrapped around the registry rather than set
	// on each instrument, for the same reason the sentinel does it: the
	// sources declare their instruments without knowing where they run,
	// and one wrapper cannot be forgotten on the next metric added.
	reg := prometheus.NewRegistry()

	// Go and process collectors, as the sentinel registers them: the
	// standalone's whole reason to exist is measuring leeway's cost in
	// isolation, and it cannot do that without exporting what it costs.
	// Unwrapped, deliberately — runtime metrics are the process's, not
	// the cluster's, and a cluster label on them would invite summing
	// across sentinels.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	var registerer prometheus.Registerer = reg
	if o.cluster != "" {
		registerer = prometheus.WrapRegistererWith(prometheus.Labels{"cluster": o.cluster}, reg)
	}

	mp, shutdownMetrics, err := telemetry.SetupMetrics(ctx, telemetry.MetricsOptions{
		Mode:       o.otelExporter,
		Registerer: registerer,
		Cluster:    o.cluster,
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownMetrics(shutdownCtx)
	}()

	em, err := newEmitter(registerer, stderr)
	if err != nil {
		return err
	}

	srcs, err := buildSources(o, mp, stderr)
	if err != nil {
		return err
	}

	// Serving starts before the sources do. A process whose informers
	// are still listing is up and not yet ready, and that window is the
	// one /readyz exists to describe — so it has to be answerable
	// during it, not after.
	//
	// A listener that dies takes the process with it, which is why the
	// two share a cancel. The whole output of this binary is that
	// endpoint: sources that keep running behind a dead one are a
	// process that looks healthy and tells nobody anything.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- serve(ctx, o.metricsAddr, reg, srcs, stderr)
		cancel()
	}()

	runErr := sources.RunAll(ctx, srcs, em.emit)
	// Cancelling here and not only in the deferred call is what makes a
	// source failure an exit rather than a hang: with the sources gone
	// the endpoint has nothing left to serve, and serve() returns on
	// ctx and on nothing else.
	cancel()
	// The listener's error is preferred when both have one: a source
	// failing during a shutdown the listener caused is a consequence,
	// and reporting the consequence hides the cause.
	if err := <-serveErr; err != nil {
		return err
	}
	return runErr
}

// buildSources constructs the enabled sources against their own clients
// and their own informer factories. Each source builds its factory
// itself when none is injected (§2.4 discipline 2 read the other way
// round: injectable, not injected), which is what keeps this function
// short enough to be obviously correct.
func buildSources(o options, mp metric.MeterProvider, stderr *os.File) ([]sources.Source, error) {
	names := splitCSV(o.sources)
	if len(names) == 0 {
		return nil, errors.New("--sources names nothing to run")
	}

	kopts := kube.Options{InCluster: o.inCluster, Kubeconfig: o.kubeconfig, Context: o.kubeContext}
	client, err := kube.BuildClient(kopts)
	if err != nil {
		return nil, err
	}
	dyn, err := kube.BuildDynamicClient(kopts)
	if err != nil {
		return nil, err
	}

	var out []sources.Source
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			return nil, fmt.Errorf("--sources names %q twice", name)
		}
		seen[name] = true
		switch name {
		case topologydrift.Name:
			keys := o.topologyKeys
			parsed, perr := topologydrift.ParseClusterDefaults(o.topologyDefaults)
			if perr != nil {
				return nil, perr
			}
			topoKeys := topologyKeysFrom(keys)
			if len(topoKeys) == 0 {
				return nil, errors.New("--topology-keys must name at least one node label")
			}
			cfg := topologydrift.Config{
				TopologyKeys:              topoKeys,
				ClusterDefaultConstraints: parsed,
				PerDomainSeries:           o.topologyPerDomain,
				TierCSignals:              o.topologyTierC,
			}
			src := topologydrift.New(client, cfg)
			// Optional, exactly as in the sentinel: a nil dynamic client
			// would just mean no LeewayPolicy watch, which is the state
			// of every cluster that never installed the CRD.
			src.WithDynamic(dyn)
			src.WithMeter(mp.Meter(topologydrift.MeterName))
			out = append(out, src)
		case computeclass.Name:
			// The discovery gate the sentinel's auto mode applies, for
			// the same reason and with the same asymmetry. compute-class
			// reads a GKE CRD and its Run fails loudly when the cluster
			// does not serve it; that is right when somebody asked for
			// it and wrong when it merely came from the default, which
			// would make the default configuration fail on kind, on kwok
			// and on every cluster that is not GKE.
			if !o.sourcesNamed && !computeclass.ComputeClassesServed(client) {
				fmt.Fprintf(stderr, "leeway: source %s skipped — this cluster does not serve the GKE ComputeClass CRD (pass --sources=%s to make that a startup error instead)\n",
					name, name)
				continue
			}
			cfg := computeclass.DefaultConfig()
			cfg.TierCSignals = o.computeClassTierC
			src, cerr := computeclass.New(client, dyn, cfg)
			if cerr != nil {
				return nil, cerr
			}
			src.WithMeter(mp.Meter(computeclass.MeterName))
			out = append(out, src)
		default:
			return nil, fmt.Errorf("--sources: unknown source %q (this binary runs %s)",
				name, strings.Join(defaultSources, " and "))
		}
		fmt.Fprintf(stderr, "leeway: source %s enabled\n", name)
	}
	if len(out) == 0 {
		return nil, errors.New("every source was skipped: this process would export an empty endpoint forever, which is indistinguishable from a healthy cluster")
	}
	return out, nil
}

func topologyKeysFrom(csv string) []leeway.TopologyKey {
	fields := splitCSV(csv)
	out := make([]leeway.TopologyKey, 0, len(fields))
	for _, f := range fields {
		out = append(out, leeway.TopologyKey(f))
	}
	return out
}

// splitCSV splits a comma-separated flag value, trimming spaces and
// dropping empties, so "a, ,b," is two values rather than four.
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
