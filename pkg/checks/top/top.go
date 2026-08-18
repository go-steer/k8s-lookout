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

// Package top implements `lookout triage top` (DESIGN.md §5):
// CPU/memory saturation vs limits, RIGHT NOW. Point-in-time only by
// design — v2 top-analyzer's slope→ETA math needs a time series, and
// that lives in the sentinel's saturation source (§7.2), which owns
// one; this command reports the instant from a single metrics.k8s.io
// read over the requested scope, reusing the saturation source's
// fetcher seam so the metrics-client join exists exactly once.
//
// # The severity asymmetry (deliberate, documented)
//
// Memory and CPU limits fail differently, so the same percentage
// must not carry the same severity:
//
//   - MEMORY is incompressible: usage at the limit is an OOM kill.
//     ≥ --top-warn (default 80%) is a warning; ≥95% is CRITICAL.
//   - CPU is compressible: usage at the limit is throttling — degraded,
//     never dead. A point-in-time sample also cannot distinguish a
//     harmless burst from sustained starvation (that proof needs a
//     window, i.e. --history or the sentinel). CPU therefore CAPS AT
//     WARNING here, no matter the percentage.
//
// Zero nominal state (§4.2): only rows at/above --top-warn become
// findings; --all dumps every sampled row (info below the threshold,
// sorted by pct descending, capped) for exploratory use. Containers
// without limits are invisible to a usage-vs-limit judgment, so they
// are counted in one aggregate info finding (--show-unlimited lists
// them). With -A the node dimension is added: usage vs allocatable
// per node, the node-pressure precursor.
//
// # The two censuses, and why one contains the other
//
// Two aggregate info findings count containers missing a resource
// spec, on the same walk (#235):
//
//   - top.unlimited — no cpu/memory LIMIT. These cannot be judged
//     against a ceiling, so they are invisible to everything above.
//   - top.unrequested — no cpu/memory REQUEST. The scheduler
//     bin-packs these as zero, which is the direct cause of the
//     FailedScheduling churn, noisy-neighbour eviction and bad
//     packing this command otherwise reports only as symptoms.
//
// unrequested is a strict SUBSET of unlimited, and not by accident:
// the apiserver copies a limit down into an unset request, so a
// container with no request necessarily has no limit either. The
// subset is the worse half — no ceiling AND no floor.
//
// # LimitRange
//
// A namespace LimitRange can supply both values at admission, so
// either census could be qualified by one. It is loaded (the only
// LimitRange read in the tree) and applied as an ANNOTATION, not a
// suppression, because this command reads LIVE PODS: LimitRanger is a
// mutating plugin that writes its defaults into the spec at CREATE
// and never touches existing pods. A live pod still missing a request
// in a defaulting namespace therefore genuinely has none — it
// predates the LimitRange — and dropping it would hide a real
// problem. Naming the LimitRange instead says what the fix is:
// recreate the pod. See pkg/checks/state/limitrange.go for the case
// where suppression IS correct (pod templates, which admission has
// not touched).
//
// --history=<dur> goes through the pkg/cloud provider boundary
// (Metrics capability): max/avg/p95 usage-vs-limit over the window
// per container finding. No provider → the §2 explicit unavailable
// finding + summary marker; the point-in-time output is unaffected.
package top

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

func init() {
	checks.Register(New(Deps{}))
}

const (
	// defaultWarnPct is the --top-warn default: the §5 "right now"
	// attention line.
	defaultWarnPct = 80
	// memCritPct is where memory turns critical: this close to the
	// limit, one allocation burst is an OOM kill. Deliberately not a
	// flag — the warn/crit asymmetry vs CPU is the point of the
	// command, not a tunable.
	memCritPct = 95.0
	// defaultAllLimit caps the --all exploratory dump.
	defaultAllLimit = 50
)

// Backend-neutral series names `--history` asks the provider
// MetricsBackend for (cloud.SeriesQuery is deliberately not a query
// language, §15 Q4). The GKE backend translates them to Cloud
// Monitoring metric types in M4.
const (
	HistoryMetricCPU    = "container/cpu/used_millicores"
	HistoryMetricMemory = "container/memory/used_bytes"
)

// Deps are the injectable seams. The zero value is the production
// wiring; tests inject fakes (§13).
type Deps struct {
	// Client yields the core Kubernetes client. Nil means
	// kube.DefaultSource.
	Client kube.ClientSource
	// Metrics yields the metrics.k8s.io client. Nil resolves the
	// same kubeconfig kube.DefaultSource uses.
	Metrics func(ctx context.Context) (metricsv.Interface, error)
	// Provider yields the cloud provider for --history. Nil means
	// cloud.New default detection (NoProvider on vanilla builds —
	// --history then reports unavailable, never silence, §2).
	Provider func(ctx context.Context) (cloud.Provider, error)
	// Now anchors the --history window. Nil means time.Now.
	Now func() time.Time
}

func (d Deps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.DefaultSource()(ctx)
}

func (d Deps) metrics(ctx context.Context) (metricsv.Interface, error) {
	if d.Metrics != nil {
		return d.Metrics(ctx)
	}
	cfg, err := kube.BuildConfig(kube.OptionsFrom(ctx))
	if err != nil {
		return nil, err
	}
	mc, err := metricsv.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("metrics client: %w", err)
	}
	return mc, nil
}

func (d Deps) provider(ctx context.Context) (cloud.Provider, error) {
	if d.Provider != nil {
		return d.Provider(ctx)
	}
	return cloud.New(ctx, cloud.Config{})
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// New builds the `triage top` command around deps.
func New(deps Deps) checks.Command {
	return checks.Command{
		Name:    "triage top",
		MCPName: "k8s_resource_top",
		Summary: "Point-in-time CPU/memory saturation vs limits: kubectl top, but judged — usage-vs-limit percent per container with the OOM asymmetry built in (memory ≥95% of limit is critical, CPU caps at warning: it throttles, it does not kill); -A adds node usage vs allocatable. Trends/ETAs live in the sentinel's saturation source; --history adds window stats via the cloud provider.",
		Flags: []emit.FlagSpec{
			{Name: "top-warn", Type: emit.FlagInt, Default: strconv.Itoa(defaultWarnPct),
				Help: "report a container/node only at or above this percent of its limit/allocatable (zero nominal state; memory additionally turns critical at 95%)"},
			{Name: "all", Type: emit.FlagBool, Default: "false",
				Help: "exploratory dump: emit every sampled row regardless of --top-warn (info severity below it), sorted by pct descending; containers capped at --limit"},
			{Name: "limit", Type: emit.FlagInt, Default: strconv.Itoa(defaultAllLimit),
				Help: "row cap for the --all container dump"},
			{Name: "show-unlimited", Type: emit.FlagBool, Default: "false",
				Help: "list each container missing a cpu or memory limit individually (default: one aggregate count)"},
			{Name: "show-unrequested", Type: emit.FlagBool, Default: "false",
				Help: "list each container missing a cpu or memory request individually (default: one aggregate count); a missing request is the scheduler-side half of the census, always a subset of --show-unlimited"},
			{Name: "history", Type: emit.FlagDuration, Default: "0s",
				Help: "enrich container findings with max/avg/p95 usage-vs-limit over this window via the cloud provider metrics backend; no provider → explicit unavailable finding + summary marker, point-in-time output unaffected"},
		},
		Kinds: []checks.KindField{
			checks.Kind("top.saturation", "a container's usage is close to its limit — critical near the limit, info for an --all row below the threshold", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind("top.node", "a node's allocatable is close to committed — critical near the limit, info for an --all row below the threshold", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind("top.unlimited", "how many containers in scope set no cpu/memory limit, and are therefore invisible to saturation analysis", emit.SeverityInfo),
			checks.Kind("top.unlimited_container", "one container that sets no cpu/memory limit (--show-unlimited)", emit.SeverityInfo),
			checks.Kind("top.unrequested", "how many containers in scope set no cpu/memory request, so the scheduler bin-packs them as zero", emit.SeverityInfo),
			checks.Kind("top.unrequested_container", "one container that sets no cpu/memory request (--show-unrequested)", emit.SeverityInfo),
			checks.CloudUnavailableKind(),
		},
		Output: []checks.OutputField{
			{Name: "resource", Doc: "the judged dimension: cpu or memory"},
			{Name: "usage", Doc: "current usage in the dimension's natural unit (millicores for cpu, IEC bytes for memory)"},
			{Name: "limit", Doc: "the container's configured limit (top.saturation), same unit as usage"},
			{Name: "allocatable", Doc: "the node's allocatable capacity (top.node), same unit as usage"},
			{Name: "pct", Doc: "usage as a percent of the limit/allocatable, one decimal"},
			{Name: "container", Doc: "container name within the pod"},
			{Name: "node", Doc: "node the pod runs on (top.saturation; top.node carries the node as name)"},
			{Name: "pods", Doc: "top.unlimited/top.unrequested: pods in scope with at least one container missing a cpu or memory limit (resp. request)"},
			{Name: "containers", Doc: "top.unlimited/top.unrequested: containers in scope missing a cpu or memory limit (resp. request)"},
			{Name: "missing", Doc: "top.unlimited_container/top.unrequested_container: which dimensions are absent (cpu, memory, or both)"},
			{Name: "limitrange", Doc: "top.unlimited_container/top.unrequested_container: the namespace LimitRange(s) that default a dimension this container is missing — the pod predates them, so recreating it picks the value up"},
			{Name: "limitrange_defaulted", Doc: "top.unlimited/top.unrequested: how many of the counted containers sit in a namespace whose LimitRange now defaults the dimension they lack"},
			{Name: "max_pct", Doc: "--history: highest usage-vs-limit percent observed in the window"},
			{Name: "avg_pct", Doc: "--history: mean usage-vs-limit percent over the window"},
			{Name: "p95_pct", Doc: "--history: 95th-percentile usage-vs-limit percent over the window"},
			{Name: "capability", Doc: "cloud.unavailable: the provider capability --history needed (metrics)"},
			{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
			{Name: "history", Doc: "summary-line note: the --history window the stats cover"},
			{Name: "unavailable", Doc: "summary-line note (§2 marker): why --history could not be served"},
		},
		Examples: []string{
			"lookout triage top --namespace=prod",
			"lookout triage top -A",
			"lookout triage top --workload=Deployment/prod/api",
			"lookout triage top --namespace=prod --all --limit=20",
			"lookout triage top -A --top-warn=90 --show-unlimited",
			"lookout triage top -A --show-unrequested",
			"lookout triage top --namespace=prod --history=1h --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return run(ctx, deps, inv)
		},
	}
}

// canonicalKinds maps the accepted --workload kind spellings to the
// canonical names state.LoadCluster resolves (the usual short forms,
// matching the other triage commands).
var canonicalKinds = map[string]string{
	"pod": "Pod", "pods": "Pod", "po": "Pod",
	"deployment": "Deployment", "deployments": "Deployment", "deploy": "Deployment",
	"replicaset": "ReplicaSet", "replicasets": "ReplicaSet", "rs": "ReplicaSet",
	"statefulset": "StatefulSet", "statefulsets": "StatefulSet", "sts": "StatefulSet",
	"daemonset": "DaemonSet", "daemonsets": "DaemonSet", "ds": "DaemonSet",
	"job": "Job", "jobs": "Job",
	"cronjob": "CronJob", "cronjobs": "CronJob", "cj": "CronJob",
}

// rated is one (container, resource) sample with a limit, judged.
type rated struct {
	s   saturation.ContainerSample
	pct float64
}

// nodeRated is one (node, resource) usage-vs-allocatable sample.
type nodeRated struct {
	node        string
	resource    string
	used        float64
	allocatable float64
	pct         float64
}

// containerKey identifies one container across its cpu+memory
// samples.
type containerKey struct {
	namespace, pod, container string
}

func run(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	warn := inv.Flags.Int("top-warn")
	if warn < 1 || warn > 100 {
		return 0, emit.UsageErrorf("--top-warn must be a percent in 1..100, got %d", warn)
	}
	limit := inv.Flags.Int("limit")
	if limit < 1 {
		return 0, emit.UsageErrorf("--limit must be at least 1, got %d", limit)
	}
	history := inv.Flags.Duration("history")
	if history < 0 {
		return 0, emit.UsageErrorf("--history must not be negative, got %s", history)
	}
	all := inv.Flags.Bool("all")
	showUnlimited := inv.Flags.Bool("show-unlimited")
	showUnrequested := inv.Flags.Bool("show-unrequested")

	wl := inv.Scope.Workload
	switch {
	case wl.IsZero() && inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns>, -A (adds the node view), or --workload=<Kind>/<ns>/<name>")
	case !wl.IsZero() && inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("-A does not combine with --workload: a workload lives in one namespace (%s)", wl.Namespace)
	case !wl.IsZero() && inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace:
		return 0, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
	}
	if !wl.IsZero() {
		canonical, ok := canonicalKinds[strings.ToLower(wl.Kind)]
		if !ok {
			return 0, emit.UsageErrorf("unsupported workload kind %q (want Pod|Deployment|ReplicaSet|StatefulSet|DaemonSet|Job|CronJob)", wl.Kind)
		}
		wl.Kind = canonical
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	metrics, err := deps.metrics(ctx)
	if err != nil {
		return 0, err
	}

	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	if !wl.IsZero() {
		listNS = wl.Namespace
	}

	scanned := 0

	// Workload mode: resolve the member pod set through the same
	// one-List-pass graph `triage events`/`bundle` use (owner-chain
	// traversal — no name-prefix guessing).
	var podSet map[string]bool
	if !wl.IsZero() {
		cluster, err := state.LoadCluster(ctx, client, listNS)
		if err != nil {
			return 0, err
		}
		pods, err := cluster.WorkloadPods(wl)
		if err != nil {
			return 0, err
		}
		scanned += cluster.Scanned()
		podSet = make(map[string]bool, len(pods))
		for _, p := range pods {
			podSet[p.Namespace+"/"+p.Name] = true
		}
	}

	// THE one metrics.k8s.io read over the scope (§5), through the
	// saturation source's fetcher seam.
	fetcher := saturation.NewScopedMetricsPodFetcher(metrics, client, listNS)
	samples, err := fetcher.FetchPodUsage(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w — triage top needs metrics.k8s.io (install metrics-server)", err)
	}

	var entries []rated
	noLimit := map[containerKey][]string{}
	noRequest := map[containerKey][]string{}
	seen := map[containerKey]bool{}
	for _, s := range samples {
		if podSet != nil && !podSet[s.Namespace+"/"+s.Pod] {
			continue
		}
		key := containerKey{s.Namespace, s.Pod, s.Container}
		seen[key] = true
		// The request census is independent of the limit judgment:
		// a container can be judged against its limit and still have
		// no request (the apiserver only copies limits DOWN into
		// requests, never up), so this is recorded before the
		// unlimited short-circuit below.
		if s.Request <= 0 {
			noRequest[key] = append(noRequest[key], s.Resource)
		}
		if s.Limit <= 0 {
			noLimit[key] = append(noLimit[key], s.Resource)
			continue
		}
		entries = append(entries, rated{s: s, pct: s.Used / s.Limit * 100})
	}
	scanned += len(seen)

	// LimitRange, over the same scope: the annotation input for both
	// censuses (package comment). Only read when there is something
	// to qualify — a clean scope should not pay for the List.
	limitRanges := &state.LimitRangeDefaults{}
	if len(noLimit) > 0 || len(noRequest) > 0 {
		lr, examined, err := state.LoadLimitRanges(ctx, client, listNS)
		if err != nil {
			return 0, err
		}
		limitRanges, scanned = lr, scanned+examined
	}

	sort.Slice(entries, func(i, j int) bool { return ratedLess(entries[i], entries[j]) })

	// Judge containers; keep the rated entry next to its finding so
	// --history can enrich before emission.
	type judged struct {
		r rated
		f emit.Finding
	}
	var containerRows []judged
	for _, e := range entries {
		sev, above := judge(e.s.Resource, e.pct, warn)
		if !above && !all {
			continue
		}
		containerRows = append(containerRows, judged{r: e, f: containerFinding(e, sev, above)})
	}
	if all && len(containerRows) > limit {
		containerRows = containerRows[:limit]
	}

	// Node view (-A only): usage vs allocatable, the node-pressure
	// precursor. Point-in-time only, same asymmetry.
	var nodeRows []emit.Finding
	if inv.Scope.AllNamespaces {
		nodes, examined, err := fetchNodeUsage(ctx, metrics, client)
		if err != nil {
			return 0, err
		}
		scanned += examined
		sort.Slice(nodes, func(i, j int) bool { return nodeLess(nodes[i], nodes[j]) })
		for _, n := range nodes {
			sev, above := judge(n.resource, n.pct, warn)
			if !above && !all {
				continue
			}
			nodeRows = append(nodeRows, nodeFinding(n, sev, above))
		}
	}

	// --history: provider metrics over the same scope, window stats
	// per container finding. Unavailable is EXPLICIT (§2): a finding
	// plus the summary marker; the instant findings above are
	// emitted regardless.
	var unavailable *emit.Finding
	if history > 0 {
		provider, err := deps.provider(ctx)
		if err != nil {
			return 0, err
		}
		if backend, ok := provider.Metrics(); ok {
			now := deps.now()
			window := cloud.TimeWindow{Start: now.Add(-history), End: now}
			for i := range containerRows {
				if err := enrich(ctx, backend, &containerRows[i].f, containerRows[i].r, window); err != nil {
					return 0, err
				}
			}
			if err := inv.Out.Note("history", history.String()); err != nil {
				return 0, err
			}
		} else {
			u := cloud.Unavailable(provider, cloud.CapabilityMetrics)
			unavailable = &emit.Finding{
				Kind:     "cloud.unavailable",
				Severity: emit.SeverityInfo,
				Reason:   "CapabilityUnavailable",
				Message:  fmt.Sprintf("--history=%s needs the provider metrics backend: %s — point-in-time findings are unaffected", history, u.Reason),
				Details: []emit.Field{
					{Key: "capability", Value: string(u.Capability)},
					{Key: "provider", Value: u.Provider},
				},
			}
			if err := inv.Out.Note("unavailable", u.Reason); err != nil {
				return 0, err
			}
		}
	}

	// Emission order: the unavailable marker first (it qualifies
	// everything below), saturation rows (containers, then nodes),
	// then the no-limits census.
	if unavailable != nil {
		if err := inv.Out.Emit(*unavailable); err != nil {
			return 0, err
		}
	}
	for _, row := range containerRows {
		if err := inv.Out.Emit(row.f); err != nil {
			return 0, err
		}
	}
	for _, f := range nodeRows {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := emitCensus(inv.Out, unlimitedCensus, noLimit, showUnlimited, limitRanges); err != nil {
		return 0, err
	}
	if err := emitCensus(inv.Out, unrequestedCensus, noRequest, showUnrequested, limitRanges); err != nil {
		return 0, err
	}
	return scanned, nil
}

// judge maps one (resource, pct) to its severity and whether it
// crossed --top-warn. The asymmetry (package comment): memory ≥95%
// is critical — one allocation burst from an OOM kill; CPU never
// exceeds warning point-in-time — over-limit CPU is throttled, not
// killed, and a single sample cannot prove sustained starvation.
func judge(resource string, pct float64, warn int) (severity string, above bool) {
	if pct < float64(warn) {
		return emit.SeverityInfo, false
	}
	if resource == saturation.ResourceMemory && pct >= memCritPct {
		return emit.SeverityCritical, true
	}
	return emit.SeverityWarning, true
}

// containerFinding renders one judged container row. Below-threshold
// rows (--all) carry numbers only — no reason, no message (zero
// nominal state applies to fields too).
func containerFinding(e rated, severity string, above bool) emit.Finding {
	f := emit.Finding{
		Kind:         "top.saturation",
		Severity:     severity,
		Namespace:    e.s.Namespace,
		KindOfObject: "Pod",
		Name:         e.s.Pod,
		Details: []emit.Field{
			{Key: "container", Value: e.s.Container},
			{Key: "resource", Value: e.s.Resource},
			{Key: "usage", Value: fmtValue(e.s.Resource, e.s.Used)},
			{Key: "limit", Value: fmtValue(e.s.Resource, e.s.Limit)},
			{Key: "pct", Value: fmtPct(e.pct)},
		},
	}
	if e.s.Node != "" {
		f.Details = append(f.Details, emit.Field{Key: "node", Value: e.s.Node})
	}
	if !above {
		return f
	}
	if e.s.Resource == saturation.ResourceMemory {
		f.Reason = "MemoryNearLimit"
		if severity == emit.SeverityCritical {
			f.Message = fmt.Sprintf("memory at %s%% of limit — OOM-kill risk: memory is incompressible, the kernel kills at 100%%", fmtPct(e.pct))
		} else {
			f.Message = fmt.Sprintf("memory at %s%% of limit — OOM kill at 100%%; critical from %.0f%%", fmtPct(e.pct), memCritPct)
		}
	} else {
		f.Reason = "CPUNearLimit"
		f.Message = fmt.Sprintf("cpu at %s%% of limit — throttling risk only: CPU is compressible (over-limit throttles, never kills), so warning is this dimension's point-in-time ceiling", fmtPct(e.pct))
	}
	return f
}

// nodeFinding renders one judged node row.
func nodeFinding(n nodeRated, severity string, above bool) emit.Finding {
	f := emit.Finding{
		Kind:         "top.node",
		Severity:     severity,
		KindOfObject: "Node",
		Name:         n.node,
		Details: []emit.Field{
			{Key: "resource", Value: n.resource},
			{Key: "usage", Value: fmtValue(n.resource, n.used)},
			{Key: "allocatable", Value: fmtValue(n.resource, n.allocatable)},
			{Key: "pct", Value: fmtPct(n.pct)},
		},
	}
	if !above {
		return f
	}
	if n.resource == saturation.ResourceMemory {
		f.Reason = "NodeMemoryPressure"
		f.Message = fmt.Sprintf("node memory at %s%% of allocatable — kubelet eviction precursor", fmtPct(n.pct))
	} else {
		f.Reason = "NodeCPUPressure"
		f.Message = fmt.Sprintf("node cpu at %s%% of allocatable — scheduling/throttling pressure precursor", fmtPct(n.pct))
	}
	return f
}

// censusSpec is one missing-resource-spec census. The two differ only
// in which dimension they count and which LimitRange default would
// have supplied it, so they share the emission shape: always one
// aggregate info finding, plus the per-container listing behind the
// census's own --show-* flag.
type censusSpec struct {
	kind          string // aggregate finding kind
	containerKind string // per-container finding kind
	reason        string
	flag          string // the --show-* flag that lists containers
	dimension     string // the word for what is absent: "limit"/"request"
	// consequence completes "N container(s) in M pod(s) have no cpu
	// or memory ⟨dimension⟩ — ⟨consequence⟩".
	consequence string
	// defaults asks the index whether ns supplies an admission
	// default for this census's dimension of resource.
	defaults func(*state.LimitRangeDefaults, string, string) (string, bool)
}

var unlimitedCensus = censusSpec{
	kind:          "top.unlimited",
	containerKind: "top.unlimited_container",
	reason:        "NoLimits",
	flag:          "--show-unlimited",
	dimension:     "limit",
	consequence:   "invisible to saturation-vs-limit analysis",
	defaults:      (*state.LimitRangeDefaults).DefaultsLimit,
}

var unrequestedCensus = censusSpec{
	kind:          "top.unrequested",
	containerKind: "top.unrequested_container",
	reason:        "NoRequests",
	flag:          "--show-unrequested",
	dimension:     "request",
	consequence:   "the scheduler bin-packs them as zero, the cause behind FailedScheduling churn, noisy-neighbour eviction and bad packing",
	defaults:      (*state.LimitRangeDefaults).DefaultsRequest,
}

// emitCensus emits one census: the aggregate info finding whenever
// anything is missing (saying so is the honest answer — a silent
// census is indistinguishable from a clean one), plus the
// per-container listing when the census's flag is set.
//
// LimitRange annotates, never suppresses (package comment): these are
// live pods, so a defaulting namespace means the pod predates the
// LimitRange, not that the finding is wrong.
func emitCensus(out *emit.Writer, spec censusSpec, missing map[containerKey][]string, list bool, lr *state.LimitRangeDefaults) error {
	if len(missing) == 0 {
		return nil
	}
	pods := map[string]bool{}
	keys := make([]containerKey, 0, len(missing))
	defaulted := 0
	// annotations[k] names the LimitRange(s) that would have supplied
	// a dimension k is missing; empty when none applies.
	annotations := make(map[containerKey]string, len(missing))
	for k, dims := range missing {
		pods[k.namespace+"/"+k.pod] = true
		keys = append(keys, k)
		if names := censusAnnotation(spec, lr, k.namespace, dims); names != "" {
			annotations[k] = names
			defaulted++
		}
	}
	message := fmt.Sprintf("%d container(s) in %d pod(s) have no cpu or memory %s — %s (%s lists them)",
		len(missing), len(pods), spec.dimension, spec.consequence, spec.flag)
	details := []emit.Field{
		{Key: "pods", Value: strconv.Itoa(len(pods))},
		{Key: "containers", Value: strconv.Itoa(len(missing))},
	}
	if defaulted > 0 {
		message += fmt.Sprintf("; %d of them sit in a namespace whose LimitRange now defaults the missing dimension — those pods predate it and pick the value up on recreation", defaulted)
		details = append(details, emit.Field{Key: "limitrange_defaulted", Value: strconv.Itoa(defaulted)})
	}
	if err := out.Emit(emit.Finding{
		Kind:     spec.kind,
		Severity: emit.SeverityInfo,
		Reason:   spec.reason,
		Message:  message,
		Details:  details,
	}); err != nil {
		return err
	}
	if !list {
		return nil
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.pod != b.pod {
			return a.pod < b.pod
		}
		return a.container < b.container
	})
	for _, k := range keys {
		dims := missing[k]
		sort.Strings(dims)
		f := emit.Finding{
			Kind:         spec.containerKind,
			Severity:     emit.SeverityInfo,
			Namespace:    k.namespace,
			KindOfObject: "Pod",
			Name:         k.pod,
			Reason:       spec.reason,
			Details: []emit.Field{
				{Key: "container", Value: k.container},
				{Key: "missing", Value: strings.Join(dims, ",")},
			},
		}
		if names := annotations[k]; names != "" {
			f.Details = append(f.Details, emit.Field{Key: "limitrange", Value: names})
		}
		if err := out.Emit(f); err != nil {
			return err
		}
	}
	return nil
}

// censusAnnotation names the LimitRange(s) defaulting any dimension
// this container lacks, or "" when none does.
func censusAnnotation(spec censusSpec, lr *state.LimitRangeDefaults, namespace string, dims []string) string {
	for _, d := range dims {
		if names, ok := spec.defaults(lr, namespace, d); ok {
			return names
		}
	}
	return ""
}

// fetchNodeUsage joins metrics.k8s.io node usage with allocatable
// from the Node objects. Nodes present in only one of the two lists
// are skipped (added/removed between reads). Returns the samples and
// how many nodes were examined.
func fetchNodeUsage(ctx context.Context, metrics metricsv.Interface, client kubernetes.Interface) ([]nodeRated, int, error) {
	nmList, err := metrics.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("list node metrics: %w", err)
	}
	nodeList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("list nodes: %w", err)
	}
	type alloc struct{ cpu, mem float64 }
	allocs := make(map[string]alloc, len(nodeList.Items))
	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		allocs[n.Name] = alloc{
			cpu: float64(n.Status.Allocatable.Cpu().MilliValue()),
			mem: float64(n.Status.Allocatable.Memory().Value()),
		}
	}
	var out []nodeRated
	examined := 0
	for i := range nmList.Items {
		nm := &nmList.Items[i]
		a, ok := allocs[nm.Name]
		if !ok {
			continue
		}
		examined++
		if a.cpu > 0 {
			used := float64(nm.Usage.Cpu().MilliValue())
			out = append(out, nodeRated{node: nm.Name, resource: saturation.ResourceCPU, used: used, allocatable: a.cpu, pct: used / a.cpu * 100})
		}
		if a.mem > 0 {
			used := float64(nm.Usage.Memory().Value())
			out = append(out, nodeRated{node: nm.Name, resource: saturation.ResourceMemory, used: used, allocatable: a.mem, pct: used / a.mem * 100})
		}
	}
	return out, examined, nil
}

// enrich appends --history window stats (max/avg/p95 as percent of
// the limit) to one container finding, from the provider metrics
// backend. A window with no points adds nothing (zero nominal
// state).
func enrich(ctx context.Context, backend cloud.MetricsBackend, f *emit.Finding, r rated, window cloud.TimeWindow) error {
	metric := HistoryMetricCPU
	if r.s.Resource == saturation.ResourceMemory {
		metric = HistoryMetricMemory
	}
	series, err := backend.QuerySeries(ctx, cloud.SeriesQuery{
		Metric: metric,
		Matchers: map[string]string{
			"namespace": r.s.Namespace,
			"pod":       r.s.Pod,
			"container": r.s.Container,
		},
		Window: window,
		Step:   time.Minute,
	})
	if err != nil {
		return fmt.Errorf("history query for %s/%s/%s %s: %w", r.s.Namespace, r.s.Pod, r.s.Container, r.s.Resource, err)
	}
	var values []float64
	for _, s := range series {
		for _, p := range s.Points {
			values = append(values, p.Value)
		}
	}
	if len(values) == 0 {
		return nil
	}
	sort.Float64s(values)
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	maxV := values[len(values)-1]
	avg := sum / float64(len(values))
	p95 := values[p95Index(len(values))]
	pct := func(v float64) string { return fmtPct(v / r.s.Limit * 100) }
	f.Details = append(f.Details,
		emit.Field{Key: "max_pct", Value: pct(maxV)},
		emit.Field{Key: "avg_pct", Value: pct(avg)},
		emit.Field{Key: "p95_pct", Value: pct(p95)},
	)
	return nil
}

// p95Index is the 95th-percentile index (nearest-rank) into a sorted
// slice of length n.
func p95Index(n int) int {
	i := int(math.Ceil(0.95*float64(n))) - 1
	if i < 0 {
		return 0
	}
	return i
}

// ratedLess orders container rows: pct descending, then identity for
// determinism.
func ratedLess(a, b rated) bool {
	if a.pct != b.pct {
		return a.pct > b.pct
	}
	if a.s.Namespace != b.s.Namespace {
		return a.s.Namespace < b.s.Namespace
	}
	if a.s.Pod != b.s.Pod {
		return a.s.Pod < b.s.Pod
	}
	if a.s.Container != b.s.Container {
		return a.s.Container < b.s.Container
	}
	return a.s.Resource < b.s.Resource
}

// nodeLess orders node rows the same way.
func nodeLess(a, b nodeRated) bool {
	if a.pct != b.pct {
		return a.pct > b.pct
	}
	if a.node != b.node {
		return a.node < b.node
	}
	return a.resource < b.resource
}

// fmtPct renders a percentage to at most one decimal, dropping a
// trailing ".0" for token density.
func fmtPct(p float64) string {
	return strconv.FormatFloat(math.Round(p*10)/10, 'f', -1, 64)
}

// fmtValue renders a value in the dimension's natural unit:
// millicores for CPU, IEC bytes for memory.
func fmtValue(resource string, v float64) string {
	if resource == saturation.ResourceCPU {
		return fmt.Sprintf("%.0fm", v)
	}
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%.1fGiB", v/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.1fMiB", v/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%.1fKiB", v/(1<<10))
	}
	return fmt.Sprintf("%.0fB", v)
}
