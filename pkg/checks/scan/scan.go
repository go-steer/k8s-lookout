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

// Package scan implements `lookout scan`: the zero-argument entry
// point. "Something is wrong with this cluster" is the question an
// agent actually arrives with, and until this command existed the
// answer required already knowing which of thirty-odd checks to
// reach for. `lookout scan` needs nothing but a kubeconfig.
//
// # It composes the registry, not Go seams
//
// `bundle` and `health` compose by calling the check packages'
// exported Go functions. That works, and it is why neither of them
// has ever picked up a check added after it was written: the
// composition names its parts. `scan` instead looks its stages up in
// the command registry and calls Command.Run through a child
// emit.Invocation that shares scan's own Writer — the same shape
// internal/watch's enrichment bundle uses. One stream, one envelope,
// one summary line; each finding stamped with the `check` that
// produced it, which is also the command to run for more.
//
// The corollary is the contract test in this package: every visible
// command in the default registry must be in scan's default set or in
// its exclusion table with a stated reason. Adding a check without
// deciding whether a bare `lookout scan` should run it fails CI. That
// gate is the whole point — without it `scan` decays into a
// hand-maintained list exactly the way `bundle` and `health` did.
//
// # Three stages
//
//  1. Every target-free INCIDENT check, in declared order. Posture
//     (`audit`), cloud, and control-plane-metric groups are opt-in via
//     --include: posture findings never self-clear, so defaulting them
//     on would both flood a healthy cluster's first run and swamp the
//     `findings diff` transition stream with a flat backlog. The
//     groups left out are named in the summary line so they stay
//     discoverable while off.
//  2. Edge drill-down. Every workload stage 1 flagged at warning or
//     above gets the `state edges` dependency checks — from ONE
//     state.LoadCluster pass and N in-memory EdgeFindings calls, not N
//     List passes. Bounded by --max-drilldown.
//  3. The standard §4.2 envelope, so `lookout scan | lookout findings
//     diff --store=…` works with no glue.
//
// A stage that degrades (`state wi` on a cluster with no cloud
// provider, `state gateway` where the Gateway API is not installed)
// emits its own explicit unavailable finding and scan names it in the
// summary's `unavailable` note. A scan that could not run a check says
// so; it never reports a clean bill of health it did not earn (§11).
package scan

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// Finding kinds scan owns. Everything else in the stream belongs to
// the check that emitted it.
const (
	// KindCheckSkipped: a stage declined this invocation — it needs
	// something a zero-argument scan does not supply (`audit
	// exemptions` without --exemptions). Info: nothing is wrong with
	// the cluster, but the coverage claim is smaller than it looks.
	KindCheckSkipped = "scan.check_skipped"
	// KindCheckFailed: a stage errored. Warning, not fatal — one
	// broken check must not void the other twelve, and the §4.2
	// alternative (exit 1, no summary) would discard everything
	// already found.
	KindCheckFailed = "scan.check_failed"
	// KindIncomplete: the --timeout expired with stages still to run.
	KindIncomplete = "scan.incomplete"
)

// stage1 is the default scan, in emission order: the target-free
// incident checks. Order is deliberate — `triage delta` first because
// it is the broadest and most likely to name the thing that is wrong,
// then the dependency and configuration verifications, then drift.
//
// A name here that is not registered is skipped silently: tests build
// registries holding two commands, and the contract test in this
// package is what holds the production registry to the full set.
var stage1 = []string{
	"triage delta",
	"state webhooks",
	"state volumes",
	"state storage",
	"state gateway",
	"state wi",
	"stab drift",
}

// optionalGroups are the §4.1 groups a bare scan leaves out and
// --include switches on, whole. They are groups rather than
// individual commands because the reason for leaving each one out is
// a property of the group: `audit` is posture rather than incident,
// `cloud` needs a provider build and bills API calls, `perf` queries
// Cloud Monitoring.
var optionalGroups = []string{"audit", "cloud", "perf"}

// excluded is the other half of the contract test: every visible
// command that is neither in stage1 nor in an optional group must
// appear here with the reason a bare `lookout scan` does not run it.
// The reasons are load-bearing documentation, not commentary — they
// are what the next contributor reads when the test tells them to
// make a decision.
var excluded = map[string]string{
	"scan": "the entry point itself; a scan that ran scan would not terminate",

	"health": "a composition over the same underlying derivations: every finding would appear twice, once as a scorecard category and once on its own. `lookout health` is the scorecard view of this data, `lookout scan` the finding view",
	"bundle": "a composition, and workload-targeted: `lookout bundle --workload=…` is the SECOND call, made once a scan has named the workload worth looking at",

	"triage list":    "an inventory aggregator (`kubectl get` across every kind at once), not a detector: it emits one line per object that EXISTS, which would bury the findings under the cluster",
	"triage top":     "needs the metrics.k8s.io API, an optional add-on, and answers \"how loaded is this\" rather than \"what is broken\": a cluster without metrics-server would collect a degradation record on every scan",
	"triage spec":    "renders one named object; a zero-argument scan has no target",
	"triage logs":    "distills one workload's logs; a zero-argument scan has no target, and log retrieval costs more than every other stage combined",
	"triage events":  "builds one target's event timeline; a zero-argument scan has no target",
	"triage radius":  "answers the blast radius OF something; a zero-argument scan has no target",
	"triage changes": "answers what changed around a target before onset; a zero-argument scan has neither a target nor an onset",
	"triage status":  "reads and WRITES §9.4 triage-status records; a scan reports, it does not record",

	"findings diff": "consumes a scan rather than producing one — `lookout scan | lookout findings diff --store=…` is the intended pairing",
	"findings ack":  "mutates suppression state for one named subject",

	"state edges": "workload-targeted, and stage 2 already runs exactly these checks in memory for every workload stage 1 flagged; standalone it would need a target scan does not have",

	"stab drain": "answers a hypothetical — what WOULD block draining a node — ahead of a planned operation, rather than a fault that exists now",

	"net probe": "requires explicit probe targets (--dns/--tcp/--http): there is nothing for a zero-argument scan to probe, and generating active traffic is not a default a scan should take",
}

// Deps injects the seams. The zero value is the production wiring.
type Deps struct {
	// Registry is where stages are resolved from. Nil means
	// checks.Default() — see New's note on when that is populated.
	Registry *checks.Registry
	// Client builds the Kubernetes client for the stage-2 List pass.
	// Nil means kube.BuildClient with default resolution.
	Client func(ctx context.Context) (kubernetes.Interface, error)
	// Now is the clock (TLS expiry math in the drill-down). Nil means
	// time.Now.
	Now func() time.Time
}

func (d Deps) registry() *checks.Registry {
	if d.Registry != nil {
		return d.Registry
	}
	return checks.Default()
}

func (d Deps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.BuildClient(kube.OptionsFrom(ctx))
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// New builds the `scan` command against deps.
//
// Unlike every other check, scan does not register itself from an
// init function: its output glossary is the union of its stages',
// read out of the registry, so it can only be built once every other
// command has registered. pkg/checks/all — the one place that imports
// them all, and therefore the one place Go guarantees their inits
// have finished — calls this and registers the result.
func New(deps Deps) checks.Command {
	return checks.Command{
		Name:    "scan",
		MCPName: "k8s_scan",
		Summary: "Start here when you know something is wrong but not what: one call runs every target-free incident check across the cluster — broken workloads, dead admission webhooks, stuck volumes and PVCs, rejected Gateway routes, config drift — then drills into the dependency edges of whatever it flagged. Needs no target; `--include=audit` adds the posture sweep.",
		// One invocation is a dozen checks and two List passes; the
		// single-check budget would abort a healthy scan of a large
		// cluster.
		TimeoutDefault: 60 * time.Second,
		Flags: []emit.FlagSpec{
			{Name: "include", Type: emit.FlagString, Default: "",
				Help: "additionally run these opt-in groups: " + strings.Join(optionalGroups, ",") + ". Comma-separated, 'all' for every one, '-' to subtract (all,-cloud). Left out by default because they answer a different question (audit = posture, no incident) or need a provider build (cloud, perf)"},
			{Name: "max-drilldown", Type: emit.FlagInt, Default: "20",
				Help: "cap the stage-2 dependency-edge drill-down at this many workloads, worst severity first (0 disables it); the number dropped is reported as truncated= in the summary"},
			{Name: "cert-warn", Type: emit.FlagDuration, Default: "720h",
				Help: "report TLS certificates expiring within this window (drill-down stage; same meaning as `state edges --cert-warn`)"},
		},
		Kinds:    kindLedger(deps.registry()),
		Output:   outputGlossary(deps.registry()),
		Examples: []string{"lookout scan", "lookout scan --namespace=prod", "lookout scan --include=audit --format=json", "lookout scan --max-drilldown=0"},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return run(ctx, deps, inv)
		},
	}
}

// ownFields are the glossary entries scan itself owns; the rest of
// its glossary is the union of its stages'.
func ownFields() []checks.OutputField {
	return []checks.OutputField{
		{Name: "check", Doc: "which registered command produced this finding — also the command to run for the full detail behind it"},
		{Name: "not_run", Doc: "on scan.incomplete: the checks the timeout left unrun, comma-separated"},
		{Name: "checks", Doc: "summary-line note: how many checks this scan actually ran"},
		{Name: "skipped", Doc: "summary-line note: opt-in groups this scan did NOT run (switch one on with --include=<group>) — stated so a quiet scan is never mistaken for a complete one"},
		{Name: "drilldown", Doc: "summary-line note: workloads the stage-2 dependency-edge drill-down covered"},
		{Name: "truncated", Doc: "summary-line note: drill-down candidates dropped by --max-drilldown"},
	}
}

// ownKinds are the three claims scan makes in its own voice. Every
// other kind in the stream belongs to the stage that emitted it.
func ownKinds() []checks.KindField {
	return []checks.KindField{
		checks.Kind(KindCheckSkipped,
			"a stage declined this invocation because a zero-argument scan cannot supply something it needs — the coverage claim is smaller than it looks (§11)",
			emit.SeverityInfo),
		checks.Kind(KindCheckFailed,
			"a stage errored; the scan continued without it, so this run saw less than a whole cluster",
			emit.SeverityWarning),
		checks.Kind(KindIncomplete,
			"the --timeout expired with stages still to run; not_run names them",
			emit.SeverityWarning),
	}
}

// kindLedger is scan's own kinds plus the union of every command it
// can run, read out of the registry exactly the way outputGlossary
// reads their fields. A check that gains a kind gains it here on the
// next build; a check scan runs cannot emit a kind scan has not
// declared.
func kindLedger(reg *checks.Registry) []checks.KindField {
	out := ownKinds()
	seen := map[string]bool{}
	for _, k := range out {
		seen[k.Name] = true
	}
	add := func(kinds []checks.KindField) {
		for _, k := range kinds {
			if seen[k.Name] {
				continue
			}
			seen[k.Name] = true
			out = append(out, k)
		}
	}
	names := append([]string{}, stage1...)
	for _, g := range optionalGroups {
		names = append(names, groupStages(reg, g)...)
	}
	for _, name := range names {
		if c, ok := reg.Lookup(name); ok {
			add(c.Kinds)
		}
	}
	// Stage 2 is `state edges` called in memory, past the registry —
	// same reason outputGlossary reaches for the constructor directly.
	add(state.EdgesCommand(state.Deps{}).Kinds)
	return out
}

// outputGlossary is scan's own fields plus the union of every command
// it can run — sourced from the commands' own metadata where they are
// registered, so the glossaries cannot drift apart. Same trick as
// bundle.composedOutput, over a set derived from the registry rather
// than a hand-written list.
func outputGlossary(reg *checks.Registry) []checks.OutputField {
	out := ownFields()
	seen := map[string]bool{}
	for _, f := range out {
		seen[f.Name] = true
	}
	add := func(fields []checks.OutputField) {
		for _, f := range fields {
			if seen[f.Name] {
				continue
			}
			seen[f.Name] = true
			out = append(out, f)
		}
	}
	names := append([]string{}, stage1...)
	for _, g := range optionalGroups {
		names = append(names, groupStages(reg, g)...)
	}
	for _, name := range names {
		if c, ok := reg.Lookup(name); ok {
			add(c.Output)
		}
	}
	// The drill-down is `state edges`, which scan calls through
	// Cluster.EdgeFindings rather than through the registry — and
	// which is in the exclusion table, so the loop above never sees
	// it. Take its glossary from the same constructor that registers
	// it, so stage 2's fields are declared whatever the registry
	// holds.
	add(state.EdgesCommand(state.Deps{}).Output)
	return out
}

// groupStages lists the visible, non-excluded commands of one
// optional group, in name order.
func groupStages(reg *checks.Registry, group string) []string {
	var out []string
	for _, c := range reg.GroupCommands(group) {
		if _, skip := excluded[c.Name]; skip {
			continue
		}
		out = append(out, c.Name)
	}
	return out
}

// parseInclude resolves --include into the set of opt-in groups to
// run. Syntax mirrors `bundle --lists` (state.ParseListSelection):
// left to right, "all" adds every group, a bare name adds one, a "-"
// prefix removes one.
func parseInclude(spec string) (map[string]bool, error) {
	set := map[string]bool{}
	known := map[string]bool{}
	for _, g := range optionalGroups {
		known[g] = true
	}
	for _, tok := range strings.Split(spec, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if tok == "all" {
			for _, g := range optionalGroups {
				set[g] = true
			}
			continue
		}
		remove := false
		if strings.HasPrefix(tok, "-") {
			remove, tok = true, strings.TrimSpace(tok[1:])
		}
		if !known[tok] {
			return nil, fmt.Errorf("unknown group %q (want one of %s, or all)", tok, strings.Join(optionalGroups, ", "))
		}
		if remove {
			delete(set, tok)
		} else {
			set[tok] = true
		}
	}
	return set, nil
}

// subject is one stage-1 finding's drill-down candidate, before the
// List pass exists to resolve it.
type subject struct {
	severity  string
	kind      string
	namespace string
	name      string
}

// scanner carries the state one invocation accumulates through the
// Writer tap.
type scanner struct {
	stage       string // the check currently running ("" = scan itself)
	subjects    []subject
	unavailable []string
}

// observe is the emit.Writer tap: every finding any stage writes
// passes through here on its way to the stream. It never changes
// anything — it only records what stage 2 needs to know.
func (s *scanner) observe(f emit.Finding) {
	if s.stage == "" {
		return // scan's own bookkeeping findings
	}
	// `cloud.unavailable` / `crd.unavailable`: the stage ran but could
	// not answer. Roll them up into one summary note so the coverage
	// gap is on the line a reader cannot miss.
	if strings.HasSuffix(f.Kind, ".unavailable") {
		if len(s.unavailable) == 0 || s.unavailable[len(s.unavailable)-1] != s.stage {
			s.unavailable = append(s.unavailable, s.stage)
		}
		return
	}
	if f.Severity != emit.SeverityWarning && f.Severity != emit.SeverityCritical {
		return
	}
	if f.KindOfObject == "" || f.Name == "" {
		return
	}
	s.subjects = append(s.subjects, subject{severity: f.Severity, kind: f.KindOfObject, namespace: f.Namespace, name: f.Name})
}

func run(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("--workload does not apply: scan answers \"what is wrong with this cluster\" with no target at all; for one workload run `lookout bundle --workload=%s`", inv.Scope.Workload)
	}
	maxDrill := inv.Flags.Int("max-drilldown")
	if maxDrill < 0 {
		return 0, emit.UsageErrorf("--max-drilldown must not be negative, got %d (0 disables the drill-down)", maxDrill)
	}
	certWarn := inv.Flags.Duration("cert-warn")
	if certWarn < 0 {
		return 0, emit.UsageErrorf("--cert-warn must not be negative, got %s", certWarn)
	}
	included, err := parseInclude(inv.Flags.String("include"))
	if err != nil {
		return 0, emit.UsageErrorf("--include: %v", err)
	}

	reg := deps.registry()
	stages := make([]string, 0, len(stage1)+8)
	for _, name := range stage1 {
		if _, ok := reg.Lookup(name); ok {
			stages = append(stages, name)
		}
	}
	var left []string
	for _, g := range optionalGroups {
		if included[g] {
			stages = append(stages, groupStages(reg, g)...)
		} else {
			left = append(left, g)
		}
	}

	s := &scanner{}
	inv.Out.Watch(s.observe)

	scanned, ran, notRun := runStages(ctx, reg, inv, s, stages)

	drilled, truncated, drillScanned := 0, 0, 0
	if len(notRun) == 0 && maxDrill > 0 && len(s.subjects) > 0 {
		drilled, truncated, drillScanned = drilldown(ctx, deps, inv, s, maxDrill, certWarn)
		scanned += drillScanned
	}

	if err := inv.Out.Note("checks", strconv.Itoa(ran)); err != nil {
		return scanned, err
	}
	if len(left) > 0 {
		if err := inv.Out.Note("skipped", strings.Join(left, ",")); err != nil {
			return scanned, err
		}
	}
	if err := inv.Out.Note("drilldown", strconv.Itoa(drilled)); err != nil {
		return scanned, err
	}
	if truncated > 0 {
		if err := inv.Out.Note("truncated", strconv.Itoa(truncated)); err != nil {
			return scanned, err
		}
	}
	if len(s.unavailable) > 0 {
		if err := inv.Out.Note("unavailable", strings.Join(s.unavailable, ",")); err != nil {
			return scanned, err
		}
	}
	return scanned, nil
}

// runStages executes stage 1 (and any included opt-in group) in
// order, returning the summed scanned count, how many checks actually
// ran, and the names the timeout left unrun.
func runStages(ctx context.Context, reg *checks.Registry, inv emit.Invocation, s *scanner, stages []string) (scanned, ran int, notRun []string) {
	for i, name := range stages {
		if ctx.Err() != nil {
			notRun = stages[i:]
			break
		}
		c, ok := reg.Lookup(name)
		if !ok {
			continue
		}
		n, err := stage(ctx, c, inv, s)
		scanned += n
		if err == nil {
			ran++
			continue
		}
		if ctx.Err() != nil {
			notRun = stages[i:]
			break
		}
		emitStageResult(inv, s, name, err)
		ran++
	}
	s.stage = ""
	_ = inv.Out.Stamp("check", "")
	if len(notRun) > 0 {
		_ = inv.Out.Emit(emit.Finding{
			Kind:     KindIncomplete,
			Severity: emit.SeverityWarning,
			Reason:   "Timeout",
			Message:  "the --timeout expired before every check ran; this scan is a partial view, raise --timeout or narrow with --namespace",
			Details:  []emit.Field{{Key: "not_run", Value: strings.Join(notRun, ",")}},
		})
	}
	return scanned, ran, notRun
}

// stage runs one registered command through a child Invocation that
// shares scan's Writer: same stream, same envelope, same summary. The
// child sees its own flags at their defaults (a stage has no argv)
// and scan's scope, minus the target scan refuses to take.
func stage(ctx context.Context, c checks.Command, inv emit.Invocation, s *scanner) (int, error) {
	flags, err := c.DefaultFlags()
	if err != nil {
		return 0, err
	}
	s.stage = c.Name
	if err := inv.Out.Stamp("check", c.Name); err != nil {
		return 0, err
	}
	return c.Run(ctx, emit.Invocation{
		Scope: emit.Scope{
			Namespace:     inv.Scope.Namespace,
			AllNamespaces: inv.Scope.AllNamespaces,
			Since:         inv.Scope.Since,
		},
		Flags:      flags,
		In:         inv.In,
		Out:        inv.Out,
		Exemptions: inv.Exemptions,
	})
}

// emitStageResult records one stage's failure in the stream. A usage
// error from a stage is not a failure: it means the check needs
// something a zero-argument scan does not supply, which is a coverage
// statement, not a fault.
func emitStageResult(inv emit.Invocation, s *scanner, name string, err error) {
	f := emit.Finding{
		Kind:     KindCheckFailed,
		Severity: emit.SeverityWarning,
		Reason:   "CheckFailed",
		Message:  err.Error(),
	}
	if emit.IsUsageError(err) {
		f.Kind, f.Severity, f.Reason = KindCheckSkipped, emit.SeverityInfo, "NotApplicable"
	}
	// Emitted under the stage's own stamp, so the finding carries
	// check=<name> like everything else that stage produced.
	s.stage = name
	_ = inv.Out.Emit(f)
}

// drilldown is stage 2: one List pass, then the `state edges`
// dependency checks in memory for each workload stage 1 flagged. The
// gap analysis worried about an unbounded fan-out of List calls; there
// is exactly one, and EdgeFindings runs over the objects it already
// returned.
func drilldown(ctx context.Context, deps Deps, inv emit.Invocation, s *scanner, maxDrill int, certWarn time.Duration) (drilled, truncated, scanned int) {
	s.stage = "state edges"
	_ = inv.Out.Stamp("check", "state edges")
	defer func() {
		s.stage = ""
		_ = inv.Out.Stamp("check", "")
	}()

	fail := func(err error) {
		_ = inv.Out.Emit(emit.Finding{
			Kind:     KindCheckFailed,
			Severity: emit.SeverityWarning,
			Reason:   "DrilldownFailed",
			Message:  "dependency-edge drill-down over the flagged workloads: " + err.Error(),
		})
	}

	client, err := deps.client(ctx)
	if err != nil {
		fail(err)
		return 0, 0, 0
	}
	// Tolerate: a scan under a least-privilege credential drills into
	// what it can read rather than failing the whole stage.
	cluster, err := state.LoadCluster(ctx, client, inv.Scope.Namespace, state.Tolerate())
	if err != nil {
		fail(err)
		return 0, 0, 0
	}
	scanned = cluster.Scanned()

	targets, truncated := resolveTargets(cluster, s.subjects, maxDrill)
	now := deps.now()
	seen := map[string]bool{}
	for _, wl := range targets {
		if ctx.Err() != nil {
			break
		}
		fs, err := cluster.EdgeFindings(wl, certWarn, now)
		if err != nil {
			// A workload that vanished between stage 1 and the List
			// pass is a race, not a defect: drop it silently rather
			// than reporting on lookout's own timing.
			continue
		}
		drilled++
		for _, f := range fs {
			// Two flagged workloads can share a broken ConfigMap; the
			// finding is about the ConfigMap, so emit it once.
			key := edgeKey(f)
			if seen[key] {
				continue
			}
			seen[key] = true
			_ = inv.Out.Emit(f)
		}
	}
	return drilled, truncated, scanned
}

// resolveTargets turns stage-1 subjects into the workloads to drill
// into: worst severity first, each rolled up to its outermost
// controller (twenty pods of one Deployment are one target),
// deduplicated, capped at maxDrill. The second return is how many
// distinct targets the cap dropped.
func resolveTargets(cluster *state.Cluster, subjects []subject, maxDrill int) ([]emit.WorkloadRef, int) {
	ranked := append([]subject{}, subjects...)
	sort.SliceStable(ranked, func(i, j int) bool {
		return severityRank(ranked[i].severity) < severityRank(ranked[j].severity)
	})
	var out []emit.WorkloadRef
	seen := map[emit.WorkloadRef]bool{}
	dropped := 0
	for _, sub := range ranked {
		wl, ok := cluster.TopWorkload(sub.kind, sub.namespace, sub.name)
		if !ok || seen[wl] {
			continue
		}
		seen[wl] = true
		if len(out) >= maxDrill {
			dropped++
			continue
		}
		out = append(out, wl)
	}
	return out, dropped
}

func severityRank(s string) int {
	if s == emit.SeverityCritical {
		return 0
	}
	return 1
}

// edgeKey identifies one drill-down finding for deduplication across
// targets: the subject plus the claim, not the details.
func edgeKey(f emit.Finding) string {
	return strings.Join([]string{f.Kind, f.Namespace, f.KindOfObject, f.Name, f.Reason, f.Message}, "\x00")
}
