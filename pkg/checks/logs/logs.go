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

// Package logs implements `lookout triage logs` (DESIGN.md §5): the
// token-density workhorse of the read path. Raw container logs are
// distilled into Drain-clustered templates — probe noise stripped,
// stack traces collapsed to their top frames — so ~150k tokens of
// kubectl-logs output become a few hundred tokens of templates with
// counts, spreads, and time bounds the model can actually reason
// over.
package logs

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// defaultSince is the lookback used when --since is 0 (§4.2: 0 means
// "the command default").
const defaultSince = time.Hour

func init() {
	checks.Register(New(Deps{}))
}

// Deps injects the cluster access seams. The zero value is the
// production wiring; tests supply a fake.Clientset for discovery and
// fixture-backed log streams (the fake's GetLogs subresource returns
// a canned constant, hence the seam).
type Deps struct {
	// Client returns the client used for pod/workload discovery.
	// Nil means kube.BuildClient with default config resolution.
	Client func() (kubernetes.Interface, error)
	// Logs streams one container's logs. Nil means the Client's
	// GetLogs subresource.
	Logs PodLogGetter
}

// New builds the `triage logs` command with the given dependencies.
func New(deps Deps) checks.Command {
	return checks.Command{
		Name:    "triage logs",
		MCPName: "k8s_triage_logs",
		Summary: "kubectl logs, distilled: Drain-clusters raw lines into templates with counts (probe noise stripped, stack traces collapsed to top frames) — reach for this instead of reading logs whole.",
		Flags: []emit.FlagSpec{
			{Name: "pod", Type: emit.FlagString, Default: "", Help: "read one pod by name (requires --namespace)"},
			{Name: "container", Type: emit.FlagString, Default: "", Help: "restrict to one container (default: all init + regular + ephemeral containers)"},
			{Name: "previous", Type: emit.FlagBool, Default: "false", Help: "read the previous container instance (what a crashed container said before it died)"},
			{Name: "tail", Type: emit.FlagInt, Default: "5000", Help: "max lines fetched per container stream (0 = no limit)"},
			{Name: "max-templates", Type: emit.FlagInt, Default: "40", Help: "cap emitted template clusters; the low-count tail is summarized in one log.overflow finding"},
			{Name: "keep-probes", Type: emit.FlagBool, Default: "false", Help: "keep health/readiness probe request lines instead of stripping them"},
		},
		Output: []checks.OutputField{
			{Name: "template", Doc: "log template; <*> marks positions that varied across merged lines"},
			{Name: "count", Doc: "lines merged into this cluster (on log.probe_noise: probe lines stripped)"},
			{Name: "pods", Doc: "distinct pods that emitted this template (present when >1)"},
			{Name: "level", Doc: "guessed log level (fatal|error|warn|info|debug) from token/field match"},
			{Name: "first_seen", Doc: "RFC3339 timestamp of the oldest merged line (from log timestamps when parseable)"},
			{Name: "last_seen", Doc: "RFC3339 timestamp of the newest merged line"},
			{Name: "lang", Doc: "stack-trace runtime on log.stacktrace findings: go|java|python"},
			{Name: "frames", Doc: "top stack frames on log.stacktrace findings, innermost first, ' < ' separated"},
			{Name: "sample", Doc: "one representative raw line, truncated and sanitized"},
			{Name: "container", Doc: "container the finding refers to (log.fetch_error only)"},
			{Name: "omitted_templates", Doc: "clusters dropped by --max-templates (log.overflow only)"},
			{Name: "omitted_lines", Doc: "lines inside the dropped clusters (log.overflow only)"},
		},
		Examples: []string{
			"lookout triage logs --workload=Deployment/prod/api --since=30m",
			"lookout triage logs --namespace=payments --previous --container=app",
			"lookout triage logs --pod=api-6d5f9c7b4-xk2p1 --namespace=prod --format=json",
		},
		Run: runCheck(deps),
	}
}

// fetchFailure records one stream that could not be read; partial
// failures become findings, not aborts.
type fetchFailure struct {
	target target
	err    error
}

func runCheck(deps Deps) emit.CheckFunc {
	return func(ctx context.Context, inv emit.Invocation) (int, error) {
		podName := inv.Flags.String("pod")
		container := inv.Flags.String("container")
		previous := inv.Flags.Bool("previous")
		tail := inv.Flags.Int("tail")
		maxTemplates := inv.Flags.Int("max-templates")
		keepProbes := inv.Flags.Bool("keep-probes")

		if maxTemplates <= 0 {
			return 0, emit.UsageErrorf("--max-templates must be positive, got %d", maxTemplates)
		}
		if tail < 0 {
			return 0, emit.UsageErrorf("--tail must not be negative, got %d", tail)
		}
		if podName != "" && (!inv.Scope.Workload.IsZero() || inv.Scope.AllNamespaces) {
			return 0, emit.UsageErrorf("--pod is mutually exclusive with --workload and -A")
		}
		if !inv.Scope.Workload.IsZero() && (inv.Scope.Namespace != "" || inv.Scope.AllNamespaces) {
			return 0, emit.UsageErrorf("--workload carries its own namespace; drop --namespace/-A")
		}

		cs, err := buildClient(deps)
		if err != nil {
			return 0, err
		}
		getter := deps.Logs
		if getter == nil {
			getter = clientLogGetter{cs: cs}
		}

		targets, err := resolveTargets(ctx, cs, inv.Scope, podName, container)
		if err != nil {
			return 0, err
		}

		since := inv.Scope.Since
		if since == 0 {
			since = defaultSince
		}
		eng := newEngine(!keepProbes)
		var failures []fetchFailure
		for _, t := range targets {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if err := fetchOne(ctx, getter, eng, t, since, previous, tail); err != nil {
				failures = append(failures, fetchFailure{target: t, err: err})
			}
		}
		if len(targets) > 0 && len(failures) == len(targets) && eng.lines == 0 {
			return 0, fmt.Errorf("all %d log streams failed; first: %v", len(targets), failures[0].err)
		}

		for _, f := range collectFindings(eng, failures, maxTemplates) {
			if err := inv.Out.Emit(f); err != nil {
				return 0, err
			}
		}
		return eng.lines, nil
	}
}

func buildClient(deps Deps) (kubernetes.Interface, error) {
	if deps.Client != nil {
		return deps.Client()
	}
	return kube.BuildClient(kube.Options{})
}

// fetchOne streams one container's logs into the engine. Timestamps
// are always requested so first_seen/last_seen and timestamp
// stripping are deterministic regardless of app log format.
func fetchOne(ctx context.Context, getter PodLogGetter, eng *engine, t target, since time.Duration, previous bool, tail int) error {
	secs := int64(since / time.Second)
	if secs < 1 {
		secs = 1
	}
	opts := &corev1.PodLogOptions{
		Container:    t.container,
		Timestamps:   true,
		Previous:     previous,
		SinceSeconds: &secs,
	}
	if tail > 0 {
		tl := int64(tail)
		opts.TailLines = &tl
	}
	rc, err := getter.Stream(ctx, t.namespace, t.pod, opts)
	if err != nil {
		return err
	}
	// Close errors are uninteresting after a full read; the scanner
	// error below carries any real stream failure.
	defer func() { _ = rc.Close() }()

	s := eng.stream(t.namespace, t.pod)
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		s.add(sc.Text())
	}
	s.close()
	return sc.Err()
}

// collectFindings renders the engine's clusters under the §4.2
// envelope: fetch errors first (the agent must know its view is
// partial), then clusters error-ish-first/count-desc capped at
// maxTemplates, then the explicit overflow and probe-noise records.
func collectFindings(eng *engine, failures []fetchFailure, maxTemplates int) []emit.Finding {
	var out []emit.Finding
	for _, f := range failures {
		out = append(out, emit.Finding{
			Kind:         "log.fetch_error",
			Severity:     emit.SeverityWarning,
			Namespace:    f.target.namespace,
			KindOfObject: "Pod",
			Name:         f.target.pod,
			Reason:       "LogFetchFailed",
			Message:      f.err.Error(),
			Details:      []emit.Field{{Key: "container", Value: f.target.container}},
		})
	}

	results := eng.results()
	emitted := results
	if len(results) > maxTemplates {
		emitted = results[:maxTemplates]
	}
	for _, r := range emitted {
		out = append(out, resultFinding(r))
	}
	if n := len(results) - len(emitted); n > 0 {
		lines := 0
		for _, r := range results[len(emitted):] {
			lines += r.count
		}
		out = append(out, emit.Finding{
			Kind:     "log.overflow",
			Severity: emit.SeverityInfo,
			Message:  "template cap reached; the omitted clusters are the low-count, non-error tail (raise --max-templates to see them)",
			Details: []emit.Field{
				{Key: "omitted_templates", Value: strconv.Itoa(n)},
				{Key: "omitted_lines", Value: strconv.Itoa(lines)},
			},
		})
	}
	if eng.probes > 0 {
		out = append(out, emit.Finding{
			Kind:     "log.probe_noise",
			Severity: emit.SeverityInfo,
			Message:  "health/readiness probe request lines stripped (--keep-probes to keep them)",
			Details:  []emit.Field{{Key: "count", Value: strconv.Itoa(eng.probes)}},
		})
	}
	return out
}

// resultFinding renders one cluster. The subject envelope fields are
// filled only when unambiguous: a single contributing pod names it;
// a single namespace names that; a multi-namespace template carries
// neither and relies on pods=N.
func resultFinding(r result) emit.Finding {
	f := emit.Finding{
		Kind:     "log.template",
		Severity: severityFor(r),
	}
	if r.stack {
		f.Kind = "log.stacktrace"
		f.Reason = stackReason(r.lang)
	}
	fillSubject(&f, r.pods)

	add := func(k, v string) {
		if v != "" {
			f.Details = append(f.Details, emit.Field{Key: k, Value: v})
		}
	}
	add("template", r.template)
	add("count", strconv.Itoa(r.count))
	if len(r.pods) > 1 {
		add("pods", strconv.Itoa(len(r.pods)))
	}
	add("level", levelNames[r.level])
	if !r.first.IsZero() {
		add("first_seen", r.first.UTC().Format(time.RFC3339))
	}
	if !r.last.IsZero() {
		add("last_seen", r.last.UTC().Format(time.RFC3339))
	}
	if r.stack {
		add("lang", r.lang)
		frames := ""
		for i, fr := range r.frames {
			if i > 0 {
				frames += " < "
			}
			frames += fr
		}
		add("frames", frames)
	}
	if r.sample != r.template {
		add("sample", r.sample)
	}
	return f
}

func severityFor(r result) string {
	switch {
	case r.level >= levelFatal:
		return emit.SeverityCritical
	case r.errorish():
		return emit.SeverityWarning
	}
	return emit.SeverityInfo
}

func stackReason(lang string) string {
	switch lang {
	case "go":
		return "GoPanic"
	case "java":
		return "JavaException"
	case "python":
		return "PythonTraceback"
	}
	return ""
}

func fillSubject(f *emit.Finding, pods map[podKey]struct{}) {
	var ns, pod string
	uniformNS := true
	for k := range pods {
		if ns == "" {
			ns, pod = k.namespace, k.pod
		} else if k.namespace != ns {
			uniformNS = false
		}
	}
	if !uniformNS {
		return
	}
	f.Namespace = ns
	if len(pods) == 1 {
		f.KindOfObject = "Pod"
		f.Name = pod
	}
}
