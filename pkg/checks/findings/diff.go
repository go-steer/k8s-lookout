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

package findings

import (
	"context"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	findingstate "github.com/go-steer/k8s-lookout/pkg/findings"
)

// DiffCommand builds `lookout findings diff`.
func DiffCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "findings diff",
		MCPName: "k8s_findings_diff",
		// The diff ADVANCES persisted state as a side effect of
		// answering, so the MCP surface must advertise it as a write
		// (ReadOnlyHint:false) rather than let a client auto-approve it
		// as a read (issue #105). --dry-run is the read-only mode.
		Writes:  true,
		Summary: "Diff a health report against the previous run and report what CHANGED — new, ongoing, escalated, resolved, suppressed — instead of re-listing every open finding; the command that makes a scheduled scan produce a digest an operator will keep reading.",
		Flags: []emit.FlagSpec{
			{Name: "report", Type: emit.FlagString, Default: "-",
				Help: "the §4.2 finding report to classify: `-` reads stdin (the usual `lookout health | lookout findings diff --report -`), or a file path. Either wire format is accepted, detected per line, so the upstream command does not need --format=json"},
			{Name: "store", Type: emit.FlagString, Default: "",
				Help: "path to the sentinel's SQLite store (its --store file), where the previous run's state lives. Required: " + storeHint},
			{Name: "cluster", Type: emit.FlagString, Default: "",
				Help: "cluster label to bind these findings to; becomes the first segment of every subject key. Give the same value on every run for a cluster — changing it makes every subject look new"},
			{Name: "transitions", Type: emit.FlagString, Default: "",
				Help: "emit only these transition classes, comma-separated: " + transitionList() + " (empty = all). `--transitions=new,escalated,resolved` is the digest view: everything that changed, nothing that didn't"},
			{Name: "dry-run", Type: emit.FlagBool, Default: "false",
				Help: "classify and print, but do not advance the stored state. Use to preview a report without consuming it — a normal run is not repeatable, because after it the second run's findings are all `ongoing`"},
		},
		Kinds: []checks.KindField{
			checks.Kind("findings.transition", "a finding subject changed state since the previous run ("+transitionList()+"); the severity is the underlying finding's current one, not a judgment about the transition", emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "transition", Doc: "how this subject changed since the previous run: " + transitionList()},
			{Name: "subject_key", Doc: "the normalized instance-grain key this diff tracks: <cluster>/<namespace>/<kind_of_object>/<normalized-name>/<canonical-reason>. Distinct from the envelope's class-level fingerprint (§8); pass it to `lookout findings ack`"},
			{Name: "prev_severity", Doc: "the severity recorded at the previous run; absent on `new`. Compare with the envelope's severity to see a de-escalation, which stays classified `ongoing`"},
			{Name: "first_seen", Doc: "when this subject was first observed, RFC 3339 — carried across runs, so it is the \"broken since\" timestamp, not this run's clock"},
			{Name: "last_seen", Doc: "when this subject was last observed, RFC 3339"},
			{Name: "ack_until", Doc: "expiry of the operator ack window on a `suppressed` subject, RFC 3339"},
			{Name: "ack_by", Doc: "who took the ack, as forwarded by the caller"},
		},
		Examples: []string{
			"lookout health --store=/var/lib/lookout/lookout.db | lookout findings diff --report=- --store=/var/lib/lookout/lookout.db --cluster=prod-east",
			"lookout health | lookout findings diff --report=- --store=/var/lib/lookout/lookout.db --cluster=prod-east --transitions=new,escalated,resolved",
			"lookout findings diff --report=/tmp/scan.logfmt --store=/var/lib/lookout/lookout.db --cluster=prod-east --dry-run",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runDiff(ctx, inv, deps)
		},
	}
}

func runDiff(ctx context.Context, inv emit.Invocation, deps Deps) (int, error) {
	want, err := parseTransitions(inv.Flags.String("transitions"))
	if err != nil {
		return 0, err
	}
	report, closer, err := openReport(inv, inv.Flags.String("report"))
	if err != nil {
		return 0, err
	}
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}
	st, err := openStore(inv.Flags.String("store"))
	if err != nil {
		return 0, err
	}
	defer func() { _ = st.Close() }()

	cluster := inv.Flags.String("cluster")
	observations, err := findingstate.ParseReport(report, cluster)
	if err != nil {
		return 0, err
	}
	prev, err := st.FindingStates(ctx, cluster)
	if err != nil {
		return 0, err
	}

	// One clock read for the whole run: every subject is classified
	// against the same instant, so an ack cannot expire halfway down
	// a report and split one scan across two boundaries.
	now := deps.now().UTC()
	res := findingstate.Diff(prev, observations, now)

	// Persist BEFORE emitting. If the write fails the operator gets
	// exit 1 and no output, which is recoverable — they rerun. Emitting
	// first and failing the write would hand them a digest whose
	// transitions never happened as far as the next run is concerned,
	// and every `new` in it would be reported `new` again.
	if !inv.Flags.Bool("dry-run") {
		if err := st.ReplaceFindingStates(ctx, cluster, res.Next); err != nil {
			return 0, err
		}
	}

	for _, ch := range res.Changes {
		if want != nil && !want[ch.Transition] {
			continue
		}
		if err := inv.Out.Emit(changeFinding(ch)); err != nil {
			return 0, err
		}
	}
	// scanned is the number of subjects considered — the report's
	// findings plus the previously-open subjects that did not recur.
	// findings (the summary's other half) is what was emitted, which
	// --transitions can make much smaller. The pair is exactly the
	// "40 open, 3 changed" reading the digest exists for.
	return len(res.Changes), nil
}

// changeFinding renders one transition as a §4.2 finding.
//
// The envelope carries the SUBJECT (namespace/kind/name/reason) and
// the class fingerprint unchanged from the report, so a transition
// record reads like — and can be joined against — the finding it came
// from. The transition itself and the instance-grain subject key ride
// as details.
func changeFinding(ch findingstate.Change) emit.Finding {
	f := emit.Finding{
		Kind: "findings.transition",
		// Severity is the CURRENT severity of the underlying finding,
		// not a judgment about the transition. A resolved critical is
		// still reported severity=critical: what recovered matters as
		// much as that something did, and a consumer that wants to
		// route on the change reads `transition`.
		Severity:     ch.Severity,
		Namespace:    ch.Namespace,
		KindOfObject: ch.KindOfObject,
		Name:         ch.Name,
		Reason:       ch.Reason,
		Message:      ch.Message,
		Fingerprint:  ch.Fingerprint,
		Details: []emit.Field{
			{Key: "transition", Value: string(ch.Transition)},
			{Key: "subject_key", Value: ch.SubjectKey},
		},
	}
	add := func(k, v string) {
		if v != "" {
			f.Details = append(f.Details, emit.Field{Key: k, Value: v})
		}
	}
	addTime := func(k string, t time.Time) {
		if !t.IsZero() {
			add(k, t.UTC().Format(time.RFC3339))
		}
	}
	add("prev_severity", ch.PrevSeverity)
	addTime("first_seen", ch.FirstSeen)
	addTime("last_seen", ch.LastSeen)
	addTime("ack_until", ch.AckUntil)
	add("ack_by", ch.AckBy)
	return f
}
