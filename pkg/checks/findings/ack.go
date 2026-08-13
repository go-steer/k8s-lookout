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

// defaultAckWindow is the ack duration when --for is not given: long
// enough to cover a remediation and a rollout, short enough that
// forgetting one is not a silent outage. An ack always expires; there
// is deliberately no "forever" — a standing judgment about an
// incident is the §9.4 severity_override, which asserts a diagnosis.
const defaultAckWindow = 4 * time.Hour

// AckCommand builds `lookout findings ack`.
func AckCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "findings ack",
		MCPName: "k8s_findings_ack",
		Writes:  true,
		Summary: "Suppress one finding for a window after an operator has taken it — later diffs report it `suppressed` instead of re-raising it, and it comes back on its own when the window expires; the \"I'm on this, stop paging me until lunch\" surface.",
		Positional: &checks.Positional{
			Meta: "<subject-key>",
			Doc:  "the subject key from a `findings diff` record's subject_key field: <cluster>/<namespace>/<kind_of_object>/<normalized-name>/<canonical-reason>. Must name a currently-open subject — a resolved subject has no row to ack",
		},
		Flags: []emit.FlagSpec{
			{Name: "store", Type: emit.FlagString, Default: "",
				Help: "path to the sentinel's SQLite store (its --store file). Required: " + storeHint},
			{Name: "for", Type: emit.FlagDuration, Default: defaultAckWindow.String(),
				Help: "how long to suppress the subject. The window is absolute from now and always expires; to end one early use --clear"},
			{Name: "by", Type: emit.FlagString, Default: "",
				Help: "who took the ack, recorded verbatim on the row and echoed in later `suppressed` records. Lookout does not authenticate this: the caller (mast) owns identity and the audit trail, lookout owns the state"},
			{Name: "clear", Type: emit.FlagBool, Default: "false",
				Help: "end the ack window now instead of opening one; the subject goes back to being classified normally on the next diff"},
		},
		Output: []checks.OutputField{
			{Name: "subject_key", Doc: "the acked subject's key, as stored"},
			{Name: "ack_until", Doc: "when the window expires, RFC 3339; absent after --clear"},
			{Name: "ack_by", Doc: "who took the ack, as given by --by"},
			{Name: "first_seen", Doc: "when the acked subject was first observed, RFC 3339 — the \"broken since\" timestamp, so an operator can see what they are taking"},
			{Name: "last_seen", Doc: "when the acked subject was last observed, RFC 3339"},
		},
		Examples: []string{
			"lookout findings ack prod-east/prod/Pod/payment-backend/CrashLoopBackOff --store=/var/lib/lookout/lookout.db --for=4h --by=gari",
			"lookout findings ack prod-east/prod/Pod/payment-backend/CrashLoopBackOff --store=/var/lib/lookout/lookout.db --clear",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runAck(ctx, inv, deps)
		},
	}
}

func runAck(ctx context.Context, inv emit.Invocation, deps Deps) (int, error) {
	if len(inv.Args) != 1 || inv.Args[0] == "" {
		return 0, emit.UsageErrorf("findings ack takes one subject key (the subject_key field of a `findings diff` record, e.g. prod-east/prod/Pod/payment-backend/CrashLoopBackOff)")
	}
	subjectKey := inv.Args[0]
	clear := inv.Flags.Bool("clear")
	window := inv.Flags.Duration("for")
	if !clear && window <= 0 {
		return 0, emit.UsageErrorf("--for must be positive, got %s; to end an ack use --clear", window)
	}

	st, err := openStore(inv.Flags.String("store"))
	if err != nil {
		return 0, err
	}
	defer func() { _ = st.Close() }()

	var row findingstate.State
	if clear {
		row, err = st.UnackFinding(ctx, subjectKey)
	} else {
		row, err = st.AckFinding(ctx, subjectKey, inv.Flags.String("by"), deps.now().UTC().Add(window))
	}
	if err != nil {
		return 0, err
	}
	if err := inv.Out.Emit(ackFinding(row)); err != nil {
		return 0, err
	}
	return 1, nil
}

// ackFinding renders the resulting state row. It echoes the row that
// was actually stored rather than the request, so an operator sees the
// resolved absolute expiry — and, on --clear, sees an empty ack_until
// as confirmation rather than having to take silence for it.
func ackFinding(row findingstate.State) emit.Finding {
	f := emit.Finding{
		Kind:         "findings.ack",
		Severity:     emit.SeverityInfo,
		Namespace:    row.Namespace,
		KindOfObject: row.KindOfObject,
		Name:         row.Name,
		Reason:       row.Reason,
		Fingerprint:  row.Fingerprint,
		Details: []emit.Field{
			{Key: "subject_key", Value: row.SubjectKey},
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
	addTime("ack_until", row.AckUntil)
	add("ack_by", row.AckBy)
	addTime("first_seen", row.FirstSeen)
	addTime("last_seen", row.LastSeen)
	return f
}
