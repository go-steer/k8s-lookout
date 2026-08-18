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

package audit

import (
	"context"
	"fmt"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/exempt"
)

// The two kinds this command emits.
const (
	kindExpired  = "audit.exemption_expired"
	kindExpiring = "audit.exemption_expiring"
)

// defaultWithin is the look-ahead window when --within is not given.
// Two weeks is a sprint: long enough that the owner can renew or fix
// the underlying thing without an emergency, short enough that the
// warning still feels like it is about them.
const defaultWithin = 14 * 24 * time.Hour

// ExemptionsCommand builds `lookout audit exemptions`.
func ExemptionsCommand() checks.Command {
	return checks.Command{
		Name:    "audit exemptions",
		MCPName: "k8s_audit_exemptions",
		Summary: "Audit the exemption file itself: which reviewed exemptions have lapsed (and are therefore no longer annotating anything) and which are about to. The mechanism that keeps an exemption file from becoming a permanent, unread list of things nobody checks any more.",
		Flags: []emit.FlagSpec{
			{Name: "within", Type: emit.FlagDuration, Default: defaultWithin.String(),
				Help: "how far ahead to warn about entries that are still live but expiring soon; 0 reports only entries that have already lapsed"},
		},
		Kinds: []checks.KindField{
			checks.Kind(kindExpired, "an exemption entry has lapsed: the findings it used to annotate are being reported unqualified again", emit.SeverityWarning),
			checks.Kind(kindExpiring, "an exemption entry lapses within --within — renew it or let it go deliberately", emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "exempt_kind", Doc: "the finding kind the entry covers — this is the entry's `kind:` field, not this finding's own kind"},
			{Name: "subject", Doc: "the entry's match scope as written: `<kind>`, `<kind> in <ns>`, or `<kind> on <ns>/<name>`"},
			{Name: "expires", Doc: "when the entry stops applying, RFC 3339 (a bare `YYYY-MM-DD` in the file resolves to 00:00:00Z that day)"},
			{Name: "expired_for", Doc: "how long ago the entry lapsed, rounded to whole days — only on audit.exemption_expired"},
			{Name: "expires_in", Doc: "how long until the entry lapses, rounded to whole days — only on audit.exemption_expiring"},
			{Name: "owner", Doc: "the entry's `owner:` field, absent if it has none — which is itself worth fixing, since \"expired, and nobody knows whose it was\" is where these files end up"},
			{Name: "justification", Doc: "the entry's `reason:` field: why the exempted finding was accepted. Distinct from the envelope's exempt_reason, which is the justification for THIS finding being exempt"},
		},
		Examples: []string{
			"lookout audit exemptions --exemptions=exemptions.yaml",
			"lookout audit exemptions --exemptions=exemptions.yaml --within=720h",
			"lookout audit exemptions --exemptions=exemptions.yaml --within=0s",
		},
		Run: runExemptions,
	}
}

func runExemptions(_ context.Context, inv emit.Invocation) (int, error) {
	set := inv.Exemptions
	if set == nil {
		return 0, emit.UsageErrorf("audit exemptions requires --exemptions=<path>: this command reports on an exemption file, so there is nothing to report without one")
	}
	within := inv.Flags.Duration("within")
	if within < 0 {
		return 0, emit.UsageErrorf("--within must not be negative, got %s (0 reports only already-lapsed entries)", within)
	}

	// The set is bound to a single instant at load time, so every entry
	// in one run is judged against the same "now" — including the
	// boundary between "expired" and "expiring", which would otherwise
	// be decidable two ways in one report.
	now := set.Now()
	entries := set.Entries() // sorted soonest-expiry first
	for _, e := range entries {
		f, ok := exemptionFinding(e, now, within)
		if !ok {
			continue
		}
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	return len(entries), nil
}

// exemptionFinding renders one entry, or reports ok=false for an entry
// that is live and not yet due — the zero-nominal-state case.
//
// Note that these findings pass through the same exemption seam as
// every other finding, so an entry covering audit.exemption_expiring
// annotates this command's own output. That is not a loop: such an
// entry must itself carry an expiry, so the escape hatch expires too.
func exemptionFinding(e exempt.Entry, now time.Time, within time.Duration) (emit.Finding, bool) {
	left := e.ExpiresAt().Sub(now)
	var f emit.Finding
	switch {
	case left <= 0:
		f = emit.Finding{
			Kind:     kindExpired,
			Severity: emit.SeverityWarning,
			Reason:   "ExemptionExpired",
			Message: fmt.Sprintf("exemption for %s lapsed %s ago and no longer annotates anything; renew it or fix what it covered",
				e.Subject(), roundDays(-left)),
			Details: []emit.Field{{Key: "expired_for", Value: roundDays(-left)}},
		}
	case left <= within:
		f = emit.Finding{
			Kind:     kindExpiring,
			Severity: emit.SeverityInfo,
			Reason:   "ExemptionExpiring",
			Message:  fmt.Sprintf("exemption for %s expires in %s", e.Subject(), roundDays(left)),
			Details:  []emit.Field{{Key: "expires_in", Value: roundDays(left)}},
		}
	default:
		return emit.Finding{}, false
	}

	// The finding's subject is the exemption entry, so its namespace and
	// name are the entry's scope — which is also what makes these
	// findings addressable by the exemption mechanism itself.
	f.Namespace = e.Namespace
	f.Name = e.Name
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, "")
	f.Details = append([]emit.Field{
		{Key: "exempt_kind", Value: e.Kind},
		{Key: "subject", Value: e.Subject()},
		{Key: "expires", Value: e.ExpiresAt().UTC().Format(time.RFC3339)},
	}, f.Details...)
	if e.Owner != "" {
		f.Details = append(f.Details, emit.Field{Key: "owner", Value: e.Owner})
	}
	f.Details = append(f.Details, emit.Field{Key: "justification", Value: e.Reason})
	return f, true
}

// roundDays renders a duration at the grain an expiry is written in.
// Exemptions are dated in days, so reporting "1583h24m0s" would be
// precision the input never had; under a day it falls back to hours so
// "expires in 0d" never appears.
func roundDays(d time.Duration) string {
	if days := int(d.Hours() / 24); days >= 1 {
		return fmt.Sprintf("%dd", days)
	}
	return d.Truncate(time.Hour).String()
}
