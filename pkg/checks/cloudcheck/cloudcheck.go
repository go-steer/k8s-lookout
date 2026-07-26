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

// Package cloudcheck implements the `lookout cloud` command group
// (DESIGN.md §5): stockout, orphans, ipspace, and quota — the
// GCP-side point-in-time reads. Every command talks to the cloud
// exclusively through a pkg/cloud capability (§2: pkg/checks never
// imports cloud SDKs); when the capability is absent the command
// emits the standard explicit degradation record — one
// `cloud.unavailable` finding plus the `unavailable reason="..."`
// summary marker — and exits 0 with scanned=0, mirroring `triage top
// --history` (§2 "explicit, not broken": a vanilla-k8s build answers
// the question honestly instead of failing or staying silent).
//
// These are the one-shot companions of the resident capacity/quota
// sources (§10): `cloud stockout` and `cloud ipspace` report the
// instant, trend/slope math lives in `lookout watch`.
package cloudcheck

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(StockoutCommand(Deps{}))
	checks.Register(OrphansCommand(Deps{}))
	checks.Register(IPSpaceCommand(Deps{}))
	checks.Register(QuotaCommand(Deps{}))
}

// Deps are the injectable seams shared by the four commands. The
// zero value is the production wiring; tests inject a fake provider
// and clock (§13).
type Deps struct {
	// Provider yields the cloud provider. Nil means cloud.New
	// default detection (the NoProvider sentinel on vanilla builds —
	// every command then reports unavailable, never silence, §2).
	Provider func(ctx context.Context) (cloud.Provider, error)
	// Now anchors --since windows and age math. Nil means time.Now.
	Now func() time.Time
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

// rejectClusterScope is the shared usage guard: the cloud group
// reads the GCP project, not the cluster's namespaces, so the §4.2
// k8s scoping flags are a usage error rather than a silent no-op.
func rejectClusterScope(inv emit.Invocation, name string) error {
	if inv.Scope.Namespace != "" || inv.Scope.AllNamespaces || !inv.Scope.Workload.IsZero() {
		return emit.UsageErrorf("%s reads the cloud project, not cluster objects: --namespace/-A/--workload do not apply", name)
	}
	return nil
}

// emitUnavailable is the §2-mandated degradation path shared by all
// four commands: one explicit cloud.unavailable finding, the summary
// marker, exit 0 with scanned=0 (nothing was examined — and that is
// reported, not implied).
func emitUnavailable(inv emit.Invocation, p cloud.Provider, c cloud.Capability, what string) (int, error) {
	u := cloud.Unavailable(p, c)
	if err := inv.Out.Emit(emit.Finding{
		Kind:     "cloud.unavailable",
		Severity: emit.SeverityInfo,
		Reason:   "CapabilityUnavailable",
		Message:  fmt.Sprintf("%s needs the provider %s capability: %s", what, c, u.Reason),
		Details: []emit.Field{
			{Key: "capability", Value: string(u.Capability)},
			{Key: "provider", Value: u.Provider},
		},
	}); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("unavailable", u.Reason); err != nil {
		return 0, err
	}
	return 0, nil
}

// unavailableFields are the output-glossary entries every cloud
// command declares for the degradation path.
func unavailableFields(capability cloud.Capability) []checks.OutputField {
	return []checks.OutputField{
		{Name: "capability", Doc: "cloud.unavailable: the provider capability this command needed (" + string(capability) + ")"},
		{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
		{Name: "unavailable", Doc: "summary-line note (§2 marker): why the cloud read could not be served"},
	}
}

// fmtPct renders a percentage to at most one decimal, dropping a
// trailing ".0" (same rendering as `triage top` for token density).
func fmtPct(p float64) string {
	return strconv.FormatFloat(math.Round(p*10)/10, 'f', -1, 64)
}

// fmtFloat renders a quota usage/limit number: integral values stay
// integral ("600", not "600.000000").
func fmtFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// fmtTime renders finding timestamps: RFC3339 in UTC.
func fmtTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
