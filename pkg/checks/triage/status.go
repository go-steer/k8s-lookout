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

package triage

// `lookout triage status` — the §9.4 triage-status PRODUCER surface
// (docs/triage-status-write-design.md, the §4.1 design-doc change the
// M4 drill's observation 1 called for). Incident playbooks call it to
// write the compact record at each material transition (diagnosed,
// action taken, escalated) so every later reader — the sentinel's
// severity routing, `lookout health --store`, a bundle — reports the
// TRIAGED reality instead of a fresh unknown. Without --status the
// same command answers the read question: what did this (or a
// previous) session already conclude about the incident?
//
// The write goes through pkg/memory's TriageWriter bound to the
// sentinel store — the identical contract the sentinel's §7.4
// recovery flip uses. WAL + busy-timeout absorb the CLI writer next
// to the resident sentinel (proven live in the M4 drill). Per §9.4
// there is deliberately no locking: agents write record CONTENT,
// the sentinel owns record LIFECYCLE (the resolved flip), and the
// upsert on (fingerprint, resource_key) keeps the record current
// state, not a journal.

import (
	"context"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// statusStoreHint is the usage-error tail pointing at the design
// note that admitted this write surface (§4.1 discipline).
const statusStoreHint = "triage-status records live in the sentinel's --store SQLite file (§9.4); see docs/triage-status-write-design.md"

// agentStatuses are the values an AGENT may write. "resolved" is the
// sentinel's lifecycle terminal (§7.4 recovery flips it) — an agent
// claiming it without the observed stability window would corrupt
// the §9.3 corpus labels.
const agentStatuses = "investigating|triaged|actioned|escalated"

// StatusCommand builds `lookout triage status`. Unlike the graph
// commands it needs no injected dependencies: the store path comes
// from --store and timestamps come from the store's own clock.
func StatusCommand() checks.Command {
	return checks.Command{
		Name:    "triage status",
		MCPName: "k8s_triage_status",
		Summary: "Write (or read back) the §9.4 triage-status record for an incident — diagnosis, action taken, and your severity judgment — so health scans stop reporting it as a fresh unknown and the sentinel stops re-paging followups; the incident playbooks' closing move.",
		Flags: []emit.FlagSpec{
			{Name: "store", Type: emit.FlagString, Default: "",
				Help: "path to the sentinel's SQLite store (its --store file). Required: " + statusStoreHint},
			{Name: "fingerprint", Type: emit.FlagString, Default: "",
				Help: "the incident-class fingerprint from the inject payload or store row (§8, sha256:…). Required to write; to read, this or --resource selects the record(s)"},
			{Name: "resource", Type: emit.FlagString, Default: "",
				Help: "resource key pinning the record to one object: <KindOfObject>/<namespace>/<name> (namespace segment empty for cluster-scoped objects, e.g. Node//gke-node-1). Required to write"},
			{Name: "status", Type: emit.FlagString, Default: "",
				Help: "triage state to record: " + agentStatuses + " (resolved is written by the sentinel's §7.4 recovery flip, never by agents). Empty = read mode: print the current record(s) instead of writing"},
			{Name: "session", Type: emit.FlagString, Default: "",
				Help: "incident session id that produced this record — the paper trail's pointer back to the transcript"},
			{Name: "root-cause", Type: emit.FlagString, Default: "",
				Help: "root-cause hypothesis one-liner"},
			{Name: "severity-override", Type: emit.FlagString, Default: "",
				Help: "your routing judgment for further signals of this incident: critical|warning|info (empty = keep the signal's own class). Honored by sentinel routing and health scans while the record is open"},
			{Name: "action", Type: emit.FlagString, Default: "",
				Help: "action taken / paper trail (\"fix PR opened; config rollout pending\")"},
		},
		Output: []checks.OutputField{
			{Name: "fingerprint", Doc: "the record's §8 incident-class fingerprint"},
			{Name: "resource_key", Doc: "the record's resource pin, as stored (<KindOfObject>/<namespace>/<name>)"},
			{Name: memory.DetailTriageStatus, Doc: "the record's triage state (investigating|triaged|actioned|escalated|resolved)"},
			{Name: memory.DetailTriageRootCause, Doc: "the recorded root-cause hypothesis"},
			{Name: memory.DetailTriageAction, Doc: "the recorded action / paper trail"},
			{Name: memory.DetailTriageSession, Doc: "the incident session that wrote the record"},
			{Name: "severity_override", Doc: "the recorded severity judgment (critical|warning|info), when one is set"},
			{Name: "updated", Doc: "when the record last changed, RFC 3339"},
		},
		Examples: []string{
			"lookout triage status --store=/var/lib/lookout/lookout.db --fingerprint=sha256:e2957792a0b3 --resource=Pod/prod/checkout-697567895d-2gglt --session=sess-0004 --status=triaged --severity-override=warning --root-cause=\"DB connection string invalid in checkout-config\" --action=\"fix PR opened; config rollout pending\"",
			"lookout triage status --store=/var/lib/lookout/lookout.db --fingerprint=sha256:e2957792a0b3",
			"lookout triage status --store=/var/lib/lookout/lookout.db --resource=Pod/prod/checkout-697567895d-2gglt",
		},
		Run: runStatus,
	}
}

func runStatus(ctx context.Context, inv emit.Invocation) (int, error) {
	storePath := inv.Flags.String("store")
	if storePath == "" {
		return 0, emit.UsageErrorf("--store is required: %s", statusStoreHint)
	}
	fingerprint := inv.Flags.String("fingerprint")
	resource := inv.Flags.String("resource")
	status := inv.Flags.String("status")
	if status == "" {
		return readStatus(ctx, inv, storePath, fingerprint, resource)
	}
	return writeStatus(ctx, inv, storePath, fingerprint, resource, status)
}

// writeStatus is the producer path: validate, upsert through the
// §9.4 TriageWriter, and echo the stored record.
func writeStatus(ctx context.Context, inv emit.Invocation, storePath, fingerprint, resource, status string) (int, error) {
	switch memory.TriageStatus(status) {
	case memory.StatusInvestigating, memory.StatusTriaged, memory.StatusActioned, memory.StatusEscalated:
	case memory.StatusResolved:
		return 0, emit.UsageErrorf("--status=resolved is the sentinel's §7.4 lifecycle terminal (the recovery flip writes it after the observed stability window); agents record %s", agentStatuses)
	default:
		return 0, emit.UsageErrorf("--status=%q is not a triage state (want %s)", status, agentStatuses)
	}
	if fingerprint == "" || resource == "" {
		return 0, emit.UsageErrorf("writing a triage-status record needs both --fingerprint (the §8 incident-class hash from the inject payload) and --resource (<KindOfObject>/<namespace>/<name> — the fingerprint alone is class-level and spans objects)")
	}
	if ov := inv.Flags.String("severity-override"); ov != "" && ov != "critical" && ov != "warning" && ov != "info" {
		return 0, emit.UsageErrorf("--severity-override=%q is not a §7.7 class (critical|warning|info)", ov)
	}
	rec := memory.TriageStatusRecord{
		Fingerprint:         fingerprint,
		ResourceKey:         resource,
		Session:             inv.Flags.String("session"),
		Status:              memory.TriageStatus(status),
		RootCauseHypothesis: inv.Flags.String("root-cause"),
		SeverityOverride:    inv.Flags.String("severity-override"),
		Action:              inv.Flags.String("action"),
	}
	st, err := store.Open(storePath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = st.Close() }()
	written, err := st.UpsertTriageStatus(ctx, rec)
	if err != nil {
		return 0, err
	}
	if err := inv.Out.Emit(statusFinding(written)); err != nil {
		return 0, err
	}
	return 1, nil
}

// readStatus is the read mode: print the current record(s) for
// --fingerprint / --resource (the record IS current state — §9.4
// upsert semantics — so this is "what was already concluded?").
func readStatus(ctx context.Context, inv emit.Invocation, storePath, fingerprint, resource string) (int, error) {
	if fingerprint == "" && resource == "" {
		return 0, emit.UsageErrorf("read mode (no --status) needs --fingerprint and/or --resource to select the record(s); to write one, add --status=" + agentStatuses)
	}
	st, err := store.OpenRead(storePath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = st.Close() }()
	records, err := st.TriageStatuses(ctx, memory.TriageQuery{Fingerprint: fingerprint})
	if err != nil {
		return 0, err
	}
	scanned := len(records)
	for _, rec := range records {
		if resource != "" && !rec.MatchesResource(resource) {
			continue
		}
		if err := inv.Out.Emit(statusFinding(rec)); err != nil {
			return 0, err
		}
	}
	return scanned, nil
}

// statusFinding renders one record. The envelope's object identity is
// recovered from the resource key so the line reads like every other
// lookout finding; the record's own fields ride as details under the
// same triage_* keys the health/bundle join emits (§9.4: one shape
// for every reader).
func statusFinding(rec memory.TriageStatusRecord) emit.Finding {
	kindOfObject, namespace, name := splitResourceKey(rec.ResourceKey)
	f := emit.Finding{
		Kind:         "triage.status",
		Severity:     emit.SeverityInfo,
		Namespace:    namespace,
		KindOfObject: kindOfObject,
		Name:         name,
		Details: []emit.Field{
			{Key: "fingerprint", Value: rec.Fingerprint},
			{Key: "resource_key", Value: rec.ResourceKey},
			{Key: memory.DetailTriageStatus, Value: string(rec.Status)},
		},
	}
	add := func(k, v string) {
		if v != "" {
			f.Details = append(f.Details, emit.Field{Key: k, Value: v})
		}
	}
	add(memory.DetailTriageRootCause, rec.RootCauseHypothesis)
	add("severity_override", rec.SeverityOverride)
	add(memory.DetailTriageAction, rec.Action)
	add(memory.DetailTriageSession, rec.Session)
	if !rec.Updated.IsZero() {
		add("updated", rec.Updated.UTC().Format(time.RFC3339))
	}
	return f
}

// splitResourceKey recovers (kindOfObject, namespace, name) from a
// stored resource key, tolerating the documented group/version
// prefix ("apps/v1/Deployment/prod/x") by reading the LAST three
// segments. Keys in another shape yield empty envelope fields; the
// verbatim resource_key detail always carries the truth.
func splitResourceKey(key string) (kindOfObject, namespace, name string) {
	parts := strings.Split(key, "/")
	if len(parts) < 3 {
		return "", "", ""
	}
	return parts[len(parts)-3], parts[len(parts)-2], parts[len(parts)-1]
}
