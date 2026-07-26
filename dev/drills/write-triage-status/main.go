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

// write-triage-status is a DRILL FIXTURE, not a product surface: it
// writes one §9.4 triage-status record into a sentinel store the way
// an incident agent eventually will through core-agent's shared
// Memory interface.
//
// That interface does not exist yet (core-agent v2.7.0 ships no
// Memory surface, and lookout deliberately has no CLI/MCP write
// command for these records — pkg/store/memory.go documents that
// adding one is a §4.1 design-doc change first). Until it lands,
// M4's "health scan mid-incident reports the triage state" drill
// needs SOMETHING to play the diagnosing agent, and that something
// must write through the same pkg/memory contract the real path
// will use — hence this ~100-line stand-in. Documented as a gap in
// docs/milestones/M4.md; delete this tool when the real write path
// ships.
//
// The fingerprint is computed exactly as the sentinel stamps it
// (engine.Fingerprint over kind, canonicalized reason, object class,
// zone), so the record joins the live incident's signals in both
// consumers: `lookout health --store` (scan-side join) and the
// sentinel's §7.7 routing override (followups stop re-paging).
//
// Usage (drill doc: dev/drills/quota-exhaustion.md §health-check):
//
//	write-triage-status --store=/data/lookout.db \
//	  --signal-kind=k8s-event --reason=CrashLoopBackOff \
//	  --object=Pod --namespace=triagelab --name=checkout-abc \
//	  --status=triaged --severity-override=warning \
//	  --root-cause="bad connection string in checkout-config" \
//	  --action="PR #402 opened" --session=stub-sess-0007
//
// The write uses the sentinel store's own writer (WAL + busy_timeout
// absorb the concurrent sentinel); the sentinel's routing cache
// picks the record up within its 30s refresh.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func main() {
	var (
		storePath  = flag.String("store", "", "path to the sentinel's --store SQLite file (required)")
		signalKind = flag.String("signal-kind", engine.KindK8sEvent, "signal schema kind of the incident being triaged (§8), e.g. k8s-event, quota.forecast")
		reason     = flag.String("reason", "", "incident reason as fired, e.g. CrashLoopBackOff or BackOff (canonicalized like the sentinel does)")
		object     = flag.String("object", "", "kind of object, e.g. Pod, Deployment, Quota")
		namespace  = flag.String("namespace", "", "object namespace (empty for cluster-scoped objects)")
		name       = flag.String("name", "", "object name")
		zone       = flag.String("zone", "", "zone the sentinel stamps (empty today — §8)")
		status     = flag.String("status", string(memory.StatusTriaged), "triage status: investigating|triaged|actioned|escalated")
		rootCause  = flag.String("root-cause", "", "root-cause hypothesis")
		override   = flag.String("severity-override", "", "agent's severity judgment: critical|warning|info (empty = keep class)")
		action     = flag.String("action", "", "action taken / paper trail")
		session    = flag.String("session", "", "incident session id")
	)
	flag.Parse()
	if *storePath == "" || *reason == "" || *object == "" || *name == "" {
		flag.Usage()
		os.Exit(2)
	}

	fingerprint := engine.Fingerprint(*signalKind, engine.CanonicalReason(*reason), *object, *zone)
	rec := memory.TriageStatusRecord{
		Fingerprint:         fingerprint,
		ResourceKey:         memory.ResourceKey(*object, *namespace, *name),
		Session:             *session,
		Status:              memory.TriageStatus(*status),
		RootCauseHypothesis: *rootCause,
		SeverityOverride:    *override,
		Action:              *action,
	}

	st, err := store.Open(*storePath)
	if err != nil {
		log.Fatalf("write-triage-status: open %s: %v", *storePath, err)
	}
	written, upsertErr := st.UpsertTriageStatus(context.Background(), rec)
	if closeErr := st.Close(); closeErr != nil {
		log.Printf("write-triage-status: close: %v", closeErr)
	}
	if upsertErr != nil {
		log.Fatalf("write-triage-status: upsert: %v", upsertErr)
	}
	fmt.Printf("wrote triage-status record:\n  fingerprint  %s\n  resource_key %s\n  status       %s\n  override     %s\n  updated      %s\n",
		written.Fingerprint, written.ResourceKey, written.Status, written.SeverityOverride, written.Updated.Format("2006-01-02T15:04:05Z07:00"))
}
