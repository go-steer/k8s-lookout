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

//go:build gke || allproviders

package gke

// Recorded-fixture tests per DESIGN.md §13: the Cloud Logging
// dependency stays behind the EntryLister interface and these tests
// run against testdata/*.json — fixtures AUTHORED FROM THE DOCUMENTED
// visibility-event format (see each fixture's _comment), validated
// for shape by the contract assertions below. No live-project tests.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// fixtureFile is the on-disk fixture shape: entries as
// (timestamp, jsonPayload) pairs, mirroring the wire fields the
// logadmin lister consumes.
type fixtureFile struct {
	Entries []struct {
		Timestamp   time.Time       `json:"timestamp"`
		JSONPayload json.RawMessage `json:"jsonPayload"`
	} `json:"entries"`
}

type fixtureLister struct {
	entries []LogEntry
	filter  string
	err     error
}

func (f *fixtureLister) ListEntries(_ context.Context, filter string) ([]LogEntry, error) {
	f.filter = filter
	return f.entries, f.err
}

func loadFixture(t *testing.T, name string) *fixtureLister {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f fixtureFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	if len(f.Entries) == 0 {
		t.Fatalf("fixture %s has no entries", name)
	}
	l := &fixtureLister{}
	for _, e := range f.Entries {
		if e.Timestamp.IsZero() || len(e.JSONPayload) == 0 {
			t.Fatalf("fixture %s: entry missing timestamp or jsonPayload (shape contract)", name)
		}
		l.entries = append(l.entries, LogEntry{Timestamp: e.Timestamp, Payload: e.JSONPayload})
	}
	return l
}

func fixtureAPI(l *fixtureLister) *capacityAPI {
	return &capacityAPI{
		project:  "prod-project",
		location: "us-east1",
		cluster:  "prod",
		lister:   l,
	}
}

var window = cloud.TimeWindow{
	Start: time.Date(2026, 7, 26, 9, 50, 0, 0, time.UTC),
	End:   time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC),
}

// decisionByGroup indexes decisions for assertion convenience.
func decisionByGroup(ds []cloud.ScaleDecision) map[string]cloud.ScaleDecision {
	out := make(map[string]cloud.ScaleDecision, len(ds))
	for _, d := range ds {
		out[d.NodeGroup] = d
	}
	return out
}

// TestScaleDecisions_NoScaleUpFixture: per-MIG rejections with GCE_*
// reason tokens map through verbatim; documentation-form messageIds
// pass through as the reason when no token parameter is present; a
// MIG-less top-level reason yields one MIG-less record.
func TestScaleDecisions_NoScaleUpFixture(t *testing.T) {
	l := loadFixture(t, "visibility_noscaleup.json")
	ds, err := fixtureAPI(l).ScaleDecisions(context.Background(), window)
	if err != nil {
		t.Fatalf("ScaleDecisions: %v", err)
	}
	if len(ds) != 5 {
		t.Fatalf("got %d decisions, want 5 (1 skipped + 3 rejected + 1 top-level)", len(ds))
	}
	// Shape contract (§13): every record fully populated.
	for _, d := range ds {
		if d.Time.IsZero() || d.Decision == "" || d.Reason == "" {
			t.Errorf("under-populated decision: %+v", d)
		}
		if d.Decision != "noScaleUp" {
			t.Errorf("decision = %q, want noScaleUp", d.Decision)
		}
	}
	by := decisionByGroup(ds)
	if got := by["gke-prod-pool-a-grp"].Reason; got != "GCE_STOCKOUT" {
		t.Errorf("pool-a reason = %q, want GCE_STOCKOUT", got)
	}
	if got := by["gke-prod-pool-b-grp"].Reason; got != "GCE_QUOTA_EXCEEDED" {
		t.Errorf("pool-b reason = %q, want GCE_QUOTA_EXCEEDED", got)
	}
	if got := by["gke-prod-pool-c-grp"].Reason; got != "no.scale.up.mig.failing.predicate" {
		t.Errorf("pool-c reason = %q, want the messageId (no token parameter)", got)
	}
	if got := by["gke-prod-pool-skipped-grp"].Reason; got != "no.scale.up.mig.skipped" {
		t.Errorf("skipped reason = %q", got)
	}
	if got := by[""].Reason; got != "no.scale.up.in.backoff" {
		t.Errorf("top-level MIG-less reason = %q, want no.scale.up.in.backoff", got)
	}
	if msg := by["gke-prod-pool-c-grp"].Message; !strings.Contains(msg, "node affinity") {
		t.Errorf("message %q must retain the descriptive parameters", msg)
	}
}

// TestScaleDecisions_ResultErrorFixture: eventResult errors normalize
// the documented messageIds to the boundary's machine-matchable
// tokens (out.of.resources = GCE stockout, per the GKE docs), keyed
// to the failing MIG names in parameters.
func TestScaleDecisions_ResultErrorFixture(t *testing.T) {
	l := loadFixture(t, "visibility_scaleup_result.json")
	ds, err := fixtureAPI(l).ScaleDecisions(context.Background(), window)
	if err != nil {
		t.Fatalf("ScaleDecisions: %v", err)
	}
	if len(ds) != 4 {
		t.Fatalf("got %d decisions, want 4 (1 triggered + 3 result errors)", len(ds))
	}
	var stockout, quota, ip, triggered *cloud.ScaleDecision
	for i := range ds {
		switch ds[i].Reason {
		case "GCE_STOCKOUT":
			stockout = &ds[i]
		case "GCE_QUOTA_EXCEEDED":
			quota = &ds[i]
		case "IP_SPACE_EXHAUSTED":
			ip = &ds[i]
		case "TRIGGERED":
			triggered = &ds[i]
		}
	}
	if stockout == nil || stockout.NodeGroup != "gke-prod-pool-a-grp" {
		t.Errorf("stockout = %+v, want it keyed to pool-a", stockout)
	}
	if quota == nil || quota.NodeGroup != "gke-prod-pool-b-grp" {
		t.Errorf("quota = %+v, want it keyed to pool-b", quota)
	}
	if ip == nil || ip.NodeGroup != "" {
		t.Errorf("ip exhaustion = %+v, want MIG-less (no parameter)", ip)
	}
	if triggered == nil || triggered.Decision != "scaleUp" || !strings.Contains(triggered.Message, "requested 2 node(s)") {
		t.Errorf("triggered = %+v", triggered)
	}
	if stockout != nil && !strings.Contains(stockout.Message, "scale.up.error.out.of.resources") {
		t.Errorf("stockout message %q must retain the raw messageId", stockout.Message)
	}
}

// TestScaleDecisions_Filter pins the Cloud Logging filter: the
// visibility logName, the k8s_cluster resource scoped to this
// cluster/location, and the half-open window.
func TestScaleDecisions_Filter(t *testing.T) {
	l := loadFixture(t, "visibility_noscaleup.json")
	if _, err := fixtureAPI(l).ScaleDecisions(context.Background(), window); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`logName="projects/prod-project/logs/container.googleapis.com%2Fcluster-autoscaler-visibility"`,
		`resource.type="k8s_cluster"`,
		`resource.labels.cluster_name="prod"`,
		`resource.labels.location="us-east1"`,
		`timestamp>="2026-07-26T09:50:00Z"`,
		`timestamp<"2026-07-26T10:00:00Z"`,
	} {
		if !strings.Contains(l.filter, want) {
			t.Errorf("filter %q missing %q", l.filter, want)
		}
	}
}

// TestScaleDecisions_MalformedEntrySkipped: one broken record must
// not blind the window.
func TestScaleDecisions_MalformedEntrySkipped(t *testing.T) {
	good := loadFixture(t, "visibility_noscaleup.json")
	l := &fixtureLister{entries: append([]LogEntry{
		{Timestamp: window.Start, Payload: json.RawMessage(`{"decision": "not-an-object"`)},
	}, good.entries...)}
	ds, err := fixtureAPI(l).ScaleDecisions(context.Background(), window)
	if err != nil {
		t.Fatalf("ScaleDecisions: %v", err)
	}
	if len(ds) != 5 {
		t.Errorf("got %d decisions, want the 5 good ones", len(ds))
	}
}

// TestScaleDecisions_ListerErrorSurfaces: a Logging failure is the
// caller's to log-and-retry, never swallowed.
func TestScaleDecisions_ListerErrorSurfaces(t *testing.T) {
	l := &fixtureLister{err: errors.New("PERMISSION_DENIED")}
	if _, err := fixtureAPI(l).ScaleDecisions(context.Background(), window); err == nil || !strings.Contains(err.Error(), "PERMISSION_DENIED") {
		t.Fatalf("err = %v, want the lister error wrapped", err)
	}
}

// TestProviderCapacityUsesLazyLister: the provider-constructed API
// dials Logging on first use only — construction stays offline-safe.
func TestProviderCapacityUsesLazyLister(t *testing.T) {
	t.Setenv(metadataHostEnv, "localhost:1")
	p, err := New(context.Background(), cloud.Config{Project: "p", Cluster: "c", Location: "l"})
	if err != nil {
		t.Fatal(err)
	}
	api, ok := p.Capacity()
	if !ok {
		t.Fatal("Capacity() unavailable")
	}
	c, ok := api.(*capacityAPI)
	if !ok {
		t.Fatalf("Capacity() = %T", api)
	}
	if c.lister != nil {
		t.Error("lister dialed at construction — must be lazy")
	}
	if c.newLister == nil {
		t.Error("no lister factory wired")
	}
}
