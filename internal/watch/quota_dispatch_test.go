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

package watch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/quota"
)

// TestDispatchSignal_QuotaDraftExactWireShape pins the §10.3
// quota_increase_draft envelope field on the wire: additive after
// "forecast" via omitempty, so every non-quota payload stays
// byte-identical to the frozen shapes. The draft is the write path's
// structured half — the agent files it through core-agent's
// permission gate; the sentinel only attaches it.
func TestDispatchSignal_QuotaDraftExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "prod-us-central1",
		mode:      "shared",
		targetSid: "sess-shared",
	}
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	disp.DispatchSignal(context.Background(), engine.Signal{
		Kind:     quota.KindForecast,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "quota:CPUS/us-east1", Reason: quota.Reason},
			KindOfObject: "Quota",
			Name:         "CPUS",
			Message:      "quota CPUS in us-east1 at 85.0% (usage 1700 / limit 2000), growing 50/day over the last 7d (8 points) — exhausted in ~6d at current slope; drafted increase to 3000 attached — file it via core-agent's permission gate",
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
		Forecast: &engine.Forecast{
			ETA:             ts.Add(6 * 24 * time.Hour),
			ConfidenceBasis: "linear-7d-window",
		},
		QuotaDraft: &engine.QuotaIncreaseDraft{
			QuotaID:        "compute.googleapis.com/CpusPerProjectPerRegion",
			Region:         "us-east1",
			CurrentUsage:   1700,
			CurrentLimit:   2000,
			SuggestedLimit: 3000,
			SlopePerDay:    50,
			Justification:  "CPUS in us-east1 is at 1700 of 2000 (85.0%). Usage grew 50/day over the observation window; at that slope the quota is exhausted in ~6d (around 2026-08-01). Requesting an increase to 3000 to cover twice the expected request turnaround at the observed growth.",
		},
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v (body=%q)", err, (*injects)[0])
	}
	want := `{"kind":"quota.forecast","reason":"quota_forecast","namespace":"","kind_of_object":"Quota","name":"CPUS","uid":"quota:CPUS/us-east1","message":"quota CPUS in us-east1 at 85.0% (usage 1700 / limit 2000), growing 50/day over the last 7d (8 points) — exhausted in ~6d at current slope; drafted increase to 3000 attached — file it via core-agent's permission gate","count":1,"first_seen":"2026-07-26T10:00:00Z","last_seen":"2026-07-26T10:00:00Z","cluster":"prod-us-central1","context":{},"forecast":{"eta":"2026-08-01T10:00:00Z","confidence_basis":"linear-7d-window"},"quota_increase_draft":{"quota_id":"compute.googleapis.com/CpusPerProjectPerRegion","region":"us-east1","current_usage":1700,"current_limit":2000,"suggested_limit":3000,"slope_per_day":50,"justification":"CPUS in us-east1 is at 1700 of 2000 (85.0%). Usage grew 50/day over the observation window; at that slope the quota is exhausted in ~6d (around 2026-08-01). Requesting an increase to 3000 to cover twice the expected request turnaround at the observed growth."}}`
	if envelope.Message != want {
		t.Errorf("quota draft payload wire shape drifted:\n got: %s\nwant: %s", envelope.Message, want)
	}
}

// wireQuotaAPI is a minimal cloud.QuotaAPI for buildSources wiring
// tests.
type wireQuotaAPI struct{}

func (wireQuotaAPI) Quotas(context.Context) ([]cloud.QuotaUsage, error) { return nil, nil }
func (wireQuotaAPI) History(context.Context, string, string, cloud.TimeWindow) (cloud.QuotaHistory, error) {
	return cloud.QuotaHistory{}, nil
}

// wireQuotaProvider grants exactly the quota capability.
type wireQuotaProvider struct{ cloud.Provider }

func (wireQuotaProvider) Name() string                  { return "test" }
func (wireQuotaProvider) Quota() (cloud.QuotaAPI, bool) { return wireQuotaAPI{}, true }

// TestSourcesFlag_QuotaRequiresProvider is the §11 startup contract:
// enabling the quota source without a quota-capable provider (the
// default build's NoProvider, or off-cloud) is a LOUD buildSources
// error naming the source — `lookout watch` refuses to start.
func TestSourcesFlag_QuotaRequiresProvider(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,quota", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	_, err = buildSources(f, fake.NewSimpleClientset(), nil, nil, nil)
	if err == nil {
		t.Fatal("buildSources must fail loudly when the quota source has no quota-capable provider")
	}
	for _, want := range []string{`source "quota"`, "project"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q must contain %q", err, want)
		}
	}
	// NoProvider explicitly (what cloud.New yields on an untagged
	// build): same refusal.
	_, err = buildSources(f, fake.NewSimpleClientset(), nil, nil, cloud.NoProvider)
	if err == nil || !strings.Contains(err.Error(), `source "quota"`) {
		t.Fatalf("buildSources with NoProvider = %v, want the loud quota error", err)
	}
}

// TestSourcesFlag_QuotaEnabled: with a quota-capable provider the
// source registers, and the quota flags flow into its config.
func TestSourcesFlag_QuotaEnabled(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,quota", "--quota-poll=30m", "--quota-window=336h", "--quota-warn=0.75", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bs, err := buildSources(f, fake.NewSimpleClientset(), nil, nil, wireQuotaProvider{Provider: cloud.NoProvider})
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if bs.quota == nil {
		t.Fatal("quota source not constructed")
	}
	all := bs.registry.All()
	if len(all) != 2 || all[1].Name() != quota.Name {
		t.Errorf("registry = %d sources, want k8s-events + quota", len(all))
	}
	if all[0].Name() != k8sevents.Name {
		t.Errorf("first source = %q, want k8s-events", all[0].Name())
	}
}

// TestQuotaFlags_Validation: nonsensical quota knobs are config
// errors in every mode, like the other source thresholds.
func TestQuotaFlags_Validation(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"--quota-poll=0s", "--dry-run"},
		{"--quota-window=0s", "--dry-run"},
		{"--quota-warn=0", "--dry-run"},
		{"--quota-warn=1", "--dry-run"},
		{"--quota-warn=1.5", "--dry-run"},
	}
	for _, args := range cases {
		f, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", args, err)
		}
		if err := f.validate(); err == nil {
			t.Errorf("validate(%v) accepted a nonsensical value", args)
		}
	}
}
