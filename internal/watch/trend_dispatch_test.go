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
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

// TestDispatchSignal_ForecastExactWireShape pins the §8 "forecast"
// envelope field on the wire for trend-source signals: additive after
// "context" via omitempty, so every non-trend payload stays
// byte-identical to the frozen shapes (the M0 pin
// TestDispatcher_ExactInjectPayloadWireShape is untouched by design).
func TestDispatchSignal_ForecastExactWireShape(t *testing.T) {
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
	ts := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	disp.DispatchSignal(context.Background(), engine.Signal{
		Kind:     saturation.KindForecast,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "u1/app", Reason: "forecast_memory"},
			Namespace:    "prod",
			KindOfObject: "Pod",
			Name:         "web-1",
			Container:    "app",
			Node:         "node-1",
			Message:      "memory saturation forecast for web-1: current=980.0MiB limit=1.0GiB slope_per_min=3.2MiB — limit reached in ~14m at the observed trend (180 samples over 1h30m0s)",
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
		Forecast: &engine.Forecast{
			ETA:             ts.Add(14 * time.Minute),
			ConfidenceBasis: "linear-90m-window",
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
	want := `{"kind":"saturation.forecast","reason":"forecast_memory","namespace":"prod","kind_of_object":"Pod","name":"web-1","container":"app","uid":"u1/app","message":"memory saturation forecast for web-1: current=980.0MiB limit=1.0GiB slope_per_min=3.2MiB — limit reached in ~14m at the observed trend (180 samples over 1h30m0s)","count":1,"first_seen":"2026-07-25T10:00:00Z","last_seen":"2026-07-25T10:00:00Z","cluster":"prod-us-central1","source":"sentinel","severity":"critical","fingerprint":"sha256:68a1c9ff2e58dfb1485ce7be4b9b007b6e9da8b4294201663db908c8f9d9aea0","context":{"node":"node-1"},"forecast":{"eta":"2026-07-25T10:14:00Z","confidence_basis":"linear-90m-window"}}`
	if envelope.Message != want {
		t.Errorf("forecast payload wire shape drifted:\n got: %s\nwant: %s", envelope.Message, want)
	}
}
