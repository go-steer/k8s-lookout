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

package topologydrift

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// counts reads the watch-event totals back as resource/outcome → n.
func (w *watchEvents) counts() map[string]int64 {
	out := map[string]int64{}
	w.each(func(resource, outcome string, n int64) { out[resource+"/"+outcome] = n })
	return out
}

// The outcome is the §6.3 split §6.6.1's model asserts ~80/20, so the State
// has to report it exactly: an event that moved the index, and one that did
// not.
func TestState_ReportsWhetherTheIndexMoved(t *testing.T) {
	h := newHarness(t)
	h.OnNodeUpsert(node("n1", "zone-a"))
	h.OnNodeUpsert(node("n2", "zone-b"))

	steps := []struct {
		what string
		do   func() bool
		want bool
	}{
		{"a new pod", func() bool { return h.OnPodAdd(webPod("web-1", "n1")) }, true},
		{"the relist's replay of it", func() bool { return h.OnPodAdd(webPod("web-1", "n1")) }, false},
		{"status churn", func() bool {
			p := webPod("web-1", "n1")
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", RestartCount: 7}}
			return h.OnPodUpdate(nil, p)
		}, false},
		{"a move to another zone", func() bool { return h.OnPodUpdate(nil, webPod("web-1", "n2")) }, true},
		{"reaching a terminal phase", func() bool { return h.OnPodUpdate(nil, webPod("web-1", "n2", phase(corev1.PodSucceeded))) }, true},
		{"a terminal pod never counted", func() bool { return h.OnPodAdd(webPod("web-2", "n1", phase(corev1.PodFailed))) }, false},
		{"a bare pod", func() bool { return h.OnPodAdd(pod("bare", "prod", "n1")) }, false},
		{"deleting a counted pod", func() bool {
			h.OnPodAdd(webPod("web-3", "n1"))
			return h.OnPodDelete(webPod("web-3", "n1"))
		}, true},
		{"deleting it again", func() bool { return h.OnPodDelete(webPod("web-3", "n1")) }, false},
		{"a tombstone for a counted pod", func() bool {
			h.OnPodAdd(webPod("web-4", "n1"))
			return h.OnPodDelete(cache.DeletedFinalStateUnknown{Key: "prod/web-4", Obj: webPod("web-4", "n1")})
		}, true},
		{"a tombstone holding something else", func() bool {
			return h.OnPodDelete(cache.DeletedFinalStateUnknown{Key: "x", Obj: "not a pod"})
		}, false},
		{"not a pod at all", func() bool { return h.OnPodDelete(42) }, false},
	}
	for _, st := range steps {
		if got := st.do(); got != st.want {
			t.Errorf("%s: reported moved=%v, want %v", st.what, got, st.want)
		}
	}
}

func TestScaleLog_ReportsWhetherItChanged(t *testing.T) {
	l := newScaleLog()
	sub := deploySubject("prod", "web")
	for _, st := range []struct {
		what string
		got  bool
		want bool
	}{
		{"a first sighting", l.Observe(sub, 3, t0), true},
		{"a resync at the same size", l.Observe(sub, 3, t0), false},
		{"a resize", l.Observe(sub, 9, t0), true},
		{"forgetting it", l.Forget(sub), true},
		{"forgetting it again", l.Forget(sub), false},
	} {
		if st.got != st.want {
			t.Errorf("%s: reported changed=%v, want %v", st.what, st.got, st.want)
		}
	}
}

// Every delivered event is counted once, malformed ones as inert, so the total
// is the informer's delivery count. A scalable of neither kind cannot be
// attributed to an informer and is not counted at all.
func TestSource_TheHandlersCountEveryDeliveredEvent(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	ctx := context.Background()
	db := func(name, node string) *corev1.Pod {
		return pod(name, "prod", node, ownedBy("StatefulSet", "db"))
	}

	s.onNode(ctx, node("n-a", "zone-a"))  // applied: added
	s.onNode(ctx, node("n-a", "zone-a"))  // inert: heartbeat
	s.onNode(ctx, "not a node")           // inert
	s.onNodeDelete(ctx, node("n-z", "z")) // inert: never seen
	s.onPod(ctx, db("db-0", "n-a"))       // applied
	s.onPod(ctx, db("db-0", "n-a"))       // inert: the §6.3 early return
	s.onPod(ctx, db("db-0", "n-a"))       // inert
	s.onPod(ctx, "not a pod")             // inert
	s.onPodDelete(ctx, db("db-0", "n-a")) // applied
	s.onPodDelete(ctx, db("db-0", "n-a")) // inert
	s.onScalable(deploy("prod", "web", want32(3)))
	s.onScalable(deploy("prod", "web", want32(3)))
	s.onScalable(stateful("prod", "db", want32(3)))
	s.onScalableDelete(stateful("prod", "db", want32(3)))
	s.onScalableDelete(stateful("prod", "db", want32(3)))
	s.onScalable("neither kind")

	want := map[string]int64{
		"pod/inert": 4, "pod/applied": 2,
		"node/inert": 3, "node/applied": 1,
		"deployment/inert": 1, "deployment/applied": 1,
		"statefulset/inert": 1, "statefulset/applied": 2,
	}
	got := s.events.counts()
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s = %d, want %d (all: %v)", k, got[k], n, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d series, want %d: %v", len(got), len(want), got)
	}
}

// End to end through a running source: the counter reaches /metrics with all
// eight series, and the initial sync is in it. The handlers are registered
// before the instruments exist, so a counter kept by the instruments would
// have dropped exactly these events.
func TestSource_WatchEventsReachTheRegistryIncludingTheInitialSync(t *testing.T) {
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutTargetInfo(), otelprom.WithoutScopeInfo())
	if err != nil {
		t.Fatalf("otelprom.New: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	cfg := quickAlerts()
	cfg.Meter = mp.Meter(MeterName)
	pods := make([]runtime.Object, 0, 6)
	for i := range 6 {
		pods = append(pods, pod(fmt.Sprintf("db-%d", i), "prod", []string{"n-a", "n-b", "n-c"}[i%3], ownedBy("StatefulSet", "db")))
	}
	runAlerting(t, cfg, nil, threeZoneCluster(pods...)...)

	h := &promHarness{registry: reg}
	fam := h.family(t, "lookout_leeway_watch_events_total")
	got := map[string]float64{}
	for _, m := range fam.GetMetric() {
		var resource, outcome string
		for _, l := range m.GetLabel() {
			switch l.GetName() {
			case "resource":
				resource = l.GetValue()
			case "outcome":
				outcome = l.GetValue()
			}
		}
		got[resource+"/"+outcome] = m.GetCounter().GetValue()
	}
	if len(got) != 8 {
		t.Errorf("%d series, want all 8 from the first scrape: %v", len(got), got)
	}
	if got["pod/applied"] != 6 {
		t.Errorf("pod/applied = %v, want the 6 pods of the initial sync: %v", got["pod/applied"], got)
	}
	if got["node/applied"] != 3 {
		t.Errorf("node/applied = %v, want the 3 nodes of the initial sync: %v", got["node/applied"], got)
	}
}

// The path §6.6.1 prices at ~2 µs and says ~80% of events take: a pod update
// that moves nothing. #491 asked that the counter not show up in it; read the
// two benchmarks against each other.
//
// "metered" is the production shape: a real SDK meter, so the handler also
// pays for last_event_timestamp's Record.
func BenchmarkSource_InertPodEvent(b *testing.B) {
	for _, metered := range []bool{false, true} {
		b.Run(map[bool]string{false: "bare", true: "metered"}[metered], func(b *testing.B) {
			cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
			if metered {
				mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
				b.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
				cfg.Meter = mp.Meter(MeterName)
			}
			s := New(fake.NewSimpleClientset(), cfg)
			if metered {
				if err := s.startMetrics(); err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = s.metrics.Close() })
			}
			ctx := context.Background()
			s.onNode(ctx, node("n-a", "zone-a"))
			p := pod("db-0", "prod", "n-a", ownedBy("StatefulSet", "db"))
			s.onPod(ctx, p)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s.onPod(ctx, p)
			}
		})
	}
}

func BenchmarkWatchEvents_Note(b *testing.B) {
	var w watchEvents
	b.ReportAllocs()
	for b.Loop() {
		w.note(eventPod, false)
	}
}
