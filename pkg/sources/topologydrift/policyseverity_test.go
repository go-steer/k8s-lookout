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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// §10.1's `thresholds.severity` (shipped 2026-10-07, #543): a LeewayPolicy
// sets the routing level of its subjects' findings on one axis. The fixture is
// #543's own — a hostname anti-affinity sitting 8/1/1 across three zones,
// which is Tier B `placement_drift` at warning with no policy.

// promotingPolicy declares zone spread for app=api and promotes its findings.
func promotingPolicy(severity string) string {
	return fmt.Sprintf(`
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata: { name: api, namespace: payments }
spec:
  selector: { matchLabels: { app: api } }
  topologyKeys:
    - key: topology.kubernetes.io/zone
      mode: Spread
      thresholds:
        severity: %s
`, severity)
}

func TestPolicySeverity_PromotesTierBWithoutMovingTheTier(t *testing.T) {
	base := eightOneOne[0]
	inv := hostSpreadInventory(hostSpreadZones(base.perZone))
	pods := replicas(base.rep, base.nodes...)

	_, plain := zoneVerdict(inv, base.rep, pods, ResolveConfig{})
	if v := plain.Verdict; v.Tier != leeway.TierB || v.Severity != string(engine.SeverityWarning) || v.SeverityFromPolicy {
		t.Fatalf("no policy: verdict = %+v, want Tier B at warning from the tier", v)
	}

	policy := decode(t, promotingPolicy("critical"), false)
	_, ev := zoneVerdict(inv, base.rep, pods, ResolveConfig{Policy: policy})
	v := ev.Verdict
	if v.Tier != leeway.TierB || v.Kind != leeway.BreachDrift {
		t.Fatalf("verdict = %+v, want Tier B drift — a severity is not a contract", v)
	}
	if v.Severity != string(engine.SeverityCritical) || !v.SeverityFromPolicy {
		t.Errorf("severity = %q (from policy %v), want critical from the policy", v.Severity, v.SeverityFromPolicy)
	}
	if d := v.Route(false); !d.Signal || d.Severity != string(engine.SeverityCritical) {
		t.Errorf("route = %+v, want a critical signal", d)
	}
	if f := leeway.NewFinding(leeway.FindingInput{Verdict: v, Intent: ev.Intent, TopologyKey: zoneKey}); f.Kind != leeway.KindPlacementDrift || !f.SeverityFromPolicy {
		t.Errorf("finding = %s (severityFromPolicy %v), want placement_drift marked as policy-set", f.Kind, f.SeverityFromPolicy)
	}
}

// policySeverityRun runs #543's required-term cluster through Run with a
// LeewayPolicy served by discovery and delivered by a dynamic client, and
// returns the one signal it emits.
func policySeverityRun(t *testing.T, policyDocs ...string) sources.Signal {
	t.Helper()
	c := eightOneOne[0]
	objs := []runtime.Object{replicaSet("api-7f9", "payments", "api")}
	for _, n := range hostSpreadZones(c.perZone) {
		objs = append(objs, n)
	}
	for i, p := range replicas(c.rep, c.nodes...) {
		p.Name = fmt.Sprintf("api-7f9-%d", i)
		p.UID = types.UID("payments/" + p.Name)
		p.Status.Phase = corev1.PodRunning
		ownedBy("ReplicaSet", "api-7f9")(p)
		objs = append(objs, p)
	}
	client := fake.NewSimpleClientset(objs...)
	client.Resources = []*metav1.APIResourceList{{
		GroupVersion: policyGV.String(),
		APIResources: []metav1.APIResource{
			{Name: policyGVR.Resource, Kind: PolicyKind, Namespaced: true},
			{Name: clusterPolicyGVR.Resource, Kind: ClusterPolicyKind},
		},
	}}
	var policies []runtime.Object
	for _, doc := range policyDocs {
		policies = append(policies, policyObj(t, doc))
	}

	cfg := quickAlerts()
	cfg.TopologyKeys = DefaultTopologyKeys
	s := New(client, cfg)
	s.WithDynamic(dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), policyListKinds, policies...))
	s.logf = func(string, ...any) {}

	var mu sync.Mutex
	var got []sources.Signal
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(sig sources.Signal) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, sig)
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})
	signals := func() []sources.Signal {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
	waitFor(t, "the finding to reach the wire", func() bool { return len(signals()) > 0 })
	time.Sleep(50 * time.Millisecond)
	sigs := signals()
	if len(sigs) != 1 {
		t.Fatalf("%d signal(s), want exactly one zone finding: %v", len(sigs), sigs)
	}
	return sigs[0]
}

// TestPolicySeverity_CriticalOpensASessionForThePolicysSubjects is the
// maintainer's case end to end: the policy promotes the Tier B finding to
// critical, and §7.7 routes critical to a per-incident session.
func TestPolicySeverity_CriticalOpensASessionForThePolicysSubjects(t *testing.T) {
	sig := policySeverityRun(t, promotingPolicy("critical"))
	if sig.Kind != leeway.KindPlacementDrift || sig.Severity != engine.SeverityCritical {
		t.Fatalf("signal = %s at %s, want %s at critical", sig.Kind, sig.Severity, leeway.KindPlacementDrift)
	}
	for _, want := range []string{"tier B", "intent policy-crd", "severity critical set by policy"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message %q does not contain %q", sig.Message, want)
		}
	}
	// The dispatcher with no --severity flag keeps the source's level.
	if got := engine.RouteFor(engine.NewRoutingPolicy(nil).Classify(sig)); got != engine.RoutePerIncident {
		t.Errorf("§7.7 route = %v, want per-incident", got)
	}
}

// TestPolicySeverity_NoPolicyStaysAWarning: the same cluster with the CRD
// served and no policy installed is unchanged — Tier B, warning, watchboard.
func TestPolicySeverity_NoPolicyStaysAWarning(t *testing.T) {
	sig := policySeverityRun(t)
	if sig.Kind != leeway.KindPlacementDrift || sig.Severity != engine.SeverityWarning {
		t.Fatalf("signal = %s at %s, want %s at warning", sig.Kind, sig.Severity, leeway.KindPlacementDrift)
	}
	if strings.Contains(sig.Message, "set by policy") {
		t.Errorf("message claims a policy severity with no policy: %q", sig.Message)
	}
	if got := engine.RouteFor(engine.NewRoutingPolicy(nil).Classify(sig)); got != engine.RouteWatchboard {
		t.Errorf("§7.7 route = %v, want the watchboard", got)
	}
}

// TestPolicySeverity_TheGlobalSeverityFlagWins pins the precedence decision
// (§10.1 amendment, 2026-10-07): `lookout watch --severity kind=level` is
// applied by the dispatcher after the source stamps the policy's level, and
// it wins. The flag is the sentinel operator's; a LeewayPolicy is namespaced
// and can be written by a namespace tenant, so the cluster-wide setting is the
// one that can cap it.
func TestPolicySeverity_TheGlobalSeverityFlagWins(t *testing.T) {
	sig := policySeverityRun(t, promotingPolicy("critical"))
	overrides, err := engine.ParseSeverityOverrides([]string{leeway.KindPlacementDrift + "=warning"})
	if err != nil {
		t.Fatal(err)
	}
	routing := engine.NewRoutingPolicy(overrides)
	if got := routing.Classify(sig); got != engine.SeverityWarning {
		t.Errorf("classified = %s, want the flag's warning over the policy's critical", got)
	}
	if got := engine.RouteFor(routing.Classify(sig)); got != engine.RouteWatchboard {
		t.Errorf("§7.7 route = %v, want the watchboard", got)
	}

	// And a flag for a different kind leaves the policy's level alone.
	other, err := engine.ParseSeverityOverrides([]string{leeway.KindContractViolated + "=info"})
	if err != nil {
		t.Fatal(err)
	}
	if got := engine.NewRoutingPolicy(other).Classify(sig); got != engine.SeverityCritical {
		t.Errorf("classified = %s, want the policy's critical untouched by another kind's override", got)
	}
}
