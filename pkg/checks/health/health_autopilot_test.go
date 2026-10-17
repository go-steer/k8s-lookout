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

package health_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/health"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// configProvider is NoProvider plus a cluster record.
type configProvider struct {
	cloud.Provider
	cfg cloud.ClusterConfig
}

type fixedConfig struct{ cfg cloud.ClusterConfig }

func (f fixedConfig) Config(context.Context) (cloud.ClusterConfig, error) { return f.cfg, nil }
func (f fixedConfig) UpgradeTargets(context.Context, string) (cloud.UpgradeTargets, error) {
	return cloud.UpgradeTargets{}, nil
}

func (p configProvider) Name() string { return "fake" }
func (p configProvider) ClusterConfig() (cloud.ClusterConfigAPI, bool) {
	return fixedConfig{p.cfg}, true
}

func testCommandWithConfig(cfg cloud.ClusterConfig, objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return health.New(health.Deps{
		Client:   func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Provider: func(context.Context) (cloud.Provider, error) { return configProvider{cloud.NoProvider, cfg}, nil },
		Now:      func() time.Time { return fixedNow },
	})
}

// idleAutopilot is an Autopilot cluster at 0 nodes: GKE's own pods
// Pending and its managed webhook's backend with no endpoints.
func idleAutopilot() []runtime.Object {
	return []runtime.Object{
		pendingPod("gke-gmp-system", "alertmanager-0"),
		pendingPod("kube-system", "kube-dns-1"),
		webhook("gmp-operator.gke-gmp-system.monitoring.googleapis.com", "gke-gmp-system", "gmp-operator"),
		service("gke-gmp-system", "gmp-operator"),
	}
}

func scorecard(t *testing.T, stdout string) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if rec := parseLine(t, line); rec["kind"] == "health.category" {
			out[rec["category"]] = rec
		}
	}
	return out
}

// TestIdleAutopilotScoresHealthy (#735): the idle state leads, and the
// categories it explains score healthy, counting what they folded — not
// a cluster "down" with every add-on and the GMP webhooks degraded.
func TestIdleAutopilotScoresHealthy(t *testing.T) {
	cfg := cloud.ClusterConfig{Name: "container-days-ap-demo", Autopilot: true}
	res := checktest.Run(t, testCommandWithConfig(cfg, idleAutopilot()...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.HasPrefix(res.Stdout, "kind=cluster.autopilot_idle severity=info kind_of_object=Cluster name=container-days-ap-demo reason=AutopilotIdle") {
		t.Fatalf("the idle finding must lead:\n%s", res.Stdout)
	}
	card := scorecard(t, res.Stdout)
	for cat, folded := range map[string]string{"pending": "2", "webhooks": "1"} {
		if card[cat]["status"] != "healthy" || card[cat]["idle_folded"] != folded {
			t.Errorf("%s = %v, want healthy with idle_folded=%s", cat, card[cat], folded)
		}
	}
	for _, rec := range card {
		if rec["status"] == "degraded" {
			t.Errorf("no category may degrade on an idle Autopilot cluster: %v", rec)
		}
	}
	if strings.Contains(res.Stdout, "kind=pod.pending") || strings.Contains(res.Stdout, "kind=webhook.") {
		t.Errorf("folded findings leaked:\n%s", res.Stdout)
	}
	if err := checktest.Verify(health.New(health.Deps{}), res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("output contract: %v", err)
	}
}

// TestUserPodPendingKeepsTheScorecard: one pod outside the GKE-managed
// namespaces is a real fault; everything is reported, and pending
// carries the scale-up explanation (#733).
func TestUserPodPendingKeepsTheScorecard(t *testing.T) {
	cfg := cloud.ClusterConfig{Name: "container-days-ap-demo", Autopilot: true}
	res := checktest.Run(t, testCommandWithConfig(cfg, append(idleAutopilot(), pendingPod("shop", "web-1"))...))
	if strings.Contains(res.Stdout, "autopilot_idle") || strings.Contains(res.Stdout, "idle_folded") {
		t.Fatalf("a user pod pending must not read as idle:\n%s", res.Stdout)
	}
	card := scorecard(t, res.Stdout)
	if card["pending"]["status"] != "degraded" || card["webhooks"]["status"] != "degraded" {
		t.Errorf("pending=%v webhooks=%v, want both degraded", card["pending"], card["webhooks"])
	}
	if !strings.Contains(res.Stdout, "kind=autoscaler.scaleup_blocked") || !strings.Contains(res.Stdout, "category=pending") {
		t.Errorf("pending must carry the scale-up explanation:\n%s", res.Stdout)
	}
	if err := checktest.Verify(health.New(health.Deps{}), res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("output contract: %v", err)
	}
}

// TestStandardClusterAtZeroNodesIsNotIdle: only Autopilot scales to 0
// by design; a Standard cluster there is an outage.
func TestStandardClusterAtZeroNodesIsNotIdle(t *testing.T) {
	res := checktest.Run(t, testCommandWithConfig(cloud.ClusterConfig{Name: "obi"}, idleAutopilot()...))
	if strings.Contains(res.Stdout, "autopilot_idle") || scorecard(t, res.Stdout)["pending"]["status"] != "degraded" {
		t.Errorf("a Standard cluster at 0 nodes must stay degraded:\n%s", res.Stdout)
	}
}
