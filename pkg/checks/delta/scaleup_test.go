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

package delta

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// scaleProvider serves a fixed cluster config and autoscaler decisions.
type scaleProvider struct {
	cloud.Provider
	cfg       cloud.ClusterConfig
	cfgErr    error
	decisions []cloud.ScaleDecision
	decErr    error
	noCap     bool
}

type fixedConfig struct {
	cfg cloud.ClusterConfig
	err error
}

func (f fixedConfig) Config(context.Context) (cloud.ClusterConfig, error) { return f.cfg, f.err }
func (f fixedConfig) UpgradeTargets(context.Context, string) (cloud.UpgradeTargets, error) {
	return cloud.UpgradeTargets{}, nil
}

type fixedDecisions struct {
	ds  []cloud.ScaleDecision
	err error
}

func (f fixedDecisions) ScaleDecisions(context.Context, cloud.TimeWindow) ([]cloud.ScaleDecision, error) {
	return f.ds, f.err
}

func (p scaleProvider) Name() string { return "fake" }
func (p scaleProvider) ClusterConfig() (cloud.ClusterConfigAPI, bool) {
	return fixedConfig{p.cfg, p.cfgErr}, true
}
func (p scaleProvider) Capacity() (cloud.CapacityAPI, bool) {
	if p.noCap {
		return nil, false
	}
	return fixedDecisions{p.decisions, p.decErr}, true
}

func pool(name string, enabled bool, min, max int64) cloud.NodePoolConfig {
	return cloud.NodePoolConfig{Name: name, Autoscaling: cloud.NodePoolAutoscaling{Enabled: enabled, MinNodeCount: min, MaxNodeCount: max}}
}

func poolNode(name, poolName string, ready bool) corev1.Node {
	n := *healthyNode(name)
	if !ready {
		n = *notReadyNode(name)
	}
	n.Labels = map[string]string{gkeNodePoolLabel: poolName}
	return n
}

func detail(f emit.Finding, key string) string {
	for _, d := range f.Details {
		if d.Key == key {
			return d.Value
		}
	}
	return ""
}

// TestExplainScaleUpNoPoolCanGrow is the live case behind #733: a
// cluster at 0 nodes whose pools all have autoscaling off.
func TestExplainScaleUpNoPoolCanGrow(t *testing.T) {
	p := scaleProvider{Provider: cloud.NoProvider, cfg: cloud.ClusterConfig{
		Name:      "obi-agent-matrix",
		NodePools: []cloud.NodePoolConfig{pool("cos-135", false, 0, 0), pool("ubuntu", true, 0, 0)},
	}}
	f, _, ok := explainScaleUp(t.Context(), p, 2, nil, true, testNow)
	if !ok {
		t.Fatal("no finding")
	}
	if f.Kind != kindScaleUpBlocked || f.Severity != emit.SeverityCritical || f.Reason != "NoPoolCanGrow" || f.Name != "obi-agent-matrix" {
		t.Errorf("finding %+v", f)
	}
	for _, want := range []string{"2 pod(s) are unschedulable with 0 Ready node(s)", "no node pool can grow: cos-135 (autoscaling off), ubuntu (autoscaling maximum is 0)"} {
		if !strings.Contains(f.Message, want) {
			t.Errorf("message %q missing %q", f.Message, want)
		}
	}
	if got := detail(f, "pools"); got != "cos-135 (fixed, 0 node(s), autoscaling off); ubuntu (0-0 per zone, 0 node(s), autoscaling maximum is 0)" {
		t.Errorf("pools = %q", got)
	}
	if detail(f, "nodes") != "0/0" {
		t.Errorf("nodes = %q", detail(f, "nodes"))
	}
}

// TestExplainScaleUpRefusals: pools can grow, but the autoscaler said
// why it would not, and each refusal is put on its pool.
func TestExplainScaleUpRefusals(t *testing.T) {
	p := scaleProvider{Provider: cloud.NoProvider,
		cfg: cloud.ClusterConfig{Name: "c", NodePools: []cloud.NodePoolConfig{pool("pool-a", true, 1, 3), pool("pool-a-spot", true, 0, 5)}},
		decisions: []cloud.ScaleDecision{
			{Decision: "noScaleUp", NodeGroup: "gke-c-pool-a-spot-1234abcd-grp", Reason: "no.scale.up.mig.failing.predicate"},
			{Decision: "noScaleUp", NodeGroup: "gke-c-pool-a-spot-1234abcd-grp", Reason: "no.scale.up.mig.failing.predicate"},
			{Decision: "noScaleUp", NodeGroup: "gke-c-pool-a-9876fedc-grp", Reason: "max.nodes.reached"},
			{Decision: "scaleUp", NodeGroup: "gke-c-pool-a-9876fedc-grp"},
		},
	}
	nodes := []corev1.Node{poolNode("n1", "pool-a", true), poolNode("n2", "pool-a", true), poolNode("n3", "pool-a", false)}
	f, _, ok := explainScaleUp(t.Context(), p, 1, nodes, true, testNow)
	if !ok || f.Severity != emit.SeverityWarning || f.Reason != "ScaleUpRefused" {
		t.Fatalf("finding %+v", f)
	}
	if got := detail(f, "no_scale_up"); got != "pool-a-spot: no.scale.up.mig.failing.predicate ×2; pool-a: max.nodes.reached ×1" {
		t.Errorf("no_scale_up = %q", got)
	}
	if detail(f, "nodes") != "2/3" || !strings.Contains(detail(f, "pools"), "pool-a (1-3 per zone, 3 node(s))") {
		t.Errorf("nodes %q pools %q", detail(f, "nodes"), detail(f, "pools"))
	}
}

// TestExplainScaleUpNothingBlocks: autoscaling can grow and refused
// nothing, so the pods' own constraints are named as the likelier cause.
func TestExplainScaleUpNothingBlocks(t *testing.T) {
	p := scaleProvider{Provider: cloud.NoProvider, cfg: cloud.ClusterConfig{Name: "c", NodePools: []cloud.NodePoolConfig{pool("p", true, 1, 5)}}}
	f, _, ok := explainScaleUp(t.Context(), p, 1, []corev1.Node{poolNode("n1", "p", true)}, true, testNow)
	if !ok || f.Severity != emit.SeverityInfo || !strings.Contains(f.Message, "pods' own constraints") {
		t.Errorf("finding %+v", f)
	}
}

func TestExplainScaleUpAutopilot(t *testing.T) {
	p := scaleProvider{Provider: cloud.NoProvider, cfg: cloud.ClusterConfig{Name: "ap", Autopilot: true, NodePools: []cloud.NodePoolConfig{pool("nap-x", false, 0, 0)}}}
	f, _, ok := explainScaleUp(t.Context(), p, 1, []corev1.Node{poolNode("n1", "nap-x", true)}, true, testNow)
	if !ok || f.Severity != emit.SeverityInfo || detail(f, "autopilot") != "true" || strings.Contains(f.Message, "autoscaling off") {
		t.Errorf("an Autopilot pool was judged like a Standard one: %+v", f)
	}
}

// TestExplainScaleUpDegrades: no permission for the pools is a note;
// decisions that cannot be read are said in the finding.
func TestExplainScaleUpDegrades(t *testing.T) {
	_, note, ok := explainScaleUp(t.Context(), scaleProvider{Provider: cloud.NoProvider, cfgErr: errors.New("permission denied")}, 1, nil, false, testNow)
	if ok || !strings.Contains(note, "node pool configuration: permission denied") {
		t.Errorf("note %q ok %t", note, ok)
	}
	_, note, ok = explainScaleUp(t.Context(), cloud.NoProvider, 1, nil, false, testNow)
	if ok || !strings.Contains(note, "cannot read node pool configuration") {
		t.Errorf("no provider: note %q ok %t", note, ok)
	}
	f, _, ok := explainScaleUp(t.Context(), scaleProvider{Provider: cloud.NoProvider, cfg: cloud.ClusterConfig{Name: "c", NodePools: []cloud.NodePoolConfig{pool("p", true, 1, 3)}}, decErr: errors.New("logging denied")}, 1, nil, false, testNow)
	if !ok || !strings.Contains(detail(f, "decisions"), "logging denied") || detail(f, "nodes") != "" {
		t.Errorf("decisions error: %+v", f)
	}
}

func TestPoolOfPrefersTheLongestName(t *testing.T) {
	pools := []cloud.NodePoolConfig{{Name: "pool-a"}, {Name: "pool-a-spot"}}
	for group, want := range map[string]string{
		"gke-c-pool-a-spot-1a2b-grp": "pool-a-spot",
		"gke-c-pool-a-3c4d-grp":      "pool-a",
		"gke-c-other-5e6f-grp":       "",
	} {
		if got := poolOf(group, pools); got != want {
			t.Errorf("poolOf(%q) = %q, want %q", group, got, want)
		}
	}
}

// TestTriageDeltaExplainsUnschedulablePods: end to end, an unschedulable
// pod brings the explanation into the stream; without one the provider
// is never asked.
func TestTriageDeltaExplainsUnschedulablePods(t *testing.T) {
	asked := 0
	provider := func(context.Context) (cloud.Provider, error) {
		asked++
		return scaleProvider{Provider: cloud.NoProvider, cfg: cloud.ClusterConfig{Name: "c", NodePools: []cloud.NodePoolConfig{pool("p", false, 0, 0)}}}, nil
	}
	run := func(objs ...runtime.Object) checktest.Result {
		client := fake.NewClientset(objs...)
		cmd := newCommand(func(context.Context) (kubernetes.Interface, error) { return client, nil }, func() time.Time { return testNow })
		cmd.Run = (&delta{source: func(context.Context) (kubernetes.Interface, error) { return client, nil }, now: func() time.Time { return testNow }, provider: provider}).run
		return checktest.Run(t, cmd)
	}

	res := run(pendingPod("prod", "batch-1", 30*time.Minute, true), &corev1.Node{})
	if !strings.Contains(res.Stdout, "kind=autoscaler.scaleup_blocked severity=critical kind_of_object=Cluster name=c reason=NoPoolCanGrow") {
		t.Errorf("no explanation:\n%s", res.Stdout)
	}
	if asked != 1 {
		t.Errorf("provider asked %d times", asked)
	}

	asked = 0
	res = run(pendingPod("prod", "slow", 30*time.Minute, false))
	if strings.Contains(res.Stdout, "autoscaler") || asked != 0 {
		t.Errorf("a pod Pending for other reasons consulted the provider (%d) or explained scale-up:\n%s", asked, res.Stdout)
	}
}
