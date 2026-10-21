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

package audit_test

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// The pilot's acceptance test: over the same fixture, the CEL rules and
// the Go detectors must emit the same records for the kinds both
// implement — same message, same details, same order, same
// fingerprint. Everything else the Go commands emit (probe claims,
// PDB claims, the namespace's pod-security finding, the summary line)
// is filtered out of the comparison because the rules command does
// not make those claims.

// pilotKinds are the four kinds with a twin in each arm.
var pilotKinds = []string{
	"kind=audit.liveness_preempts_readiness ",
	"kind=audit.rollout_drops_traffic ",
	"kind=audit.recreate_strategy ",
	"kind=audit.image_floating_tag ",
}

func pilotLines(stdout string) []string {
	var out []string
	for _, line := range strings.Split(stdout, "\n") {
		for _, k := range pilotKinds {
			if strings.HasPrefix(line, k) {
				out = append(out, line)
				break
			}
		}
	}
	return out
}

func runPilot(t *testing.T, c checks.Command, objs []runtime.Object) []string {
	t.Helper()
	res := checktest.Run(t, c, "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("%s: exit %d, stderr: %s", c.Name, res.Code, res.Stderr)
	}
	return pilotLines(res.Stdout)
}

func assertSameLines(t *testing.T, what string, want, got []string) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("%s: the Go arm emitted nothing for the pilot kinds; the comparison is vacuous", what)
	}
	if strings.Join(want, "\n") == strings.Join(got, "\n") {
		return
	}
	t.Errorf("%s: CEL arm differs from Go arm\n--- go (%d lines)\n%s\n--- cel (%d lines)\n%s",
		what, len(want), strings.Join(want, "\n"), len(got), strings.Join(got, "\n"))
}

func TestRulesMatchGoArmOnRolloutCluster(t *testing.T) {
	objs := rolloutCluster()
	want := runPilot(t, audit.WorkloadsCommand(testDeps(objs...)), objs)
	got := runPilot(t, audit.RulesCommand(testDeps(objs...)), objs)
	assertSameLines(t, "rollout fixture", want, got)
}

func TestRulesMatchGoArmOnImagesCluster(t *testing.T) {
	objs := imagesCluster()
	want := runPilot(t, audit.HardeningCommand(testDeps(objs...)), objs)
	got := runPilot(t, audit.RulesCommand(testDeps(objs...)), objs)
	assertSameLines(t, "images fixture", want, got)
}

func TestRulesGolden(t *testing.T) {
	for name, objs := range map[string][]runtime.Object{"rollout": rolloutCluster(), "images": imagesCluster()} {
		t.Run(name, func(t *testing.T) {
			res := checktest.Run(t, audit.RulesCommand(testDeps(objs...)), "--namespace=prod")
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}
			checktest.Golden(t, "testdata/rules-"+name+".golden", res.Stdout)
		})
	}
}

func TestRulesContract(t *testing.T) {
	checktest.AssertEnvelopeContract(t, audit.RulesCommand(testDeps(rolloutCluster()...)), "--namespace=prod")
}

func TestRulesNeedsScope(t *testing.T) {
	res := checktest.Run(t, audit.RulesCommand(testDeps()))
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want usage", res.Code)
	}
}
