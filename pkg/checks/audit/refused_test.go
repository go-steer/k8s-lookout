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
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// refusedDeps is testDeps under a custom role refusing one read the
// built-in `view` role grants (#584).
func refusedDeps(gr schema.GroupResource, objs ...runtime.Object) audit.Deps {
	client := fake.NewClientset(objs...)
	checktest.RefuseRead(&client.Fake, gr)
	return audit.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return client, nil },
	}
}

func kindsIn(t *testing.T, stdout string) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, r := range findingLines(t, stdout) {
		out[r["kind"]]++
	}
	return out
}

// A refused PDB List skips the coverage claim. Judged against an empty
// PDB set, every multi-replica workload would read as uncovered — a
// finding invented from a permission gap.
func TestWorkloadsPDBsRefusedSkipsCoverageOnly(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(refusedDeps(
		schema.GroupResource{Group: "policy", Resource: "poddisruptionbudgets"}, postureCluster()...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kinds := kindsIn(t, res.Stdout)
	if kinds["audit.no_pdb"] != 0 {
		t.Errorf("audit.no_pdb judged against PDBs that were never read:\n%s", res.Stdout)
	}
	if kinds["read.unavailable"] != 1 || kinds["audit.single_replica"] == 0 {
		t.Errorf("want one refusal record and the other claims intact:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "audit.no_pdb not judged") {
		t.Errorf("the record does not name the skipped claim:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, " pdbs=") {
		t.Errorf("a pdbs= count of objects never read:\n%s", res.Stdout)
	}
}

// A refused HPA List skips every claim that judges the replica floor:
// spec.replicas is the autoscaler's current answer, so judging it in
// minReplicas' place would flap single_replica with load.
func TestWorkloadsHPAsRefusedSkipsReplicaFloorClaims(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(refusedDeps(
		schema.GroupResource{Group: "autoscaling", Resource: "horizontalpodautoscalers"}, postureCluster()...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kinds := kindsIn(t, res.Stdout)
	for _, k := range []string{"audit.single_replica", "audit.no_pdb", "audit.no_spread", "audit.hpa_cannot_scale"} {
		if kinds[k] != 0 {
			t.Errorf("%s judged without the autoscalers:\n%s", k, res.Stdout)
		}
	}
	if kinds["read.unavailable"] != 1 {
		t.Errorf("want one refusal record:\n%s", res.Stdout)
	}
}

// --workload naming a refused kind has nothing to audit: that is a
// failure in the shared wording, not "workload not found".
func TestWorkloadsTargetKindRefusedFailsWorded(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(refusedDeps(
		schema.GroupResource{Group: "apps", Resource: "deployments"}, postureCluster()...)),
		"--workload=Deployment/prod/legacy-api")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want 1\nstdout: %s", res.Code, res.Stdout)
	}
	want := "forbidden: list deployments.apps — namespaced, this identity lacks it; grant list on deployments (apps) via a ClusterRole or Role, as lookout's shipped ClusterRole does — workload Deployment/prod/legacy-api cannot be audited without it"
	if !strings.Contains(res.Stderr, want) {
		t.Errorf("stderr:\n%s\nwant it to contain:\n%s", res.Stderr, want)
	}
}

// audit hardening: refused ServiceAccounts skip the default-token
// claim, and refused Namespaces skip both namespace claims and the
// namespaces= count — the template claims still answer.
func TestHardeningRefusedReadsSkipTheirClaims(t *testing.T) {
	for _, tc := range []struct {
		gr      schema.GroupResource
		skipped string
	}{
		{schema.GroupResource{Resource: "serviceaccounts"}, "audit.default_sa_automount not judged"},
		{schema.GroupResource{Resource: "namespaces"}, "audit.podsecurity_gaps and audit.default_sa_automount not judged"},
		{schema.GroupResource{Group: "apps", Resource: "deployments"}, "Deployment pod templates not audited"},
	} {
		t.Run(tc.gr.String(), func(t *testing.T) {
			c := audit.HardeningCommand(refusedDeps(tc.gr, postureCluster()...))
			res := checktest.Run(t, c, "-A")
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}
			if !strings.Contains(res.Stdout, tc.skipped) {
				t.Errorf("the record does not name what was skipped:\n%s", res.Stdout)
			}
			if err := checktest.Verify(c, res.Stdout, emit.FormatLogfmt); err != nil {
				t.Errorf("contract: %v", err)
			}
			if tc.gr.Resource == "namespaces" && strings.Contains(res.Stdout, " namespaces=") {
				t.Errorf("a namespaces= count of objects never read:\n%s", res.Stdout)
			}
		})
	}
}
