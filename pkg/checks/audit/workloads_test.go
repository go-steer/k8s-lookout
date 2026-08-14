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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func testDeps(objs ...runtime.Object) audit.Deps {
	client := fake.NewClientset(objs...)
	return audit.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return client, nil },
	}
}

func ptr[T any](v T) *T { return &v }

// container builds a container whose probes are present or absent as
// asked; the probe bodies are irrelevant to every claim here, only
// their existence is.
func container(name string, readiness, liveness bool) corev1.Container {
	c := corev1.Container{Name: name, Image: "example.com/" + name + ":v1"}
	probe := func() *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{},
		}}
	}
	if readiness {
		c.ReadinessProbe = probe()
	}
	if liveness {
		c.LivenessProbe = probe()
	}
	return c
}

func template(labels map[string]string, cs ...corev1.Container) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels},
		Spec:       corev1.PodSpec{Containers: cs},
	}
}

func spread(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	t.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew: 1, TopologyKey: "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
	}}
	return t
}

func antiAffinity(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	t.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
			Weight:          100,
			PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: "kubernetes.io/hostname"},
		}},
	}}
	return t
}

func deploy(ns, name string, replicas int32, t corev1.PodTemplateSpec) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(replicas), Template: t},
	}
}

func sts(ns, name string, replicas int32, t corev1.PodTemplateSpec) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(replicas), Template: t},
	}
}

func daemon(ns, name string, t corev1.PodTemplateSpec) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DaemonSetSpec{Template: t},
	}
}

func pdb(ns, name string, sel *metav1.LabelSelector) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: sel},
	}
}

func matching(l map[string]string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: l}
}

// postureCluster is the shared fixture: one workload per posture
// shape the command has to get right.
//
//	prod/Deployment/checkout     3 replicas, PDB, spread, one bare sidecar
//	prod/Deployment/legacy-api   1 replica, fully probed
//	prod/Deployment/paused       0 replicas, no probes
//	prod/StatefulSet/cache       3 replicas, no PDB, no spread, no liveness
//	batch/Deployment/worker      2 replicas, no PDB, anti-affinity, fully probed
//	batch/DaemonSet/log-shipper  no probes
func postureCluster() []runtime.Object {
	return []runtime.Object{
		deploy("prod", "checkout", 3, spread(template(
			map[string]string{"app": "checkout"},
			container("app", true, true),
			container("envoy", false, false),
		))),
		pdb("prod", "checkout-pdb", matching(map[string]string{"app": "checkout"})),
		deploy("prod", "legacy-api", 1, template(
			map[string]string{"app": "legacy-api"},
			container("api", true, true),
		)),
		deploy("prod", "paused", 0, template(
			map[string]string{"app": "paused"},
			container("app", false, false),
		)),
		sts("prod", "cache", 3, template(
			map[string]string{"app": "cache"},
			container("redis", true, false),
		)),
		deploy("batch", "worker", 2, antiAffinity(template(
			map[string]string{"app": "worker"},
			container("worker", true, true),
		))),
		daemon("batch", "log-shipper", template(
			map[string]string{"app": "log-shipper"},
			container("fluentd", false, false),
		)),
	}
}

func TestWorkloadsContract(t *testing.T) {
	checktest.VerifyContract(t, audit.WorkloadsCommand(testDeps(postureCluster()...)), "-A")
}

func TestWorkloadsGolden(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	path := filepath.Join("testdata", "workloads.golden")
	if *update {
		if err := os.WriteFile(path, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden (run 'go test ./pkg/checks/audit -update'): %v", err)
	}
	if !bytes.Equal([]byte(res.Stdout), want) {
		t.Errorf("output does not match %s:\ngot:\n%s\nwant:\n%s", path, res.Stdout, want)
	}
}

// A workload with a PDB, a spread constraint and full probe coverage
// is the whole point of the command: it produces nothing. Zero
// nominal state is what makes findings=0 readable as "protected".
func TestWorkloadsQuietWhenProtected(t *testing.T) {
	objs := []runtime.Object{
		deploy("prod", "good", 3, spread(template(
			map[string]string{"app": "good"},
			container("app", true, true),
		))),
		pdb("prod", "good-pdb", matching(map[string]string{"app": "good"})),
	}
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.HasPrefix(res.Stdout, "scanned=1 findings=0 ") {
		t.Errorf("a fully protected workload must emit nothing, got:\n%s", res.Stdout)
	}
}

// A single-replica workload gets exactly one availability claim. The
// no_pdb claim would be true but useless — a PDB over one replica is
// a drain gridlock, not protection — and reporting both would double-
// count one outage and name the wrong remedy.
func TestWorkloadsSingleReplicaSuppressesNoPDB(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)),
		"--workload=Deployment/prod/legacy-api")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 1 || recs[0]["kind"] != "audit.single_replica" {
		t.Fatalf("want exactly one single_replica finding, got:\n%s", res.Stdout)
	}
	if recs[0]["replicas"] != "1" || recs[0]["severity"] != "warning" {
		t.Errorf("finding = %v", recs[0])
	}
}

// Scaling to zero is a deliberate act, and a workload that is off has
// no availability to protect. Its probes are still judged: the
// template is what runs when it comes back.
func TestWorkloadsScaledToZeroKeepsProbeClaims(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)),
		"--workload=Deployment/prod/paused")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kinds := kindSet(t, res.Stdout)
	for _, unwanted := range []string{"audit.single_replica", "audit.no_pdb", "audit.no_spread"} {
		if kinds[unwanted] {
			t.Errorf("a scaled-to-zero workload must make no availability claim, got %s:\n%s", unwanted, res.Stdout)
		}
	}
	if !kinds["audit.no_readiness_probe"] || !kinds["audit.no_liveness_probe"] {
		t.Errorf("probe claims still apply to a scaled-to-zero template:\n%s", res.Stdout)
	}
}

// A DaemonSet's replica count is the node count and a drain skips its
// pods, so none of the three availability claims can be true of one.
// Probes still apply.
func TestWorkloadsDaemonSetIsProbesOnly(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)),
		"--workload=DaemonSet/batch/log-shipper")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("want the two probe findings only, got:\n%s", res.Stdout)
	}
	for _, r := range recs {
		if !strings.HasPrefix(r["kind"], "audit.no_") || !strings.HasSuffix(r["kind"], "_probe") {
			t.Errorf("unexpected claim against a DaemonSet: %v", r)
		}
		if r["kind_of_object"] != "DaemonSet" {
			t.Errorf("kind_of_object = %q, want DaemonSet", r["kind_of_object"])
		}
	}
}

// Anti-affinity and topologySpreadConstraints are two ways to say the
// same thing; either one silences the spread claim.
func TestWorkloadsSpreadAcceptsEitherMechanism(t *testing.T) {
	for _, tc := range []struct {
		name string
		tpl  corev1.PodTemplateSpec
		want bool // want a no_spread finding
	}{
		{"topology-spread", spread(template(map[string]string{"app": "x"}, container("app", true, true))), false},
		{"anti-affinity", antiAffinity(template(map[string]string{"app": "x"}, container("app", true, true))), false},
		{"neither", template(map[string]string{"app": "x"}, container("app", true, true)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{
				deploy("prod", "x", 3, tc.tpl),
				pdb("prod", "x-pdb", matching(map[string]string{"app": "x"})),
			}
			res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
			if got := kindSet(t, res.Stdout)["audit.no_spread"]; got != tc.want {
				t.Errorf("no_spread present = %v, want %v:\n%s", got, tc.want, res.Stdout)
			}
		})
	}
}

// The policy/v1 selector semantics this check would otherwise get
// wrong: nil selects NOTHING, empty selects every pod in the
// namespace. Getting these backwards would silently under- or
// over-report the single most load-bearing claim in the command.
func TestWorkloadsPDBSelectorSemantics(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sel     *metav1.LabelSelector
		covered bool
	}{
		{"nil-selector-covers-nothing", nil, false},
		{"empty-selector-covers-everything", &metav1.LabelSelector{}, true},
		{"matching-labels", matching(map[string]string{"app": "x"}), true},
		{"other-labels", matching(map[string]string{"app": "y"}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{
				deploy("prod", "x", 3, spread(template(
					map[string]string{"app": "x"}, container("app", true, true)))),
				pdb("prod", "some-pdb", tc.sel),
			}
			res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
			got, want := kindSet(t, res.Stdout)["audit.no_pdb"], !tc.covered
			if got != want {
				t.Errorf("no_pdb present = %v, want %v (%s):\n%s", got, want, tc.name, res.Stdout)
			}
		})
	}
}

// A PDB in a different namespace never covers this workload, however
// well its selector matches.
func TestWorkloadsPDBDoesNotCrossNamespaces(t *testing.T) {
	objs := []runtime.Object{
		deploy("prod", "x", 3, spread(template(
			map[string]string{"app": "x"}, container("app", true, true)))),
		pdb("staging", "x-pdb", matching(map[string]string{"app": "x"})),
	}
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
	rec := findingLines(t, res.Stdout)
	if len(rec) != 1 || rec[0]["kind"] != "audit.no_pdb" {
		t.Fatalf("want a no_pdb finding, got:\n%s", res.Stdout)
	}
	// The namespace has no PDBs of its own, and the finding says so
	// rather than reporting the cluster total.
	if rec[0]["namespace_pdbs"] != "0" {
		t.Errorf("namespace_pdbs = %q, want 0 (the only PDB is in staging)", rec[0]["namespace_pdbs"])
	}
}

// Probe findings are per workload, not per container: the subject is
// the thing an operator edits, and a 40-container pod should not
// produce 40 rows. The count and the names carry the detail.
func TestWorkloadsProbeFindingsAreWorkloadGrained(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)),
		"--workload=Deployment/prod/checkout")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("want one finding per missing probe, not per container, got:\n%s", res.Stdout)
	}
	for _, r := range recs {
		if r["containers"] != "1" || r["container_names"] != "envoy" || r["total_containers"] != "2" {
			t.Errorf("finding = %v, want the sidecar named as 1 of 2", r)
		}
	}
}

// The posture recipe: the fingerprint is the CLASS, so the same claim
// against two different workloads of the same kind shares it, and the
// vector matches the one pinned in pkg/engine.
func TestWorkloadsFingerprintIsClassNotInstance(t *testing.T) {
	objs := []runtime.Object{
		deploy("prod", "a", 3, spread(template(map[string]string{"app": "a"}, container("app", true, true)))),
		deploy("prod", "b", 3, spread(template(map[string]string{"app": "b"}, container("app", true, true)))),
	}
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
	// Pinned in TestPostureFingerprint_PinnedVectors for
	// ("audit.no_pdb", "NoPodDisruptionBudget", "Deployment").
	const want = "fingerprint=sha256:7fe356f9a64f75392625206434494ed78e8fd020d5d35459c93fe96db64390ec"
	if got := strings.Count(res.Stdout, want); got != 2 {
		t.Errorf("both no_pdb findings should carry the pinned class fingerprint (%d of 2):\n%s", got, res.Stdout)
	}
}

// The same claim about a StatefulSet is a different class from the
// one about a Deployment: objectClass is part of the recipe, and the
// remedies differ enough that a fleet rollup should not merge them.
func TestWorkloadsFingerprintSeparatesObjectClasses(t *testing.T) {
	objs := []runtime.Object{
		deploy("prod", "a", 3, spread(template(map[string]string{"app": "a"}, container("app", true, true)))),
		sts("prod", "b", 3, spread(template(map[string]string{"app": "b"}, container("app", true, true)))),
	}
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(objs...)), "-A")
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("want one no_pdb per workload, got:\n%s", res.Stdout)
	}
	if recs[0]["fingerprint"] == recs[1]["fingerprint"] {
		t.Errorf("Deployment and StatefulSet no_pdb must not share a class: %s", recs[0]["fingerprint"])
	}
}

func TestWorkloadsScopes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		args        []string
		wantScanned string
	}{
		{"all-namespaces", []string{"-A"}, "scanned=6"},
		{"one-namespace", []string{"--namespace=batch"}, "scanned=2"},
		{"one-workload", []string{"--workload=StatefulSet/prod/cache"}, "scanned=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)), tc.args...)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}
			if !strings.Contains(res.Stdout, tc.wantScanned) {
				t.Errorf("summary should report %s:\n%s", tc.wantScanned, res.Stdout)
			}
		})
	}
}

func TestWorkloadsScopeErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no-scope", nil, "no scope"},
		{"A-with-workload", []string{"-A", "--workload=Deployment/prod/checkout"}, "does not combine"},
		{"contradictory-namespace", []string{"--namespace=batch", "--workload=Deployment/prod/checkout"}, "contradicts"},
		{"unsupported-kind", []string{"--workload=CronJob/prod/nightly"}, "unsupported workload kind"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)), tc.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d (usage); stderr: %s", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr = %q, want it to mention %q", res.Stderr, tc.want)
			}
		})
	}
}

// A --workload that resolves to nothing is a runtime error, not an
// empty clean scan: "scanned=0 findings=0" would read as "this
// workload is fine".
func TestWorkloadsUnknownWorkloadIsAnError(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)),
		"--workload=Deployment/prod/ghost")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want %d (runtime); stderr: %s", res.Code, emit.ExitRuntime, res.Stderr)
	}
	if res.Stdout != "" {
		t.Errorf("a failed run must not emit a summary line, got %q", res.Stdout)
	}
}

// The exemption seam is wired at the Writer, so it applies here with
// no work from this command: the finding is annotated and counted,
// never dropped.
func TestWorkloadsExemptionAnnotatesNeverDrops(t *testing.T) {
	const file = `exemptions:
  - kind: audit.single_replica
    namespace: prod
    name: legacy-api
    reason: vendor appliance, replacement tracked in PLAT-8812
    expires: 2026-06-30
`
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(postureCluster()...)),
		"--workload=Deployment/prod/legacy-api", "--exemptions="+write(t, file))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "kind=audit.single_replica ") {
		t.Errorf("an exempted finding must still be emitted:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, `exempt_reason="vendor appliance, replacement tracked in PLAT-8812"`) {
		t.Errorf("the finding should carry its exemption reason:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "findings=1") || !strings.Contains(res.Stdout, "exempt=1") {
		t.Errorf("summary should report findings=1 exempt=1:\n%s", res.Stdout)
	}
}

// Logfmt helpers, mirroring the other command test suites.

func parseLine(t *testing.T, line string) map[string]string {
	t.Helper()
	out := map[string]string{}
	rest := line
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			t.Fatalf("bad logfmt line %q", line)
		}
		key := rest[:eq]
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			end := len(rest)
			for i := 1; i < len(rest); i++ {
				if rest[i] == '"' && rest[i-1] != '\\' {
					end = i + 1
					break
				}
			}
			val = strings.ReplaceAll(rest[1:end-1], `\"`, `"`)
			rest = rest[end:]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		out[key] = val
		rest = strings.TrimPrefix(rest, " ")
	}
	return out
}

func findingLines(t *testing.T, stdout string) []map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	var out []map[string]string
	for _, l := range lines[:len(lines)-1] {
		out = append(out, parseLine(t, l))
	}
	return out
}

func kindSet(t *testing.T, stdout string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, r := range findingLines(t, stdout) {
		out[r["kind"]] = true
	}
	return out
}
