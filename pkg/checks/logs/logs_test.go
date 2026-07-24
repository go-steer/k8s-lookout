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

package logs

// End-to-end command tests (§13): fake.Clientset for pod/workload
// discovery, fixture-backed PodLogGetter for the streams (the fake's
// GetLogs subresource returns a canned constant), exact findings and
// summary asserted through the real §4.2 runner via checktest.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// fakeLogs is the fixture PodLogGetter: content per
// "namespace/pod/container", optional per-key errors, and a record
// of the options each stream was requested with.
type fakeLogs struct {
	streams map[string]string
	errs    map[string]error
	opts    map[string]*corev1.PodLogOptions
}

func (f *fakeLogs) Stream(_ context.Context, ns, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	key := ns + "/" + pod + "/" + opts.Container
	if f.opts == nil {
		f.opts = map[string]*corev1.PodLogOptions{}
	}
	f.opts[key] = opts
	if err := f.errs[key]; err != nil {
		return nil, err
	}
	s, ok := f.streams[key]
	if !ok {
		return nil, fmt.Errorf("no log fixture for %s", key)
	}
	return io.NopCloser(strings.NewReader(s)), nil
}

func pod(ns, name, container string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: container}}},
	}
}

func deployment(ns, name string, selector map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selector},
		},
	}
}

// checkoutFixture is the golden-test cluster: a two-pod Deployment
// with an unrelated pod that must stay out of workload scope.
func checkoutFixture() (kubernetes.Interface, *fakeLogs) {
	cs := fake.NewClientset(
		deployment("shop", "checkout", map[string]string{"app": "checkout"}),
		pod("shop", "checkout-6d5f9c7b4-abc12", "app", map[string]string{"app": "checkout"}),
		pod("shop", "checkout-6d5f9c7b4-def34", "app", map[string]string{"app": "checkout"}),
		pod("shop", "unrelated-0", "app", map[string]string{"app": "other"}),
	)
	logs := &fakeLogs{streams: map[string]string{
		"shop/checkout-6d5f9c7b4-abc12/app": strings.Join([]string{
			`2026-07-24T09:00:00.000000000Z INFO handled request path=/api/cart/123 status=200 dur=15ms`,
			`2026-07-24T09:00:01.000000000Z INFO handled request path=/api/cart/456 status=200 dur=9ms`,
			`2026-07-24T09:00:02.000000000Z 10.4.0.1 "GET /healthz HTTP/1.1" 200 "kube-probe/1.33"`,
			`2026-07-24T09:00:03.000000000Z ERROR charge failed order=991 err="timeout after 250ms"`,
		}, "\n") + "\n",
		"shop/checkout-6d5f9c7b4-def34/app": strings.Join([]string{
			`2026-07-24T09:00:04.000000000Z INFO handled request path=/api/cart/789 status=200 dur=21ms`,
			`2026-07-24T09:00:05.000000000Z ERROR charge failed order=1002 err="timeout after 260ms"`,
			`2026-07-24T09:00:06.000000000Z 10.4.0.1 "GET /healthz HTTP/1.1" 200 "kube-probe/1.33"`,
		}, "\n") + "\n",
		"shop/unrelated-0/app": "2026-07-24T09:00:00.000000000Z INFO unrelated pod line\n",
	}}
	return cs, logs
}

func deps(cs kubernetes.Interface, logs *fakeLogs) Deps {
	return Deps{
		Client: func() (kubernetes.Interface, error) { return cs, nil },
		Logs:   logs,
	}
}

func TestContract(t *testing.T) {
	cs, logs := checkoutFixture()
	c := New(deps(cs, logs))
	checktest.VerifyContract(t, c, "--workload=Deployment/shop/checkout")
	checktest.VerifyContract(t, c, "--namespace=shop")
	checktest.VerifyContract(t, c, "-A", "--max-templates=1", "--keep-probes")
	checktest.VerifyContract(t, c, "--pod=checkout-6d5f9c7b4-abc12", "--namespace=shop")
}

// TestGoldenLogfmtWorkload pins the full stdout stream byte-for-byte:
// template merging across pods, error-ish-first ordering, probe strip
// reporting, subject attribution, and the summary line.
func TestGoldenLogfmtWorkload(t *testing.T) {
	cs, logs := checkoutFixture()
	res := checktest.Run(t, New(deps(cs, logs)), "--workload=Deployment/shop/checkout")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	want := strings.Join([]string{
		`kind=log.template severity=warning namespace=shop template="ERROR charge failed order=<*> err=\"timeout after <*>\"" count=2 pods=2 level=error first_seen=2026-07-24T09:00:03Z last_seen=2026-07-24T09:00:05Z sample="ERROR charge failed order=991 err=\"timeout after 250ms\""`,
		`kind=log.template severity=info namespace=shop template="INFO handled request path=<*> status=<*> dur=<*>" count=3 pods=2 level=info first_seen=2026-07-24T09:00:00Z last_seen=2026-07-24T09:00:04Z sample="INFO handled request path=/api/cart/123 status=200 dur=15ms"`,
		`kind=log.probe_noise severity=info message="health/readiness probe request lines stripped (--keep-probes to keep them)" count=2`,
		`scanned=7 findings=3 elapsed=100ms`,
	}, "\n") + "\n"
	if res.Stdout != want {
		t.Errorf("golden mismatch:\n got: %q\nwant: %q", res.Stdout, want)
	}
	if res.Stderr != "" {
		t.Errorf("stderr not empty: %q", res.Stderr)
	}
}

func TestFetchOptionsPropagate(t *testing.T) {
	cs, logs := checkoutFixture()
	res := checktest.Run(t, New(deps(cs, logs)),
		"--pod=checkout-6d5f9c7b4-abc12", "--namespace=shop",
		"--since=30m", "--previous", "--tail=123")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	opts := logs.opts["shop/checkout-6d5f9c7b4-abc12/app"]
	if opts == nil {
		t.Fatal("stream was not requested")
	}
	if !opts.Timestamps {
		t.Error("Timestamps must always be requested (first_seen/last_seen come from them)")
	}
	if !opts.Previous {
		t.Error("--previous not propagated")
	}
	if opts.SinceSeconds == nil || *opts.SinceSeconds != 1800 {
		t.Errorf("SinceSeconds = %v, want 1800", opts.SinceSeconds)
	}
	if opts.TailLines == nil || *opts.TailLines != 123 {
		t.Errorf("TailLines = %v, want 123", opts.TailLines)
	}
	if opts.Container != "app" {
		t.Errorf("Container = %q, want app", opts.Container)
	}
}

func TestSinglePodSubjectAttribution(t *testing.T) {
	cs, logs := checkoutFixture()
	res := checktest.Run(t, New(deps(cs, logs)),
		"--pod=checkout-6d5f9c7b4-abc12", "--namespace=shop")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "kind_of_object=Pod name=checkout-6d5f9c7b4-abc12") {
		t.Errorf("single-pod findings must name the pod:\n%s", res.Stdout)
	}
}

func TestMaxTemplatesOverflow(t *testing.T) {
	cs, logs := checkoutFixture()
	res := checktest.Run(t, New(deps(cs, logs)),
		"--workload=Deployment/shop/checkout", "--max-templates=1")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	// The error cluster wins the single slot; the info cluster (3
	// lines) is summarized by the overflow record.
	if !strings.Contains(res.Stdout, "kind=log.overflow") ||
		!strings.Contains(res.Stdout, "omitted_templates=1 omitted_lines=3") {
		t.Errorf("overflow record missing or wrong:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "INFO handled request") {
		t.Errorf("info cluster should have been capped out:\n%s", res.Stdout)
	}
}

func TestKeepProbes(t *testing.T) {
	cs, logs := checkoutFixture()
	res := checktest.Run(t, New(deps(cs, logs)),
		"--workload=Deployment/shop/checkout", "--keep-probes")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if strings.Contains(res.Stdout, "log.probe_noise") {
		t.Errorf("--keep-probes must not report stripping:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "healthz") {
		t.Errorf("--keep-probes must cluster the probe lines:\n%s", res.Stdout)
	}
}

func TestPartialFetchFailureBecomesFinding(t *testing.T) {
	cs, logs := checkoutFixture()
	logs.errs = map[string]error{
		"shop/checkout-6d5f9c7b4-def34/app": errors.New("previous terminated container not found"),
	}
	res := checktest.Run(t, New(deps(cs, logs)), "--workload=Deployment/shop/checkout")
	if res.Code != emit.ExitData {
		t.Fatalf("partial failure must not abort: exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout,
		`kind=log.fetch_error severity=warning namespace=shop kind_of_object=Pod name=checkout-6d5f9c7b4-def34 reason=LogFetchFailed message="previous terminated container not found" container=app`) {
		t.Errorf("fetch error finding missing:\n%s", res.Stdout)
	}
	// The healthy pod's lines still cluster.
	if !strings.Contains(res.Stdout, "ERROR charge failed") {
		t.Errorf("surviving stream not clustered:\n%s", res.Stdout)
	}
}

func TestAllStreamsFailedIsRuntimeError(t *testing.T) {
	cs, logs := checkoutFixture()
	logs.errs = map[string]error{
		"shop/checkout-6d5f9c7b4-abc12/app": errors.New("boom"),
		"shop/checkout-6d5f9c7b4-def34/app": errors.New("boom"),
	}
	res := checktest.Run(t, New(deps(cs, logs)), "--workload=Deployment/shop/checkout")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit = %d, want %d", res.Code, emit.ExitRuntime)
	}
	if res.Stdout != "" {
		t.Errorf("stdout must stay clean on runtime error, got %q", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "all 2 log streams failed") {
		t.Errorf("stderr = %q", res.Stderr)
	}
}

func TestScopeAndFlagValidation(t *testing.T) {
	cs, logs := checkoutFixture()
	c := New(deps(cs, logs))
	tests := []struct {
		name   string
		args   []string
		stderr string
	}{
		{"no scope", nil, "scope required"},
		{"pod without namespace", []string{"--pod=x"}, "--pod requires --namespace"},
		{"pod with workload", []string{"--pod=x", "--workload=Deployment/shop/checkout"}, "mutually exclusive"},
		{"workload with namespace", []string{"--workload=Deployment/shop/checkout", "--namespace=shop"}, "carries its own namespace"},
		{"bad kind", []string{"--workload=CronJob/shop/x"}, "unsupported workload kind"},
		{"zero cap", []string{"--namespace=shop", "--max-templates=0"}, "--max-templates must be positive"},
		{"negative tail", []string{"--namespace=shop", "--tail=-1"}, "--tail must not be negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := checktest.Run(t, c, tt.args...)
			if res.Code != emit.ExitRuntime {
				t.Fatalf("exit = %d, want %d (stderr %q)", res.Code, emit.ExitRuntime, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tt.stderr) {
				t.Errorf("stderr = %q, want substring %q", res.Stderr, tt.stderr)
			}
			if res.Stdout != "" {
				t.Errorf("stdout must stay clean, got %q", res.Stdout)
			}
		})
	}
}

func TestWorkloadKinds(t *testing.T) {
	labels := map[string]string{"app": "w"}
	sel := &metav1.LabelSelector{MatchLabels: labels}
	cs := fake.NewClientset(
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns"},
			Spec: appsv1.StatefulSetSpec{Selector: sel}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns"},
			Spec: appsv1.DaemonSetSpec{Selector: sel}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns"},
			Spec: appsv1.ReplicaSetSpec{Selector: sel}},
		pod("ns", "w-0", "app", labels),
	)
	logs := &fakeLogs{streams: map[string]string{
		"ns/w-0/app": "2026-07-24T09:00:00Z INFO line one\n",
	}}
	for _, kind := range []string{"StatefulSet", "sts", "DaemonSet", "ds", "ReplicaSet", "rs"} {
		res := checktest.Run(t, New(deps(cs, logs)), "--workload="+kind+"/ns/w")
		if res.Code != emit.ExitData {
			t.Errorf("kind %s: exit = %d, stderr: %s", kind, res.Code, res.Stderr)
			continue
		}
		if !strings.Contains(res.Stdout, "scanned=1 findings=1") {
			t.Errorf("kind %s: summary wrong:\n%s", kind, res.Stdout)
		}
	}
}

func TestContainerFilterSkipsPodsWithoutIt(t *testing.T) {
	cs := fake.NewClientset(
		pod("ns", "a-0", "app", nil),
		pod("ns", "b-0", "sidecar", nil),
	)
	logs := &fakeLogs{streams: map[string]string{
		"ns/a-0/app": "2026-07-24T09:00:00Z INFO from app container\n",
	}}
	res := checktest.Run(t, New(deps(cs, logs)), "--namespace=ns", "--container=app")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	// b-0 has no "app" container: skipped entirely, no fetch error.
	if strings.Contains(res.Stdout, "log.fetch_error") {
		t.Errorf("pod without the container must be skipped, not errored:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "scanned=1 findings=1") {
		t.Errorf("summary wrong:\n%s", res.Stdout)
	}
}

// TestSanitizerRunsOnSamples proves the §6.5 seam applies to log
// content: a credential in a raw line is masked in both the sample
// and the template on the way out.
func TestSanitizerRunsOnSamples(t *testing.T) {
	cs := fake.NewClientset(pod("ns", "a-0", "app", nil))
	logs := &fakeLogs{streams: map[string]string{
		"ns/a-0/app": "2026-07-24T09:00:00Z INFO auth header Bearer c2VjcmV0LXRva2VuLWFiY2RlZg.payload.signature\n",
	}}
	res := checktest.Run(t, New(deps(cs, logs)), "--namespace=ns")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if strings.Contains(res.Stdout, "c2VjcmV0") {
		t.Errorf("credential leaked to stdout:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "[REDACTED]") {
		t.Errorf("expected redaction marker:\n%s", res.Stdout)
	}
}

// TestEndToEndCompression runs the full command over the 10k-line
// synthetic corpus and measures the §5 compression claim at the
// output boundary: bytes in vs. bytes out.
func TestEndToEndCompression(t *testing.T) {
	corpus := syntheticCorpus(10000, 4)
	streams := map[string]string{}
	rawBytes := 0
	var clientObjs []runtime.Object
	for name, lines := range corpus {
		content := strings.Join(lines, "\n") + "\n"
		streams["prod/"+name+"/app"] = content
		rawBytes += len(content)
		clientObjs = append(clientObjs, pod("prod", name, "app", map[string]string{"app": "api"}))
	}
	cs := fake.NewClientset(clientObjs...)
	logs := &fakeLogs{streams: streams}
	res := checktest.Run(t, New(deps(cs, logs)), "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Count(res.Stdout, "\n")
	if lines > 42 { // 40 cap + probe-noise + summary
		t.Errorf("output lines = %d, want <= 42", lines)
	}
	if !strings.HasSuffix(res.Stdout, "elapsed=100ms\n") {
		t.Errorf("missing summary line:\n%s", res.Stdout)
	}
	outBytes := len(res.Stdout)
	t.Logf("compression: %d bytes raw logs -> %d bytes distilled (%.1fx), %d output lines",
		rawBytes, outBytes, float64(rawBytes)/float64(outBytes), lines)
	if outBytes*20 > rawBytes {
		t.Errorf("compression below 20x: %d -> %d bytes", rawBytes, outBytes)
	}
}
