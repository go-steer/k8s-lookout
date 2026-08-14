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

package top_test

// §13 conventions: fake.Clientset + metrics fake fixtures (seeded
// through Tracker().Create with the EXPLICIT pods/nodes GVRs — the
// fake's generic Add guesses "podmetricses" from the kind, which is
// not the "pods" resource the client lists; same quirk as the
// saturation source's tests). Covered: above/below --top-warn, the
// memory-vs-cpu severity asymmetry, the no-limits census, workload
// and namespace scoping, the -A node view, the --history
// NoProvider unavailable path and the fake-backend enrichment path,
// the checktest contract round-trip, and a golden mixed cluster.
//
// #235 adds: the no-requests census, its subset relationship to the
// no-limits one (a burstable container is unlimited but NOT
// unrequested), and LimitRange annotation — which annotates and never
// suppresses on this command, because these are live pods and
// admission has already written any default into the spec.

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/top"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var fixedNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// newMetricsFake seeds pod and node metrics through the tracker with
// explicit GVRs (see the package comment for the quirk).
func newMetricsFake(t *testing.T, objs ...runtime.Object) *metricsfake.Clientset {
	t.Helper()
	cs := metricsfake.NewSimpleClientset()
	podGVR := schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods"}
	nodeGVR := schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "nodes"}
	for _, o := range objs {
		switch m := o.(type) {
		case *metricsv1beta1.PodMetrics:
			if err := cs.Tracker().Create(podGVR, m, m.Namespace); err != nil {
				t.Fatalf("seed pod metrics: %v", err)
			}
		case *metricsv1beta1.NodeMetrics:
			if err := cs.Tracker().Create(nodeGVR, m, ""); err != nil {
				t.Fatalf("seed node metrics: %v", err)
			}
		default:
			t.Fatalf("unsupported metrics fixture %T", o)
		}
	}
	return cs
}

func testDeps(metrics *metricsfake.Clientset, provider cloud.Provider, objs ...runtime.Object) top.Deps {
	cs := fake.NewClientset(objs...)
	return top.Deps{
		Client:   func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Metrics:  func(context.Context) (metricsv.Interface, error) { return metrics, nil },
		Provider: func(context.Context) (cloud.Provider, error) { return provider, nil },
		Now:      func() time.Time { return fixedNow },
	}
}

// pod builds a pod fixture; empty cpuLim/memLim mean "no limit" for
// that dimension.
//
// Requests mirror the LIVE-pod truth these fixtures stand in for: the
// apiserver copies an unset request down from the limit, so a fixture
// that sets a limit and no request would be a shape no real cluster
// ever returns — and would make the #235 requests census look like it
// fires on every limited container. podRequesting builds the pods
// that genuinely differ.
func pod(ns, name, node, container, cpuLim, memLim string, owners ...metav1.OwnerReference) *corev1.Pod {
	return podRequesting(ns, name, node, container, cpuLim, memLim, cpuLim, memLim, owners...)
}

// podRequesting builds a pod with requests set independently of
// limits — the shapes the apiserver does NOT produce by copy-down: a
// request with no limit (burstable), or neither (BestEffort).
func podRequesting(ns, name, node, container, cpuLim, memLim, cpuReq, memReq string, owners ...metav1.OwnerReference) *corev1.Pod {
	list := func(cpu, mem string) corev1.ResourceList {
		out := corev1.ResourceList{}
		if cpu != "" {
			out[corev1.ResourceCPU] = resource.MustParse(cpu)
		}
		if mem != "" {
			out[corev1.ResourceMemory] = resource.MustParse(mem)
		}
		return out
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-" + name), OwnerReferences: owners},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{{Name: container, Resources: corev1.ResourceRequirements{
				Limits:   list(cpuLim, memLim),
				Requests: list(cpuReq, memReq),
			}}},
		},
	}
}

// limitRange builds a namespace LimitRange whose Container item
// defaults the named dimensions (empty = not defaulted).
func limitRange(ns, name, defaultCPU, defaultMem, defaultReqCPU, defaultReqMem string) *corev1.LimitRange {
	list := func(cpu, mem string) corev1.ResourceList {
		out := corev1.ResourceList{}
		if cpu != "" {
			out[corev1.ResourceCPU] = resource.MustParse(cpu)
		}
		if mem != "" {
			out[corev1.ResourceMemory] = resource.MustParse(mem)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	return &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type:           corev1.LimitTypeContainer,
			Default:        list(defaultCPU, defaultMem),
			DefaultRequest: list(defaultReqCPU, defaultReqMem),
		}}},
	}
}

func podMetrics(ns, name, container, cpu, mem string) *metricsv1beta1.PodMetrics {
	return &metricsv1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Containers: []metricsv1beta1.ContainerMetrics{{
			Name: container,
			Usage: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(mem),
			},
		}},
	}
}

func nodeMetrics(name, cpu, mem string) *metricsv1beta1.NodeMetrics {
	return &metricsv1beta1.NodeMetrics{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Usage: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(mem),
		},
	}
}

func node(name, cpuAlloc, memAlloc string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuAlloc),
			corev1.ResourceMemory: resource.MustParse(memAlloc),
		}},
	}
}

func ownedBy(kind, name string) []metav1.OwnerReference {
	ctrl := true
	return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &ctrl, UID: types.UID(kind + "-" + name)}}
}

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

// findingLines drops the summary and returns parsed finding records.
func findingLines(t *testing.T, stdout string) []map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	var out []map[string]string
	for _, l := range lines[:len(lines)-1] {
		out = append(out, parseLine(t, l))
	}
	return out
}

func summaryLine(t *testing.T, stdout string) map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	return parseLine(t, lines[len(lines)-1])
}

// mixedFixture is the shared broken-ish cluster: one container at
// critical memory, one at warning cpu (asymmetry pair at the same
// 96%), one at warning memory, one healthy, one with no limits, and
// a hot node.
func mixedFixture(t *testing.T) (*metricsfake.Clientset, []runtime.Object) {
	t.Helper()
	metrics := newMetricsFake(t,
		podMetrics("prod", "api-1", "app", "100m", "480Mi"),   // mem 93.8% warn, cpu 20% —
		podMetrics("prod", "api-2", "app", "480m", "490Mi"),   // cpu 96% warn (capped), mem 95.7% CRITICAL
		podMetrics("prod", "worker-1", "work", "50m", "64Mi"), // 10% / 12.5% — silent
		podMetrics("prod", "batch-1", "job", "900m", "2Gi"),   // no limits — census
		nodeMetrics("node-1", "3800m", "7Gi"),                 // cpu 95% warn, mem 87.5% warn
		nodeMetrics("node-2", "400m", "1Gi"),                  // 10% / 12.5% — silent
	)
	objs := []runtime.Object{
		pod("prod", "api-1", "node-1", "app", "500m", "512Mi"),
		pod("prod", "api-2", "node-1", "app", "500m", "512Mi"),
		pod("prod", "worker-1", "node-2", "work", "500m", "512Mi"),
		pod("prod", "batch-1", "node-2", "job", "", ""),
		node("node-1", "4", "8Gi"),
		node("node-2", "4", "8Gi"),
	}
	return metrics, objs
}

func TestThresholdsAndSeverityAsymmetry(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	res := checktest.Run(t, cmd, "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)

	type key struct{ pod, resource string }
	sat := map[key]map[string]string{}
	for _, r := range recs {
		if r["kind"] == "top.saturation" {
			sat[key{r["name"], r["resource"]}] = r
		}
	}
	// The asymmetry pair: memory 95.7% is critical, cpu 96% is
	// warning ONLY (throttling, not death).
	mem := sat[key{"api-2", "memory"}]
	if mem == nil || mem["severity"] != emit.SeverityCritical || mem["reason"] != "MemoryNearLimit" {
		t.Errorf("api-2 memory = %v, want critical MemoryNearLimit", mem)
	}
	cpu := sat[key{"api-2", "cpu"}]
	if cpu == nil || cpu["severity"] != emit.SeverityWarning || cpu["reason"] != "CPUNearLimit" {
		t.Errorf("api-2 cpu = %v, want warning CPUNearLimit (CPU caps at warning)", cpu)
	}
	if mem["pct"] != "95.7" || cpu["pct"] != "96" {
		t.Errorf("pcts = mem %s cpu %s, want 95.7 / 96", mem["pct"], cpu["pct"])
	}
	// Warning memory below the 95% critical line.
	if w := sat[key{"api-1", "memory"}]; w == nil || w["severity"] != emit.SeverityWarning {
		t.Errorf("api-1 memory = %v, want warning", w)
	}
	// Below-threshold rows are absent (zero nominal state).
	if r, ok := sat[key{"worker-1", "cpu"}]; ok {
		t.Errorf("worker-1 cpu emitted below --top-warn: %v", r)
	}
	if r, ok := sat[key{"api-1", "cpu"}]; ok {
		t.Errorf("api-1 cpu (20%%) emitted below --top-warn: %v", r)
	}
	// Namespace scope: no node findings without -A.
	for _, r := range recs {
		if r["kind"] == "top.node" {
			t.Errorf("node finding under --namespace: %v", r)
		}
	}
	// Ordering: pct descending among container findings.
	var pcts []string
	for _, r := range recs {
		if r["kind"] == "top.saturation" {
			pcts = append(pcts, r["pct"])
		}
	}
	if want := []string{"96", "95.7", "93.8"}; strings.Join(pcts, ",") != strings.Join(want, ",") {
		t.Errorf("container pct order = %v, want %v", pcts, want)
	}
}

func TestUnlimitedCensus(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))

	res := checktest.Run(t, cmd, "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var agg map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		switch r["kind"] {
		case "top.unlimited":
			agg = r
		case "top.unlimited_container":
			t.Errorf("listing emitted without --show-unlimited: %v", r)
		}
	}
	if agg == nil || agg["pods"] != "1" || agg["containers"] != "1" || agg["reason"] != "NoLimits" {
		t.Fatalf("aggregate = %v, want pods=1 containers=1 reason=NoLimits", agg)
	}

	res = checktest.Run(t, cmd, "--namespace=prod", "--show-unlimited")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var listed []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "top.unlimited_container" {
			listed = append(listed, r)
		}
	}
	if len(listed) != 1 || listed[0]["name"] != "batch-1" || listed[0]["container"] != "job" || listed[0]["missing"] != "cpu,memory" {
		t.Errorf("listing = %v, want batch-1/job missing cpu,memory", listed)
	}
}

func TestPartialLimitsCountedWithMissingDimension(t *testing.T) {
	metrics := newMetricsFake(t, podMetrics("prod", "half-1", "app", "100m", "100Mi"))
	cmd := top.New(testDeps(metrics, cloud.NoProvider,
		pod("prod", "half-1", "n1", "app", "500m", ""))) // cpu limited, memory not
	res := checktest.Run(t, cmd, "--namespace=prod", "--show-unlimited")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var got map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "top.unlimited_container" {
			got = r
		}
	}
	if got == nil || got["missing"] != "memory" {
		t.Errorf("half-limited container = %v, want missing=memory", got)
	}
}

// censusFixture holds the four shapes the two censuses must tell
// apart, all in one namespace:
//
//	guaranteed — limit + request      → in neither census
//	burstable  — request, no limit    → unlimited ONLY (the subset proof)
//	besteffort — neither              → both
//	halfway    — cpu both, memory none → both, memory dimension only
func censusFixture(t *testing.T) (*metricsfake.Clientset, []runtime.Object) {
	t.Helper()
	metrics := newMetricsFake(t,
		podMetrics("prod", "guaranteed-1", "app", "10m", "10Mi"),
		podMetrics("prod", "burstable-1", "app", "10m", "10Mi"),
		podMetrics("prod", "besteffort-1", "app", "10m", "10Mi"),
		podMetrics("prod", "halfway-1", "app", "10m", "10Mi"),
	)
	objs := []runtime.Object{
		pod("prod", "guaranteed-1", "n1", "app", "500m", "512Mi"),
		podRequesting("prod", "burstable-1", "n1", "app", "", "", "100m", "128Mi"),
		podRequesting("prod", "besteffort-1", "n1", "app", "", "", "", ""),
		podRequesting("prod", "halfway-1", "n1", "app", "500m", "", "500m", ""),
	}
	return metrics, objs
}

// byKind indexes finding records by their kind.
func byKind(t *testing.T, stdout string) map[string][]map[string]string {
	t.Helper()
	out := map[string][]map[string]string{}
	for _, r := range findingLines(t, stdout) {
		out[r["kind"]] = append(out[r["kind"]], r)
	}
	return out
}

// TestUnrequestedCensus is #235's R1: the requests census counts the
// containers the scheduler sees as zero, and is a strict subset of
// the limits census — a burstable container is unlimited but has a
// request, so it must appear in exactly one of the two.
func TestUnrequestedCensus(t *testing.T) {
	metrics, objs := censusFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))

	res := checktest.Run(t, cmd, "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kinds := byKind(t, res.Stdout)

	// Aggregates only, without the --show-* flags.
	for _, k := range []string{"top.unlimited_container", "top.unrequested_container"} {
		if got := kinds[k]; got != nil {
			t.Errorf("%s listed without its --show-* flag: %v", k, got)
		}
	}
	// unlimited: burstable, besteffort, halfway (memory) = 3.
	if agg := kinds["top.unlimited"]; len(agg) != 1 || agg[0]["containers"] != "3" || agg[0]["pods"] != "3" {
		t.Errorf("top.unlimited = %v, want containers=3 pods=3", agg)
	}
	// unrequested: besteffort, halfway (memory) = 2. Burstable is the
	// difference between the two censuses.
	agg := kinds["top.unrequested"]
	if len(agg) != 1 || agg[0]["containers"] != "2" || agg[0]["pods"] != "2" || agg[0]["reason"] != "NoRequests" {
		t.Fatalf("top.unrequested = %v, want containers=2 pods=2 reason=NoRequests", agg)
	}
	if agg[0]["severity"] != emit.SeverityInfo {
		t.Errorf("top.unrequested severity = %s, want info", agg[0]["severity"])
	}
	// No LimitRange in this fixture, so no annotation field (zero
	// nominal state applies to fields too).
	if v := agg[0]["limitrange_defaulted"]; v != "" {
		t.Errorf("limitrange_defaulted=%s with no LimitRange in the namespace", v)
	}

	// The per-container listing, and its dimension accounting.
	res = checktest.Run(t, cmd, "--namespace=prod", "--show-unrequested")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kinds = byKind(t, res.Stdout)
	listed := map[string]string{}
	for _, r := range kinds["top.unrequested_container"] {
		listed[r["name"]] = r["missing"]
		if r["reason"] != "NoRequests" || r["kind_of_object"] != "Pod" {
			t.Errorf("listing row %v, want reason=NoRequests kind_of_object=Pod", r)
		}
	}
	want := map[string]string{"besteffort-1": "cpu,memory", "halfway-1": "memory"}
	if len(listed) != len(want) {
		t.Fatalf("listed = %v, want %v", listed, want)
	}
	for name, dims := range want {
		if listed[name] != dims {
			t.Errorf("%s missing = %q, want %q", name, listed[name], dims)
		}
	}
	// --show-unrequested must not also switch on the limits listing.
	if got := kinds["top.unlimited_container"]; got != nil {
		t.Errorf("--show-unrequested leaked the limits listing: %v", got)
	}
}

// TestLimitRangeAnnotates is #235's R2 on this command: a namespace
// LimitRange is named on the finding, never used to drop it. These
// are live pods — LimitRanger writes its defaults into the spec at
// admission and never touches existing pods, so a pod still missing
// the value predates the LimitRange and the finding is TRUE.
func TestLimitRangeAnnotates(t *testing.T) {
	metrics, objs := censusFixture(t)
	// Defaults both dimensions, both halves — the strongest case for
	// suppression, which must still not happen.
	objs = append(objs, limitRange("prod", "defaults", "1", "1Gi", "100m", "128Mi"))
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))

	res := checktest.Run(t, cmd, "--namespace=prod", "--show-unlimited", "--show-unrequested")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	kinds := byKind(t, res.Stdout)

	// Counts are unchanged from the no-LimitRange run: annotate, never drop.
	if agg := kinds["top.unrequested"]; len(agg) != 1 || agg[0]["containers"] != "2" {
		t.Fatalf("top.unrequested = %v, want containers=2 (annotate, never drop)", agg)
	} else if agg[0]["limitrange_defaulted"] != "2" {
		t.Errorf("limitrange_defaulted = %q, want 2", agg[0]["limitrange_defaulted"])
	}
	if agg := kinds["top.unlimited"]; len(agg) != 1 || agg[0]["containers"] != "3" {
		t.Fatalf("top.unlimited = %v, want containers=3", agg)
	} else if agg[0]["limitrange_defaulted"] != "3" {
		t.Errorf("limitrange_defaulted = %q, want 3", agg[0]["limitrange_defaulted"])
	}
	for _, r := range kinds["top.unrequested_container"] {
		if r["limitrange"] != "defaults" {
			t.Errorf("%s limitrange = %q, want the LimitRange named", r["name"], r["limitrange"])
		}
	}
	for _, r := range kinds["top.unlimited_container"] {
		if r["limitrange"] != "defaults" {
			t.Errorf("%s limitrange = %q, want the LimitRange named", r["name"], r["limitrange"])
		}
	}
	// The aggregate says what the annotation means.
	if msg := kinds["top.unrequested"][0]["message"]; !strings.Contains(msg, "predate") {
		t.Errorf("aggregate message = %q, want it to explain the LimitRange count", msg)
	}
	// LimitRange objects count toward scanned= (4 containers + 1 LimitRange).
	if s := summaryLine(t, res.Stdout)["scanned"]; s != "5" {
		t.Errorf("scanned = %s, want 5 (4 containers + 1 LimitRange)", s)
	}
}

// TestLimitRangeScopedToItsDimensions pins the two ways a LimitRange
// fails to cover a finding: the wrong dimension, and the wrong
// item type (only `type: Container` defaults anything).
func TestLimitRangeScopedToItsDimensions(t *testing.T) {
	metrics, objs := censusFixture(t)
	// Defaults cpu only. halfway-1 is missing memory alone, so it
	// must NOT be annotated; besteffort-1 is missing both and must.
	objs = append(objs, limitRange("prod", "cpu-only", "1", "", "100m", ""))
	objs = append(objs, &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "pod-scoped"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypePod,
			Max:  corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
		}}},
	})
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	res := checktest.Run(t, cmd, "--namespace=prod", "--show-unrequested")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	got := map[string]string{}
	for _, r := range byKind(t, res.Stdout)["top.unrequested_container"] {
		got[r["name"]] = r["limitrange"]
	}
	if got["besteffort-1"] != "cpu-only" {
		t.Errorf("besteffort-1 (missing cpu+memory) limitrange = %q, want cpu-only", got["besteffort-1"])
	}
	if got["halfway-1"] != "" {
		t.Errorf("halfway-1 (missing memory only) annotated by a cpu-only LimitRange: %q", got["halfway-1"])
	}
}

func TestNodeViewWithAllNamespaces(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	nodes := map[string]map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "top.node" {
			nodes[r["name"]+"/"+r["resource"]] = r
		}
	}
	cpu := nodes["node-1/cpu"]
	if cpu == nil || cpu["severity"] != emit.SeverityWarning || cpu["reason"] != "NodeCPUPressure" || cpu["pct"] != "95" {
		t.Errorf("node-1 cpu = %v, want warning NodeCPUPressure pct=95", cpu)
	}
	mem := nodes["node-1/memory"]
	if mem == nil || mem["severity"] != emit.SeverityWarning || mem["reason"] != "NodeMemoryPressure" || mem["pct"] != "87.5" {
		t.Errorf("node-1 memory = %v, want warning NodeMemoryPressure pct=87.5", mem)
	}
	if len(nodes) != 2 {
		t.Errorf("node findings = %v, want exactly node-1's two dimensions (node-2 is nominal)", nodes)
	}
	// scanned = 4 containers + 2 nodes.
	if s := summaryLine(t, res.Stdout)["scanned"]; s != "6" {
		t.Errorf("scanned = %s, want 6 (4 containers + 2 nodes)", s)
	}
}

func TestWorkloadScoping(t *testing.T) {
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-rs", UID: "ReplicaSet-api-rs", OwnerReferences: ownedBy("Deployment", "api")},
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api", UID: "Deployment-api"},
	}
	metrics := newMetricsFake(t,
		podMetrics("prod", "api-rs-1", "app", "490m", "100Mi"), // in scope: cpu 98%
		podMetrics("prod", "other-1", "app", "490m", "500Mi"),  // other workload — filtered
	)
	cmd := top.New(testDeps(metrics, cloud.NoProvider,
		deploy, rs,
		pod("prod", "api-rs-1", "n1", "app", "500m", "512Mi", ownedBy("ReplicaSet", "api-rs")...),
		pod("prod", "other-1", "n1", "app", "500m", "512Mi"),
	))
	res := checktest.Run(t, cmd, "--workload=Deployment/prod/api")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var sat []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "top.saturation" {
			sat = append(sat, r)
		}
	}
	if len(sat) != 1 || sat[0]["name"] != "api-rs-1" || sat[0]["resource"] != "cpu" {
		t.Errorf("workload-scoped findings = %v, want exactly api-rs-1 cpu", sat)
	}

	// Unknown workload is a runtime error, not silence.
	res = checktest.Run(t, cmd, "--workload=Deployment/prod/ghost")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, "not found") {
		t.Errorf("ghost workload: exit %d stderr %q, want runtime not-found", res.Code, res.Stderr)
	}
}

func TestAllDumpSortedAndCapped(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	res := checktest.Run(t, cmd, "--namespace=prod", "--all")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var sat []map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "top.saturation" {
			sat = append(sat, r)
		}
	}
	// 3 limited containers × 2 resources.
	if len(sat) != 6 {
		t.Fatalf("--all rows = %d, want 6", len(sat))
	}
	if sat[len(sat)-1]["severity"] != emit.SeverityInfo {
		t.Errorf("last --all row severity = %s, want info below threshold", sat[len(sat)-1]["severity"])
	}
	if sat[len(sat)-1]["reason"] != "" || sat[len(sat)-1]["message"] != "" {
		t.Errorf("below-threshold row carries reason/message: %v (zero nominal state)", sat[len(sat)-1])
	}
	prev := 1000.0
	for _, r := range sat {
		p, err := strconv.ParseFloat(r["pct"], 64)
		if err != nil {
			t.Fatalf("pct %q: %v", r["pct"], err)
		}
		if p > prev {
			t.Fatalf("--all not sorted descending: %v", sat)
		}
		prev = p
	}

	res = checktest.Run(t, cmd, "--namespace=prod", "--all", "--limit=2")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	count := 0
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "top.saturation" {
			count++
		}
	}
	if count != 2 {
		t.Errorf("--limit=2 rows = %d, want 2", count)
	}
}

// TestHistoryUnavailable is the §2 path: no provider → an explicit
// cloud.unavailable finding, the summary marker, and the
// point-in-time findings still present.
func TestHistoryUnavailable(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	res := checktest.Run(t, cmd, "--namespace=prod", "--history=1h")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	recs := findingLines(t, res.Stdout)
	if recs[0]["kind"] != "cloud.unavailable" ||
		recs[0]["capability"] != "metrics" ||
		recs[0]["provider"] != cloud.NoProviderName ||
		!strings.Contains(recs[0]["message"], cloud.NoProviderReason) {
		t.Errorf("first finding = %v, want the explicit cloud.unavailable record", recs[0])
	}
	sat := 0
	for _, r := range recs {
		if r["kind"] == "top.saturation" {
			sat++
			if r["max_pct"] != "" {
				t.Errorf("history stats attached without a backend: %v", r)
			}
		}
	}
	if sat == 0 {
		t.Error("point-in-time findings missing on the unavailable path")
	}
	sum := summaryLine(t, res.Stdout)
	if sum["unavailable"] != cloud.NoProviderReason {
		t.Errorf("summary = %v, want the unavailable=%q marker", sum, cloud.NoProviderReason)
	}
	if sum["history"] != "" {
		t.Errorf("summary carries history= despite no backend: %v", sum)
	}
}

// fakeBackend serves canned points and records the queries.
type fakeBackend struct {
	queries []cloud.SeriesQuery
	points  map[string][]float64 // metric → values
}

func (b *fakeBackend) QuerySeries(_ context.Context, q cloud.SeriesQuery) ([]cloud.Series, error) {
	b.queries = append(b.queries, q)
	vals := b.points[q.Metric]
	if len(vals) == 0 {
		return nil, nil
	}
	s := cloud.Series{Metric: q.Metric, Labels: q.Matchers}
	for i, v := range vals {
		s.Points = append(s.Points, cloud.Point{Time: q.Window.Start.Add(time.Duration(i) * time.Minute), Value: v})
	}
	return []cloud.Series{s}, nil
}

// metricsProvider is NoProvider plus a working Metrics capability.
type metricsProvider struct {
	cloud.Provider
	backend cloud.MetricsBackend
}

func (p metricsProvider) Metrics() (cloud.MetricsBackend, bool) { return p.backend, true }

func TestHistoryEnrichment(t *testing.T) {
	metrics := newMetricsFake(t, podMetrics("prod", "api-2", "app", "480m", "490Mi"))
	backend := &fakeBackend{points: map[string][]float64{
		// memory bytes: 256Mi..512Mi ramp (limit 512Mi).
		top.HistoryMetricMemory: {256 << 20, 384 << 20, 512 << 20},
		// cpu millicores flat 250m of a 500m limit.
		top.HistoryMetricCPU: {250, 250, 250},
	}}
	provider := metricsProvider{Provider: cloud.NoProvider, backend: backend}
	cmd := top.New(testDeps(metrics, provider, pod("prod", "api-2", "n1", "app", "500m", "512Mi")))
	res := checktest.Run(t, cmd, "--namespace=prod", "--history=1h")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	byResource := map[string]map[string]string{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "top.saturation" {
			byResource[r["resource"]] = r
		}
	}
	mem := byResource["memory"]
	if mem == nil || mem["max_pct"] != "100" || mem["avg_pct"] != "75" || mem["p95_pct"] != "100" {
		t.Errorf("memory history stats = %v, want max=100 avg=75 p95=100", mem)
	}
	cpu := byResource["cpu"]
	if cpu == nil || cpu["max_pct"] != "50" || cpu["avg_pct"] != "50" || cpu["p95_pct"] != "50" {
		t.Errorf("cpu history stats = %v, want flat 50", cpu)
	}
	sum := summaryLine(t, res.Stdout)
	if sum["history"] != "1h0m0s" {
		t.Errorf("summary = %v, want history=1h0m0s", sum)
	}
	// The window is anchored on the injected clock and matchers name
	// the container.
	if len(backend.queries) != 2 {
		t.Fatalf("queries = %d, want one per finding", len(backend.queries))
	}
	q := backend.queries[0]
	if !q.Window.End.Equal(fixedNow) || !q.Window.Start.Equal(fixedNow.Add(-time.Hour)) {
		t.Errorf("window = %+v, want [now-1h, now)", q.Window)
	}
	if q.Matchers["namespace"] != "prod" || q.Matchers["pod"] != "api-2" || q.Matchers["container"] != "app" {
		t.Errorf("matchers = %v", q.Matchers)
	}
}

func TestUsageErrors(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no scope", nil, "no scope"},
		{"A with workload", []string{"-A", "--workload=Deployment/prod/api"}, "does not combine"},
		{"namespace contradicts workload", []string{"--namespace=dev", "--workload=Deployment/prod/api"}, "contradicts"},
		{"bad kind", []string{"--workload=Gateway/prod/api"}, "unsupported workload kind"},
		{"warn too high", []string{"-A", "--top-warn=101"}, "--top-warn"},
		{"warn zero", []string{"-A", "--top-warn=0"}, "--top-warn"},
		{"limit zero", []string{"-A", "--limit=0"}, "--limit"},
		{"negative history", []string{"-A", "--history=-5m"}, "--history"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, cmd, tc.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d (stderr %q)", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr %q, want it to mention %q", res.Stderr, tc.want)
			}
			if res.Stdout != "" {
				t.Errorf("usage error leaked stdout: %q", res.Stdout)
			}
		})
	}
}

func TestContract(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	checktest.VerifyContract(t, cmd, "-A")
	checktest.VerifyContract(t, cmd, "--namespace=prod", "--all", "--show-unlimited", "--show-unrequested")
	// The census pair with LimitRange annotation attached — the
	// limitrange/limitrange_defaulted keys must be in the glossary.
	cmetrics, cobjs := censusFixture(t)
	cobjs = append(cobjs, limitRange("prod", "defaults", "1", "1Gi", "100m", "128Mi"))
	census := top.New(testDeps(cmetrics, cloud.NoProvider, cobjs...))
	checktest.VerifyContract(t, census, "--namespace=prod", "--show-unlimited", "--show-unrequested")
	checktest.VerifyContract(t, cmd, "-A", "--history=30m") // unavailable marker path
	backend := &fakeBackend{points: map[string][]float64{top.HistoryMetricCPU: {100}, top.HistoryMetricMemory: {100 << 20}}}
	enriched := top.New(testDeps(metrics, metricsProvider{Provider: cloud.NoProvider, backend: backend}, objs...))
	checktest.VerifyContract(t, enriched, "-A", "--history=30m")
}

func TestRegistered(t *testing.T) {
	c, ok := checks.Lookup("triage top")
	if !ok {
		t.Fatal("triage top is not in the default registry")
	}
	if c.MCPName != "k8s_resource_top" {
		t.Errorf("MCP name = %q, want k8s_resource_top", c.MCPName)
	}
}

func TestGolden(t *testing.T) {
	metrics, objs := mixedFixture(t)
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	res := checktest.Run(t, cmd, "-A", "--show-unlimited", "--show-unrequested", "--history=1h")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checkGolden(t, "top-cluster.golden", res.Stdout)
}

// TestGoldenCensus pins the #235 pair on the fixture that separates
// them: both censuses, the burstable container in exactly one of
// them, and the LimitRange annotation on every row it covers.
func TestGoldenCensus(t *testing.T) {
	metrics, objs := censusFixture(t)
	objs = append(objs, limitRange("prod", "defaults", "1", "1Gi", "100m", "128Mi"))
	cmd := top.New(testDeps(metrics, cloud.NoProvider, objs...))
	res := checktest.Run(t, cmd, "--namespace=prod", "--show-unlimited", "--show-unrequested")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checkGolden(t, "top-census.golden", res.Stdout)
}

func checkGolden(t *testing.T, name, got string) {
	t.Helper()
	golden := filepath.Join("testdata", name)
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_GOLDEN=1 to create)", err)
	}
	if got != string(want) {
		t.Errorf("golden mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}
