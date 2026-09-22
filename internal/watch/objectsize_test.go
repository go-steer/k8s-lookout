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

// Spike S8's measurement, kept runnable rather than written down once.
//
// The design doc's §6.6 tiers and §12.1's kwok padding both rest on the
// serialised size of a real Pod and a real Node, before and after §6.1's shared
// transform. Those are properties of a cluster, not of this repo, so the number
// goes stale: a fleet that adopts sidecars, or an admission controller that
// starts writing annotations, moves the p50 without anything here changing.
//
// This is deliberately a skipped test rather than a dev/tools binary: it calls
// the real kube.Transform, because measuring a reimplementation of the
// transform would measure the reimplementation, and a binary that links
// client-go for one measurement is a maintenance cost with no other reader.
//
//	LOOKOUT_MEASURE_CONTEXT=<kubecontext> go test ./internal/watch -run ObjectSizes -v
//
// It never asserts. A spike that fails the build when somebody's cluster is
// unusual is a spike nobody runs.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// measureContextEnv names the kubecontext to sample. Unset, the test skips:
// this is the whole gate, since everything below talks to a real API server.
const measureContextEnv = "LOOKOUT_MEASURE_CONTEXT"

func TestObjectSizes_OnALiveCluster(t *testing.T) {
	kubeContext := os.Getenv(measureContextEnv)
	if kubeContext == "" {
		t.Skipf("set %s=<kubecontext> to run spike S8's measurement", measureContextEnv)
	}

	client, err := kube.BuildClient(kube.Options{Context: kubeContext})
	if err != nil {
		t.Fatalf("build client for %q: %v", kubeContext, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}

	t.Logf("cluster %s: %d pods, %d nodes", kubeContext, len(pods.Items), len(nodes.Items))

	// Kept because kube.Transform mutates in place: without a copy taken now,
	// there is nothing left to measure the untransformed heap cost against, and
	// the transform's saving — the whole quantitative case for §6.1 — could only
	// be stated on the wire, where it is a different and smaller number.
	rawPods := pods.DeepCopy().Items
	rawNodes := nodes.DeepCopy().Items

	podBefore, podAfter := make([]int, 0, len(pods.Items)), make([]int, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		podBefore = append(podBefore, sizeOf(t, p))
		// The transform mutates in place and returns the same pointer, so the
		// before size has to be taken first. Measuring a deep copy instead
		// would measure the copy's encoding, which is the same — but this
		// ordering is one less thing to be wrong about.
		trimmed, err := kube.Transform(p)
		if err != nil {
			t.Fatalf("transform pod %s/%s: %v", p.Namespace, p.Name, err)
		}
		podAfter = append(podAfter, sizeOf(t, trimmed))
	}

	nodeBefore, nodeAfter := make([]int, 0, len(nodes.Items)), make([]int, 0, len(nodes.Items))
	images := make([]int, 0, len(nodes.Items))
	for i := range nodes.Items {
		n := &nodes.Items[i]
		images = append(images, len(n.Status.Images))
		nodeBefore = append(nodeBefore, sizeOf(t, n))
		trimmed, err := kube.Transform(n)
		if err != nil {
			t.Fatalf("transform node %s: %v", n.Name, err)
		}
		nodeAfter = append(nodeAfter, sizeOf(t, trimmed))
	}

	t.Log("serialised bytes (JSON — the typed client's default content type, so this is the wire size the decoder pays for):")
	report(t, "Pod  before transform", podBefore)
	report(t, "Pod  after  transform", podAfter)
	report(t, "Node before transform", nodeBefore)
	report(t, "Node after  transform", nodeAfter)
	t.Log("node status.images length:")
	report(t, "Node status.images[]", images)

	// The single number §6.1 is judged on. A transform that pays for itself on
	// the p50 but not the p95 is a different design conversation than one that
	// pays everywhere, so both are printed rather than just the mean.
	t.Logf("transform saves: pod p50 %s, pod p95 %s, node p50 %s, node p95 %s",
		pct(podBefore, podAfter, 50), pct(podBefore, podAfter, 95),
		pct(nodeBefore, nodeAfter, 50), pct(nodeBefore, nodeAfter, 95))

	// Serialised size is the wire cost. §6.6's pod-cache line is a HEAP cost,
	// and the two are not the same number in either direction: JSON spells out
	// every key name, while a decoded Pod pays for pointers, map headers,
	// slice capacity and string headers the wire does not carry. The estimate
	// §6.1 invalidated was a heap estimate, so it has to be answered with one.
	t.Log("retained heap per cached object (measured by holding copies, not derived from the wire size):")
	podHeapBefore, podHeapAfter := heapPer(t, rawPods), heapPer(t, pods.Items)
	nodeHeapBefore, nodeHeapAfter := heapPer(t, rawNodes), heapPer(t, nodes.Items)
	t.Logf("  Pod  before transform: %d B", podHeapBefore)
	t.Logf("  Pod  after  transform: %d B  (%.1f%% saved)", podHeapAfter, saved(podHeapBefore, podHeapAfter))
	t.Logf("  Node before transform: %d B", nodeHeapBefore)
	t.Logf("  Node after  transform: %d B  (%.1f%% saved)", nodeHeapAfter, saved(nodeHeapBefore, nodeHeapAfter))
}

// heapPer is the retained heap cost of one cached object, measured by holding
// enough copies that the difference is far above allocator noise.
//
// The objects handed in have already been through kube.Transform in place, so
// this measures what the informer cache actually holds. DeepCopy is what the
// cache effectively does on each delta, and — more to the point here — it is
// what stops every copy sharing one backing array and reading as free.
func heapPer[T any, PT interface {
	*T
	DeepCopy() PT
	GetName() string
}](t *testing.T, items []T) int {
	t.Helper()
	if len(items) == 0 {
		return 0
	}
	const copies = 5000

	// Twice, both times. One GC cycle leaves spans the next one reclaims, and
	// a single collection before the baseline reads high by megabytes — enough,
	// at this object count, to turn the answer negative.
	settle := func(ms *runtime.MemStats) {
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(ms)
	}

	var before, after runtime.MemStats
	settle(&before)

	held := make([]PT, 0, copies)
	for i := range copies {
		held = append(held, PT(&items[i%len(items)]).DeepCopy())
	}

	settle(&after)

	// The copies have to be REACHABLE at the GC inside settle, and len(held) is
	// not enough to make them so: a slice's length lives in its header, so the
	// compiler can prove the backing array — and with it all 5000 objects — dead
	// before the collection runs. It does, and the answer comes out negative.
	// Reading a field out of every element after the measurement is what keeps
	// them alive through it.
	touched := 0
	for _, h := range held {
		touched += len(h.GetName())
	}
	if touched == 0 {
		t.Fatal("held nothing")
	}
	return int(after.HeapAlloc-before.HeapAlloc) / copies
}

// sizeOf is the serialised size in bytes. JSON because that is what the typed
// client negotiates by default — pkg/kube sets no ContentType — so it is both
// the bytes on the wire and the bytes the decoder walks.
func sizeOf(t *testing.T, obj any) int {
	t.Helper()
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return len(b)
}

// report prints the distribution. n is printed alongside because a p99 over
// seventy objects is the maximum wearing a percentile's name, and whoever reads
// this in six months needs to know which they are looking at.
func report(t *testing.T, label string, xs []int) {
	t.Helper()
	if len(xs) == 0 {
		t.Logf("  %s: no objects", label)
		return
	}
	s := append([]int(nil), xs...)
	sort.Ints(s)
	total := 0
	for _, x := range s {
		total += x
	}
	t.Logf("  %s: n=%d min=%d p50=%d p95=%d p99=%d max=%d mean=%d",
		label, len(s), s[0], percentile(s, 50), percentile(s, 95), percentile(s, 99),
		s[len(s)-1], total/len(s))
}

// saved is the percentage a is smaller than b.
func saved(before, after int) float64 {
	if before == 0 {
		return 0
	}
	return 100 * float64(before-after) / float64(before)
}

// percentile is nearest-rank over an already-sorted slice.
func percentile(sorted []int, p int) int {
	if len(sorted) == 0 {
		return 0
	}
	i := (p*len(sorted) + 99) / 100
	if i < 1 {
		i = 1
	}
	if i > len(sorted) {
		i = len(sorted)
	}
	return sorted[i-1]
}

// pct is the transform's saving at one percentile, as a percentage. Both slices
// are sorted independently, which is the right comparison: the question is what
// a p95-sized object costs before and after, not what one particular object did.
func pct(before, after []int, p int) string {
	b := append([]int(nil), before...)
	a := append([]int(nil), after...)
	sort.Ints(b)
	sort.Ints(a)
	bv, av := percentile(b, p), percentile(a, p)
	if bv == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d→%d (%.1f%%)", bv, av, 100*float64(bv-av)/float64(bv))
}
