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

// §7.6 enrichment tests (§13 conventions): fake.Clientset fixture
// clusters, a fixture PodLogGetter for log streams, the fake daemon
// capturing exact inject bytes, and metric assertions via testutil.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

var enrichNow = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

// enrichFakeLogs is the fixture PodLogGetter (§13: the fake clientset
// cannot serve log streams), content per "namespace/pod/container".
type enrichFakeLogs struct {
	streams map[string]string
	// block, when non-nil, makes every Stream call wait for ctx
	// cancellation — the fake slow stage for the timeout tests.
	block bool
}

func (f *enrichFakeLogs) Stream(ctx context.Context, ns, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	key := ns + "/" + pod + "/" + opts.Container
	s, ok := f.streams[key]
	if !ok {
		return nil, fmt.Errorf("no log fixture for %s", key)
	}
	return io.NopCloser(strings.NewReader(s)), nil
}

// --- the broken-workload fixture: Deployment prod/api → ReplicaSet →
// crash-looping pod on node-1, a Service selecting it, and an env
// reference to a ConfigMap that does not exist (edges section bait).

const (
	enrichNS   = "prod"
	enrichHash = "7c9d8"
)

func enrichDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: enrichNS, Name: "api", Labels: map[string]string{"app": "api"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrTo(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "api", Image: "registry.example.com/api:v2",
				}}},
			},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 0, UpdatedReplicas: 1, AvailableReplicas: 0},
	}
}

func enrichReplicaSet() *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: enrichNS, Name: "api-" + enrichHash,
			Labels:          map[string]string{"app": "api"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api"}},
		},
	}
}

// enrichPod crash-loops with a planted literal credential env var:
// the sanitizer tests assert the value never reaches the bundle.
func enrichPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: enrichNS, Name: "api-" + enrichHash + "-x2v9k",
			Labels:          map[string]string{"app": "api", "pod-template-hash": enrichHash},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "api-" + enrichHash}},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name:  "api",
				Image: "registry.example.com/api:v2",
				Env: []corev1.EnvVar{
					{Name: "DB_PASSWORD", Value: plantedSecret},
					{Name: "LOG_LEVEL", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "app-config"}, Key: "log.level"}}},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: ptrTo(metav1.Time{Time: enrichNow.Add(-time.Hour)}),
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "api", Ready: false, RestartCount: 7,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: "back-off 5m0s restarting failed container",
				}},
			}},
		},
	}
}

// plantedSecret is credential-shaped on purpose (multi-class,
// high-entropy) so both the env-name rule and the value-shape rule
// would each catch it. It must NEVER appear in any bundle.
const plantedSecret = "s3cr3tHunter2Value9XyZQ81"

func enrichNode() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
}

func enrichService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: enrichNS, Name: "api"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "api"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80}},
		},
	}
}

func enrichFixtureObjects() []runtime.Object {
	return []runtime.Object{
		enrichDeployment(), enrichReplicaSet(), enrichPod(), enrichNode(), enrichService(),
	}
}

// plantedJWT is credential material planted in the log fixture; the
// Writer's §6.5 sanitizer must mask it in the logs section.
const plantedJWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.c2lnbmF0dXJlLXNlZ21lbnQ"

func enrichLogFixture() *enrichFakeLogs {
	pod := enrichNS + "/api-" + enrichHash + "-x2v9k/api"
	return &enrichFakeLogs{streams: map[string]string{
		pod: "2026-07-24T11:59:01Z ERROR db connect failed err=timeout\n" +
			"2026-07-24T11:59:02Z ERROR db connect failed err=timeout\n" +
			"2026-07-24T11:59:03Z ERROR auth header was Bearer " + plantedJWT + "\n",
	}}
}

func testEnricher(m *metrics, cs *fake.Clientset, lg *enrichFakeLogs) *enricher {
	return &enricher{
		client:    cs,
		logGetter: lg,
		now:       func() time.Time { return enrichNow },
		metrics:   m,
		policy:    "critical",
		cap:       16384,
		logLines:  200,
		timeout:   5 * time.Second,
	}
}

func crashSignal() engine.Signal {
	return engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:           engine.EventKey{UID: "uid-api-1", Reason: "CrashLoopBackOff"},
			Namespace:     enrichNS,
			KindOfObject:  "Pod",
			Name:          "api-" + enrichHash + "-x2v9k",
			Message:       "Back-off restarting failed container",
			FirstSeen:     time.Date(2026, 7, 24, 11, 55, 0, 0, time.UTC),
			LastSeen:      time.Date(2026, 7, 24, 11, 58, 0, 0, time.UTC),
			ControllerRef: "ReplicaSet/api-" + enrichHash,
			Node:          "node-1",
			Count:         3,
		},
	}
}

func newEnrichedDispatcher(t *testing.T, base string, e *enricher) *dispatcher {
	t.Helper()
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	m := newMetrics()
	if e != nil {
		e.metrics = m
	}
	return &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  m,
		cluster:  "prod-us-central1",
		mode:     "per-incident",
		enrich:   e,
	}
}

// payloadOf unwraps the inject envelope into the generic payload map.
func payloadOf(t *testing.T, body string) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal([]byte(messageOf(t, body)), &p); err != nil {
		t.Fatalf("inject message is not JSON: %v", err)
	}
	return p
}

func bundleOf(t *testing.T, body string) string {
	t.Helper()
	p := payloadOf(t, body)
	enr, ok := p["enrichment"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no enrichment object: %v", p)
	}
	b, ok := enr["bundle"].(string)
	if !ok {
		t.Fatalf("enrichment has no bundle string: %v", enr)
	}
	return b
}

// --- flag surface ---

func TestEnrichFlags_DefaultsAndValidation(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.enrich != "critical" {
		t.Errorf("default --enrich = %q, want critical (§7.6: always for critical)", f.enrich)
	}
	if f.enrichCap != 16384 {
		t.Errorf("default --enrich-cap = %d, want 16384 (§15 Q3 fixed budget)", f.enrichCap)
	}
	if f.enrichLogLines != 200 {
		t.Errorf("default --enrich-log-lines = %d, want 200", f.enrichLogLines)
	}
	if f.enrichTimeout != 5*time.Second {
		t.Errorf("default --enrich-timeout = %v, want 5s", f.enrichTimeout)
	}

	bad := [][]string{
		{"--dry-run", "--enrich=page"},
		{"--dry-run", "--enrich-cap=0"},
		{"--dry-run", "--enrich-log-lines=0"},
		{"--dry-run", "--enrich-timeout=0s"},
		{"--dry-run", "--enrich-timeout=-1s"},
	}
	for _, args := range bad {
		f, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", args, err)
		}
		if err := f.validate(); err == nil {
			t.Errorf("validate(%v): expected an error", args)
		}
	}
	for _, val := range []string{"critical", "warning", "off"} {
		f, err := parseFlags([]string{"--dry-run", "--enrich=" + val})
		if err != nil {
			t.Fatalf("parseFlags(--enrich=%s): %v", val, err)
		}
		if err := f.validate(); err != nil {
			t.Errorf("validate(--enrich=%s): %v", val, err)
		}
	}
}

func TestEnricherEnabledFor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		policy string
		sev    engine.Severity
		want   bool
	}{
		{"critical", engine.SeverityCritical, true},
		{"critical", engine.SeverityWarning, false},
		{"critical", engine.SeverityInfo, false},
		{"warning", engine.SeverityCritical, true},
		{"warning", engine.SeverityWarning, true},
		{"warning", engine.SeverityInfo, false},
	}
	for _, c := range cases {
		e := &enricher{policy: c.policy}
		if got := e.enabledFor(c.sev); got != c.want {
			t.Errorf("policy=%s severity=%s: enabledFor=%v, want %v", c.policy, c.sev, got, c.want)
		}
	}
}

// --- end-to-end, scoped fallback path ---

// TestEnrichDispatch_ScopedEndToEnd is the §7.6 drill: a crash-looping
// pod signal → per-incident session whose INITIAL inject carries the
// in-process bundle — spec, delta, edges, radius, and logs sections —
// while the frozen k8s-event fields stay untouched.
func TestEnrichDispatch_ScopedEndToEnd(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(nil, cs, enrichLogFixture())
	d := newEnrichedDispatcher(t, base, e)

	d.DispatchSignal(context.Background(), crashSignal())

	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	p := payloadOf(t, (*injects)[0].Body)
	// Frozen fields untouched.
	if p["kind"] != "k8s-event" || p["reason"] != "CrashLoopBackOff" || p["uid"] != "uid-api-1" {
		t.Errorf("frozen fields drifted: %v", p)
	}
	b := bundleOf(t, (*injects)[0].Body)

	// The head resolves the pod up the owner chain to the Deployment
	// and lists every section that shipped.
	head, _, _ := strings.Cut(b, "\n")
	for _, want := range []string{"kind=bundle.target", "workload=Deployment/prod/api", "pods=1", "sections=spec,delta,edges,radius,logs"} {
		if !strings.Contains(head, want) {
			t.Errorf("head line missing %q:\n%s", want, head)
		}
	}
	// One line each proving every section carries real content.
	for _, want := range []string{
		"section=spec",
		"kind=pod.crashloop", // the delta derivation saw the crash loop
		"section=delta",
		"reason=CrashLoopBackOff",
		"section=edges", // edge.missing_ref: env references absent ConfigMap app-config
		"kind=edge.missing_ref",
		"section=radius", // Service + Node neighbors from the same graph
		"kind=radius.neighbor",
		"kind_of_object=Service",
		"section=logs", // distilled templates
		`template="ERROR db connect failed err=timeout" count=2`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("bundle missing %q; bundle:\n%s", want, b)
		}
	}
	// No overflow / error trailers at the default cap.
	if strings.Contains(b, "overflow ") || strings.Contains(b, "enrichment_error ") {
		t.Errorf("unexpected trailers at 16KiB cap:\n%s", b)
	}
	if got := testutil.ToFloat64(d.metrics.enrichments.WithLabelValues("ok")); got != 1 {
		t.Errorf("enrichments_total{outcome=ok} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(d.metrics.enrichmentTruncated); got != 0 {
		t.Errorf("enrichment_truncated_total = %v, want 0", got)
	}
}

// TestEnrichDispatch_SeverityGate: a warning-class signal with the
// default critical-only policy gets NO enrichment key at all — the
// wire bytes stay in the frozen M0 shape.
func TestEnrichDispatch_SeverityGate(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(nil, cs, enrichLogFixture())
	d := newEnrichedDispatcher(t, base, e)

	sig := crashSignal()
	sig.Severity = engine.SeverityWarning
	d.DispatchSignal(context.Background(), sig)

	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	if msg := messageOf(t, (*injects)[0].Body); strings.Contains(msg, "enrichment") {
		t.Errorf("warning-severity inject must not carry enrichment (policy=critical):\n%s", msg)
	}
	if got := testutil.ToFloat64(d.metrics.enrichments.WithLabelValues("ok")); got != 0 {
		t.Errorf("enrichments_total{outcome=ok} = %v, want 0", got)
	}

	// Same signal, policy=warning: enriched.
	base2, injects2 := newRoutingFakeDaemon(t)
	e2 := testEnricher(nil, fake.NewClientset(enrichFixtureObjects()...), enrichLogFixture())
	e2.policy = "warning"
	d2 := newEnrichedDispatcher(t, base2, e2)
	d2.DispatchSignal(context.Background(), sig)
	if len(*injects2) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects2))
	}
	if b := bundleOf(t, (*injects2)[0].Body); !strings.Contains(b, "kind=bundle.target") {
		t.Errorf("policy=warning did not enrich a warning signal:\n%s", b)
	}
}

// --- sanitizer (§6.5) on the enrichment surface ---

func TestEnrich_SanitizerMasksPlantedSecrets(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(newMetrics(), cs, enrichLogFixture())

	b := e.Incident(context.Background(), crashSignal())

	if strings.Contains(b, plantedSecret) {
		t.Fatalf("planted env credential leaked into the enrichment bundle:\n%s", b)
	}
	if strings.Contains(b, plantedJWT) {
		t.Fatalf("planted JWT leaked through the logs section:\n%s", b)
	}
	if !strings.Contains(b, emit.Redacted) {
		t.Errorf("expected %s markers where the planted secrets were:\n%s", emit.Redacted, b)
	}
}

// --- cap + overflow trailers ---

// TestEnrich_TinyCap_OverflowTrailers: under a cap smaller than any
// section, the bundle is head + one overflow trailer per computed
// section, each naming the real follow-up command (§7.6 overflow
// keys; §4.4.4 the inject teaches the next move).
func TestEnrich_TinyCap_OverflowTrailers(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(enrichFixtureObjects()...)
	e := testEnricher(newMetrics(), cs, enrichLogFixture())
	e.cap = 64 // smaller than the head: nothing but trailers can ship

	b := e.Incident(context.Background(), crashSignal())

	lines := strings.Split(strings.TrimSuffix(b, "\n"), "\n")
	if !strings.Contains(lines[0], "kind=bundle.target") {
		t.Fatalf("head must survive any cap (it anchors the trailers):\n%s", b)
	}
	want := []string{
		`overflow section=spec cmd="lookout triage spec --workload=Deployment/prod/api"`,
		`overflow section=delta cmd="lookout triage delta --namespace=prod"`,
		`overflow section=edges cmd="lookout state edges --workload=Deployment/prod/api"`,
		`overflow section=radius cmd="lookout bundle --workload=Deployment/prod/api"`,
		`overflow section=logs cmd="lookout triage logs --workload=Deployment/prod/api --namespace=prod"`,
	}
	if got := lines[1:]; !slices.Equal(got, want) {
		t.Errorf("overflow trailers drifted:\n got: %q\nwant: %q", got, want)
	}
	// The head's sections detail lists only what shipped: nothing.
	if strings.Contains(lines[0], "sections=") {
		t.Errorf("empty bundle must not claim sections:\n%s", lines[0])
	}
	if got := testutil.ToFloat64(e.metrics.enrichmentTruncated); got != 1 {
		t.Errorf("enrichment_truncated_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues("ok")); got != 1 {
		t.Errorf("truncation is not a failure: enrichments_total{outcome=ok} = %v, want 1", got)
	}
}

// TestEnrichBundle_PrefixCutAtSectionBoundary drives the builder with
// synthetic sections of known sizes: the first section that misses
// the budget drops it AND everything after — never a mid-line cut,
// never a smaller later section sneaking in out of order.
func TestEnrichBundle_PrefixCutAtSectionBoundary(t *testing.T) {
	t.Parallel()
	wl := emit.WorkloadRef{Kind: "Deployment", Namespace: "prod", Name: "api"}
	line := func(section string, n int) []byte {
		return []byte(fmt.Sprintf("kind=x section=%s pad=%s\n", section, strings.Repeat("p", n)))
	}
	build := func(cap int) *enrichBundle {
		b := newEnrichBundle(cap)
		b.setTarget(wl, 1)
		b.secs = []enrichSection{
			{name: "spec", body: line("spec", 40), cmd: specCmd(wl)},
			{name: "delta", body: line("delta", 400), cmd: deltaCmd(wl)},
			{name: "edges", body: line("edges", 40), cmd: edgesCmd(wl)},
		}
		b.computed = 3
		return b
	}

	// Budget fits spec but not delta → edges must be dropped too,
	// even though it would fit (prefix cut).
	b := build(len(b0Head(t, wl)) + 100)
	out, truncated := b.render()
	if !truncated {
		t.Fatalf("expected truncation:\n%s", out)
	}
	if !strings.Contains(out, "section=spec") {
		t.Errorf("spec should have shipped:\n%s", out)
	}
	if strings.Contains(out, "section=delta pad") || strings.Contains(out, "section=edges pad") {
		t.Errorf("prefix cut violated — a section after the boundary shipped:\n%s", out)
	}
	for _, want := range []string{"overflow section=delta ", "overflow section=edges "} {
		if !strings.Contains(out, want) {
			t.Errorf("missing trailer %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "sections=spec\n") && !strings.Contains(out, "sections=spec ") {
		t.Errorf("head must list only the shipped sections:\n%s", out)
	}
	// Every emitted line is complete (ends in newline, no mid-line cut).
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("output must end on a line boundary: %q", out[len(out)-20:])
	}

	// A cap that fits everything: no trailers, no truncation.
	b = build(1 << 20)
	out, truncated = b.render()
	if truncated || strings.Contains(out, "overflow ") {
		t.Errorf("nothing should have been dropped:\n%s", out)
	}
	if !strings.Contains(out, "sections=spec,delta,edges") {
		t.Errorf("head should list all sections:\n%s", out)
	}
}

// b0Head renders the head line the builder would emit with one
// section name, sizing the budget for the prefix-cut test.
func b0Head(t *testing.T, wl emit.WorkloadRef) []byte {
	t.Helper()
	b := newEnrichBundle(1)
	b.setTarget(wl, 1)
	return b.renderHead([]string{"spec"})
}

// TestTrailerSchema pins the two trailer grammars byte-exactly:
// schema-stable per §7.6, parsed by playbook skills.
func TestTrailerSchema(t *testing.T) {
	t.Parallel()
	got := trailerLine("overflow",
		emit.Field{Key: "section", Value: "logs"},
		emit.Field{Key: "cmd", Value: "lookout triage logs --workload=Deployment/prod/api --namespace=prod"})
	want := "overflow section=logs cmd=\"lookout triage logs --workload=Deployment/prod/api --namespace=prod\"\n"
	if got != want {
		t.Errorf("overflow trailer drifted:\n got: %q\nwant: %q", got, want)
	}
	got = trailerLine("enrichment_error",
		emit.Field{Key: "stage", Value: "resolve"},
		emit.Field{Key: "error", Value: "listing pods: boom"})
	want = "enrichment_error stage=resolve error=\"listing pods: boom\"\n"
	if got != want {
		t.Errorf("enrichment_error trailer drifted:\n got: %q\nwant: %q", got, want)
	}
}

// --- failure honesty ---

// TestEnrichDispatch_LoadFailure_InjectStillFires: the scoped List
// pass fails outright (RBAC, apiserver down) → the inject fires
// anyway, carrying an enrichment_error trailer instead of a bundle.
func TestEnrichDispatch_LoadFailure_InjectStillFires(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	cs := fake.NewClientset()
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("pods is forbidden: no RBAC for you")
	})
	e := testEnricher(nil, cs, enrichLogFixture())
	d := newEnrichedDispatcher(t, base, e)

	d.DispatchSignal(context.Background(), crashSignal())

	if len(*injects) != 1 {
		t.Fatalf("enrichment failure must never block the inject; got %d injects", len(*injects))
	}
	b := bundleOf(t, (*injects)[0].Body)
	if !strings.Contains(b, `enrichment_error stage=resolve error=`) || !strings.Contains(b, "forbidden") {
		t.Errorf("expected a schema-stable resolve-stage error trailer:\n%s", b)
	}
	if got := testutil.ToFloat64(d.metrics.enrichments.WithLabelValues("failed")); got != 1 {
		t.Errorf("enrichments_total{outcome=failed} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(d.metrics.enrichmentFailures.WithLabelValues("resolve")); got != 1 {
		t.Errorf("enrichment_failures_total{stage=resolve} = %v, want 1", got)
	}
}

// TestEnrich_SlowLogsStage_TimeoutPartialBundle: the logs fetch hangs
// → the --enrich-timeout context expires it, the other sections still
// attach, and the trailer names the failed stage. Also the timing
// budget check: the run returns promptly after the deadline instead
// of hanging for the fetch.
func TestEnrich_SlowLogsStage_TimeoutPartialBundle(t *testing.T) {
	t.Parallel()
	// Two pods so the Distill loop hits the ctx check after the first
	// blocked fetch and surfaces the deadline as a stage error.
	pod2 := enrichPod()
	pod2.Name = "api-" + enrichHash + "-y7q2m"
	objs := append(enrichFixtureObjects(), pod2)
	cs := fake.NewClientset(objs...)
	e := testEnricher(newMetrics(), cs, &enrichFakeLogs{block: true})
	e.timeout = 250 * time.Millisecond

	start := time.Now()
	b := e.Incident(context.Background(), crashSignal())
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Fatalf("enrichment ran %v — the %v budget was not enforced", elapsed, e.timeout)
	}
	for _, want := range []string{"section=spec", "section=delta", "section=edges", "section=radius"} {
		if !strings.Contains(b, want) {
			t.Errorf("partial bundle missing %q:\n%s", want, b)
		}
	}
	if strings.Contains(b, "section=logs") {
		t.Errorf("logs section must not ship after its stage failed:\n%s", b)
	}
	if !strings.Contains(b, "enrichment_error stage=logs error=") {
		t.Errorf("expected the logs stage error trailer:\n%s", b)
	}
	if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues("partial")); got != 1 {
		t.Errorf("enrichments_total{outcome=partial} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(e.metrics.enrichmentFailures.WithLabelValues("logs")); got != 1 {
		t.Errorf("enrichment_failures_total{stage=logs} = %v, want 1", got)
	}
}

// --- live-graph path ---

// liveSnapshotOf builds a one-shot topology graph from fixture
// objects and returns it as the enricher's live-snapshot seam.
func liveSnapshotOf(t *testing.T, objs ...any) func() (*graph.Snapshot, error) {
	t.Helper()
	g := graph.New(graph.Options{SwapInterval: -1})
	if err := g.Writer().FromObjects(slices.Values(objs)); err != nil {
		t.Fatalf("graph FromObjects: %v", err)
	}
	return g.Snapshot
}

// TestEnrich_LivePath_ReusesGraphAndCaches: with the graph feed on,
// enrichment must NOT run its own List pass — owner chain, pods, and
// radius come from the live snapshot + informer cache, the workload
// object costs one GET, and the edges section is an overflow trailer
// (the live informer set has no Service/RBAC index).
func TestEnrich_LivePath_ReusesGraphAndCaches(t *testing.T) {
	t.Parallel()
	pod := enrichPod()
	// The fake clientset holds ONLY the deployment: everything else
	// must come from the live seams, and any List would fail loudly.
	cs := fake.NewClientset(enrichDeployment())
	cs.PrependReactor("list", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("live path must not List (asked for %s)", a.GetResource().Resource)
	})
	e := testEnricher(newMetrics(), cs, enrichLogFixture())
	e.snapshot = liveSnapshotOf(t, pod, enrichReplicaSet(), enrichNode())
	e.livePod = func(ns, name string) (*corev1.Pod, error) {
		if ns == pod.Namespace && name == pod.Name {
			return pod, nil
		}
		return nil, fmt.Errorf("no cached pod %s/%s", ns, name)
	}

	b := e.Incident(context.Background(), crashSignal())

	head, _, _ := strings.Cut(b, "\n")
	for _, want := range []string{"workload=Deployment/prod/api", "pods=1", "sections=spec,delta,radius,logs"} {
		if !strings.Contains(head, want) {
			t.Errorf("live head missing %q:\n%s", want, head)
		}
	}
	for _, want := range []string{
		"section=spec",   // from the one API GET
		"section=delta",  // over cached pod + fetched deployment
		"section=radius", // from the live snapshot (node neighbor)
		"kind_of_object=Node",
		"section=logs",
		`overflow section=edges cmd="lookout state edges --workload=Deployment/prod/api"`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("live bundle missing %q:\n%s", want, b)
		}
	}
	if strings.Contains(b, "enrichment_error") {
		t.Errorf("live path should have succeeded end to end:\n%s", b)
	}
	// The scoped fallback still works when the object is NOT in the
	// live graph yet: same enricher, unknown pod → falls back, and
	// the reactor above makes the List fail → resolve-stage trailer
	// (proving it took the fallback, not the live path).
	sig := crashSignal()
	sig.Name = "not-in-graph"
	sig.Key.UID = "uid-unknown"
	b2 := e.Incident(context.Background(), sig)
	if !strings.Contains(b2, "enrichment_error stage=resolve") {
		t.Errorf("unknown object should route to the scoped fallback:\n%s", b2)
	}
}

// --- storm enrichment ---

// TestEnrichStorm_RadiusOnly: a node storm's enrichment is the
// ancestor + affected summary + radius from the live graph — cheap by
// design: no logs, no spec, no per-member reads.
func TestEnrichStorm_RadiusOnly(t *testing.T) {
	t.Parallel()
	pod1, pod2 := enrichPod(), enrichPod()
	pod2.Name = "api-" + enrichHash + "-y7q2m"
	cs := fake.NewClientset()
	cs.PrependReactor("*", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("storm enrichment must not touch the API (asked %s %s)", a.GetVerb(), a.GetResource().Resource)
	})
	e := testEnricher(newMetrics(), cs, &enrichFakeLogs{})
	e.snapshot = liveSnapshotOf(t, pod1, pod2, enrichReplicaSet(), enrichNode())

	info := engine.StormInfo{
		ID:             "Node//node-1",
		Ancestor:       engine.Ancestor{Kind: "Node", Name: "node-1"},
		Fingerprint:    "sha256:feed",
		Reason:         "CrashLoopBackOff",
		Severity:       engine.SeverityCritical,
		AffectedCount:  2,
		NamespaceCount: 1,
	}
	b := e.Storm(context.Background(), info)

	head, _, _ := strings.Cut(b, "\n")
	for _, want := range []string{"kind=bundle.target", "kind_of_object=Node", "name=node-1", "affected=2", "affected_namespaces=1", "sections=radius"} {
		if !strings.Contains(head, want) {
			t.Errorf("storm head missing %q:\n%s", want, head)
		}
	}
	if !strings.Contains(b, "section=radius") || !strings.Contains(b, pod1.Name) {
		t.Errorf("storm bundle should carry the node's radius:\n%s", b)
	}
	for _, forbidden := range []string{"section=logs", "section=spec", "section=delta"} {
		if strings.Contains(b, forbidden) {
			t.Errorf("storm enrichment must be radius-only; found %q:\n%s", forbidden, b)
		}
	}
	if got := testutil.ToFloat64(e.metrics.enrichments.WithLabelValues("ok")); got != 1 {
		t.Errorf("enrichments_total{outcome=ok} = %v, want 1", got)
	}
}

// TestEnrichStormDispatch: the kind=storm payload carries the
// enrichment field when the stage is on; the storm wire pin
// (TestStormFormed_ExactWireShape, no enricher) is unchanged.
func TestEnrichStormDispatch(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, sigs := newStormDispatcher(t, base, 3)
	pod1, pod2 := enrichPod(), enrichPod()
	pod2.Name = "api-" + enrichHash + "-y7q2m"
	pod1.Spec.NodeName = "gke-a"
	pod2.Spec.NodeName = "gke-a"
	node := enrichNode()
	node.Name = "gke-a"
	e := testEnricher(d.metrics, fake.NewClientset(), &enrichFakeLogs{})
	e.snapshot = liveSnapshotOf(t, pod1, pod2, node)
	d.enrich = e

	ctx := context.Background()
	for _, sig := range sigs {
		d.DispatchSignal(ctx, sig)
	}
	var stormBundle string
	for _, ri := range *injects {
		var p struct {
			Kind       string `json:"kind"`
			Enrichment *struct {
				Bundle string `json:"bundle"`
			} `json:"enrichment"`
		}
		if err := json.Unmarshal([]byte(messageOf(t, ri.Body)), &p); err != nil {
			continue
		}
		if p.Kind == inject.KindStorm {
			if p.Enrichment == nil {
				t.Fatalf("kind=storm payload has no enrichment: %s", ri.Body)
			}
			stormBundle = p.Enrichment.Bundle
		}
	}
	if stormBundle == "" {
		t.Fatalf("no kind=storm inject captured")
	}
	if !strings.Contains(stormBundle, "name=gke-a") || !strings.Contains(stormBundle, "sections=radius") {
		t.Errorf("storm enrichment should be the gke-a radius:\n%s", stormBundle)
	}
}

// --- byte-exact pin ---

// TestDispatchSignal_EnrichedExactWireShape pins the enriched inject
// payload byte-for-byte: the frozen k8s-event fields in their frozen
// order, then the additive enrichment object — the companion to the
// M0 pin (TestDispatcher_ExactInjectPayloadWireShape), which this PR
// leaves untouched because omitempty keeps un-enriched payloads
// byte-identical.
func TestDispatchSignal_EnrichedExactWireShape(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	// Minimal deterministic fixture: one unowned crash-looping pod,
	// no node/service (empty edges + radius sections are omitted),
	// one log line.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "solo"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.example.com/solo:v1"}},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: ptrTo(metav1.Time{Time: enrichNow.Add(-time.Hour)}),
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", Ready: false, RestartCount: 7,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: "back-off 5m0s restarting failed container",
				}},
			}},
		},
	}
	lg := &enrichFakeLogs{streams: map[string]string{
		"prod/solo/app": "2026-07-24T11:59:01Z ERROR boom\n",
	}}
	e := testEnricher(nil, fake.NewClientset(pod), lg)
	d := newEnrichedDispatcher(t, base, e)

	d.DispatchSignal(context.Background(), engine.Signal{
		Kind:     engine.KindK8sEvent,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "uid-solo", Reason: "CrashLoopBackOff"},
			Namespace:    "prod",
			KindOfObject: "Pod",
			Name:         "solo",
			Message:      "Back-off restarting failed container",
			FirstSeen:    time.Date(2026, 7, 24, 11, 55, 0, 0, time.UTC),
			LastSeen:     time.Date(2026, 7, 24, 11, 58, 0, 0, time.UTC),
			Count:        1,
		},
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	got := messageOf(t, (*injects)[0].Body)
	if got != enrichedWirePin {
		t.Errorf("enriched payload wire shape drifted:\n got: %s\nwant: %s", got, enrichedWirePin)
	}
}

func ptrTo[T any](v T) *T { return &v }

// enrichedWirePin is the full enriched inject message for the solo-pod
// fixture above. PIN: regenerating it requires the same fixture and
// enrichNow; any drift is a wire-contract change.
const enrichedWirePin = `{"kind":"k8s-event","reason":"CrashLoopBackOff","namespace":"prod","kind_of_object":"Pod","name":"solo","uid":"uid-solo","message":"Back-off restarting failed container","count":1,"first_seen":"2026-07-24T11:55:00Z","last_seen":"2026-07-24T11:58:00Z","cluster":"prod-us-central1","context":{},"enrichment":{"bundle":"kind=bundle.target severity=info namespace=prod kind_of_object=Pod name=solo workload=Pod/prod/solo pods=1 sections=spec,delta,edges,logs\nkind=spec.resource severity=info namespace=prod kind_of_object=Pod name=solo section=spec\nkind=spec.container severity=info namespace=prod kind_of_object=Pod name=solo section=spec container=app image=registry.example.com/solo:v1\nkind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=solo reason=CrashLoopBackOff section=delta container=app restarts=7\nkind=edge.missing_ref severity=critical namespace=prod kind_of_object=ServiceAccount name=default reason=FailedCreate message=\"serviceaccount not found — new pods cannot be created\" section=edges workload=Pod/prod/solo service_account=default\nkind=log.template severity=warning namespace=prod kind_of_object=Pod name=solo section=logs template=\"ERROR boom\" count=1 level=error first_seen=2026-07-24T11:59:01Z last_seen=2026-07-24T11:59:01Z\n"}}`
