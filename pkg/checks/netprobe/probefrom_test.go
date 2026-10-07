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

package netprobe

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var (
	testImage   = "ghcr.io/go-steer/lookout@sha256:" + strings.Repeat("ab", 32)
	probeNow    = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	podFlag     = "--pod=shop/frontend"
	imageFlag   = "--image=" + testImage
	wantName    = ProbeContainerPrefix + "t3st1d00"
	wantVantage = "pod:shop/frontend"
)

func runningPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "frontend"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "frontend:1"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// fakeCluster is a fake API server for one pod: it answers the
// self-reviews, records the ephemeral-container patch, and reports the
// probe container's state once it has been added.
type fakeCluster struct {
	cs *fake.Clientset

	mu       sync.Mutex
	allowed  map[string]bool // "verb resource/sub" -> SSAR answer; absent = allowed
	patchErr error
	patches  []k8stesting.PatchAction
	added    []string
	state    corev1.ContainerState
	log      string
}

func newFakeCluster(t *testing.T, pod *corev1.Pod) *fakeCluster {
	t.Helper()
	fc := &fakeCluster{
		cs:      fake.NewClientset(pod, guardPolicy(), guardBinding()),
		allowed: map[string]bool{},
		state:   corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
	}
	fc.cs.PrependReactor("create", "selfsubjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		r := a.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
		ra := r.Spec.ResourceAttributes
		key := ra.Verb + " " + ra.Resource + "/" + ra.Subresource
		fc.mu.Lock()
		allowed, set := fc.allowed[key]
		fc.mu.Unlock()
		r.Status.Allowed = !set || allowed
		return true, r, nil
	})
	fc.cs.PrependReactor("create", "selfsubjectreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		r := &authenticationv1.SelfSubjectReview{}
		r.Status.UserInfo.Username = testUser
		return true, r, nil
	})
	fc.cs.PrependReactor("patch", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "ephemeralcontainers" {
			return false, nil, nil
		}
		pa := a.(k8stesting.PatchAction)
		fc.mu.Lock()
		defer fc.mu.Unlock()
		fc.patches = append(fc.patches, pa)
		if fc.patchErr != nil {
			return true, nil, fc.patchErr
		}
		var body struct {
			Spec struct {
				EphemeralContainers []corev1.EphemeralContainer `json:"ephemeralContainers"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(pa.GetPatch(), &body); err != nil {
			return true, nil, err
		}
		for _, c := range body.Spec.EphemeralContainers {
			fc.added = append(fc.added, c.Name)
		}
		return true, pod.DeepCopy(), nil
	})
	fc.cs.PrependReactor("get", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		if len(fc.added) == 0 {
			return false, nil, nil
		}
		p := pod.DeepCopy()
		for _, n := range fc.added {
			p.Spec.EphemeralContainers = append(p.Spec.EphemeralContainers, corev1.EphemeralContainer{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: n}})
			p.Status.EphemeralContainerStatuses = append(p.Status.EphemeralContainerStatuses, corev1.ContainerStatus{Name: n, State: fc.state})
		}
		return true, p, nil
	})
	return fc
}

func (fc *fakeCluster) deps() FromDeps {
	return FromDeps{
		Client: func(context.Context) (kubernetes.Interface, error) { return fc.cs, nil },
		Logs: func(context.Context, kubernetes.Interface, string, string, string) (io.ReadCloser, error) {
			fc.mu.Lock()
			defer fc.mu.Unlock()
			return io.NopCloser(strings.NewReader(fc.log)), nil
		},
		Now:  func() time.Time { return probeNow },
		Poll: 5 * time.Millisecond,
		ID:   func() string { return "t3st1d00" },
	}
}

func (fc *fakeCluster) patchCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.patches)
}

// innerLog is what the probe container would write: the real `net
// probe` output, in JSON, for the same targets.
func innerLog(t *testing.T, args ...string) string {
	t.Helper()
	res := checktest.Run(t, New(fakeDeps(testResolver())), append([]string{"--format=json"}, args...)...)
	if res.Code != emit.ExitData {
		t.Fatalf("inner net probe: exit %d: %s", res.Code, res.Stderr)
	}
	return res.Stdout
}

func parseOutput(t *testing.T, stdout string) ([]record, record) {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	var recs []record
	for _, l := range lines[:len(lines)-1] {
		recs = append(recs, parseLogfmtLine(t, l))
	}
	return recs, parseLogfmtLine(t, lines[len(lines)-1])
}

// --- registration ------------------------------------------------------------

func TestProbeFromRegisteredAsPrivileged(t *testing.T) {
	c, ok := checks.Lookup("net probe-from")
	if !ok {
		t.Fatal("net probe-from is not registered")
	}
	if c.MCPName != "k8s_net_probe_from" || !c.Writes || !c.Privileged {
		t.Errorf("MCPName=%q Writes=%v Privileged=%v, want k8s_net_probe_from, true, true", c.MCPName, c.Writes, c.Privileged)
	}
	if len(c.MCPProfiles) != 0 {
		t.Errorf("a privileged command must not join an MCP profile, got %v", c.MCPProfiles)
	}
	if c.TimeoutDefault != 60*time.Second {
		t.Errorf("TimeoutDefault = %s, want 60s (image pull plus probes)", c.TimeoutDefault)
	}
}

func TestProbeFromFixedImageDropsTheFlag(t *testing.T) {
	c := NewFrom(FromDeps{}, FromConfig{Image: testImage})
	for _, f := range c.Flags {
		if f.Name == "image" {
			t.Fatal("FromConfig.Image set, but --image is still a flag")
		}
	}
}

func TestProbeFromContract(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.log = innerLog(t, "--dns=api.prod.svc.cluster.local,missing.prod.svc")
	checktest.VerifyContract(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag,
		"--dns=api.prod.svc.cluster.local,missing.prod.svc")
}

// --- the patch ---------------------------------------------------------------

func TestProbeFromPatchIsBuiltCorrectly(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.log = innerLog(t, "--tcp=127.0.0.1:1")
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag,
		"--dns=cart.shop.svc", "--tcp=cart.shop.svc:7070", "--http=http://cart.shop.svc:7070/healthz", "--probe-timeout=2s")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d: %s", res.Code, res.Stderr)
	}
	if fc.patchCount() != 1 {
		t.Fatalf("patches = %d, want exactly 1", fc.patchCount())
	}
	pa := fc.patches[0]
	if pa.GetPatchType() != types.StrategicMergePatchType {
		t.Errorf("patch type = %s, want strategic merge (what kubectl debug sends)", pa.GetPatchType())
	}
	if pa.GetNamespace() != "shop" || pa.GetName() != "frontend" || pa.GetSubresource() != "ephemeralcontainers" {
		t.Errorf("patched %s/%s subresource %q, want shop/frontend ephemeralcontainers", pa.GetNamespace(), pa.GetName(), pa.GetSubresource())
	}
	var body struct {
		Spec struct {
			EphemeralContainers []corev1.EphemeralContainer `json:"ephemeralContainers"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(pa.GetPatch(), &body); err != nil {
		t.Fatal(err)
	}
	if n := len(body.Spec.EphemeralContainers); n != 1 {
		t.Fatalf("patch adds %d containers, want 1", n)
	}
	c := body.Spec.EphemeralContainers[0]
	if c.Name != wantName {
		t.Errorf("name = %q, want %q", c.Name, wantName)
	}
	if c.Image != testImage {
		t.Errorf("image = %q, want the digest ref", c.Image)
	}
	if strings.Join(c.Command, " ") != "/lookout net probe" {
		t.Errorf("command = %v, want /lookout net probe", c.Command)
	}
	if len(c.Args) != 6 {
		t.Fatalf("args = %q, want 6", c.Args)
	}
	if c.Args[0] != "--format=json" || !strings.HasPrefix(c.Args[1], "--timeout=") || c.Args[2] != "--probe-timeout=2s" ||
		c.Args[3] != "--dns=cart.shop.svc" || c.Args[4] != "--tcp=cart.shop.svc:7070" || c.Args[5] != "--http=http://cart.shop.svc:7070/healthz" {
		t.Errorf("args = %q", c.Args)
	}
	inner, err := time.ParseDuration(strings.TrimPrefix(c.Args[1], "--timeout="))
	if err != nil || inner < time.Second || inner > 60*time.Second {
		t.Errorf("inner --timeout = %q, want within the outer 60s budget", c.Args[1])
	}
	if c.TargetContainerName != "" {
		t.Errorf("targetContainerName = %q: the probe must not share any container's process namespace", c.TargetContainerName)
	}
	if len(c.VolumeMounts) != 0 || len(c.VolumeDevices) != 0 || len(c.EnvFrom) != 0 {
		t.Errorf("the probe container must mount nothing: mounts=%v devices=%v envFrom=%v", c.VolumeMounts, c.VolumeDevices, c.EnvFrom)
	}
	if c.Stdin || c.TTY {
		t.Error("the probe container must not take stdin or a TTY")
	}
	sc := c.SecurityContext
	switch {
	case sc == nil:
		t.Fatal("no security context")
	case sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot,
		sc.RunAsUser == nil || *sc.RunAsUser != 65532,
		sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation,
		sc.Privileged == nil || *sc.Privileged,
		sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem,
		sc.Capabilities == nil || len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" || len(sc.Capabilities.Add) != 0,
		sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault:
		t.Errorf("security context is not Pod Security restricted: %+v", sc)
	}
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	if env[EnvRequestedBy] != testUser || env[EnvRequestedAt] != "2026-10-07T12:00:00Z" || !strings.HasPrefix(env[EnvClient], "lookout") {
		t.Errorf("audit env = %v", env)
	}
	if len(env) != 3 {
		t.Errorf("env carries %d entries, want only the three audit entries: %v", len(env), env)
	}
}

// --- output parity -----------------------------------------------------------

func TestProbeFromOutputMatchesNetProbe(t *testing.T) {
	args := []string{"--dns=api.prod.svc.cluster.local,missing.prod.svc,slow.example"}
	local, localSummary := runRecords(t, fakeDeps(testResolver()), args...)

	fc := newFakeCluster(t, runningPod())
	fc.log = innerLog(t, args...)
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), append([]string{podFlag, imageFlag}, args...)...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d: %s", res.Code, res.Stderr)
	}
	// Byte for byte, each result line is net probe's with only the
	// vantage swapped and the container named.
	localOut := checktest.Run(t, New(fakeDeps(testResolver())), args...).Stdout
	localLines := strings.Split(localOut, "\n")
	remoteLines := strings.Split(res.Stdout, "\n")
	for i := 0; i < len(localLines)-2 && i < len(remoteLines)-2; i++ {
		want := strings.Replace(localLines[i], " vantage=local ", " vantage="+wantVantage+" probe_container="+wantName+" ", 1)
		if remoteLines[i] != want {
			t.Errorf("line %d:\n got  %s\n want %s", i, remoteLines[i], want)
		}
	}

	remote, remoteSummary := parseOutput(t, res.Stdout)
	if len(remote) != len(local) {
		t.Fatalf("probe-from emitted %d results, net probe %d", len(remote), len(local))
	}
	if remoteSummary["scanned"] != localSummary["scanned"] {
		t.Errorf("scanned = %s, net probe said %s", remoteSummary["scanned"], localSummary["scanned"])
	}
	for i := range local {
		l, r := local[i], remote[i]
		if l["vantage"] != "local" || r["vantage"] != wantVantage {
			t.Errorf("vantage: net probe %q, probe-from %q", l["vantage"], r["vantage"])
		}
		if r["probe_container"] != wantName {
			t.Errorf("probe_container = %q, want %q", r["probe_container"], wantName)
		}
		delete(l, "vantage")
		delete(r, "vantage")
		delete(r, "probe_container")
		if len(l) != len(r) {
			t.Errorf("record %d keys differ:\n net probe:  %v\n probe-from: %v", i, l, r)
			continue
		}
		for k, v := range l {
			if r[k] != v {
				t.Errorf("record %d %s: net probe %q, probe-from %q", i, k, v, r[k])
			}
		}
	}
}

// --- refusals: nothing changed, exit 0 ---------------------------------------

func assertRefused(t *testing.T, fc *fakeCluster, res checktest.Result, reason string, contains ...string) {
	t.Helper()
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0 (a refusal is an answer): %s", res.Code, res.Stderr)
	}
	recs, summary := parseOutput(t, res.Stdout)
	if len(recs) != 1 {
		t.Fatalf("got %d records, want exactly one probe.refused:\n%s", len(recs), res.Stdout)
	}
	r := recs[0]
	if r["kind"] != KindRefused || r["reason"] != reason || r["vantage"] != wantVantage {
		t.Errorf("record = %v, want kind=%s reason=%s vantage=%s", r, KindRefused, reason, wantVantage)
	}
	if r["namespace"] != "shop" || r["name"] != "frontend" || r["kind_of_object"] != "Pod" {
		t.Errorf("refusal subject = %s/%s (%s)", r["namespace"], r["name"], r["kind_of_object"])
	}
	if !strings.HasSuffix(r["message"], "no probe ran and the pod was not changed") {
		t.Errorf("message does not say nothing changed: %q", r["message"])
	}
	for _, c := range contains {
		if !strings.Contains(r["message"], c) {
			t.Errorf("message lacks %q: %q", c, r["message"])
		}
	}
	if summary["scanned"] != "0" {
		t.Errorf("scanned = %s, want 0", summary["scanned"])
	}
	if _, ok := r["probe_container"]; ok {
		t.Error("a refusal names a probe container, but none was added")
	}
}

func TestProbeFromRefusedWithoutTheGrant(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.allowed["patch pods/ephemeralcontainers"] = false
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
	assertRefused(t, fc, res, "Forbidden",
		checks.Refused("patch", "", "pods/ephemeralcontainers").String(),
		"deploy-probe/", "rbac.probeFrom=true")
	if fc.patchCount() != 0 {
		t.Errorf("patched %d times after the access review said no", fc.patchCount())
	}
}

func TestProbeFromRefusedWithoutLogAccess(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.allowed["get pods/log"] = false
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
	assertRefused(t, fc, res, "Forbidden", "forbidden: get pods/log")
	if fc.patchCount() != 0 {
		t.Error("added a container whose result it could not read")
	}
}

// TestProbeFromRefusedWhenThePatchIsForbidden: the access review is a
// courtesy, not the gate (a server may not answer it). Here it says
// yes, the exact-view fixture refuses the patch itself, and the
// answer is still the one shared refusal.
func TestProbeFromRefusedWhenThePatchIsForbidden(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	checktest.ViewRole(fc.cs)
	// Let the policy reads through (an identity with the guard's read
	// grant but not the patch), so the patch itself is what is refused.
	for _, r := range []string{"validatingadmissionpolicies", "validatingadmissionpolicybindings"} {
		fc.cs.PrependReactor("list", r, k8stesting.ObjectReaction(fc.cs.Tracker()))
	}
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
	assertRefused(t, fc, res, "Forbidden",
		checks.Refused("patch", "", "pods/ephemeralcontainers").String())
}

// TestProbeFromAdmissionDenied: Pod Security answers 403 and an
// admission policy 422; neither is an RBAC gap, and neither added
// anything.
func TestProbeFromAdmissionDenied(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "frontend",
			errors.New(`violates PodSecurity "restricted:latest": allowPrivilegeEscalation != false`)), "violates PodSecurity"},
		{apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "frontend", nil), "is invalid"},
	} {
		fc := newFakeCluster(t, runningPod())
		fc.patchErr = tc.err
		res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
		assertRefused(t, fc, res, "AdmissionDenied", tc.want)
	}
}

func TestProbeFromRefusesUnprobeablePods(t *testing.T) {
	full := runningPod()
	for i := 0; i < MaxProbeContainers; i++ {
		full.Spec.EphemeralContainers = append(full.Spec.EphemeralContainers,
			corev1.EphemeralContainer{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: ProbeContainerPrefix + string(rune('a'+i))}})
	}
	host := runningPod()
	host.Spec.HostNetwork = true
	pending := runningPod()
	pending.Status.Phase = corev1.PodPending
	for _, tc := range []struct {
		pod    *corev1.Pod
		reason string
	}{
		{host, "HostNetwork"},
		{pending, "PodNotRunning"},
		{full, "ProbeLimit"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			fc := newFakeCluster(t, tc.pod)
			res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
			assertRefused(t, fc, res, tc.reason)
			if fc.patchCount() != 0 {
				t.Error("patched an unprobeable pod")
			}
		})
	}
}

// --- usage errors: exit 2, nothing touched -----------------------------------

func TestProbeFromUsageErrors(t *testing.T) {
	tcp := "--tcp=cart.shop.svc:7070"
	for _, args := range [][]string{
		{imageFlag, tcp},                                              // no --pod
		{"--pod=frontend", imageFlag, tcp},                            // no namespace
		{"--pod=shop/frontend/x", imageFlag, tcp},                     // too many parts
		{"--pod=Shop/frontend", imageFlag, tcp},                       // invalid namespace
		{"--pod=shop/", imageFlag, tcp},                               // empty name
		{podFlag, tcp},                                                // no image
		{podFlag, "--image=ghcr.io/go-steer/lookout:v0.33.0", tcp},    // a tag
		{podFlag, "--image=ghcr.io/go-steer/lookout@sha256:abc", tcp}, // short digest
		{podFlag, imageFlag},                                          // nothing to probe
		{podFlag, imageFlag, tcp, "--namespace=shop"},
		{podFlag, imageFlag, tcp, "-A"},
		{podFlag, imageFlag, tcp, "--workload=Deployment/shop/frontend"},
		{podFlag, imageFlag, tcp, "--since=5m"},
		{podFlag, imageFlag, "--http=https://user:pass@cart.shop.svc/"},
		{podFlag, imageFlag, "--http=http://cart.shop.svc/healthz?token=abc"},
		{podFlag, imageFlag, "--http=http://cart.shop.svc/healthz#x"},
		{podFlag, imageFlag, "--dns=cart.shop.svc/evil"},
		{podFlag, imageFlag, "--tcp=no-port"},
		{podFlag, imageFlag, "--dns=" + strings.Repeat("a.svc,", maxTargets+1)},
		{podFlag, imageFlag, tcp, "--probe-timeout=0s"},
	} {
		called := false
		deps := FromDeps{Client: func(context.Context) (kubernetes.Interface, error) {
			called = true
			return fake.NewClientset(), nil
		}}
		res := checktest.Run(t, NewFrom(deps, FromConfig{}), args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("%v: exit %d, want 2 (stderr %s)", args, res.Code, res.Stderr)
		}
		if res.Stdout != "" {
			t.Errorf("%v: usage error wrote stdout %q", args, res.Stdout)
		}
		if called {
			t.Errorf("%v: built a cluster client before rejecting the invocation", args)
		}
	}
}

// --- failures after the container exists: exit 1, named ----------------------

func TestProbeFromTimeout(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.state = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070", "--timeout=200ms")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want 1: %s", res.Code, res.Stderr)
	}
	for _, want := range []string{"timed out after 200ms", wantName, "remains in pod shop/frontend's spec"} {
		if !strings.Contains(res.Stderr, want) {
			t.Errorf("stderr lacks %q: %s", want, res.Stderr)
		}
	}
	if res.Stdout != "" {
		t.Errorf("a runtime failure wrote stdout: %q", res.Stdout)
	}
	// The container was told to stop on its own inside the budget.
	var body struct {
		Spec struct {
			EphemeralContainers []corev1.EphemeralContainer `json:"ephemeralContainers"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(fc.patches[0].GetPatch(), &body); err != nil {
		t.Fatal(err)
	}
	if got := body.Spec.EphemeralContainers[0].Args[1]; got != "--timeout=1s" {
		t.Errorf("inner timeout = %q, want the 1s floor under a 200ms budget", got)
	}
}

func TestProbeFromImagePullFailure(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.state = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "back-off pulling image"}}
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, "ImagePullBackOff") || !strings.Contains(res.Stderr, "remains") {
		t.Errorf("exit %d stderr %s", res.Code, res.Stderr)
	}
}

func TestProbeFromInnerFailure(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.state = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2}}
	fc.log = "lookout net probe: unknown flag --bogus\n"
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, "exited 2") || !strings.Contains(res.Stderr, "unknown flag --bogus") {
		t.Errorf("exit %d stderr %s", res.Code, res.Stderr)
	}
}

func TestProbeFromPodNotFound(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), "--pod=shop/gone", imageFlag, "--tcp=cart.shop.svc:7070")
	if res.Code != emit.ExitRuntime || !strings.Contains(res.Stderr, "not found") {
		t.Errorf("exit %d stderr %s", res.Code, res.Stderr)
	}
	if fc.patchCount() != 0 {
		t.Error("patched a pod that does not exist")
	}
}

// --- sanitization and forwarding ---------------------------------------------

func TestProbeFromSanitizesAndDropsForeignRecords(t *testing.T) {
	fc := newFakeCluster(t, runningPod())
	fc.log = strings.Join([]string{
		`{"kind":"probe.http","severity":"critical","name":"http://cart.shop.svc/","message":"Get: upstream said Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123","vantage":"local","error_class":"error","token":"s3cr3t-value"}`,
		`{"kind":"secret.dump","severity":"info","name":"x","message":"not a probe result"}`,
		`{"kind":"probe.tcp","severity":"loud","name":"x"}`,
		`not json at all`,
		`{"scanned":1,"findings":1,"elapsed":"1ms"}`,
	}, "\n") + "\n"
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--http=http://cart.shop.svc/")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d: %s", res.Code, res.Stderr)
	}
	if strings.Contains(res.Stdout, "abcdefghijklmnopqrstuvwxyz0123") {
		t.Errorf("a bearer token reached stdout:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "s3cr3t-value") || strings.Contains(res.Stdout, "secret.dump") {
		t.Errorf("an undeclared field or kind was forwarded:\n%s", res.Stdout)
	}
	recs, summary := parseOutput(t, res.Stdout)
	if len(recs) != 1 || recs[0]["kind"] != "probe.http" || recs[0]["vantage"] != wantVantage {
		t.Errorf("records = %v", recs)
	}
	// token field + secret.dump + bad severity = 3 dropped.
	if summary["dropped"] != "3" {
		t.Errorf("dropped = %q, want 3", summary["dropped"])
	}
	if err := checktest.Verify(NewFrom(fc.deps(), FromConfig{}), res.Stdout, emit.FormatLogfmt); err != nil {
		t.Errorf("contract: %v", err)
	}
}
