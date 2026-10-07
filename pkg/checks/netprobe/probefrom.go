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

// `lookout net probe-from` (issue #539; DESIGN §5, amendment of
// 2026-10-07; docs/in-pod-probe-design.md): the same DNS/TCP/HTTP
// probes as `net probe`, sent from inside one named pod's network
// namespace. It is the one lookout command that changes a workload:
// it adds an ephemeral container — `/lookout net probe`, from a
// digest-pinned lookout image — to the pod, waits for it to finish,
// and re-emits its results stamped vantage=pod:<ns>/<name>.
//
// What keeps it opt-in:
//   - Privileged + Writes on the command: never on the default MCP
//     surface, ReadOnlyHint:false wherever it is served, and never
//     composed by scan/health/bundle/the sentinel.
//   - It needs `patch pods/ephemeralcontainers`, which only the
//     deploy-probe/ overlay (Helm rbac.probeFrom=true) grants, beside
//     an admission policy that pins what that grant can add.
//   - Without the grant it changes nothing and answers with one
//     probe.refused record, exit 0 (the #546 rule for every refusal).

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

func init() {
	checks.Register(NewFrom(FromDeps{}, FromConfig{}))
}

const (
	// KindRefused is the answer when nothing was probed and the pod
	// was left unchanged.
	KindRefused = "probe.refused"

	// ProbeContainerPrefix names every container this command adds.
	// The admission policy in deploy-probe/ requires it, and the
	// per-pod limit counts it.
	ProbeContainerPrefix = "lookout-probe-"
	// MaxProbeContainers is the per-pod limit: past it the command
	// refuses rather than grow the pod's spec further. Replacing the
	// pod clears them.
	MaxProbeContainers = 10
	// probeUser is the distroless image's nonroot user. The image
	// declares it by name ("nonroot"), which the kubelet cannot check
	// against runAsNonRoot, so the container states the number.
	probeUser = int64(65532)
	// maxTargets bounds how much one invocation writes into the pod
	// spec.
	maxTargets = 32
	// maxLogBytes caps what is read back from the probe container.
	maxLogBytes = int64(1 << 20)
	// The audit-trail environment entries on the probe container.
	EnvRequestedBy = "LOOKOUT_PROBE_REQUESTED_BY"
	EnvRequestedAt = "LOOKOUT_PROBE_REQUESTED_AT"
	EnvClient      = "LOOKOUT_PROBE_CLIENT"
)

// probeCommand is what the probe container runs; the admission
// policy pins it.
var probeCommand = []string{"/lookout", "net", "probe"}

// digestRef is an image reference pinned by digest. Tags are refused:
// a digest names exactly the bytes that will run in someone else's
// pod, and it is what the admission policy checks.
var digestRef = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+@sha256:[a-f0-9]{64}$`)

// ValidImage reports whether ref is a digest-pinned image reference
// `net probe-from` accepts, with the reason when it is not.
func ValidImage(ref string) error {
	if ref == "" {
		return errors.New("no probe image: pass a lookout release image pinned by digest, <repo>@sha256:<digest> (e.g. `crane digest ghcr.io/go-steer/lookout:<version>`)")
	}
	if !digestRef.MatchString(ref) {
		return fmt.Errorf("probe image %q is not pinned by digest: pass <repo>@sha256:<64 hex>, not a tag — the digest is what runs inside the pod, and what deploy-probe/'s admission policy checks", ref)
	}
	return nil
}

// FromDeps are the injected seams. Zero values give production
// behavior.
type FromDeps struct {
	// Client builds the Kubernetes client. Nil means kube.BuildClient.
	Client func(ctx context.Context) (kubernetes.Interface, error)
	// Logs opens the probe container's log. Nil means GetLogs on the
	// client (the fake clientset cannot serve log content).
	Logs func(ctx context.Context, c kubernetes.Interface, namespace, pod, container string) (io.ReadCloser, error)
	// Now is the clock for the audit timestamp. Nil means time.Now.
	Now func() time.Time
	// Poll is how often the pod is re-read while the probe runs. Zero
	// means one second.
	Poll time.Duration
	// ID returns the probe container's name suffix. Nil means eight
	// random lowercase characters.
	ID func() string
}

// FromConfig configures one build of the command.
type FromConfig struct {
	// Image, when set, fixes the probe image and removes the --image
	// flag. `lookout mcp --probe-from-image` builds the MCP tool this
	// way, so a model calling the tool cannot choose what runs in the
	// pod. Empty — the CLI — makes --image a required flag.
	Image string
}

func (d FromDeps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.BuildClient(kube.OptionsFrom(ctx))
}

func (d FromDeps) logs(ctx context.Context, c kubernetes.Interface, ns, pod, container string) (io.ReadCloser, error) {
	if d.Logs != nil {
		return d.Logs(ctx, c, ns, pod, container)
	}
	limit := maxLogBytes
	return c.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Container: container, LimitBytes: &limit}).Stream(ctx)
}

func (d FromDeps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d FromDeps) poll() time.Duration {
	if d.Poll > 0 {
		return d.Poll
	}
	return time.Second
}

func (d FromDeps) id() string {
	if d.ID != nil {
		return d.ID()
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// NewFrom builds the `net probe-from` command.
func NewFrom(deps FromDeps, cfg FromConfig) checks.Command {
	flags := []emit.FlagSpec{
		{Name: "pod", Type: emit.FlagString, Default: "",
			Help: "required: the one pod to probe from, as <namespace>/<name>; no selectors, no -A"},
	}
	if cfg.Image == "" {
		flags = append(flags, emit.FlagSpec{Name: "image", Type: emit.FlagString, Default: "",
			Help: "required: the lookout release image the probe container runs, pinned by digest (<repo>@sha256:<digest>; tags are refused)"})
	}
	flags = append(flags, probeFlags()...)
	return checks.Command{
		Name:    "net probe-from",
		MCPName: "k8s_net_probe_from",
		Summary: "PRIVILEGED, opt-in: run net probe's DNS/TCP/HTTP checks from inside one named pod's network — the caller's vantage, for faults only that pod sees — by adding an ephemeral container that stays in the pod's spec; refused without the deploy-probe/ grant.",
		Flags:   flags,
		Kinds: append(probeKinds(),
			checks.Kind(KindRefused, "no probe ran and the pod was not changed: this identity lacks a grant the command needs (the message names it, in the shared refusal wording), an admission policy rejected the probe container (reason=AdmissionDenied), the admission policy that guards the grant is missing or does not cover this identity so the grant is unguarded (reason=PolicyMissing), or the pod cannot be probed safely (not Running, hostNetwork, or already holding the per-pod limit of lookout probe containers); reason says which", emit.SeverityInfo)),
		Output: append([]checks.OutputField{
			{Name: "vantage", Doc: "where the probe ran from: pod:<namespace>/<name>, never local — a probe-from answer never falls back to lookout's own vantage"},
			{Name: "probe_container", Doc: "the ephemeral container that ran the probe; it stays in the pod's spec for the pod's lifetime, so this ties a result to that record"},
		}, append(probeOutput(),
			checks.OutputField{Name: "dropped", Doc: "summary line: records or fields the probe container wrote that are not net probe results, and so were not forwarded (absent when none)"})...),
		Examples: []string{
			"lookout net probe-from --pod=shop/frontend-7d9 --image=ghcr.io/go-steer/lookout@sha256:<digest> --tcp=cart.shop.svc:7070",
			"lookout net probe-from --pod=shop/frontend-7d9 --image=ghcr.io/go-steer/lookout@sha256:<digest> --dns=cart.shop.svc.cluster.local --http=http://cart.shop.svc:7070/healthz",
		},
		TimeoutDefault: 60 * time.Second,
		Writes:         true,
		Privileged:     true,
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runFrom(ctx, deps, cfg, inv)
		},
	}
}

// probeRun is one validated invocation.
type probeRun struct {
	namespace, pod string
	image          string
	targets        targets
}

func parseFrom(cfg FromConfig, inv emit.Invocation) (probeRun, error) {
	var p probeRun
	if err := rejectScope(inv.Scope, "net probe-from probes network targets from inside the one pod --pod names", "the vantage point is the pod --pod names"); err != nil {
		return p, err
	}
	ref := strings.TrimSpace(inv.Flags.String("pod"))
	ns, name, ok := strings.Cut(ref, "/")
	if ref == "" || !ok || strings.Contains(name, "/") {
		return p, emit.UsageErrorf("--pod=<namespace>/<name> is required: net probe-from adds a container to exactly one named pod, so there is no default and no selector")
	}
	if errs := validation.IsDNS1123Label(ns); len(errs) > 0 {
		return p, emit.UsageErrorf("--pod namespace %q is not a valid namespace name: %s", ns, strings.Join(errs, "; "))
	}
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return p, emit.UsageErrorf("--pod name %q is not a valid pod name: %s", name, strings.Join(errs, "; "))
	}
	p.namespace, p.pod = ns, name
	p.image = cfg.Image
	if p.image == "" {
		p.image = strings.TrimSpace(inv.Flags.String("image"))
	}
	if err := ValidImage(p.image); err != nil {
		return p, emit.UsageErrorf("--image: %v", err)
	}
	t, err := parseTargets(inv.Flags, true)
	if err != nil {
		return p, err
	}
	if t.count() > maxTargets {
		return p, emit.UsageErrorf("%d probe targets: net probe-from takes at most %d per invocation, because every target is written into the pod's spec", t.count(), maxTargets)
	}
	p.targets = t
	return p, nil
}

func runFrom(ctx context.Context, deps FromDeps, cfg FromConfig, inv emit.Invocation) (int, error) {
	p, err := parseFrom(cfg, inv)
	if err != nil {
		return 0, err
	}
	if err := inv.Out.Stamp("vantage", "pod:"+p.namespace+"/"+p.pod); err != nil {
		return 0, err
	}
	c, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	refuse := func(reason, msg string) (int, error) {
		return 0, inv.Out.Emit(emit.Finding{
			Kind:         KindRefused,
			Severity:     emit.SeverityInfo,
			Namespace:    p.namespace,
			KindOfObject: "Pod",
			Name:         p.pod,
			Reason:       reason,
			Message:      msg + " — no probe ran and the pod was not changed",
		})
	}

	pod, err := c.CoreV1().Pods(p.namespace).Get(ctx, p.pod, metav1.GetOptions{})
	if err != nil {
		if r, ok := checks.ForbiddenRefusal(err); ok {
			r.Verb, r.Group, r.Resource = "get", "", "pods"
			return refuse("Forbidden", r.String())
		}
		if apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("pod %s/%s not found", p.namespace, p.pod)
		}
		return 0, fmt.Errorf("reading pod %s/%s: %w", p.namespace, p.pod, err)
	}
	if reason, msg := unprobeable(pod); reason != "" {
		return refuse(reason, msg)
	}

	// Ask before changing anything: a probe container the caller
	// could add but whose log it could not read would leave a
	// permanent record and no answer.
	for _, need := range []checks.Refusal{
		checks.Refused("patch", "", "pods/ephemeralcontainers"),
		checks.Refused("get", "", "pods/log"),
	} {
		if !mayDo(ctx, c, p.namespace, p.pod, need) {
			return refuse("Forbidden", grantMessage(need))
		}
	}

	// The grant is only safe beside the admission policy that narrows
	// it. Refuse unless that policy, and a Deny binding for it, are
	// installed and cover this identity: on a cluster without
	// ValidatingAdmissionPolicy (< 1.30), or after a partial apply,
	// the grant is unguarded and lookout will not use it.
	user, known := requester(ctx, c)
	if reason, msg, err := policyGuard(ctx, c, user, known); err != nil {
		return 0, err
	} else if reason != "" {
		return refuse(reason, msg)
	}

	name := ProbeContainerPrefix + deps.id()
	container := probeContainer(name, p, user, deps.now(), innerTimeout(ctx))
	patch, err := ephemeralPatch(container)
	if err != nil {
		return 0, err
	}
	if _, err := c.CoreV1().Pods(p.namespace).Patch(ctx, p.pod, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "ephemeralcontainers"); err != nil {
		switch {
		case apierrors.IsForbidden(err) && strings.Contains(err.Error(), "cannot patch resource"):
			return refuse("Forbidden", grantMessage(checks.Refused("patch", "", "pods/ephemeralcontainers")))
		case apierrors.IsForbidden(err), apierrors.IsInvalid(err):
			// Pod Security, deploy-probe/'s admission policy, or any
			// other admission check: the server rejected the
			// container, so nothing was added.
			return refuse("AdmissionDenied", "the API server rejected the probe container: "+statusMessage(err))
		case apierrors.IsNotFound(err):
			return 0, fmt.Errorf("pod %s/%s disappeared before the probe container could be added", p.namespace, p.pod)
		}
		return 0, fmt.Errorf("adding probe container to pod %s/%s: %w", p.namespace, p.pod, err)
	}
	// From here on the container exists and cannot be removed; every
	// result and every diagnostic names it.
	if err := inv.Out.Stamp("probe_container", name); err != nil {
		return 0, err
	}
	remains := fmt.Sprintf("probe container %s remains in pod %s/%s's spec (ephemeral containers cannot be removed)", name, p.namespace, p.pod)

	exit, err := waitTerminated(ctx, deps, c, p.namespace, p.pod, name)
	if err != nil {
		return 0, fmt.Errorf("%v; %s", err, remains)
	}
	rc, err := deps.logs(ctx, c, p.namespace, p.pod, name)
	if err != nil {
		return 0, fmt.Errorf("reading the probe container's log: %v; %s", err, remains)
	}
	defer func() { _ = rc.Close() }()
	return forward(inv.Out, io.LimitReader(rc, maxLogBytes), exit, remains)
}

// unprobeable reports why the pod must not get a probe container, or
// "" when it may.
func unprobeable(pod *corev1.Pod) (reason, msg string) {
	switch {
	case pod.Spec.HostNetwork:
		return "HostNetwork", "the pod uses hostNetwork, so its vantage is the node's network, and lookout does not add containers to host-network pods (they are usually privileged system components)"
	case pod.DeletionTimestamp != nil:
		return "PodNotRunning", "the pod is terminating"
	case pod.Status.Phase != corev1.PodRunning:
		return "PodNotRunning", fmt.Sprintf("the pod is %s, not Running; a probe needs the pod's network to be up", phaseOrUnknown(pod.Status.Phase))
	}
	n := 0
	for _, e := range pod.Spec.EphemeralContainers {
		if strings.HasPrefix(e.Name, ProbeContainerPrefix) {
			n++
		}
	}
	if n >= MaxProbeContainers {
		return "ProbeLimit", fmt.Sprintf("the pod already holds %d lookout probe containers (the limit is %d); each stays in the spec for the pod's lifetime, and replacing the pod clears them", n, MaxProbeContainers)
	}
	return "", ""
}

func phaseOrUnknown(p corev1.PodPhase) string {
	if p == "" {
		return "in an unknown phase"
	}
	return string(p)
}

// grantMessage is the shared refusal wording plus where lookout's
// own optional grant comes from.
func grantMessage(r checks.Refusal) string {
	msg := r.String()
	if r.Resource == "pods/ephemeralcontainers" {
		msg += " (the deploy-probe/ overlay, or Helm rbac.probeFrom=true, grants it to lookout's ServiceAccount)"
	}
	return msg
}

// mayDo asks the API server whether this identity may do r on the
// pod. An error asking (an old server, or a review this identity may
// not create) counts as yes: the real request is still refused if it
// is not allowed, and that refusal is worded the same way.
func mayDo(ctx context.Context, c kubernetes.Interface, ns, pod string, r checks.Refusal) bool {
	resource, sub, _ := strings.Cut(r.Resource, "/")
	review, err := c.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace:   ns,
				Verb:        r.Verb,
				Group:       r.Group,
				Resource:    resource,
				Subresource: sub,
				Name:        pod,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil || review == nil {
		return true
	}
	return review.Status.Allowed
}

// requester is the caller's username for the audit trail, or
// "unknown" when the server will not say.
func requester(ctx context.Context, c kubernetes.Interface) (string, bool) {
	r, err := c.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil || r == nil || r.Status.UserInfo.Username == "" {
		return "unknown", false
	}
	return r.Status.UserInfo.Username, true
}

// ClientVersion is recorded on every probe container as
// LOOKOUT_PROBE_CLIENT. The binary sets it to "lookout/<release>" at
// startup (cmd/lookout); pkg/ cannot read internal/version itself.
var ClientVersion = "lookout"

func clientVersion() string { return ClientVersion }

// innerTimeout is the probe container's own --timeout: what is left
// of this invocation's budget, so the container exits by itself even
// if this process disappears.
func innerTimeout(ctx context.Context) time.Duration {
	d := emit.DefaultTimeout
	if dl, ok := ctx.Deadline(); ok {
		d = time.Until(dl).Truncate(time.Second)
	}
	if d < time.Second {
		d = time.Second
	}
	return d
}

// probeArgs is the probe container's argv after the command. Each
// flag is one argument and no shell is involved, so a target cannot
// add a flag or run anything.
func probeArgs(t targets, timeout time.Duration) []string {
	args := []string{"--format=json", "--timeout=" + timeout.String(), "--probe-timeout=" + t.timeout.String()}
	for _, f := range []struct {
		flag string
		list []string
	}{{"dns", t.dns}, {"tcp", t.tcp}, {"http", t.http}} {
		if len(f.list) > 0 {
			args = append(args, "--"+f.flag+"="+strings.Join(f.list, ","))
		}
	}
	return args
}

// probeContainer is the ephemeral container this command adds:
// nothing mounted, no target container (so no process namespace is
// shared), and a security context valid under Pod Security
// `restricted`.
func probeContainer(name string, p probeRun, requestedBy string, at time.Time, timeout time.Duration) corev1.EphemeralContainer {
	no, yes := false, true
	user := probeUser
	return corev1.EphemeralContainer{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name:            name,
			Image:           p.image,
			Command:         append([]string(nil), probeCommand...),
			Args:            probeArgs(p.targets, timeout),
			ImagePullPolicy: corev1.PullIfNotPresent,
			Env: []corev1.EnvVar{
				{Name: EnvRequestedBy, Value: requestedBy},
				{Name: EnvRequestedAt, Value: at.UTC().Format(time.RFC3339)},
				{Name: EnvClient, Value: clientVersion()},
			},
			TerminationMessagePolicy: corev1.TerminationMessageReadFile,
			SecurityContext: &corev1.SecurityContext{
				RunAsNonRoot:             &yes,
				RunAsUser:                &user,
				RunAsGroup:               &user,
				AllowPrivilegeEscalation: &no,
				Privileged:               &no,
				ReadOnlyRootFilesystem:   &yes,
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
		},
	}
}

// ephemeralPatch is the strategic-merge patch for the
// pods/ephemeralcontainers subresource, the shape `kubectl debug`
// sends: ephemeralContainers merge on name, so this appends one.
func ephemeralPatch(c corev1.EphemeralContainer) ([]byte, error) {
	return json.Marshal(map[string]any{
		"spec": map[string]any{
			"ephemeralContainers": []corev1.EphemeralContainer{c},
		},
	})
}

// imageFailures are waiting reasons that will not resolve by waiting.
var imageFailures = map[string]bool{
	"ErrImagePull":               true,
	"ImagePullBackOff":           true,
	"InvalidImageName":           true,
	"ErrImageNeverPull":          true,
	"CreateContainerError":       true,
	"CreateContainerConfigError": true,
}

// waitTerminated polls the pod until the probe container has exited,
// returning its exit code.
func waitTerminated(ctx context.Context, deps FromDeps, c kubernetes.Interface, ns, pod, name string) (int32, error) {
	for {
		p, err := c.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
		if err != nil {
			if ctx.Err() != nil {
				return 0, fmt.Errorf("waiting for probe container %s to finish", name)
			}
			return 0, fmt.Errorf("re-reading pod %s/%s while the probe ran: %v", ns, pod, err)
		}
		for _, s := range p.Status.EphemeralContainerStatuses {
			if s.Name != name {
				continue
			}
			if t := s.State.Terminated; t != nil {
				return t.ExitCode, nil
			}
			if w := s.State.Waiting; w != nil && imageFailures[w.Reason] {
				return 0, fmt.Errorf("probe container %s cannot start: %s: %s", name, w.Reason, w.Message)
			}
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("waiting for probe container %s to finish", name)
		case <-time.After(deps.poll()):
		}
	}
}

// allowedKinds are the records forwarded from the probe container.
var allowedKinds = map[string]bool{"probe.dns": true, "probe.tcp": true, "probe.http": true}

// forward re-emits the probe container's results through this
// command's writer (and so its sanitizer), keeping only net probe
// result kinds and glossary fields.
func forward(out *emit.Writer, log io.Reader, exit int32, remains string) (int, error) {
	var (
		records    []emit.Finding
		diag       []string
		dropped    int
		sawSummary bool
	)
	sc := bufio.NewScanner(log)
	sc.Buffer(make([]byte, 64*1024), int(maxLogBytes))
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if line[0] != '{' {
			// The container's stderr shares its log: diagnostics.
			if len(diag) < 5 {
				diag = append(diag, truncate(string(line), 300))
			}
			continue
		}
		rec, err := decodeRecord(line)
		if err != nil {
			dropped++
			continue
		}
		if rec.has("scanned") {
			sawSummary = true
			continue
		}
		f, n, ok := toFinding(rec)
		dropped += n
		if !ok {
			dropped++
			continue
		}
		records = append(records, f)
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("reading the probe container's log: %v; %s", err, remains)
	}
	if exit != 0 {
		detail := strings.Join(diag, " | ")
		if detail == "" {
			detail = "no diagnostics"
		}
		return 0, fmt.Errorf("probe container exited %d: %s; %s", exit, detail, remains)
	}
	if len(records) == 0 && !sawSummary {
		return 0, fmt.Errorf("probe container wrote no results; %s", remains)
	}
	for _, f := range records {
		if err := out.Emit(f); err != nil {
			return 0, err
		}
	}
	if dropped > 0 {
		if err := out.Note("dropped", strconv.Itoa(dropped)); err != nil {
			return 0, err
		}
	}
	return len(records), nil
}

// jsonRecord is one flat JSON line from the probe container, in the order
// its keys were written. Values that are not strings (the summary's
// counts) are kept as their JSON text.
type jsonRecord []emit.Field

func (r jsonRecord) has(k string) bool {
	_, ok := r.get(k)
	return ok
}

func (r jsonRecord) get(k string) (string, bool) {
	for _, f := range r {
		if f.Key == k {
			return f.Value, true
		}
	}
	return "", false
}

// decodeRecord parses one flat JSON object, keeping key order so a
// forwarded result carries its fields exactly as `net probe` wrote
// them.
func decodeRecord(line []byte) (jsonRecord, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, errors.New("not a JSON object")
	}
	var r jsonRecord
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := kt.(string)
		if !ok {
			return nil, errors.New("non-string key")
		}
		vt, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch v := vt.(type) {
		case string:
			r = append(r, emit.Field{Key: key, Value: v})
		case json.Number:
			r = append(r, emit.Field{Key: key, Value: v.String()})
		default:
			// Nested values are never part of the flat record
			// contract; refusing them keeps a malformed line out.
			return nil, errors.New("non-scalar value")
		}
	}
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return nil, errors.New("unterminated JSON object")
	}
	return r, nil
}

// toFinding rebuilds one net probe result from its record, reporting
// how many fields it dropped and whether it is a result at all. Only
// the envelope fields net probe sets and its glossary keys survive,
// in the order the probe container wrote them; the container's own
// vantage is replaced by this command's stamp.
func toFinding(rec jsonRecord) (emit.Finding, int, bool) {
	kind, _ := rec.get("kind")
	sev, _ := rec.get("severity")
	name, _ := rec.get("name")
	if !allowedKinds[kind] || !emit.ValidSeverity(sev) || name == "" {
		return emit.Finding{}, 0, false
	}
	f := emit.Finding{Kind: kind, Severity: sev, Name: name}
	detail := map[string]bool{}
	for _, o := range probeOutput() {
		detail[o.Name] = true
	}
	dropped := 0
	for _, fld := range rec {
		switch {
		case fld.Key == "kind", fld.Key == "severity", fld.Key == "name", fld.Key == "vantage":
		case fld.Key == "message":
			f.Message = fld.Value
		case detail[fld.Key]:
			if fld.Value != "" {
				f.Details = append(f.Details, fld)
			}
		default:
			dropped++
		}
	}
	return f, dropped, true
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// statusMessage is the API server's own message for a rejected
// request.
func statusMessage(err error) string {
	var st apierrors.APIStatus
	if errors.As(err, &st) && st.Status().Message != "" {
		return st.Status().Message
	}
	return err.Error()
}
