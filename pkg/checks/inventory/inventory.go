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

// Package inventory implements `lookout triage list` (issue #252):
// kubectl get, aggregated — every namespaced kind an incident
// normally involves, in one call, one line per object.
//
// # Why the read surface needs it
//
// Every detail-returning command takes a `<Kind>/<namespace>/<name>`
// target, and until this one nothing produced one. The broad scans
// (`health`, `triage delta`) report what is ABNORMAL and correctly
// name nothing when a namespace is clean, so an agent dropped into an
// unfamiliar namespace had no way to learn what was in it — and the
// faults they could not open a door to were exactly the ones that
// matter most: an absence. A Service whose selector matches nothing
// in front of a perfectly healthy Deployment; a namespace with one
// running pod and no Service at all. Both are invisible to a scan
// that reports only abnormality, and both fall straight out of an
// enumeration.
//
// # Why it withholds judgement
//
// This command's value is that it does NOT diagnose, in a toolset
// built on diagnosing. That is the same scoping discipline the rest
// of the surface already applies: `triage workload` does not answer
// `kubectl get events`, and it does not answer `kubectl get pods`.
// A listing must not report a Service's selector mismatch, because
// that check belongs to `state edges` and `state edges` makes it
// better; if enumeration diagnoses, it duplicates a judgement the
// surface already owns and the caller stops taking the target to the
// tool that owns it. The formatter rule in render.go is what enforces
// this mechanically, and TestItDoesNotDiagnose pins it.
//
// It still satisfies the §4.2 summary-line contract without inventing
// a severity: the summary is the scan's own extent
// (`scanned=47 findings=47 elapsed=… kinds=18`), which is what keeps
// a clean namespace legible rather than ambiguous silence.
package inventory

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// defaultMax bounds one listing. Chosen as a token budget, not a
// cluster size: ~500 lines of this shape is roughly 8k tokens, which
// is a large but affordable read for an agent that then makes a
// second, narrower call. Past it the listing truncates and SAYS SO,
// losing the least important kinds last (defaultKinds is ordered for
// exactly this).
const defaultMax = 500

// pageSize keeps single List responses bounded on large clusters; the
// loop follows Continue tokens until the pass is complete.
const pageSize = 500

func init() {
	checks.Register(New(DefaultDeps()))
}

// Deps supplies the clients the listing reads through. Production
// wiring resolves the kubeconfig lazily on first use so `--help` and
// usage errors never touch cluster credentials; tests inject fakes
// (§13).
type Deps struct {
	// Dynamic lists every kind through one client — eighteen
	// unrelated schemas, two or three fields read from each, is not
	// worth eighteen typed clients.
	Dynamic func() (dynamic.Interface, error)
	// Discovery resolves `--kinds` tokens the built-in table does not
	// know (a CRD, an aggregated API). The default listing never
	// calls it.
	Discovery func() (discovery.DiscoveryInterface, error)
}

// DefaultDeps is the production wiring.
func DefaultDeps() Deps {
	return Deps{
		Dynamic: func() (dynamic.Interface, error) {
			return kube.BuildDynamicClient(kube.Options{})
		},
		Discovery: func() (discovery.DiscoveryInterface, error) {
			c, err := kube.BuildClient(kube.Options{})
			if err != nil {
				return nil, err
			}
			return c.Discovery(), nil
		},
	}
}

// New returns the `triage list` command bound to the given clients.
func New(deps Deps) checks.Command { return newCommand(deps, time.Now) }

// newCommand additionally injects the clock; tests pin it so ages are
// golden-testable.
func newCommand(deps Deps, now func() time.Time) checks.Command {
	l := &lister{deps: deps, now: now}
	return checks.Command{
		Name:    "triage list",
		MCPName: "k8s_list_resources",
		Summary: "List what EXISTS in a namespace — kubectl get across every kind at once, one line per object, leading with the <Kind>/<namespace>/<name> target the other read tools take. The first call for a namespace you have not enumerated: the health scans report only what is abnormal and name nothing when a namespace is clean, so they cannot tell you what is in one. An inventory, not a diagnosis — never guess an object's name, list the namespace.",
		Flags: []emit.FlagSpec{
			{Name: "kinds", Type: emit.FlagString, Default: "",
				Help: "comma-separated resource kinds to list, spelled as kubectl spells them (pods, deploy, certificates.cert-manager.io); " +
					"empty lists the default set — " + strings.Join(defaultKinds, ",") + " — which is every namespaced kind an incident normally involves EXCEPT replicasets (one per Deployment revision; ask for them explicitly)"},
			{Name: "max", Type: emit.FlagInt, Default: strconv.Itoa(defaultMax),
				Help: "stop after this many objects; the summary line reports how many were left out (pass --kinds to narrow instead)"},
		},
		Kinds: []checks.KindField{
			checks.Kind("inventory.object", "one object in scope, rendered as kubectl's default columns for its kind — an aggregated `kubectl get`, so every row is emitted, healthy or not", emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "target", Doc: "the object as <Kind>/<namespace>/<name> (<Kind>/<name> when cluster-scoped) — paste it into triage spec, state edges, triage radius or triage workload unchanged"},
			{Name: "ready", Doc: "ready over desired, as kubectl's READY column: containers for a Pod, replicas for a workload"},
			{Name: "status", Doc: "kubectl's STATUS column verbatim: a Pod's phase or its blocking container reason, a Job's condition, a Node's readiness"},
			{Name: "restarts", Doc: "total container restarts of a Pod"},
			{Name: "up_to_date", Doc: "replicas on the current revision"},
			{Name: "available", Doc: "replicas counted available"},
			{Name: "completions", Doc: "a Job's succeeded over requested completions"},
			{Name: "schedule", Doc: "a CronJob's cron expression"},
			{Name: "timezone", Doc: "a CronJob's spec.timeZone, when it sets one"},
			{Name: "suspend", Doc: "\"true\" on a suspended CronJob (omitted otherwise)"},
			{Name: "active", Doc: "a CronJob's currently running Jobs"},
			{Name: "last_schedule", Doc: "how long ago a CronJob last created a Job"},
			{Name: "type", Doc: "a Service's type or a Secret's type"},
			{Name: "cluster_ip", Doc: "a Service's cluster IP (\"None\" for a headless Service)"},
			{Name: "external_ip", Doc: "a Service's provisioned load-balancer address, or \"pending\" for a LoadBalancer that has none yet"},
			{Name: "ports", Doc: "a Service's ports as port[:nodePort]/protocol"},
			{Name: "addresses", Doc: "how many endpoint addresses an Endpoints object holds (0 means nothing is behind the Service)"},
			{Name: "class", Doc: "an Ingress's ingressClassName or a PVC/PV's storage class"},
			{Name: "hosts", Doc: "an Ingress's rule hosts"},
			{Name: "address", Doc: "an Ingress's provisioned load-balancer address"},
			{Name: "keys", Doc: "how many keys a ConfigMap or Secret holds — Secret VALUES are never read, only counted"},
			{Name: "phase", Doc: "status.phase of a PVC, PV or Namespace"},
			{Name: "volume", Doc: "the PersistentVolume a PVC is bound to"},
			{Name: "capacity", Doc: "a PVC's or PV's storage capacity"},
			{Name: "access_modes", Doc: "a PVC's or PV's access modes, kubectl-abbreviated (RWO, ROX, RWX, RWOP)"},
			{Name: "claim", Doc: "the PVC a PersistentVolume is bound to, as <namespace>/<name>"},
			{Name: "scale_target", Doc: "an HPA's scaleTargetRef as <Kind>/<name>"},
			{Name: "min", Doc: "an HPA's minimum replicas"},
			{Name: "max", Doc: "an HPA's maximum replicas"},
			{Name: "replicas", Doc: "an HPA's current replica count"},
			{Name: "min_available", Doc: "a PDB's spec.minAvailable (count or percentage)"},
			{Name: "max_unavailable", Doc: "a PDB's spec.maxUnavailable (count or percentage)"},
			{Name: "allowed_disruptions", Doc: "how many pods a PDB currently allows to be evicted"},
			{Name: "pod_selector", Doc: "a NetworkPolicy's spec.podSelector; \"all\" when it is empty, which selects every pod in the namespace"},
			{Name: "roles", Doc: "a Node's node-role.kubernetes.io/* labels, or \"none\""},
			{Name: "version", Doc: "a Node's kubelet version"},
			{Name: "age", Doc: "time since metadata.creationTimestamp, kubectl-style (45s, 3h20m, 12d)"},
			{Name: "kinds", Doc: "summary-line note: how many kinds the listing covered"},
			{Name: "truncated", Doc: "summary-line note: how many objects --max left out; they are the LAST kinds of the listing, which is ordered workloads → routing → configuration for this reason"},
			{Name: "skipped", Doc: "summary-line note: kinds that could not be listed and why, as <Kind>:<reason> (forbidden = the caller may not list it, so its absence from the output is a blind spot, not a fact)"},
			{Name: "namespace_absent", Doc: "summary-line note: \"true\" when the listing was empty because the namespace does not exist, which an empty listing alone cannot distinguish from an empty namespace"},
		},
		Examples: []string{
			"lookout triage list --namespace=storefront",
			"lookout triage list --namespace=prod --kinds=pods,services,endpoints",
			"lookout triage list --namespace=prod --kinds=replicasets",
			"lookout triage list -A --kinds=ingresses --format=json",
		},
		Run: l.run,
	}
}

// lister carries one command instance's seams.
type lister struct {
	deps Deps
	now  func() time.Time
}

// dnsLabel is the namespace charset (RFC 1123 label). Validating it
// here turns a typo into one clear usage error instead of an API
// server 400 rendered as a runtime failure — "that namespace name is
// not a DNS label" is a fact a caller can act on; a malfunction is
// something it retries.
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// namespacesGVR backs the "does this namespace exist?" probe below.
var namespacesGVR = gvr("", "v1", "namespaces")

func (l *lister) run(ctx context.Context, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("--workload does not apply: `triage list` enumerates a namespace, not one object — " +
			"scope it with --namespace, then take a target from the output to triage spec / triage workload")
	}
	maxObjects := inv.Flags.Int("max")
	if maxObjects < 1 {
		return 0, emit.UsageErrorf("--max must be at least 1, got %d", maxObjects)
	}
	ns := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		ns = metav1.NamespaceAll
	}
	if ns != "" && (len(ns) > 63 || !dnsLabel.MatchString(ns)) {
		return 0, emit.UsageErrorf("--namespace=%q is not a DNS label, so no namespace can carry that name; nothing was listed", ns)
	}

	tokens := defaultKinds
	if raw := strings.TrimSpace(inv.Flags.String("kinds")); raw != "" {
		tokens = strings.Split(raw, ",")
	}
	kinds, err := resolve(l.deps, tokens)
	if err != nil {
		return 0, err
	}

	dyn, err := l.deps.Dynamic()
	if err != nil {
		return 0, err
	}

	var (
		objects []object
		skipped []string
		listed  int
	)
	for _, k := range kinds {
		items, err := listKind(ctx, dyn, k, ns)
		if err != nil {
			// A refusal is a RESULT, not an error. The caller may be
			// allowed to list sixteen kinds and not the seventeenth
			// (an operator role without secrets is the usual one), and
			// the difference between "there is no Secret here" and "I
			// was not allowed to look" is the difference between a
			// finding and a blind spot — so it is reported, on the one
			// line that cannot be missed, instead of failing the run.
			if reason, tolerable := skipReason(err); tolerable {
				skipped = append(skipped, k.kind+":"+reason)
				continue
			}
			return 0, fmt.Errorf("listing %s: %w", k.gvr.Resource, err)
		}
		listed++
		objects = append(objects, items...)
	}

	if err := inv.Out.Note("kinds", strconv.Itoa(listed)); err != nil {
		return 0, err
	}
	if len(skipped) > 0 {
		if err := inv.Out.Note("skipped", strings.Join(skipped, ",")); err != nil {
			return 0, err
		}
	}

	shown := objects
	if len(shown) > maxObjects {
		shown = shown[:maxObjects]
		if err := inv.Out.Note("truncated", strconv.Itoa(len(objects)-maxObjects)); err != nil {
			return 0, err
		}
	}
	now := l.now()
	for _, o := range shown {
		if err := inv.Out.Emit(o.finding(now)); err != nil {
			return 0, err
		}
	}

	if len(objects) == 0 && ns != "" && l.namespaceAbsent(ctx, dyn, ns) {
		if err := inv.Out.Note("namespace_absent", "true"); err != nil {
			return 0, err
		}
	}
	return len(objects), nil
}

// object is one listed resource, kept as the decoded map because the
// point is to read two or three fields out of eighteen unrelated
// schemas.
type object struct {
	kind      string
	namespace string
	name      string
	created   time.Time
	raw       map[string]any
}

// target is the object as the other read tools accept it, spelled
// exactly as `triage spec` parses it — a cluster-scoped kind takes no
// namespace segment. This is the whole point of the command: the
// output is not a table, it is the input to the next call.
func (o object) target() string {
	if o.namespace == "" {
		return o.kind + "/" + o.name
	}
	return o.kind + "/" + o.namespace + "/" + o.name
}

func (o object) finding(now time.Time) emit.Finding {
	k := &kv{now: now}
	k.add("target", o.target())
	if f := formatters[o.kind]; f != nil {
		f(k, o.raw)
	}
	if !o.created.IsZero() {
		k.add("age", compactAge(now.Sub(o.created)))
	}
	return emit.Finding{
		// One finding kind for every object: this command reports
		// existence, and existence is one class. WHAT exists is
		// kind_of_object's job. There is no Reason and no Message
		// either — a message would be a judgement, and the fields
		// already say everything the line knows.
		Kind: "inventory.object",
		// info is the surface's "not a problem" level (§4.2). It is
		// the neutral value, not a verdict: nothing here is graded.
		Severity:     emit.SeverityInfo,
		Namespace:    o.namespace,
		KindOfObject: o.kind,
		Name:         o.name,
		Details:      k.fields,
	}
}

// listKind pages one kind's List call chain and returns its objects
// sorted by (namespace, name), so output order is fully determined by
// the kind order plus the object names.
func listKind(ctx context.Context, dyn dynamic.Interface, k kindSpec, ns string) ([]object, error) {
	var ri dynamic.ResourceInterface = dyn.Resource(k.gvr)
	if k.namespaced && ns != metav1.NamespaceAll {
		ri = dyn.Resource(k.gvr).Namespace(ns)
	}
	var out []object
	opts := metav1.ListOptions{Limit: pageSize}
	for {
		list, err := ri.List(ctx, opts)
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			out = append(out, newObject(k.kind, &list.Items[i]))
		}
		if opts.Continue = list.GetContinue(); opts.Continue == "" {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].namespace != out[j].namespace {
			return out[i].namespace < out[j].namespace
		}
		return out[i].name < out[j].name
	})
	return out, nil
}

func newObject(kind string, u *unstructured.Unstructured) object {
	// The item's own kind wins when the server set one: a listing of
	// a kind served under several versions still names itself
	// correctly, and the built-in table's kind is only a fallback.
	if k := u.GetKind(); k != "" {
		kind = k
	}
	o := object{
		kind:      kind,
		namespace: u.GetNamespace(),
		name:      u.GetName(),
		raw:       u.Object,
	}
	if ts := u.GetCreationTimestamp(); !ts.IsZero() {
		o.created = ts.Time
	}
	return o
}

// skipReason classifies a per-kind List failure into the ones a
// listing survives (reported as a skipped= note) and the ones that
// end the run.
func skipReason(err error) (string, bool) {
	switch {
	case apierrors.IsForbidden(err):
		return "forbidden", true
	case apierrors.IsNotFound(err):
		// The resource is not served — a CRD uninstalled between
		// discovery and the List, or an aggregated API that is down.
		return "not-served", true
	case apierrors.IsMethodNotSupported(err):
		return "not-listable", true
	}
	return "", false
}

// namespaceAbsent answers the one question an empty listing cannot:
// is this namespace empty, or does it not exist? The two call for
// completely different next moves, and the probe only runs when the
// listing came back with nothing. A caller who may not read
// namespaces gets no note rather than a wrong one.
func (l *lister) namespaceAbsent(ctx context.Context, dyn dynamic.Interface, ns string) bool {
	_, err := dyn.Resource(namespacesGVR).Get(ctx, ns, metav1.GetOptions{})
	return apierrors.IsNotFound(err)
}
