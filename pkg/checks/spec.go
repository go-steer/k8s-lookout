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

package checks

// `triage spec` (DESIGN.md §5): the sanitized, token-dense spec for
// ONE resource — "kubectl describe, but for agents". The fetched
// object passes the §6.5 spec sanitizer (emit.SanitizeUnstructured)
// and is then flattened into the §4.2 finding model: one finding for
// the metadata + kind highlights, one per container, one per
// ABNORMAL status condition (healthy conditions are elided — zero
// nominal state). Raw YAML is never dumped.
//
// Fetching: the §6.1 pod-nexus kinds go through the typed client-go
// clients (specKinds table below); anything else resolves its
// group/version/resource via discovery and reads through the dynamic
// client. `--diff` against the previous graph-history revision is
// deliberately a registered-but-unavailable surface until the
// store-backed diff is implemented (§6.6).

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

func init() {
	Register(SpecCommand(SpecDeps{
		Typed:   func() (kubernetes.Interface, error) { return kube.BuildClient(kube.Options{}) },
		Dynamic: func() (dynamic.Interface, error) { return kube.BuildDynamicClient(kube.Options{}) },
	}))
}

// SpecDeps supplies the Kubernetes clients `triage spec` reads
// through. Production wiring (init above) resolves the kubeconfig
// lazily on first use so `--help` and usage errors never touch
// cluster credentials; tests inject fakes (§13). The typed client
// also serves discovery for the dynamic fallback.
type SpecDeps struct {
	Typed   func() (kubernetes.Interface, error)
	Dynamic func() (dynamic.Interface, error)
}

// specKind is one kind `triage spec` fetches through a typed client,
// with its accepted short names. Lowercased full kind names always
// resolve too (pod, deployment, …); kinds outside this table go
// through discovery + the dynamic client.
type specKind struct {
	kind       string
	aliases    []string
	namespaced bool
	// nominalPhases are the status.phase values elided as healthy
	// (zero nominal state); any other phase is rendered.
	nominalPhases []string
	fetch         func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error)
}

// specKinds is the §6.1 pod-nexus fetch table. Keep the alias set
// small and kubectl-familiar; it is documented verbatim in --help.
var specKinds = []specKind{
	{kind: "Pod", aliases: []string{"po"}, namespaced: true, nominalPhases: []string{"Running", "Succeeded"},
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "Deployment", aliases: []string{"deploy"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "ReplicaSet", aliases: []string{"rs"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.AppsV1().ReplicaSets(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "StatefulSet", aliases: []string{"sts"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "DaemonSet", aliases: []string{"ds"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "Service", aliases: []string{"svc"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "ConfigMap", aliases: []string{"cm"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "Secret", namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "PersistentVolumeClaim", aliases: []string{"pvc"}, namespaced: true, nominalPhases: []string{"Bound"},
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "Ingress", aliases: []string{"ing"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.NetworkingV1().Ingresses(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "NetworkPolicy", aliases: []string{"netpol"}, namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.NetworkingV1().NetworkPolicies(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "EndpointSlice", namespaced: true,
		fetch: func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error) {
			return c.DiscoveryV1().EndpointSlices(ns).Get(ctx, name, metav1.GetOptions{})
		}},
	{kind: "Node", aliases: []string{"no"}, namespaced: false,
		fetch: func(ctx context.Context, c kubernetes.Interface, _, name string) (any, error) {
			return c.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		}},
}

// lookupSpecKind resolves a kind token (any case; alias or full
// name) against the typed table. nil means "not typed — try the
// dynamic path".
func lookupSpecKind(token string) *specKind {
	lower := strings.ToLower(token)
	for i := range specKinds {
		k := &specKinds[i]
		if strings.ToLower(k.kind) == lower {
			return k
		}
		for _, a := range k.aliases {
			if a == lower {
				return k
			}
		}
	}
	return nil
}

// specAliasDoc renders the alias table for --help ("po=Pod, …"),
// generated from specKinds so the doc cannot drift from the code.
func specAliasDoc() string {
	var parts []string
	for _, k := range specKinds {
		for _, a := range k.aliases {
			parts = append(parts, a+"="+k.kind)
		}
	}
	return strings.Join(parts, ", ")
}

// SpecCommand builds the `triage spec` declaration around the given
// clients. The default registry gets the production wiring; tests
// build their own with fakes.
func SpecCommand(deps SpecDeps) Command {
	return Command{
		Name:    "triage spec",
		MCPName: "k8s_resource_spec",
		Summary: "Read ONE resource's spec: kubectl describe, but token-dense, secret-safe, and default-elided — healthy conditions are omitted.",
		Positional: &Positional{
			Meta: "<Kind>/[<namespace>/]<name>",
			Doc: "the resource to read; Kind is case-insensitive, accepts the aliases " + specAliasDoc() +
				", and unlisted kinds (CRDs) resolve via API discovery (qualify as <Kind>.<group> if ambiguous). " +
				"Omit <namespace> for cluster-scoped kinds, or to use --namespace (falling back to \"default\"). " +
				"--workload=<Kind>/<ns>/<name> is the flag-shaped alternative.",
		},
		Flags: []emit.FlagSpec{
			{Name: "diff", Type: emit.FlagBool, Default: "false",
				Help: "diff against the previous graph-history revision — requires a sentinel store; not yet implemented (§6.6)"},
		},
		Kinds: []KindField{
			Kind("spec.resource", "the object itself: metadata, owner, and the kind-specific highlights (one per target)", emit.SeverityInfo),
			Kind("spec.container", "one container of the target: image, resources, ports, probes, env (one per container)", emit.SeverityInfo),
			Kind("spec.condition", "a status condition of the target that is not in its nominal state", emit.SeverityWarning),
		},
		Output: []OutputField{
			{Name: "labels", Doc: "resource labels as sorted k=v pairs"},
			{Name: "owner", Doc: "controlling owner as Kind/name"},
			{Name: "phase", Doc: "status.phase, only when abnormal for the kind (zero nominal state)"},
			{Name: "node", Doc: "node the pod is scheduled on"},
			{Name: "service_account", Doc: "pod's service account"},
			{Name: "volumes", Doc: "pod volumes as name:source (source names its referent, never its payload)"},
			{Name: "container", Doc: "container name (one spec.container finding per container)"},
			{Name: "init", Doc: "\"true\" when the container is an init container"},
			{Name: "image", Doc: "container image reference"},
			{Name: "requests", Doc: "resource requests as sorted k=v pairs"},
			{Name: "limits", Doc: "resource limits as sorted k=v pairs"},
			{Name: "ports", Doc: "container or service ports, compact ([name:]port[->target][/proto])"},
			{Name: "liveness", Doc: "liveness probe one-liner (kind, target, non-default timings)"},
			{Name: "readiness", Doc: "readiness probe one-liner"},
			{Name: "env", Doc: "env vars; literal credential values are [REDACTED], valueFrom entries render as named references"},
			{Name: "env_from", Doc: "envFrom sources as kind:name"},
			{Name: "replicas", Doc: "desired replica count"},
			{Name: "strategy", Doc: "rollout strategy summary (type + non-default knobs)"},
			{Name: "selector", Doc: "workload/service selector as sorted k=v pairs"},
			{Name: "type", Doc: "Service or Secret type, only when non-default"},
			{Name: "external_name", Doc: "ExternalName service target"},
			{Name: "session_affinity", Doc: "service session affinity, only when not None"},
			{Name: "keys", Doc: "ConfigMap/Secret data KEYS with byte sizes — values are never rendered"},
			{Name: "condition", Doc: "abnormal status condition as Type=Status"},
			{Name: "since", Doc: "the condition's lastTransitionTime"},
			{Name: "spec", Doc: "kinds without a dedicated renderer: sanitized spec flattened to path=value pairs"},
		},
		Examples: []string{
			"lookout triage spec Deployment/prod/api",
			"lookout triage spec po/payments-api-7d9c4b-x2n8p --namespace=prod",
			"lookout triage spec Node/gke-prod-pool-1-8f2a",
			"lookout triage spec Certificate/prod/api-tls --format=json",
			"lookout triage spec --workload=Deployment/prod/api",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runSpec(ctx, deps, inv)
		},
	}
}

// SpecFindings renders one already-fetched object into the `triage
// spec` finding stream: sanitize (§6.5), then flatten — no API
// calls. It is the seam `bundle` (§5) composes its spec section
// over. kind must be the canonical kind name (it keys the
// sanitizer's Secret masking); obj is a typed API object or an
// unstructured map.
func SpecFindings(kind, namespace, name string, obj any) ([]emit.Finding, error) {
	u, err := toUnstructured(obj)
	if err != nil {
		return nil, err
	}
	u["kind"] = kind
	t := specTarget{kindToken: kind, typed: lookupSpecKind(kind), namespace: namespace, name: name}
	return specFindings(kind, t, emit.SanitizeUnstructured(u)), nil
}

// specTarget is the resolved resource reference.
type specTarget struct {
	kindToken string    // as given (dynamic resolution needs it)
	typed     *specKind // nil → dynamic fallback
	namespace string    // empty for cluster-scoped
	name      string
	// hasNamespace records whether the reference carried an
	// explicit namespace segment (Kind/ns/name form).
	hasNamespace bool
}

func runSpec(ctx context.Context, deps SpecDeps, inv emit.Invocation) (int, error) {
	if inv.Flags.Bool("diff") {
		return 0, emit.UsageErrorf("--diff is not yet implemented (§6.6)")
	}
	target, err := resolveSpecTarget(inv)
	if err != nil {
		return 0, err
	}
	kind, u, err := fetchSpecObject(ctx, deps, &target)
	if err != nil {
		return 0, err
	}
	sanitized := emit.SanitizeUnstructured(u)
	if err := renderSpec(inv.Out, kind, target, sanitized); err != nil {
		return 0, err
	}
	return 1, nil
}

// resolveSpecTarget merges the positional reference and --workload
// into one target, applying the namespace-defaulting rules.
func resolveSpecTarget(inv emit.Invocation) (specTarget, error) {
	var t specTarget
	w := inv.Scope.Workload
	switch {
	case len(inv.Args) == 1 && !w.IsZero():
		return t, emit.UsageErrorf("give the target either as an argument or via --workload, not both")
	case len(inv.Args) == 1:
		parts := strings.Split(inv.Args[0], "/")
		for _, p := range parts {
			if p == "" {
				return t, emit.UsageErrorf("invalid resource reference %q (want <Kind>/[<namespace>/]<name>)", inv.Args[0])
			}
		}
		switch len(parts) {
		case 2:
			t = specTarget{kindToken: parts[0], name: parts[1]}
		case 3:
			t = specTarget{kindToken: parts[0], namespace: parts[1], name: parts[2], hasNamespace: true}
		default:
			return t, emit.UsageErrorf("invalid resource reference %q (want <Kind>/[<namespace>/]<name>)", inv.Args[0])
		}
	case !w.IsZero():
		t = specTarget{kindToken: w.Kind, namespace: w.Namespace, name: w.Name, hasNamespace: true}
	default:
		return t, emit.UsageErrorf("no target: pass <Kind>/[<namespace>/]<name> or --workload=<Kind>/<ns>/<name>")
	}
	if inv.Scope.AllNamespaces {
		return t, emit.UsageErrorf("-A does not apply: triage spec reads exactly one resource")
	}

	t.typed = lookupSpecKind(t.kindToken)
	if t.typed != nil {
		if !t.typed.namespaced {
			if t.hasNamespace {
				return t, emit.UsageErrorf("%s is cluster-scoped; use %s/<name>", t.typed.kind, t.typed.kind)
			}
			t.namespace = ""
		} else if !t.hasNamespace {
			t.namespace = defaultNamespace(inv.Scope)
		}
	} else if !t.hasNamespace {
		// Dynamic kinds: namespaced-ness is known only after
		// discovery; record the fallback for fetchSpecObject.
		t.namespace = defaultNamespace(inv.Scope)
	}
	return t, nil
}

func defaultNamespace(s emit.Scope) string {
	if s.Namespace != "" {
		return s.Namespace
	}
	return "default"
}

// fetchSpecObject reads the target and returns its canonical kind
// plus the object as an unstructured map, ready for the sanitizer.
func fetchSpecObject(ctx context.Context, deps SpecDeps, t *specTarget) (string, map[string]any, error) {
	if t.typed != nil {
		c, err := deps.Typed()
		if err != nil {
			return "", nil, err
		}
		obj, err := t.typed.fetch(ctx, c, t.namespace, t.name)
		if err != nil {
			return "", nil, fmt.Errorf("fetching %s %s: %w", t.typed.kind, t.qualifiedName(), err)
		}
		u, err := toUnstructured(obj)
		if err != nil {
			return "", nil, err
		}
		// Typed Gets return empty TypeMeta; the sanitizer's
		// Secret masking keys off u["kind"], so it MUST be set
		// before sanitizing.
		u["kind"] = t.typed.kind
		return t.typed.kind, u, nil
	}
	return fetchDynamic(ctx, deps, t)
}

func (t specTarget) qualifiedName() string {
	if t.namespace == "" {
		return t.name
	}
	return t.namespace + "/" + t.name
}

// fetchDynamic resolves an unlisted kind through API discovery and
// reads it with the dynamic client. A "Kind.group" token pins the
// group when the bare kind is served by more than one.
func fetchDynamic(ctx context.Context, deps SpecDeps, t *specTarget) (string, map[string]any, error) {
	c, err := deps.Typed()
	if err != nil {
		return "", nil, err
	}
	kindToken, group := t.kindToken, ""
	if i := strings.IndexByte(kindToken, '.'); i > 0 {
		kindToken, group = kindToken[:i], kindToken[i+1:]
	}
	gvr, kind, namespaced, err := discoverKind(c, kindToken, group)
	if err != nil {
		return "", nil, err
	}
	if !namespaced {
		if t.hasNamespace {
			return "", nil, emit.UsageErrorf("%s is cluster-scoped; use %s/<name>", kind, kind)
		}
		t.namespace = ""
	}
	dyn, err := deps.Dynamic()
	if err != nil {
		return "", nil, err
	}
	var ri dynamic.ResourceInterface = dyn.Resource(gvr)
	if namespaced {
		ri = dyn.Resource(gvr).Namespace(t.namespace)
	}
	obj, err := ri.Get(ctx, t.name, metav1.GetOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("fetching %s %s: %w", kind, t.qualifiedName(), err)
	}
	return kind, obj.Object, nil
}

// discoverKind maps a kind name (case-insensitive, optionally pinned
// to a group) to its preferred-version GVR via discovery.
func discoverKind(c kubernetes.Interface, kindToken, group string) (schema.GroupVersionResource, string, bool, error) {
	var zero schema.GroupVersionResource
	groups, lists, err := c.Discovery().ServerGroupsAndResources()
	if err != nil && len(lists) == 0 {
		return zero, "", false, fmt.Errorf("discovering API resources: %w", err)
	}
	preferred := map[string]string{}
	for _, g := range groups {
		preferred[g.Name] = g.PreferredVersion.GroupVersion
	}
	type match struct {
		gvr        schema.GroupVersionResource
		kind       string
		namespaced bool
	}
	matches := map[string]match{} // by group, preferred version wins
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		if group != "" && gv.Group != group {
			continue
		}
		// Within a group, only the preferred version's resource
		// list is considered (or any version when discovery did
		// not report one, e.g. partial results).
		if pv := preferred[gv.Group]; pv != "" && pv != list.GroupVersion {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") { // subresource
				continue
			}
			if !strings.EqualFold(r.Kind, kindToken) {
				continue
			}
			matches[gv.Group] = match{
				gvr:        gv.WithResource(r.Name),
				kind:       r.Kind,
				namespaced: r.Namespaced,
			}
		}
	}
	switch len(matches) {
	case 0:
		if group != "" {
			return zero, "", false, fmt.Errorf("kind %q not served by group %q on this cluster", kindToken, group)
		}
		return zero, "", false, fmt.Errorf("unknown kind %q: not a known alias and not served by this cluster", kindToken)
	case 1:
		for _, m := range matches {
			return m.gvr, m.kind, m.namespaced, nil
		}
	}
	names := make([]string, 0, len(matches))
	for g := range matches {
		names = append(names, g)
	}
	sort.Strings(names)
	return zero, "", false, emit.UsageErrorf("kind %q is served by multiple groups (%s); qualify as <Kind>.<group>",
		kindToken, strings.Join(names, ", "))
}

// toUnstructured round-trips a typed object through JSON, yielding
// the wire-named map form the sanitizer and renderers work on.
func toUnstructured(obj any) (map[string]any, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("encoding object: %w", err)
	}
	var u map[string]any
	if err := json.Unmarshal(raw, &u); err != nil {
		return nil, fmt.Errorf("object is not a JSON object: %w", err)
	}
	return u, nil
}
