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

package state

// `state gateway`: the Gateway API path, end to end — GatewayClass →
// Gateway → listener → HTTPRoute → Service. It is the read-path
// counterpart to the sentinel's pkg/sources/gateway, and the first
// detector built on the pkg/checks/crd seam: the group is
// discovery-gated, the objects are read dynamically as unstructured,
// and a cluster without the CRDs installed gets one explicit
// crd.unavailable record rather than a clean bill of health.
//
// Finding kinds and severities:
//
//	gateway.missing_class      critical  Gateway names a GatewayClass that does not exist
//	gateway.class_not_accepted critical  the GatewayClass exists but its controller rejected it
//	gateway.not_accepted       critical  Gateway config was rejected — nothing is being programmed
//	gateway.not_programmed     critical  Gateway was accepted but no data plane serves it yet
//	gateway.listener_invalid   warning   one listener is unusable while the Gateway as a whole is not
//	route.missing_parent       critical  HTTPRoute attaches to a Gateway that does not exist
//	route.not_accepted         critical  the Gateway refused the attachment (namespace policy, hostname, …)
//	route.missing_backend      critical  backendRef names a Service that does not exist
//	route.backend_port         critical  backendRef names a port the Service does not expose
//
// Two of these are deliberately status-driven rather than
// recomputed. `route.not_accepted` reads the Accepted condition the
// Gateway controller wrote per parent, instead of re-implementing
// AllowedRoutes namespace and label matching in this process the way
// k8sgpt's httproute analyzer does. The controller is the authority
// on whether it took the route, it accounts for every rejection
// reason at once (NotAllowedByListeners, NoMatchingListenerHostname,
// UnsupportedValue, …), and it cannot drift from the spec version the
// cluster is actually running. The same argument applies to listener
// status.
//
// The backendRef checks go the other way and are recomputed, because
// ResolvedRefs tells you a reference did not resolve without telling
// you which one — and "which Service, which port" is the whole
// answer.
//
// Healthy Gateway plumbing is silent (§4.2 zero nominal state).

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/crd"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(GatewayCommand(Deps{}))
}

// The Gateway API group this check reads. v1 is the GA version;
// gateways, httproutes and gatewayclasses all graduated together, so
// a cluster serving any of them serves the ones it has.
var (
	gatewayGV       = schema.GroupVersion{Group: "gateway.networking.k8s.io", Version: "v1"}
	gatewayGVR      = gatewayGV.WithResource("gateways")
	gatewayClassGVR = gatewayGV.WithResource("gatewayclasses")
	httpRouteGVR    = gatewayGV.WithResource("httproutes")
	gatewayAPIGroup = crd.Group{
		Name:      "Gateway API",
		GV:        gatewayGV,
		Resources: []string{gatewayGVR.Resource, gatewayClassGVR.Resource, httpRouteGVR.Resource},
		Install:   "install the upstream CRDs, or enable a managed implementation (on GKE, the Gateway API add-on)",
	}
)

// GatewayCommand builds the `lookout state gateway` command.
func GatewayCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "state gateway",
		MCPName: "k8s_gateway_routes",
		Summary: "When traffic through the Gateway API does not arrive — walk GatewayClass → Gateway → listener → HTTPRoute → Service and report every hop that is rejected, unprogrammed, or points at something that is not there. Silent, and cheap, on clusters without the Gateway API installed.",
		Output: append([]checks.OutputField{
			{Name: "gateway_class", Doc: "GatewayClass the Gateway names"},
			{Name: "controller", Doc: "the GatewayClass's spec.controllerName — which implementation owns it"},
			{Name: "gateway", Doc: "Gateway the route attaches to, as namespace/name"},
			{Name: "listener", Doc: "listener name within the Gateway"},
			{Name: "port", Doc: "listener port, or the backendRef port the Service does not expose"},
			{Name: "protocol", Doc: "listener protocol"},
			{Name: "condition", Doc: "the status condition that is not True"},
			{Name: "service", Doc: "backend Service the route names, as namespace/name"},
			{Name: "service_ports", Doc: "ports the backend Service does expose, sorted"},
			{Name: "classes", Doc: "GatewayClasses the cluster does have, sorted"},
		}, crd.UnavailableFields()...),
		Examples: []string{
			"lookout state gateway",
			"lookout state gateway --namespace=prod",
			"lookout state gateway --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runGateway(ctx, deps, inv)
		},
	}
}

func runGateway(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("state gateway scans the Gateway API path cluster-wide; scope with --namespace")
	}
	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	avail := deps.resolver(client).Resolve(gatewayAPIGroup)
	if !avail.Any() {
		return crd.EmitUnavailable(inv, avail)
	}
	if err := crd.PartialNote(inv, avail); err != nil {
		return 0, err
	}
	dyn, err := deps.dynamic(ctx)
	if err != nil {
		return 0, err
	}
	// --namespace restricts Gateways and HTTPRoutes; GatewayClasses
	// are cluster-scoped and always listed, because "the class this
	// Gateway names is gone" is the finding.
	ns := inv.Scope.Namespace
	if ns == "" || inv.Scope.AllNamespaces {
		ns = metav1.NamespaceAll
	}
	gix, err := listGatewayIndex(ctx, client, dyn, ns, avail)
	if err != nil {
		return 0, err
	}
	findings := gix.findings()
	sort.SliceStable(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.Kind < b.Kind
	})
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	return gix.scanned, nil
}

// gatewayIndex holds the one List pass `state gateway` joins over.
type gatewayIndex struct {
	scanned int

	classes  map[string]*unstructured.Unstructured // name
	gateways map[string]*unstructured.Unstructured // ns/name
	routes   []*unstructured.Unstructured

	// services is the one typed read: backendRef validation needs the
	// ports a Service exposes, and Services are a built-in kind.
	services map[string]*corev1.Service // ns/name
}

func listGatewayIndex(ctx context.Context, client kubernetes.Interface, dyn dynamic.Interface, ns string, avail crd.Availability) (*gatewayIndex, error) {
	gix := &gatewayIndex{
		classes:  map[string]*unstructured.Unstructured{},
		gateways: map[string]*unstructured.Unstructured{},
		services: map[string]*corev1.Service{},
	}
	if avail.Serves(gatewayClassGVR.Resource) {
		items, err := crd.ListAll(ctx, dyn, "", gatewayClassGVR)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			gix.classes[it.GetName()] = it
			gix.scanned++
		}
	}
	if avail.Serves(gatewayGVR.Resource) {
		items, err := crd.ListAll(ctx, dyn, ns, gatewayGVR)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			gix.gateways[key(it.GetNamespace(), it.GetName())] = it
			gix.scanned++
		}
	}
	if avail.Serves(httpRouteGVR.Resource) {
		items, err := crd.ListAll(ctx, dyn, ns, httpRouteGVR)
		if err != nil {
			return nil, err
		}
		gix.routes = items
		gix.scanned += len(items)
	}
	// Services are listed only when there are routes to validate: on
	// a cluster with the CRDs installed but nothing using them, this
	// check should cost three empty Lists and stop.
	if len(gix.routes) > 0 {
		err := listPages("services", func(o metav1.ListOptions) ([]corev1.Service, string, error) {
			l, err := client.CoreV1().Services(ns).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(s *corev1.Service) { gix.services[key(s.Namespace, s.Name)] = s; gix.scanned++ })
		if err != nil {
			return nil, err
		}
	}
	return gix, nil
}

func (gix *gatewayIndex) findings() []emit.Finding {
	var out []emit.Finding
	for _, gw := range sortedValues(gix.gateways) {
		out = append(out, gix.gatewayFindings(gw)...)
	}
	for _, r := range gix.routes {
		out = append(out, gix.routeFindings(r)...)
	}
	return out
}

// gatewayFindings reports one Gateway's class reference and its own
// programming status.
//
// Only one of missing_class / class_not_accepted / not_accepted /
// not_programmed is emitted, most-specific first. They are a chain,
// not independent facts: a Gateway whose class does not exist is also
// unaccepted and also unprogrammed, and saying so three times buries
// the one line that names the cause. Listener findings are separate
// and additive — a Gateway can be programmed with one bad listener,
// which is precisely the case worth reporting on its own.
func (gix *gatewayIndex) gatewayFindings(gw *unstructured.Unstructured) []emit.Finding {
	ns, name := gw.GetNamespace(), gw.GetName()
	className := crd.Str(gw.Object, "spec", "gatewayClassName")
	conds := crd.Conditions(gw.Object, "status", "conditions")

	// Every claim this closure builds is critical: each one means
	// the gateway carries no traffic at all.
	base := func(kind, reason, message string, extra ...emit.Field) emit.Finding {
		return emit.Finding{
			Kind:         kind,
			Severity:     emit.SeverityCritical,
			Namespace:    ns,
			KindOfObject: "Gateway",
			Name:         name,
			Reason:       reason,
			Message:      message,
			Details:      append([]emit.Field{{Key: "gateway_class", Value: className}}, extra...),
		}
	}

	gc := gix.classes[className]
	switch {
	case className == "":
		// gatewayClassName is required by the schema; an empty one
		// means we are looking at something the API server would have
		// rejected. Nothing useful to say.
	case gc == nil:
		have := sortedKeys(gix.classes)
		return []emit.Finding{base("gateway.missing_class", "MissingGatewayClass",
			fmt.Sprintf("gateway names gatewayclass %q, which does not exist — no controller claims this gateway, so no address is assigned and no route attaches; the cluster has %s",
				className, gatewayClassList(have)),
			emit.Field{Key: "classes", Value: strings.Join(have, ",")})}
	default:
		if c, ok := crd.FindCondition(crd.Conditions(gc.Object, "status", "conditions"), "Accepted"); ok && !c.True() {
			return []emit.Finding{base("gateway.class_not_accepted", "GatewayClassNotAccepted",
				fmt.Sprintf("gatewayclass %q is not accepted by its controller (%s: %s) — every gateway of that class is inert",
					className, condReason(c), c.Message),
				emit.Field{Key: "controller", Value: crd.Str(gc.Object, "spec", "controllerName")},
				emit.Field{Key: "condition", Value: "Accepted=" + c.Status})}
		}
	}

	if c, ok := crd.FindCondition(conds, "Accepted"); ok && !c.True() {
		return []emit.Finding{base("gateway.not_accepted", "GatewayNotAccepted",
			fmt.Sprintf("gateway config was rejected by its controller (%s: %s) — nothing is programmed and no route attaches",
				condReason(c), c.Message),
			emit.Field{Key: "condition", Value: "Accepted=" + c.Status})}
	}
	if c, ok := crd.FindCondition(conds, "Programmed"); ok && !c.True() {
		return []emit.Finding{base("gateway.not_programmed", "GatewayNotProgrammed",
			fmt.Sprintf("gateway is accepted but not programmed (%s: %s) — the config is valid and no data plane serves it, so traffic reaches nothing",
				condReason(c), c.Message),
			emit.Field{Key: "condition", Value: "Programmed=" + c.Status})}
	}
	return gix.listenerFindings(gw, className)
}

// listenerFindings reports listeners the controller marked unusable
// on an otherwise healthy Gateway: a port conflict with another
// Gateway, a TLS secret that does not resolve, a protocol the
// implementation does not support. Warning, not critical — the
// Gateway serves its other listeners, so this is one door shut rather
// than the building closed.
func (gix *gatewayIndex) listenerFindings(gw *unstructured.Unstructured, className string) []emit.Finding {
	var out []emit.Finding
	for _, ls := range crd.Slice(gw.Object, "status", "listeners") {
		lname := crd.Str(ls, "name")
		conds := crd.Conditions(ls, "conditions")
		bad, ok := firstBadListenerCondition(conds)
		if !ok {
			continue
		}
		port, protocol := listenerSpec(gw, lname)
		out = append(out, emit.Finding{
			Kind:         "gateway.listener_invalid",
			Severity:     emit.SeverityWarning,
			Namespace:    gw.GetNamespace(),
			KindOfObject: "Gateway",
			Name:         gw.GetName(),
			Reason:       "ListenerInvalid",
			Message: fmt.Sprintf("listener %q is not serving (%s=%s, %s: %s) — the gateway's other listeners are unaffected",
				lname, bad.Type, bad.Status, condReason(bad), bad.Message),
			Details: []emit.Field{
				{Key: "gateway_class", Value: className},
				{Key: "listener", Value: lname},
				{Key: "port", Value: port},
				{Key: "protocol", Value: protocol},
				{Key: "condition", Value: bad.Type + "=" + bad.Status},
			},
		})
	}
	return out
}

// listenerConditionsPositive are the listener conditions where True
// is healthy, in report priority: Accepted first because it explains
// the others, Conflicted last because it is inverted and handled
// separately.
var listenerConditionsPositive = []string{"Accepted", "ResolvedRefs", "Programmed"}

// firstBadListenerCondition picks the one condition worth reporting
// for a listener. An absent condition is not a failure: a controller
// that has not written Programmed yet has not refused anything.
func firstBadListenerCondition(conds []crd.Condition) (crd.Condition, bool) {
	if c, ok := crd.FindCondition(conds, "Conflicted"); ok && c.True() {
		return c, true // inverted: True means another listener won the port
	}
	for _, want := range listenerConditionsPositive {
		if c, ok := crd.FindCondition(conds, want); ok && !c.True() {
			return c, true
		}
	}
	return crd.Condition{}, false
}

// listenerSpec finds the named listener in the Gateway spec and
// returns its port and protocol as strings, for the finding details.
func listenerSpec(gw *unstructured.Unstructured, name string) (port, protocol string) {
	for _, ls := range crd.Slice(gw.Object, "spec", "listeners") {
		if crd.Str(ls, "name") != name {
			continue
		}
		if p, ok := crd.Int(ls, "port"); ok {
			port = strconv.FormatInt(p, 10)
		}
		return port, crd.Str(ls, "protocol")
	}
	return "", ""
}

// routeFindings reports one HTTPRoute's parent attachments and its
// backend references.
func (gix *gatewayIndex) routeFindings(route *unstructured.Unstructured) []emit.Finding {
	out := gix.parentFindings(route)
	out = append(out, gix.backendFindings(route)...)
	return out
}

// parentFindings reports the Gateways a route wanted to attach to and
// did not: the Gateway is absent, or it is there and refused.
func (gix *gatewayIndex) parentFindings(route *unstructured.Unstructured) []emit.Finding {
	ns, name := route.GetNamespace(), route.GetName()
	// Status is written per parent; index it so the spec walk can ask
	// "what did this parent say about me".
	accepted := map[string]crd.Condition{}
	for _, ps := range crd.Slice(route.Object, "status", "parents") {
		pns, pname := parentRefTarget(crd.Map(ps, "parentRef"), ns)
		if c, ok := crd.FindCondition(crd.Conditions(ps, "conditions"), "Accepted"); ok {
			accepted[key(pns, pname)] = c
		}
	}
	var out []emit.Finding
	for _, pr := range crd.Slice(route.Object, "spec", "parentRefs") {
		if kind := crd.Str(pr, "kind"); kind != "" && kind != "Gateway" {
			continue // a mesh Service parent, or another implementation's kind
		}
		pns, pname := parentRefTarget(pr, ns)
		target := key(pns, pname)
		if gix.gateways[target] == nil {
			// Only claim absence when the Gateway would be in scope:
			// under --namespace, a parent in another namespace was
			// never listed and its absence here means nothing.
			if !gix.namespaceWasScanned(pns) {
				continue
			}
			out = append(out, emit.Finding{
				Kind:         "route.missing_parent",
				Severity:     emit.SeverityCritical,
				Namespace:    ns,
				KindOfObject: "HTTPRoute",
				Name:         name,
				Reason:       "MissingGateway",
				Message: fmt.Sprintf("route attaches to gateway %s, which does not exist — the route is inert and serves no traffic",
					target),
				Details: []emit.Field{{Key: "gateway", Value: target}},
			})
			continue
		}
		if c, ok := accepted[target]; ok && !c.True() {
			out = append(out, emit.Finding{
				Kind:         "route.not_accepted",
				Severity:     emit.SeverityCritical,
				Namespace:    ns,
				KindOfObject: "HTTPRoute",
				Name:         name,
				Reason:       "RouteNotAccepted",
				Message: fmt.Sprintf("gateway %s refused the attachment (%s: %s) — the route exists and carries no traffic",
					target, condReason(c), c.Message),
				Details: []emit.Field{
					{Key: "gateway", Value: target},
					{Key: "condition", Value: "Accepted=" + c.Status},
				},
			})
		}
	}
	return out
}

// namespaceWasScanned reports whether a namespace was covered by this
// scan. Under -A or an unscoped run everything is; under --namespace
// only that one is, and a cross-namespace parentRef must not be
// reported missing on the strength of not having been listed.
func (gix *gatewayIndex) namespaceWasScanned(ns string) bool {
	for k := range gix.gateways {
		if strings.HasPrefix(k, ns+"/") {
			return true
		}
	}
	// No Gateway from that namespace was seen. That is ambiguous
	// between "out of scope" and "genuinely none", so fall back to
	// the routes' own namespaces: a route can only be scanned if its
	// namespace was in scope, and a parentRef into the same namespace
	// is therefore answerable.
	for _, r := range gix.routes {
		if r.GetNamespace() == ns {
			return true
		}
	}
	return false
}

// parentRefTarget resolves a parentRef to namespace/name, defaulting
// the namespace to the route's own the way the Gateway API does.
func parentRefTarget(ref map[string]any, routeNS string) (ns, name string) {
	ns = crd.Str(ref, "namespace")
	if ns == "" {
		ns = routeNS
	}
	return ns, crd.Str(ref, "name")
}

// backendFindings reports backendRefs that name a Service which does
// not exist, or a port it does not expose. Both are recomputed rather
// than read off ResolvedRefs: the condition says a reference failed
// to resolve, and the operator needs to know which one.
//
// Deduped per (service, port) across rules: a route with ten rules
// pointing at one dead backend is one problem, not ten.
func (gix *gatewayIndex) backendFindings(route *unstructured.Unstructured) []emit.Finding {
	ns, name := route.GetNamespace(), route.GetName()
	seen := map[string]bool{}
	var out []emit.Finding
	for _, rule := range crd.Slice(route.Object, "spec", "rules") {
		for _, ref := range crd.Slice(rule, "backendRefs") {
			if kind := crd.Str(ref, "kind"); kind != "" && kind != "Service" {
				continue // an implementation-specific backend we cannot resolve
			}
			bns := crd.Str(ref, "namespace")
			if bns == "" {
				bns = ns
			}
			bname := crd.Str(ref, "name")
			if bname == "" {
				continue
			}
			target := key(bns, bname)
			port, hasPort := crd.Int(ref, "port")
			dedup := target + "#" + strconv.FormatInt(port, 10)
			if seen[dedup] {
				continue
			}
			seen[dedup] = true

			svc := gix.services[target]
			if svc == nil {
				if !gix.namespaceWasScanned(bns) {
					continue
				}
				out = append(out, emit.Finding{
					Kind:         "route.missing_backend",
					Severity:     emit.SeverityCritical,
					Namespace:    ns,
					KindOfObject: "HTTPRoute",
					Name:         name,
					Reason:       "MissingBackendService",
					Message: fmt.Sprintf("route sends traffic to service %s, which does not exist — requests matching this rule get a 500 from the gateway",
						target),
					Details: []emit.Field{{Key: "service", Value: target}},
				})
				continue
			}
			if !hasPort {
				continue // port is optional on a backendRef
			}
			if servicePortExposed(svc, port) {
				continue
			}
			out = append(out, emit.Finding{
				Kind:         "route.backend_port",
				Severity:     emit.SeverityCritical,
				Namespace:    ns,
				KindOfObject: "HTTPRoute",
				Name:         name,
				Reason:       "BackendPortNotExposed",
				Message: fmt.Sprintf("route sends traffic to service %s on port %d, which that service does not expose (it has %s) — the reference never resolves to an endpoint",
					target, port, servicePortList(svc)),
				Details: []emit.Field{
					{Key: "service", Value: target},
					{Key: "port", Value: strconv.FormatInt(port, 10)},
					{Key: "service_ports", Value: servicePortList(svc)},
				},
			})
		}
	}
	return out
}

func servicePortExposed(svc *corev1.Service, port int64) bool {
	for _, p := range svc.Spec.Ports {
		if int64(p.Port) == port {
			return true
		}
	}
	return false
}

func servicePortList(svc *corev1.Service) string {
	ports := make([]string, 0, len(svc.Spec.Ports))
	for _, p := range svc.Spec.Ports {
		ports = append(ports, strconv.FormatInt(int64(p.Port), 10))
	}
	if len(ports) == 0 {
		return "no ports"
	}
	sort.Strings(ports)
	return strings.Join(ports, ",")
}

// condReason renders a condition's reason, falling back to its type
// when a controller left it empty.
func condReason(c crd.Condition) string {
	if c.Reason == "" {
		return c.Type
	}
	return c.Reason
}

// gatewayClassList renders the "the cluster has …" message tail.
func gatewayClassList(names []string) string {
	if len(names) == 0 {
		return "no gatewayclasses at all"
	}
	return "gatewayclass(es) " + strings.Join(names, ",")
}

// sortedValues returns a map's values ordered by key, so a scan over
// a map produces stable output before the final sort.
func sortedValues(m map[string]*unstructured.Unstructured) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, 0, len(m))
	for _, k := range sortedKeys(m) {
		out = append(out, m[k])
	}
	return out
}
