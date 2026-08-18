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

package state_test

// `state gateway` tests. Two things are under test here that the other
// state checks do not have: the CRD gate (a cluster with no Gateway
// API says so instead of passing), and status-driven claims read from
// conditions a controller wrote rather than recomputed locally.
//
// The absent-condition cases matter most. A Gateway whose controller
// has not reconciled it yet has no conditions at all, and that is a
// cluster starting up, not a broken one. All helpers are gw-prefixed.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const gwAPIVersion = "gateway.networking.k8s.io/v1"

var gwGV = schema.GroupVersion{Group: "gateway.networking.k8s.io", Version: "v1"}

var gwListKinds = map[schema.GroupVersionResource]string{
	gwGV.WithResource("gateways"):       "GatewayList",
	gwGV.WithResource("gatewayclasses"): "GatewayClassList",
	gwGV.WithResource("httproutes"):     "HTTPRouteList",
}

// gwGVR maps a Gateway API kind to its resource. It is spelled out
// rather than guessed because the dynamic fake's guesser gets
// "Gateway" wrong — its heuristic turns a trailing "y" into "ies", so
// seeded gateways would land under "gatewaies" and every List would
// come back empty.
var gwGVR = map[string]schema.GroupVersionResource{
	"Gateway":      gwGV.WithResource("gateways"),
	"GatewayClass": gwGV.WithResource("gatewayclasses"),
	"HTTPRoute":    gwGV.WithResource("httproutes"),
}

// gwCommand wires `state gateway` over a fake cluster: served names
// the Gateway API resources discovery advertises (nil means all
// three), crdObjs are the unstructured Gateway API objects, and core
// are the built-in objects (Services).
func gwCommand(t *testing.T, served []string, crdObjs []*unstructured.Unstructured, core ...runtime.Object) checks.Command {
	t.Helper()
	cs := fake.NewClientset(core...)
	if served == nil {
		served = []string{"gateways", "gatewayclasses", "httproutes"}
	}
	if len(served) > 0 {
		list := &metav1.APIResourceList{GroupVersion: gwAPIVersion}
		for _, r := range served {
			list.APIResources = append(list.APIResources, metav1.APIResource{Name: r})
		}
		cs.Resources = []*metav1.APIResourceList{list}
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gwListKinds)
	for _, obj := range crdObjs {
		gvr, ok := gwGVR[obj.GetKind()]
		if !ok {
			t.Fatalf("no GVR for kind %q", obj.GetKind())
		}
		if err := dyn.Tracker().Create(gvr, obj, obj.GetNamespace()); err != nil {
			t.Fatalf("seed %s %s: %v", obj.GetKind(), obj.GetName(), err)
		}
	}
	return state.GatewayCommand(state.Deps{
		Client:  func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Dynamic: func(context.Context) (dynamic.Interface, error) { return dyn, nil },
		Now:     func() time.Time { return fixedNow },
	})
}

// gwCond builds one metav1.Condition entry.
func gwCond(typ, status, reason, message string) any {
	return map[string]any{"type": typ, "status": status, "reason": reason, "message": message}
}

// gwClass is a GatewayClass; conds may be empty for one the controller
// has not looked at yet.
func gwClass(name, controller string, conds ...any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gwAPIVersion,
		"kind":       "GatewayClass",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"controllerName": controller},
	}}
	if len(conds) > 0 {
		u.Object["status"] = map[string]any{"conditions": conds}
	}
	return u
}

// gwAcceptedClass is the ordinary case: a class its controller took.
func gwAcceptedClass(name string) *unstructured.Unstructured {
	return gwClass(name, "example.net/gateway-controller", gwCond("Accepted", "True", "Accepted", "the controller owns this class"))
}

// gwListener is one spec listener.
func gwListener(name, protocol string, port int64) any {
	return map[string]any{"name": name, "protocol": protocol, "port": port}
}

// gwListenerStatus is one status listener with its conditions.
func gwListenerStatus(name string, conds ...any) any {
	return map[string]any{"name": name, "conditions": conds}
}

// gwGateway is a Gateway with the given spec listeners; status is
// applied verbatim so a test can leave it out entirely.
func gwGateway(ns, name, class string, listeners []any, status map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gwAPIVersion,
		"kind":       "Gateway",
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec":       map[string]any{"gatewayClassName": class, "listeners": listeners},
	}}
	if status != nil {
		u.Object["status"] = status
	}
	return u
}

// gwHealthyGateway is an accepted, programmed Gateway with one working
// listener.
func gwHealthyGateway(ns, name, class string) *unstructured.Unstructured {
	return gwGateway(ns, name, class,
		[]any{gwListener("http", "HTTP", 80)},
		map[string]any{
			"conditions": []any{
				gwCond("Accepted", "True", "Accepted", "gateway accepted"),
				gwCond("Programmed", "True", "Programmed", "address assigned"),
			},
			"listeners": []any{gwListenerStatus("http",
				gwCond("Accepted", "True", "Accepted", ""),
				gwCond("ResolvedRefs", "True", "ResolvedRefs", ""),
				gwCond("Programmed", "True", "Programmed", ""),
			)},
		})
}

// gwParent is one spec parentRef; ns "" means the route's own.
func gwParent(ns, name string) any {
	ref := map[string]any{"name": name}
	if ns != "" {
		ref["namespace"] = ns
	}
	return ref
}

// gwBackend is one backendRef; port 0 means unset.
func gwBackend(ns, name string, port int64) any {
	ref := map[string]any{"name": name}
	if ns != "" {
		ref["namespace"] = ns
	}
	if port != 0 {
		ref["port"] = port
	}
	return ref
}

// gwParentStatus is one status.parents entry.
func gwParentStatus(ns, name string, conds ...any) any {
	return map[string]any{"parentRef": gwParent(ns, name), "conditions": conds}
}

// gwAcceptedParent is the ordinary case: the Gateway took the route.
func gwAcceptedParent(ns, name string) any {
	return gwParentStatus(ns, name, gwCond("Accepted", "True", "Accepted", "route attached"))
}

// gwRoute is an HTTPRoute with one rule carrying the given backends.
func gwRoute(ns, name string, parents []any, backends []any, parentStatus []any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gwAPIVersion,
		"kind":       "HTTPRoute",
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec": map[string]any{
			"parentRefs": parents,
			"rules":      []any{map[string]any{"backendRefs": backends}},
		},
	}}
	if parentStatus != nil {
		u.Object["status"] = map[string]any{"parents": parentStatus}
	}
	return u
}

// gwService is a Service exposing the given ports.
func gwService(ns, name string, ports ...int32) *corev1.Service {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	for _, p := range ports {
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{Port: p})
	}
	return svc
}

// gwFindings runs `state gateway` and returns the finding lines
// (summary stripped), failing on non-zero exit.
func gwFindings(t *testing.T, cmd checks.Command, args ...string) []string {
	t.Helper()
	res := checktest.Run(t, cmd, args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
		t.Fatalf("no summary line: %q", res.Stdout)
	}
	return lines[:len(lines)-1]
}

// gwHealthy is a working Gateway API install: an accepted class, a
// programmed gateway, a route the gateway took, and a backend that
// exposes the port the route sends to.
func gwHealthyCluster() (crdObjs []*unstructured.Unstructured, core []runtime.Object) {
	return []*unstructured.Unstructured{
			gwAcceptedClass("external"),
			gwHealthyGateway(ns, "edge", "external"),
			gwRoute(ns, "api", []any{gwParent("", "edge")}, []any{gwBackend("", "api", 8080)},
				[]any{gwAcceptedParent("", "edge")}),
		}, []runtime.Object{
			gwService(ns, "api", 8080),
		}
}

// The whole point of §4.2 zero nominal state: working plumbing emits
// nothing, so a non-empty result is always worth reading.
func TestGatewayHealthyIsSilent(t *testing.T) {
	crdObjs, core := gwHealthyCluster()
	res := checktest.Run(t, gwCommand(t, nil, crdObjs, core...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if got := strings.TrimSpace(res.Stdout); !strings.HasPrefix(got, "scanned=4 findings=0 ") {
		t.Errorf("healthy cluster should emit only a summary: %q", res.Stdout)
	}
}

// The gate: a cluster that never installed the Gateway API gets an
// explicit record of what was not examined, not a clean bill of
// health. Nothing was scanned, so scanned=0.
func TestGatewayNotInstalled(t *testing.T) {
	res := checktest.Run(t, gwCommand(t, []string{}, nil))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d — an absent CRD is a degradation, not an error; stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want one finding and a summary, got %q", res.Stdout)
	}
	want := `kind=crd.unavailable severity=info reason=APIGroupNotServed message="Gateway API is not installed: the gateway.networking.k8s.io/v1 API group is not served by this cluster — install the upstream CRDs, or enable a managed implementation (on GKE, the Gateway API add-on)" api_group=gateway.networking.k8s.io/v1 resources=gateways,gatewayclasses,httproutes`
	if lines[0] != want {
		t.Errorf("finding\n got: %s\nwant: %s", lines[0], want)
	}
	if !strings.HasPrefix(lines[1], "scanned=0 findings=1 ") {
		t.Errorf("nothing was examined: %s", lines[1])
	}
}

// A partial install still answers, and says what it could not read
// rather than quietly narrowing coverage (§11).
func TestGatewayPartiallyServed(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwHealthyGateway(ns, "edge", "external"),
	}
	res := checktest.Run(t, gwCommand(t, []string{"gateways", "gatewayclasses"}, crdObjs))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "not_served=httproutes") {
		t.Errorf("summary should name what could not be read: %q", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "scanned=2 ") {
		t.Errorf("the served resources should still be scanned: %q", res.Stdout)
	}
}

func TestGatewayMissingClass(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("internal"),
		gwGateway(ns, "edge", "external", []any{gwListener("http", "HTTP", 80)}, nil),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=gateway.missing_class severity=critical namespace=prod kind_of_object=Gateway name=edge reason=MissingGatewayClass message="gateway names gatewayclass \"external\", which does not exist — no controller claims this gateway, so no address is assigned and no route attaches; the cluster has gatewayclass(es) internal" gateway_class=external classes=internal`,
	})
}

// Naming what does exist is what turns the finding into a fix; with
// nothing installed the message says so rather than printing an empty
// list.
func TestGatewayMissingClassNamesWhatExists(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{gwGateway(ns, "edge", "external", nil, nil)}
	got := gwFindings(t, gwCommand(t, nil, crdObjs))
	if len(got) != 1 {
		t.Fatalf("got %d findings, want 1: %v", len(got), got)
	}
	if !strings.Contains(got[0], "the cluster has no gatewayclasses at all") {
		t.Errorf("message should say the cluster has none: %s", got[0])
	}
}

func TestGatewayClassNotAccepted(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwClass("external", "example.net/gateway-controller",
			gwCond("Accepted", "False", "InvalidParameters", "parametersRef points at a missing ConfigMap")),
		gwGateway(ns, "edge", "external", []any{gwListener("http", "HTTP", 80)}, nil),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=gateway.class_not_accepted severity=critical namespace=prod kind_of_object=Gateway name=edge reason=GatewayClassNotAccepted message="gatewayclass \"external\" is not accepted by its controller (InvalidParameters: parametersRef points at a missing ConfigMap) — every gateway of that class is inert" gateway_class=external controller=example.net/gateway-controller condition="Accepted=False"`,
	})
}

func TestGatewayNotAccepted(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwGateway(ns, "edge", "external", []any{gwListener("http", "HTTP", 80)},
			map[string]any{"conditions": []any{
				gwCond("Accepted", "False", "UnsupportedProtocol", "listener http uses an unsupported protocol"),
			}}),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=gateway.not_accepted severity=critical namespace=prod kind_of_object=Gateway name=edge reason=GatewayNotAccepted message="gateway config was rejected by its controller (UnsupportedProtocol: listener http uses an unsupported protocol) — nothing is programmed and no route attaches" gateway_class=external condition="Accepted=False"`,
	})
}

// Accepted but not Programmed is the load-balancer-still-provisioning
// shape, and the one operators most often mistake for a working
// gateway: the config is valid, and nothing serves it.
func TestGatewayNotProgrammed(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwGateway(ns, "edge", "external", []any{gwListener("http", "HTTP", 80)},
			map[string]any{"conditions": []any{
				gwCond("Accepted", "True", "Accepted", ""),
				gwCond("Programmed", "False", "Pending", "waiting for a load balancer address"),
			}}),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=gateway.not_programmed severity=critical namespace=prod kind_of_object=Gateway name=edge reason=GatewayNotProgrammed message="gateway is accepted but not programmed (Pending: waiting for a load balancer address) — the config is valid and no data plane serves it, so traffic reaches nothing" gateway_class=external condition="Programmed=False"`,
	})
}

// The chain claims are exclusive, most-specific first. A gateway whose
// class does not exist is also unaccepted and also unprogrammed;
// saying so three times buries the one line that names the cause.
func TestGatewayReportsOneCauseNotThree(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwGateway(ns, "edge", "gone", []any{gwListener("http", "HTTP", 80)},
			map[string]any{"conditions": []any{
				gwCond("Accepted", "False", "InvalidGatewayClass", "no such class"),
				gwCond("Programmed", "False", "Invalid", "not programmed"),
			}}),
	}
	got := gwFindings(t, gwCommand(t, nil, crdObjs))
	if len(got) != 1 {
		t.Fatalf("got %d findings, want just the root cause: %v", len(got), got)
	}
	if !strings.Contains(got[0], "kind=gateway.missing_class") {
		t.Errorf("want the most specific claim, got: %s", got[0])
	}
}

// A gateway no controller has touched yet has no conditions at all.
// Absent is not False — that is a cluster starting up, not a defect.
func TestGatewayUnreconciledIsSilent(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwGateway(ns, "edge", "external", []any{gwListener("http", "HTTP", 80)}, nil),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), nil)
}

// An unreconciled GatewayClass is likewise not a rejected one.
func TestGatewayUnreconciledClassIsSilent(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwClass("external", "example.net/gateway-controller"),
		gwHealthyGateway(ns, "edge", "external"),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), nil)
}

// One bad listener on a working gateway is a warning, not a critical:
// the other listeners serve. This is also the only additive claim —
// it rides alongside a gateway that is otherwise fine.
func TestGatewayListenerInvalid(t *testing.T) {
	gw := gwGateway(ns, "edge", "external",
		[]any{gwListener("http", "HTTP", 80), gwListener("https", "HTTPS", 443)},
		map[string]any{
			"conditions": []any{
				gwCond("Accepted", "True", "Accepted", ""),
				gwCond("Programmed", "True", "Programmed", ""),
			},
			"listeners": []any{
				gwListenerStatus("http", gwCond("Accepted", "True", "Accepted", ""), gwCond("Programmed", "True", "Programmed", "")),
				gwListenerStatus("https",
					gwCond("Accepted", "True", "Accepted", ""),
					gwCond("ResolvedRefs", "False", "InvalidCertificateRef", "secret prod/tls-cert not found"),
				),
			},
		})
	crdObjs := []*unstructured.Unstructured{gwAcceptedClass("external"), gw}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=gateway.listener_invalid severity=warning namespace=prod kind_of_object=Gateway name=edge reason=ListenerInvalid message="listener \"https\" is not serving (ResolvedRefs=False, InvalidCertificateRef: secret prod/tls-cert not found) — the gateway's other listeners are unaffected" gateway_class=external listener=https port=443 protocol=HTTPS condition="ResolvedRefs=False"`,
	})
}

// Conflicted is the one inverted listener condition: True means
// another gateway won the port.
func TestGatewayListenerConflicted(t *testing.T) {
	gw := gwGateway(ns, "edge", "external",
		[]any{gwListener("http", "HTTP", 80)},
		map[string]any{
			"conditions": []any{gwCond("Accepted", "True", "Accepted", ""), gwCond("Programmed", "True", "Programmed", "")},
			"listeners": []any{gwListenerStatus("http",
				gwCond("Accepted", "True", "Accepted", ""),
				gwCond("Conflicted", "True", "HostnameConflict", "another listener already claims port 80"),
			)},
		})
	got := gwFindings(t, gwCommand(t, nil, []*unstructured.Unstructured{gwAcceptedClass("external"), gw}))
	if len(got) != 1 || !strings.Contains(got[0], `condition="Conflicted=True"`) {
		t.Fatalf("want one Conflicted finding, got: %v", got)
	}
}

// A listener with no conditions written yet is not a broken listener.
func TestGatewayListenerUnreconciledIsSilent(t *testing.T) {
	gw := gwGateway(ns, "edge", "external",
		[]any{gwListener("http", "HTTP", 80)},
		map[string]any{
			"conditions": []any{gwCond("Accepted", "True", "Accepted", ""), gwCond("Programmed", "True", "Programmed", "")},
			"listeners":  []any{gwListenerStatus("http")},
		})
	wantFindings(t, gwFindings(t, gwCommand(t, nil, []*unstructured.Unstructured{gwAcceptedClass("external"), gw})), nil)
}

func TestRouteMissingParent(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwHealthyGateway(ns, "edge", "external"),
		gwRoute(ns, "api", []any{gwParent("", "gone")}, nil, nil),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=route.missing_parent severity=critical namespace=prod kind_of_object=HTTPRoute name=api reason=MissingGateway message="route attaches to gateway prod/gone, which does not exist — the route is inert and serves no traffic" gateway=prod/gone`,
	})
}

// The Accepted condition the gateway wrote is authoritative about
// whether it took the route — the controller accounts for every
// rejection reason at once, and cannot drift from the spec version the
// cluster is running.
func TestRouteNotAccepted(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwHealthyGateway(ns, "edge", "external"),
		gwRoute(ns, "api", []any{gwParent("", "edge")}, nil,
			[]any{gwParentStatus("", "edge",
				gwCond("Accepted", "False", "NotAllowedByListeners", "no listener allows routes from namespace prod"))}),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=route.not_accepted severity=critical namespace=prod kind_of_object=HTTPRoute name=api reason=RouteNotAccepted message="gateway prod/edge refused the attachment (NotAllowedByListeners: no listener allows routes from namespace prod) — the route exists and carries no traffic" gateway=prod/edge condition="Accepted=False"`,
	})
}

// A parentRef the gateway has not answered about yet is pending, not
// refused.
func TestRouteUnansweredParentIsSilent(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwHealthyGateway(ns, "edge", "external"),
		gwRoute(ns, "api", []any{gwParent("", "edge")}, nil, nil),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), nil)
}

// A parent of some other kind — a mesh Service, an implementation's
// own CRD — is not ours to resolve.
func TestRouteNonGatewayParentIsSilent(t *testing.T) {
	route := gwRoute(ns, "api", []any{map[string]any{"kind": "Service", "name": "mesh"}}, nil, nil)
	wantFindings(t, gwFindings(t, gwCommand(t, nil, []*unstructured.Unstructured{route})), nil)
}

func TestRouteMissingBackend(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwHealthyGateway(ns, "edge", "external"),
		gwRoute(ns, "api", []any{gwParent("", "edge")}, []any{gwBackend("", "gone", 8080)},
			[]any{gwAcceptedParent("", "edge")}),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs)), []string{
		`kind=route.missing_backend severity=critical namespace=prod kind_of_object=HTTPRoute name=api reason=MissingBackendService message="route sends traffic to service prod/gone, which does not exist — requests matching this rule get a 500 from the gateway" service=prod/gone`,
	})
}

// ResolvedRefs would tell you a reference failed; it would not tell
// you which port. Naming the ports the service does expose is the
// whole answer, and it is why this claim is recomputed.
func TestRouteBackendPort(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwHealthyGateway(ns, "edge", "external"),
		gwRoute(ns, "api", []any{gwParent("", "edge")}, []any{gwBackend("", "api", 8080)},
			[]any{gwAcceptedParent("", "edge")}),
	}
	cmd := gwCommand(t, nil, crdObjs, gwService(ns, "api", 80, 443))
	wantFindings(t, gwFindings(t, cmd), []string{
		`kind=route.backend_port severity=critical namespace=prod kind_of_object=HTTPRoute name=api reason=BackendPortNotExposed message="route sends traffic to service prod/api on port 8080, which that service does not expose (it has 443,80) — the reference never resolves to an endpoint" service=prod/api port=8080 service_ports=443,80`,
	})
}

func TestRouteBackendSilentCases(t *testing.T) {
	base := func(backends []any, core ...runtime.Object) checks.Command {
		crdObjs := []*unstructured.Unstructured{
			gwAcceptedClass("external"),
			gwHealthyGateway(ns, "edge", "external"),
			gwRoute(ns, "api", []any{gwParent("", "edge")}, backends, []any{gwAcceptedParent("", "edge")}),
		}
		return gwCommand(t, nil, crdObjs, core...)
	}
	tests := []struct {
		name string
		cmd  checks.Command
	}{{
		// port is optional on a backendRef; without one there is
		// nothing to check it against.
		name: "no port on the ref",
		cmd:  base([]any{gwBackend("", "api", 0)}, gwService(ns, "api", 80)),
	}, {
		// A backend we cannot resolve is not a backend we can fault.
		name: "non-Service backend",
		cmd:  base([]any{map[string]any{"kind": "AWSLambda", "group": "vendor.example.com", "name": "fn"}}),
	}, {
		name: "port matches",
		cmd:  base([]any{gwBackend("", "api", 8080)}, gwService(ns, "api", 8080)),
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantFindings(t, gwFindings(t, tc.cmd), nil)
		})
	}
}

// Ten rules pointing at one dead backend is one problem, not ten.
func TestRouteBackendDedupedAcrossRules(t *testing.T) {
	route := gwRoute(ns, "api", []any{gwParent("", "edge")}, nil, []any{gwAcceptedParent("", "edge")})
	rules := []any{}
	for i := 0; i < 4; i++ {
		rules = append(rules, map[string]any{"backendRefs": []any{gwBackend("", "gone", 8080)}})
	}
	route.Object["spec"].(map[string]any)["rules"] = rules
	crdObjs := []*unstructured.Unstructured{gwAcceptedClass("external"), gwHealthyGateway(ns, "edge", "external"), route}
	got := gwFindings(t, gwCommand(t, nil, crdObjs))
	if len(got) != 1 {
		t.Fatalf("got %d findings, want 1: %v", len(got), got)
	}
}

// Under --namespace, a reference into a namespace that was never
// listed must not be reported missing on the strength of not having
// been looked at (§11 no coverage lies).
func TestGatewayCrossNamespaceRefsOutOfScopeAreSilent(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwHealthyGateway("infra", "shared", "external"),
		gwRoute(ns, "api", []any{gwParent("infra", "shared")}, []any{gwBackend("other", "svc", 80)},
			[]any{gwAcceptedParent("infra", "shared")}),
	}
	wantFindings(t, gwFindings(t, gwCommand(t, nil, crdObjs, gwService("other", "svc", 80)), "--namespace="+ns), nil)
}

func TestGatewayNamespaceScoping(t *testing.T) {
	crdObjs := []*unstructured.Unstructured{
		gwAcceptedClass("external"),
		gwGateway(ns, "edge", "gone-a", nil, nil),
		gwGateway("staging", "edge", "gone-b", nil, nil),
	}
	got := gwFindings(t, gwCommand(t, nil, crdObjs), "--namespace="+ns)
	if len(got) != 1 {
		t.Fatalf("got %d findings, want only the prod one: %v", len(got), got)
	}
	if !strings.Contains(got[0], "namespace=prod") {
		t.Errorf("wrong namespace: %s", got[0])
	}
}

func TestGatewayWorkloadIsUsageError(t *testing.T) {
	res := checktest.Run(t, gwCommand(t, nil, nil), "--workload=Deployment/prod/api")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d", res.Code, emit.ExitUsage)
	}
	if !strings.Contains(res.Stderr, "cluster-wide") {
		t.Errorf("error should explain the scope: %s", res.Stderr)
	}
}

// gwMixed is a cluster with one of each failure alongside working
// plumbing, so the golden shows the ordering and the silence together.
func gwMixed() (crdObjs []*unstructured.Unstructured, core []runtime.Object) {
	brokenListener := gwGateway(ns, "edge", "external",
		[]any{gwListener("http", "HTTP", 80), gwListener("https", "HTTPS", 443)},
		map[string]any{
			"conditions": []any{gwCond("Accepted", "True", "Accepted", ""), gwCond("Programmed", "True", "Programmed", "")},
			"listeners": []any{
				gwListenerStatus("http", gwCond("Accepted", "True", "Accepted", "")),
				gwListenerStatus("https", gwCond("ResolvedRefs", "False", "InvalidCertificateRef", "secret prod/tls not found")),
			},
		})
	return []*unstructured.Unstructured{
			gwAcceptedClass("external"),
			gwClass("legacy", "example.net/old-controller",
				gwCond("Accepted", "False", "Waiting", "controller not installed")),
			brokenListener,
			gwGateway(ns, "internal", "missing", nil, nil),
			gwGateway(ns, "old", "legacy", nil, nil),
			gwGateway(ns, "pending", "external", []any{gwListener("http", "HTTP", 80)},
				map[string]any{"conditions": []any{
					gwCond("Accepted", "True", "Accepted", ""),
					gwCond("Programmed", "False", "Pending", "waiting for an address"),
				}}),
			// Healthy: attached, accepted, backend resolves.
			gwRoute(ns, "good", []any{gwParent("", "edge")}, []any{gwBackend("", "api", 8080)},
				[]any{gwAcceptedParent("", "edge")}),
			gwRoute(ns, "orphan", []any{gwParent("", "deleted")}, nil, nil),
			gwRoute(ns, "refused", []any{gwParent("", "edge")}, []any{gwBackend("", "api", 9999)},
				[]any{gwParentStatus("", "edge", gwCond("Accepted", "False", "NoMatchingListenerHostname", "no listener matches api.example.com"))}),
			gwRoute(ns, "stale", []any{gwParent("", "edge")}, []any{gwBackend("", "removed", 80)},
				[]any{gwAcceptedParent("", "edge")}),
		}, []runtime.Object{
			gwService(ns, "api", 8080),
		}
}

func TestGatewayMixedGolden(t *testing.T) {
	crdObjs, core := gwMixed()
	res := checktest.Run(t, gwCommand(t, nil, crdObjs, core...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	volGolden(t, "gateway-mixed.golden", res.Stdout)
}

func TestGatewayContract(t *testing.T) {
	crdObjs, core := gwMixed()
	checktest.VerifyContract(t, gwCommand(t, nil, crdObjs, core...))
}

// The degradation path has its own envelope and must satisfy the
// contract too — it is what most clusters will actually see.
func TestGatewayUnavailableContract(t *testing.T) {
	checktest.VerifyContract(t, gwCommand(t, []string{}, nil))
}

func TestGatewayRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("state gateway")
	if !ok {
		t.Fatal("state gateway is not registered in the default registry")
	}
	if c.MCPName != "k8s_gateway_routes" {
		t.Errorf("MCP tool name = %q, want k8s_gateway_routes", c.MCPName)
	}
	if !strings.Contains(c.Help(), "--namespace") {
		t.Error("generated help does not document --namespace scoping")
	}
}
