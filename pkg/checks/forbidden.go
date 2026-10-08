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

import (
	"errors"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Refusal is one read the API server refused on authorization grounds
// (#546). It is the ONE place an unavailable answer caused by RBAC is
// worded: ListForbidden, `read.unavailable` records, health's category
// reasons and unverified= notes all render through it, so every such
// line says the same three things — what was refused, why this
// identity lacks it, and what grant fixes it.
type Refusal struct {
	Verb     string // "list", "get", "watch"
	Group    string // API group; "" is core
	Resource string // plural resource name, e.g. "nodes"
}

// Refused builds the Refusal for verb on group/resource.
func Refused(verb, group, resource string) Refusal {
	return Refusal{Verb: verb, Group: group, Resource: resource}
}

// Target is the resource as resource[.group] — the form the concise
// summary-line notes (skipped=, drilldown_skipped=) and the resource=
// detail already use.
func (r Refusal) Target() string {
	if r.Group == "" {
		return r.Resource
	}
	return r.Resource + "." + r.Group
}

// Short is the concise form, "forbidden: list nodes", for places a
// whole sentence does not fit.
func (r Refusal) Short() string {
	if r.Resource == "" {
		return "forbidden: " + r.verb()
	}
	return "forbidden: " + r.verb() + " " + r.Target()
}

// String is the full explanation, e.g.
//
//	forbidden: list nodes — cluster-scoped, not granted by the built-in
//	view role; grant list on nodes (core) via a ClusterRole, as
//	lookout's shipped ClusterRole does
//
// The view clause appears only when the resource really is outside
// the built-in `view` role (ViewGrants), so a refusal under a custom
// role does not wrongly blame `view`; the shipped-role clause only
// when deploy/12-clusterrole-watcher.yaml really grants it
// (ShippedGrants).
func (r Refusal) String() string {
	if r.Resource == "" {
		return r.Short() + " — this identity lacks the read; grant it via a ClusterRole or Role"
	}
	var b strings.Builder
	b.WriteString(r.Short())
	b.WriteString(" — ")
	cluster := ClusterScoped(r.Group, r.Resource)
	if cluster {
		b.WriteString("cluster-scoped, ")
	} else {
		b.WriteString("namespaced, ")
	}
	if ViewGrants(r.verb(), r.Group, r.Resource) {
		b.WriteString("this identity lacks it")
	} else {
		b.WriteString("not granted by the built-in view role")
	}
	b.WriteString("; grant ")
	b.WriteString(r.verb())
	b.WriteString(" on ")
	b.WriteString(r.Resource)
	b.WriteString(" (")
	if r.Group == "" {
		b.WriteString("core")
	} else {
		b.WriteString(r.Group)
	}
	if cluster {
		b.WriteString(") via a ClusterRole")
	} else {
		b.WriteString(") via a ClusterRole or Role")
	}
	if ShippedGrants(r.verb(), r.Group, r.Resource) {
		b.WriteString(", as lookout's shipped ClusterRole does")
	}
	return b.String()
}

func (r Refusal) verb() string {
	if r.Verb == "" {
		return "list"
	}
	return r.Verb
}

// cannotVerb pulls the verb out of the API server's Forbidden message
// (`User "u" cannot list resource "nodes" in API group ""…`).
var cannotVerb = regexp.MustCompile(`cannot ([a-z]+) resource`)

// ForbiddenRefusal classifies an error from a read-path call: when the
// API server refused it on authorization grounds it reports the
// Refusal (verb from the server's message, "list" when it names none).
// Any other error reports false.
func ForbiddenRefusal(err error) (Refusal, bool) {
	var re *RefusalError
	if errors.As(err, &re) {
		return re.Refusal, true
	}
	if err == nil || !apierrors.IsForbidden(err) {
		return Refusal{}, false
	}
	r := Refusal{Verb: "list"}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		st := status.Status()
		if d := st.Details; d != nil {
			r.Resource, r.Group = d.Kind, d.Group
		}
		if m := cannotVerb.FindStringSubmatch(st.Message); m != nil {
			r.Verb = m[1]
		}
	}
	return r, true
}

// ListForbidden classifies an error from a read-path List call: when
// the API server refused it on authorization grounds it reports the
// full Refusal explanation ("forbidden: list nodes — cluster-scoped,
// not granted by the built-in view role; grant list on nodes (core)
// via a ClusterRole, as lookout's shipped ClusterRole does"), which a
// composition turns into an explicit unavailable answer instead of
// failing the whole command (#546). Any other error reports false and
// stays fatal — a broken API server is not a permission gap.
//
// It lives at the bottom of the checks tree so every group can use it
// without importing another group; state.ListForbidden is the same
// function.
func ListForbidden(err error) (string, bool) {
	r, ok := ForbiddenRefusal(err)
	if !ok {
		return "", false
	}
	return r.String(), true
}

// KindReadUnavailable is the degradation record for one resource a
// command could not read.
const KindReadUnavailable = "read.unavailable"

// UnreadKind is the ledger entry for KindReadUnavailable. Every command
// that emits it declares it from here, so the one claim reads the same
// wherever it is rendered.
func UnreadKind() KindField {
	return Kind(KindReadUnavailable,
		"a resource this command reads was refused (RBAC forbidden, e.g. Nodes, Secrets or RBAC objects under the built-in view role) or is not served, so the checks that need it did not run and their silence is not a clean bill; everything else was still verified. The message names the refused verb and resource, whether it is cluster-scoped, whether the built-in view role grants it, and the grant that fixes it; resource= carries the refused resource as resource[.group] — an explicit degradation record, never silence (§11)",
		emit.SeverityInfo)
}

// UnreadFields are the output-glossary entries for the read.unavailable
// record.
func UnreadFields() []OutputField {
	return []OutputField{
		{Name: "resource", Doc: "read.unavailable: the resource that could not be read, as resource[.group]"},
	}
}

// RefusedFinding is the read.unavailable record for one refused read:
// the Refusal's full explanation, then what the command could not
// judge without it. lead is prepended to the details (a command that
// stamps every finding, e.g. workload=, passes it here).
func RefusedFinding(r Refusal, what string, lead ...emit.Field) emit.Finding {
	details := append(append([]emit.Field(nil), lead...), emit.Field{Key: "resource", Value: r.Target()})
	msg := r.String()
	if what != "" {
		msg += " — " + what
	}
	return emit.Finding{
		Kind:     KindReadUnavailable,
		Severity: emit.SeverityInfo,
		Reason:   "ListForbidden",
		Message:  msg,
		Details:  details,
	}
}

// RefusedAnswer is the whole answer of a command whose every check
// depends on the read err refused: one read.unavailable record, and
// nothing else (#546). It reports false for any error that is not an
// authorization refusal, which stays fatal.
func RefusedAnswer(out *emit.Writer, err error, what string) (bool, error) {
	r, ok := ForbiddenRefusal(err)
	if !ok {
		return false, err
	}
	return true, out.Emit(RefusedFinding(r, what))
}

// RefusalError is a refused read a command cannot answer without
// (#584): its subject, or the one input every check it runs needs.
// Such a command still fails (exit 1), but its diagnostic says the
// same three things a read.unavailable record does — what was
// refused, why this identity lacks it, and the grant that fixes it —
// instead of the API server's bare "is forbidden".
type RefusalError struct {
	Refusal Refusal
	// What names what could not be answered without the read.
	What string
	// Err is the API server's error, when the refusal came from one
	// (nil when a List pass recorded the refusal earlier and the
	// command only now finds it needed the resource).
	Err error
	// Context is the caller's wrapping around the API server's error
	// ("workload X: listing pods"), kept ahead of the shared wording so
	// the diagnostic still says which read inside the command was
	// refused. Empty when there was none.
	Context string
}

func (e *RefusalError) Error() string {
	msg := e.Refusal.String()
	if e.What != "" {
		msg += " — " + e.What
	}
	if e.Context != "" {
		msg = e.Context + ": " + msg
	}
	return msg
}

func (e *RefusalError) Unwrap() error { return e.Err }

// cannotAnswer is what a refusal the command did not word itself
// says it cost.
const cannotAnswer = "the command cannot answer without it"

// WordRefusal rewrites an authorization refusal in err as a
// RefusalError, so it reads in the shared wording wherever it is
// rendered: the runner applies it to every command's error
// (Command.RunConfig, which both the CLI and the MCP surface go
// through), and scan to each stage's. An error that already carries a
// RefusalError, or is no refusal at all, comes back unchanged.
func WordRefusal(err error) error {
	if err == nil {
		return nil
	}
	var re *RefusalError
	if errors.As(err, &re) {
		return err
	}
	r, ok := ForbiddenRefusal(err)
	if !ok {
		return err
	}
	return &RefusalError{Refusal: r, What: cannotAnswer, Err: err, Context: wrapContext(err)}
}

// wrapContext is the text err's wrappers put ahead of the API server's
// own message — "workload X: listing pods" for
// fmt.Errorf("workload X: listing pods: %w", apiErr) — or "" when the
// API error is unwrapped or its message is not the tail of err's.
func wrapContext(err error) string {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return ""
	}
	inner, ok := status.(error)
	if !ok {
		return ""
	}
	outer, tail := err.Error(), inner.Error()
	if outer == tail || !strings.HasSuffix(outer, tail) {
		return ""
	}
	return strings.TrimRight(strings.TrimSuffix(outer, tail), ": ")
}

// isRead reports whether verb is one of the read verbs `view` grants.
func isRead(verb string) bool { return verb == "get" || verb == "list" || verb == "watch" }

// ViewGrants reports whether the built-in `view` ClusterRole grants
// verb on group/resource (ViewRoleReads).
func ViewGrants(verb, group, resource string) bool {
	return isRead(verb) && ViewRoleReads[schema.GroupResource{Group: group, Resource: resource}]
}

// ClusterScoped reports whether group/resource is a cluster-scoped
// kind lookout reads (or might be refused). Anything not listed is
// treated as namespaced.
func ClusterScoped(group, resource string) bool {
	return clusterScoped[schema.GroupResource{Group: group, Resource: resource}]
}

// ShippedGrants reports whether lookout's shipped ClusterRole
// (deploy/12-clusterrole-watcher.yaml, and the chart rendered from
// the same rules) grants verb on group/resource. The table is pinned
// to the YAML by TestShippedRoleGrantsMatchManifest.
func ShippedGrants(verb, group, resource string) bool {
	for _, v := range ShippedRoleGrants[schema.GroupResource{Group: group, Resource: resource}] {
		if v == verb {
			return true
		}
	}
	return false
}

// ViewRoleReads is every (group, resource) the built-in `view`
// ClusterRole lets a subject get, list and watch, transcribed from the
// aggregated rules of system:aggregate-to-view in the upstream
// bootstrap policy (kubernetes/kubernetes plugin/pkg/auth/authorizer/
// rbac/bootstrappolicy/testdata/cluster-roles.yaml). It is an ALLOW
// list on purpose: anything not named here — nodes, secrets, every
// rbac.authorization.k8s.io kind, PersistentVolumes, StorageClasses,
// IngressClasses, admission webhook configurations, CRDs — is outside
// `view`. checktest.ViewRole refuses exactly the complement, so a
// read-path command that grows a new read is tested against `view`
// without anyone updating a deny list.
//
// metrics.k8s.io is absent: metrics-server's own aggregate-to-view
// role adds it on clusters that run metrics-server, but `view` itself
// does not grant it.
var ViewRoleReads = map[schema.GroupResource]bool{
	{Resource: "configmaps"}:                                       true,
	{Resource: "endpoints"}:                                        true,
	{Resource: "persistentvolumeclaims"}:                           true,
	{Resource: "pods"}:                                             true,
	{Resource: "replicationcontrollers"}:                           true,
	{Resource: "serviceaccounts"}:                                  true,
	{Resource: "services"}:                                         true,
	{Resource: "bindings"}:                                         true,
	{Resource: "limitranges"}:                                      true,
	{Resource: "resourcequotas"}:                                   true,
	{Resource: "namespaces"}:                                       true,
	{Resource: "events"}:                                           true,
	{Group: "events.k8s.io", Resource: "events"}:                   true,
	{Group: "discovery.k8s.io", Resource: "endpointslices"}:        true,
	{Group: "apps", Resource: "controllerrevisions"}:               true,
	{Group: "apps", Resource: "daemonsets"}:                        true,
	{Group: "apps", Resource: "deployments"}:                       true,
	{Group: "apps", Resource: "replicasets"}:                       true,
	{Group: "apps", Resource: "statefulsets"}:                      true,
	{Group: "autoscaling", Resource: "horizontalpodautoscalers"}:   true,
	{Group: "batch", Resource: "cronjobs"}:                         true,
	{Group: "batch", Resource: "jobs"}:                             true,
	{Group: "extensions", Resource: "daemonsets"}:                  true,
	{Group: "extensions", Resource: "deployments"}:                 true,
	{Group: "extensions", Resource: "ingresses"}:                   true,
	{Group: "extensions", Resource: "networkpolicies"}:             true,
	{Group: "extensions", Resource: "replicasets"}:                 true,
	{Group: "policy", Resource: "poddisruptionbudgets"}:            true,
	{Group: "networking.k8s.io", Resource: "ingresses"}:            true,
	{Group: "networking.k8s.io", Resource: "networkpolicies"}:      true,
	{Group: "resource.k8s.io", Resource: "resourceclaims"}:         true,
	{Group: "resource.k8s.io", Resource: "resourceclaimtemplates"}: true,
}

// clusterScoped lists the cluster-scoped kinds a read-path command
// may be refused.
var clusterScoped = map[schema.GroupResource]bool{
	{Resource: "nodes"}:                                                                    true,
	{Resource: "namespaces"}:                                                               true,
	{Resource: "persistentvolumes"}:                                                        true,
	{Resource: "componentstatuses"}:                                                        true,
	{Group: "storage.k8s.io", Resource: "storageclasses"}:                                  true,
	{Group: "storage.k8s.io", Resource: "volumeattachments"}:                               true,
	{Group: "storage.k8s.io", Resource: "csidrivers"}:                                      true,
	{Group: "storage.k8s.io", Resource: "csinodes"}:                                        true,
	{Group: "networking.k8s.io", Resource: "ingressclasses"}:                               true,
	{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"}:                         true,
	{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"}:                  true,
	{Group: "admissionregistration.k8s.io", Resource: "validatingwebhookconfigurations"}:   true,
	{Group: "admissionregistration.k8s.io", Resource: "mutatingwebhookconfigurations"}:     true,
	{Group: "admissionregistration.k8s.io", Resource: "validatingadmissionpolicies"}:       true,
	{Group: "admissionregistration.k8s.io", Resource: "validatingadmissionpolicybindings"}: true,
	{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}:                 true,
	{Group: "apiregistration.k8s.io", Resource: "apiservices"}:                             true,
	{Group: "scheduling.k8s.io", Resource: "priorityclasses"}:                              true,
	{Group: "node.k8s.io", Resource: "runtimeclasses"}:                                     true,
	{Group: "certificates.k8s.io", Resource: "certificatesigningrequests"}:                 true,
	{Group: "metrics.k8s.io", Resource: "nodes"}:                                           true,
	{Group: "leeway.lookout.go-steer.io", Resource: "clusterleewaypolicies"}:               true,
	{Group: "cloud.google.com", Resource: "computeclasses"}:                                true,
}

// ShippedRoleGrants is the rules of lookout's shipped ClusterRole,
// deploy/12-clusterrole-watcher.yaml, as (group, resource) → verbs.
// TestShippedRoleGrantsMatchManifest fails when the two drift, so the
// "as lookout's shipped ClusterRole does" clause is never a false
// promise.
var ShippedRoleGrants = map[schema.GroupResource][]string{
	{Resource: "events"}:                                                                 {"get", "list", "watch"},
	{Resource: "pods"}:                                                                   {"get", "list", "watch"},
	{Resource: "pods/log"}:                                                               {"get"},
	{Resource: "nodes"}:                                                                  {"list", "watch"},
	{Group: "apps", Resource: "deployments"}:                                             {"get", "list", "watch"},
	{Group: "apps", Resource: "replicasets"}:                                             {"get", "list", "watch"},
	{Group: "apps", Resource: "statefulsets"}:                                            {"get", "list", "watch"},
	{Group: "apps", Resource: "daemonsets"}:                                              {"get", "list"},
	{Group: "batch", Resource: "jobs"}:                                                   {"get", "list", "watch"},
	{Group: "batch", Resource: "cronjobs"}:                                               {"get", "list", "watch"},
	{Resource: "configmaps"}:                                                             {"list"},
	{Resource: "services"}:                                                               {"list", "watch"},
	{Group: "networking.k8s.io", Resource: "ingresses"}:                                  {"list", "watch"},
	{Group: "networking.k8s.io", Resource: "ingressclasses"}:                             {"list"},
	{Group: "networking.k8s.io", Resource: "networkpolicies"}:                            {"list", "watch"},
	{Resource: "persistentvolumeclaims"}:                                                 {"list", "watch"},
	{Resource: "persistentvolumes"}:                                                      {"list", "watch"},
	{Group: "storage.k8s.io", Resource: "storageclasses"}:                                {"list"},
	{Group: "rbac.authorization.k8s.io", Resource: "roles"}:                              {"list"},
	{Group: "rbac.authorization.k8s.io", Resource: "rolebindings"}:                       {"list"},
	{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"}:                       {"list"},
	{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"}:                {"list"},
	{Group: "metrics.k8s.io", Resource: "pods"}:                                          {"get", "list"},
	{Resource: "nodes/proxy"}:                                                            {"get"},
	{Group: "autoscaling", Resource: "horizontalpodautoscalers"}:                         {"list", "watch"},
	{Group: "discovery.k8s.io", Resource: "endpointslices"}:                              {"list", "watch"},
	{Group: "policy", Resource: "poddisruptionbudgets"}:                                  {"list", "watch"},
	{Group: "leeway.lookout.go-steer.io", Resource: "leewaypolicies"}:                    {"list", "watch"},
	{Group: "leeway.lookout.go-steer.io", Resource: "clusterleewaypolicies"}:             {"list", "watch"},
	{Resource: "secrets"}:                                                                {"list"},
	{Resource: "serviceaccounts"}:                                                        {"list"},
	{Group: "admissionregistration.k8s.io", Resource: "validatingwebhookconfigurations"}: {"list"},
	{Group: "admissionregistration.k8s.io", Resource: "mutatingwebhookconfigurations"}:   {"list"},
	{Group: "cert-manager.io", Resource: "certificates"}:                                 {"list"},
	{Group: "acme.cert-manager.io", Resource: "challenges"}:                              {"list", "watch"},
	{Group: "acme.cert-manager.io", Resource: "orders"}:                                  {"list", "watch"},
	{Group: "gateway.networking.k8s.io", Resource: "gateways"}:                           {"list", "watch"},
	{Group: "gateway.networking.k8s.io", Resource: "httproutes"}:                         {"list", "watch"},
	{Group: "cloud.google.com", Resource: "computeclasses"}:                              {"list", "watch"},
}
