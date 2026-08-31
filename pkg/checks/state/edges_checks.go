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

// The per-edge validity checks of `state edges` (DESIGN.md §5). The
// graph supplies traversal — PodsUnder resolves the workload to its
// pods, WorkloadEdges yields the aggregated dependency edges,
// Ref.Observed answers "does the referenced object exist" — and each
// check here decides whether an edge is *valid*. Every failure is one
// finding; healthy edges are silent (§4.2 zero nominal state).
//
// The finding kinds and their severities are declared once, in
// EdgeKinds below — that ledger is the command's contract, is
// rendered into --help and the reference docs, and is enforced
// against what the checks actually emit, so there is no prose copy
// here to drift.
//
// No secret material can appear here: cert findings carry only
// subject/notAfter/days-left, and the pkg/emit sanitizer additionally
// masks every emitted value (§6.5) — but nothing below relies on it.

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// EdgeKinds is the finding-kind ledger of the edge checks. It is
// exported because the checks run from two places: `state edges`
// itself, and every composition that calls (*Cluster).EdgeFindings
// directly — `lookout scan`, `bundle`, `health`. Each of those has to
// declare the kinds it can emit, and all of them must say the same
// thing, so they all say it from here.
func EdgeKinds() []checks.KindField {
	return []checks.KindField{
		checks.Kind("edge.missing_ref", "a referenced ConfigMap, Secret, ServiceAccount, TLS secret, IngressClass, StorageClass, or governing Service does not exist", emit.SeverityCritical),
		checks.Kind("edge.missing_key", "the referenced key is absent from an existing ConfigMap/Secret", emit.SeverityCritical),
		checks.Kind("edge.invalid_ref", "the referenced object exists but is the wrong type to serve the reference", emit.SeverityWarning),
		checks.Kind("edge.unclassed", "the Ingress names no class and no IngressClass declares itself the cluster default — no controller will claim it", emit.SeverityWarning),
		checks.Kind("edge.selector_empty", "a Service selector selects zero pods, so the service routes nowhere", emit.SeverityCritical),
		checks.Kind("edge.selector_unready", "the Service selects pods but some are not Ready; critical when none are", emit.SeverityCritical, emit.SeverityWarning),
		checks.Kind("edge.endpoints_missing", "a selecting Service has no EndpointSlices at all", emit.SeverityCritical),
		checks.Kind("edge.endpoints_orphaned", "an endpoint targetRef names a pod that no longer exists", emit.SeverityWarning),
		checks.Kind("edge.endpoints_unready", "the endpoint ready-count disagrees with the selected pods (stale or lagging slices); critical at zero ready", emit.SeverityCritical, emit.SeverityWarning),
		checks.Kind("edge.backend_missing", "an Ingress backend service, or the port it names, does not exist", emit.SeverityCritical),
		checks.Kind("edge.cert_expired", "a TLS certificate's NotAfter is in the past", emit.SeverityCritical),
		checks.Kind("edge.cert_expiring", "a TLS certificate expires within --cert-warn", emit.SeverityWarning),
		checks.Kind("edge.cert_invalid", "tls.crt is missing or unparseable, or the secret is not kubernetes.io/tls", emit.SeverityWarning),
		checks.Kind("edge.rbac_dangling", "a (Cluster)RoleBinding for the workload's ServiceAccount points at a missing (Cluster)Role", emit.SeverityWarning),
	}
}

// edgeScan is one `state edges` evaluation: the workload, the graph
// snapshot it is resolved in, and the listed objects the validity
// checks read.
type edgeScan struct {
	wl       emit.WorkloadRef
	ix       *index
	snap     *graph.Snapshot
	id       graph.NodeID
	now      time.Time
	certWarn time.Duration

	// sweep is the target-free mode: no workload, every Service and
	// Ingress in the loaded scope, and only the checks whose subject is
	// the edge rather than the workload (#331). See runSweep.
	sweep bool

	pods   []*corev1.Pod // the workload's pods, sorted by name
	podSet map[string]bool

	candidates map[string]*candidate // service ns/name → role of that service
	secretRefs map[string]bool       // secrets referenced by the pods (mount reachability)
	ingressTLS map[string]string     // secret ns/name → ingress name referencing it

	findings []emit.Finding
}

// candidate is a Service associated with the workload: either its
// selector selects the workload's pods (viaSelector), its
// EndpointSlices route to them, or — for the misconfiguration case —
// its selector selects *nothing* while sharing label keys with the
// workload's pods (emptyIntent: the service that was meant to select
// this workload).
type candidate struct {
	svc         *corev1.Service
	viaSelector bool
	emptyIntent bool
	selected    []string // pod ns/names the selector currently selects
	readyPods   int
}

func (e *edgeScan) run() []emit.Finding {
	e.podSet = map[string]bool{}
	e.candidates = map[string]*candidate{}
	e.secretRefs = map[string]bool{}
	e.ingressTLS = map[string]string{}

	e.resolvePods()
	e.checkRefs()
	e.checkImagePullSecrets()
	e.checkStatefulSet()
	e.buildCandidates()
	e.checkSelectors()
	e.checkEndpoints()
	e.checkIngresses()
	e.checkCerts()
	e.checkRBAC()
	return e.findings
}

// runSweep is the target-free pass: the same validity checks over
// every Service and Ingress in the loaded scope, with no workload to
// trace from.
//
// The checks it runs are exactly the ones whose subject is not the
// workload — a Service selecting nothing, an Ingress backend or class
// that does not resolve, an expiring TLS certificate. Each of those is
// unreachable from a workload-first drill-down when every workload is
// healthy, which is the whole of #331: a Service can black-hole
// traffic while `kubectl get deploy` looks perfect, and the drill-down
// only ever explains workloads something else already flagged.
//
// The workload-scoped checks (mount refs, image-pull secrets,
// StatefulSet governance, endpoint agreement, RBAC) are deliberately
// absent: they need a workload, and their faults do surface a sick pod
// for stage 1 to catch.
func (e *edgeScan) runSweep() []emit.Finding {
	e.sweep = true
	e.podSet = map[string]bool{}
	e.candidates = map[string]*candidate{}
	e.secretRefs = map[string]bool{}
	e.ingressTLS = map[string]string{}

	e.sweepCandidates()
	e.checkSelectors()
	e.checkIngresses()
	e.checkCerts()
	return e.findings
}

// sweepCandidates is buildCandidates with the workload removed: every
// Service in scope whose selector selects nothing. There is no owner
// to name and none is needed — "this Service routes nowhere" is true
// of the Service alone. Attribution is the harder question and it
// belongs to `state edges --workload=X`, which is where the
// selector-intent heuristic lives (#331, #332).
func (e *edgeScan) sweepCandidates() {
	for _, k := range sortedKeys(e.ix.services) {
		svc := e.ix.services[k]
		if len(svc.Spec.Selector) == 0 || len(e.selectedPods(svc)) > 0 {
			continue
		}
		e.candidates[k] = &candidate{svc: svc, emptyIntent: true}
	}
}

// workloadDetail is the `workload=` field every edge finding leads
// with — the workload whose edges were being traced. A sweep finding
// has no such workload, and inventing one is exactly the
// misattribution #332 was about, so it carries none.
func (e *edgeScan) workloadDetail() []emit.Field {
	if e.sweep {
		return nil
	}
	return []emit.Field{{Key: "workload", Value: e.wl.String()}}
}

func (e *edgeScan) add(f emit.Finding) { e.findings = append(e.findings, f) }

// observed reports whether the identified object was actually seen
// from the API server (graph Ref.Observed): false means it exists
// only as a dangling reference.
func (e *edgeScan) observed(kind graph.NodeKind, ns, name string) bool {
	id, ok := e.snap.Lookup(kind, ns, name)
	if !ok {
		return false
	}
	ref, ok := e.snap.Resolve(id)
	return ok && ref.Observed
}

// resolvePods resolves the workload to its pods via the graph's
// owner-chain traversal (§6.4: PodsUnder).
func (e *edgeScan) resolvePods() {
	for _, id := range e.snap.PodsUnder(e.id) {
		ref, ok := e.snap.Resolve(id)
		if !ok || !ref.Observed {
			continue
		}
		if p := e.ix.pods[key(ref.Namespace, ref.Name)]; p != nil {
			e.pods = append(e.pods, p)
			e.podSet[key(ref.Namespace, ref.Name)] = true
		}
	}
	sort.Slice(e.pods, func(i, j int) bool { return e.pods[i].Name < e.pods[j].Name })
}

// templateLabels returns the labels the workload's pods carry (from a
// live pod when one exists, else from the workload's pod template) —
// the reference point for the selector-intent heuristic.
func (e *edgeScan) templateLabels() map[string]string {
	if len(e.pods) > 0 {
		return e.pods[0].Labels
	}
	if tpl, ok := e.ix.templates[e.wl.Kind+"/"+key(e.wl.Namespace, e.wl.Name)]; ok {
		return tpl.labels
	}
	return nil
}

// ---------------------------------------------------------------------------
// Mounts / env references: edge.missing_ref, edge.missing_key
// ---------------------------------------------------------------------------

// objRef is one ConfigMap/Secret reference declared by a pod spec.
// key=="" means the reference needs only existence (envFrom, volume
// without items).
type objRef struct {
	kind      graph.NodeKind // KindConfigMap or KindSecret
	name      string
	refKey    string
	via       string // "env", "envFrom", "volume"
	container string // env/envFrom only
	envVar    string // env only
	volume    string // volume only
	optional  bool
}

// podRefs enumerates every ConfigMap/Secret reference of one pod, in
// spec order: container env valueFrom, envFrom, then pod volumes
// (plain and projected), item keys expanded.
func podRefs(p *corev1.Pod) []objRef {
	var refs []objRef
	optional := func(b *bool) bool { return b != nil && *b }
	containers := make([]corev1.Container, 0, len(p.Spec.InitContainers)+len(p.Spec.Containers))
	containers = append(containers, p.Spec.InitContainers...)
	containers = append(containers, p.Spec.Containers...)
	for i := range containers {
		c := &containers[i]
		for j := range c.Env {
			vf := c.Env[j].ValueFrom
			if vf == nil {
				continue
			}
			if r := vf.ConfigMapKeyRef; r != nil {
				refs = append(refs, objRef{kind: graph.KindConfigMap, name: r.Name, refKey: r.Key,
					via: "env", container: c.Name, envVar: c.Env[j].Name, optional: optional(r.Optional)})
			}
			if r := vf.SecretKeyRef; r != nil {
				refs = append(refs, objRef{kind: graph.KindSecret, name: r.Name, refKey: r.Key,
					via: "env", container: c.Name, envVar: c.Env[j].Name, optional: optional(r.Optional)})
			}
		}
		for j := range c.EnvFrom {
			if r := c.EnvFrom[j].ConfigMapRef; r != nil {
				refs = append(refs, objRef{kind: graph.KindConfigMap, name: r.Name,
					via: "envFrom", container: c.Name, optional: optional(r.Optional)})
			}
			if r := c.EnvFrom[j].SecretRef; r != nil {
				refs = append(refs, objRef{kind: graph.KindSecret, name: r.Name,
					via: "envFrom", container: c.Name, optional: optional(r.Optional)})
			}
		}
	}
	volume := func(kind graph.NodeKind, name, vol string, opt bool, items []corev1.KeyToPath) {
		if len(items) == 0 {
			refs = append(refs, objRef{kind: kind, name: name, via: "volume", volume: vol, optional: opt})
			return
		}
		for i := range items {
			refs = append(refs, objRef{kind: kind, name: name, refKey: items[i].Key,
				via: "volume", volume: vol, optional: opt})
		}
	}
	for i := range p.Spec.Volumes {
		switch v := &p.Spec.Volumes[i]; {
		case v.ConfigMap != nil:
			volume(graph.KindConfigMap, v.ConfigMap.Name, v.Name, optional(v.ConfigMap.Optional), v.ConfigMap.Items)
		case v.Secret != nil:
			volume(graph.KindSecret, v.Secret.SecretName, v.Name, optional(v.Secret.Optional), v.Secret.Items)
		case v.Projected != nil:
			for j := range v.Projected.Sources {
				s := &v.Projected.Sources[j]
				if s.ConfigMap != nil {
					volume(graph.KindConfigMap, s.ConfigMap.Name, v.Name, optional(s.ConfigMap.Optional), s.ConfigMap.Items)
				}
				if s.Secret != nil {
					volume(graph.KindSecret, s.Secret.Name, v.Name, optional(s.Secret.Optional), s.Secret.Items)
				}
			}
		}
	}
	return refs
}

// checkRefs validates every ConfigMap/Secret reference of the
// workload's pods: object existence via the graph's Ref.Observed,
// key existence against the listed live objects. Identical breakage
// across replicas folds into one finding with a pods=<n> count.
func (e *edgeScan) checkRefs() {
	type refIssue struct {
		finding emit.Finding
		pods    map[string]bool
	}
	issues := map[string]*refIssue{}
	var order []string

	issue := func(p *corev1.Pod, dedup string, f emit.Finding) {
		got, ok := issues[dedup]
		if !ok {
			got = &refIssue{finding: f, pods: map[string]bool{}}
			issues[dedup] = got
			order = append(order, dedup)
		}
		got.pods[p.Name] = true
	}

	for _, p := range e.pods {
		for _, r := range podRefs(p) {
			if r.kind == graph.KindSecret {
				// Track secret reachability for the TLS expiry check
				// regardless of validity outcome.
				e.secretRefs[key(p.Namespace, r.name)] = true
			}
			if r.optional {
				continue
			}
			kindName := r.kind.String()
			dedup := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s", kindName, r.name, r.via, r.container, r.envVar, r.volume, r.refKey)
			if !e.observed(r.kind, p.Namespace, r.name) {
				issue(p, "ref|"+dedup, emit.Finding{
					Kind:         "edge.missing_ref",
					Severity:     emit.SeverityCritical,
					Namespace:    p.Namespace,
					KindOfObject: kindName,
					Name:         r.name,
					Reason:       refReason(r.via),
					Message:      refMessage(kindName, r, false),
					Details:      e.refDetails(r),
				})
				continue
			}
			if r.refKey != "" && !e.hasKey(r.kind, p.Namespace, r.name, r.refKey) {
				issue(p, "key|"+dedup, emit.Finding{
					Kind:         "edge.missing_key",
					Severity:     emit.SeverityCritical,
					Namespace:    p.Namespace,
					KindOfObject: kindName,
					Name:         r.name,
					Reason:       refReason(r.via),
					Message:      refMessage(kindName, r, true),
					Details:      e.refDetails(r),
				})
			}
		}
	}

	for _, dedup := range order {
		is := issues[dedup]
		f := is.finding
		f.Details = append(f.Details, emit.Field{Key: "pods", Value: strconv.Itoa(len(is.pods))})
		e.add(f)
	}
}

// refReason mirrors the kubelet event reason the breakage would
// produce (§8: Reason is machine-matchable and mirrors k8s where one
// exists): FailedMount for volumes, CreateContainerConfigError for
// env/envFrom.
func refReason(via string) string {
	if via == "volume" {
		return "FailedMount"
	}
	return "CreateContainerConfigError"
}

func refMessage(kindName string, r objRef, keyMissing bool) string {
	lower := map[string]string{"ConfigMap": "configmap", "Secret": "secret"}[kindName]
	var at string
	switch r.via {
	case "env":
		at = fmt.Sprintf("env %s in container %s", r.envVar, r.container)
	case "envFrom":
		at = fmt.Sprintf("envFrom in container %s", r.container)
	default:
		at = "volume " + r.volume
	}
	if keyMissing {
		return fmt.Sprintf("key %s not found in %s %s (%s)", r.refKey, lower, r.name, at)
	}
	return fmt.Sprintf("%s %s not found (%s)", lower, r.name, at)
}

func (e *edgeScan) refDetails(r objRef) []emit.Field {
	d := []emit.Field{{Key: "workload", Value: e.wl.String()}}
	if r.container != "" {
		d = append(d, emit.Field{Key: "container", Value: r.container})
	}
	if r.envVar != "" {
		d = append(d, emit.Field{Key: "env", Value: r.envVar})
	}
	if r.volume != "" {
		d = append(d, emit.Field{Key: "volume", Value: r.volume})
	}
	if r.refKey != "" {
		d = append(d, emit.Field{Key: "key", Value: r.refKey})
	}
	return d
}

// hasKey checks the referenced key against the live object from the
// List pass — the graph deliberately stores no payloads (§6.5), so
// key-level validity needs the objects themselves.
func (e *edgeScan) hasKey(kind graph.NodeKind, ns, name, k string) bool {
	switch kind {
	case graph.KindConfigMap:
		cm := e.ix.configMaps[key(ns, name)]
		if cm == nil {
			return false
		}
		_, inData := cm.Data[k]
		_, inBinary := cm.BinaryData[k]
		return inData || inBinary
	case graph.KindSecret:
		s := e.ix.secrets[key(ns, name)]
		if s == nil {
			return false
		}
		_, inData := s.Data[k]
		_, inString := s.StringData[k]
		return inData || inString
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// imagePullSecrets: edge.missing_ref, edge.invalid_ref
// ---------------------------------------------------------------------------

// pullSecretTypes are the two Secret types the kubelet will actually
// use as registry credentials. Anything else is silently ignored at
// pull time, which is what makes it worth reporting.
var pullSecretTypes = map[corev1.SecretType]bool{
	corev1.SecretTypeDockerConfigJson: true,
	corev1.SecretTypeDockercfg:        true,
}

// checkImagePullSecrets validates the registry credentials a pod will
// be pulled with. `triage delta` reports pod.imagepull — the symptom,
// "this pod cannot pull its image". This reports the cause, and the
// cause is a different sentence to act on: the Secret the pod names
// does not exist, or exists and is the wrong type.
//
// Both sources count. imagePullSecrets on the pod spec are the obvious
// one; the ones on the pod's ServiceAccount are the trap, because the
// kubelet merges them in at pull time and they appear nowhere in the
// pod spec, so `kubectl get pod -o yaml` shows a pod with no
// credentials problem at all.
func (e *edgeScan) checkImagePullSecrets() {
	type source struct {
		secret         string
		serviceAccount string // "" when named by the pod spec itself
	}
	// Deduped by (secret, source): identical breakage across replicas
	// is one finding, exactly as checkRefs folds its own.
	seen := map[source]int{}
	var order []source
	record := func(s source) {
		if _, ok := seen[s]; !ok {
			order = append(order, s)
		}
		seen[s]++
	}

	collect := func(ns string, podSpec []string, sa string) {
		for _, name := range podSpec {
			record(source{secret: name})
		}
		if acct := e.ix.serviceAccounts[key(ns, sa)]; acct != nil {
			for _, r := range acct.ImagePullSecrets {
				record(source{secret: r.Name, serviceAccount: sa})
			}
		}
	}
	for _, p := range e.pods {
		names := make([]string, 0, len(p.Spec.ImagePullSecrets))
		for _, r := range p.Spec.ImagePullSecrets {
			names = append(names, r.Name)
		}
		sa := p.Spec.ServiceAccountName
		if sa == "" {
			sa = "default"
		}
		collect(p.Namespace, names, sa)
	}
	if len(e.pods) == 0 {
		// Scaled to zero: read the template, same as checkRBAC does.
		if tpl, ok := e.ix.templates[e.wl.Kind+"/"+key(e.wl.Namespace, e.wl.Name)]; ok {
			sa := tpl.serviceAccount
			if sa == "" {
				sa = "default"
			}
			collect(e.wl.Namespace, tpl.imagePullSecrets, sa)
		}
	}

	for _, s := range order {
		details := []emit.Field{
			{Key: "workload", Value: e.wl.String()},
			{Key: "via", Value: "imagePullSecret"},
		}
		if s.serviceAccount != "" {
			details = append(details, emit.Field{Key: "service_account", Value: s.serviceAccount})
		}
		details = append(details, emit.Field{Key: "pods", Value: strconv.Itoa(seen[s])})
		from := "pod spec"
		if s.serviceAccount != "" {
			from = "serviceaccount " + s.serviceAccount
		}

		sec := e.ix.secrets[key(e.wl.Namespace, s.secret)]
		switch {
		case sec == nil:
			e.add(emit.Finding{
				Kind:         "edge.missing_ref",
				Severity:     emit.SeverityCritical,
				Namespace:    e.wl.Namespace,
				KindOfObject: "Secret",
				Name:         s.secret,
				// The kubelet's own event reason for this exact failure.
				Reason:  "FailedToRetrieveImagePullSecret",
				Message: fmt.Sprintf("imagePullSecret %s not found (referenced by %s) — private images cannot be pulled", s.secret, from),
				Details: details,
			})
		case !pullSecretTypes[sec.Type]:
			e.add(emit.Finding{
				Kind:         "edge.invalid_ref",
				Severity:     emit.SeverityWarning,
				Namespace:    sec.Namespace,
				KindOfObject: "Secret",
				Name:         sec.Name,
				Reason:       "InvalidImagePullSecret",
				Message: fmt.Sprintf("imagePullSecret is type %s, want kubernetes.io/dockerconfigjson or kubernetes.io/dockercfg — the kubelet ignores it (referenced by %s)",
					sec.Type, from),
				Details: details,
			})
		}
	}
}

// ---------------------------------------------------------------------------
// StatefulSet: edge.missing_ref (governing Service, volumeClaimTemplate SC)
// ---------------------------------------------------------------------------

// checkStatefulSet validates the two references a StatefulSet has and
// no other workload kind does.
//
// The governing Service is what a StatefulSet is *for*: without it the
// per-pod DNS names never resolve, so peers cannot find each other and
// nothing in the pod's own status says why. The volumeClaimTemplate
// StorageClass is the slow-motion version — replica 0 may have been
// bound before the class was deleted and looks perfectly healthy while
// every replica after it sits Pending forever.
//
// Reached both from a StatefulSet target and from a Pod target owned by
// one, since a pod is the more common thing to have in hand when
// something is wrong.
func (e *edgeScan) checkStatefulSet() {
	sets := map[string]*appsv1.StatefulSet{}
	if e.wl.Kind == "StatefulSet" {
		if s := e.ix.statefulSets[key(e.wl.Namespace, e.wl.Name)]; s != nil {
			sets[key(s.Namespace, s.Name)] = s
		}
	}
	for _, p := range e.pods {
		for _, o := range p.OwnerReferences {
			if o.Kind != "StatefulSet" {
				continue
			}
			if s := e.ix.statefulSets[key(p.Namespace, o.Name)]; s != nil {
				sets[key(s.Namespace, s.Name)] = s
			}
		}
	}

	for _, k := range sortedKeys(sets) {
		s := sets[k]
		details := func(extra ...emit.Field) []emit.Field {
			return append([]emit.Field{{Key: "workload", Value: e.wl.String()}}, extra...)
		}
		// serviceName is optional from apps/v1 onward; only a name that
		// resolves to nothing is a broken edge.
		if svc := s.Spec.ServiceName; svc != "" && e.ix.services[key(s.Namespace, svc)] == nil {
			e.add(emit.Finding{
				Kind:         "edge.missing_ref",
				Severity:     emit.SeverityCritical,
				Namespace:    s.Namespace,
				KindOfObject: "Service",
				Name:         svc,
				Reason:       "MissingGoverningService",
				Message:      fmt.Sprintf("governing service %s not found — statefulset pods have no stable DNS identity", svc),
				Details:      details(emit.Field{Key: "service", Value: svc}),
			})
		}
		for i := range s.Spec.VolumeClaimTemplates {
			vct := &s.Spec.VolumeClaimTemplates[i]
			sc := vct.Spec.StorageClassName
			// nil means "use the cluster default" — a different claim,
			// and `state storage` owns it. "" means "no dynamic
			// provisioning", which is legal.
			if sc == nil || *sc == "" || e.ix.storageClasses[*sc] {
				continue
			}
			e.add(emit.Finding{
				Kind:         "edge.missing_ref",
				Severity:     emit.SeverityCritical,
				Namespace:    s.Namespace,
				KindOfObject: "StorageClass",
				Name:         *sc,
				Reason:       "MissingStorageClass",
				Message: fmt.Sprintf("volumeClaimTemplate %s names storageclass %s, which does not exist — new replicas stay Pending",
					vct.Name, *sc),
				Details: details(emit.Field{Key: "volume", Value: vct.Name}),
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Service selectors: edge.selector_empty, edge.selector_unready
// ---------------------------------------------------------------------------

// buildCandidates associates Services with the workload. The graph's
// WorkloadEdges answer (§6.4) supplies the observed associations —
// Selects edges onto the workload's pods and the
// Service→EndpointSlice→Pod routing chain — and one heuristic adds
// the failure case no edge can represent: a service whose selector
// matches nothing but looks like it was aimed at this workload's pods
// (the service that was *meant* to select this workload).
func (e *edgeScan) buildCandidates() {
	edges := e.snap.WorkloadEdges(e.id)

	sliceIDs := map[graph.NodeID]bool{}
	for _, we := range edges {
		if we.To != e.id || we.Kind != graph.EdgeRoutesTo {
			continue
		}
		if ref, ok := e.snap.Resolve(we.From); ok && ref.Kind == graph.KindEndpointSlice {
			sliceIDs[we.From] = true
		}
	}
	upsert := func(ref graph.Ref, viaSelector bool) {
		svc := e.ix.services[key(ref.Namespace, ref.Name)]
		if svc == nil {
			return
		}
		c, ok := e.candidates[key(ref.Namespace, ref.Name)]
		if !ok {
			c = &candidate{svc: svc}
			e.candidates[key(ref.Namespace, ref.Name)] = c
		}
		c.viaSelector = c.viaSelector || viaSelector
	}
	for _, we := range edges {
		switch we.Kind {
		case graph.EdgeSelects:
			if we.To != e.id {
				continue
			}
			if ref, ok := e.snap.Resolve(we.From); ok && ref.Kind == graph.KindService {
				upsert(ref, true)
			}
		case graph.EdgeRoutesTo:
			// Northbound hop of the routing chain: Service → slice,
			// where the slice targets the workload's pods.
			if !sliceIDs[we.To] {
				continue
			}
			if ref, ok := e.snap.Resolve(we.From); ok && ref.Kind == graph.KindService && ref.Observed {
				upsert(ref, e.ix.services[key(ref.Namespace, ref.Name)] != nil &&
					len(e.ix.services[key(ref.Namespace, ref.Name)].Spec.Selector) > 0)
			}
		}
	}

	// Intent heuristic for zero-selection services. Attribution has to
	// be earned: the finding claims this Service was meant for THIS
	// workload, and `state edges --workload=X` is the command whose
	// whole contract is single-workload scoping (#332).
	tpl := e.templateLabels()
	for _, k := range sortedKeys(e.ix.services) {
		svc := e.ix.services[k]
		if svc.Namespace != e.wl.Namespace || len(svc.Spec.Selector) == 0 {
			continue
		}
		if _, known := e.candidates[k]; known {
			continue
		}
		if len(e.selectedPods(svc)) > 0 {
			continue // selects some other workload's pods — not ours
		}
		score := selectorAffinity(svc.Spec.Selector, tpl)
		if score <= 0 || !e.bestOwnerInNamespace(svc, score) {
			continue
		}
		e.candidates[k] = &candidate{svc: svc, viaSelector: true, emptyIntent: true}
	}
}

// selectorAffinity scores how plausibly a zero-result selector was
// aimed at pods labeled like tpl. -1 means it cannot have been: some
// selector key is absent from the pod labels entirely, so the two are
// written in different label vocabularies.
//
// Key membership on its own is NOT evidence, which is what this
// heuristic used to test (#332). In any namespace whose workloads
// follow one convention — `app`, `app.kubernetes.io/name` — every
// selector shares its keys with every workload's pods, so a single
// broken Service was reported against every sound workload in the
// namespace with each one's name stamped on it. Values are the only
// thing that discriminates.
//
// A zero-scoring selector is deliberately not a candidate: it shares
// the vocabulary and agrees on nothing, which describes some other
// workload's Service. Detecting a broken Service nobody can be shown
// to own is a target-free question, not this workload's.
func selectorAffinity(selector, tpl map[string]string) int {
	if len(selector) == 0 || len(tpl) == 0 {
		return -1
	}
	score := 0
	for k, want := range selector {
		got, ok := tpl[k]
		if !ok {
			return -1
		}
		switch {
		case want == got:
			// This pair agrees; a different key is what broke the
			// selection. The strongest evidence available.
			score += 4
		case relatedValues(want, got):
			score += 2
		}
	}
	return score
}

// relatedValues reports whether two label values look like versions of
// one name rather than two different names: one a prefix of the other
// at a separator boundary. That is the `api` → `api-v2` rename this
// heuristic exists to catch, and it rejects `unplugged-00-v2` against
// `wired-00` — the case that made every workload claim every broken
// Service. The boundary requirement keeps `api` from matching
// `apiserver`, which are two workloads, not one renamed.
func relatedValues(a, b string) bool {
	long, short := a, b
	if len(short) > len(long) {
		long, short = short, long
	}
	if short == "" || !strings.HasPrefix(long, short) {
		return false
	}
	switch long[len(short)] {
	case '-', '.', '_':
		return true
	}
	return false
}

// bestOwnerInNamespace reports whether the scanned workload is as
// plausible an owner of a zero-selecting Service as anything else in
// its namespace — insurance for the shapes value proximity cannot
// separate on its own.
//
// Ties are allowed through rather than suppressed: two workloads that
// tie scored identically against the same selector, which means their
// pods carry the same labels, which means the Service really was aimed
// at both and repairing it would select both. Only a strictly better
// match elsewhere disqualifies this one.
//
// Other workloads are scored from their declared pod templates while
// the scanned workload is scored from its live pods when it has any
// (templateLabels). The asymmetry only ever makes this workload harder
// to disqualify, so the guard cannot suppress a finding that the
// value-proximity test already earned.
func (e *edgeScan) bestOwnerInNamespace(svc *corev1.Service, score int) bool {
	self := e.wl.Kind + "/" + key(e.wl.Namespace, e.wl.Name)
	for _, tk := range sortedKeys(e.ix.templates) {
		if tk == self {
			continue
		}
		// templates are keyed "Kind/ns/name"; the Services considered
		// here are already namespace-local to the workload.
		if _, rest, ok := strings.Cut(tk, "/"); !ok || !strings.HasPrefix(rest, e.wl.Namespace+"/") {
			continue
		}
		if selectorAffinity(svc.Spec.Selector, e.ix.templates[tk].labels) > score {
			return false
		}
	}
	return true
}

// selectorDiff renders why a selector missed the workload's pods: the
// keys it agrees on are noise, the ones it disagrees on are the fix.
// "the pods carry the same label keys" — the message this replaces —
// was both true and useless, since sharing `app` is what every
// workload in a namespace does.
func selectorDiff(selector, tpl map[string]string) string {
	var diffs []string
	for _, k := range sortedKeys(selector) {
		got, ok := tpl[k]
		if !ok || got == selector[k] {
			continue
		}
		diffs = append(diffs, fmt.Sprintf("%s=%s", k, got))
	}
	if len(diffs) == 0 {
		return ""
	}
	return strings.Join(diffs, ",")
}

// selectedPods returns the pods a service's selector currently
// selects, from the graph's Selects edges.
func (e *edgeScan) selectedPods(svc *corev1.Service) []string {
	id, ok := e.snap.Lookup(graph.KindService, svc.Namespace, svc.Name)
	if !ok {
		return nil
	}
	var pods []string
	for _, edge := range e.snap.Out(id) {
		if edge.Kind != graph.EdgeSelects {
			continue
		}
		if ref, ok := e.snap.Resolve(edge.To); ok {
			pods = append(pods, key(ref.Namespace, ref.Name))
		}
	}
	sort.Strings(pods)
	return pods
}

func selectorString(selector map[string]string) string {
	return labels.SelectorFromSet(labels.Set(selector)).String()
}

func (e *edgeScan) checkSelectors() {
	for _, k := range sortedKeys(e.candidates) {
		c := e.candidates[k]
		if c.emptyIntent {
			diff := selectorDiff(c.svc.Spec.Selector, e.templateLabels())
			msg := fmt.Sprintf("selector %s selects no pods", selectorString(c.svc.Spec.Selector))
			details := append(e.workloadDetail(),
				emit.Field{Key: "selector", Value: selectorString(c.svc.Spec.Selector)})
			// Name the disagreement rather than the agreement: the
			// value the pods actually carry IS the fix (#332).
			if diff != "" {
				msg += fmt.Sprintf(" (the workload's pods carry %s)", diff)
				details = append(details, emit.Field{Key: "pod_labels", Value: diff})
			}
			e.add(emit.Finding{
				Kind:         "edge.selector_empty",
				Severity:     emit.SeverityCritical,
				Namespace:    c.svc.Namespace,
				KindOfObject: "Service",
				Name:         c.svc.Name,
				Reason:       "NoMatchingPods",
				Message:      msg,
				Details:      details,
			})
			continue
		}
		if !c.viaSelector {
			continue
		}
		c.selected = e.selectedPods(c.svc)
		for _, pk := range c.selected {
			if p := e.ix.pods[pk]; p != nil && podReady(p) {
				c.readyPods++
			}
		}
		if c.readyPods < len(c.selected) {
			sev := emit.SeverityWarning
			if c.readyPods == 0 {
				sev = emit.SeverityCritical
			}
			e.add(emit.Finding{
				Kind:         "edge.selector_unready",
				Severity:     sev,
				Namespace:    c.svc.Namespace,
				KindOfObject: "Service",
				Name:         c.svc.Name,
				Reason:       "PodsNotReady",
				Message:      fmt.Sprintf("service selects %d pod(s), %d ready", len(c.selected), c.readyPods),
				Details: []emit.Field{
					{Key: "workload", Value: e.wl.String()},
					{Key: "selector", Value: selectorString(c.svc.Spec.Selector)},
					{Key: "selected", Value: strconv.Itoa(len(c.selected))},
					{Key: "ready", Value: strconv.Itoa(c.readyPods)},
				},
			})
		}
	}
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Service → EndpointSlice → Pod: edge.endpoints_missing,
// edge.endpoints_orphaned, edge.endpoints_unready
// ---------------------------------------------------------------------------

func (e *edgeScan) checkEndpoints() {
	for _, k := range sortedKeys(e.candidates) {
		c := e.candidates[k]
		if c.emptyIntent {
			continue // no pods selected; slices are not expected
		}
		epSlices := e.ix.slicesByService[k]
		if len(epSlices) == 0 {
			if c.viaSelector {
				e.add(emit.Finding{
					Kind:         "edge.endpoints_missing",
					Severity:     emit.SeverityCritical,
					Namespace:    c.svc.Namespace,
					KindOfObject: "Service",
					Name:         c.svc.Name,
					Reason:       "NoEndpointSlices",
					Message:      fmt.Sprintf("no endpointslices exist for a service selecting %d pod(s) — no traffic can flow", len(c.selected)),
					Details: []emit.Field{
						{Key: "workload", Value: e.wl.String()},
						{Key: "selected", Value: strconv.Itoa(len(c.selected))},
					},
				})
			}
			continue
		}
		total, ready := 0, 0
		for _, sl := range epSlices {
			for i := range sl.Endpoints {
				ep := &sl.Endpoints[i]
				total++
				if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
					ready++
				}
				if ep.TargetRef == nil || ep.TargetRef.Kind != "Pod" || ep.TargetRef.Name == "" {
					continue
				}
				if _, exists := e.ix.pods[key(sl.Namespace, ep.TargetRef.Name)]; !exists {
					e.add(emit.Finding{
						Kind:         "edge.endpoints_orphaned",
						Severity:     emit.SeverityWarning,
						Namespace:    sl.Namespace,
						KindOfObject: "EndpointSlice",
						Name:         sl.Name,
						Reason:       "OrphanedEndpoint",
						Message:      fmt.Sprintf("endpoint targets pod %s, which no longer exists", ep.TargetRef.Name),
						Details: []emit.Field{
							{Key: "workload", Value: e.wl.String()},
							{Key: "service", Value: c.svc.Name},
							{Key: "pod", Value: ep.TargetRef.Name},
						},
					})
				}
			}
		}
		mismatch := false
		var msg string
		if c.viaSelector {
			// Readiness itself is reported by edge.selector_unready;
			// here the question is whether the endpoint state
			// *disagrees* with the selected pods (stale or lagging
			// slices).
			mismatch = total != len(c.selected) || ready != c.readyPods
			msg = fmt.Sprintf("%d/%d endpoints ready across %d slice(s); selector selects %d pod(s), %d ready",
				ready, total, len(epSlices), len(c.selected), c.readyPods)
		} else if ready < total {
			mismatch = true
			msg = fmt.Sprintf("%d/%d endpoints ready across %d slice(s)", ready, total, len(epSlices))
		}
		if mismatch {
			sev := emit.SeverityWarning
			if ready == 0 {
				sev = emit.SeverityCritical
			}
			details := []emit.Field{
				{Key: "workload", Value: e.wl.String()},
				{Key: "endpoints", Value: strconv.Itoa(total)},
				{Key: "ready", Value: strconv.Itoa(ready)},
				{Key: "slices", Value: strconv.Itoa(len(epSlices))},
			}
			if c.viaSelector {
				details = append(details, emit.Field{Key: "selected", Value: strconv.Itoa(len(c.selected))})
			}
			e.add(emit.Finding{
				Kind:         "edge.endpoints_unready",
				Severity:     sev,
				Namespace:    c.svc.Namespace,
				KindOfObject: "Service",
				Name:         c.svc.Name,
				Reason:       "EndpointMismatch",
				Message:      msg,
				Details:      details,
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Ingress → Service: edge.backend_missing
// ---------------------------------------------------------------------------

// checkIngresses validates every backend of the Ingresses routing to
// the workload's services, and records their TLS secrets for the
// expiry check.
func (e *edgeScan) checkIngresses() {
	for _, ing := range e.ix.ingresses {
		if !e.sweep && ing.Namespace != e.wl.Namespace {
			continue
		}
		type backendAt struct {
			b          *netv1.IngressBackend
			host, path string
		}
		var backends []backendAt
		if ing.Spec.DefaultBackend != nil {
			backends = append(backends, backendAt{b: ing.Spec.DefaultBackend})
		}
		for i := range ing.Spec.Rules {
			rule := &ing.Spec.Rules[i]
			if rule.HTTP == nil {
				continue
			}
			for j := range rule.HTTP.Paths {
				backends = append(backends, backendAt{b: &rule.HTTP.Paths[j].Backend, host: rule.Host, path: rule.HTTP.Paths[j].Path})
			}
		}
		routesToWorkload := false
		for _, ba := range backends {
			if ba.b.Service == nil {
				continue
			}
			if _, ok := e.candidates[key(ing.Namespace, ba.b.Service.Name)]; ok {
				routesToWorkload = true
				break
			}
		}
		// A sweep has no workload to route to: every Ingress in scope
		// is in scope, which is what makes a backend or class that
		// resolves to nothing findable at all (#331).
		if !routesToWorkload && !e.sweep {
			continue
		}

		e.checkIngressClass(ing)

		for _, ba := range backends {
			if ba.b.Service == nil {
				continue // resource backends are out of scope here
			}
			name := ba.b.Service.Name
			details := func(port string) []emit.Field {
				d := append(e.workloadDetail(),
					emit.Field{Key: "service", Value: name})
				if port != "" {
					d = append(d, emit.Field{Key: "port", Value: port})
				}
				d = append(d,
					emit.Field{Key: "host", Value: ba.host},
					emit.Field{Key: "path", Value: ba.path})
				return d
			}
			svc := e.ix.services[key(ing.Namespace, name)]
			if svc == nil {
				e.add(emit.Finding{
					Kind:         "edge.backend_missing",
					Severity:     emit.SeverityCritical,
					Namespace:    ing.Namespace,
					KindOfObject: "Ingress",
					Name:         ing.Name,
					Reason:       "BackendServiceMissing",
					Message:      fmt.Sprintf("backend service %s not found", name),
					Details:      details(""),
				})
				continue
			}
			port, ok := backendPort(svc, ba.b.Service.Port)
			if !ok {
				e.add(emit.Finding{
					Kind:         "edge.backend_missing",
					Severity:     emit.SeverityCritical,
					Namespace:    ing.Namespace,
					KindOfObject: "Ingress",
					Name:         ing.Name,
					Reason:       "BackendPortMissing",
					Message:      fmt.Sprintf("backend service %s has no port %s", name, port),
					Details:      details(port),
				})
			}
		}

		for i := range ing.Spec.TLS {
			if sec := ing.Spec.TLS[i].SecretName; sec != "" {
				k := key(ing.Namespace, sec)
				if _, dup := e.ingressTLS[k]; !dup {
					e.ingressTLS[k] = ing.Name
				}
			}
		}
	}
}

// legacyIngressClassAnnotation is the pre-1.18 way of choosing a
// controller. Controllers still honour it, so an Ingress carrying it
// is classed even with no ingressClassName, and reporting it as
// unserved would be wrong.
const legacyIngressClassAnnotation = "kubernetes.io/ingress.class"

// builtinIngressClasses are class names a controller honours WITHOUT a
// matching IngressClass object existing. GKE's built-in load-balancer
// controller claims both by name, so "the object does not exist" is
// not evidence of anything there. Keep this list short: every entry is
// a detection hole, and it is only justified where the controller is
// part of the platform rather than something an operator installed.
var builtinIngressClasses = map[string]bool{
	"gce":          true,
	"gce-internal": true,
}

// checkIngressClass answers whether anything is configured to serve
// this Ingress at all. Both failures produce the same silence: the
// object is accepted, its status stays empty, no controller ever
// claims it, and the only symptom is that the hostname resolves to
// nothing. There is no event and no condition to find, which is what
// makes it worth a finding.
func (e *edgeScan) checkIngressClass(ing *netv1.Ingress) {
	if ing.Annotations[legacyIngressClassAnnotation] != "" {
		return // pre-1.18 selection; controllers still honour it
	}
	details := append(e.workloadDetail(), emit.Field{Key: "ingress", Value: ing.Name})

	if name := ing.Spec.IngressClassName; name != nil && *name != "" {
		if e.ix.ingressClasses[*name] || builtinIngressClasses[*name] {
			return
		}
		msg := fmt.Sprintf("ingressClassName %s does not exist — no controller will serve this ingress", *name)
		if have := sortedKeys(e.ix.ingressClasses); len(have) > 0 {
			msg += " (cluster has: " + strings.Join(have, ", ") + ")"
		}
		e.add(emit.Finding{
			Kind:         "edge.missing_ref",
			Severity:     emit.SeverityCritical,
			Namespace:    ing.Namespace,
			KindOfObject: "IngressClass",
			Name:         *name,
			Reason:       "MissingIngressClass",
			Message:      msg,
			Details:      details,
		})
		return
	}

	// No class named, so the cluster default applies — if there is
	// one. More than one default is a cluster-scoped misconfiguration
	// rather than this workload's problem, so it is not reported here.
	if len(e.ix.defaultIngressClasses) > 0 {
		return
	}
	// Warning, not critical, and hedged in the message: a controller
	// may still claim an unclassed Ingress by convention rather than
	// by declaring itself the default (GKE's does). We can see that
	// nothing declares itself the default; we cannot see that nothing
	// serves it, so the finding must not claim to.
	e.add(emit.Finding{
		Kind:         "edge.unclassed",
		Severity:     emit.SeverityWarning,
		Namespace:    ing.Namespace,
		KindOfObject: "Ingress",
		Name:         ing.Name,
		Reason:       "NoIngressClass",
		Message:      "ingress names no class and no ingressclass declares itself the cluster default — unless a controller claims it by convention, nothing serves it",
		Details:      details,
	})
}

// backendPort resolves an Ingress backend port against the service's
// ports; the string form is returned for the finding either way.
func backendPort(svc *corev1.Service, p netv1.ServiceBackendPort) (string, bool) {
	if p.Name != "" {
		for i := range svc.Spec.Ports {
			if svc.Spec.Ports[i].Name == p.Name {
				return p.Name, true
			}
		}
		return p.Name, false
	}
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Port == p.Number {
			return strconv.Itoa(int(p.Number)), true
		}
	}
	return strconv.Itoa(int(p.Number)), false
}

// ---------------------------------------------------------------------------
// TLS expiry: edge.cert_expired, edge.cert_expiring, edge.cert_invalid
// ---------------------------------------------------------------------------

// checkCerts examines every kubernetes.io/tls Secret reachable from
// the workload — mounted by its pods or named by an Ingress routing
// to its services. Findings carry only subject / NotAfter / days
// left; certificate and key bytes never enter a finding (§6.5 is a
// second net, not the mechanism).
func (e *edgeScan) checkCerts() {
	names := map[string]bool{}
	for k := range e.secretRefs {
		names[k] = true
	}
	for k := range e.ingressTLS {
		names[k] = true
	}
	for _, k := range sortedKeys(names) {
		ingress, fromIngress := e.ingressTLS[k]
		via := "mount"
		if !e.secretRefs[k] {
			via = "ingress"
		}
		baseDetails := func() []emit.Field {
			d := append(e.workloadDetail(), emit.Field{Key: "via", Value: via})
			if fromIngress {
				d = append(d, emit.Field{Key: "ingress", Value: ingress})
			}
			return d
		}

		s := e.ix.secrets[k]
		if s == nil {
			// A mounted-but-missing secret is already reported by
			// checkRefs; a missing Ingress TLS secret is reported
			// here — it is an edge of the routing chain.
			if fromIngress && !e.secretRefs[k] {
				e.add(emit.Finding{
					Kind:         "edge.missing_ref",
					Severity:     emit.SeverityCritical,
					Namespace:    namespaceOf(k),
					KindOfObject: "Secret",
					Name:         nameOf(k),
					Reason:       "MissingTLSSecret",
					Message:      fmt.Sprintf("TLS secret %s referenced by ingress %s not found", nameOf(k), ingress),
					Details:      baseDetails(),
				})
			}
			continue
		}
		if s.Type != corev1.SecretTypeTLS {
			if fromIngress {
				e.add(emit.Finding{
					Kind:         "edge.cert_invalid",
					Severity:     emit.SeverityWarning,
					Namespace:    s.Namespace,
					KindOfObject: "Secret",
					Name:         s.Name,
					Reason:       "InvalidCertificate",
					Message:      fmt.Sprintf("ingress TLS secret is type %s, want kubernetes.io/tls", s.Type),
					Details:      baseDetails(),
				})
			}
			continue // non-TLS mounted secrets have no cert to check
		}

		block, _ := pem.Decode(s.Data[corev1.TLSCertKey])
		var cert *x509.Certificate
		if block != nil && block.Type == "CERTIFICATE" {
			cert, _ = x509.ParseCertificate(block.Bytes)
		}
		if cert == nil {
			e.add(emit.Finding{
				Kind:         "edge.cert_invalid",
				Severity:     emit.SeverityWarning,
				Namespace:    s.Namespace,
				KindOfObject: "Secret",
				Name:         s.Name,
				Reason:       "InvalidCertificate",
				Message:      "tls.crt does not contain a parseable X.509 certificate",
				Details:      baseDetails(),
			})
			continue
		}

		subject := cert.Subject.CommonName
		if subject == "" {
			subject = cert.Subject.String()
		}
		days := int(math.Floor(cert.NotAfter.Sub(e.now).Hours() / 24))
		certDetails := func() []emit.Field {
			return append(baseDetails(),
				emit.Field{Key: "subject", Value: subject},
				emit.Field{Key: "not_after", Value: cert.NotAfter.UTC().Format(time.RFC3339)},
				emit.Field{Key: "days_left", Value: strconv.Itoa(days)})
		}
		switch {
		case cert.NotAfter.Before(e.now):
			e.add(emit.Finding{
				Kind:         "edge.cert_expired",
				Severity:     emit.SeverityCritical,
				Namespace:    s.Namespace,
				KindOfObject: "Secret",
				Name:         s.Name,
				Reason:       "CertificateExpired",
				Message:      fmt.Sprintf("certificate expired %dd ago", -days),
				Details:      certDetails(),
			})
		case cert.NotAfter.Sub(e.now) <= e.certWarn:
			e.add(emit.Finding{
				Kind:         "edge.cert_expiring",
				Severity:     emit.SeverityWarning,
				Namespace:    s.Namespace,
				KindOfObject: "Secret",
				Name:         s.Name,
				Reason:       "CertificateExpiringSoon",
				Message:      fmt.Sprintf("certificate expires in %dd", days),
				Details:      certDetails(),
			})
		}
	}
}

// ---------------------------------------------------------------------------
// RBAC reference integrity: edge.missing_ref (ServiceAccount),
// edge.rbac_dangling
// ---------------------------------------------------------------------------

// checkRBAC stays at reference-integrity level (does the
// ServiceAccount exist; do bindings naming it point at existing
// roles) — permission *analysis* is deliberately out of scope.
func (e *edgeScan) checkRBAC() {
	sas := map[string]bool{}
	for _, p := range e.pods {
		sa := p.Spec.ServiceAccountName
		if sa == "" {
			sa = "default"
		}
		sas[sa] = true
	}
	if len(e.pods) == 0 {
		if tpl, ok := e.ix.templates[e.wl.Kind+"/"+key(e.wl.Namespace, e.wl.Name)]; ok {
			sa := tpl.serviceAccount
			if sa == "" {
				sa = "default"
			}
			sas[sa] = true
		}
	}

	for _, sa := range sortedKeys(sas) {
		if e.ix.serviceAccounts[key(e.wl.Namespace, sa)] == nil {
			e.add(emit.Finding{
				Kind:         "edge.missing_ref",
				Severity:     emit.SeverityCritical,
				Namespace:    e.wl.Namespace,
				KindOfObject: "ServiceAccount",
				Name:         sa,
				Reason:       "FailedCreate",
				Message:      "serviceaccount not found — new pods cannot be created",
				Details: []emit.Field{
					{Key: "workload", Value: e.wl.String()},
					{Key: "service_account", Value: sa},
				},
			})
		}
	}

	dangling := func(kindOfObject, ns, name, sa, refKind, refName string) {
		e.add(emit.Finding{
			Kind:         "edge.rbac_dangling",
			Severity:     emit.SeverityWarning,
			Namespace:    ns,
			KindOfObject: kindOfObject,
			Name:         name,
			Reason:       "DanglingRoleRef",
			Message:      fmt.Sprintf("roleRef %s %s not found (binds serviceaccount %s)", refKind, refName, sa),
			Details: []emit.Field{
				{Key: "workload", Value: e.wl.String()},
				{Key: "service_account", Value: sa},
				{Key: "role_ref", Value: refKind + "/" + refName},
			},
		})
	}

	for _, rb := range e.ix.roleBindings {
		sa := matchedServiceAccount(rb.Subjects, sas, e.wl.Namespace, rb.Namespace)
		if sa == "" {
			continue
		}
		switch rb.RoleRef.Kind {
		case "Role":
			if !e.ix.roles[key(rb.Namespace, rb.RoleRef.Name)] {
				dangling("RoleBinding", rb.Namespace, rb.Name, sa, "Role", rb.RoleRef.Name)
			}
		case "ClusterRole":
			if !e.ix.clusterRoles[rb.RoleRef.Name] {
				dangling("RoleBinding", rb.Namespace, rb.Name, sa, "ClusterRole", rb.RoleRef.Name)
			}
		}
	}
	for _, crb := range e.ix.clusterRoleBindings {
		sa := matchedServiceAccount(crb.Subjects, sas, e.wl.Namespace, "")
		if sa == "" {
			continue
		}
		if crb.RoleRef.Kind == "ClusterRole" && !e.ix.clusterRoles[crb.RoleRef.Name] {
			dangling("ClusterRoleBinding", "", crb.Name, sa, "ClusterRole", crb.RoleRef.Name)
		}
	}
}

// matchedServiceAccount returns the first subject naming one of the
// workload's ServiceAccounts. A RoleBinding subject with an empty
// namespace refers to the binding's own namespace; ClusterRoleBinding
// subjects must name the namespace explicitly (bindingNS == "").
func matchedServiceAccount(subjects []rbacv1.Subject, sas map[string]bool, saNS, bindingNS string) string {
	for i := range subjects {
		s := &subjects[i]
		if s.Kind != "ServiceAccount" || !sas[s.Name] {
			continue
		}
		if s.Namespace == saNS || (s.Namespace == "" && bindingNS == saNS) {
			return s.Name
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// namespaceOf splits the namespace out of an ns/name index key.
func namespaceOf(k string) string {
	ns, _, _ := strings.Cut(k, "/")
	return ns
}

// nameOf splits the name out of an ns/name index key.
func nameOf(k string) string {
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '/' {
			return k[i+1:]
		}
	}
	return k
}
