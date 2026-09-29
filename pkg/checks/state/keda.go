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

// `state keda`: KEDA's scalers, and every one of them that has
// stopped scaling. A ScaledObject drives an HPA KEDA creates for it;
// a ScaledJob creates Jobs directly. Either way, when the scaler
// cannot resolve what it scales or authenticate to what it measures,
// the workload keeps running at whatever size it last had and nothing
// else in the cluster says so — the Deployment is Available, the HPA
// is quiet, and the queue backs up. It is the second detector on the
// pkg/checks/crd seam, after `state gateway`, and the k8sgpt KEDA
// integration's parity item (#268).
//
// Finding kinds, all warning — scaling has stopped, the workload has
// not:
//
//	keda.missing_target  the ScaledObject's scaleTargetRef names a Deployment/StatefulSet that does not exist
//	keda.missing_auth    a trigger's authenticationRef names a (Cluster)TriggerAuthentication that does not exist
//	keda.not_ready       KEDA reports the scaler Ready=False for any other reason
//
// They are a chain, most specific first, as in `state gateway`: a
// ScaledObject whose target is gone is also not Ready, and saying so
// twice buries the line that names the cause. keda.not_ready is
// status-driven — KEDA is the authority on whether its scaler can
// reach Kafka or Prometheus, and it cannot drift from the scaler
// version the cluster runs — so it covers everything this process
// cannot recompute: a TriggerAuthentication that exists but names a
// Secret key that does not, a metrics endpoint that refuses, an HPA
// someone else already attached to the same target.
//
// Deliberately not judged:
//   - a scaleTargetRef to anything but an apps/v1 Deployment or
//     StatefulSet (Argo Rollouts, custom /scale resources): resolving
//     an arbitrary kind needs a discovery walk per ref, and KEDA's own
//     Ready condition reports it anyway.
//   - the Secrets a TriggerAuthentication names. Reading them would
//     widen this check's footprint to secrets, and not-Ready says the
//     same thing with KEDA's own reason.
//   - a paused scaler (Paused=True): stopped on purpose.
//   - a scaler KEDA has not reconciled yet (no Ready condition).
//
// Healthy scalers are silent (§4.2 zero nominal state).

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
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
	checks.Register(KEDACommand(Deps{}))
}

// The KEDA group this check reads. v1alpha1 is the only version KEDA
// has ever served for these kinds, despite the name.
var (
	kedaGV       = schema.GroupVersion{Group: "keda.sh", Version: "v1alpha1"}
	scaledObjGVR = kedaGV.WithResource("scaledobjects")
	scaledJobGVR = kedaGV.WithResource("scaledjobs")
	trigAuthGVR  = kedaGV.WithResource("triggerauthentications")
	cTrigAuthGVR = kedaGV.WithResource("clustertriggerauthentications")
	kedaGroup    = crd.Group{
		Name:      "KEDA",
		GV:        kedaGV,
		Resources: []string{scaledObjGVR.Resource, scaledJobGVR.Resource, trigAuthGVR.Resource, cTrigAuthGVR.Resource},
		Install:   "install KEDA (keda.sh), or enable a managed offering that bundles it",
	}
)

// KEDACommand builds the `lookout state keda` command.
func KEDACommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "state keda",
		MCPName: "k8s_keda_scalers",
		Summary: "When a KEDA-scaled workload stops scaling — report every ScaledObject or ScaledJob whose target is gone, whose trigger authentication does not exist, or that KEDA itself reports not Ready. Silent, and cheap, on clusters without KEDA installed.",
		Kinds: []checks.KindField{
			checks.Kind("keda.missing_target", "the ScaledObject's scaleTargetRef names a Deployment or StatefulSet that does not exist — nothing is being scaled", emit.SeverityWarning),
			checks.Kind("keda.missing_auth", "a trigger's authenticationRef names a TriggerAuthentication or ClusterTriggerAuthentication that does not exist — the scaler cannot read its metric", emit.SeverityWarning),
			checks.Kind("keda.not_ready", "KEDA reports the scaler Ready=False: scaling has stopped at the current size", emit.SeverityWarning),
			crd.UnavailableKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "target", Doc: "the scaleTargetRef the ScaledObject names, as <Kind>/<name>"},
			{Name: "trigger", Doc: "trigger type (prometheus, kafka, …) whose authenticationRef does not resolve"},
			{Name: "authentication", Doc: "the authenticationRef that does not resolve, as <Kind>/<name>"},
			{Name: "condition", Doc: "the status condition that is not True"},
		}, crd.UnavailableFields()...),
		Examples: []string{
			"lookout state keda",
			"lookout state keda --namespace=prod",
			"lookout state keda --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runKEDA(ctx, deps, inv)
		},
	}
}

func runKEDA(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("state keda scans KEDA scalers cluster-wide; scope with --namespace")
	}
	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	avail := deps.resolver(client).Resolve(kedaGroup)
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
	// --namespace restricts the scalers and their namespaced
	// references; ClusterTriggerAuthentications are cluster-scoped and
	// always listed, because "the one this trigger names is gone" is
	// the finding.
	ns := inv.Scope.Namespace
	if ns == "" || inv.Scope.AllNamespaces {
		ns = metav1.NamespaceAll
	}
	kix, err := listKEDAIndex(ctx, client, dyn, ns, avail)
	if err != nil {
		return 0, err
	}
	findings := kix.findings()
	sort.SliceStable(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.KindOfObject != b.KindOfObject {
			return a.KindOfObject < b.KindOfObject
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
	return kix.scanned, nil
}

// kedaIndex holds the one List pass `state keda` joins over.
type kedaIndex struct {
	scanned int
	avail   crd.Availability

	scaledObjects []*unstructured.Unstructured
	scaledJobs    []*unstructured.Unstructured
	trigAuths     map[string]bool // ns/name
	cTrigAuths    map[string]bool // name

	// targets are the typed reads: the Deployments and StatefulSets a
	// scaleTargetRef may name, keyed <Kind>/<ns>/<name>.
	targets map[string]bool
}

func listKEDAIndex(ctx context.Context, client kubernetes.Interface, dyn dynamic.Interface, ns string, avail crd.Availability) (*kedaIndex, error) {
	kix := &kedaIndex{
		avail:      avail,
		trigAuths:  map[string]bool{},
		cTrigAuths: map[string]bool{},
		targets:    map[string]bool{},
	}
	for _, r := range []struct {
		gvr  schema.GroupVersionResource
		ns   string
		each func(*unstructured.Unstructured)
	}{
		{scaledObjGVR, ns, func(u *unstructured.Unstructured) { kix.scaledObjects = append(kix.scaledObjects, u) }},
		{scaledJobGVR, ns, func(u *unstructured.Unstructured) { kix.scaledJobs = append(kix.scaledJobs, u) }},
		{trigAuthGVR, ns, func(u *unstructured.Unstructured) { kix.trigAuths[key(u.GetNamespace(), u.GetName())] = true }},
		{cTrigAuthGVR, "", func(u *unstructured.Unstructured) { kix.cTrigAuths[u.GetName()] = true }},
	} {
		if !avail.Serves(r.gvr.Resource) {
			continue
		}
		items, err := crd.ListAll(ctx, dyn, r.ns, r.gvr)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			r.each(it)
		}
		kix.scanned += len(items)
	}
	// The workloads are listed only when there are ScaledObjects to
	// resolve: on a cluster with KEDA installed but nothing using it,
	// this check should cost four empty Lists and stop.
	if len(kix.scaledObjects) == 0 {
		return kix, nil
	}
	err := listPages("deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
		l, err := client.AppsV1().Deployments(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(d *appsv1.Deployment) { kix.targets["Deployment/"+key(d.Namespace, d.Name)] = true; kix.scanned++ })
	if err != nil {
		return nil, err
	}
	err = listPages("statefulsets", func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
		l, err := client.AppsV1().StatefulSets(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(s *appsv1.StatefulSet) {
		kix.targets["StatefulSet/"+key(s.Namespace, s.Name)] = true
		kix.scanned++
	})
	if err != nil {
		return nil, err
	}
	return kix, nil
}

func (kix *kedaIndex) findings() []emit.Finding {
	var out []emit.Finding
	for _, so := range kix.scaledObjects {
		out = append(out, kix.scalerFindings(so, "ScaledObject")...)
	}
	for _, sj := range kix.scaledJobs {
		out = append(out, kix.scalerFindings(sj, "ScaledJob")...)
	}
	return out
}

// scalerFindings reports one ScaledObject or ScaledJob: its target
// (ScaledObjects only — a ScaledJob's job template is inline), then
// its triggers' authentication, then KEDA's own verdict, stopping at
// the first link of the chain that is broken.
func (kix *kedaIndex) scalerFindings(u *unstructured.Unstructured, kind string) []emit.Finding {
	ns, name := u.GetNamespace(), u.GetName()
	base := func(k, reason, message string, details ...emit.Field) emit.Finding {
		return emit.Finding{
			Kind:         k,
			Severity:     emit.SeverityWarning,
			Namespace:    ns,
			KindOfObject: kind,
			Name:         name,
			Reason:       reason,
			Message:      message,
			Details:      details,
		}
	}

	if kind == "ScaledObject" {
		if tkind, tname, ok := kedaTarget(u); ok && !kix.targets[tkind+"/"+key(ns, tname)] {
			return []emit.Finding{base("keda.missing_target", "ScaleTargetNotFound",
				fmt.Sprintf("scaleTargetRef names %s %q, which does not exist in namespace %s — nothing is being scaled, and if the workload was renamed, the new one is running unscaled",
					tkind, tname, ns),
				emit.Field{Key: "target", Value: tkind + "/" + tname})}
		}
	}

	var out []emit.Finding
	seen := map[string]bool{}
	for _, tr := range crd.Slice(u.Object, "spec", "triggers") {
		ref := crd.Map(tr, "authenticationRef")
		aname := crd.Str(ref, "name")
		if aname == "" {
			continue
		}
		akind := crd.Str(ref, "kind")
		if akind == "" {
			akind = "TriggerAuthentication"
		}
		exists, judged := kix.authExists(akind, ns, aname)
		if !judged || exists || seen[akind+"/"+aname] {
			continue
		}
		seen[akind+"/"+aname] = true
		ttype := crd.Str(tr, "type")
		out = append(out, base("keda.missing_auth", "TriggerAuthenticationNotFound",
			fmt.Sprintf("the %s trigger authenticates through %s %q, which does not exist — the scaler cannot read its metric, so scaling has stopped at the current size",
				ttype, akind, aname),
			emit.Field{Key: "trigger", Value: ttype},
			emit.Field{Key: "authentication", Value: akind + "/" + aname}))
	}
	if len(out) > 0 {
		return out
	}

	conds := crd.Conditions(u.Object, "status", "conditions")
	if c, ok := crd.FindCondition(conds, "Paused"); ok && c.True() {
		return nil
	}
	if c, ok := crd.FindCondition(conds, "Ready"); ok && c.Status == "False" {
		return []emit.Finding{base("keda.not_ready", "ScalerNotReady",
			fmt.Sprintf("KEDA reports the %s not ready (%s: %s) — scaling has stopped at the current size",
				kind, condReason(c), c.Message),
			emit.Field{Key: "condition", Value: "Ready=" + c.Status})}
	}
	return nil
}

// kedaTarget returns the Deployment or StatefulSet a ScaledObject's
// scaleTargetRef names. ok is false for anything this check does not
// resolve: another group, another kind, or no name at all. KEDA
// defaults an empty apiVersion to apps/v1 and an empty kind to
// Deployment.
func kedaTarget(so *unstructured.Unstructured) (kind, name string, ok bool) {
	ref := crd.Map(so.Object, "spec", "scaleTargetRef")
	name = crd.Str(ref, "name")
	if name == "" {
		return "", "", false
	}
	if av := crd.Str(ref, "apiVersion"); av != "" && av != "apps/v1" {
		return "", "", false
	}
	kind = crd.Str(ref, "kind")
	if kind == "" {
		kind = "Deployment"
	}
	if kind != "Deployment" && kind != "StatefulSet" {
		return "", "", false
	}
	return kind, name, true
}

// authExists resolves an authenticationRef. judged is false when the
// resource it names is not served, or the kind is not one KEDA
// defines: absence the check could not observe is not a finding.
func (kix *kedaIndex) authExists(kind, ns, name string) (exists, judged bool) {
	switch kind {
	case "TriggerAuthentication":
		if !kix.avail.Serves(trigAuthGVR.Resource) {
			return false, false
		}
		return kix.trigAuths[key(ns, name)], true
	case "ClusterTriggerAuthentication":
		if !kix.avail.Serves(cTrigAuthGVR.Resource) {
			return false, false
		}
		return kix.cTrigAuths[name], true
	}
	return false, false
}
