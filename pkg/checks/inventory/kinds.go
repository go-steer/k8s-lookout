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

package inventory

import (
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// kindSpec is one listable resource: where to read it (gvr), what to
// call it in output (kind — the canonical Kind, so a target lands in
// `triage spec` unchanged), and the spellings a caller may use for it.
type kindSpec struct {
	gvr        schema.GroupVersionResource
	kind       string
	namespaced bool
	// aliases are the accepted --kinds spellings beyond the plural
	// resource name and the lowercased kind: kubectl's singular and
	// short names.
	aliases []string
}

// builtins is the resolution table for the kinds an incident normally
// involves. It exists so the default listing needs no discovery round
// trip and cannot be broken by a partially-unreachable aggregated API
// — anything NOT here still resolves, through discovery (resolve
// below), which is what makes `--kinds=certificates` work.
//
// Order is not meaningful here; defaultKinds fixes the listing order.
var builtins = []kindSpec{
	{gvr("apps", "v1", "deployments"), "Deployment", true, []string{"deploy"}},
	{gvr("apps", "v1", "statefulsets"), "StatefulSet", true, []string{"sts"}},
	{gvr("apps", "v1", "daemonsets"), "DaemonSet", true, []string{"ds"}},
	{gvr("apps", "v1", "replicasets"), "ReplicaSet", true, []string{"rs"}},
	{gvr("batch", "v1", "cronjobs"), "CronJob", true, []string{"cj"}},
	{gvr("batch", "v1", "jobs"), "Job", true, nil},
	{gvr("", "v1", "pods"), "Pod", true, []string{"po"}},
	{gvr("", "v1", "services"), "Service", true, []string{"svc"}},
	{gvr("", "v1", "endpoints"), "Endpoints", true, []string{"ep"}},
	{gvr("networking.k8s.io", "v1", "ingresses"), "Ingress", true, []string{"ing"}},
	{gvr("", "v1", "configmaps"), "ConfigMap", true, []string{"cm"}},
	{gvr("", "v1", "secrets"), "Secret", true, nil},
	{gvr("", "v1", "persistentvolumeclaims"), "PersistentVolumeClaim", true, []string{"pvc"}},
	{gvr("autoscaling", "v2", "horizontalpodautoscalers"), "HorizontalPodAutoscaler", true, []string{"hpa"}},
	{gvr("policy", "v1", "poddisruptionbudgets"), "PodDisruptionBudget", true, []string{"pdb"}},
	{gvr("", "v1", "serviceaccounts"), "ServiceAccount", true, []string{"sa"}},
	{gvr("networking.k8s.io", "v1", "networkpolicies"), "NetworkPolicy", true, []string{"netpol"}},
	{gvr("", "v1", "resourcequotas"), "ResourceQuota", true, []string{"quota"}},
	{gvr("", "v1", "limitranges"), "LimitRange", true, []string{"limits"}},
	{gvr("", "v1", "nodes"), "Node", false, []string{"no"}},
	{gvr("", "v1", "namespaces"), "Namespace", false, []string{"ns"}},
	{gvr("", "v1", "persistentvolumes"), "PersistentVolume", false, []string{"pv"}},
}

func gvr(group, version, resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}

// defaultKinds is what a bare call enumerates, IN OUTPUT ORDER.
//
// Workloads first, then the routing objects, then configuration, then
// the governance objects, so a listing truncated by --max loses the
// least important things last.
//
// ReplicaSets are out: a namespace accumulates one per Deployment
// revision, they are an implementation detail of a Deployment rather
// than something an operator reasons about, and they would routinely
// be the bulk of the output. `--kinds=replicasets` still asks for
// them. Events are out for a different reason — `triage events` owns
// the event stream, collapsed by (object, reason family), and a raw
// event list here would be both a firehose and a second answer to a
// question another command already answers better.
//
// Every entry is a built-in namespaced resource, so the default set
// cannot fail against a cluster that merely lacks a CRD.
var defaultKinds = []string{
	"deployments", "statefulsets", "daemonsets", "cronjobs", "jobs", "pods",
	"services", "endpoints", "ingresses",
	"configmaps", "secrets", "persistentvolumeclaims",
	"horizontalpodautoscalers", "poddisruptionbudgets",
	"serviceaccounts", "networkpolicies", "resourcequotas", "limitranges",
}

// kindToken is the charset a --kinds entry must match: a resource
// name, optionally group-qualified (`certificates.cert-manager.io`).
// Nothing is interpolated into a shell here — the dynamic client
// takes a GVR — but rejecting `--all`-shaped tokens up front turns a
// typo into one clear usage error instead of a silently short
// listing.
var kindToken = regexp.MustCompile(`^[a-z][a-z0-9.-]*$`)

// resolve maps the requested --kinds tokens (or defaultKinds) to
// listable resources, preserving the caller's order and dropping
// repeats (`pods,po` is one kind, listed once).
//
// The built-in table answers first and covers every default kind;
// only tokens it does not know reach discovery, and discovery runs at
// most once per invocation. A token that resolves nowhere is a usage
// error naming the token — the caller misspelled a kind, and
// returning the other seventeen as if nothing had happened would hide
// that.
func resolve(deps Deps, tokens []string) ([]kindSpec, error) {
	clean := make([]string, 0, len(tokens))
	needDiscovery := false
	for _, raw := range tokens {
		token := strings.ToLower(strings.TrimSpace(raw))
		if token == "" {
			continue
		}
		if !kindToken.MatchString(token) {
			return nil, emit.UsageErrorf("--kinds: %q is not a resource name (want e.g. pods, deploy, or certificates.cert-manager.io)", raw)
		}
		clean = append(clean, token)
		if _, ok := lookupBuiltin(token); !ok {
			needDiscovery = true
		}
	}
	if len(clean) == 0 {
		return nil, emit.UsageErrorf("--kinds: no resource names given")
	}

	served := map[string]kindSpec{}
	if needDiscovery {
		var err error
		if served, err = discoverResources(deps); err != nil {
			return nil, err
		}
	}

	out := make([]kindSpec, 0, len(clean))
	seen := map[schema.GroupVersionResource]bool{}
	for _, token := range clean {
		k, ok := lookupBuiltin(token)
		if !ok {
			if k, ok = served[token]; !ok {
				return nil, emit.UsageErrorf("--kinds: %q is not a resource this cluster serves (qualify as <resource>.<group> if the name is ambiguous)", token)
			}
		}
		if seen[k.gvr] {
			continue
		}
		seen[k.gvr] = true
		out = append(out, k)
	}
	return out, nil
}

// lookupBuiltin resolves a token against the built-in table by plural
// name, lowercased kind, or short name, honoring a `.group` suffix.
func lookupBuiltin(token string) (kindSpec, bool) {
	name, group, qualified := strings.Cut(token, ".")
	for _, k := range builtins {
		if qualified && k.gvr.Group != group {
			continue
		}
		if name == k.gvr.Resource || name == strings.ToLower(k.kind) {
			return k, true
		}
		for _, a := range k.aliases {
			if name == a {
				return k, true
			}
		}
	}
	return kindSpec{}, false
}

// discoverResources indexes everything the cluster serves by every
// spelling kubectl accepts — plural, singular, short name, and the
// lowercased kind — so `--kinds` takes whatever the caller would have
// typed at kubectl. Within a group only the preferred version is
// indexed, mirroring `triage spec`'s dynamic path.
func discoverResources(deps Deps) (map[string]kindSpec, error) {
	dc, err := deps.Discovery()
	if err != nil {
		return nil, err
	}
	groups, lists, err := dc.ServerGroupsAndResources()
	if err != nil && len(lists) == 0 {
		return nil, fmt.Errorf("discovering API resources: %w", err)
	}
	preferred := map[string]string{}
	for _, g := range groups {
		preferred[g.Name] = g.PreferredVersion.GroupVersion
	}
	out := map[string]kindSpec{}
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		if pv := preferred[gv.Group]; pv != "" && pv != list.GroupVersion {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") { // subresource
				continue
			}
			spec := kindSpec{gvr: gv.WithResource(r.Name), kind: r.Kind, namespaced: r.Namespaced}
			names := append([]string{r.Name, r.SingularName, strings.ToLower(r.Kind)}, r.ShortNames...)
			for _, n := range names {
				if n == "" {
					continue
				}
				if gv.Group != "" {
					out[n+"."+gv.Group] = spec
				}
				// A bare name is ambiguous when two groups serve it.
				// The group-qualified spelling above always wins; the
				// bare one keeps the alphabetically first group so the
				// answer is deterministic rather than map-ordered, and
				// the core group (empty name) beats every other.
				if prev, dup := out[n]; !dup || gv.Group < prev.gvr.Group {
					out[n] = spec
				}
			}
		}
	}
	return out, nil
}
