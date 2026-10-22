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

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/crd"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(KCCCommand(KCCDeps{}))
}

// KCCDeps aliases Deps so callers can pass either state.Deps or state.KCCDeps.
type KCCDeps = Deps

// kccDeletionStuckThreshold is how long a Config Connector object can
// carry a deletionTimestamp without a DeleteFailed reason before we
// report it as stuck terminating.
const kccDeletionStuckThreshold = 5 * time.Minute

// KCCCommand builds the `lookout state kcc` command.
func KCCCommand(deps KCCDeps) checks.Command {
	return checks.Command{
		Name:    "state kcc",
		MCPName: "k8s_state_kcc",
		Summary: "When GCP resources managed by Config Connector stop reconciling or wedge on deletion — inspect *.cnrm.cloud.google.com objects for UpdateFailed, DependencyNotReady, ManagementConflict, DeleteFailed/stuck finalizers, and generation drift. Degrades explicitly on clusters without Config Connector installed.",
		Kinds: []checks.KindField{
			checks.Kind("kcc.not_ready", "a Config Connector object is not Ready, stuck terminating, or has an unreconciled spec generation", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind("kcc.unavailable", "no Config Connector (*.cnrm.cloud.google.com) CRDs are served by the cluster, so nothing was examined (§11: no coverage lies)", emit.SeverityInfo),
			UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "gvr", Doc: "GroupVersionResource of the Config Connector object (group/version/resource)"},
			{Name: "api_version", Doc: "apiVersion of the Config Connector object"},
			{Name: "condition_reason", Doc: "the Ready condition's reason reported by the Config Connector controller"},
			{Name: "observed_generation", Doc: "status.observedGeneration last reconciled by Config Connector"},
			{Name: "generation", Doc: "metadata.generation of the object"},
			{Name: "age", Doc: "how long the object has been terminating (DeletionBlocked) or existed"},
			{Name: "unavailable", Doc: "summary-line note when Config Connector CRDs are not installed (kcc-not-installed)"},
		}, UnreadFields()...),
		Examples: []string{
			"lookout state kcc",
			"lookout state kcc --namespace=prod",
			"lookout state kcc --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			if !inv.Scope.Workload.IsZero() {
				return 0, emit.UsageErrorf("state kcc scans Config Connector resources cluster-wide; scope with --namespace")
			}

			client, err := deps.client(ctx)
			if err != nil {
				return 0, err
			}

			gvrs, err := discoverKCCResources(client.Discovery())
			if err != nil {
				return 0, err
			}
			if len(gvrs) == 0 {
				if err := inv.Out.Emit(emit.Finding{
					Kind:     "kcc.unavailable",
					Severity: emit.SeverityInfo,
					Reason:   "CRDNotInstalled",
					Message:  "Config Connector is not installed: no *.cnrm.cloud.google.com API groups are served by this cluster",
				}); err != nil {
					return 0, err
				}
				if err := inv.Out.Note("unavailable", "kcc-not-installed"); err != nil {
					return 0, err
				}
				return 0, nil
			}

			dyn, err := deps.dynamic(ctx)
			if err != nil {
				return 0, err
			}

			ns := inv.Scope.Namespace
			if ns == "" || inv.Scope.AllNamespaces {
				ns = metav1.NamespaceAll
			}

			now := deps.now()
			var (
				scanned  int
				refused  []checks.Refusal
				findings []emit.Finding
			)
			for _, gvr := range gvrs {
				items, err := crd.ListAll(ctx, dyn, ns, gvr)
				if err != nil {
					if _, forbidden := ListForbidden(err); forbidden {
						refused = append(refused, checks.Refused("list", gvr.Group, gvr.Resource))
						continue
					}
					return 0, fmt.Errorf("listing %s.%s: %w", gvr.Resource, gvr.Group, err)
				}
				sort.Slice(items, func(i, j int) bool {
					if items[i].GetNamespace() != items[j].GetNamespace() {
						return items[i].GetNamespace() < items[j].GetNamespace()
					}
					if items[i].GetKind() != items[j].GetKind() {
						return items[i].GetKind() < items[j].GetKind()
					}
					return items[i].GetName() < items[j].GetName()
				})
				scanned += len(items)
				for _, obj := range items {
					if f, ok := checkKCCObject(obj, gvr, now); ok {
						findings = append(findings, f)
					}
				}
			}

			for _, r := range refused {
				if err := inv.Out.Emit(checks.RefusedFinding(r, fmt.Sprintf("Config Connector %s.%s objects were not checked", r.Resource, r.Group))); err != nil {
					return 0, err
				}
			}
			for _, f := range findings {
				if err := inv.Out.Emit(f); err != nil {
					return 0, err
				}
			}
			return scanned, nil
		},
	}
}

// discoverKCCResources returns all listable *.cnrm.cloud.google.com
// GroupVersionResources served by the cluster, sorted deterministically.
func discoverKCCResources(disc discovery.DiscoveryInterface) ([]schema.GroupVersionResource, error) {
	if disc == nil {
		return nil, nil
	}
	lists, err := disc.ServerPreferredResources()
	if err != nil && !discovery.IsGroupDiscoveryFailedError(err) && len(lists) == 0 {
		return nil, fmt.Errorf("discovering Config Connector resources: %w", err)
	}
	if len(lists) == 0 {
		_, lists, _ = disc.ServerGroupsAndResources()
	}

	seen := map[schema.GroupVersionResource]bool{}
	var gvrs []schema.GroupVersionResource
	for _, list := range lists {
		if list == nil {
			continue
		}
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		if gv.Group != "cnrm.cloud.google.com" && !strings.HasSuffix(gv.Group, ".cnrm.cloud.google.com") {
			continue
		}
		for _, res := range list.APIResources {
			if strings.Contains(res.Name, "/") {
				continue
			}
			if len(res.Verbs) > 0 && !hasVerb(res.Verbs, "list") {
				continue
			}
			gvr := gv.WithResource(res.Name)
			if !seen[gvr] {
				seen[gvr] = true
				gvrs = append(gvrs, gvr)
			}
		}
	}
	sort.Slice(gvrs, func(i, j int) bool {
		if gvrs[i].Group != gvrs[j].Group {
			return gvrs[i].Group < gvrs[j].Group
		}
		if gvrs[i].Version != gvrs[j].Version {
			return gvrs[i].Version < gvrs[j].Version
		}
		return gvrs[i].Resource < gvrs[j].Resource
	})
	return gvrs, nil
}

func hasVerb(verbs metav1.Verbs, want string) bool {
	for _, v := range verbs {
		if v == want {
			return true
		}
	}
	return false
}

func checkKCCObject(obj *unstructured.Unstructured, gvr schema.GroupVersionResource, now time.Time) (emit.Finding, bool) {
	conds := crd.Conditions(obj.Object, "status", "conditions")
	ready, hasReady := crd.FindCondition(conds, "Ready")
	obsGen, hasObsGen := crd.Int(obj.Object, "status", "observedGeneration")
	gen := obj.GetGeneration()
	delTS := obj.GetDeletionTimestamp()

	kind := obj.GetKind()
	if kind == "" {
		kind = gvr.Resource
	}

	// 1. DeletionBlocked: deletionTimestamp is set and either Ready
	// reports DeleteFailed or the deletion has been pending >= 5m.
	if delTS != nil {
		delAge := now.Sub(delTS.Time)
		if ready.Reason == "DeleteFailed" || delAge >= kccDeletionStuckThreshold {
			msg := fmt.Sprintf("%s/%s is stuck terminating", kind, obj.GetName())
			if ready.Message != "" {
				msg = fmt.Sprintf("%s/%s is stuck terminating (%s): %s", kind, obj.GetName(), ready.Reason, ready.Message)
			} else if delAge > 0 {
				msg = fmt.Sprintf("%s/%s has been terminating for %s without completing finalization", kind, obj.GetName(), delAge.Round(time.Second))
			}
			return buildKCCFinding(obj, kind, gvr, emit.SeverityWarning, "DeletionBlocked", msg, ready, gen, obsGen, hasObsGen, delTS, now), true
		}
	}

	// 2-4. Ready condition is False.
	if hasReady && ready.Status == "False" {
		reason := "UpToDateFailed"
		switch ready.Reason {
		case "DependencyNotReady", "DependencyInvalid":
			reason = "DependencyNotReady"
		case "ManagementConflict":
			reason = "ManagementConflict"
		case "DeleteFailed":
			reason = "DeletionBlocked"
		}
		msg := fmt.Sprintf("%s/%s Ready=False (%s)", kind, obj.GetName(), ready.Reason)
		if ready.Message != "" {
			msg = fmt.Sprintf("%s/%s Ready=False (%s): %s", kind, obj.GetName(), ready.Reason, ready.Message)
		}
		return buildKCCFinding(obj, kind, gvr, emit.SeverityWarning, reason, msg, ready, gen, obsGen, hasObsGen, delTS, now), true
	}

	// 5. ReconcileStalled: observedGeneration < generation while Ready is not False.
	if hasObsGen && gen > 0 && obsGen < gen {
		msg := fmt.Sprintf("%s/%s spec generation %d has not been reconciled (observedGeneration=%d)", kind, obj.GetName(), gen, obsGen)
		return buildKCCFinding(obj, kind, gvr, emit.SeverityInfo, "ReconcileStalled", msg, ready, gen, obsGen, hasObsGen, delTS, now), true
	}

	return emit.Finding{}, false
}

func buildKCCFinding(
	obj *unstructured.Unstructured,
	kind string,
	gvr schema.GroupVersionResource,
	sev emit.Severity,
	reason, msg string,
	ready crd.Condition,
	gen, obsGen int64,
	hasObsGen bool,
	delTS *metav1.Time,
	now time.Time,
) emit.Finding {
	details := []emit.Field{
		{Key: "gvr", Value: gvr.Group + "/" + gvr.Version + "/" + gvr.Resource},
	}
	if apiVer := obj.GetAPIVersion(); apiVer != "" {
		details = append(details, emit.Field{Key: "api_version", Value: apiVer})
	}
	if ready.Reason != "" {
		details = append(details, emit.Field{Key: "condition_reason", Value: ready.Reason})
	}
	if hasObsGen {
		details = append(details, emit.Field{Key: "observed_generation", Value: strconv.FormatInt(obsGen, 10)})
	}
	if gen > 0 {
		details = append(details, emit.Field{Key: "generation", Value: strconv.FormatInt(gen, 10)})
	}
	if delTS != nil && !delTS.IsZero() && now.After(delTS.Time) {
		details = append(details, emit.Field{Key: "age", Value: now.Sub(delTS.Time).Round(time.Second).String()})
	} else if created := obj.GetCreationTimestamp(); !created.IsZero() && now.After(created.Time) {
		details = append(details, emit.Field{Key: "age", Value: now.Sub(created.Time).Round(time.Second).String()})
	}
	return emit.Finding{
		Kind:         "kcc.not_ready",
		Severity:     sev,
		Namespace:    obj.GetNamespace(),
		KindOfObject: kind,
		Name:         obj.GetName(),
		Reason:       reason,
		Message:      msg,
		Details:      details,
	}
}
