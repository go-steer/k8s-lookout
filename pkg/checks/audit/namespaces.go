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

package audit

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

const (
	kindNoResourceQuota     = "audit.no_resource_quota"
	kindNoLimitRange        = "audit.no_limit_range"
	kindNamespaceOnboarding = "audit.namespace_onboarding_gaps"
)

// Frozen audit.no_resource_quota, audit.no_limit_range, and
// audit.namespace_onboarding_gaps reason tokens.
const (
	reasonNoResourceQuota           = "NoResourceQuota"
	reasonResourceQuotaUsedNearHard = "ResourceQuotaUsedNearHard"
	reasonNoLimitRange              = "NoLimitRange"
	reasonMissingRequiredLabels     = "MissingRequiredLabels"
)

const quotaNearHardThreshold = 0.90

// NamespacesCommand returns the `lookout audit namespaces` command.
func NamespacesCommand(deps Deps) checks.Command {
	flags := []emit.FlagSpec{
		{
			Name:    "required-labels",
			Type:    emit.FlagString,
			Default: "",
			Help:    "comma-separated namespace labels required on every workload-bearing namespace (e.g. owner,team,cost-center)",
		},
		skipSystemNamespacesFlag(),
	}
	flags = append(flags, PolicyFlags()...)
	return checks.Command{
		Name:        "audit namespaces",
		MCPName:     "k8s_audit_namespaces",
		MCPProfiles: []string{"audit"},
		Summary:     "Audit workload-bearing namespaces for ResourceQuotas, LimitRanges, and required onboarding labels. Scope with --namespace or -A.",
		Flags:       flags,
		Kinds: []checks.KindField{
			checks.Kind(kindNoResourceQuota, "workload-bearing namespace has no ResourceQuota or a ResourceQuota is >= 90% of hard limit", emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindNoLimitRange, "workload-bearing namespace has no LimitRange while containers omit resource requests/limits", emit.SeverityInfo),
			checks.Kind(kindNamespaceOnboarding, "workload-bearing namespace is missing required onboarding labels", emit.SeverityWarning, emit.SeverityInfo),
			checks.UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "quota", Doc: "name of the ResourceQuota at or above 90% of its hard limit"},
			{Name: "used", Doc: "current ResourceQuota status.used quantity for the resource"},
			{Name: "hard", Doc: "configured ResourceQuota status.hard quantity for the resource"},
			{Name: "pct", Doc: "integer percentage of status.used relative to status.hard"},
			{Name: "workloads", Doc: "pod templates examined in the namespace"},
			{Name: "missing_labels", Doc: "comma-separated required onboarding labels missing from the Namespace"},
			skippedSystemNamespacesField(),
		}, checks.UnreadFields()...),
		Examples: []string{
			"lookout audit namespaces -A",
			"lookout audit namespaces --namespace=prod --required-labels=owner,team",
			"lookout audit namespaces -A --required-labels=owner,cost-center --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runAuditNamespaces(ctx, deps, inv)
		},
	}
}

func runAuditNamespaces(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit namespaces reports namespace-subject claims (ResourceQuotas, LimitRanges, onboarding labels), so it is scoped by namespace: use --namespace=%s or -A", inv.Scope.Workload.Namespace)
	}
	if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns> or -A")
	}
	pol, err := bindPolicy(inv)
	if err != nil {
		return 0, err
	}
	skipSys := inv.Flags.Bool("skip-system-namespaces")
	cliRequiredLabels := parseCSVFlag(inv.Flags.String("required-labels"))
	requiredLabels := pol.StringSliceParam(kindNamespaceOnboarding, "required_labels", cliRequiredLabels)

	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ctx, snap := ensureSnapshot(ctx, client, listNS)

	templates, tmplRefused, err := listPodTemplates(ctx, client, listNS, true)
	if err != nil {
		return 0, err
	}
	namespaces, nsRefused, err := snap.Namespaces(ctx)
	if err != nil {
		return 0, err
	}
	quotas, rqRefused, err := snap.ResourceQuotas(ctx)
	if err != nil {
		return 0, err
	}
	limitRanges, lrRefused, err := snap.LimitRanges(ctx)
	if err != nil {
		return 0, err
	}

	var refusals []emit.Finding
	for _, r := range tmplRefused {
		refusals = append(refusals, checks.RefusedFinding(r.Refusal, r.kind+" pod templates not audited"))
	}
	if nsRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*nsRefused,
			kindNamespaceOnboarding+" not judged: it reads Namespace labels"))
	}
	if rqRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*rqRefused,
			kindNoResourceQuota+" not judged: it reads ResourceQuota objects"))
	}
	if lrRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*lrRefused,
			kindNoLimitRange+" not judged: it reads LimitRange objects"))
	}
	for _, f := range refusals {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}

	nsByName := make(map[string]*corev1.Namespace, len(namespaces))
	for i := range namespaces {
		ns := &namespaces[i]
		if skipSys && pol.IsSystemNamespace(ns.Name) {
			snap.recordSkippedNS(ns.Name)
			continue
		}
		nsByName[ns.Name] = ns
	}

	byNS := map[string][]podTemplate{}
	candidateSet := map[string]bool{}
	for _, t := range templates {
		if skipSys && pol.IsSystemNamespace(t.namespace) {
			snap.recordSkippedNS(t.namespace)
			continue
		}
		byNS[t.namespace] = append(byNS[t.namespace], t)
		candidateSet[t.namespace] = true
	}

	rqByNS := map[string][]corev1.ResourceQuota{}
	for _, rq := range quotas {
		if skipSys && pol.IsSystemNamespace(rq.Namespace) {
			snap.recordSkippedNS(rq.Namespace)
			continue
		}
		rqByNS[rq.Namespace] = append(rqByNS[rq.Namespace], rq)
		candidateSet[rq.Namespace] = true
	}

	lrByNS := map[string][]corev1.LimitRange{}
	for _, lr := range limitRanges {
		if skipSys && pol.IsSystemNamespace(lr.Namespace) {
			snap.recordSkippedNS(lr.Namespace)
			continue
		}
		lrByNS[lr.Namespace] = append(lrByNS[lr.Namespace], lr)
	}

	if !inv.Scope.AllNamespaces && inv.Scope.Namespace != "" && nsRefused == nil {
		candidateSet[inv.Scope.Namespace] = true
	}

	candidateNames := make([]string, 0, len(candidateSet))
	for nsName := range candidateSet {
		candidateNames = append(candidateNames, nsName)
	}
	sort.Strings(candidateNames)

	var findings []emit.Finding
	for _, nsName := range candidateNames {
		if (inv.Scope.AllNamespaces || skipSys) && pol.IsSystemNamespace(nsName) {
			continue
		}
		tmpls := byNS[nsName]
		workloadCount := len(tmpls)
		workloadBearing := workloadCount > 0 || (!inv.Scope.AllNamespaces && nsName == inv.Scope.Namespace)

		if rqRefused == nil {
			nsQuotas := rqByNS[nsName]
			if workloadBearing && len(nsQuotas) == 0 {
				findings = append(findings, emit.Finding{
					Kind:         kindNoResourceQuota,
					Severity:     emit.SeverityInfo,
					Namespace:    nsName,
					KindOfObject: "Namespace",
					Name:         nsName,
					Reason:       reasonNoResourceQuota,
					Message:      "workload-bearing namespace has no ResourceQuota: any workload in this namespace can consume unbounded cluster capacity",
					Details: []emit.Field{
						{Key: "workloads", Value: itoa(workloadCount)},
					},
				})
			}
			for _, rq := range nsQuotas {
				findings = append(findings, checkQuotaNearHard(nsName, rq)...)
			}
		}

		if lrRefused == nil && workloadBearing && len(lrByNS[nsName]) == 0 && hasUnboundedContainer(tmpls) {
			findings = append(findings, emit.Finding{
				Kind:         kindNoLimitRange,
				Severity:     emit.SeverityInfo,
				Namespace:    nsName,
				KindOfObject: "Namespace",
				Name:         nsName,
				Reason:       reasonNoLimitRange,
				Message:      "workload-bearing namespace has no LimitRange while at least one container omits CPU or memory requests/limits",
				Details: []emit.Field{
					{Key: "workloads", Value: itoa(workloadCount)},
				},
			})
		}

		if nsRefused == nil && workloadBearing && len(requiredLabels) > 0 {
			var nsLabels map[string]string
			if nsObj := nsByName[nsName]; nsObj != nil {
				nsLabels = nsObj.Labels
			}
			var missing []string
			for _, key := range requiredLabels {
				if strings.TrimSpace(nsLabels[key]) == "" {
					missing = append(missing, key)
				}
			}
			if len(missing) > 0 {
				findings = append(findings, emit.Finding{
					Kind:         kindNamespaceOnboarding,
					Severity:     emit.SeverityInfo,
					Namespace:    nsName,
					KindOfObject: "Namespace",
					Name:         nsName,
					Reason:       reasonMissingRequiredLabels,
					Message:      fmt.Sprintf("workload-bearing namespace is missing required onboarding %s: %s", plural(len(missing), "label"), strings.Join(missing, ",")),
					Details: []emit.Field{
						{Key: "missing_labels", Value: strings.Join(missing, ",")},
						{Key: "workloads", Value: itoa(workloadCount)},
					},
				})
			}
		}
	}

	for i := range findings {
		f := &findings[i]
		f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	}

	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if skipSys {
		if err := inv.Out.Note("skipped_system_namespaces", itoa(snap.skippedNSCount())); err != nil {
			return 0, err
		}
	}
	scanned := len(namespaces) + len(templates) + len(quotas) + len(limitRanges)
	return scanned, nil
}

func parseCSVFlag(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if k := strings.TrimSpace(p); k != "" {
			out = append(out, k)
		}
	}
	return out
}

func checkQuotaNearHard(nsName string, rq corev1.ResourceQuota) []emit.Finding {
	if len(rq.Status.Hard) == 0 || len(rq.Status.Used) == 0 {
		return nil
	}
	resNames := make([]string, 0, len(rq.Status.Hard))
	for res := range rq.Status.Hard {
		resNames = append(resNames, string(res))
	}
	sort.Strings(resNames)

	var out []emit.Finding
	for _, name := range resNames {
		res := corev1.ResourceName(name)
		hardQty := rq.Status.Hard[res]
		usedQty, ok := rq.Status.Used[res]
		if !ok {
			continue
		}
		hv := hardQty.AsApproximateFloat64()
		if hv <= 0 {
			continue
		}
		uv := usedQty.AsApproximateFloat64()
		if uv < 0 {
			continue
		}
		ratio := uv / hv
		if ratio < quotaNearHardThreshold {
			continue
		}
		pct := int(math.Round(ratio * 100))
		out = append(out, emit.Finding{
			Kind:         kindNoResourceQuota,
			Severity:     emit.SeverityWarning,
			Namespace:    nsName,
			KindOfObject: "Namespace",
			Name:         nsName,
			Reason:       reasonResourceQuotaUsedNearHard,
			Message:      fmt.Sprintf("ResourceQuota %s has %s at %d%% of hard limit (%s / %s): new pods or scale-ups in this namespace risk admission rejection", rq.Name, name, pct, usedQty.String(), hardQty.String()),
			Details: []emit.Field{
				{Key: "quota", Value: rq.Name},
				{Key: "resource", Value: name},
				{Key: "used", Value: usedQty.String()},
				{Key: "hard", Value: hardQty.String()},
				{Key: "pct", Value: itoa(pct)},
			},
		})
	}
	return out
}

func hasUnboundedContainer(tmpls []podTemplate) bool {
	for _, t := range tmpls {
		for _, c := range t.spec.Containers {
			if containerOmitsRequestsOrLimits(c) {
				return true
			}
		}
	}
	return false
}

func containerOmitsRequestsOrLimits(c corev1.Container) bool {
	reqCPU := c.Resources.Requests[corev1.ResourceCPU]
	reqMem := c.Resources.Requests[corev1.ResourceMemory]
	limCPU := c.Resources.Limits[corev1.ResourceCPU]
	limMem := c.Resources.Limits[corev1.ResourceMemory]
	return reqCPU.IsZero() || reqMem.IsZero() || limCPU.IsZero() || limMem.IsZero()
}
