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
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

const (
	kindDeprecatedAPIInUse = "audit.deprecated_api_in_use"

	reasonRemovedInTargetMinor    = "RemovedInTargetMinor"
	reasonDeprecatedInTargetMinor = "DeprecatedInTargetMinor"
	reasonRemovedAPIInUse         = "RemovedAPIInUse"

	lastAppliedConfigAnnotation = "kubectl.kubernetes.io/last-applied-configuration"
)

type removedAPIEntry struct {
	apiVersion      string
	kind            string
	deprecatedMinor int
	removedMinor    int
	replacement     string
}

// removedAPIs catalogues Kubernetes API versions removed across 1.16–1.32+
// along with the minor version that deprecated them, the minor version that
// stopped serving them, and the stable replacement group/version.
var removedAPIs = []removedAPIEntry{
	// Removed in 1.16
	{apiVersion: "extensions/v1beta1", kind: "Deployment", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "extensions/v1beta1", kind: "DaemonSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "extensions/v1beta1", kind: "ReplicaSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "extensions/v1beta1", kind: "StatefulSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "extensions/v1beta1", kind: "NetworkPolicy", deprecatedMinor: 9, removedMinor: 16, replacement: "networking.k8s.io/v1"},
	{apiVersion: "extensions/v1beta1", kind: "PodSecurityPolicy", deprecatedMinor: 10, removedMinor: 16, replacement: "policy/v1beta1"},
	{apiVersion: "apps/v1beta1", kind: "Deployment", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "apps/v1beta1", kind: "StatefulSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "apps/v1beta1", kind: "ReplicaSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "apps/v1beta2", kind: "Deployment", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "apps/v1beta2", kind: "DaemonSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "apps/v1beta2", kind: "ReplicaSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},
	{apiVersion: "apps/v1beta2", kind: "StatefulSet", deprecatedMinor: 9, removedMinor: 16, replacement: "apps/v1"},

	// Removed in 1.22
	{apiVersion: "networking.k8s.io/v1beta1", kind: "Ingress", deprecatedMinor: 19, removedMinor: 22, replacement: "networking.k8s.io/v1"},
	{apiVersion: "networking.k8s.io/v1beta1", kind: "IngressClass", deprecatedMinor: 19, removedMinor: 22, replacement: "networking.k8s.io/v1"},
	{apiVersion: "extensions/v1beta1", kind: "Ingress", deprecatedMinor: 14, removedMinor: 22, replacement: "networking.k8s.io/v1"},
	{apiVersion: "rbac.authorization.k8s.io/v1beta1", kind: "Role", deprecatedMinor: 17, removedMinor: 22, replacement: "rbac.authorization.k8s.io/v1"},
	{apiVersion: "rbac.authorization.k8s.io/v1beta1", kind: "ClusterRole", deprecatedMinor: 17, removedMinor: 22, replacement: "rbac.authorization.k8s.io/v1"},
	{apiVersion: "rbac.authorization.k8s.io/v1beta1", kind: "RoleBinding", deprecatedMinor: 17, removedMinor: 22, replacement: "rbac.authorization.k8s.io/v1"},
	{apiVersion: "rbac.authorization.k8s.io/v1beta1", kind: "ClusterRoleBinding", deprecatedMinor: 17, removedMinor: 22, replacement: "rbac.authorization.k8s.io/v1"},
	{apiVersion: "admissionregistration.k8s.io/v1beta1", kind: "ValidatingWebhookConfiguration", deprecatedMinor: 16, removedMinor: 22, replacement: "admissionregistration.k8s.io/v1"},
	{apiVersion: "admissionregistration.k8s.io/v1beta1", kind: "MutatingWebhookConfiguration", deprecatedMinor: 16, removedMinor: 22, replacement: "admissionregistration.k8s.io/v1"},
	{apiVersion: "apiextensions.k8s.io/v1beta1", kind: "CustomResourceDefinition", deprecatedMinor: 16, removedMinor: 22, replacement: "apiextensions.k8s.io/v1"},
	{apiVersion: "coordination.k8s.io/v1beta1", kind: "Lease", deprecatedMinor: 14, removedMinor: 22, replacement: "coordination.k8s.io/v1"},

	// Removed in 1.25
	{apiVersion: "batch/v1beta1", kind: "CronJob", deprecatedMinor: 21, removedMinor: 25, replacement: "batch/v1"},
	{apiVersion: "discovery.k8s.io/v1beta1", kind: "EndpointSlice", deprecatedMinor: 21, removedMinor: 25, replacement: "discovery.k8s.io/v1"},
	{apiVersion: "events.k8s.io/v1beta1", kind: "Event", deprecatedMinor: 19, removedMinor: 25, replacement: "events.k8s.io/v1"},
	{apiVersion: "policy/v1beta1", kind: "PodDisruptionBudget", deprecatedMinor: 21, removedMinor: 25, replacement: "policy/v1"},
	{apiVersion: "policy/v1beta1", kind: "PodSecurityPolicy", deprecatedMinor: 21, removedMinor: 25, replacement: "PodSecurity"},
	{apiVersion: "autoscaling/v2beta1", kind: "HorizontalPodAutoscaler", deprecatedMinor: 19, removedMinor: 25, replacement: "autoscaling/v2"},
	{apiVersion: "node.k8s.io/v1beta1", kind: "RuntimeClass", deprecatedMinor: 22, removedMinor: 25, replacement: "node.k8s.io/v1"},

	// Removed in 1.26
	{apiVersion: "autoscaling/v2beta2", kind: "HorizontalPodAutoscaler", deprecatedMinor: 23, removedMinor: 26, replacement: "autoscaling/v2"},
	{apiVersion: "flowcontrol.apiserver.k8s.io/v1beta1", kind: "FlowSchema", deprecatedMinor: 23, removedMinor: 26, replacement: "flowcontrol.apiserver.k8s.io/v1"},
	{apiVersion: "flowcontrol.apiserver.k8s.io/v1beta1", kind: "PriorityLevelConfiguration", deprecatedMinor: 23, removedMinor: 26, replacement: "flowcontrol.apiserver.k8s.io/v1"},

	// Removed in 1.27
	{apiVersion: "storage.k8s.io/v1beta1", kind: "CSIStorageCapacity", deprecatedMinor: 24, removedMinor: 27, replacement: "storage.k8s.io/v1"},
	{apiVersion: "storage.k8s.io/v1beta1", kind: "CSIDriver", deprecatedMinor: 19, removedMinor: 27, replacement: "storage.k8s.io/v1"},
	{apiVersion: "storage.k8s.io/v1beta1", kind: "CSINode", deprecatedMinor: 17, removedMinor: 27, replacement: "storage.k8s.io/v1"},
	{apiVersion: "storage.k8s.io/v1beta1", kind: "StorageClass", deprecatedMinor: 17, removedMinor: 27, replacement: "storage.k8s.io/v1"},
	{apiVersion: "storage.k8s.io/v1beta1", kind: "VolumeAttachment", deprecatedMinor: 17, removedMinor: 27, replacement: "storage.k8s.io/v1"},

	// Removed in 1.29
	{apiVersion: "flowcontrol.apiserver.k8s.io/v1beta2", kind: "FlowSchema", deprecatedMinor: 26, removedMinor: 29, replacement: "flowcontrol.apiserver.k8s.io/v1"},
	{apiVersion: "flowcontrol.apiserver.k8s.io/v1beta2", kind: "PriorityLevelConfiguration", deprecatedMinor: 26, removedMinor: 29, replacement: "flowcontrol.apiserver.k8s.io/v1"},

	// Removed in 1.32
	{apiVersion: "flowcontrol.apiserver.k8s.io/v1beta3", kind: "FlowSchema", deprecatedMinor: 29, removedMinor: 32, replacement: "flowcontrol.apiserver.k8s.io/v1"},
	{apiVersion: "flowcontrol.apiserver.k8s.io/v1beta3", kind: "PriorityLevelConfiguration", deprecatedMinor: 29, removedMinor: 32, replacement: "flowcontrol.apiserver.k8s.io/v1"},
}

func lookupRemovedAPI(apiVersion, objKind string) (removedAPIEntry, bool) {
	if apiVersion == "" {
		return removedAPIEntry{}, false
	}
	for _, e := range removedAPIs {
		if e.apiVersion == apiVersion && strings.EqualFold(e.kind, objKind) {
			return e, true
		}
	}
	for _, e := range removedAPIs {
		if e.apiVersion == apiVersion {
			return e, true
		}
	}
	return removedAPIEntry{}, false
}

type lastAppliedEnvelope struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
}

func extractAppliedAPI(meta metav1.ObjectMeta, tm metav1.TypeMeta, fallbackKind string) (string, string) {
	if raw := strings.TrimSpace(meta.Annotations[lastAppliedConfigAnnotation]); raw != "" {
		if strings.HasPrefix(raw, "{") {
			var env lastAppliedEnvelope
			if err := json.Unmarshal([]byte(raw), &env); err == nil && env.APIVersion != "" {
				k := env.Kind
				if k == "" {
					k = fallbackKind
				}
				return env.APIVersion, k
			}
		} else {
			return raw, fallbackKind
		}
	}
	if tm.APIVersion != "" {
		k := tm.Kind
		if k == "" {
			k = fallbackKind
		}
		return tm.APIVersion, k
	}
	return "", fallbackKind
}

func evaluateObjectDeprecatedAPI(meta metav1.ObjectMeta, tm metav1.TypeMeta, objKind, targetStr string, tv version) []emit.Finding {
	apiVer, resolvedKind := extractAppliedAPI(meta, tm, objKind)
	entry, ok := lookupRemovedAPI(apiVer, resolvedKind)
	if !ok {
		return nil
	}
	removedIn := fmt.Sprintf("1.%d", entry.removedMinor)
	details := []emit.Field{
		{Key: "api_version", Value: entry.apiVersion},
		{Key: "removed_in", Value: removedIn},
		{Key: "replacement", Value: entry.replacement},
		{Key: "target_version", Value: targetStr},
	}

	if tv.major > 1 || (tv.major == 1 && entry.removedMinor > 0 && entry.removedMinor <= tv.minor) {
		depFinding := emit.Finding{
			Kind:         kindDeprecatedAPIInUse,
			Severity:     emit.SeverityCritical,
			KindOfObject: objKind,
			Namespace:    meta.Namespace,
			Name:         meta.Name,
			Reason:       reasonRemovedInTargetMinor,
			Message: fmt.Sprintf("%s %s was last applied with %s, which was removed in Kubernetes %s (target is %s): re-applying this manifest after upgrading will fail; migrate to %s",
				objKind, objectRefName(meta), entry.apiVersion, removedIn, targetStr, entry.replacement),
			Details:     details,
			Fingerprint: engine.PostureFingerprint(kindDeprecatedAPIInUse, reasonRemovedInTargetMinor, objKind),
		}
		blockedFinding := emit.Finding{
			Kind:         kindUpgradeBlocked,
			Severity:     emit.SeverityCritical,
			KindOfObject: objKind,
			Namespace:    meta.Namespace,
			Name:         meta.Name,
			Reason:       reasonRemovedAPIInUse,
			Message: fmt.Sprintf("upgrade to %s is blocked by %s %s using %s (removed in %s): migrate manifests and controllers to %s before upgrading",
				targetStr, objKind, objectRefName(meta), entry.apiVersion, removedIn, entry.replacement),
			Details:     details,
			Fingerprint: engine.PostureFingerprint(kindUpgradeBlocked, reasonRemovedAPIInUse, objKind),
		}
		return []emit.Finding{depFinding, blockedFinding}
	}

	if tv.major == 1 && entry.deprecatedMinor > 0 && entry.deprecatedMinor <= tv.minor && tv.minor < entry.removedMinor {
		return []emit.Finding{{
			Kind:         kindDeprecatedAPIInUse,
			Severity:     emit.SeverityWarning,
			KindOfObject: objKind,
			Namespace:    meta.Namespace,
			Name:         meta.Name,
			Reason:       reasonDeprecatedInTargetMinor,
			Message: fmt.Sprintf("%s %s was last applied with %s, which is deprecated in %s and removed in Kubernetes %s: migrate to %s before reaching %s",
				objKind, objectRefName(meta), entry.apiVersion, targetStr, removedIn, entry.replacement, removedIn),
			Details:     details,
			Fingerprint: engine.PostureFingerprint(kindDeprecatedAPIInUse, reasonDeprecatedInTargetMinor, objKind),
		}}
	}
	return nil
}

func objectRefName(meta metav1.ObjectMeta) string {
	if meta.Namespace != "" {
		return meta.Namespace + "/" + meta.Name
	}
	return meta.Name
}

func deprecatedAPIFindings(ctx context.Context, snap *Snapshot, targetStr string, tv version) []emit.Finding {
	if snap == nil || targetStr == "" {
		return nil
	}
	var out []emit.Finding

	if items, _, err := snap.Deployments(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "Deployment", targetStr, tv)...)
		}
	}
	if items, _, err := snap.StatefulSets(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "StatefulSet", targetStr, tv)...)
		}
	}
	if items, _, err := snap.DaemonSets(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "DaemonSet", targetStr, tv)...)
		}
	}
	if items, _, err := snap.CronJobs(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "CronJob", targetStr, tv)...)
		}
	}
	if items, _, err := snap.Jobs(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "Job", targetStr, tv)...)
		}
	}
	if items, _, err := snap.Services(ctx); err == nil {
		for _, svc := range items {
			out = append(out, evaluateObjectDeprecatedAPI(svc.ObjectMeta, svc.TypeMeta, "Service", targetStr, tv)...)
		}
	}
	if items, _, err := snap.Ingresses(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "Ingress", targetStr, tv)...)
		}
	}
	if items, _, err := snap.NetworkPolicies(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "NetworkPolicy", targetStr, tv)...)
		}
	}
	if items, _, err := snap.PDBs(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "PodDisruptionBudget", targetStr, tv)...)
		}
	}
	if items, _, err := snap.HPAs(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "HorizontalPodAutoscaler", targetStr, tv)...)
		}
	}
	if items, _, err := snap.Roles(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "Role", targetStr, tv)...)
		}
	}
	if items, _, err := snap.RoleBindings(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "RoleBinding", targetStr, tv)...)
		}
	}
	if items, _, err := snap.ClusterRoles(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "ClusterRole", targetStr, tv)...)
		}
	}
	if items, _, err := snap.ClusterRoleBindings(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "ClusterRoleBinding", targetStr, tv)...)
		}
	}
	if items, _, err := snap.ValidatingWebhookConfigurations(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "ValidatingWebhookConfiguration", targetStr, tv)...)
		}
	}
	if items, _, err := snap.MutatingWebhookConfigurations(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "MutatingWebhookConfiguration", targetStr, tv)...)
		}
	}
	if items, _, err := snap.Namespaces(ctx); err == nil {
		for i := range items {
			out = append(out, evaluateObjectDeprecatedAPI(items[i].ObjectMeta, items[i].TypeMeta, "Namespace", targetStr, tv)...)
		}
	}
	return out
}
