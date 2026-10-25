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
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

const (
	// kindAIWorkloadExposure: an AI/inference workload template with a
	// network, identity, storage, or image-pinning posture gap that
	// exposes model weights, serving endpoints, or accelerator nodes.
	kindAIWorkloadExposure = "audit.ai_workload_exposure"
)

const (
	reasonServingPortClusterWideNoNetPol = "ServingPortClusterWideNoNetPol"
	reasonHostNetworkWithAccelerator     = "HostNetworkWithAccelerator"
	reasonSharedModelCacheHostPath       = "SharedModelCacheHostPath"
	reasonDefaultSAWithAccelerator       = "DefaultSAWithAccelerator"
	reasonUnpinnedModelLoaderImage       = "UnpinnedModelLoaderImage"
)

// acceleratorResourceNames are extended resource names that identify
// GPU or TPU accelerator requests/limits on a container.
var acceleratorResourceNames = []corev1.ResourceName{
	"nvidia.com/gpu",
	"amd.com/gpu",
	"google.com/tpu",
}

// acceleratorNodeLabels are node selector / node affinity label keys
// that schedule a pod onto GPU or TPU accelerator nodes.
var acceleratorNodeLabels = []string{
	"cloud.google.com/gke-accelerator",
	"cloud.google.com/gke-tpu-accelerator",
}

// inferenceServerTokens match common model-serving and distributed
// inference runtimes in container images, commands, or args.
var inferenceServerTokens = []string{
	"vllm",
	"tritonserver",
	"text-generation-inference",
	"tgi",
	"ollama",
	"ray",
	"sglang",
	"lmdeploy",
	"torchserve",
	"huggingface",
}

// AICommand builds `lookout audit ai`: inference-workload security and
// scheduling posture across workload templates.
func AICommand(deps Deps) checks.Command {
	return checks.Command{
		Name:           "audit ai",
		MCPName:        "k8s_audit_ai",
		MCPProfiles:    []string{"audit"},
		TimeoutDefault: 30 * time.Second,
		Summary:        "Inference-workload security and scheduling posture across workload templates: serving Services in namespaces with no ingress NetworkPolicy isolating the model endpoint, hostNetwork on accelerator workloads, hostPath model-cache volumes, default ServiceAccount token automount on accelerator workloads, and unpinned model-loader or inference images. Scope with --namespace, -A, or --workload.",
		Flags: append([]emit.FlagSpec{
			skipSystemNamespacesFlag(),
		}, PolicyFlags()...),
		Kinds: []checks.KindField{
			checks.Kind(kindAIWorkloadExposure,
				"an AI/inference workload has an unisolated serving Service, hostNetwork with accelerators, a hostPath model-cache mount, an automounted default ServiceAccount token, or an unpinned model-loader/inference container image",
				emit.SeverityWarning, emit.SeverityInfo),
			checks.UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "service", Doc: "Service(s) selecting the unisolated AI inference workload (ServingPortClusterWideNoNetPol)"},
			{Name: "ports", Doc: "Service port(s) exposed without an ingress NetworkPolicy (ServingPortClusterWideNoNetPol)"},
			{Name: "accelerator", Doc: "accelerator resource or node selector detected on the workload, when present"},
			{Name: "volumes", Doc: "comma-separated name:path pairs (capped at 8) of hostPath volumes mounted by the AI workload (SharedModelCacheHostPath)"},
			{Name: "service_account", Doc: "ServiceAccount name used by the AI workload (DefaultSAWithAccelerator)"},
			{Name: "containers", Doc: "count of containers in the AI workload template with an unpinned image reference (UnpinnedModelLoaderImage)"},
			{Name: "container_names", Doc: "comma-separated container names (capped at 8) with an unpinned image reference (UnpinnedModelLoaderImage)"},
			{Name: "total_containers", Doc: "total containers (init + regular) in the AI workload template (UnpinnedModelLoaderImage)"},
			{Name: "ai_workloads", Doc: "summary-line note: count of AI/inference workload templates identified in the scan scope"},
			skippedSystemNamespacesField(),
		}, checks.UnreadFields()...),
		Examples: []string{
			"lookout audit ai -A",
			"lookout audit ai --namespace=ml",
			"lookout audit ai --workload=Deployment/ml/vllm-llama",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runAI(ctx, deps, inv)
		},
	}
}

func runAI(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		if inv.Scope.AllNamespaces {
			return 0, emit.UsageErrorf("-A and --workload are mutually exclusive")
		}
		if inv.Scope.Namespace != "" && inv.Scope.Namespace != inv.Scope.Workload.Namespace {
			return 0, emit.UsageErrorf("contradictory scope: --namespace=%s vs --workload=%s",
				inv.Scope.Namespace, inv.Scope.Workload)
		}
		switch inv.Scope.Workload.Kind {
		case "Deployment", "StatefulSet", "DaemonSet", "CronJob", "Job", "Pod":
		default:
			return 0, emit.UsageErrorf("--workload kind %q is not a workload template (want Deployment, StatefulSet, DaemonSet, CronJob, Job, or Pod)",
				inv.Scope.Workload.Kind)
		}
	} else if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns>, -A, or --workload=<Kind/[ns/]name>")
	}
	pol, err := bindPolicy(inv)
	if err != nil {
		return 0, err
	}
	skipSys := inv.Flags.Bool("skip-system-namespaces")

	listNS := inv.Scope.Namespace
	if !inv.Scope.Workload.IsZero() {
		listNS = inv.Scope.Workload.Namespace
	}
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ctx, snap := ensureSnapshot(ctx, client, listNS)

	templates, kindRefused, err := listPodTemplates(ctx, client, listNS, inv.Scope.Workload.IsZero())
	if err != nil {
		return 0, err
	}
	for _, r := range kindRefused {
		if err := inv.Out.Emit(checks.RefusedFinding(r.Refusal,
			r.kind+" pod templates not audited")); err != nil {
			return 0, err
		}
	}
	if !inv.Scope.Workload.IsZero() {
		var matched []podTemplate
		for _, t := range templates {
			if t.kind == inv.Scope.Workload.Kind && t.name == inv.Scope.Workload.Name {
				matched = append(matched, t)
			}
		}
		if len(matched) == 0 {
			return 0, fmt.Errorf("%s %s/%s not found",
				inv.Scope.Workload.Kind, listNS, inv.Scope.Workload.Name)
		}
		templates = matched
	}

	if skipSys && inv.Scope.Workload.IsZero() && inv.Scope.Namespace == "" {
		filtered := templates[:0]
		for _, t := range templates {
			if pol.IsSystemNamespace(t.namespace) {
				snap.recordSkippedNS(t.namespace)
				continue
			}
			filtered = append(filtered, t)
		}
		templates = filtered
	}

	var svcs []*corev1.Service
	var svcsOK bool
	if sList, r, err := snap.Services(ctx); err != nil {
		return 0, fmt.Errorf("listing services: %w", err)
	} else if r != nil {
		if err := inv.Out.Emit(checks.RefusedFinding(*r,
			"ServingPortClusterWideNoNetPol not judged: Services could not be listed")); err != nil {
			return 0, err
		}
	} else {
		svcs = sList
		svcsOK = true
	}

	var netpols []networkingv1.NetworkPolicy
	var netpolsOK bool
	if npList, r, err := snap.NetworkPolicies(ctx); err != nil {
		return 0, fmt.Errorf("listing networkpolicies: %w", err)
	} else if r != nil {
		if err := inv.Out.Emit(checks.RefusedFinding(*r,
			"ServingPortClusterWideNoNetPol not judged: NetworkPolicies could not be listed")); err != nil {
			return 0, err
		}
	} else {
		netpols = npList
		netpolsOK = true
	}

	ingressPoliciesByNS := map[string][]networkingv1.NetworkPolicy{}
	for _, np := range netpols {
		if coversDirection(np, networkingv1.PolicyTypeIngress) {
			ingressPoliciesByNS[np.Namespace] = append(ingressPoliciesByNS[np.Namespace], np)
		}
	}

	tags := floatingTags
	if customTags := pol.StringSliceParam(kindAIWorkloadExposure, "floating_tags", nil); len(customTags) > 0 {
		tags = make(map[string]bool, len(customTags))
		for _, tag := range customTags {
			tags[tag] = true
		}
	}

	var aiCount int
	var findings []emit.Finding
	for _, t := range templates {
		isAI, accel := classifyAIWorkload(t.spec)
		if !isAI {
			continue
		}
		aiCount++

		var raw []emit.Finding
		if svcsOK && netpolsOK {
			if f, ok := aiServingPortFinding(t, svcs, ingressPoliciesByNS[t.namespace]); ok {
				raw = append(raw, f)
			}
		}
		if f, ok := aiHostNetworkFinding(t.spec, accel); ok {
			raw = append(raw, f)
		}
		if f, ok := aiSharedModelCacheHostPathFinding(t.spec); ok {
			raw = append(raw, f)
		}
		if f, ok := aiDefaultSAFinding(t.spec, accel); ok {
			raw = append(raw, f)
		}
		if f, ok := aiUnpinnedImageFinding(t.spec, tags); ok {
			raw = append(raw, f)
		}

		for _, f := range raw {
			f.Namespace = t.namespace
			f.KindOfObject = t.kind
			f.Name = t.name
			f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, t.kind)
			findings = append(findings, f)
		}
	}

	sortFindings(findings)
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return len(templates), err
		}
	}
	if err := inv.Out.Note("ai_workloads", itoa(aiCount)); err != nil {
		return len(templates), err
	}
	if skipSys && inv.Scope.Workload.IsZero() && inv.Scope.Namespace == "" {
		if err := inv.Out.Note("skipped_system_namespaces", itoa(snap.skippedNSCount())); err != nil {
			return len(templates), err
		}
	}
	return len(templates), nil
}

// classifyAIWorkload reports whether spec is an AI/inference workload
// and returns a concise accelerator label when one is present.
func classifyAIWorkload(spec corev1.PodSpec) (bool, string) {
	if accel := detectAccelerator(spec); accel != "" {
		return true, accel
	}
	if detectInferenceRuntime(spec) {
		return true, ""
	}
	return false, ""
}

func detectAccelerator(spec corev1.PodSpec) string {
	for _, c := range allContainers(spec) {
		for _, resName := range acceleratorResourceNames {
			if q, ok := c.Resources.Limits[resName]; ok && !q.IsZero() && q.Sign() > 0 {
				return string(resName)
			}
			if q, ok := c.Resources.Requests[resName]; ok && !q.IsZero() && q.Sign() > 0 {
				return string(resName)
			}
		}
	}
	for _, key := range acceleratorNodeLabels {
		if val, ok := spec.NodeSelector[key]; ok && val != "" {
			return key + "=" + val
		}
	}
	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil {
		na := spec.Affinity.NodeAffinity
		if na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			for _, term := range na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				if accel := matchAcceleratorExpressions(term.MatchExpressions); accel != "" {
					return accel
				}
			}
		}
		for _, pref := range na.PreferredDuringSchedulingIgnoredDuringExecution {
			if accel := matchAcceleratorExpressions(pref.Preference.MatchExpressions); accel != "" {
				return accel
			}
		}
	}
	return ""
}

func matchAcceleratorExpressions(exprs []corev1.NodeSelectorRequirement) string {
	for _, req := range exprs {
		for _, key := range acceleratorNodeLabels {
			if req.Key == key && (req.Operator == corev1.NodeSelectorOpIn || req.Operator == corev1.NodeSelectorOpExists) {
				if len(req.Values) > 0 {
					return key + "=" + req.Values[0]
				}
				return key
			}
		}
	}
	return ""
}

func detectInferenceRuntime(spec corev1.PodSpec) bool {
	for _, c := range allContainers(spec) {
		if containsInferenceToken(c.Image) {
			return true
		}
		for _, cmd := range c.Command {
			if containsInferenceToken(cmd) {
				return true
			}
		}
		for _, arg := range c.Args {
			if containsInferenceToken(arg) {
				return true
			}
		}
	}
	return false
}

func containsInferenceToken(s string) bool {
	lower := strings.ToLower(s)
	for _, tok := range inferenceServerTokens {
		if strings.Contains(lower, tok) {
			return true
		}
	}
	return false
}

func aiServingPortFinding(t podTemplate, allSvcs []*corev1.Service, ingressPolicies []networkingv1.NetworkPolicy) (emit.Finding, bool) {
	matching := selectingServices(allSvcs, t.namespace, t.labels)
	if len(matching) == 0 {
		return emit.Finding{}, false
	}
	var matchedSvcs []string
	var matchedPorts []string
	seenPorts := map[string]bool{}
	for _, svc := range matching {
		if len(svc.Spec.Ports) == 0 {
			continue
		}
		matchedSvcs = append(matchedSvcs, svc.Name)
		for _, p := range svc.Spec.Ports {
			proto := string(p.Protocol)
			if proto == "" {
				proto = "TCP"
			}
			portStr := fmt.Sprintf("%d/%s", p.Port, proto)
			if !seenPorts[portStr] {
				seenPorts[portStr] = true
				matchedPorts = append(matchedPorts, portStr)
			}
		}
	}
	if len(matchedSvcs) == 0 {
		return emit.Finding{}, false
	}
	sort.Strings(matchedSvcs)
	sort.Strings(matchedPorts)

	podSet := labels.Set(t.labels)
	for _, np := range ingressPolicies {
		sel, err := metav1.LabelSelectorAsSelector(&np.Spec.PodSelector)
		if err != nil {
			continue
		}
		if sel.Matches(podSet) {
			return emit.Finding{}, false
		}
	}

	return emit.Finding{
		Kind:     kindAIWorkloadExposure,
		Severity: emit.SeverityWarning,
		Reason:   reasonServingPortClusterWideNoNetPol,
		Message: fmt.Sprintf("AI inference workload is exposed by Service %s (%s) in namespace %s with no ingress NetworkPolicy selecting its pods: any pod in the cluster can reach the model serving endpoint directly",
			cappedList(matchedSvcs), cappedList(matchedPorts), t.namespace),
		Details: []emit.Field{
			{Key: "service", Value: cappedList(matchedSvcs)},
			{Key: "ports", Value: cappedList(matchedPorts)},
		},
	}, true
}

func aiHostNetworkFinding(spec corev1.PodSpec, accel string) (emit.Finding, bool) {
	if !spec.HostNetwork {
		return emit.Finding{}, false
	}
	details := []emit.Field{}
	if accel != "" {
		details = append(details, emit.Field{Key: "accelerator", Value: accel})
	}
	return emit.Finding{
		Kind:     kindAIWorkloadExposure,
		Severity: emit.SeverityWarning,
		Reason:   reasonHostNetworkWithAccelerator,
		Message:  "AI workload runs with hostNetwork=true: inference server ports bind directly to the accelerator node's network namespace, bypassing NetworkPolicy and exposing node-local services",
		Details:  details,
	}, true
}

func aiSharedModelCacheHostPathFinding(spec corev1.PodSpec) (emit.Finding, bool) {
	var hostPaths []string
	for _, v := range spec.Volumes {
		if v.HostPath != nil {
			hostPaths = append(hostPaths, v.Name+":"+v.HostPath.Path)
		}
	}
	if len(hostPaths) == 0 {
		return emit.Finding{}, false
	}
	sort.Strings(hostPaths)
	return emit.Finding{
		Kind:     kindAIWorkloadExposure,
		Severity: emit.SeverityWarning,
		Reason:   reasonSharedModelCacheHostPath,
		Message: fmt.Sprintf("AI workload mounts %d hostPath %s (%s): a writable or shared node-filesystem model cache lets co-scheduled pods tamper with model weights or tokenizer artifacts across tenants",
			len(hostPaths), plural(len(hostPaths), "volume"), cappedList(hostPaths)),
		Details: []emit.Field{
			{Key: "volumes", Value: cappedList(hostPaths)},
		},
	}, true
}

func aiDefaultSAFinding(spec corev1.PodSpec, accel string) (emit.Finding, bool) {
	if !mountsDefaultToken(spec) {
		return emit.Finding{}, false
	}
	details := []emit.Field{
		{Key: "service_account", Value: "default"},
	}
	if accel != "" {
		details = append(details, emit.Field{Key: "accelerator", Value: accel})
	}
	return emit.Finding{
		Kind:     kindAIWorkloadExposure,
		Severity: emit.SeverityWarning,
		Reason:   reasonDefaultSAWithAccelerator,
		Message:  "AI workload runs as the namespace's default ServiceAccount with an automounted API token: a prompt-injection or model-deserialization RCE inherits whatever RBAC bindings accumulate on default",
		Details:  details,
	}, true
}

func aiUnpinnedImageFinding(spec corev1.PodSpec, tags map[string]bool) (emit.Finding, bool) {
	if len(tags) == 0 {
		tags = floatingTags
	}
	all := allContainers(spec)
	var unpinned []string
	for _, c := range all {
		if classifyImageRefWithTags(c.Image, tags) != "" {
			unpinned = append(unpinned, c.Name)
		}
	}
	if len(unpinned) == 0 {
		return emit.Finding{}, false
	}
	total := len(all)
	return emit.Finding{
		Kind:     kindAIWorkloadExposure,
		Severity: emit.SeverityInfo,
		Reason:   reasonUnpinnedModelLoaderImage,
		Message: fmt.Sprintf("%d of %d containers in the AI workload run an unpinned image reference (%s): model-loader and inference runtime restarts or node scale-ups can pull different bytes across accelerator replicas",
			len(unpinned), total, strings.Join(unpinned, ", ")),
		Details: []emit.Field{
			{Key: "containers", Value: itoa(len(unpinned))},
			{Key: "container_names", Value: cappedList(unpinned)},
			{Key: "total_containers", Value: itoa(total)},
		},
	}, true
}
