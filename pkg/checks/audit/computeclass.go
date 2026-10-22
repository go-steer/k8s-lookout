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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

const (
	kindComputeClassPosture = "audit.compute_class_posture"
	kindSingleZoneNodePool  = "audit.single_zone_nodepool"
)

const (
	reasonComputeClassMissing              = "ComputeClassMissing"
	reasonComputeClassNoMatchingNodes      = "ComputeClassNoMatchingNodes"
	reasonSpotWithoutFallback              = "SpotWithoutFallback"
	reasonUnsatisfiableZoneWithAccelerator = "UnsatisfiableZoneWithAccelerator"
	reasonSingleZoneNodePool               = "SingleZoneNodePool"
)

const (
	labelComputeClass    = "cloud.google.com/compute-class"
	labelGKENodepool     = "cloud.google.com/gke-nodepool"
	labelGKESpot         = "cloud.google.com/gke-spot"
	labelGKEProvisioning = "cloud.google.com/gke-provisioning"
	labelGKEAccelerator  = "cloud.google.com/gke-accelerator"
	labelTopologyZone    = "topology.kubernetes.io/zone"
	labelLegacyZone      = "failure-domain.beta.kubernetes.io/zone"
)

var (
	computeClassGV  = schema.GroupVersion{Group: "cloud.google.com", Version: "v1"}
	computeClassGVR = computeClassGV.WithResource("computeclasses")
)

// builtInComputeClasses are the GKE platform-provided compute classes that do
// not require a user-managed ComputeClass custom resource to exist in the API.
var builtInComputeClasses = map[string]bool{
	"balanced":        true,
	"performance":     true,
	"scale-out":       true,
	"scale-in":        true,
	"accelerator":     true,
	"autopilot":       true,
	"autopilot-spot":  true,
	"general-purpose": true,
}

func isBuiltInComputeClass(name string) bool {
	return builtInComputeClasses[strings.ToLower(strings.TrimSpace(name))]
}

// ComputeClassCommand builds `lookout audit computeclass` (issue #787, L-50
// and L-51): GKE ComputeClass, Spot fallback, accelerator-zone placement, and
// single-zone nodepool posture checks.
func ComputeClassCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:        "audit computeclass",
		MCPName:     "k8s_audit_computeclass",
		MCPProfiles: []string{"audit"},
		Summary:     "GKE ComputeClass, hardware-placement, and node-pool zone posture: workloads referencing a non-existent ComputeClass (`ComputeClassMissing`), workloads requesting a compute class or accelerator that matches zero nodes without node-pool auto-creation (`ComputeClassNoMatchingNodes`), multi-replica workloads pinned directly to Spot nodes without a ComputeClass fallback chain (`SpotWithoutFallback`), accelerator workloads pinned to a zone that hosts no matching accelerator nodes while other zones do (`UnsatisfiableZoneWithAccelerator`), and multi-node pools confined to a single availability zone (`SingleZoneNodePool`). Scope with --namespace, -A, or --workload.",
		Flags: append([]emit.FlagSpec{
			skipSystemNamespacesFlag(),
		}, PolicyFlags()...),
		Kinds: []checks.KindField{
			checks.Kind(kindComputeClassPosture,
				"a workload's GKE ComputeClass or hardware-placement spec has no fallback or cannot be satisfied by the cluster: references a non-existent ComputeClass, requests a class or accelerator with zero matching nodes and no node-pool auto-creation, pins multi-replica pods directly to Spot without a ComputeClass priority fallback chain, or pins an accelerator workload to a zone with zero matching accelerator nodes while other zones have them",
				emit.SeverityWarning, emit.SeverityInfo),
			checks.Kind(kindSingleZoneNodePool,
				"a multi-node pool has all of its nodes in a single availability zone while the cluster spans multiple zones, or a workload with zone topologySpreadConstraints is pinned to a single-zone node pool",
				emit.SeverityWarning, emit.SeverityInfo),
			checks.UnreadKind(),
		},
		Output: append([]checks.OutputField{
			{Name: "compute_class", Doc: "the `cloud.google.com/compute-class` name requested by the workload template"},
			{Name: "accelerator", Doc: "the accelerator type (`cloud.google.com/gke-accelerator`) or extended resource (`nvidia.com/gpu`, `google.com/tpu`) requested by the workload template"},
			{Name: "zone", Doc: "the availability zone (`topology.kubernetes.io/zone`) the workload or single-zone node pool is pinned to"},
			{Name: "accelerator_zones", Doc: "zones in the cluster that actually host nodes with the requested accelerator, capped at 8"},
			{Name: "replicas", Doc: "the workload's desired replica count (`spec.replicas`)"},
			{Name: "spot_selector", Doc: "the nodeSelector or affinity expression pinning the workload directly to Spot nodes"},
			{Name: "services", Doc: "count of serving Services routing to the workload, when Service-backed"},
			{Name: "service_names", Doc: "names of the serving Services routing to the workload, capped at 8"},
			{Name: "nodepool", Doc: "the `cloud.google.com/gke-nodepool` name for a single-zone node pool"},
			{Name: "nodes", Doc: "on a single-zone nodepool finding, the number of nodes in that pool; as a summary note, the total nodes in the cluster"},
			{Name: "cluster_zones", Doc: "distinct availability zones across all nodes in the cluster, capped at 8"},
			{Name: "affected_workloads", Doc: "workloads with zone topologySpreadConstraints pinned to the single-zone node pool"},
			{Name: "affected_workload_names", Doc: "their Kind/namespace/name, sorted and capped at 8"},
			{Name: "compute_classes", Doc: "summary note: ComputeClass custom resources listed in the cluster, when the CRD is served"},
			skippedSystemNamespacesField(),
		}, checks.UnreadFields()...),
		Examples: []string{
			"lookout audit computeclass -A",
			"lookout audit computeclass --namespace=prod",
			"lookout audit computeclass --workload=Deployment/prod/inference",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runComputeClass(ctx, deps, inv)
		},
	}
}

type decodedClassInfo struct {
	name         string
	class        *leeway.ComputeClass
	autoCreation bool
	hasNonSpot   bool
}

type computeClassIndex struct {
	templates       []podTemplate
	replicasByOwner map[string]int32 // "Kind/ns/name" -> replicas
	services        []*corev1.Service
	servicesRefused *checks.Refusal
	nodes           []*corev1.Node
	nodesRefused    *checks.Refusal
	refused         []kindRefusal
	classes         map[string]decodedClassInfo
	classesListed   bool
	classesRefused  *checks.Refusal
}

func runComputeClass(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	wl := inv.Scope.Workload
	switch {
	case wl.IsZero() && inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns>, -A, or --workload=<Kind>/<ns>/<name>")
	case !wl.IsZero() && inv.Scope.AllNamespaces:
		return 0, emit.UsageErrorf("-A does not combine with --workload: a workload lives in one namespace (%s)", wl.Namespace)
	case !wl.IsZero() && inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace:
		return 0, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
	}
	if !wl.IsZero() {
		canonical, ok := canonicalWorkloadKinds[strings.ToLower(wl.Kind)]
		if !ok && strings.EqualFold(wl.Kind, "pod") {
			canonical, ok = "Pod", true
		}
		if !ok {
			return 0, emit.UsageErrorf("unsupported workload kind %q (want Deployment|StatefulSet|DaemonSet|CronJob|Job|Pod)", wl.Kind)
		}
		wl.Kind = canonical
	}
	pol, err := bindPolicy(inv)
	if err != nil {
		return 0, err
	}
	skipSys := inv.Flags.Bool("skip-system-namespaces")

	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	if !wl.IsZero() {
		listNS = wl.Namespace
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ctx, snap := ensureSnapshot(ctx, client, listNS)
	ix, err := loadComputeClassIndex(ctx, deps, client, snap, listNS)
	if err != nil {
		return 0, err
	}

	if !wl.IsZero() {
		for _, r := range ix.refused {
			if r.kind == wl.Kind || (wl.Kind == "Job" && r.kind == "unowned Job") || (wl.Kind == "Pod" && r.kind == "unowned Pod") {
				return 0, &checks.RefusalError{Refusal: r.Refusal, What: "workload " + wl.String() + " cannot be audited without it"}
			}
		}
	}

	if skipSys {
		filtered := ix.templates[:0]
		for _, t := range ix.templates {
			if pol.IsSystemNamespace(t.namespace) {
				snap.recordSkippedNS(t.namespace)
				continue
			}
			filtered = append(filtered, t)
		}
		ix.templates = filtered
	}

	var refusals []emit.Finding
	if wl.IsZero() {
		for _, r := range ix.refused {
			refusals = append(refusals, checks.RefusedFinding(r.Refusal, r.kind+" pod templates not audited"))
		}
	}
	if ix.servicesRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*ix.servicesRefused,
			kindComputeClassPosture+" Service routing context not judged: Services could not be listed"))
	}
	if ix.nodesRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*ix.nodesRefused,
			kindComputeClassPosture+" node matching and "+kindSingleZoneNodePool+" not judged: Nodes could not be listed"))
	}
	if ix.classesRefused != nil {
		refusals = append(refusals, checks.RefusedFinding(*ix.classesRefused,
			kindComputeClassPosture+" ComputeClass existence and fallback rules not judged: ComputeClasses could not be listed"))
	}

	var (
		findings         []emit.Finding
		matchedTemplates []podTemplate
		scannedWorkloads int
	)
	for _, t := range ix.templates {
		if !wl.IsZero() && (t.kind != wl.Kind || t.name != wl.Name) {
			continue
		}
		scannedWorkloads++
		matchedTemplates = append(matchedTemplates, t)
		findings = append(findings, ix.judgeWorkload(t)...)
	}

	if !wl.IsZero() && scannedWorkloads == 0 {
		return 0, fmt.Errorf("workload %s not found", wl)
	}

	poolFindings, poolCount := ix.judgeNodePools(matchedTemplates, !wl.IsZero())
	findings = append(findings, poolFindings...)

	sortFindings(findings)
	for _, f := range refusals {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}

	if ix.nodesRefused == nil {
		if err := inv.Out.Note("nodes", itoa(len(ix.nodes))); err != nil {
			return 0, err
		}
	}
	if ix.classesListed && ix.classesRefused == nil {
		if err := inv.Out.Note("compute_classes", itoa(len(ix.classes))); err != nil {
			return 0, err
		}
	}
	if skipSys {
		if err := inv.Out.Note("skipped_system_namespaces", itoa(snap.skippedNSCount())); err != nil {
			return 0, err
		}
	}

	scanned := scannedWorkloads
	if wl.IsZero() {
		scanned += poolCount
	}
	return scanned, nil
}

func loadComputeClassIndex(ctx context.Context, deps Deps, client kubernetes.Interface, snap *Snapshot, listNS string) (*computeClassIndex, error) {
	ix := &computeClassIndex{
		replicasByOwner: map[string]int32{},
		classes:         map[string]decodedClassInfo{},
	}

	templates, refused, err := listPodTemplates(ctx, client, listNS, true)
	if err != nil {
		return nil, err
	}
	ix.templates, ix.refused = templates, refused

	if depsList, r, err := snap.Deployments(ctx); err == nil && r == nil {
		for i := range depsList {
			d := &depsList[i]
			rep := int32(1)
			if d.Spec.Replicas != nil {
				rep = *d.Spec.Replicas
			}
			ix.replicasByOwner["Deployment/"+d.Namespace+"/"+d.Name] = rep
		}
	}
	if stsList, r, err := snap.StatefulSets(ctx); err == nil && r == nil {
		for i := range stsList {
			s := &stsList[i]
			rep := int32(1)
			if s.Spec.Replicas != nil {
				rep = *s.Spec.Replicas
			}
			ix.replicasByOwner["StatefulSet/"+s.Namespace+"/"+s.Name] = rep
		}
	}

	services, servicesRefused, err := snap.Services(ctx)
	if err != nil {
		return nil, err
	}
	ix.services, ix.servicesRefused = services, servicesRefused

	if nodes, r, err := snap.Nodes(ctx); err != nil {
		return nil, err
	} else if r != nil {
		ix.nodesRefused = r
	} else {
		for i := range nodes {
			ix.nodes = append(ix.nodes, &nodes[i])
		}
	}

	if deps.Dynamic != nil || computeClassesServed(client) {
		dyn, err := deps.dynamic(ctx)
		if err != nil {
			return nil, err
		}
		list, err := dyn.Resource(computeClassGVR).List(ctx, metav1.ListOptions{})
		switch {
		case err == nil:
			ix.classesListed = true
			for i := range list.Items {
				info := parseComputeClassUnstructured(&list.Items[i])
				ix.classes[info.name] = info
			}
		case apierrors.IsNotFound(err):
			// CRD not installed on this cluster.
		default:
			if r, ok := checks.ForbiddenRefusal(err); ok {
				ix.classesRefused = &r
			} else {
				return nil, fmt.Errorf("listing computeclasses: %w", err)
			}
		}
	}

	return ix, nil
}

func computeClassesServed(client kubernetes.Interface) bool {
	if client == nil || client.Discovery() == nil {
		return false
	}
	resources, err := client.Discovery().ServerResourcesForGroupVersion(computeClassGV.String())
	if err != nil || resources == nil {
		return false
	}
	for _, r := range resources.APIResources {
		if r.Name == computeClassGVR.Resource {
			return true
		}
	}
	return false
}

func parseComputeClassUnstructured(u *unstructured.Unstructured) decodedClassInfo {
	name := u.GetName()
	spec, _, _ := unstructured.NestedMap(u.Object, "spec")
	info := decodedClassInfo{name: name}
	if spec == nil {
		return info
	}
	if cc, err := leeway.DecodeComputeClass(name, spec); err == nil {
		info.class = cc
		for _, rule := range cc.Axis.Rules {
			spotVal, hasSpot := rule.Raw["spot"].(bool)
			if !hasSpot || !spotVal {
				info.hasNonSpot = true
				break
			}
		}
	}
	if npac, ok := spec["nodePoolAutoCreation"].(map[string]any); ok {
		if enabled, ok := npac["enabled"].(bool); ok && enabled {
			info.autoCreation = true
		}
	} else if enabled, ok := spec["nodePoolAutoCreation"].(bool); ok && enabled {
		info.autoCreation = true
	}
	return info
}

func (ix *computeClassIndex) wantReplicas(t podTemplate) int32 {
	if rep, ok := ix.replicasByOwner[t.kind+"/"+t.namespace+"/"+t.name]; ok {
		return rep
	}
	return 1
}

func (ix *computeClassIndex) judgeWorkload(t podTemplate) []emit.Finding {
	var out []emit.Finding

	ccName := requestedComputeClass(t)
	accName, accResource := requestedAccelerator(t.spec)
	reqZone := requestedZone(t.spec)
	replicas := ix.wantReplicas(t)

	// 1. ComputeClassMissing: workload requests a custom ComputeClass that does not exist.
	ccMissing := false
	if ccName != "" && !isBuiltInComputeClass(ccName) && ix.classesListed && ix.classesRefused == nil {
		if _, exists := ix.classes[ccName]; !exists {
			ccMissing = true
			out = append(out, emit.Finding{
				Kind:     kindComputeClassPosture,
				Severity: emit.SeverityWarning,
				Reason:   reasonComputeClassMissing,
				Message: fmt.Sprintf("workload requests cloud.google.com/compute-class=%s, but no ComputeClass named %q exists in the cluster: pods will stay Pending until the ComputeClass is created or the selector is updated",
					ccName, ccName),
				Details: []emit.Field{
					{Key: "compute_class", Value: ccName},
				},
			})
		}
	}

	// 2. UnsatisfiableZoneWithAccelerator: workload requests both an accelerator
	// and a specific zone, and 0 nodes in that zone carry the accelerator while
	// other zones in the cluster do.
	unsatisfiableZoneAcc := false
	accTarget := accName
	if accTarget == "" {
		accTarget = accResource
	}
	if ix.nodesRefused == nil && accTarget != "" && reqZone != "" {
		inZoneCount := 0
		otherZoneSet := map[string]bool{}
		for _, n := range ix.nodes {
			if !nodeHasAccelerator(n, accName, accResource) {
				continue
			}
			nz := nodeZone(n)
			if nz == reqZone {
				inZoneCount++
			} else if nz != "" {
				otherZoneSet[nz] = true
			}
		}
		if inZoneCount == 0 && len(otherZoneSet) > 0 {
			unsatisfiableZoneAcc = true
			otherZones := sortedMapKeys(otherZoneSet)
			out = append(out, emit.Finding{
				Kind:     kindComputeClassPosture,
				Severity: emit.SeverityWarning,
				Reason:   reasonUnsatisfiableZoneWithAccelerator,
				Message: fmt.Sprintf("workload pins to zone %s and requests accelerator %s, but 0 nodes in %s carry that accelerator while zones %s do: pods cannot schedule unless the zone pin is moved or capacity is added in %s",
					reqZone, accTarget, reqZone, strings.Join(otherZones, ","), reqZone),
				Details: []emit.Field{
					{Key: "accelerator", Value: accTarget},
					{Key: "zone", Value: reqZone},
					{Key: "accelerator_zones", Value: cappedList(otherZones)},
				},
			})
		}
	}

	// 3. ComputeClassNoMatchingNodes: workload sets cloud.google.com/compute-class
	// or cloud.google.com/gke-accelerator, and 0 nodes currently match (and no
	// ComputeClass with nodePoolAutoCreation enabled covers it).
	if ix.nodesRefused == nil && !ccMissing && !unsatisfiableZoneAcc {
		if ccName != "" && !isBuiltInComputeClass(ccName) {
			classInfo, hasClass := ix.classes[ccName]
			autoCreate := hasClass && classInfo.autoCreation
			if !autoCreate && ix.matchingComputeClassNodes(ccName) == 0 {
				details := []emit.Field{{Key: "compute_class", Value: ccName}}
				if accName != "" {
					details = append(details, emit.Field{Key: "accelerator", Value: accName})
				}
				out = append(out, emit.Finding{
					Kind:     kindComputeClassPosture,
					Severity: emit.SeverityInfo,
					Reason:   reasonComputeClassNoMatchingNodes,
					Message: fmt.Sprintf("workload requests cloud.google.com/compute-class=%s and 0 nodes in the cluster currently carry that class (with no nodePoolAutoCreation enabled): pods cannot schedule until matching nodes scale up",
						ccName),
					Details: details,
				})
			}
		} else if accName != "" {
			autoCreate := false
			if ccName != "" {
				if classInfo, ok := ix.classes[ccName]; ok && classInfo.autoCreation {
					autoCreate = true
				}
			}
			if !autoCreate && ix.matchingAcceleratorNodes(accName) == 0 {
				out = append(out, emit.Finding{
					Kind:     kindComputeClassPosture,
					Severity: emit.SeverityInfo,
					Reason:   reasonComputeClassNoMatchingNodes,
					Message: fmt.Sprintf("workload requests cloud.google.com/gke-accelerator=%s and 0 nodes in the cluster currently carry that accelerator: pods cannot schedule until accelerator nodes are provisioned",
						accName),
					Details: []emit.Field{
						{Key: "accelerator", Value: accName},
					},
				})
			}
		}
	}

	// 4. SpotWithoutFallback: a multi-replica (replicas >= 2) Service-backed or
	// production workload template pins directly to Spot nodes without a
	// ComputeClass priority fallback chain.
	if spotSel := directSpotSelector(t.spec); spotSel != "" && replicas >= 2 {
		if !ix.hasComputeClassFallback(ccName) {
			var serving []*corev1.Service
			if ix.servicesRefused == nil {
				serving = servingServices(ix.services, t.namespace, t.labels)
			}
			details := []emit.Field{
				{Key: "replicas", Value: itoa(int(replicas))},
				{Key: "spot_selector", Value: spotSel},
			}
			if len(serving) > 0 {
				details = append(details,
					emit.Field{Key: "services", Value: itoa(len(serving))},
					emit.Field{Key: "service_names", Value: cappedList(serviceNames(serving))},
				)
			}
			out = append(out, emit.Finding{
				Kind:     kindComputeClassPosture,
				Severity: emit.SeverityWarning,
				Reason:   reasonSpotWithoutFallback,
				Message: fmt.Sprintf("%d-replica workload pins directly to Spot nodes (%s) without a ComputeClass priority fallback chain: a Spot preemption wave or zone stockout can evict all %d replicas at once with no on-demand fallback",
					replicas, spotSel, replicas),
				Details: details,
			})
		}
	}

	for i := range out {
		out[i].Namespace = t.namespace
		out[i].KindOfObject = t.kind
		out[i].Name = t.name
		out[i].Fingerprint = engine.PostureFingerprint(out[i].Kind, out[i].Reason, t.kind)
	}
	return out
}

func (ix *computeClassIndex) matchingComputeClassNodes(cc string) int {
	n := 0
	for _, node := range ix.nodes {
		if node.Labels[labelComputeClass] == cc {
			n++
		}
	}
	return n
}

func (ix *computeClassIndex) matchingAcceleratorNodes(acc string) int {
	n := 0
	for _, node := range ix.nodes {
		if node.Labels[labelGKEAccelerator] == acc {
			n++
		}
	}
	return n
}

func (ix *computeClassIndex) hasComputeClassFallback(ccName string) bool {
	if ccName == "" {
		return false
	}
	info, ok := ix.classes[ccName]
	if !ok || info.class == nil {
		// If ComputeClasses were not listed, a custom compute-class selector
		// paired with a direct Spot nodeSelector still pins every pod to Spot
		// because the nodeSelector is a hard AND requirement on the pod.
		return false
	}
	return info.class.Scorable() && info.hasNonSpot
}

// judgeNodePools evaluates `audit.single_zone_nodepool` across the cluster's
// nodes grouped by `cloud.google.com/gke-nodepool`.
func (ix *computeClassIndex) judgeNodePools(templates []podTemplate, workloadScoped bool) ([]emit.Finding, int) {
	if ix.nodesRefused != nil || len(ix.nodes) == 0 {
		return nil, 0
	}

	clusterZoneSet := map[string]bool{}
	nodesByPool := map[string][]*corev1.Node{}
	for _, n := range ix.nodes {
		if z := nodeZone(n); z != "" {
			clusterZoneSet[z] = true
		}
		pool := n.Labels[labelGKENodepool]
		if pool == "" {
			continue
		}
		nodesByPool[pool] = append(nodesByPool[pool], n)
	}
	if len(nodesByPool) == 0 {
		return nil, 0
	}

	clusterZones := sortedMapKeys(clusterZoneSet)
	poolNames := make([]string, 0, len(nodesByPool))
	for pool := range nodesByPool {
		poolNames = append(poolNames, pool)
	}
	sort.Strings(poolNames)

	var out []emit.Finding
	for _, pool := range poolNames {
		pNodes := nodesByPool[pool]
		zoneSet := map[string]bool{}
		for _, n := range pNodes {
			if z := nodeZone(n); z != "" {
				zoneSet[z] = true
			}
		}
		if len(zoneSet) != 1 {
			continue
		}
		onlyZone := sortedMapKeys(zoneSet)[0]

		var affected []string
		for _, t := range templates {
			if !hasZoneTopologySpread(t.spec) {
				continue
			}
			if workloadTargetsNodePool(t.spec, pool) {
				affected = append(affected, t.kind+"/"+t.namespace+"/"+t.name)
			}
		}
		sort.Strings(affected)

		if workloadScoped && len(affected) == 0 {
			continue
		}
		if len(affected) == 0 && (len(pNodes) < 2 || len(clusterZones) < 2) {
			continue
		}

		sev := emit.SeverityInfo
		msg := fmt.Sprintf("node pool %s has all %d of its nodes in a single zone (%s) while the cluster spans %d zones (%s): a zonal outage or stockout takes the entire pool down",
			pool, len(pNodes), onlyZone, len(clusterZones), strings.Join(clusterZones, ","))
		details := []emit.Field{
			{Key: "nodepool", Value: pool},
			{Key: "zone", Value: onlyZone},
			{Key: "nodes", Value: itoa(len(pNodes))},
			{Key: "cluster_zones", Value: cappedList(clusterZones)},
		}
		if len(affected) > 0 {
			sev = emit.SeverityWarning
			msg = fmt.Sprintf("node pool %s exists only in zone %s (%d %s) and %d workload%s with zone topologySpreadConstraints (%s) %s pinned to it: zone spread cannot be satisfied across a single-zone pool",
				pool, onlyZone, len(pNodes), plural(len(pNodes), "node"), len(affected), plural(len(affected), ""), cappedList(affected), verb(len(affected), "is", "are"))
			details = append(details,
				emit.Field{Key: "affected_workloads", Value: itoa(len(affected))},
				emit.Field{Key: "affected_workload_names", Value: cappedList(affected)},
			)
		}

		f := emit.Finding{
			Kind:         kindSingleZoneNodePool,
			Severity:     sev,
			Reason:       reasonSingleZoneNodePool,
			Message:      msg,
			Details:      details,
			KindOfObject: "NodePool",
			Name:         pool,
		}
		f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
		out = append(out, f)
	}

	return out, len(poolNames)
}

func requestedComputeClass(t podTemplate) string {
	if v := strings.TrimSpace(t.spec.NodeSelector[labelComputeClass]); v != "" {
		return v
	}
	if v := strings.TrimSpace(t.labels[labelComputeClass]); v != "" {
		return v
	}
	return requiredAffinitySingleValue(t.spec, labelComputeClass)
}

func requestedAccelerator(spec corev1.PodSpec) (labelVal, resourceName string) {
	if v := strings.TrimSpace(spec.NodeSelector[labelGKEAccelerator]); v != "" {
		labelVal = v
	} else {
		labelVal = requiredAffinitySingleValue(spec, labelGKEAccelerator)
	}
	for _, c := range allContainers(spec) {
		for _, rl := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
			for resName, qty := range rl {
				s := string(resName)
				if (strings.HasPrefix(s, "nvidia.com/gpu") || strings.HasPrefix(s, "google.com/tpu")) && !qty.IsZero() && qty.Sign() > 0 {
					return labelVal, s
				}
			}
		}
	}
	return labelVal, ""
}

func requestedZone(spec corev1.PodSpec) string {
	for _, key := range []string{labelTopologyZone, labelLegacyZone} {
		if v := strings.TrimSpace(spec.NodeSelector[key]); v != "" {
			return v
		}
		if v := requiredAffinitySingleValue(spec, key); v != "" {
			return v
		}
	}
	return ""
}

func directSpotSelector(spec corev1.PodSpec) string {
	if strings.EqualFold(strings.TrimSpace(spec.NodeSelector[labelGKESpot]), "true") {
		return labelGKESpot + "=true"
	}
	if strings.EqualFold(strings.TrimSpace(spec.NodeSelector[labelGKEProvisioning]), "spot") {
		return labelGKEProvisioning + "=spot"
	}
	if strings.EqualFold(requiredAffinitySingleValue(spec, labelGKESpot), "true") {
		return labelGKESpot + "=true"
	}
	if strings.EqualFold(requiredAffinitySingleValue(spec, labelGKEProvisioning), "spot") {
		return labelGKEProvisioning + "=spot"
	}
	return ""
}

func requiredAffinitySingleValue(spec corev1.PodSpec, key string) string {
	if spec.Affinity == nil || spec.Affinity.NodeAffinity == nil {
		return ""
	}
	req := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) != 1 {
		return ""
	}
	for _, expr := range req.NodeSelectorTerms[0].MatchExpressions {
		if expr.Key == key && expr.Operator == corev1.NodeSelectorOpIn && len(expr.Values) == 1 {
			return strings.TrimSpace(expr.Values[0])
		}
	}
	return ""
}

func nodeZone(n *corev1.Node) string {
	if z := strings.TrimSpace(n.Labels[labelTopologyZone]); z != "" {
		return z
	}
	return strings.TrimSpace(n.Labels[labelLegacyZone])
}

func nodeHasAccelerator(n *corev1.Node, accLabel, accResource string) bool {
	if accLabel != "" {
		return n.Labels[labelGKEAccelerator] == accLabel
	}
	if accResource != "" {
		if qty, ok := n.Status.Allocatable[corev1.ResourceName(accResource)]; ok && !qty.IsZero() && qty.Sign() > 0 {
			return true
		}
		if qty, ok := n.Status.Capacity[corev1.ResourceName(accResource)]; ok && !qty.IsZero() && qty.Sign() > 0 {
			return true
		}
		return n.Labels[labelGKEAccelerator] != ""
	}
	return false
}

func hasZoneTopologySpread(spec corev1.PodSpec) bool {
	for _, c := range spec.TopologySpreadConstraints {
		if c.TopologyKey == labelTopologyZone || c.TopologyKey == labelLegacyZone {
			return true
		}
	}
	return false
}

func workloadTargetsNodePool(spec corev1.PodSpec, pool string) bool {
	if spec.NodeSelector[labelGKENodepool] == pool {
		return true
	}
	return requiredAffinitySingleValue(spec, labelGKENodepool) == pool
}

func sortedMapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
