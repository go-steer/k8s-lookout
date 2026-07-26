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

// `state volumes` (DESIGN.md §5 tool matrix row, ex `volume-binder`):
// RWO multi-attach and cross-zone PV locks via VolumeAttachment. One
// paged List pass over pods + PVCs (namespace-scopable) and PVs +
// VolumeAttachments + Nodes (cluster-scoped, always listed), then
// pure joins — no graph needed: the claim chain is pod →
// spec.volumes[].persistentVolumeClaim → PVC → spec.volumeName → PV,
// and the attachment side is VolumeAttachment → {PV, node}.
//
// Finding kinds and severities:
//
//	volume.multi_attach        critical      RWO(/RWOP) claim wanted by scheduled pods on ≥2 nodes
//	volume.attach_error        warn/critical VolumeAttachment attach/detach error (critical once it has aged)
//	volume.zone_conflict       critical      pod scheduled outside every zone the PV's node affinity allows
//	volume.orphaned_attachment info          VolumeAttachment referencing a deleted PV and/or node
//
// Healthy volumes are silent (§4.2 zero nominal state).

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(VolumesCommand(Deps{}))
}

// volumeAttachErrCritAge is the attach/detach-error age at which
// volume.attach_error escalates from warning to critical: transient
// attach errors self-heal within a couple of controller retries; ten
// minutes of retrying means it will not.
const volumeAttachErrCritAge = 10 * time.Minute

// volumeErrMsgCap bounds the `error` detail: driver errors embed
// whole gRPC statuses and the first ~200 chars carry the cause.
const volumeErrMsgCap = 200

// volumePodListCap bounds the `pods` detail list; beyond it the list
// ends with "+K more".
const volumePodListCap = 6

// The zone topology labels, stable and legacy-beta (older clusters
// and some CSI drivers still stamp only the beta key).
const (
	volumeZoneLabel     = "topology.kubernetes.io/zone"
	volumeZoneLabelBeta = "failure-domain.beta.kubernetes.io/zone"
)

// VolumesCommand builds the `lookout state volumes` command.
func VolumesCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "state volumes",
		MCPName: "k8s_volume_conflicts",
		Summary: "When pods hang in ContainerCreating with Multi-Attach or FailedAttachVolume events — join VolumeAttachment + PV/PVC + pods to name the exact conflict: RWO claims wanted on two nodes, attachments stuck in error, cross-zone PV locks, orphaned attachments.",
		Output: []checks.OutputField{
			{Name: "pods", Doc: "scheduled pods referencing the conflicted claim, sorted (list capped, then +K more)"},
			{Name: "nodes", Doc: "distinct nodes those pods are scheduled on, sorted"},
			{Name: "access_modes", Doc: "the claim's declared access modes"},
			{Name: "pv", Doc: "PersistentVolume behind the claim or attachment"},
			{Name: "pvc", Doc: "PersistentVolumeClaim the pod mounts (same namespace as the pod)"},
			{Name: "node", Doc: "node the attachment targets or the pod is scheduled on"},
			{Name: "attacher", Doc: "CSI driver responsible for the attachment (spec.attacher)"},
			{Name: "age", Doc: "how long the attach/detach error has persisted, truncated to seconds"},
			{Name: "error", Doc: "the attach/detach error message, truncated to 200 chars"},
			{Name: "attached", Doc: "the attachment's status.attached at scan time"},
			{Name: "pv_zones", Doc: "zones the PV's node affinity allows, sorted"},
			{Name: "node_zone", Doc: "zone label of the node the pod is scheduled on"},
			{Name: "orphan", Doc: "which referenced side is gone: \"pv missing\", \"node missing\", or both"},
		},
		Examples: []string{
			"lookout state volumes",
			"lookout state volumes --namespace=prod",
			"lookout state volumes --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runVolumes(ctx, deps, inv)
		},
	}
}

func runVolumes(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("state volumes scans claim/attachment conflicts cluster-wide; scope with --namespace")
	}
	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	// --namespace restricts the namespaced kinds (pods, PVCs); PVs,
	// VolumeAttachments and Nodes are cluster-scoped and always
	// listed. Default and -A both mean all namespaces.
	ns := inv.Scope.Namespace
	if ns == "" || inv.Scope.AllNamespaces {
		ns = metav1.NamespaceAll
	}
	vix, err := listVolumeIndex(ctx, client, ns)
	if err != nil {
		return 0, err
	}
	findings := vix.findings(deps.now())
	sort.SliceStable(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
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
	return vix.scanned, nil
}

// volumeIndex holds the one List pass `state volumes` joins over.
type volumeIndex struct {
	scanned int // every listed object across all five kinds

	pods        []*corev1.Pod
	pvcs        map[string]*corev1.PersistentVolumeClaim // ns/name
	pvs         map[string]*corev1.PersistentVolume      // name
	nodes       map[string]*corev1.Node                  // name
	attachments []*storagev1.VolumeAttachment
}

func listVolumeIndex(ctx context.Context, client kubernetes.Interface, ns string) (*volumeIndex, error) {
	vix := &volumeIndex{
		pvcs:  map[string]*corev1.PersistentVolumeClaim{},
		pvs:   map[string]*corev1.PersistentVolume{},
		nodes: map[string]*corev1.Node{},
	}
	steps := []func() error{
		func() error {
			return listPages("pods", func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
				l, err := client.CoreV1().Pods(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(p *corev1.Pod) { vix.pods = append(vix.pods, p); vix.scanned++ })
		},
		func() error {
			return listPages("persistentvolumeclaims", func(o metav1.ListOptions) ([]corev1.PersistentVolumeClaim, string, error) {
				l, err := client.CoreV1().PersistentVolumeClaims(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *corev1.PersistentVolumeClaim) { vix.pvcs[key(c.Namespace, c.Name)] = c; vix.scanned++ })
		},
		func() error {
			return listPages("persistentvolumes", func(o metav1.ListOptions) ([]corev1.PersistentVolume, string, error) {
				l, err := client.CoreV1().PersistentVolumes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(pv *corev1.PersistentVolume) { vix.pvs[pv.Name] = pv; vix.scanned++ })
		},
		func() error {
			return listPages("volumeattachments", func(o metav1.ListOptions) ([]storagev1.VolumeAttachment, string, error) {
				l, err := client.StorageV1().VolumeAttachments().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(a *storagev1.VolumeAttachment) { vix.attachments = append(vix.attachments, a); vix.scanned++ })
		},
		func() error {
			return listPages("nodes", func(o metav1.ListOptions) ([]corev1.Node, string, error) {
				l, err := client.CoreV1().Nodes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(n *corev1.Node) { vix.nodes[n.Name] = n; vix.scanned++ })
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, err
		}
	}
	return vix, nil
}

func (vix *volumeIndex) findings(now time.Time) []emit.Finding {
	var out []emit.Finding
	out = append(out, vix.multiAttach()...)
	out = append(out, vix.zoneConflicts()...)
	out = append(out, vix.attachmentFindings(now)...)
	return out
}

// volumeClaimUse aggregates the scheduled pods referencing one PVC.
type volumeClaimUse struct {
	pods  map[string]bool // pod names (claim refs are namespace-local)
	nodes map[string]bool // distinct nodeNames
}

// multiAttach reports single-node claims (RWO/RWOP without RWX)
// wanted by scheduled pods on two or more distinct nodes: the volume
// can attach to one of them, and every pod on the others is stuck in
// ContainerCreating behind Multi-Attach events.
func (vix *volumeIndex) multiAttach() []emit.Finding {
	use := map[string]*volumeClaimUse{} // PVC ns/name
	for _, p := range vix.pods {
		if p.Spec.NodeName == "" {
			continue // unscheduled pods hold no attachment anywhere
		}
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}
			k := key(p.Namespace, v.PersistentVolumeClaim.ClaimName)
			u := use[k]
			if u == nil {
				u = &volumeClaimUse{pods: map[string]bool{}, nodes: map[string]bool{}}
				use[k] = u
			}
			u.pods[p.Name] = true
			u.nodes[p.Spec.NodeName] = true
		}
	}
	var out []emit.Finding
	for k, u := range use {
		pvc := vix.pvcs[k]
		if pvc == nil || len(u.nodes) < 2 || !volumeSingleNode(pvc.Spec.AccessModes) {
			continue
		}
		out = append(out, emit.Finding{
			Kind:         "volume.multi_attach",
			Severity:     emit.SeverityCritical,
			Namespace:    pvc.Namespace,
			KindOfObject: "PersistentVolumeClaim",
			Name:         pvc.Name,
			Reason:       "RWOMultiAttach",
			Message: fmt.Sprintf("RWO claim is wanted on %d nodes — an RWO volume can attach to only one node; pods on the other node(s) stay stuck in ContainerCreating",
				len(u.nodes)),
			Details: []emit.Field{
				{Key: "pods", Value: volumeCapList(sortedKeys(u.pods))},
				{Key: "nodes", Value: strings.Join(sortedKeys(u.nodes), ",")},
				{Key: "access_modes", Value: volumeAccessModes(pvc.Spec.AccessModes)},
			},
		})
	}
	return out
}

// volumeSingleNode reports whether the access modes pin the volume to
// a single node: ReadWriteOnce, or ReadWriteOncePod — RWOP is
// stricter still (one *pod*), so it is at least as single-node as
// RWO. A ReadWriteMany mode anywhere in the list lifts the
// restriction (the volume advertises multi-node writes).
func volumeSingleNode(modes []corev1.PersistentVolumeAccessMode) bool {
	var single, many bool
	for _, m := range modes {
		switch m {
		case corev1.ReadWriteOnce, corev1.ReadWriteOncePod:
			single = true
		case corev1.ReadWriteMany:
			many = true
		}
	}
	return single && !many
}

// zoneConflicts reports pods scheduled on a node outside every zone
// the backing PV's node affinity allows: the kubelet will retry the
// mount forever, and no reschedule onto the same node can succeed.
func (vix *volumeIndex) zoneConflicts() []emit.Finding {
	var out []emit.Finding
	for _, p := range vix.pods {
		if p.Spec.NodeName == "" {
			continue
		}
		node := vix.nodes[p.Spec.NodeName]
		if node == nil {
			continue
		}
		nodeZone := volumeNodeZone(node)
		if nodeZone == "" {
			continue
		}
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}
			pvc := vix.pvcs[key(p.Namespace, v.PersistentVolumeClaim.ClaimName)]
			if pvc == nil || pvc.Spec.VolumeName == "" {
				continue
			}
			pv := vix.pvs[pvc.Spec.VolumeName]
			if pv == nil {
				continue
			}
			zones, constrained := volumePVZones(pv)
			if !constrained || zones[nodeZone] {
				continue
			}
			zoneList := sortedKeys(zones)
			out = append(out, emit.Finding{
				Kind:         "volume.zone_conflict",
				Severity:     emit.SeverityCritical,
				Namespace:    p.Namespace,
				KindOfObject: "Pod",
				Name:         p.Name,
				Reason:       "ZoneConflict",
				Message: fmt.Sprintf("volume is locked to zone(s) %s; the pod landed in %s and can never mount it",
					strings.Join(zoneList, ","), nodeZone),
				Details: []emit.Field{
					{Key: "pvc", Value: pvc.Name},
					{Key: "pv", Value: pv.Name},
					{Key: "pv_zones", Value: strings.Join(zoneList, ",")},
					{Key: "node", Value: p.Spec.NodeName},
					{Key: "node_zone", Value: nodeZone},
				},
			})
		}
	}
	return out
}

// volumeNodeZone returns the node's zone from the stable topology
// label, falling back to the legacy beta key.
func volumeNodeZone(n *corev1.Node) string {
	if z := n.Labels[volumeZoneLabel]; z != "" {
		return z
	}
	return n.Labels[volumeZoneLabelBeta]
}

// volumePVZones returns the union of zones the PV's required node
// affinity allows, and whether the PV is zone-constrained at all.
// NodeSelectorTerms are ORed, so the union across terms is the
// allowed set — and a term carrying no zone expression is satisfiable
// in ANY zone, which makes the whole PV unconstrained (a conflict
// exists only when the node's zone matches no term).
func volumePVZones(pv *corev1.PersistentVolume) (map[string]bool, bool) {
	na := pv.Spec.NodeAffinity
	if na == nil || na.Required == nil {
		return nil, false
	}
	zones := map[string]bool{}
	for _, term := range na.Required.NodeSelectorTerms {
		termConstrained := false
		for _, expr := range term.MatchExpressions {
			if expr.Operator != corev1.NodeSelectorOpIn {
				continue
			}
			if expr.Key != volumeZoneLabel && expr.Key != volumeZoneLabelBeta {
				continue
			}
			termConstrained = true
			for _, z := range expr.Values {
				zones[z] = true
			}
		}
		if !termConstrained {
			return nil, false
		}
	}
	if len(zones) == 0 {
		return nil, false
	}
	return zones, true
}

// attachmentFindings reports VolumeAttachments stuck in an
// attach/detach error and attachments orphaned by a deleted PV or
// node. One attachment can hit both (they answer different
// questions: "why won't it attach" vs "what stale state remains").
func (vix *volumeIndex) attachmentFindings(now time.Time) []emit.Finding {
	var out []emit.Finding
	for _, va := range vix.attachments {
		pvName := ""
		if va.Spec.Source.PersistentVolumeName != nil {
			pvName = *va.Spec.Source.PersistentVolumeName
		}
		if f, ok := volumeAttachErr(va, pvName, "attach", va.Status.AttachError, now); ok {
			out = append(out, f)
		}
		if f, ok := volumeAttachErr(va, pvName, "detach", va.Status.DetachError, now); ok {
			out = append(out, f)
		}
		if f, ok := vix.volumeOrphaned(va, pvName); ok {
			out = append(out, f)
		}
	}
	return out
}

// volumeAttachErr builds one volume.attach_error finding for a set
// attach/detach error. Age (err.Time vs now) picks the severity; a
// zero err.Time gives a warning with the age omitted (some drivers
// never stamp it).
func volumeAttachErr(va *storagev1.VolumeAttachment, pvName, verb string, verr *storagev1.VolumeError, now time.Time) (emit.Finding, bool) {
	if verr == nil {
		return emit.Finding{}, false
	}
	severity := emit.SeverityWarning
	message := fmt.Sprintf("volume %s is failing", verb)
	details := []emit.Field{
		{Key: "pv", Value: pvName},
		{Key: "node", Value: va.Spec.NodeName},
		{Key: "attacher", Value: va.Spec.Attacher},
	}
	if !verr.Time.IsZero() {
		age := now.Sub(verr.Time.Time).Truncate(time.Second)
		if age >= volumeAttachErrCritAge {
			severity = emit.SeverityCritical
		}
		message = fmt.Sprintf("volume %s has been failing for %s", verb, age)
		details = append(details, emit.Field{Key: "age", Value: age.String()})
	}
	details = append(details,
		emit.Field{Key: "error", Value: volumeTruncate(verr.Message)},
		emit.Field{Key: "attached", Value: strconv.FormatBool(va.Status.Attached)},
	)
	reason := "AttachError"
	if verb == "detach" {
		reason = "DetachError"
	}
	return emit.Finding{
		Kind:         "volume.attach_error",
		Severity:     severity,
		KindOfObject: "VolumeAttachment",
		Name:         va.Name,
		Reason:       reason,
		Message:      message,
		Details:      details,
	}, true
}

// volumeOrphaned builds one volume.orphaned_attachment finding when
// the attachment's PV and/or node no longer exists.
func (vix *volumeIndex) volumeOrphaned(va *storagev1.VolumeAttachment, pvName string) (emit.Finding, bool) {
	var missing []string
	what := ""
	if pvName != "" && vix.pvs[pvName] == nil {
		missing = append(missing, "pv missing")
		what = "PersistentVolume"
	}
	if va.Spec.NodeName != "" && vix.nodes[va.Spec.NodeName] == nil {
		missing = append(missing, "node missing")
		if what == "" {
			what = "node"
		} else {
			what = "PersistentVolume and node"
		}
	}
	if len(missing) == 0 {
		return emit.Finding{}, false
	}
	return emit.Finding{
		Kind:         "volume.orphaned_attachment",
		Severity:     emit.SeverityInfo,
		KindOfObject: "VolumeAttachment",
		Name:         va.Name,
		Reason:       "OrphanedAttachment",
		Message: fmt.Sprintf("attachment references a deleted %s — the external-attacher should clean it up; stale entries can block reattachment",
			what),
		Details: []emit.Field{
			{Key: "pv", Value: pvName},
			{Key: "node", Value: va.Spec.NodeName},
			{Key: "orphan", Value: strings.Join(missing, "; ")},
		},
	}, true
}

// volumeCapList joins a sorted list, capping it at volumePodListCap
// entries with a trailing "+K more".
func volumeCapList(items []string) string {
	if len(items) > volumePodListCap {
		return strings.Join(items[:volumePodListCap], ",") +
			fmt.Sprintf(",+%d more", len(items)-volumePodListCap)
	}
	return strings.Join(items, ",")
}

func volumeAccessModes(modes []corev1.PersistentVolumeAccessMode) string {
	out := make([]string, len(modes))
	for i, m := range modes {
		out[i] = string(m)
	}
	return strings.Join(out, ",")
}

// volumeTruncate caps a driver error message at volumeErrMsgCap.
func volumeTruncate(s string) string {
	if len(s) <= volumeErrMsgCap {
		return s
	}
	return s[:volumeErrMsgCap] + "…"
}
