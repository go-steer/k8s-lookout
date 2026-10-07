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
//	volume.unconsumed_pvc      info          Bound claim that no pod mounts and no workload template references
//
// Healthy volumes are silent (§4.2 zero nominal state).
//
// # volume.unconsumed_pvc (#231, the fleet-audit `unconsumed-pvc` slug)
//
// A Bound claim holds a provisioned volume — and, on a cloud, a billed
// disk — whether or not anything mounts it. The claim is reported only
// when the API says nothing can be using it, and every look-alike that
// is a normal intermediate or intentionally parked state is excluded,
// because a cost finding that fires on a healthy cluster trains people
// to ignore the check:
//
//   - Any pod referencing the claim counts as a consumer, in ANY phase
//     and scheduled or not: a pod that is Pending, unschedulable, or
//     Completed still names the claim and will (or did) mount it.
//     Generic ephemeral volumes count too (claim <pod>-<volume>).
//   - Any workload pod TEMPLATE referencing the claim counts —
//     Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob. This
//     is what keeps a Deployment scaled to zero, or a CronJob between
//     runs, from reading as waste: the claim is parked, not orphaned.
//   - A claim matching a live StatefulSet's volumeClaimTemplates
//     (<template>-<statefulset>-<ordinal>) is excluded, at any ordinal.
//     A StatefulSet scaled down — or to zero — keeps the claims of its
//     removed ordinals on purpose (persistentVolumeClaimRetentionPolicy
//     whenScaled=Retain, the default) so that scaling back up gets the
//     same data. The claims of a StatefulSet that no longer exists ARE
//     reported: nothing will ever re-adopt them.
//   - A claim owned by a Pod (an ephemeral volume) or a StatefulSet (a
//     retention policy of Delete) is excluded: its lifecycle is the
//     owner's, and the garbage collector removes it.
//   - A claim younger than volumeUnconsumedGrace is excluded — the race
//     between creating a claim and creating the pod that mounts it.
//   - A claim being deleted is excluded.
//
// What the check cannot see, and says so in the ledger: a consumer that
// is not a pod or a built-in workload (a KubeVirt VirtualMachine, a CI
// workspace, a backup tool, a claim kept as a clone source). Those read
// as unconsumed, which is why the severity is info and why an exemption
// is the reviewed way to say "parked on purpose". The workload Lists
// are what make the exclusions above possible, so when any of them is
// refused the whole claim judgment is skipped with an explicit
// read.unavailable record — guessing without them would report every
// claim a scaled-to-zero workload owns.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
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

// volumeUnconsumedGrace is how old a Bound claim must be before
// volume.unconsumed_pvc may report it. It is a race guard, not a
// waste threshold: the claim and the pod that mounts it are created by
// separate calls (an operator, a StatefulSet controller, a Helm
// install), and a claim caught between the two is not orphaned. An
// hour is far longer than that window and far shorter than any
// interval at which waste is worth reporting.
const volumeUnconsumedGrace = time.Hour

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
		Summary: "When pods hang in ContainerCreating with Multi-Attach or FailedAttachVolume events — join VolumeAttachment + PV/PVC + pods to name the exact conflict: RWO claims wanted on two nodes, attachments stuck in error, cross-zone PV locks, orphaned attachments; also names Bound claims nothing mounts or references (provisioned storage billing for nobody).",
		Kinds: []checks.KindField{
			checks.Kind("volume.multi_attach", "an RWO claim is wanted by pods on more than one node — the second pod never starts", emit.SeverityCritical),
			checks.Kind("volume.zone_conflict", "the PV is locked to a zone the pod's node is not in", emit.SeverityCritical),
			checks.Kind("volume.attach_error", "the attach or detach is failing; critical once it has been failing long enough to be stuck rather than slow", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("volume.orphaned_attachment", "a VolumeAttachment survives its PV or its node", emit.SeverityInfo),
			checks.Kind("volume.unconsumed_pvc", "a Bound claim that no pod (in any phase) mounts and no workload template or live StatefulSet claim template references — its volume is provisioned and billed for nothing; consumers outside the built-in workload kinds (a VM operator, a CI workspace) are invisible here, so read it as a lead, not a verdict", emit.SeverityInfo),
			UnreadKind(),
		},
		Output: append([]checks.OutputField{
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
			{Name: "capacity", Doc: "volume.unconsumed_pvc: the claim's bound capacity (status.capacity.storage); omitted when unreported"},
			{Name: "storage_class", Doc: "volume.unconsumed_pvc: the claim's StorageClass; omitted when it names none"},
			{Name: "reclaim_policy", Doc: "volume.unconsumed_pvc: the bound PV's reclaim policy — Delete means deleting the claim frees the disk, Retain means the PV must be deleted too; omitted when the PV is not visible"},
			{Name: "claim_age", Doc: "volume.unconsumed_pvc: how long ago the claim was created, truncated to minutes"},
		}, UnreadFields()...),
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

	// templateClaims are the claims a workload pod template names
	// (ns/claim), and stsClaimPrefixes the "<template>-<sts>-" name
	// prefixes of every live StatefulSet's volumeClaimTemplates, keyed
	// by namespace — the two halves of "parked, not orphaned".
	templateClaims   map[string]bool
	stsClaimPrefixes map[string][]string
	// unread lists the workload resources whose List was refused; any
	// entry suppresses the unconsumed-claim judgment (see the package
	// comment) and becomes a read.unavailable record.
	unread []checks.Refusal
	// refused is every refused List of the main pass, keyed by
	// resource (#546). Under the built-in `view` role that is
	// persistentvolumes, volumeattachments and nodes — all
	// cluster-scoped — so the checks joining over them are skipped,
	// each with a read.unavailable record, and the rest still answer.
	refused map[string]checks.Refusal
}

// volumeWorkloadResources names the workload Lists the unconsumed-claim
// judgment depends on.
var (
	volResDeployments  = checks.Refused("list", "apps", "deployments")
	volResStatefulSets = checks.Refused("list", "apps", "statefulsets")
	volResDaemonSets   = checks.Refused("list", "apps", "daemonsets")
	volResReplicaSets  = checks.Refused("list", "apps", "replicasets")
	volResJobs         = checks.Refused("list", "batch", "jobs")
	volResCronJobs     = checks.Refused("list", "batch", "cronjobs")
)

// The main pass's resources, as the refused map keys them.
const (
	volPods        = "pods"
	volPVCs        = "persistentvolumeclaims"
	volPVs         = "persistentvolumes"
	volAttachments = "volumeattachments"
	volNodes       = "nodes"
)

// volumeMainLists are the main pass's Lists, in order.
var volumeMainLists = map[string]checks.Refusal{
	volPods:        checks.Refused("list", "", volPods),
	volPVCs:        checks.Refused("list", "", volPVCs),
	volPVs:         checks.Refused("list", "", volPVs),
	volAttachments: checks.Refused("list", "storage.k8s.io", volAttachments),
	volNodes:       checks.Refused("list", "", volNodes),
}

func listVolumeIndex(ctx context.Context, client kubernetes.Interface, ns string) (*volumeIndex, error) {
	vix := &volumeIndex{
		pvcs:             map[string]*corev1.PersistentVolumeClaim{},
		pvs:              map[string]*corev1.PersistentVolume{},
		nodes:            map[string]*corev1.Node{},
		templateClaims:   map[string]bool{},
		stsClaimPrefixes: map[string][]string{},
		refused:          map[string]checks.Refusal{},
	}
	steps := []struct {
		resource string
		run      func() error
	}{
		{volPods, func() error {
			return listPages("pods", func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
				l, err := client.CoreV1().Pods(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(p *corev1.Pod) { vix.pods = append(vix.pods, p); vix.scanned++ })
		}},
		{volPVCs, func() error {
			return listPages("persistentvolumeclaims", func(o metav1.ListOptions) ([]corev1.PersistentVolumeClaim, string, error) {
				l, err := client.CoreV1().PersistentVolumeClaims(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *corev1.PersistentVolumeClaim) { vix.pvcs[key(c.Namespace, c.Name)] = c; vix.scanned++ })
		}},
		{volPVs, func() error {
			return listPages("persistentvolumes", func(o metav1.ListOptions) ([]corev1.PersistentVolume, string, error) {
				l, err := client.CoreV1().PersistentVolumes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(pv *corev1.PersistentVolume) { vix.pvs[pv.Name] = pv; vix.scanned++ })
		}},
		{volAttachments, func() error {
			return listPages("volumeattachments", func(o metav1.ListOptions) ([]storagev1.VolumeAttachment, string, error) {
				l, err := client.StorageV1().VolumeAttachments().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(a *storagev1.VolumeAttachment) { vix.attachments = append(vix.attachments, a); vix.scanned++ })
		}},
		{volNodes, func() error {
			return listPages("nodes", func(o metav1.ListOptions) ([]corev1.Node, string, error) {
				l, err := client.CoreV1().Nodes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(n *corev1.Node) { vix.nodes[n.Name] = n; vix.scanned++ })
		}},
	}
	// A refused List degrades (#546): the checks that join over it are
	// skipped with a read.unavailable record (see findings), and the
	// rest still answer. Any other error stays fatal.
	for _, step := range steps {
		err := step.run()
		if err == nil {
			continue
		}
		if _, forbidden := ListForbidden(err); forbidden {
			vix.refused[step.resource] = volumeMainLists[step.resource]
			continue
		}
		return nil, err
	}
	if err := vix.listWorkloadTemplates(ctx, client, ns); err != nil {
		return nil, err
	}
	return vix, nil
}

// listWorkloadTemplates pages through the workload kinds whose pod
// templates (and StatefulSet claim templates) can reference a claim.
// Unlike the Lists above, a refused one degrades rather than fails
// (#546): it costs only the unconsumed-claim judgment, so every other
// volume check still answers. Any other error stays fatal.
func (vix *volumeIndex) listWorkloadTemplates(ctx context.Context, client kubernetes.Interface, ns string) error {
	addSpec := func(namespace string, spec *corev1.PodSpec) {
		for _, v := range spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				vix.templateClaims[key(namespace, v.PersistentVolumeClaim.ClaimName)] = true
			}
		}
	}
	steps := []struct {
		resource checks.Refusal
		run      func() error
	}{
		{volResDeployments, func() error {
			return listPages("deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
				l, err := client.AppsV1().Deployments(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.Deployment) { addSpec(d.Namespace, &d.Spec.Template.Spec); vix.scanned++ })
		}},
		{volResStatefulSets, func() error {
			return listPages("statefulsets", func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
				l, err := client.AppsV1().StatefulSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *appsv1.StatefulSet) {
				addSpec(s.Namespace, &s.Spec.Template.Spec)
				for _, vct := range s.Spec.VolumeClaimTemplates {
					vix.stsClaimPrefixes[s.Namespace] = append(vix.stsClaimPrefixes[s.Namespace], vct.Name+"-"+s.Name+"-")
				}
				vix.scanned++
			})
		}},
		{volResDaemonSets, func() error {
			return listPages("daemonsets", func(o metav1.ListOptions) ([]appsv1.DaemonSet, string, error) {
				l, err := client.AppsV1().DaemonSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.DaemonSet) { addSpec(d.Namespace, &d.Spec.Template.Spec); vix.scanned++ })
		}},
		{volResReplicaSets, func() error {
			return listPages("replicasets", func(o metav1.ListOptions) ([]appsv1.ReplicaSet, string, error) {
				l, err := client.AppsV1().ReplicaSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(r *appsv1.ReplicaSet) { addSpec(r.Namespace, &r.Spec.Template.Spec); vix.scanned++ })
		}},
		{volResJobs, func() error {
			return listPages("jobs", func(o metav1.ListOptions) ([]batchv1.Job, string, error) {
				l, err := client.BatchV1().Jobs(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(j *batchv1.Job) { addSpec(j.Namespace, &j.Spec.Template.Spec); vix.scanned++ })
		}},
		{volResCronJobs, func() error {
			return listPages("cronjobs", func(o metav1.ListOptions) ([]batchv1.CronJob, string, error) {
				l, err := client.BatchV1().CronJobs(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *batchv1.CronJob) { addSpec(c.Namespace, &c.Spec.JobTemplate.Spec.Template.Spec); vix.scanned++ })
		}},
	}
	for _, step := range steps {
		err := step.run()
		if err == nil {
			continue
		}
		if _, forbidden := ListForbidden(err); forbidden {
			vix.unread = append(vix.unread, step.resource)
			continue
		}
		return err
	}
	return nil
}

// volumeRefusedCost names, per main-pass List, what `state volumes`
// cannot judge without it.
var volumeRefusedCost = map[string]string{
	volPVs:         "zone conflicts not checked and attachments orphaned by a deleted PersistentVolume not reported; unconsumed claims carry no reclaim_policy",
	volAttachments: "attach/detach errors and orphaned attachments not checked",
	volNodes:       "zone conflicts not checked and attachments orphaned by a deleted node not reported",
}

func (vix *volumeIndex) findings(now time.Time) []emit.Finding {
	var out []emit.Finding
	// Every check joins pods to the claims they mount: without either
	// side there is no answer at all, only the reason.
	_, noPods := vix.refused[volPods]
	_, noPVCs := vix.refused[volPVCs]
	if noPods || noPVCs {
		for _, res := range []string{volPods, volPVCs} {
			if r, ok := vix.refused[res]; ok {
				out = append(out, checks.RefusedFinding(r, "no volume check ran: every one joins pods to the claims they mount"))
			}
		}
		return out
	}
	for _, res := range []string{volPVs, volAttachments, volNodes} {
		if r, ok := vix.refused[res]; ok {
			out = append(out, checks.RefusedFinding(r, volumeRefusedCost[res]))
		}
	}
	out = append(out, vix.multiAttach()...)
	out = append(out, vix.zoneConflicts()...)
	out = append(out, vix.attachmentFindings(now)...)
	out = append(out, vix.unconsumedClaims(now)...)
	return out
}

// unconsumedClaims reports Bound claims nothing can be using — every
// exclusion is listed, with its reason, in the package comment. When a
// workload List was refused it reports nothing and says why instead.
func (vix *volumeIndex) unconsumedClaims(now time.Time) []emit.Finding {
	if len(vix.unread) > 0 {
		out := make([]emit.Finding, 0, len(vix.unread))
		for _, r := range vix.unread {
			out = append(out, checks.RefusedFinding(r,
				"unconsumed-claim detection skipped: a claim referenced only by an unread workload template would be misreported as unconsumed"))
		}
		return out
	}

	consumed := map[string]bool{}
	for _, p := range vix.pods {
		for _, v := range p.Spec.Volumes {
			switch {
			case v.PersistentVolumeClaim != nil:
				consumed[key(p.Namespace, v.PersistentVolumeClaim.ClaimName)] = true
			case v.Ephemeral != nil:
				// A generic ephemeral volume's claim is named
				// <pod>-<volume> by the ephemeral-volume controller.
				consumed[key(p.Namespace, p.Name+"-"+v.Name)] = true
			}
		}
	}

	var out []emit.Finding
	for k, pvc := range vix.pvcs {
		if pvc.Status.Phase != corev1.ClaimBound || pvc.DeletionTimestamp != nil {
			continue
		}
		if consumed[k] || vix.templateClaims[k] || volumeOwnedByLifecycle(pvc) {
			continue
		}
		if volumeMatchesClaimTemplate(pvc.Name, vix.stsClaimPrefixes[pvc.Namespace]) {
			continue
		}
		age := now.Sub(pvc.CreationTimestamp.Time)
		if !pvc.CreationTimestamp.IsZero() && age < volumeUnconsumedGrace {
			continue
		}
		out = append(out, vix.unconsumedFinding(pvc, age))
	}
	return out
}

func (vix *volumeIndex) unconsumedFinding(pvc *corev1.PersistentVolumeClaim, age time.Duration) emit.Finding {
	capacity := ""
	if q, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
		capacity = q.String()
	}
	what := "its volume"
	if pvc.Spec.VolumeName != "" {
		what = "volume " + pvc.Spec.VolumeName
	}
	if capacity != "" {
		what += " (" + capacity + ")"
	}
	details := []emit.Field{{Key: "pv", Value: pvc.Spec.VolumeName}}
	if capacity != "" {
		details = append(details, emit.Field{Key: "capacity", Value: capacity})
	}
	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		details = append(details, emit.Field{Key: "storage_class", Value: *pvc.Spec.StorageClassName})
	}
	remedy := "delete the claim if the data is no longer needed"
	if pv := vix.pvs[pvc.Spec.VolumeName]; pv != nil && pv.Spec.PersistentVolumeReclaimPolicy != "" {
		details = append(details, emit.Field{Key: "reclaim_policy", Value: string(pv.Spec.PersistentVolumeReclaimPolicy)})
		if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
			remedy = "the volume is Retain, so deleting the claim leaves the disk behind as a Released PV — delete both if the data is no longer needed"
		}
	}
	details = append(details, emit.Field{Key: "access_modes", Value: volumeAccessModes(pvc.Spec.AccessModes)})
	if !pvc.CreationTimestamp.IsZero() {
		details = append(details, emit.Field{Key: "claim_age", Value: age.Truncate(time.Minute).String()})
	}
	return emit.Finding{
		Kind:         "volume.unconsumed_pvc",
		Severity:     emit.SeverityInfo,
		Namespace:    pvc.Namespace,
		KindOfObject: "PersistentVolumeClaim",
		Name:         pvc.Name,
		Reason:       "NoConsumer",
		Message: fmt.Sprintf("claim is Bound to %s but no pod mounts it and no workload template references it — the storage is provisioned and billed for nothing; %s",
			what, remedy),
		Details: details,
	}
}

// volumeOwnedByLifecycle reports whether a claim's lifetime belongs to
// an owner the garbage collector tracks: a Pod (generic ephemeral
// volume) or a StatefulSet (retention policy Delete).
func volumeOwnedByLifecycle(pvc *corev1.PersistentVolumeClaim) bool {
	for _, o := range pvc.OwnerReferences {
		if o.Kind == "Pod" || o.Kind == "StatefulSet" {
			return true
		}
	}
	return false
}

// volumeMatchesClaimTemplate reports whether name is
// <template>-<statefulset>-<ordinal> for one of the live StatefulSet
// claim-template prefixes in its namespace.
func volumeMatchesClaimTemplate(name string, prefixes []string) bool {
	for _, p := range prefixes {
		rest, ok := strings.CutPrefix(name, p)
		if !ok || rest == "" {
			continue
		}
		if _, err := strconv.ParseUint(rest, 10, 32); err == nil {
			return true
		}
	}
	return false
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
	// A side whose List was refused is unknown, not missing (#546).
	_, pvsUnread := vix.refused[volPVs]
	_, nodesUnread := vix.refused[volNodes]
	if pvName != "" && !pvsUnread && vix.pvs[pvName] == nil {
		missing = append(missing, "pv missing")
		what = "PersistentVolume"
	}
	if va.Spec.NodeName != "" && !nodesUnread && vix.nodes[va.Spec.NodeName] == nil {
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
