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

// `state storage`: PersistentVolumeClaim *binding* hygiene — the
// claims that will never bind, and the StorageClass/PersistentVolume
// state that explains why.
//
// This is the other half of `state volumes`. That check answers "the
// volume exists and bound, so why won't it attach"; this one answers
// "why is the claim still Pending". They deliberately do not overlap:
// nothing here looks at VolumeAttachments, and nothing there looks at
// StorageClasses.
//
// One paged List pass over StorageClasses + PersistentVolumes
// (cluster-scoped, always listed) and PersistentVolumeClaims
// (namespace-scopable), then pure joins. It does NOT go through
// state.LoadCluster: adding these kinds to
// LoadClusterListRequirements() would pull in the shipped ClusterRole,
// rbac_test.go which pins the two together, and `bundle --lists` — a
// wide blast radius for kinds one check reads.
//
// Finding kinds and severities:
//
//	storage.missing_class      critical  Pending claim names a StorageClass that does not exist
//	storage.no_default_class   critical  Pending claim names no class and the cluster has no default
//	storage.multiple_defaults  warning   more than one StorageClass claims to be the default
//	storage.no_provisioner     warning   Pending claim wants dynamic provisioning from a static-only class
//	storage.pv_failed          warning   PersistentVolume in Failed — reclaim did not complete
//	storage.pv_released        info      PersistentVolume in Released — capacity stranded until reclaimed
//
// Every Pending-claim rule is gated on evidence, not on shape alone.
// A claim that is Bound is never reported, however odd its spec looks:
// it bound, so the question is moot. A claim that is Pending because
// its class binds WaitForFirstConsumer, or because a matching
// pre-provisioned volume has not been created yet, is also not
// reported — those are normal intermediate states, and reporting them
// would make the check fire on every healthy cluster that uses local
// or statically provisioned storage.
//
// Healthy storage is silent (§4.2 zero nominal state).

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(StorageCommand(Deps{}))
}

// storageDefaultAnnotation marks the StorageClass a claim with no
// storageClassName falls back to. Like the IngressClass equivalent it
// is still an annotation — there is no field — so the string is the
// API. The beta key is honoured by the DefaultStorageClass admission
// plugin to this day and is still what several installers write.
const (
	storageDefaultAnnotation     = "storageclass.kubernetes.io/is-default-class"
	storageDefaultAnnotationBeta = "storageclass.beta.kubernetes.io/is-default-class"
)

// storageNoProvisioner is the sentinel provisioner for classes that
// create nothing: local volumes and other statically provisioned
// storage. It is not deprecated — k8sgpt's storage analyzer calls it
// that and flags every such class, which is a false positive on every
// cluster running local-path or the local-volume static provisioner.
// What is actually reportable is narrower: a claim that expects
// dynamic provisioning from one of these classes and has no volume
// waiting for it.
const storageNoProvisioner = "kubernetes.io/no-provisioner"

// StorageCommand builds the `lookout state storage` command.
func StorageCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "state storage",
		MCPName: "k8s_storage_binding",
		Summary: "When a PersistentVolumeClaim sits Pending and the pod behind it will not schedule — name the reason: a StorageClass that does not exist, no class and no cluster default, a static-only class with nothing pre-provisioned, plus the default-class ambiguity and stranded volumes behind it.",
		Output: []checks.OutputField{
			{Name: "storage_class", Doc: "StorageClass the claim names, or the class the finding is about"},
			{Name: "classes", Doc: "StorageClasses the cluster does have, sorted (empty when there are none)"},
			{Name: "defaults", Doc: "StorageClasses annotated as the cluster default, sorted"},
			{Name: "provisioner", Doc: "the class's spec.provisioner"},
			{Name: "phase", Doc: "the claim's or volume's status.phase at scan time"},
			{Name: "requested", Doc: "storage the claim requests (spec.resources.requests.storage)"},
			{Name: "capacity", Doc: "the volume's spec.capacity.storage"},
			{Name: "reclaim_policy", Doc: "the volume's spec.persistentVolumeReclaimPolicy"},
			{Name: "claim", Doc: "the claim the volume was bound to, as namespace/name"},
			{Name: "binding_mode", Doc: "the class's volumeBindingMode (Immediate when unset)"},
		},
		Examples: []string{
			"lookout state storage",
			"lookout state storage --namespace=prod",
			"lookout state storage --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runStorage(ctx, deps, inv)
		},
	}
}

func runStorage(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("state storage scans claim binding cluster-wide; scope with --namespace")
	}
	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	// --namespace restricts claims; StorageClasses and volumes are
	// cluster-scoped and always listed — the whole point of the check
	// is that a namespaced claim fails on cluster-scoped state.
	ns := inv.Scope.Namespace
	if ns == "" || inv.Scope.AllNamespaces {
		ns = metav1.NamespaceAll
	}
	six, err := listStorageIndex(ctx, client, ns)
	if err != nil {
		return 0, err
	}
	findings := six.findings()
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
	return six.scanned, nil
}

// storageIndex holds the one List pass `state storage` joins over.
type storageIndex struct {
	scanned int // every listed object across all three kinds

	classes  map[string]*storagev1.StorageClass // name
	defaults []string                           // class names annotated default, sorted
	pvs      []*corev1.PersistentVolume
	pvcs     []*corev1.PersistentVolumeClaim

	// availableByClass counts volumes sitting in Available keyed by
	// their spec.storageClassName ("" for classless). A claim waiting
	// on a class with a free volume is mid-bind, not stuck.
	availableByClass map[string]int
}

func listStorageIndex(ctx context.Context, client kubernetes.Interface, ns string) (*storageIndex, error) {
	six := &storageIndex{
		classes:          map[string]*storagev1.StorageClass{},
		availableByClass: map[string]int{},
	}
	steps := []func() error{
		func() error {
			return listPages("storageclasses", func(o metav1.ListOptions) ([]storagev1.StorageClass, string, error) {
				l, err := client.StorageV1().StorageClasses().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(sc *storagev1.StorageClass) {
				six.classes[sc.Name] = sc
				if storageIsDefault(sc) {
					six.defaults = append(six.defaults, sc.Name)
				}
				six.scanned++
			})
		},
		func() error {
			return listPages("persistentvolumes", func(o metav1.ListOptions) ([]corev1.PersistentVolume, string, error) {
				l, err := client.CoreV1().PersistentVolumes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(pv *corev1.PersistentVolume) {
				six.pvs = append(six.pvs, pv)
				if pv.Status.Phase == corev1.VolumeAvailable {
					six.availableByClass[pv.Spec.StorageClassName]++
				}
				six.scanned++
			})
		},
		func() error {
			return listPages("persistentvolumeclaims", func(o metav1.ListOptions) ([]corev1.PersistentVolumeClaim, string, error) {
				l, err := client.CoreV1().PersistentVolumeClaims(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *corev1.PersistentVolumeClaim) { six.pvcs = append(six.pvcs, c); six.scanned++ })
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, err
		}
	}
	sort.Strings(six.defaults)
	return six, nil
}

// storageIsDefault reports whether the class carries either spelling
// of the is-default-class annotation set to "true".
func storageIsDefault(sc *storagev1.StorageClass) bool {
	return sc.Annotations[storageDefaultAnnotation] == "true" ||
		sc.Annotations[storageDefaultAnnotationBeta] == "true"
}

func (six *storageIndex) findings() []emit.Finding {
	var out []emit.Finding
	out = append(out, six.multipleDefaults()...)
	out = append(out, six.claimFindings()...)
	out = append(out, six.volumeFindings()...)
	return out
}

// multipleDefaults reports every StorageClass in a cluster that has
// more than one default. Which of them a classless claim actually
// gets is decided by the DefaultStorageClass admission plugin, which
// picks the most recently created — so the answer changes when
// somebody reinstalls a CSI driver, and the claim that bound to fast
// SSD last month binds to spinning disk today with nothing in any log
// to say so.
//
// One finding per offending class, not one for the cluster: each of
// them is an object somebody has to go and edit, and un-annotating one
// resolves that one and silences the rest at the same time.
func (six *storageIndex) multipleDefaults() []emit.Finding {
	if len(six.defaults) < 2 {
		return nil
	}
	var out []emit.Finding
	for _, name := range six.defaults {
		out = append(out, emit.Finding{
			Kind:         "storage.multiple_defaults",
			Severity:     emit.SeverityWarning,
			KindOfObject: "StorageClass",
			Name:         name,
			Reason:       "MultipleDefaultStorageClasses",
			Message: fmt.Sprintf("%d storageclasses are annotated as the cluster default (%s) — a claim that names no class gets whichever was created most recently, so the class a workload lands on changes without its spec changing",
				len(six.defaults), strings.Join(six.defaults, ",")),
			Details: []emit.Field{
				{Key: "defaults", Value: strings.Join(six.defaults, ",")},
				{Key: "provisioner", Value: six.provisionerOf(name)},
			},
		})
	}
	return out
}

func (six *storageIndex) provisionerOf(name string) string {
	if sc := six.classes[name]; sc != nil {
		return sc.Provisioner
	}
	return ""
}

// claimFindings reports Pending claims whose class is the reason they
// are Pending. Order matters: the three rules are mutually exclusive
// by construction (a class is named and missing, or named and
// static-only, or not named at all), so a claim yields at most one.
func (six *storageIndex) claimFindings() []emit.Finding {
	var out []emit.Finding
	for _, pvc := range six.pvcs {
		if pvc.Status.Phase != corev1.ClaimPending {
			continue // Bound and Lost are other checks' questions
		}
		if pvc.Spec.VolumeName != "" {
			continue // pre-bound to a named volume; the class is not in play
		}
		switch {
		case pvc.Spec.StorageClassName == nil:
			if f, ok := six.noDefaultClass(pvc); ok {
				out = append(out, f)
			}
		case *pvc.Spec.StorageClassName == "":
			// An explicit "" means "no dynamic provisioning, match me
			// to a pre-provisioned volume". That is a deliberate
			// choice, and a Pending claim under it is waiting for a
			// human to create the volume — not a defect we can see.
			continue
		default:
			if f, ok := six.namedClass(pvc, *pvc.Spec.StorageClassName); ok {
				out = append(out, f)
			}
		}
	}
	return out
}

// namedClass handles a Pending claim that names a class: the class is
// missing, or it exists but provisions nothing.
func (six *storageIndex) namedClass(pvc *corev1.PersistentVolumeClaim, want string) (emit.Finding, bool) {
	sc := six.classes[want]
	if sc == nil {
		have := sortedKeys(six.classes)
		message := fmt.Sprintf("claim names storageclass %q, which does not exist — nothing will provision a volume and the claim stays Pending forever; the cluster has %s",
			want, storageClassList(have))
		return emit.Finding{
			Kind:         "storage.missing_class",
			Severity:     emit.SeverityCritical,
			Namespace:    pvc.Namespace,
			KindOfObject: "PersistentVolumeClaim",
			Name:         pvc.Name,
			Reason:       "MissingStorageClass",
			Message:      message,
			Details: []emit.Field{
				{Key: "storage_class", Value: want},
				{Key: "classes", Value: strings.Join(have, ",")},
				{Key: "phase", Value: string(pvc.Status.Phase)},
				{Key: "requested", Value: storageRequested(pvc)},
			},
		}, true
	}
	if sc.Provisioner != storageNoProvisioner {
		return emit.Finding{}, false // a real provisioner owns this; its failures are its own events
	}
	if storageBindingMode(sc) == storagev1.VolumeBindingWaitForFirstConsumer {
		return emit.Finding{}, false // Pending until a pod is scheduled is the whole design
	}
	if six.availableByClass[want] > 0 {
		return emit.Finding{}, false // a free volume is sitting there; binding is in flight
	}
	return emit.Finding{
		Kind:         "storage.no_provisioner",
		Severity:     emit.SeverityWarning,
		Namespace:    pvc.Namespace,
		KindOfObject: "PersistentVolumeClaim",
		Name:         pvc.Name,
		Reason:       "NoDynamicProvisioner",
		Message: fmt.Sprintf("claim wants storageclass %q, whose provisioner is %s — that class creates nothing, and no volume of it is Available; the claim binds only once somebody pre-provisions one",
			want, storageNoProvisioner),
		Details: []emit.Field{
			{Key: "storage_class", Value: want},
			{Key: "provisioner", Value: sc.Provisioner},
			{Key: "binding_mode", Value: string(storageBindingMode(sc))},
			{Key: "phase", Value: string(pvc.Status.Phase)},
			{Key: "requested", Value: storageRequested(pvc)},
		},
	}, true
}

// noDefaultClass handles a Pending claim that names no class at all in
// a cluster with no default. The admission plugin had nothing to
// stamp, so the claim carries no class and can only ever bind to a
// classless volume — and if none is Available it never will.
func (six *storageIndex) noDefaultClass(pvc *corev1.PersistentVolumeClaim) (emit.Finding, bool) {
	if len(six.defaults) > 0 {
		return emit.Finding{}, false
	}
	if six.availableByClass[""] > 0 {
		return emit.Finding{}, false // a classless volume is free; this is a normal static setup
	}
	have := sortedKeys(six.classes)
	return emit.Finding{
		Kind:         "storage.no_default_class",
		Severity:     emit.SeverityCritical,
		Namespace:    pvc.Namespace,
		KindOfObject: "PersistentVolumeClaim",
		Name:         pvc.Name,
		Reason:       "NoDefaultStorageClass",
		Message: fmt.Sprintf("claim names no storageclass and no storageclass is annotated as the cluster default — nothing provisions it and no classless volume is Available; the cluster has %s",
			storageClassList(have)),
		Details: []emit.Field{
			{Key: "classes", Value: strings.Join(have, ",")},
			{Key: "defaults", Value: ""},
			{Key: "phase", Value: string(pvc.Status.Phase)},
			{Key: "requested", Value: storageRequested(pvc)},
		},
	}, true
}

// volumeFindings reports volumes in the two phases that mean nobody
// can use the space they hold.
//
// Failed is a warning: the reclaim — a Delete the driver could not
// complete, or a recycle that errored — is stuck, and the underlying
// disk is almost certainly still being billed for.
//
// Released is info, not a warning: with reclaimPolicy Retain it is
// exactly what the operator asked for, and the data is deliberately
// being kept. It is worth naming because the capacity is unusable
// until somebody clears spec.claimRef, and because a shelf of Released
// volumes is the usual explanation for a cluster that is out of quota
// with nothing running.
func (six *storageIndex) volumeFindings() []emit.Finding {
	var out []emit.Finding
	for _, pv := range six.pvs {
		var kind, severity, reason, message string
		switch pv.Status.Phase {
		case corev1.VolumeFailed:
			kind, severity, reason = "storage.pv_failed", emit.SeverityWarning, "VolumeFailed"
			message = "volume is Failed — its reclaim did not complete, so the backing disk is still allocated and the volume cannot be reused"
			if pv.Status.Message != "" {
				message = fmt.Sprintf("volume is Failed: %s", volumeTruncate(pv.Status.Message))
			}
		case corev1.VolumeReleased:
			kind, severity, reason = "storage.pv_released", emit.SeverityInfo, "VolumeReleased"
			message = "volume is Released — its claim is gone but the volume was retained; the capacity stays unusable until spec.claimRef is cleared or the volume is deleted"
		default:
			continue
		}
		out = append(out, emit.Finding{
			Kind:         kind,
			Severity:     severity,
			KindOfObject: "PersistentVolume",
			Name:         pv.Name,
			Reason:       reason,
			Message:      message,
			Details: []emit.Field{
				{Key: "phase", Value: string(pv.Status.Phase)},
				{Key: "storage_class", Value: pv.Spec.StorageClassName},
				{Key: "reclaim_policy", Value: string(pv.Spec.PersistentVolumeReclaimPolicy)},
				{Key: "capacity", Value: storageCapacity(pv)},
				{Key: "claim", Value: storageClaimRef(pv)},
			},
		})
	}
	return out
}

// storageBindingMode returns the class's volumeBindingMode, defaulting
// to Immediate the way the API server does when the field is unset.
func storageBindingMode(sc *storagev1.StorageClass) storagev1.VolumeBindingMode {
	if sc.VolumeBindingMode == nil {
		return storagev1.VolumeBindingImmediate
	}
	return *sc.VolumeBindingMode
}

// storageClassList renders the "the cluster has …" tail of a message,
// which is the part that turns the finding into a fix: a typo is
// obvious the moment the real names are next to it.
func storageClassList(names []string) string {
	if len(names) == 0 {
		return "no storageclasses at all"
	}
	return "storageclass(es) " + strings.Join(names, ",")
}

func storageRequested(pvc *corev1.PersistentVolumeClaim) string {
	if q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		return q.String()
	}
	return ""
}

func storageCapacity(pv *corev1.PersistentVolume) string {
	if q, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
		return q.String()
	}
	return ""
}

func storageClaimRef(pv *corev1.PersistentVolume) string {
	if pv.Spec.ClaimRef == nil {
		return ""
	}
	return key(pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
}
