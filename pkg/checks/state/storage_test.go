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

package state_test

// `state storage` tests, §13 conventions: fake.Clientset fixtures,
// exact findings per binding failure, a healthy fixture proving zero
// nominal state, a golden mixed cluster, and the checktest contract
// round-trip.
//
// The silent cases carry as much weight as the loud ones here. Every
// Pending-claim rule has a legitimate look-alike — WaitForFirstConsumer,
// a static volume that has not been created yet, an explicit
// storageClassName: "" — and each one is a shape that exists on real,
// healthy clusters. All helpers are stg-prefixed.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func stgCommand(objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return state.StorageCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
}

// stgClass is a StorageClass with a real dynamic provisioner.
func stgClass(name string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Provisioner: "pd.csi.storage.gke.io",
	}
}

// stgDefault marks a class as the cluster default via the stable
// annotation.
func stgDefault(sc *storagev1.StorageClass) *storagev1.StorageClass {
	if sc.Annotations == nil {
		sc.Annotations = map[string]string{}
	}
	sc.Annotations["storageclass.kubernetes.io/is-default-class"] = "true"
	return sc
}

// stgStatic is a class that provisions nothing — the local-volume
// shape.
func stgStatic(name string, mode storagev1.VolumeBindingMode) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: name},
		Provisioner:       "kubernetes.io/no-provisioner",
		VolumeBindingMode: &mode,
	}
}

// stgPVC is a Pending claim naming class (nil class when the pointer
// is nil).
func stgPVC(ns, name string, class *string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: class,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
}

func stgBound(pvc *corev1.PersistentVolumeClaim, volume string) *corev1.PersistentVolumeClaim {
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Spec.VolumeName = volume
	return pvc
}

// stgPV is a volume in the named phase, of the named class.
func stgPV(name, class string, phase corev1.PersistentVolumePhase) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName:              class,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
		},
		Status: corev1.PersistentVolumeStatus{Phase: phase},
	}
}

func stgPtr(s string) *string { return &s }

// stgHealthy is a cluster where storage works: one default class, one
// extra class, a bound claim against each, and their volumes.
func stgHealthy() []runtime.Object {
	return []runtime.Object{
		stgDefault(stgClass("standard")),
		stgClass("fast"),
		stgBound(stgPVC(ns, "data", stgPtr("standard")), "pv-data"),
		stgBound(stgPVC(ns, "cache", stgPtr("fast")), "pv-cache"),
		stgPV("pv-data", "standard", corev1.VolumeBound),
		stgPV("pv-cache", "fast", corev1.VolumeBound),
	}
}

// stgFindings runs `state storage` over objs and returns the finding
// lines (summary stripped), failing on non-zero exit.
func stgFindings(t *testing.T, objs []runtime.Object, args ...string) []string {
	t.Helper()
	res := checktest.Run(t, stgCommand(objs...), args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
		t.Fatalf("no summary line: %q", res.Stdout)
	}
	return lines[:len(lines)-1]
}

func TestStorageMissingClass(t *testing.T) {
	objs := []runtime.Object{
		stgDefault(stgClass("standard")),
		stgClass("fast"),
		stgPVC(ns, "data", stgPtr("premium-rwo")),
	}
	wantFindings(t, stgFindings(t, objs), []string{
		`kind=storage.missing_class severity=critical namespace=prod kind_of_object=PersistentVolumeClaim name=data reason=MissingStorageClass message="claim names storageclass \"premium-rwo\", which does not exist — nothing will provision a volume and the claim stays Pending forever; the cluster has storageclass(es) fast,standard" storage_class=premium-rwo classes=fast,standard phase=Pending requested=10Gi`,
	})
}

// The message names the classes that DO exist, which is what turns
// the finding into a fix — a typo is obvious the moment the real
// names sit next to it. A cluster with no classes at all says so
// rather than rendering an empty list.
func TestStorageMissingClassNamesWhatExists(t *testing.T) {
	got := stgFindings(t, []runtime.Object{stgPVC(ns, "data", stgPtr("standard"))})
	if len(got) != 1 {
		t.Fatalf("got %d findings, want 1: %v", len(got), got)
	}
	if !strings.Contains(got[0], "the cluster has no storageclasses at all") {
		t.Errorf("message should say the cluster has none: %s", got[0])
	}
}

// A Bound claim is never reported, however odd its spec: it bound, so
// the class question is moot. This is the shape left behind when a
// class is deleted after its claims bound — common, and not a defect.
func TestStorageBoundClaimIsSilent(t *testing.T) {
	objs := []runtime.Object{
		stgBound(stgPVC(ns, "data", stgPtr("deleted-class")), "pv-data"),
		stgPV("pv-data", "deleted-class", corev1.VolumeBound),
	}
	wantFindings(t, stgFindings(t, objs), nil)
}

// A claim pre-bound to a named volume is waiting on that volume, not
// on a class.
func TestStoragePreBoundClaimIsSilent(t *testing.T) {
	pvc := stgPVC(ns, "data", stgPtr("nope"))
	pvc.Spec.VolumeName = "pv-manual"
	wantFindings(t, stgFindings(t, []runtime.Object{pvc}), nil)
}

func TestStorageNoDefaultClass(t *testing.T) {
	objs := []runtime.Object{
		stgClass("fast"), // exists, but nothing is the default
		stgPVC(ns, "data", nil),
	}
	wantFindings(t, stgFindings(t, objs), []string{
		`kind=storage.no_default_class severity=critical namespace=prod kind_of_object=PersistentVolumeClaim name=data reason=NoDefaultStorageClass message="claim names no storageclass and no storageclass is annotated as the cluster default — nothing provisions it and no classless volume is Available; the cluster has storageclass(es) fast" classes=fast phase=Pending requested=10Gi`,
	})
}

func TestStorageNoDefaultClassSilentCases(t *testing.T) {
	tests := []struct {
		name string
		objs []runtime.Object
	}{{
		// The ordinary case: a default exists, so the admission plugin
		// stamped it and the claim is Pending for some other reason.
		name: "a default exists",
		objs: []runtime.Object{stgDefault(stgClass("standard")), stgPVC(ns, "data", nil)},
	}, {
		// The beta annotation is still honoured by the admission
		// plugin and still written by several installers.
		name: "beta default annotation",
		objs: func() []runtime.Object {
			sc := stgClass("standard")
			sc.Annotations = map[string]string{"storageclass.beta.kubernetes.io/is-default-class": "true"}
			return []runtime.Object{sc, stgPVC(ns, "data", nil)}
		}(),
	}, {
		// A free classless volume means this is a working static
		// setup and the binder is about to match them.
		name: "classless volume available",
		objs: []runtime.Object{stgPVC(ns, "data", nil), stgPV("pv-manual", "", corev1.VolumeAvailable)},
	}, {
		// An explicit "" is "do not provision me, match me to a
		// pre-provisioned volume" — a deliberate choice, and Pending
		// under it means a human has not created the volume yet.
		name: "explicit empty class",
		objs: []runtime.Object{stgPVC(ns, "data", stgPtr(""))},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantFindings(t, stgFindings(t, tc.objs), nil)
		})
	}
}

func TestStorageMultipleDefaults(t *testing.T) {
	objs := []runtime.Object{
		stgDefault(stgClass("standard")),
		stgDefault(stgClass("premium-rwo")),
		stgClass("fast"),
	}
	// One finding per offending class: each is an object somebody has
	// to go and edit.
	wantFindings(t, stgFindings(t, objs), []string{
		`kind=storage.multiple_defaults severity=warning kind_of_object=StorageClass name=premium-rwo reason=MultipleDefaultStorageClasses message="2 storageclasses are annotated as the cluster default (premium-rwo,standard) — a claim that names no class gets whichever was created most recently, so the class a workload lands on changes without its spec changing" defaults=premium-rwo,standard provisioner=pd.csi.storage.gke.io`,
		`kind=storage.multiple_defaults severity=warning kind_of_object=StorageClass name=standard reason=MultipleDefaultStorageClasses message="2 storageclasses are annotated as the cluster default (premium-rwo,standard) — a claim that names no class gets whichever was created most recently, so the class a workload lands on changes without its spec changing" defaults=premium-rwo,standard provisioner=pd.csi.storage.gke.io`,
	})
}

func TestStorageSingleDefaultIsSilent(t *testing.T) {
	objs := []runtime.Object{stgDefault(stgClass("standard")), stgClass("fast")}
	wantFindings(t, stgFindings(t, objs), nil)
}

func TestStorageNoProvisioner(t *testing.T) {
	objs := []runtime.Object{
		stgStatic("local", storagev1.VolumeBindingImmediate),
		stgPVC(ns, "data", stgPtr("local")),
	}
	wantFindings(t, stgFindings(t, objs), []string{
		`kind=storage.no_provisioner severity=warning namespace=prod kind_of_object=PersistentVolumeClaim name=data reason=NoDynamicProvisioner message="claim wants storageclass \"local\", whose provisioner is kubernetes.io/no-provisioner — that class creates nothing, and no volume of it is Available; the claim binds only once somebody pre-provisions one" storage_class=local provisioner=kubernetes.io/no-provisioner binding_mode=Immediate phase=Pending requested=10Gi`,
	})
}

// The two shapes that make a static-only class perfectly healthy.
// k8sgpt flags every no-provisioner class outright, which fires on
// every cluster running local-path or the local-volume provisioner.
func TestStorageNoProvisionerSilentCases(t *testing.T) {
	tests := []struct {
		name string
		objs []runtime.Object
	}{{
		// WaitForFirstConsumer: Pending until a pod is scheduled is
		// the entire design of the mode, and it is what the standard
		// local-volume class ships with.
		name: "wait for first consumer",
		objs: []runtime.Object{
			stgStatic("local", storagev1.VolumeBindingWaitForFirstConsumer),
			stgPVC(ns, "data", stgPtr("local")),
		},
	}, {
		// A free volume of that class is sitting there; the binder is
		// mid-flight.
		name: "matching volume available",
		objs: []runtime.Object{
			stgStatic("local", storagev1.VolumeBindingImmediate),
			stgPVC(ns, "data", stgPtr("local")),
			stgPV("pv-local", "local", corev1.VolumeAvailable),
		},
	}, {
		// A class with a real provisioner is never this check's
		// problem: a provisioner that fails writes its own events.
		name: "real provisioner",
		objs: []runtime.Object{stgClass("standard"), stgPVC(ns, "data", stgPtr("standard"))},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantFindings(t, stgFindings(t, tc.objs), nil)
		})
	}
}

func TestStorageVolumePhases(t *testing.T) {
	released := stgPV("pv-released", "standard", corev1.VolumeReleased)
	released.Spec.ClaimRef = &corev1.ObjectReference{Namespace: ns, Name: "gone"}
	failed := stgPV("pv-failed", "standard", corev1.VolumeFailed)
	failed.Status.Message = "error deleting EBS volume: VolumeInUse"
	objs := []runtime.Object{stgDefault(stgClass("standard")), released, failed}
	wantFindings(t, stgFindings(t, objs), []string{
		`kind=storage.pv_failed severity=warning kind_of_object=PersistentVolume name=pv-failed reason=VolumeFailed message="volume is Failed: error deleting EBS volume: VolumeInUse" phase=Failed storage_class=standard reclaim_policy=Retain capacity=10Gi`,
		`kind=storage.pv_released severity=info kind_of_object=PersistentVolume name=pv-released reason=VolumeReleased message="volume is Released — its claim is gone but the volume was retained; the capacity stays unusable until spec.claimRef is cleared or the volume is deleted" phase=Released storage_class=standard reclaim_policy=Retain capacity=10Gi claim=prod/gone`,
	})
}

// Available and Bound are the two working phases and neither is worth
// a line — including Available, which is the whole point of a
// statically provisioned pool.
func TestStorageHealthyVolumePhasesAreSilent(t *testing.T) {
	objs := []runtime.Object{
		stgDefault(stgClass("standard")),
		stgPV("pv-bound", "standard", corev1.VolumeBound),
		stgPV("pv-free", "standard", corev1.VolumeAvailable),
	}
	wantFindings(t, stgFindings(t, objs), nil)
}

func TestStorageHealthyIsSilent(t *testing.T) {
	res := checktest.Run(t, stgCommand(stgHealthy()...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "scanned=6 findings=0 elapsed=100ms\n"
	if res.Stdout != want {
		t.Errorf("healthy cluster must emit only the summary:\ngot:  %qwant: %q", res.Stdout, want)
	}
}

// --namespace restricts claims. Classes and volumes are cluster-scoped
// and stay in the scan — a namespaced claim failing on cluster-scoped
// state is the whole subject of the check — so the multiple-defaults
// finding survives scoping while the other namespace's claim does not.
func TestStorageNamespaceScoping(t *testing.T) {
	objs := []runtime.Object{
		stgClass("fast"),
		stgPVC("other", "data", stgPtr("premium-rwo")),
		stgBound(stgPVC(ns, "cache", stgPtr("fast")), "pv-cache"),
		stgPV("pv-cache", "fast", corev1.VolumeBound),
	}
	if got := stgFindings(t, objs); len(got) != 1 {
		t.Fatalf("unscoped run should see the claim in namespace other, got %d findings: %v", len(got), got)
	}
	wantFindings(t, stgFindings(t, objs, "--namespace="+ns), nil)
}

func TestStorageWorkloadIsUsageError(t *testing.T) {
	res := checktest.Run(t, stgCommand(stgHealthy()...), "--workload=Deployment/prod/api")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d (stderr: %s)", res.Code, emit.ExitUsage, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "scope with --namespace") {
		t.Errorf("stderr %q should point at --namespace", res.Stderr)
	}
	if strings.Contains(res.Stdout, "scanned=") {
		t.Errorf("usage-error run must not emit a summary, stdout: %q", res.Stdout)
	}
}

// stgMixed hits every finding class at once. The golden pins
// full-output ordering and formatting.
func stgMixed() []runtime.Object {
	released := stgPV("pv-released", "standard", corev1.VolumeReleased)
	released.Spec.ClaimRef = &corev1.ObjectReference{Namespace: ns, Name: "gone"}
	return []runtime.Object{
		stgDefault(stgClass("standard")),
		stgDefault(stgClass("premium-rwo")),
		stgStatic("local", storagev1.VolumeBindingImmediate),
		stgPVC(ns, "typo", stgPtr("premium")),
		stgPVC(ns, "static", stgPtr("local")),
		stgBound(stgPVC(ns, "ok", stgPtr("standard")), "pv-ok"),
		stgPV("pv-ok", "standard", corev1.VolumeBound),
		released,
		stgPV("pv-failed", "standard", corev1.VolumeFailed),
	}
}

func TestStorageMixedGolden(t *testing.T) {
	res := checktest.Run(t, stgCommand(stgMixed()...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	volGolden(t, "storage-mixed.golden", res.Stdout)
}

func TestStorageContract(t *testing.T) {
	checktest.VerifyContract(t, stgCommand(stgMixed()...))
}

func TestStorageRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("state storage")
	if !ok {
		t.Fatal("state storage is not registered in the default registry")
	}
	if c.MCPName != "k8s_storage_binding" {
		t.Errorf("MCP tool name = %q, want k8s_storage_binding", c.MCPName)
	}
	if !strings.Contains(c.Help(), "--namespace") {
		t.Error("generated help does not document --namespace scoping")
	}
}
