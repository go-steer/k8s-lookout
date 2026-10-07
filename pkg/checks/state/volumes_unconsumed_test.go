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

// volume.unconsumed_pvc (#231): one exact assertion for the defect,
// one look-alike per exclusion the package comment names, the
// deleted-StatefulSet case that must still fire, and the refused-List
// degradation.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// volBoundPVC is a Bound claim created age before fixedNow, with a
// 10Gi capacity on class standard-rwo.
func volBoundPVC(ns, name, volumeName string, age time.Duration) *corev1.PersistentVolumeClaim {
	pvc := volPVC(ns, name, volumeName, corev1.ReadWriteOnce)
	pvc.CreationTimestamp = metav1.NewTime(fixedNow.Add(-age))
	class := "standard-rwo"
	pvc.Spec.StorageClassName = &class
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
	return pvc
}

// volRecords runs cmd with --format=json and returns its finding
// records (summary dropped), failing on a non-zero exit.
func volRecords(t *testing.T, cmd checks.Command) []map[string]any {
	t.Helper()
	res := checktest.Run(t, cmd, "--format=json")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		rec := map[string]any{}
		if err := json.Unmarshal([]byte(l), &rec); err != nil {
			t.Fatalf("parsing %q: %v", l, err)
		}
		if _, ok := rec["kind"]; !ok {
			continue // the summary record
		}
		out = append(out, rec)
	}
	return out
}

// volReclaimPV is a PV with the given reclaim policy.
func volReclaimPV(name string, policy corev1.PersistentVolumeReclaimPolicy) *corev1.PersistentVolume {
	pv := volPV(name)
	pv.Spec.PersistentVolumeReclaimPolicy = policy
	return pv
}

// volClaimTemplate is a pod spec mounting claim.
func volClaimTemplate(claim string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
		Name:         "data",
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}},
	}}}}
}

func TestVolumesUnconsumedClaim(t *testing.T) {
	wantFindings(t, volFindings(t, []runtime.Object{
		volBoundPVC(ns, "old-data", "pv-old", 72*time.Hour),
		volReclaimPV("pv-old", corev1.PersistentVolumeReclaimRetain),
		volBoundPVC(ns, "scratch", "pv-scratch", 48*time.Hour),
		volReclaimPV("pv-scratch", corev1.PersistentVolumeReclaimDelete),
	}), []string{
		`kind=volume.unconsumed_pvc severity=info namespace=prod kind_of_object=PersistentVolumeClaim name=old-data reason=NoConsumer message="claim is Bound to volume pv-old (10Gi) but no pod mounts it and no workload template references it — the storage is provisioned and billed for nothing; the volume is Retain, so deleting the claim leaves the disk behind as a Released PV — delete both if the data is no longer needed" pv=pv-old capacity=10Gi storage_class=standard-rwo reclaim_policy=Retain access_modes=ReadWriteOnce claim_age=72h0m0s`,
		`kind=volume.unconsumed_pvc severity=info namespace=prod kind_of_object=PersistentVolumeClaim name=scratch reason=NoConsumer message="claim is Bound to volume pv-scratch (10Gi) but no pod mounts it and no workload template references it — the storage is provisioned and billed for nothing; delete the claim if the data is no longer needed" pv=pv-scratch capacity=10Gi storage_class=standard-rwo reclaim_policy=Delete access_modes=ReadWriteOnce claim_age=48h0m0s`,
	})
}

// TestVolumesUnconsumedLookalikesAreSilent is one fixture per
// exclusion: every claim below is Bound and old, and every one has a
// reason it is not waste.
func TestVolumesUnconsumedLookalikesAreSilent(t *testing.T) {
	old := 72 * time.Hour
	zero := int32(0)
	one := int32(1)

	pendingPod := volPod(ns, "api-0", "", "by-pending-pod") // unscheduled
	donePod := volPod(ns, "migrate-x", "node-a", "by-completed-pod")
	donePod.Status.Phase = corev1.PodSucceeded
	ephemeralPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "builder"},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{
			Name:         "scratch",
			VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}},
		}}},
	}

	parkedDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "parked"},
		Spec:       appsv1.DeploymentSpec{Replicas: &zero, Template: volClaimTemplate("by-scaled-to-zero")},
	}
	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "nightly"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{
			Spec: batchv1.JobSpec{Template: volClaimTemplate("by-cronjob")},
		}},
	}
	// A StatefulSet scaled down to one replica keeps data-db-1 and
	// data-db-2 for the next scale-up.
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "db"},
		Spec: appsv1.StatefulSetSpec{
			Replicas:             &one,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
		},
	}

	ownedByPod := volBoundPVC(ns, "owned-by-pod", "pv-8", old)
	ownedByPod.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: "gone", APIVersion: "v1"}}
	deleting := volBoundPVC(ns, "deleting", "pv-9", old)
	deleting.DeletionTimestamp = &metav1.Time{Time: fixedNow}
	deleting.Finalizers = []string{"kubernetes.io/pvc-protection"}
	pending := volBoundPVC(ns, "still-pending", "", old)
	pending.Status.Phase = corev1.ClaimPending

	wantFindings(t, volFindings(t, []runtime.Object{
		pendingPod, donePod, ephemeralPod, parkedDeploy, cron, sts,
		volNode("node-a", "us-east1-b"),
		volBoundPVC(ns, "by-pending-pod", "pv-1", old),
		volBoundPVC(ns, "by-completed-pod", "pv-2", old),
		volBoundPVC(ns, "builder-scratch", "pv-3", old),
		volBoundPVC(ns, "by-scaled-to-zero", "pv-4", old),
		volBoundPVC(ns, "by-cronjob", "pv-5", old),
		volBoundPVC(ns, "data-db-2", "pv-6", old),
		volBoundPVC(ns, "fresh", "pv-7", 10*time.Minute), // inside the grace window
		ownedByPod, deleting, pending,
	}), nil)
}

// A claim template only matches its exact <template>-<sts>-<ordinal>
// shape; the claims of a StatefulSet that no longer exists are waste.
func TestVolumesUnconsumedOrphanedStatefulSetClaims(t *testing.T) {
	one := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "db"},
		Spec: appsv1.StatefulSetSpec{
			Replicas:             &one,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
		},
	}
	recs := volRecords(t, volCommand(
		sts,
		volBoundPVC(ns, "data-db-0", "pv-0", 72*time.Hour),
		volBoundPVC(ns, "data-db-backup", "pv-1", 72*time.Hour), // not an ordinal
		volBoundPVC(ns, "data-cache-0", "pv-2", 72*time.Hour),   // StatefulSet cache was deleted
		volBoundPVC("other", "data-db-0", "pv-3", 72*time.Hour), // same name, another namespace
	))
	var names []string
	for _, r := range recs {
		names = append(names, fmt.Sprint(r["namespace"], "/", r["name"]))
	}
	want := "other/data-db-0,prod/data-cache-0,prod/data-db-backup"
	if strings.Join(names, ",") != want {
		t.Errorf("unconsumed claims = %v, want %s", names, want)
	}
}

// A refused workload List costs the claim judgment and nothing else:
// the multi-attach conflict still reports, the unconsumed claim does
// not, and the gap is named.
func TestVolumesUnconsumedDegradesOnForbiddenList(t *testing.T) {
	objs := []runtime.Object{
		volPod(ns, "web-0", "node-a", "shared"),
		volPod(ns, "web-1", "node-b", "shared"),
		volPVC(ns, "shared", "pv-shared", corev1.ReadWriteOnce),
		volBoundPVC(ns, "old-data", "pv-old", 72*time.Hour),
		volNode("node-a", "us-east1-b"),
		volNode("node-b", "us-east1-b"),
	}
	cs := fake.NewClientset(objs...)
	cs.PrependReactor("list", "cronjobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "batch", Resource: "cronjobs"}, "", errors.New("cannot list resource"))
	})
	cmd := state.VolumesCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
	var kinds []string
	for _, rec := range volRecords(t, cmd) {
		kinds = append(kinds, fmt.Sprint(rec["kind"]))
		if rec["kind"] == state.KindReadUnavailable && (rec["resource"] != "cronjobs.batch" || rec["reason"] != "ListForbidden") {
			t.Errorf("read.unavailable = %v, want resource=cronjobs.batch reason=ListForbidden", rec)
		}
	}
	if strings.Join(kinds, ",") != "read.unavailable,volume.multi_attach" {
		t.Errorf("kinds = %v, want the gap named and the multi-attach still reported, no unconsumed claim", kinds)
	}
}
