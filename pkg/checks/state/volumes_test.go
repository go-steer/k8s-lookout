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

// `state volumes` tests, §13 conventions: fake.Clientset fixtures,
// exact findings per conflict class, a healthy fixture proving zero
// nominal state, a golden mixed cluster, and the checktest contract
// round-trip. All helpers here are vol-prefixed; fixedNow (shared
// with the edges tests) anchors the VolumeAttachment error-age math.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func volCommand(objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return state.VolumesCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
}

// volPod is a pod scheduled on node (unscheduled when node is "")
// mounting the named claims.
func volPod(ns, name, node string, claims ...string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
	}
	for _, c := range claims {
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
			Name: "vol-" + c,
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: c,
			}},
		})
	}
	return p
}

func volPVC(ns, name, volumeName string, modes ...corev1.PersistentVolumeAccessMode) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  volumeName,
			AccessModes: modes,
		},
	}
}

// volPV is a PV constrained to zones via one required
// nodeSelectorTerm (no node affinity at all when zones is empty).
func volPV(name string, zones ...string) *corev1.PersistentVolume {
	if len(zones) == 0 {
		return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	return volPVZoneTerms(name, zones)
}

// volPVZoneTerms is a PV whose required node affinity carries one
// term per zone list (terms are ORed).
func volPVZoneTerms(name string, terms ...[]string) *corev1.PersistentVolume {
	var sel []corev1.NodeSelectorTerm
	for _, zones := range terms {
		sel = append(sel, corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      "topology.kubernetes.io/zone",
				Operator: corev1.NodeSelectorOpIn,
				Values:   zones,
			}},
		})
	}
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{NodeSelectorTerms: sel},
			},
		},
	}
}

func volNode(name, zone string) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if zone != "" {
		n.Labels = map[string]string{"topology.kubernetes.io/zone": zone}
	}
	return n
}

func volVA(name, pv, node string, attached bool) *storagev1.VolumeAttachment {
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "pd.csi.storage.gke.io",
			NodeName: node,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: attached},
	}
}

// volErr is a VolumeError stamped age before fixedNow.
func volErr(msg string, age time.Duration) *storagev1.VolumeError {
	return &storagev1.VolumeError{Message: msg, Time: metav1.NewTime(fixedNow.Add(-age))}
}

// volFindings runs `state volumes` over objs and returns the finding
// lines (summary stripped), failing on non-zero exit.
func volFindings(t *testing.T, objs []runtime.Object, args ...string) []string {
	t.Helper()
	res := checktest.Run(t, volCommand(objs...), args...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
		t.Fatalf("no summary line: %q", res.Stdout)
	}
	return lines[:len(lines)-1]
}

func TestVolumesMultiAttach(t *testing.T) {
	objs := []runtime.Object{
		volPod(ns, "web-0", "node-a", "data"),
		volPod(ns, "web-1", "node-b", "data"),
		volPVC(ns, "data", "pv-data", corev1.ReadWriteOnce),
		volPV("pv-data"),
		volNode("node-a", "us-east1-b"),
		volNode("node-b", "us-east1-b"),
	}
	got := volFindings(t, objs)
	wantFindings(t, got, []string{
		`kind=volume.multi_attach severity=critical namespace=prod kind_of_object=PersistentVolumeClaim name=data reason=RWOMultiAttach message="RWO claim is wanted on 2 nodes — an RWO volume can attach to only one node; pods on the other node(s) stay stuck in ContainerCreating" pods=web-0,web-1 nodes=node-a,node-b access_modes=ReadWriteOnce`,
	})
}

func TestVolumesMultiAttachSilentCases(t *testing.T) {
	base := func(modes ...corev1.PersistentVolumeAccessMode) []runtime.Object {
		return []runtime.Object{
			volPVC(ns, "data", "pv-data", modes...),
			volPV("pv-data"),
			volNode("node-a", "us-east1-b"),
			volNode("node-b", "us-east1-b"),
		}
	}
	tests := []struct {
		name string
		objs []runtime.Object
	}{
		{"same node", append(base(corev1.ReadWriteOnce),
			volPod(ns, "web-0", "node-a", "data"),
			volPod(ns, "web-1", "node-a", "data"))},
		{"rwx claim", append(base(corev1.ReadWriteMany),
			volPod(ns, "web-0", "node-a", "data"),
			volPod(ns, "web-1", "node-b", "data"))},
		{"unscheduled second pod", append(base(corev1.ReadWriteOnce),
			volPod(ns, "web-0", "node-a", "data"),
			volPod(ns, "web-1", "", "data"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantFindings(t, volFindings(t, tt.objs), nil)
		})
	}
}

// RWOP is stricter than RWO (single pod, hence single node), so it
// conflicts the same way.
func TestVolumesMultiAttachRWOP(t *testing.T) {
	objs := []runtime.Object{
		volPod(ns, "web-0", "node-a", "data"),
		volPod(ns, "web-1", "node-b", "data"),
		volPVC(ns, "data", "pv-data", corev1.ReadWriteOncePod),
		volPV("pv-data"),
		volNode("node-a", "us-east1-b"),
		volNode("node-b", "us-east1-b"),
	}
	got := volFindings(t, objs)
	if len(got) != 1 || !strings.Contains(got[0], "access_modes=ReadWriteOncePod") {
		t.Errorf("want one multi_attach with access_modes=ReadWriteOncePod, got:\n%s", strings.Join(got, "\n"))
	}
}

func TestVolumesAttachError(t *testing.T) {
	fresh := volVA("va-fresh", "pv-data", "node-a", false)
	fresh.Status.AttachError = volErr("rpc error: attach failed", 2*time.Minute)
	old := volVA("va-old", "pv-data", "node-b", false)
	old.Status.AttachError = volErr("rpc error: attach failed", 30*time.Minute)
	detach := volVA("va-stuck-detach", "pv-data", "node-a", true)
	detach.Status.DetachError = volErr("rpc error: detach failed", 2*time.Minute)
	timeless := volVA("va-timeless", "pv-data", "node-b", false)
	timeless.Status.AttachError = &storagev1.VolumeError{Message: "attach failed"}

	objs := []runtime.Object{
		fresh, old, detach, timeless,
		volPV("pv-data"),
		volNode("node-a", "us-east1-b"),
		volNode("node-b", "us-east1-b"),
	}
	got := volFindings(t, objs)
	wantFindings(t, got, []string{
		`kind=volume.attach_error severity=warning kind_of_object=VolumeAttachment name=va-fresh reason=AttachError message="volume attach has been failing for 2m0s" pv=pv-data node=node-a attacher=pd.csi.storage.gke.io age=2m0s error="rpc error: attach failed" attached=false`,
		`kind=volume.attach_error severity=critical kind_of_object=VolumeAttachment name=va-old reason=AttachError message="volume attach has been failing for 30m0s" pv=pv-data node=node-b attacher=pd.csi.storage.gke.io age=30m0s error="rpc error: attach failed" attached=false`,
		`kind=volume.attach_error severity=warning kind_of_object=VolumeAttachment name=va-stuck-detach reason=DetachError message="volume detach has been failing for 2m0s" pv=pv-data node=node-a attacher=pd.csi.storage.gke.io age=2m0s error="rpc error: detach failed" attached=true`,
		`kind=volume.attach_error severity=warning kind_of_object=VolumeAttachment name=va-timeless reason=AttachError message="volume attach is failing" pv=pv-data node=node-b attacher=pd.csi.storage.gke.io error="attach failed" attached=false`,
	})
}

func TestVolumesZoneConflict(t *testing.T) {
	objs := []runtime.Object{
		volPod(ns, "db-0", "node-c", "zonal"),
		volPVC(ns, "zonal", "pv-zonal", corev1.ReadWriteOnce),
		volPV("pv-zonal", "us-east1-b"),
		volNode("node-c", "us-east1-c"),
	}
	got := volFindings(t, objs)
	wantFindings(t, got, []string{
		`kind=volume.zone_conflict severity=critical namespace=prod kind_of_object=Pod name=db-0 reason=ZoneConflict message="volume is locked to zone(s) us-east1-b; the pod landed in us-east1-c and can never mount it" pvc=zonal pv=pv-zonal pv_zones=us-east1-b node=node-c node_zone=us-east1-c`,
	})
}

func TestVolumesZoneConflictSilentCases(t *testing.T) {
	pod := func() []runtime.Object {
		return []runtime.Object{
			volPod(ns, "db-0", "node-c", "zonal"),
			volPVC(ns, "zonal", "pv-zonal", corev1.ReadWriteOnce),
			volNode("node-c", "us-east1-c"),
		}
	}
	tests := []struct {
		name string
		objs []runtime.Object
	}{
		{"matching zone", append(pod(), volPV("pv-zonal", "us-east1-c"))},
		{"no node affinity", append(pod(), volPV("pv-zonal"))},
		{"zone in second ORed term", append(pod(),
			volPVZoneTerms("pv-zonal", []string{"us-east1-a"}, []string{"us-east1-c"}))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantFindings(t, volFindings(t, tt.objs), nil)
		})
	}
}

func TestVolumesOrphanedAttachment(t *testing.T) {
	objs := []runtime.Object{
		volVA("va-no-pv", "pv-ghost", "node-a", true),
		volVA("va-no-node", "pv-data", "node-ghost", true),
		volVA("va-both", "pv-ghost", "node-ghost", false),
		volPV("pv-data"),
		volNode("node-a", "us-east1-b"),
	}
	got := volFindings(t, objs)
	wantFindings(t, got, []string{
		`kind=volume.orphaned_attachment severity=info kind_of_object=VolumeAttachment name=va-both reason=OrphanedAttachment message="attachment references a deleted PersistentVolume and node — the external-attacher should clean it up; stale entries can block reattachment" pv=pv-ghost node=node-ghost orphan="pv missing; node missing"`,
		`kind=volume.orphaned_attachment severity=info kind_of_object=VolumeAttachment name=va-no-node reason=OrphanedAttachment message="attachment references a deleted node — the external-attacher should clean it up; stale entries can block reattachment" pv=pv-data node=node-ghost orphan="node missing"`,
		`kind=volume.orphaned_attachment severity=info kind_of_object=VolumeAttachment name=va-no-pv reason=OrphanedAttachment message="attachment references a deleted PersistentVolume — the external-attacher should clean it up; stale entries can block reattachment" pv=pv-ghost node=node-a orphan="pv missing"`,
	})
}

// volHealthy is a fully consistent fixture: one RWO claim used on
// one node, one zonal claim whose pod sits in the PV's zone, and a
// healthy attachment. 9 objects, zero findings.
func volHealthy() []runtime.Object {
	return []runtime.Object{
		volPod(ns, "web-0", "node-a", "data"),
		volPod(ns, "db-0", "node-c", "zonal"),
		volPVC(ns, "data", "pv-data", corev1.ReadWriteOnce),
		volPVC(ns, "zonal", "pv-zonal", corev1.ReadWriteOnce),
		volPV("pv-data"),
		volPV("pv-zonal", "us-east1-c"),
		volVA("va-ok", "pv-data", "node-a", true),
		volNode("node-a", "us-east1-b"),
		volNode("node-c", "us-east1-c"),
	}
}

func TestVolumesHealthyIsSilent(t *testing.T) {
	res := checktest.Run(t, volCommand(volHealthy()...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "scanned=9 findings=0 elapsed=100ms\n"
	if res.Stdout != want {
		t.Errorf("healthy cluster must emit only the summary:\ngot:  %qwant: %q", res.Stdout, want)
	}
}

func TestVolumesNamespaceScoping(t *testing.T) {
	objs := []runtime.Object{
		// The multi-attach conflict lives entirely in namespace other.
		volPod("other", "web-0", "node-a", "data"),
		volPod("other", "web-1", "node-b", "data"),
		volPVC("other", "data", "pv-data", corev1.ReadWriteOnce),
		// prod is healthy.
		volPod(ns, "api-0", "node-a", "cache"),
		volPVC(ns, "cache", "pv-cache", corev1.ReadWriteOnce),
		volPV("pv-data"),
		volPV("pv-cache"),
		volNode("node-a", "us-east1-b"),
		volNode("node-b", "us-east1-b"),
	}
	if got := volFindings(t, objs); len(got) != 1 {
		t.Fatalf("unscoped run should see the conflict in namespace other, got %d findings", len(got))
	}
	wantFindings(t, volFindings(t, objs, "--namespace="+ns), nil)
}

func TestVolumesWorkloadIsUsageError(t *testing.T) {
	res := checktest.Run(t, volCommand(volHealthy()...), "--workload=Deployment/prod/api")
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

// volMixed hits every finding class at once: a multi-attach on the
// shared claim, a zone conflict on the zonal claim, one fresh and one
// aged attach error, a failing detach, and a fully orphaned
// attachment. The golden pins full-output ordering and formatting.
func volMixed() []runtime.Object {
	fresh := volVA("va-fresh", "pv-shared", "node-a", false)
	fresh.Status.AttachError = volErr("rpc error: attach failed", 2*time.Minute)
	stuck := volVA("va-stuck", "pv-zonal", "node-c", false)
	stuck.Status.AttachError = volErr("rpc error: attach failed", 30*time.Minute)
	detach := volVA("va-detach", "pv-shared", "node-b", true)
	detach.Status.DetachError = volErr("rpc error: detach failed", 12*time.Minute)
	return []runtime.Object{
		volPod(ns, "web-0", "node-a", "shared"),
		volPod(ns, "web-1", "node-b", "shared"),
		volPod(ns, "db-0", "node-c", "zonal"),
		volPVC(ns, "shared", "pv-shared", corev1.ReadWriteOnce),
		volPVC(ns, "zonal", "pv-zonal", corev1.ReadWriteOnce),
		volPV("pv-shared"),
		volPV("pv-zonal", "us-east1-b"),
		fresh, stuck, detach,
		volVA("va-ghost", "pv-ghost", "node-ghost", false),
		volNode("node-a", "us-east1-b"),
		volNode("node-b", "us-east1-b"),
		volNode("node-c", "us-east1-c"),
	}
}

func TestVolumesMixedGolden(t *testing.T) {
	res := checktest.Run(t, volCommand(volMixed()...))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/volumes-mixed.golden", res.Stdout)
}

func TestVolumesContract(t *testing.T) {
	checktest.VerifyContract(t, volCommand(volMixed()...))
}

func TestVolumesRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("state volumes")
	if !ok {
		t.Fatal("state volumes is not registered in the default registry")
	}
	if c.MCPName != "k8s_volume_conflicts" {
		t.Errorf("MCP tool name = %q, want k8s_volume_conflicts", c.MCPName)
	}
	if !strings.Contains(c.Help(), "--namespace") {
		t.Error("generated help does not document --namespace scoping")
	}
}
