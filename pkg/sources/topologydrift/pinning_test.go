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

package topologydrift

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// pvcPod builds a pod mounting one PVC by name.
func pvcPod(name, claim string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: name},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{Name: "config", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{},
			}},
			{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			}},
		}},
	}
}

// affinityPV builds a PV whose Required node affinity is the given terms.
func affinityPV(terms ...corev1.NodeSelectorTerm) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1"}}
	if terms != nil {
		pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{
			Required: &corev1.NodeSelector{NodeSelectorTerms: terms},
		}
	}
	return pv
}

// oneVolume is a VolumeLookup that resolves a single named claim.
func oneVolume(claim string, pv *corev1.PersistentVolume) VolumeLookup {
	return func(_, name string) (*corev1.PersistentVolume, bool) {
		if name != claim {
			return nil, false
		}
		return pv, true
	}
}

func TestVolumePins(t *testing.T) {
	zone := string(zoneKey)

	cases := []struct {
		name string
		pod  *corev1.Pod
		pv   *corev1.PersistentVolume
		want bool
	}{{
		// The case that has to be free: nearly every pod in a cluster.
		name: "no claim at all",
		pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web-1"},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{
				{Name: "tmp", VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				}},
			}},
		},
		want: false,
	}, {
		name: "claim the cache does not know",
		pod:  pvcPod("db-0", "missing"),
		want: false,
	}, {
		name: "bound volume with no node affinity",
		pod:  pvcPod("db-0", "data-db-0"),
		pv:   affinityPV(),
		want: false,
	}, {
		name: "zonal disk",
		pod:  pvcPod("db-0", "data-db-0"),
		pv:   affinityPV(term(zone, corev1.NodeSelectorOpIn, "us-central1-a")),
		want: true,
	}, {
		// Regional PD: two zones out of three is still a volume its pod
		// cannot be rebalanced away from.
		name: "regional disk naming two zones",
		pod:  pvcPod("db-0", "data-db-0"),
		pv:   affinityPV(term(zone, corev1.NodeSelectorOpIn, "us-central1-a", "us-central1-b")),
		want: true,
	}, {
		// The hostname is a pinning key whether or not it is scored, because
		// one node is one domain on every axis there is.
		name: "local volume on one node",
		pod:  pvcPod("db-0", "data-db-0"),
		pv:   affinityPV(term(corev1.LabelHostname, corev1.NodeSelectorOpIn, "n1")),
		want: true,
	}, {
		name: "constrained on an axis nobody scores",
		pod:  pvcPod("db-0", "data-db-0"),
		pv:   affinityPV(term("kubernetes.io/os", corev1.NodeSelectorOpIn, "linux")),
		want: false,
	}, {
		// Exists names a pinning key and narrows nothing: every node in every
		// zone carries a zone label, which is what makes zone an axis.
		name: "exists on a pinning key",
		pod:  pvcPod("db-0", "data-db-0"),
		pv:   affinityPV(term(zone, corev1.NodeSelectorOpExists)),
		want: false,
	}, {
		name: "not-in on a pinning key",
		pod:  pvcPod("db-0", "data-db-0"),
		pv:   affinityPV(term(zone, corev1.NodeSelectorOpNotIn, "us-central1-c")),
		want: true,
	}, {
		// Terms are ORed, so one term without a pinning key is an escape
		// route and the volume pins nothing.
		name: "one pinning term ORed with a free one",
		pod:  pvcPod("db-0", "data-db-0"),
		pv: affinityPV(
			term(zone, corev1.NodeSelectorOpIn, "us-central1-a"),
			term("kubernetes.io/os", corev1.NodeSelectorOpIn, "linux"),
		),
		want: false,
	}, {
		name: "every term pins",
		pod:  pvcPod("db-0", "data-db-0"),
		pv: affinityPV(
			term(zone, corev1.NodeSelectorOpIn, "us-central1-a"),
			term(zone, corev1.NodeSelectorOpIn, "us-central1-b"),
		),
		want: true,
	}, {
		name: "match fields names one node",
		pod:  pvcPod("db-0", "data-db-0"),
		pv: affinityPV(corev1.NodeSelectorTerm{
			MatchFields: []corev1.NodeSelectorRequirement{
				{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"n1"}},
			},
		}),
		want: true,
	}, {
		// An empty Required matches no node, which is a broken volume rather
		// than a pinned one. Reported unpinned: the false-positive direction
		// is the visible one.
		name: "required with no terms",
		pod:  pvcPod("db-0", "data-db-0"),
		pv: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
			Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{},
			}},
		},
		want: false,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pins := VolumePins(oneVolume("data-db-0", tc.pv), DefaultTopologyKeys)
			if got := pins(tc.pod); got != tc.want {
				t.Errorf("pinned = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestVolumePins_GenericEphemeralVolume(t *testing.T) {
	// The kubelet's PVC for a generic ephemeral volume is named <pod>-<volume>
	// by the API, not by convention, so the predicate can resolve it without
	// reading ownerReferences off a claim it has not found yet.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "scratch-7"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{Name: "work", VolumeSource: corev1.VolumeSource{
				Ephemeral: &corev1.EphemeralVolumeSource{},
			}},
		}},
	}
	pv := affinityPV(term(string(zoneKey), corev1.NodeSelectorOpIn, "us-central1-a"))

	pins := VolumePins(oneVolume("scratch-7-work", pv), DefaultTopologyKeys)
	if !pins(pod) {
		t.Error("a generic ephemeral volume on a zonal class did not pin")
	}
}

func TestVolumePins_NilLookupIsNoPredicate(t *testing.T) {
	// Nil rather than a predicate that always says false: StateOptions already
	// documents nil as "never pinned", and two ways to say the same thing is
	// one way too many.
	if VolumePins(nil, DefaultTopologyKeys) != nil {
		t.Error("VolumePins(nil, ...) returned a predicate")
	}
}

func TestVolumePins_ConfiguredKeysDecideTheQuestion(t *testing.T) {
	// The same volume, asked about by two clusters that score different axes.
	pod := pvcPod("db-0", "data-db-0")
	pv := affinityPV(term("topology.gke.io/rack", corev1.NodeSelectorOpIn, "r1"))

	if VolumePins(oneVolume("data-db-0", pv), DefaultTopologyKeys)(pod) {
		t.Error("an unscored axis pinned")
	}
	scored := []leeway.TopologyKey{"topology.gke.io/rack"}
	if !VolumePins(oneVolume("data-db-0", pv), scored)(pod) {
		t.Error("a scored axis did not pin")
	}
}

// withClaim mounts a PVC on a pod built by the shared pod() helper.
func withClaim(claim string) podOpt {
	return func(p *corev1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			},
		})
	}
}

func TestSource_PinsAPodOnAZonalVolume(t *testing.T) {
	// The wiring end to end: the pod says which claim, the claim says which
	// volume, the volume says which zone, and the counter says the pod is not
	// free to move. Two pods, one of them pinned, so the assertion cannot pass
	// by everything being pinned or nothing being.
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "data-web-1"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-a"},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-a"},
		Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{
			Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
				term(string(zoneKey), corev1.NodeSelectorOpIn, "zone-a"),
			}},
		}},
	}

	s, _ := runSource(t, evalConfig(zoneKey),
		node("n1", "zone-a"), node("n2", "zone-b"),
		replicaSet("web-7c9f", "prod", "web"),
		webPod("web-1", "n1", withClaim("data-web-1")),
		webPod("web-2", "n2"),
		claim, pv,
	)

	waitFor(t, "both pods to be counted", func() bool {
		snap := s.State().Snapshot(webSubject)
		return snap[zoneKey] != nil && snap[zoneKey].Total == 2
	})

	dist := s.State().Snapshot(webSubject)[zoneKey]
	if got := dist.ByDomain[leeway.Domain("zone-a")]; got == nil || got.Pinned != 1 {
		t.Errorf("zone-a = %+v, want one pinned pod", got)
	}
	if got := dist.ByDomain[leeway.Domain("zone-b")]; got == nil || got.Pinned != 0 {
		t.Errorf("zone-b = %+v, want no pinned pods — nothing binds web-2", got)
	}
}

func TestSource_BoundVolumeBeforeRun(t *testing.T) {
	// The predicate is built in New, so it is live before Run has listers. It
	// has to answer rather than panic, and the answer has to be "not pinned" —
	// reapply re-counts every pod once the caches are up.
	s := New(nil, Config{})
	if _, ok := s.boundVolume("shop", "data-db-0"); ok {
		t.Error("boundVolume resolved a claim before Run")
	}
	if s.state.pinned == nil {
		t.Fatal("New left the pin predicate unset")
	}
	if s.state.pinned(pvcPod("db-0", "data-db-0")) {
		t.Error("a pod was pinned before the listers existed")
	}
}
