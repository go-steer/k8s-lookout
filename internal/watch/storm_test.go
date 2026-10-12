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

package watch

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// nonNamespaceResolver wraps an AncestorResolver to test workload, placement,
// and shared config/secret/PVC correlation in isolation from the catch-all
// Namespace fallback.
type nonNamespaceResolver struct {
	inner engine.AncestorResolver
}

func (n nonNamespaceResolver) Ancestors(ref engine.ObjectRef) []engine.Ancestor {
	var out []engine.Ancestor
	for _, a := range n.inner.Ancestors(ref) {
		if a.Kind == "Namespace" {
			continue
		}
		out = append(out, a)
	}
	return out
}

// podWithKubeAPIAccess constructs a realistic pod mounting the default
// service-account projected volume ("kube-api-access-*") containing
// ConfigMap "kube-root-ca.crt", plus any extra user volumes.
func podWithKubeAPIAccess(ns, name, node, rs string, extraVolumes ...corev1.Volume) *corev1.Pod {
	vols := []corev1.Volume{
		{
			Name: "kube-api-access-" + name,
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{
					Sources: []corev1.VolumeProjection{
						{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
						{ConfigMap: &corev1.ConfigMapProjection{
							LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
						}},
						{DownwardAPI: &corev1.DownwardAPIProjection{}},
					},
				},
			},
		},
	}
	vols = append(vols, extraVolumes...)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			UID:       types.UID("uid-" + name),
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Volumes:  vols,
		},
	}
	if rs != "" {
		pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: rs}}
	}
	return pod
}

// TestStorm_KubeRootCAAndAPIAccessDoNotCorrelateUnrelatedPods verifies that
// pods mounting Kubernetes' default "kube-api-access-*" projected volume and
// "kube-root-ca.crt" ConfigMap do not emit Mounts edges to kube-root-ca.crt
// and do not form a bogus storm across unrelated failing deployments (#589).
func TestStorm_KubeRootCAAndAPIAccessDoNotCorrelateUnrelatedPods(t *testing.T) {
	t.Parallel()
	const ns = "shop"
	apps := []string{"web", "api", "worker"}

	var objs []any
	for i, app := range apps {
		nodeName := fmt.Sprintf("node-%d", i+1)
		rsName := app + "-7b9d"
		podName := app + "-1"
		objs = append(objs,
			testNode(nodeName),
			testRS(ns, rsName, app),
			podWithKubeAPIAccess(ns, podName, nodeName, rsName),
		)
	}
	feed := buildFeedGraph(t, objs...)

	// 1. None of the pods carry ConfigMap kube-root-ca.crt as an ancestor.
	rootCA := engine.Ancestor{Kind: "ConfigMap", Namespace: ns, Name: "kube-root-ca.crt"}
	for _, app := range apps {
		cands := feed.Ancestors(engine.ObjectRef{Kind: "Pod", Namespace: ns, Name: app + "-1"})
		for _, c := range cands {
			if c == rootCA {
				t.Errorf("pod %s/%s unexpectedly carries auto-injected ancestor %+v", ns, app+"-1", c)
			}
		}
	}

	// 2. Over workload/placement/config ancestors, 3 unrelated failing deployments
	// share nothing and form zero storms.
	correlator, err := engine.NewStormCorrelator(time.Minute, 3, nonNamespaceResolver{feed})
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	for i, app := range apps {
		sig := stormPodSignal(i+1, ns)
		sig.Name = app + "-1"
		sig.Node = fmt.Sprintf("node-%d", i+1)
		if v := correlator.Observe(sig); v.Kind != engine.StormNone {
			t.Errorf("unrelated deployment %s verdict = %v (ancestor=%+v), want StormNone", app, v.Kind, v.Storm.Ancestor)
		}
	}

	// 3. Even over the full resolver (including Namespace), a storm must NEVER
	// form on ConfigMap kube-root-ca.crt.
	fullCorrelator, err := engine.NewStormCorrelator(time.Minute, 3, feed)
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	for i, app := range apps {
		sig := stormPodSignal(i+1, ns)
		sig.Name = app + "-1"
		sig.Node = fmt.Sprintf("node-%d", i+1)
		if v := fullCorrelator.Observe(sig); v.Kind == engine.StormFormed && v.Storm.Ancestor == rootCA {
			t.Errorf("bogus storm formed on %+v", v.Storm.Ancestor)
		}
	}
}

// TestStorm_SharedAppConfigSecretAndPVCCorrelateCleanly verifies that genuine
// application ConfigMaps ("app-config"), Secrets, and PersistentVolumeClaims
// mounted across multiple workloads DO emit Mounts edges and correlate into
// a storm on that shared dependency — even when all 3 workloads in the
// namespace mount it (#589).
func TestStorm_SharedAppConfigSecretAndPVCCorrelateCleanly(t *testing.T) {
	t.Parallel()
	const ns = "shop"
	apps := []string{"web", "api", "worker"}

	cases := []struct {
		name         string
		extraVol     corev1.Volume
		wantAncestor engine.Ancestor
	}{
		{
			name: "shared ConfigMap app-config",
			extraVol: corev1.Volume{
				Name: "cfg",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "app-config"},
					},
				},
			},
			wantAncestor: engine.Ancestor{Kind: "ConfigMap", Namespace: ns, Name: "app-config"},
		},
		{
			name: "shared Secret app-secret",
			extraVol: corev1.Volume{
				Name: "creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: "app-secret"},
				},
			},
			wantAncestor: engine.Ancestor{Kind: "Secret", Namespace: ns, Name: "app-secret"},
		},
		{
			name: "shared PVC app-pvc",
			extraVol: corev1.Volume{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "app-pvc"},
				},
			},
			wantAncestor: engine.Ancestor{Kind: "PersistentVolumeClaim", Namespace: ns, Name: "app-pvc"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var objs []any
			for i, app := range apps {
				nodeName := fmt.Sprintf("node-%d", i+1)
				rsName := app + "-7b9d"
				podName := app + "-1"
				objs = append(objs,
					testNode(nodeName),
					testRS(ns, rsName, app),
					podWithKubeAPIAccess(ns, podName, nodeName, rsName, tc.extraVol),
				)
			}
			feed := buildFeedGraph(t, objs...)
			correlator, err := engine.NewStormCorrelator(time.Minute, 3, feed)
			if err != nil {
				t.Fatalf("NewStormCorrelator: %v", err)
			}

			var finalVerdict engine.StormVerdict
			for i, app := range apps {
				sig := stormPodSignal(i+1, ns)
				sig.Name = app + "-1"
				sig.Node = fmt.Sprintf("node-%d", i+1)
				finalVerdict = correlator.Observe(sig)
			}
			if finalVerdict.Kind != engine.StormFormed {
				t.Fatalf("final verdict = %v, want StormFormed", finalVerdict.Kind)
			}
			if finalVerdict.Storm.Ancestor != tc.wantAncestor {
				t.Errorf("storm formed on %+v, want %+v", finalVerdict.Storm.Ancestor, tc.wantAncestor)
			}
		})
	}
}
