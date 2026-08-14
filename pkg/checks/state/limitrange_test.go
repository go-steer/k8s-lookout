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

// §13 conventions: fake.Clientset fixtures. The interesting surface
// here is not the List — it is the apiserver's LimitRangeItem
// defaulting, reproduced in the index so the answer is right for a
// LimitRange that sets only Max, or only Min, or only Default. Those
// three fallbacks are what make "does this namespace default cpu?"
// answerable without guessing.

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/state"
)

func rl(pairs map[corev1.ResourceName]string) corev1.ResourceList {
	if len(pairs) == 0 {
		return nil
	}
	out := corev1.ResourceList{}
	for k, v := range pairs {
		out[k] = resource.MustParse(v)
	}
	return out
}

func lr(ns, name string, items ...corev1.LimitRangeItem) *corev1.LimitRange {
	return &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.LimitRangeSpec{Limits: items},
	}
}

func load(t *testing.T, objs ...*corev1.LimitRange) (*state.LimitRangeDefaults, int) {
	t.Helper()
	// Seeded through Create rather than NewClientset(objs...) so the
	// fixture keeps the exact Default/DefaultRequest/Max/Min shape
	// under test: the fake runs no apiserver defaulting, which is
	// precisely what makes the derivation in the index observable.
	cs := fake.NewClientset()
	for _, o := range objs {
		if _, err := cs.CoreV1().LimitRanges(o.Namespace).Create(context.Background(), o, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed %s/%s: %v", o.Namespace, o.Name, err)
		}
	}
	d, n, err := state.LoadLimitRanges(context.Background(), cs, metav1.NamespaceAll)
	if err != nil {
		t.Fatalf("LoadLimitRanges: %v", err)
	}
	return d, n
}

// TestLimitRangeDefaultingFallbacks pins the apiserver's three
// LimitRangeItem fallbacks. Getting these wrong means annotating (or,
// for a template-scoped caller, suppressing) against the wrong
// dimension set.
func TestLimitRangeDefaultingFallbacks(t *testing.T) {
	cases := []struct {
		name        string
		item        corev1.LimitRangeItem
		wantLimit   []string // dimensions with a default LIMIT
		wantRequest []string // dimensions with a default REQUEST
	}{
		{
			name: "explicit default and defaultRequest",
			item: corev1.LimitRangeItem{
				Type:           corev1.LimitTypeContainer,
				Default:        rl(map[corev1.ResourceName]string{corev1.ResourceCPU: "1"}),
				DefaultRequest: rl(map[corev1.ResourceName]string{corev1.ResourceMemory: "128Mi"}),
			},
			wantLimit:   []string{"cpu"},
			wantRequest: []string{"cpu", "memory"}, // cpu via DefaultRequest←Default
		},
		{
			name: "max alone defaults the limit, and through it the request",
			item: corev1.LimitRangeItem{
				Type: corev1.LimitTypeContainer,
				Max:  rl(map[corev1.ResourceName]string{corev1.ResourceCPU: "2"}),
			},
			wantLimit:   []string{"cpu"},
			wantRequest: []string{"cpu"},
		},
		{
			name: "min alone defaults the request only",
			item: corev1.LimitRangeItem{
				Type: corev1.LimitTypeContainer,
				Min:  rl(map[corev1.ResourceName]string{corev1.ResourceMemory: "64Mi"}),
			},
			wantLimit:   nil,
			wantRequest: []string{"memory"},
		},
		{
			name: "pod-typed items default nothing",
			item: corev1.LimitRangeItem{
				Type:    corev1.LimitTypePod,
				Default: rl(map[corev1.ResourceName]string{corev1.ResourceCPU: "1"}),
				Max:     rl(map[corev1.ResourceName]string{corev1.ResourceMemory: "4Gi"}),
			},
			wantLimit:   nil,
			wantRequest: nil,
		},
		{
			name: "pvc-typed items default nothing",
			item: corev1.LimitRangeItem{
				Type: corev1.LimitTypePersistentVolumeClaim,
				Max:  rl(map[corev1.ResourceName]string{corev1.ResourceStorage: "10Gi"}),
			},
			wantLimit:   nil,
			wantRequest: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, n := load(t, lr("prod", "lr", tc.item))
			if n != 1 {
				t.Errorf("scanned = %d, want 1", n)
			}
			for _, dim := range []string{"cpu", "memory"} {
				_, gotLimit := d.DefaultsLimit("prod", dim)
				if want := contains(tc.wantLimit, dim); gotLimit != want {
					t.Errorf("DefaultsLimit(%s) = %v, want %v", dim, gotLimit, want)
				}
				_, gotRequest := d.DefaultsRequest("prod", dim)
				if want := contains(tc.wantRequest, dim); gotRequest != want {
					t.Errorf("DefaultsRequest(%s) = %v, want %v", dim, gotRequest, want)
				}
			}
		})
	}
}

// TestLimitRangeNamespaceScoped: an index built cluster-wide must not
// leak one namespace's defaults into another.
func TestLimitRangeNamespaceScoped(t *testing.T) {
	item := corev1.LimitRangeItem{
		Type:           corev1.LimitTypeContainer,
		DefaultRequest: rl(map[corev1.ResourceName]string{corev1.ResourceCPU: "100m"}),
	}
	d, n := load(t, lr("prod", "prod-lr", item))
	if n != 1 {
		t.Fatalf("scanned = %d, want 1", n)
	}
	if names, ok := d.DefaultsRequest("prod", "cpu"); !ok || names != "prod-lr" {
		t.Errorf("prod cpu = (%q, %v), want (prod-lr, true)", names, ok)
	}
	if _, ok := d.DefaultsRequest("dev", "cpu"); ok {
		t.Error("dev inherited prod's LimitRange")
	}
}

// TestLimitRangeMultipleNamesAllReported: the apiserver does not
// define which of two LimitRanges wins, so the annotation names all
// of them rather than picking one and being wrong half the time.
func TestLimitRangeMultipleNamesAllReported(t *testing.T) {
	item := corev1.LimitRangeItem{
		Type:           corev1.LimitTypeContainer,
		DefaultRequest: rl(map[corev1.ResourceName]string{corev1.ResourceCPU: "100m"}),
	}
	d, _ := load(t, lr("prod", "b-second", item), lr("prod", "a-first", item))
	names, ok := d.DefaultsRequest("prod", "cpu")
	if !ok || names != "a-first,b-second" {
		t.Errorf("names = %q (ok=%v), want sorted a-first,b-second", names, ok)
	}
}

// TestLimitRangeZeroValueSafe: a caller that could not load
// LimitRanges must degrade to unqualified findings, never to a panic
// or to silence.
func TestLimitRangeZeroValueSafe(t *testing.T) {
	var zero state.LimitRangeDefaults
	if _, ok := zero.DefaultsLimit("prod", "cpu"); ok {
		t.Error("zero value claims a default")
	}
	if _, ok := zero.DefaultsRequest("prod", "memory"); ok {
		t.Error("zero value claims a default")
	}
	var nilD *state.LimitRangeDefaults
	if _, ok := nilD.DefaultsRequest("prod", "cpu"); ok {
		t.Error("nil index claims a default")
	}
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}
