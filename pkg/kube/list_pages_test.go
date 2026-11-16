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

package kube

import (
	"context"
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestListPagesPaginatesAcrossContinueTokens(t *testing.T) {
	var seenLimits []int64
	var seenContinues []string

	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		seenLimits = append(seenLimits, opts.Limit)
		seenContinues = append(seenContinues, opts.Continue)
		switch opts.Continue {
		case "":
			return []string{"pod-a", "pod-b"}, "tok-1", nil
		case "tok-1":
			return []string{"pod-c"}, "tok-2", nil
		case "tok-2":
			return []string{"pod-d", "pod-e"}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	var got []string
	err := ListPages(context.Background(), listFn, func(item *string) {
		got = append(got, *item)
	})
	if err != nil {
		t.Fatalf("ListPages returned error: %v", err)
	}
	want := []string{"pod-a", "pod-b", "pod-c", "pod-d", "pod-e"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, lim := range seenLimits {
		if lim != DefaultPageLimit {
			t.Errorf("page %d Limit = %d, want %d", i, lim, DefaultPageLimit)
		}
	}
	wantContinues := []string{"", "tok-1", "tok-2"}
	if !reflect.DeepEqual(seenContinues, wantContinues) {
		t.Errorf("seen continues = %v, want %v", seenContinues, wantContinues)
	}
}

func TestListPagesRecoversTransparentlyFromGone(t *testing.T) {
	goneInjected := false
	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		switch opts.Continue {
		case "":
			if !goneInjected {
				return []string{"stale-a", "stale-b"}, "expired-token", nil
			}
			return []string{"fresh-a", "fresh-b"}, "fresh-tok", nil
		case "expired-token":
			goneInjected = true
			return nil, "", apierrors.NewResourceExpired("continue token expired")
		case "fresh-tok":
			return []string{"fresh-c"}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	var got []string
	err := ListPages(context.Background(), listFn, func(item *string) {
		got = append(got, *item)
	})
	if err != nil {
		t.Fatalf("expected transparent 410 Gone recovery, got error: %v", err)
	}
	want := []string{"fresh-a", "fresh-b", "fresh-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v (must not duplicate stale partial items)", got, want)
	}
}

func TestListPagesFailsOnRepeatedGone(t *testing.T) {
	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		if opts.Continue == "" {
			return []string{"a"}, "tok", nil
		}
		return nil, "", apierrors.NewResourceExpired("still gone")
	}

	err := ListPages(context.Background(), listFn, func(*string) {
		t.Fatal("each must not be called when pagination ultimately errors")
	})
	if !apierrors.IsResourceExpired(err) && !apierrors.IsGone(err) {
		t.Fatalf("expected 410 Gone error after retry exhaustion, got %v", err)
	}
}

func TestWrapClientDeduplicatesPodAndNodeLists(t *testing.T) {
	raw := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-1"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "staging", Name: "web-1"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	)
	var podCalls, nodeCalls int
	raw.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		podCalls++
		return false, nil, nil
	})
	raw.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		nodeCalls++
		return false, nil, nil
	})

	ctx := WithListCache(context.Background())
	wrapped := WrapClient(ctx, raw)

	pods1, err := wrapped.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods1.Items) != 2 {
		t.Fatalf("first cluster pod list: items=%d err=%v", len(pods1.Items), err)
	}
	pods2, err := wrapped.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods2.Items) != 2 {
		t.Fatalf("second cluster pod list: items=%d err=%v", len(pods2.Items), err)
	}
	nsPods, err := wrapped.CoreV1().Pods("prod").List(ctx, metav1.ListOptions{})
	if err != nil || len(nsPods.Items) != 1 || nsPods.Items[0].Name != "api-1" {
		t.Fatalf("namespace pod slice from cluster cache: items=%v err=%v", nsPods.Items, err)
	}
	if podCalls != 1 {
		t.Errorf("underlying pods list calls = %d, want 1", podCalls)
	}

	if _, err := wrapped.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("first node list: %v", err)
	}
	if _, err := wrapped.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("second node list: %v", err)
	}
	if nodeCalls != 1 {
		t.Errorf("underlying nodes list calls = %d, want 1", nodeCalls)
	}

	// Non-Gone error is never cached.
	rawErr := fake.NewSimpleClientset()
	rawErr.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("boom")
	})
	wrappedErr := WrapClient(ctx, rawErr)
	if _, err := wrappedErr.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("expected service unavailable error, got %v", err)
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) && err != nil {
		_ = schema.GroupResource{}
	}
}

func TestListAllPagesTrimTrimsPerPageAndRecoversFromGone(t *testing.T) {
	type item struct {
		name  string
		heavy string
	}
	var page1 []item
	trimmedBeforePage2 := false
	goneInjected := false

	listFn := func(_ context.Context, opts metav1.ListOptions) ([]item, string, error) {
		switch opts.Continue {
		case "":
			if !goneInjected {
				page1 = []item{{name: "stale", heavy: "big"}}
				return page1, "expired", nil
			}
			page1 = []item{{name: "a", heavy: "big-a"}, {name: "b", heavy: "big-b"}}
			return page1, "tok-2", nil
		case "expired":
			// Page 1 must have been trimmed before requesting page 2.
			if page1[0].heavy == "" {
				trimmedBeforePage2 = true
			}
			goneInjected = true
			return nil, "", apierrors.NewResourceExpired("gone")
		case "tok-2":
			if page1[0].heavy != "" || page1[1].heavy != "" {
				t.Errorf("page 1 was not trimmed before fetching page 2: %+v", page1)
			}
			return []item{{name: "c", heavy: "big-c"}}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	got, err := ListAllPagesTrim(context.Background(), listFn, func(it *item) {
		it.heavy = ""
	})
	if err != nil {
		t.Fatalf("ListAllPagesTrim: %v", err)
	}
	if !trimmedBeforePage2 {
		t.Error("expected page 1 to be trimmed before requesting the next page")
	}
	want := []item{{name: "a"}, {name: "b"}, {name: "c"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestListAllPagesMapProjectsFiltersAndRecoversFromGone(t *testing.T) {
	goneInjected := false
	listFn := func(_ context.Context, opts metav1.ListOptions) ([]int, string, error) {
		switch opts.Continue {
		case "":
			if !goneInjected {
				return []int{99, 100}, "expired", nil
			}
			return []int{1, 2, 3}, "tok-2", nil
		case "expired":
			goneInjected = true
			return nil, "", apierrors.NewResourceExpired("gone")
		case "tok-2":
			return []int{4, 5}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	got, err := ListAllPagesMap(context.Background(), listFn, func(v *int) (int, bool) {
		if *v%2 != 0 {
			return 0, false
		}
		return *v * 10, true
	})
	if err != nil {
		t.Fatalf("ListAllPagesMap: %v", err)
	}
	want := []int{20, 40}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestListPagesStreamStreamsPerPageAndResetsOnGone(t *testing.T) {
	goneInjected := false
	var got []string
	resets := 0
	seenOnPage2Request := 0

	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		switch opts.Continue {
		case "":
			if !goneInjected {
				return []string{"stale-1"}, "expired", nil
			}
			return []string{"fresh-1", "fresh-2"}, "tok-2", nil
		case "expired":
			goneInjected = true
			return nil, "", apierrors.NewResourceExpired("gone")
		case "tok-2":
			// Page 1 must have been streamed before page 2 is requested.
			seenOnPage2Request = len(got)
			return []string{"fresh-3"}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	err := ListPagesStream(context.Background(), listFn, func() {
		resets++
		got = nil
	}, func(s *string) {
		got = append(got, *s)
	})
	if err != nil {
		t.Fatalf("ListPagesStream: %v", err)
	}
	if resets != 1 {
		t.Errorf("resets = %d, want 1", resets)
	}
	if seenOnPage2Request != 2 {
		t.Errorf("items streamed before page 2 request = %d, want 2", seenOnPage2Request)
	}
	want := []string{"fresh-1", "fresh-2", "fresh-3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWrapClientTrimsReadPathMetadataWhilePreservingUsedFields(t *testing.T) {
	raw := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod",
				Name:      "api-1",
				Annotations: map[string]string{
					"kubectl.kubernetes.io/last-applied-configuration": `{"huge":"manifest"}`,
					"cluster-autoscaler.kubernetes.io/safe-to-evict":   "false",
				},
				ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kube-controller-manager"}},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app",
					Env:  []corev1.EnvVar{{Name: "MODE", Value: "prod"}},
				}},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
				Annotations: map[string]string{
					"kubectl.kubernetes.io/last-applied-configuration":     `{"huge":"node"}`,
					"cluster-autoscaler.kubernetes.io/scale-down-disabled": "true",
				},
				ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "node-controller"}},
			},
			Status: corev1.NodeStatus{
				Images:          []corev1.ContainerImage{{Names: []string{"gcr.io/foo/bar:v1"}, SizeBytes: 12345}},
				VolumesInUse:    []corev1.UniqueVolumeName{"kubernetes.io/csi/vol-1"},
				VolumesAttached: []corev1.AttachedVolume{{Name: "kubernetes.io/csi/vol-1"}},
			},
		},
	)

	ctx := WithListCache(context.Background())
	wrapped := WrapClient(ctx, raw)

	pods, err := wrapped.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("pods list: items=%d err=%v", len(pods.Items), err)
	}
	pod := pods.Items[0]
	if len(pod.ManagedFields) != 0 {
		t.Errorf("Pod.ManagedFields not trimmed: %+v", pod.ManagedFields)
	}
	if _, ok := pod.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Error("Pod last-applied-configuration annotation not trimmed")
	}
	if pod.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] != "false" {
		t.Errorf("Pod operational annotation lost: %+v", pod.Annotations)
	}
	if len(pod.Spec.Containers) != 1 || len(pod.Spec.Containers[0].Env) != 1 || pod.Spec.Containers[0].Env[0].Value != "prod" {
		t.Errorf("Pod spec/env must be preserved on read path: %+v", pod.Spec.Containers)
	}

	nodes, err := wrapped.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil || len(nodes.Items) != 1 {
		t.Fatalf("nodes list: items=%d err=%v", len(nodes.Items), err)
	}
	node := nodes.Items[0]
	if len(node.Status.Images) != 0 || len(node.Status.VolumesInUse) != 0 || len(node.Status.VolumesAttached) != 0 {
		t.Errorf("Node status blobs not trimmed: images=%d inUse=%d attached=%d",
			len(node.Status.Images), len(node.Status.VolumesInUse), len(node.Status.VolumesAttached))
	}
	if _, ok := node.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Error("Node last-applied-configuration annotation not trimmed")
	}
	if node.Annotations["cluster-autoscaler.kubernetes.io/scale-down-disabled"] != "true" {
		t.Errorf("Node operational annotation lost: %+v", node.Annotations)
	}
	// Node.ManagedFields MUST be preserved for pkg/checks/delta/squeeze.go:taintWriter.
	if len(node.ManagedFields) != 1 || node.ManagedFields[0].Manager != "node-controller" {
		t.Errorf("Node.ManagedFields must be preserved for delta/squeeze.go, got %+v", node.ManagedFields)
	}
}
