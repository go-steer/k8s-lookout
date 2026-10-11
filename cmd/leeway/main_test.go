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

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/sources"
)

func TestBuildSourcesForClients_SharesSingleTransformingFactory(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "prod"},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name: "app",
				Env:  []corev1.EnvVar{{Name: "DB_PASS", Value: "super-secret"}},
			}},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-a"},
		},
	}
	client := fake.NewSimpleClientset(pod, node)
	client.Resources = []*metav1.APIResourceList{{
		GroupVersion: "cloud.google.com/v1",
		APIResources: []metav1.APIResource{{
			Name:       "computeclasses",
			Kind:       "ComputeClass",
			Namespaced: false,
		}},
	}}

	var podWatches, nodeWatches atomic.Int64
	client.PrependWatchReactor("*", func(action ktesting.Action) (bool, watch.Interface, error) {
		switch action.GetResource().Resource {
		case "pods":
			podWatches.Add(1)
		case "nodes":
			nodeWatches.Add(1)
		}
		return false, nil, nil
	})

	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		{Group: "cloud.google.com", Version: "v1", Resource: "computeclasses"}: "ComputeClassList",
	})

	var stderr bytes.Buffer
	srcs, stopFactory, err := buildSourcesForClients(options{
		sources:      strings.Join(defaultSources, ","),
		topologyKeys: strings.Join(defaultTopologyKeys(), ","),
	}, client, dyn, noop.NewMeterProvider(), &stderr)
	if err != nil {
		t.Fatalf("buildSourcesForClients: %v", err)
	}
	defer stopFactory()

	if len(srcs) != 2 {
		t.Fatalf("built %d sources, want 2 (stderr: %s)", len(srcs), stderr.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = sources.RunAll(ctx, srcs, func(sources.Signal) {})

	if got := podWatches.Load(); got != 1 {
		t.Errorf("pod watch streams opened = %d, want 1 (both sources must share one transforming factory)", got)
	}
	if got := nodeWatches.Load(); got != 1 {
		t.Errorf("node watch streams opened = %d, want 1 (both sources must share one transforming factory)", got)
	}
}

func TestProbeSources_FailsLoudlyOnMissingRBAC(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		create := action.(ktesting.CreateAction)
		ssar := create.GetObject().(*authorizationv1.SelfSubjectAccessReview)
		attr := ssar.Spec.ResourceAttributes
		allowed := attr.Resource != "pods" || attr.Verb != "watch"
		ssar.Status = authorizationv1.SubjectAccessReviewStatus{
			Allowed: allowed,
			Reason:  "RBAC denied in test",
		}
		return true, ssar, nil
	})

	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClient(scheme)

	var stderr bytes.Buffer
	srcs, stopFactory, err := buildSourcesForClients(options{
		sources:      "topology-drift",
		topologyKeys: strings.Join(defaultTopologyKeys(), ","),
	}, client, dyn, noop.NewMeterProvider(), &stderr)
	if err != nil {
		t.Fatalf("buildSourcesForClients: %v", err)
	}
	defer stopFactory()

	err = probeSources(context.Background(), client, srcs, &stderr)
	if !errors.Is(err, sources.ErrAccessDenied) {
		t.Fatalf("probeSources error = %v, want errors.Is(_, sources.ErrAccessDenied)", err)
	}
}

func TestRealMain_FlagValidation(t *testing.T) {
	dir := t.TempDir()
	outFile, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()
	errFile, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer errFile.Close()

	if code := realMain([]string{"--version"}, outFile, errFile); code != 0 {
		t.Errorf("--version exit code = %d, want 0", code)
	}
	for _, args := range [][]string{
		{"--kube-qps=0"},
		{"--kube-burst=-1"},
		{"--access-recheck=-1s"},
	} {
		if code := realMain(args, outFile, errFile); code != 2 {
			t.Errorf("realMain(%v) exit code = %d, want 2", args, code)
		}
	}
}
