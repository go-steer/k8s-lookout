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
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// DefaultPageLimit is the chunk size requested on paginated API list calls so
// large clusters stream in bounded pages instead of one unchunked response.
const DefaultPageLimit int64 = 500

// ListPages fetches all pages from list using Limit/Continue pagination and
// delivers each item in order to each. If the API server expires the continue
// token mid-stream (HTTP 410 Gone), ListPages discards partial results and
// restarts once from Continue="" so callers never observe duplicate or torn
// object sets.
func ListPages[T any](
	ctx context.Context,
	list func(context.Context, metav1.ListOptions) ([]T, string, error),
	each func(*T),
) error {
	items, err := ListAllPages(ctx, list)
	if err != nil {
		return err
	}
	for i := range items {
		each(&items[i])
	}
	return nil
}

// ListAllPages fetches all items across Continue tokens and returns the complete
// slice. A single HTTP 410 Gone continuation error triggers one clean restart
// from page 1.
func ListAllPages[T any](
	ctx context.Context,
	list func(context.Context, metav1.ListOptions) ([]T, string, error),
) ([]T, error) {
	opts := metav1.ListOptions{Limit: DefaultPageLimit}
	var all []T
	restarted := false

	for {
		page, next, err := list(ctx, opts)
		if err != nil {
			if (apierrors.IsResourceExpired(err) || apierrors.IsGone(err)) && !restarted {
				restarted = true
				all = nil
				opts = metav1.ListOptions{Limit: DefaultPageLimit}
				continue
			}
			return nil, err
		}
		all = append(all, page...)
		if next == "" {
			return all, nil
		}
		opts.Continue = next
	}
}

type listCacheKey struct{}

type podCacheKey struct {
	client    kubernetes.Interface
	namespace string
}

type listCache struct {
	mu    sync.Mutex
	pods  map[podCacheKey][]corev1.Pod
	nodes map[kubernetes.Interface][]corev1.Node
}

// WithListCache attaches a short-lived in-memory list cache to ctx so multiple
// sub-checks within a single command execution (such as `lookout scan` or
// `lookout health`) can share cluster-wide or namespace Pod/Node lists without
// re-listing from the API server.
func WithListCache(ctx context.Context) context.Context {
	if ctx.Value(listCacheKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, listCacheKey{}, &listCache{
		pods:  make(map[podCacheKey][]corev1.Pod),
		nodes: make(map[kubernetes.Interface][]corev1.Node),
	})
}

// WrapClient wraps client so unscoped Pods().List and Nodes().List calls use
// paginated reads backed by any ListCache attached to ctx via WithListCache.
// When ctx carries no ListCache, client is returned unchanged.
func WrapClient(ctx context.Context, client kubernetes.Interface) kubernetes.Interface {
	if client == nil {
		return nil
	}
	cache, ok := ctx.Value(listCacheKey{}).(*listCache)
	if !ok || cache == nil {
		return client
	}
	if wc, already := client.(*cachedClient); already && wc.cache == cache {
		return client
	}
	if wc, already := client.(*cachedClient); already {
		client = wc.Interface
	}
	return &cachedClient{Interface: client, cache: cache}
}

type cachedClient struct {
	kubernetes.Interface
	cache *listCache
}

func (c *cachedClient) CoreV1() corev1client.CoreV1Interface {
	return &cachedCoreV1{
		CoreV1Interface: c.Interface.CoreV1(),
		rawClient:       c.Interface,
		cache:           c.cache,
	}
}

type cachedCoreV1 struct {
	corev1client.CoreV1Interface
	rawClient kubernetes.Interface
	cache     *listCache
}

func (c *cachedCoreV1) Pods(namespace string) corev1client.PodInterface {
	return &cachedPods{
		PodInterface: c.CoreV1Interface.Pods(namespace),
		rawClient:    c.rawClient,
		namespace:    namespace,
		cache:        c.cache,
	}
}

func (c *cachedCoreV1) Nodes() corev1client.NodeInterface {
	return &cachedNodes{
		NodeInterface: c.CoreV1Interface.Nodes(),
		rawClient:     c.rawClient,
		cache:         c.cache,
	}
}

type cachedPods struct {
	corev1client.PodInterface
	rawClient kubernetes.Interface
	namespace string
	cache     *listCache
}

func (p *cachedPods) List(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
	if opts.LabelSelector != "" || opts.FieldSelector != "" || opts.ResourceVersion != "" || opts.Continue != "" {
		return p.PodInterface.List(ctx, opts)
	}
	p.cache.mu.Lock()
	exactKey := podCacheKey{client: p.rawClient, namespace: p.namespace}
	if cached, ok := p.cache.pods[exactKey]; ok {
		items := make([]corev1.Pod, len(cached))
		copy(items, cached)
		p.cache.mu.Unlock()
		return &corev1.PodList{Items: items}, nil
	}
	if p.namespace != "" {
		if allPods, ok := p.cache.pods[podCacheKey{client: p.rawClient, namespace: ""}]; ok {
			var filtered []corev1.Pod
			for i := range allPods {
				if allPods[i].Namespace == p.namespace {
					filtered = append(filtered, allPods[i])
				}
			}
			p.cache.mu.Unlock()
			return &corev1.PodList{Items: filtered}, nil
		}
	}
	p.cache.mu.Unlock()

	items, err := ListAllPages(ctx, func(ctx context.Context, pageOpts metav1.ListOptions) ([]corev1.Pod, string, error) {
		list, err := p.PodInterface.List(ctx, pageOpts)
		if err != nil {
			return nil, "", err
		}
		return list.Items, list.Continue, nil
	})
	if err != nil {
		return nil, err
	}

	p.cache.mu.Lock()
	stored := make([]corev1.Pod, len(items))
	copy(stored, items)
	p.cache.pods[exactKey] = stored
	p.cache.mu.Unlock()

	out := make([]corev1.Pod, len(items))
	copy(out, items)
	return &corev1.PodList{Items: out}, nil
}

type cachedNodes struct {
	corev1client.NodeInterface
	rawClient kubernetes.Interface
	cache     *listCache
}

func (n *cachedNodes) List(ctx context.Context, opts metav1.ListOptions) (*corev1.NodeList, error) {
	if opts.LabelSelector != "" || opts.FieldSelector != "" || opts.ResourceVersion != "" || opts.Continue != "" {
		return n.NodeInterface.List(ctx, opts)
	}
	n.cache.mu.Lock()
	if cached, ok := n.cache.nodes[n.rawClient]; ok {
		items := make([]corev1.Node, len(cached))
		copy(items, cached)
		n.cache.mu.Unlock()
		return &corev1.NodeList{Items: items}, nil
	}
	n.cache.mu.Unlock()

	items, err := ListAllPages(ctx, func(ctx context.Context, pageOpts metav1.ListOptions) ([]corev1.Node, string, error) {
		list, err := n.NodeInterface.List(ctx, pageOpts)
		if err != nil {
			return nil, "", err
		}
		return list.Items, list.Continue, nil
	})
	if err != nil {
		return nil, err
	}

	n.cache.mu.Lock()
	stored := make([]corev1.Node, len(items))
	copy(stored, items)
	n.cache.nodes[n.rawClient] = stored
	n.cache.mu.Unlock()

	out := make([]corev1.Node, len(items))
	copy(out, items)
	return &corev1.NodeList{Items: out}, nil
}
