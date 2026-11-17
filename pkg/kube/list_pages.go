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
	"reflect"
	"slices"
	"sync"

	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// DefaultPageLimit is the chunk size requested on paginated API list calls so
// large clusters stream in bounded pages instead of one unchunked response,
// while keeping a 100k-pod sweep to ~21 pages (well inside DefaultBurst=100
// with zero client-side token-bucket delay).
const DefaultPageLimit int64 = 5000

const (
	// clusterStreamWorkers bounds concurrent per-namespace list workers when a
	// cluster-wide read spans more than DefaultPageLimit items across multiple
	// namespaces, parallelizing Protobuf marshal/unmarshal across cores while
	// staying well inside DefaultBurst=100.
	clusterStreamWorkers = 12
	// namespaceStreamPageLimit bounds per-worker page size during parallel
	// namespace streaming so transient memory stays small across active workers.
	namespaceStreamPageLimit int64 = 2500
)

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
	return ListAllPagesTrim(ctx, list, nil)
}

// ListAllPagesTrim fetches all items across Continue tokens and calls trim on
// each item as soon as its page arrives, before appending to the returned
// slice and requesting the next page. Trimming per page bounds transient memory
// to one untrimmed page plus the trimmed items collected so far, while keeping
// single-restart HTTP 410 Gone recovery.
func ListAllPagesTrim[T any](
	ctx context.Context,
	list func(context.Context, metav1.ListOptions) ([]T, string, error),
	trim func(*T),
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
		if trim != nil {
			for i := range page {
				trim(&page[i])
			}
		}
		if len(all) == 0 && next == "" {
			return page, nil
		}
		all = append(all, page...)
		if next == "" {
			return slices.Clip(all), nil
		}
		opts.Continue = next
	}
}

// ListAllPagesMap fetches all pages across Continue tokens, projecting and
// optionally filtering each item via fn as soon as its page arrives. Items for
// which fn returns ok=false are dropped immediately, so callers that only keep
// a small summary or filtered subset never buffer full Kubernetes objects across
// pages. A single HTTP 410 Gone continuation error discards partial results and
// restarts cleanly from page 1.
func ListAllPagesMap[T, R any](
	ctx context.Context,
	list func(context.Context, metav1.ListOptions) ([]T, string, error),
	fn func(*T) (R, bool),
) ([]R, error) {
	opts := metav1.ListOptions{Limit: DefaultPageLimit}
	var all []R
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
		for i := range page {
			if mapped, ok := fn(&page[i]); ok {
				all = append(all, mapped)
			}
		}
		if next == "" {
			return slices.Clip(all), nil
		}
		opts.Continue = next
	}
}

// ListPagesStream streams pages across Continue tokens, delivering each item to
// each as its page arrives without buffering the entire list in memory. If the
// API server expires the continue token mid-stream (HTTP 410 Gone), reset is
// invoked (when non-nil) to discard partial caller state and pagination restarts
// once from Continue="".
//
// Callers that must not leave partial mutations behind when pagination fails
// even after retry should accumulate into local state and commit only when
// ListPagesStream returns nil.
func ListPagesStream[T any](
	ctx context.Context,
	list func(context.Context, metav1.ListOptions) ([]T, string, error),
	reset func(),
	each func(*T),
) error {
	opts := metav1.ListOptions{Limit: DefaultPageLimit}
	restarted := false

	for {
		page, next, err := list(ctx, opts)
		if err != nil {
			if (apierrors.IsResourceExpired(err) || apierrors.IsGone(err)) && !restarted {
				restarted = true
				if reset != nil {
					reset()
				}
				opts = metav1.ListOptions{Limit: DefaultPageLimit}
				continue
			}
			return err
		}
		for i := range page {
			each(&page[i])
		}
		if next == "" {
			return nil
		}
		opts.Continue = next
	}
}

// StreamClusterNamespaced streams a namespaced resource across scope ns.
//
// Small or single-namespace clusters complete in one sequential paginated stream
// with zero extra API calls. When ns == "" (all namespaces) and page 1 returns a
// non-empty Continue token (> DefaultPageLimit items), StreamClusterNamespaced
// probes CoreV1().Namespaces() and fans out bounded concurrent per-namespace
// workers to parallelize API-server and client Protobuf encoding across cores,
// while delivering every item sequentially to each on the caller's goroutine.
// If listing namespaces fails (e.g. RBAC) or returns <= 1 namespace, page 1 is
// preserved and sequential Continue pagination resumes transparently.
func StreamClusterNamespaced[T any](
	ctx context.Context,
	client kubernetes.Interface,
	ns string,
	listForNS func(string) func(context.Context, metav1.ListOptions) ([]T, string, error),
	trim func(*T),
	reset func(),
	each func(*T),
) error {
	raw := unwrapCachedClient(client)
	if ns != "" {
		nsList := listForNS(ns)
		return ListPagesStream(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]T, string, error) {
			items, next, err := nsList(ctx, opts)
			if err != nil {
				return nil, "", err
			}
			if trim != nil {
				for i := range items {
					trim(&items[i])
				}
			}
			return items, next, nil
		}, reset, each)
	}

	allList := listForNS("")
	firstPage, next, err := allList(ctx, metav1.ListOptions{Limit: DefaultPageLimit})
	if err != nil {
		return err
	}
	if next == "" {
		for i := range firstPage {
			if trim != nil {
				trim(&firstPage[i])
			}
			each(&firstPage[i])
		}
		return nil
	}

	nsNames, nsErr := listNamespaceNames(ctx, raw)
	if nsErr != nil || len(nsNames) <= 1 {
		return finishPagesSequential(ctx, allList, firstPage, next, trim, reset, each)
	}
	firstPage = nil

	err = runParallelNamespaceStream(ctx, nsNames, listForNS, trim, each)
	if err != nil && (apierrors.IsResourceExpired(err) || apierrors.IsGone(err)) && ctx.Err() == nil {
		if reset != nil {
			reset()
		}
		err = runParallelNamespaceStream(ctx, nsNames, listForNS, trim, each)
	}
	return err
}

func unwrapCachedClient(client kubernetes.Interface) kubernetes.Interface {
	if wc, ok := client.(*cachedClient); ok {
		return wc.Interface
	}
	return client
}

func listNamespaceNames(ctx context.Context, client kubernetes.Interface) ([]string, error) {
	if client == nil {
		return nil, nil
	}
	return ListAllPagesMap(ctx, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Namespace, string, error) {
		l, err := client.CoreV1().Namespaces().List(ctx, opts)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(nsObj *corev1.Namespace) (string, bool) {
		return nsObj.Name, nsObj.Name != ""
	})
}

func finishPagesSequential[T any](
	ctx context.Context,
	list func(context.Context, metav1.ListOptions) ([]T, string, error),
	firstPage []T,
	next string,
	trim func(*T),
	reset func(),
	each func(*T),
) error {
	for i := range firstPage {
		if trim != nil {
			trim(&firstPage[i])
		}
		each(&firstPage[i])
	}
	opts := metav1.ListOptions{Limit: DefaultPageLimit, Continue: next}
	restarted := false
	for {
		page, cont, err := list(ctx, opts)
		if err != nil {
			if (apierrors.IsResourceExpired(err) || apierrors.IsGone(err)) && !restarted {
				restarted = true
				if reset != nil {
					reset()
				}
				opts = metav1.ListOptions{Limit: DefaultPageLimit}
				continue
			}
			return err
		}
		for i := range page {
			if trim != nil {
				trim(&page[i])
			}
			each(&page[i])
		}
		if cont == "" {
			return nil
		}
		opts.Continue = cont
	}
}

func runParallelNamespaceStream[T any](
	ctx context.Context,
	nsNames []string,
	listForNS func(string) func(context.Context, metav1.ListOptions) ([]T, string, error),
	trim func(*T),
	each func(*T),
) error {
	workers := min(clusterStreamWorkers, len(nsNames))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	nsCh := make(chan string, len(nsNames))
	for _, name := range nsNames {
		nsCh <- name
	}
	close(nsCh)

	pageCh := make(chan []T, workers)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	recordErr := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for nsName := range nsCh {
				if workCtx.Err() != nil {
					return
				}
				nsList := listForNS(nsName)
				opts := metav1.ListOptions{Limit: namespaceStreamPageLimit}
				for {
					page, next, err := nsList(workCtx, opts)
					if err != nil {
						if apierrors.IsNotFound(err) {
							break
						}
						recordErr(err)
						return
					}
					if len(page) > 0 {
						if trim != nil {
							for i := range page {
								trim(&page[i])
							}
						}
						select {
						case pageCh <- page:
						case <-workCtx.Done():
							return
						}
					}
					if next == "" {
						break
					}
					opts.Continue = next
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(pageCh)
	}()

	for page := range pageCh {
		if workCtx.Err() != nil {
			continue
		}
		for i := range page {
			each(&page[i])
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}

// StreamClusterPods streams Pods in scope ns, consulting any WithListCache
// wrapper on client first and parallelizing >5,000-pod cluster-wide reads across
// namespaces while invoking each sequentially on the caller's goroutine.
func StreamClusterPods(
	ctx context.Context,
	client kubernetes.Interface,
	ns string,
	trim func(*corev1.Pod),
	reset func(),
	each func(*corev1.Pod),
) error {
	if wc, ok := client.(*cachedClient); ok {
		l, err := wc.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		for i := range l.Items {
			if trim != nil {
				trim(&l.Items[i])
			}
			each(&l.Items[i])
		}
		return nil
	}
	return StreamClusterNamespaced(ctx, client, ns, func(targetNS string) func(context.Context, metav1.ListOptions) ([]corev1.Pod, string, error) {
		return func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Pod, string, error) {
			l, err := client.CoreV1().Pods(targetNS).List(ctx, opts)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}
	}, trim, reset, each)
}

// ListClusterPodsTrim fetches all Pods in scope ns with TrimReadPod applied per
// page, using parallel per-namespace workers on >5,000-pod cluster-wide sweeps.
func ListClusterPodsTrim(ctx context.Context, client kubernetes.Interface, ns string) ([]corev1.Pod, error) {
	var all []corev1.Pod
	err := StreamClusterPods(ctx, client, ns, TrimReadPod, func() {
		all = nil
	}, func(p *corev1.Pod) {
		all = append(all, *p)
	})
	if err != nil {
		return nil, err
	}
	return slices.Clip(all), nil
}

// ListClusterPodsMap streams all Pods in scope ns and collects compact mapped
// projections for items where fn returns ok=true, never retaining full Pod
// objects across pages.
func ListClusterPodsMap[R any](
	ctx context.Context,
	client kubernetes.Interface,
	ns string,
	fn func(*corev1.Pod) (R, bool),
) ([]R, error) {
	var all []R
	err := StreamClusterPods(ctx, client, ns, nil, func() {
		all = nil
	}, func(p *corev1.Pod) {
		if mapped, ok := fn(p); ok {
			all = append(all, mapped)
		}
	})
	if err != nil {
		return nil, err
	}
	return slices.Clip(all), nil
}

// StreamClusterEvents streams corev1.Events in scope ns, parallelizing
// >5,000-event cluster-wide reads across namespaces while delivering every
// event sequentially to each on the caller's goroutine.
func StreamClusterEvents(
	ctx context.Context,
	client kubernetes.Interface,
	ns string,
	reset func(),
	each func(*corev1.Event),
) error {
	raw := unwrapCachedClient(client)
	return StreamClusterNamespaced(ctx, raw, ns, func(targetNS string) func(context.Context, metav1.ListOptions) ([]corev1.Event, string, error) {
		return func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Event, string, error) {
			l, err := raw.CoreV1().Events(targetNS).List(ctx, opts)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}
	}, nil, reset, each)
}

// TrimReadPod drops high-overhead metadata from a Pod fetched on the one-shot
// read path without touching any field inspected by read-path checks (including
// `audit`, `delta`, `state`, and `stab`, which read container specs, probes,
// env vars, security contexts, status, labels, and operational annotations).
//
// Repeated calls on an already-trimmed Pod perform zero memory writes so shared
// read-only slices returned from WithListCache remain race-free across
// concurrent sub-checks.
func TrimReadPod(p *corev1.Pod) {
	if p == nil {
		return
	}
	if p.ManagedFields != nil {
		p.ManagedFields = nil
	}
	if trimmed, changed := stripAppliedAnnotationsChecked(p.Annotations); changed {
		p.Annotations = trimmed
	}
}

// TrimReadNode drops high-overhead status blobs from a Node fetched on the
// one-shot read path — most notably Status.Images (10-40 KiB per node), plus
// VolumesInUse, VolumesAttached, and full-manifest last-applied annotations.
//
// Node.ManagedFields is deliberately preserved here (unlike the watch-path
// informer transform): pkg/checks/delta/squeeze.go reads ManagedFields to
// attribute taint writers when explaining unschedulable pods.
//
// Repeated calls on an already-trimmed Node perform zero memory writes so
// shared read-only slices returned from WithListCache remain race-free across
// concurrent sub-checks.
func TrimReadNode(n *corev1.Node) {
	if n == nil {
		return
	}
	if trimmed, changed := stripAppliedAnnotationsChecked(n.Annotations); changed {
		n.Annotations = trimmed
	}
	if n.Status.Images != nil {
		n.Status.Images = nil
	}
	if n.Status.VolumesInUse != nil {
		n.Status.VolumesInUse = nil
	}
	if n.Status.VolumesAttached != nil {
		n.Status.VolumesAttached = nil
	}
}

type listCacheKey struct{}

type podCacheKey struct {
	client    kubernetes.Interface
	namespace string
}

type clientCacheEntry struct {
	opts   Options
	done   chan struct{}
	client kubernetes.Interface
	err    error
}

type inflightPods struct {
	done  chan struct{}
	items []corev1.Pod
	err   error
}

type inflightNodes struct {
	done  chan struct{}
	items []corev1.Node
	err   error
}

type listCache struct {
	mu          sync.Mutex
	clients     []*clientCacheEntry
	pods        map[podCacheKey][]corev1.Pod
	podFlights  map[podCacheKey]*inflightPods
	nodes       map[kubernetes.Interface][]corev1.Node
	nodeFlights map[kubernetes.Interface]*inflightNodes
}

// WithListCache attaches a short-lived in-memory list cache to ctx so multiple
// sub-checks within a single command execution (such as `lookout scan`,
// `lookout health`, or `lookout audit all`) share one default Kubernetes client
// and cluster-wide or namespace Pod/Node lists (including concurrent in-flight
// requests) without re-listing from the API server or duplicating multi-GiB Pod
// slices in memory.
func WithListCache(ctx context.Context) context.Context {
	if ctx.Value(listCacheKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, listCacheKey{}, &listCache{
		pods:        make(map[podCacheKey][]corev1.Pod),
		podFlights:  make(map[podCacheKey]*inflightPods),
		nodes:       make(map[kubernetes.Interface][]corev1.Node),
		nodeFlights: make(map[kubernetes.Interface]*inflightNodes),
	})
}

func cachedDefaultClient(ctx context.Context, opts Options) (kubernetes.Interface, error) {
	cache, ok := ctx.Value(listCacheKey{}).(*listCache)
	if !ok || cache == nil {
		return BuildClient(opts)
	}
	cache.mu.Lock()
	for _, entry := range cache.clients {
		if sameOptions(entry.opts, opts) {
			cache.mu.Unlock()
			select {
			case <-entry.done:
				if entry.err != nil {
					return nil, entry.err
				}
				return WrapClient(ctx, entry.client), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	entry := &clientCacheEntry{opts: opts, done: make(chan struct{})}
	cache.clients = append(cache.clients, entry)
	cache.mu.Unlock()

	raw, err := BuildClient(opts)

	cache.mu.Lock()
	if err != nil {
		// Evict failed builds so a subsequent call can retry cleanly.
		for i, e := range cache.clients {
			if e == entry {
				cache.clients = slices.Delete(cache.clients, i, i+1)
				break
			}
		}
		entry.err = err
	} else {
		entry.client = raw
	}
	close(entry.done)
	cache.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return WrapClient(ctx, raw), nil
}

func sameOptions(a, b Options) bool {
	if a.InCluster != b.InCluster ||
		a.Kubeconfig != b.Kubeconfig ||
		a.Context != b.Context ||
		a.QPS != b.QPS ||
		a.Burst != b.Burst ||
		a.DialTimeout != b.DialTimeout ||
		!sameTokenSource(a.CallerToken, b.CallerToken) {
		return false
	}
	if a.Target == b.Target {
		return true
	}
	if a.Target == nil || b.Target == nil {
		return false
	}
	return *a.Target == *b.Target
}

func sameTokenSource(a, b oauth2.TokenSource) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	ta, tb := reflect.TypeOf(a), reflect.TypeOf(b)
	if ta != tb || !ta.Comparable() {
		return false
	}
	return a == b
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
		p.cache.mu.Unlock()
		return &corev1.PodList{Items: cached}, nil
	}
	if p.namespace != "" {
		allKey := podCacheKey{client: p.rawClient, namespace: ""}
		if allPods, ok := p.cache.pods[allKey]; ok {
			p.cache.mu.Unlock()
			return &corev1.PodList{Items: filterPodsByNamespace(allPods, p.namespace)}, nil
		}
		if allFlight, ok := p.cache.podFlights[allKey]; ok {
			p.cache.mu.Unlock()
			select {
			case <-allFlight.done:
				if allFlight.err == nil {
					return &corev1.PodList{Items: filterPodsByNamespace(allFlight.items, p.namespace)}, nil
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			p.cache.mu.Lock()
			if cached, ok := p.cache.pods[exactKey]; ok {
				p.cache.mu.Unlock()
				return &corev1.PodList{Items: cached}, nil
			}
		}
	}
	if flight, ok := p.cache.podFlights[exactKey]; ok {
		p.cache.mu.Unlock()
		select {
		case <-flight.done:
			if flight.err != nil {
				return nil, flight.err
			}
			return &corev1.PodList{Items: flight.items}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	flight := &inflightPods{done: make(chan struct{})}
	p.cache.podFlights[exactKey] = flight
	p.cache.mu.Unlock()

	items, err := ListClusterPodsTrim(ctx, p.rawClient, p.namespace)

	p.cache.mu.Lock()
	delete(p.cache.podFlights, exactKey)
	if err != nil {
		flight.err = err
		close(flight.done)
		p.cache.mu.Unlock()
		return nil, err
	}
	p.cache.pods[exactKey] = items
	flight.items = items
	close(flight.done)
	p.cache.mu.Unlock()

	return &corev1.PodList{Items: items}, nil
}

func filterPodsByNamespace(allPods []corev1.Pod, ns string) []corev1.Pod {
	var filtered []corev1.Pod
	for i := range allPods {
		if allPods[i].Namespace == ns {
			filtered = append(filtered, allPods[i])
		}
	}
	return slices.Clip(filtered)
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
		n.cache.mu.Unlock()
		return &corev1.NodeList{Items: cached}, nil
	}
	if flight, ok := n.cache.nodeFlights[n.rawClient]; ok {
		n.cache.mu.Unlock()
		select {
		case <-flight.done:
			if flight.err != nil {
				return nil, flight.err
			}
			return &corev1.NodeList{Items: flight.items}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	flight := &inflightNodes{done: make(chan struct{})}
	n.cache.nodeFlights[n.rawClient] = flight
	n.cache.mu.Unlock()

	items, err := ListAllPagesTrim(ctx, func(ctx context.Context, pageOpts metav1.ListOptions) ([]corev1.Node, string, error) {
		list, err := n.NodeInterface.List(ctx, pageOpts)
		if err != nil {
			return nil, "", err
		}
		return list.Items, list.Continue, nil
	}, TrimReadNode)
	if err == nil {
		items = slices.Clip(items)
	}

	n.cache.mu.Lock()
	delete(n.cache.nodeFlights, n.rawClient)
	if err != nil {
		flight.err = err
		close(flight.done)
		n.cache.mu.Unlock()
		return nil, err
	}
	n.cache.nodes[n.rawClient] = items
	flight.items = items
	close(flight.done)
	n.cache.mu.Unlock()

	return &corev1.NodeList{Items: items}, nil
}
