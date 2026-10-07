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
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"k8s.io/client-go/informers"
	appsinformers "k8s.io/client-go/informers/apps"
	appsv1informers "k8s.io/client-go/informers/apps/v1"
	autoscalinginformers "k8s.io/client-go/informers/autoscaling"
	autoscalingv2informers "k8s.io/client-go/informers/autoscaling/v2"
	batchinformers "k8s.io/client-go/informers/batch"
	batchv1informers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core"
	corev1informers "k8s.io/client-go/informers/core/v1"
	discoveryinformers "k8s.io/client-go/informers/discovery"
	discoveryv1informers "k8s.io/client-go/informers/discovery/v1"
	networkinginformers "k8s.io/client-go/informers/networking"
	networkingv1informers "k8s.io/client-go/informers/networking/v1"
	policyinformers "k8s.io/client-go/informers/policy"
	policyv1informers "k8s.io/client-go/informers/policy/v1"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	autoscalingv2listers "k8s.io/client-go/listers/autoscaling/v2"
	batchv1listers "k8s.io/client-go/listers/batch/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	discoveryv1listers "k8s.io/client-go/listers/discovery/v1"
	networkingv1listers "k8s.io/client-go/listers/networking/v1"
	policyv1listers "k8s.io/client-go/listers/policy/v1"
	"k8s.io/client-go/tools/cache"
)

// The read side of a multi-namespace watch scope (#407): one shard per
// namespace, one brain.
//
// Each scope namespace gets its own informers.WithNamespace factory — its own
// LIST+WATCH streams, its own reflector goroutines, its own cache — because
// that is the only way a namespaced Role can be honoured: field selectors
// have no OR, and `?fieldSelector=metadata.namespace=x` is still authorised
// as a cluster-wide list. Those shards are the whole of what multiplies.
//
// Everything downstream stays single: ONE instance of each source, ONE
// dispatcher, dedup cache, store, storm correlator and topology graph. The
// sources reach the shards through unionFactory, which presents the M
// namespaced factories as one SharedInformerFactory: an informer from it adds
// each handler to every shard's informer of that type, and its lister reads
// across all of them.
//
// Why union views rather than one source instance per shard: several sources
// hold state that must span the scope — topology-drift's domain inventory,
// compute-class's rank shares and the storm graph are distributions over the
// union, and object-state, capacity and saturation would each emit their
// cluster-scoped findings (node conditions, autoscaler status) once per
// shard. Per-shard instances would also each answer §7.4 clearance for
// incidents in namespaces they cannot see — "not in my cache" reads as
// "gone", which is a false recovery. Merging below the sources instead keeps
// every source exactly as it runs at the cluster scope.
//
// The view is read-only and covers exactly the informer types the sentinel
// takes from its namespaced factory. Every other accessor is a nil embedded
// interface, so a new call site that reaches for an uncovered type panics on
// first use — in its tests — instead of silently watching one shard.
// TestWatchScopeNamespaces_HybridShardsOneBrain runs the real wiring over
// a union to keep that true.

// unionFactory is informers.SharedInformerFactory over per-namespace shards.
type unionFactory struct {
	informers.SharedInformerFactory // nil: uncovered accessors panic loudly

	shards  []informers.SharedInformerFactory
	cluster informers.SharedInformerFactory
}

func newUnionFactory(shards []informers.SharedInformerFactory, cluster informers.SharedInformerFactory) *unionFactory {
	return &unionFactory{shards: shards, cluster: cluster}
}

func (u *unionFactory) Start(stopCh <-chan struct{}) {
	for _, s := range u.shards {
		s.Start(stopCh)
	}
}

func (u *unionFactory) StartWithContext(ctx context.Context) { u.Start(ctx.Done()) }

func (u *unionFactory) Shutdown() {
	for _, s := range u.shards {
		s.Shutdown()
	}
}

// WaitForCacheSync reports a type synced only when it is synced in every
// shard.
func (u *unionFactory) WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool {
	out := map[reflect.Type]bool{}
	for i, s := range u.shards {
		for t, ok := range s.WaitForCacheSync(stopCh) {
			if i == 0 {
				out[t] = ok
			} else {
				out[t] = out[t] && ok
			}
		}
	}
	return out
}

// union builds the merged informer for one type.
func (u *unionFactory) union(get func(informers.SharedInformerFactory) cache.SharedIndexInformer) cache.SharedIndexInformer {
	subs := make([]cache.SharedIndexInformer, len(u.shards))
	for i, s := range u.shards {
		subs[i] = get(s)
	}
	return &unionInformer{subs: subs}
}

// unionTyped is one type's informer+lister pair over the union. Its method
// set is the generated XInformer interface, which ToTypedXInformer adapts.
type unionTyped[L any] struct {
	u      *unionFactory
	get    func(informers.SharedInformerFactory) cache.SharedIndexInformer
	lister func(cache.Indexer) L
}

func (t unionTyped[L]) Informer() cache.SharedIndexInformer { return t.u.union(t.get) }
func (t unionTyped[L]) Lister() L                           { return t.lister(t.Informer().GetIndexer()) }

// ---- group accessors: exactly the types the sentinel reads ----

func (u *unionFactory) Core() coreinformers.Interface { return unionCore{u: u} }

type unionCore struct {
	coreinformers.Interface
	u *unionFactory
}

func (c unionCore) V1() corev1informers.Interface { return unionCoreV1{u: c.u} }

type unionCoreV1 struct {
	corev1informers.Interface
	u *unionFactory
}

func (c unionCoreV1) Pods() corev1informers.TypedPodInformer {
	return corev1informers.ToTypedPodInformer(unionTyped[corev1listers.PodLister]{c.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Core().V1().Pods().Informer()
		},
		corev1listers.NewPodLister})
}

func (c unionCoreV1) Events() corev1informers.TypedEventInformer {
	return corev1informers.ToTypedEventInformer(unionTyped[corev1listers.EventLister]{c.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Core().V1().Events().Informer()
		},
		corev1listers.NewEventLister})
}

func (c unionCoreV1) PersistentVolumeClaims() corev1informers.TypedPersistentVolumeClaimInformer {
	return corev1informers.ToTypedPersistentVolumeClaimInformer(unionTyped[corev1listers.PersistentVolumeClaimLister]{c.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Core().V1().PersistentVolumeClaims().Informer()
		},
		corev1listers.NewPersistentVolumeClaimLister})
}

func (c unionCoreV1) Services() corev1informers.TypedServiceInformer {
	return corev1informers.ToTypedServiceInformer(unionTyped[corev1listers.ServiceLister]{c.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Core().V1().Services().Informer()
		},
		corev1listers.NewServiceLister})
}

// Nodes and PersistentVolumes are cluster-scoped: one stream on the cluster
// factory, shared by every reader, never multiplied by the shards.
func (c unionCoreV1) Nodes() corev1informers.TypedNodeInformer {
	return c.u.cluster.Core().V1().Nodes()
}

func (c unionCoreV1) PersistentVolumes() corev1informers.TypedPersistentVolumeInformer {
	return c.u.cluster.Core().V1().PersistentVolumes()
}

func (u *unionFactory) Apps() appsinformers.Interface { return unionApps{u: u} }

type unionApps struct {
	appsinformers.Interface
	u *unionFactory
}

func (a unionApps) V1() appsv1informers.Interface { return unionAppsV1{u: a.u} }

type unionAppsV1 struct {
	appsv1informers.Interface
	u *unionFactory
}

func (a unionAppsV1) Deployments() appsv1informers.TypedDeploymentInformer {
	return appsv1informers.ToTypedDeploymentInformer(unionTyped[appsv1listers.DeploymentLister]{a.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Apps().V1().Deployments().Informer()
		},
		appsv1listers.NewDeploymentLister})
}

func (a unionAppsV1) ReplicaSets() appsv1informers.TypedReplicaSetInformer {
	return appsv1informers.ToTypedReplicaSetInformer(unionTyped[appsv1listers.ReplicaSetLister]{a.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Apps().V1().ReplicaSets().Informer()
		},
		appsv1listers.NewReplicaSetLister})
}

func (a unionAppsV1) StatefulSets() appsv1informers.TypedStatefulSetInformer {
	return appsv1informers.ToTypedStatefulSetInformer(unionTyped[appsv1listers.StatefulSetLister]{a.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Apps().V1().StatefulSets().Informer()
		},
		appsv1listers.NewStatefulSetLister})
}

func (u *unionFactory) Batch() batchinformers.Interface { return unionBatch{u: u} }

type unionBatch struct {
	batchinformers.Interface
	u *unionFactory
}

func (b unionBatch) V1() batchv1informers.Interface { return unionBatchV1{u: b.u} }

type unionBatchV1 struct {
	batchv1informers.Interface
	u *unionFactory
}

func (b unionBatchV1) Jobs() batchv1informers.TypedJobInformer {
	return batchv1informers.ToTypedJobInformer(unionTyped[batchv1listers.JobLister]{b.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Batch().V1().Jobs().Informer()
		},
		batchv1listers.NewJobLister})
}

func (b unionBatchV1) CronJobs() batchv1informers.TypedCronJobInformer {
	return batchv1informers.ToTypedCronJobInformer(unionTyped[batchv1listers.CronJobLister]{b.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Batch().V1().CronJobs().Informer()
		},
		batchv1listers.NewCronJobLister})
}

func (u *unionFactory) Autoscaling() autoscalinginformers.Interface { return unionAutoscaling{u: u} }

type unionAutoscaling struct {
	autoscalinginformers.Interface
	u *unionFactory
}

func (a unionAutoscaling) V2() autoscalingv2informers.Interface { return unionAutoscalingV2{u: a.u} }

type unionAutoscalingV2 struct {
	autoscalingv2informers.Interface
	u *unionFactory
}

func (a unionAutoscalingV2) HorizontalPodAutoscalers() autoscalingv2informers.TypedHorizontalPodAutoscalerInformer {
	return autoscalingv2informers.ToTypedHorizontalPodAutoscalerInformer(unionTyped[autoscalingv2listers.HorizontalPodAutoscalerLister]{a.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Autoscaling().V2().HorizontalPodAutoscalers().Informer()
		},
		autoscalingv2listers.NewHorizontalPodAutoscalerLister})
}

func (u *unionFactory) Discovery() discoveryinformers.Interface { return unionDiscovery{u: u} }

type unionDiscovery struct {
	discoveryinformers.Interface
	u *unionFactory
}

func (d unionDiscovery) V1() discoveryv1informers.Interface { return unionDiscoveryV1{u: d.u} }

type unionDiscoveryV1 struct {
	discoveryv1informers.Interface
	u *unionFactory
}

func (d unionDiscoveryV1) EndpointSlices() discoveryv1informers.TypedEndpointSliceInformer {
	return discoveryv1informers.ToTypedEndpointSliceInformer(unionTyped[discoveryv1listers.EndpointSliceLister]{d.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Discovery().V1().EndpointSlices().Informer()
		},
		discoveryv1listers.NewEndpointSliceLister})
}

func (u *unionFactory) Policy() policyinformers.Interface { return unionPolicy{u: u} }

type unionPolicy struct {
	policyinformers.Interface
	u *unionFactory
}

func (p unionPolicy) V1() policyv1informers.Interface { return unionPolicyV1{u: p.u} }

type unionPolicyV1 struct {
	policyv1informers.Interface
	u *unionFactory
}

func (p unionPolicyV1) PodDisruptionBudgets() policyv1informers.TypedPodDisruptionBudgetInformer {
	return policyv1informers.ToTypedPodDisruptionBudgetInformer(unionTyped[policyv1listers.PodDisruptionBudgetLister]{p.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Policy().V1().PodDisruptionBudgets().Informer()
		},
		policyv1listers.NewPodDisruptionBudgetLister})
}

func (u *unionFactory) Networking() networkinginformers.Interface { return unionNetworking{u: u} }

type unionNetworking struct {
	networkinginformers.Interface
	u *unionFactory
}

func (n unionNetworking) V1() networkingv1informers.Interface { return unionNetworkingV1{u: n.u} }

type unionNetworkingV1 struct {
	networkingv1informers.Interface
	u *unionFactory
}

func (n unionNetworkingV1) Ingresses() networkingv1informers.TypedIngressInformer {
	return networkingv1informers.ToTypedIngressInformer(unionTyped[networkingv1listers.IngressLister]{n.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Networking().V1().Ingresses().Informer()
		},
		networkingv1listers.NewIngressLister})
}

func (n unionNetworkingV1) NetworkPolicies() networkingv1informers.TypedNetworkPolicyInformer {
	return networkingv1informers.ToTypedNetworkPolicyInformer(unionTyped[networkingv1listers.NetworkPolicyLister]{n.u,
		func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Networking().V1().NetworkPolicies().Informer()
		},
		networkingv1listers.NewNetworkPolicyLister})
}

// ---- the merged informer ----

// unionInformer is cache.SharedIndexInformer over one type's shard
// informers. Only what the sentinel's sources call is implemented; the rest
// panics through the nil embedded interface.
type unionInformer struct {
	cache.SharedIndexInformer // nil: uncovered methods panic loudly

	subs []cache.SharedIndexInformer
}

// AddEventHandler adds handler to every shard. Delivery to it is serialised:
// a SharedInformer promises each handler its events one at a time, sources
// are written against that promise, and M shards would otherwise call the
// same handler from M goroutines at once.
func (u *unionInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return u.add(handler, func(s cache.SharedIndexInformer, h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
		return s.AddEventHandler(h)
	})
}

func (u *unionInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resync time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return u.add(handler, func(s cache.SharedIndexInformer, h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
		return s.AddEventHandlerWithResyncPeriod(h, resync)
	})
}

func (u *unionInformer) add(handler cache.ResourceEventHandler, add func(cache.SharedIndexInformer, cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error)) (cache.ResourceEventHandlerRegistration, error) {
	serial := &serialHandler{h: handler}
	reg := &unionRegistration{}
	for _, s := range u.subs {
		r, err := add(s, serial)
		if err != nil {
			_ = u.RemoveEventHandler(reg)
			return nil, err
		}
		reg.subs = append(reg.subs, r)
		reg.infs = append(reg.infs, s)
	}
	return reg, nil
}

func (u *unionInformer) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	reg, ok := handle.(*unionRegistration)
	if !ok {
		return errors.New("union informer: registration was not issued by this informer")
	}
	var errs []error
	for i, r := range reg.subs {
		errs = append(errs, reg.infs[i].RemoveEventHandler(r))
	}
	return errors.Join(errs...)
}

func (u *unionInformer) GetStore() cache.Store { return u.GetIndexer() }

func (u *unionInformer) GetIndexer() cache.Indexer {
	idx := make([]cache.Indexer, len(u.subs))
	for i, s := range u.subs {
		idx[i] = s.GetIndexer()
	}
	return unionIndexer{subs: idx}
}

func (u *unionInformer) HasSynced() bool {
	for _, s := range u.subs {
		if !s.HasSynced() {
			return false
		}
	}
	return true
}

func (u *unionInformer) IsStopped() bool {
	for _, s := range u.subs {
		if !s.IsStopped() {
			return false
		}
	}
	return true
}

// LastSyncResourceVersion has no single answer across shards: resource
// versions from different lists are not comparable, so none is claimed.
func (u *unionInformer) LastSyncResourceVersion() string { return "" }

func (u *unionInformer) AddIndexers(indexers cache.Indexers) error {
	for _, s := range u.subs {
		if err := s.AddIndexers(indexers); err != nil {
			return err
		}
	}
	return nil
}

// serialHandler delivers to one handler one event at a time.
type serialHandler struct {
	mu sync.Mutex
	h  cache.ResourceEventHandler
}

func (s *serialHandler) OnAdd(obj any, isInInitialList bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h.OnAdd(obj, isInInitialList)
}

func (s *serialHandler) OnUpdate(oldObj, newObj any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h.OnUpdate(oldObj, newObj)
}

func (s *serialHandler) OnDelete(obj any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h.OnDelete(obj)
}

// unionRegistration is synced once every shard's registration is: the
// handler has then seen every shard's initial list.
type unionRegistration struct {
	subs []cache.ResourceEventHandlerRegistration
	infs []cache.SharedIndexInformer
}

func (r *unionRegistration) HasSynced() bool {
	for _, s := range r.subs {
		if !s.HasSynced() {
			return false
		}
	}
	return true
}

func (r *unionRegistration) HasSyncedChecker() cache.DoneChecker {
	done := make(chan struct{})
	go func() {
		for _, s := range r.subs {
			<-s.HasSyncedChecker().Done()
		}
		close(done)
	}()
	return unionDone{done: done}
}

type unionDone struct{ done chan struct{} }

func (d unionDone) Name() string          { return "union of namespace shards" }
func (d unionDone) Done() <-chan struct{} { return d.done }

// ---- the merged, read-only indexer ----

var errUnionReadOnly = errors.New("union indexer is a read-only view over the namespace shards")

// unionIndexer reads across the shards' indexers. Keys are namespace/name
// and every shard holds a different namespace, so the union of the shards'
// answers is the answer and no object can appear twice.
type unionIndexer struct{ subs []cache.Indexer }

func (u unionIndexer) Add(any) error               { return errUnionReadOnly }
func (u unionIndexer) Update(any) error            { return errUnionReadOnly }
func (u unionIndexer) Delete(any) error            { return errUnionReadOnly }
func (u unionIndexer) Replace([]any, string) error { return errUnionReadOnly }
func (u unionIndexer) Resync() error               { return nil }
func (u unionIndexer) Bookmark(string)             {}
func (u unionIndexer) LastStoreSyncResourceVersion() string {
	return ""
}

func (u unionIndexer) List() []any {
	var out []any
	for _, s := range u.subs {
		out = append(out, s.List()...)
	}
	return out
}

func (u unionIndexer) ListKeys() []string {
	var out []string
	for _, s := range u.subs {
		out = append(out, s.ListKeys()...)
	}
	return out
}

func (u unionIndexer) Get(obj any) (any, bool, error) {
	for _, s := range u.subs {
		if item, ok, err := s.Get(obj); err != nil || ok {
			return item, ok, err
		}
	}
	return nil, false, nil
}

func (u unionIndexer) GetByKey(key string) (any, bool, error) {
	for _, s := range u.subs {
		if item, ok, err := s.GetByKey(key); err != nil || ok {
			return item, ok, err
		}
	}
	return nil, false, nil
}

func (u unionIndexer) Index(indexName string, obj any) ([]any, error) {
	var out []any
	for _, s := range u.subs {
		items, err := s.Index(indexName, obj)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return out, nil
}

func (u unionIndexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	var out []string
	for _, s := range u.subs {
		keys, err := s.IndexKeys(indexName, indexedValue)
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
	}
	return out, nil
}

func (u unionIndexer) ListIndexFuncValues(indexName string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range u.subs {
		for _, v := range s.ListIndexFuncValues(indexName) {
			if _, dup := seen[v]; !dup {
				seen[v] = struct{}{}
				out = append(out, v)
			}
		}
	}
	return out
}

func (u unionIndexer) ByIndex(indexName, indexedValue string) ([]any, error) {
	var out []any
	for _, s := range u.subs {
		items, err := s.ByIndex(indexName, indexedValue)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return out, nil
}

func (u unionIndexer) GetIndexers() cache.Indexers {
	if len(u.subs) == 0 {
		return cache.Indexers{}
	}
	return u.subs[0].GetIndexers()
}

func (u unionIndexer) AddIndexers(newIndexers cache.Indexers) error {
	for _, s := range u.subs {
		if err := s.AddIndexers(newIndexers); err != nil {
			return err
		}
	}
	return nil
}
