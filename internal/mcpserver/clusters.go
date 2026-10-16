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

package mcpserver

// The cluster directory of a multi-cluster endpoint (#700,
// docs/mcp-multi-tenant-design.md Part 1): which clusters a call may
// name, what each is called, and the client config that reaches it.
//
// Two modes. With the server's own identity, one set is discovered at
// startup and refreshed every --rediscover; every caller sees it. In
// Google-token mode lookout has no identity of its own, so each caller's
// set is discovered with that caller's token and cached for a few
// minutes: lookout needs no IAM, and nobody learns that a cluster they
// cannot read exists.
//
// Names are short where they can be. A cluster's name is unique only
// within a project and location, so a call names it bare when that is
// unique in the set, as name@project when it is not, and as
// name@project/location when even that collides; the canonical
// project/location/name is always accepted. The set a name is unique in
// is the caller's, so two callers can see different short names for one
// cluster, and anything that outlives the call (the access log, a
// store label) uses the canonical name.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/internal/fleet"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

const (
	// defaultCallerSetTTL is how long a caller's discovered set is reused.
	defaultCallerSetTTL = 5 * time.Minute
	// DefaultRediscover is how often the shared set is refreshed.
	DefaultRediscover = 10 * time.Minute
	// missRediscoverEvery bounds rediscovery on an unknown name, per
	// caller: a typo must not become an API call per tool call.
	missRediscoverEvery = time.Minute
)

// errUnknownCluster is a name the caller's set does not hold.
var errUnknownCluster = errors.New("unknown cluster")

// Clusters is a multi-cluster endpoint's directory.
type Clusters struct {
	src       fleet.Source
	perCaller bool
	ttl       time.Duration
	refresh   time.Duration
	now       func() time.Time

	mu       sync.Mutex
	sets     map[string]*clusterSet // by caller; "" is the shared set
	lastMiss map[string]time.Time
	configs  map[string]*rest.Config // shared mode only, by canonical name and endpoint
	// inflight shares one discovery among concurrent calls for the same
	// key: an agent's parallel fan-out is the documented way to use this
	// endpoint, and each call starting its own discovery would multiply
	// the API calls by the fan-out.
	inflight map[string]*pendingDiscovery
}

type pendingDiscovery struct {
	done chan struct{}
	set  *clusterSet
	err  error
}

// maxCallerSets bounds how many callers' sets are kept before stale ones
// are dropped.
const maxCallerSets = 512

// NewClusters builds a directory over a fleet source. perCaller is
// Google-token mode: discovery runs as each caller. refresh is the
// shared set's rediscovery interval (zero: DefaultRediscover).
func NewClusters(src fleet.Source, perCaller bool, refresh time.Duration) *Clusters {
	if refresh <= 0 {
		refresh = DefaultRediscover
	}
	return &Clusters{
		src:       src,
		perCaller: perCaller,
		ttl:       defaultCallerSetTTL,
		refresh:   refresh,
		now:       time.Now,
		sets:      map[string]*clusterSet{},
		lastMiss:  map[string]time.Time{},
		configs:   map[string]*rest.Config{},
		inflight:  map[string]*pendingDiscovery{},
	}
}

// Describe names the fleet source.
func (c *Clusters) Describe() string { return c.src.Describe() }

// PerCaller reports Google-token mode, where each caller has a set.
func (c *Clusters) PerCaller() bool { return c.perCaller }

// clusterSet is one discovery, named.
type clusterSet struct {
	entries []clusterEntry
	byName  map[string]int // every accepted name → entries index
	scopes  []fleet.ScopeResult
	at      time.Time
}

type clusterEntry struct {
	ref cloud.ClusterRef
	// name is the one to use in this set; canonical is the stable one.
	name      string
	canonical string
	// storeName is the name a multi-cluster watch gives this cluster's
	// store file (its bare name), or "" when that name repeats in the
	// set: watch watches neither of two same-named clusters, so neither
	// has a file, and guessing would read another cluster's rows.
	storeName string
	// setSize is how many clusters the set holds, for the per-cluster
	// concurrency limit.
	setSize int
}

// Start discovers the shared set, so a server with no reachable cluster
// fails at startup rather than on its first call. In Google-token mode
// there is no caller yet, and nothing to do.
func (c *Clusters) Start(ctx context.Context) ([]fleet.ScopeResult, int, error) {
	if c.perCaller {
		return nil, 0, nil
	}
	set, err := c.discover(ctx)
	if err != nil {
		return nil, 0, err
	}
	if len(set.entries) == 0 {
		return set.scopes, 0, fmt.Errorf("multi-cluster: no clusters found in %s", c.src.Describe())
	}
	c.mu.Lock()
	c.sets[""] = set
	c.mu.Unlock()
	return set.scopes, len(set.entries), nil
}

// set returns the caller's set (the shared one outside Google-token
// mode), discovering it when there is none or it is stale. A failed
// refresh keeps serving the previous set: one bad discovery should not
// take every cluster away.
func (c *Clusters) set(ctx context.Context, caller string) (*clusterSet, error) {
	key := c.key(caller)
	c.mu.Lock()
	cur := c.sets[key]
	c.mu.Unlock()
	if cur != nil && c.now().Sub(cur.at) < c.maxAge() {
		return cur, nil
	}
	fresh, err := c.rediscover(ctx, key)
	if err != nil {
		if cur != nil {
			return cur, nil
		}
		return nil, err
	}
	return fresh, nil
}

// rediscover discovers key's set and stores it, joining a discovery
// already running for the same key instead of starting another. The
// key is the caller in Google-token mode, so a joined discovery ran
// with the same caller's identity.
func (c *Clusters) rediscover(ctx context.Context, key string) (*clusterSet, error) {
	c.mu.Lock()
	if p, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-p.done:
			return p.set, p.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p := &pendingDiscovery{done: make(chan struct{})}
	c.inflight[key] = p
	c.mu.Unlock()

	p.set, p.err = c.discover(ctx)

	c.mu.Lock()
	delete(c.inflight, key)
	if p.err == nil {
		c.sets[key] = p.set
		c.pruneLocked()
	}
	c.mu.Unlock()
	close(p.done)
	return p.set, p.err
}

// pruneLocked drops callers' sets that have gone stale once there are
// many: a set is cheap, but one per caller ever seen is unbounded.
func (c *Clusters) pruneLocked() {
	if len(c.sets) <= maxCallerSets {
		return
	}
	for k, s := range c.sets {
		if k != "" && c.now().Sub(s.at) >= c.maxAge() {
			delete(c.sets, k)
			delete(c.lastMiss, k)
		}
	}
}

func (c *Clusters) key(caller string) string {
	if c.perCaller {
		return caller
	}
	return ""
}

func (c *Clusters) maxAge() time.Duration {
	if c.perCaller {
		return c.ttl
	}
	return c.refresh
}

func (c *Clusters) discover(ctx context.Context) (*clusterSet, error) {
	res, err := c.src.Discover(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovering clusters (%s): %w", c.src.Describe(), err)
	}
	set := nameClusters(res.Clusters)
	set.scopes = res.Scopes
	set.at = c.now()
	return set, nil
}

// Resolve finds the cluster a call names, in the caller's set. An
// unknown name rediscovers once (at most once a minute per caller) in
// case the cluster is new, then fails with errUnknownCluster.
func (c *Clusters) Resolve(ctx context.Context, caller, name string) (clusterEntry, error) {
	set, err := c.set(ctx, caller)
	if err != nil {
		return clusterEntry{}, err
	}
	if i, ok := set.byName[name]; ok {
		return set.entries[i], nil
	}
	key := c.key(caller)
	c.mu.Lock()
	recent := c.now().Sub(c.lastMiss[key]) < missRediscoverEvery
	if !recent {
		c.lastMiss[key] = c.now()
	}
	c.mu.Unlock()
	if !recent {
		if fresh, err := c.rediscover(ctx, key); err == nil {
			if i, ok := fresh.byName[name]; ok {
				return fresh.entries[i], nil
			}
		}
	}
	return clusterEntry{}, fmt.Errorf("%w %q", errUnknownCluster, name)
}

// List returns the caller's set, filtered by a case-insensitive
// substring of any of a cluster's names, project or location.
func (c *Clusters) List(ctx context.Context, caller, filter string) ([]clusterEntry, []fleet.ScopeResult, error) {
	set, err := c.set(ctx, caller)
	if err != nil {
		return nil, nil, err
	}
	filter = strings.ToLower(strings.TrimSpace(filter))
	var out []clusterEntry
	for _, e := range set.entries {
		if filter == "" || strings.Contains(strings.ToLower(e.canonical+" "+e.name+" "+e.ref.Location), filter) {
			out = append(out, e)
		}
	}
	return out, set.scopes, nil
}

// Config returns the client config for a cluster. With the server's own
// identity it is built once per cluster and reused (its token source
// refreshes itself); in Google-token mode it is built per call, from the
// caller's token in ctx.
func (c *Clusters) Config(ctx context.Context, e clusterEntry) (*rest.Config, error) {
	if c.perCaller {
		// The caller's Google token goes only to a GKE control plane
		// (#699): never to an API server a kubeconfig or a hand-written
		// endpoint list happens to name.
		if !IsGKEEndpoint(e.ref.Endpoint) {
			return nil, fmt.Errorf("refusing to send your Google token to %q: in Google-token mode only GKE control-plane DNS endpoints (*.gke.goog) are reached", e.ref.Endpoint)
		}
		return c.src.RESTConfig(ctx, e.ref)
	}
	// Keyed by endpoint too: a cluster recreated under the same name has
	// a new one.
	key := e.canonical + "|" + e.ref.Endpoint
	c.mu.Lock()
	cfg := c.configs[key]
	c.mu.Unlock()
	if cfg != nil {
		return cfg, nil
	}
	// Detached from the call: the config is cached for every later call,
	// and an ADC token source keeps the context it was made with for
	// each refresh, so one made with this request's context fails every
	// refresh once the request ends.
	cfg, err := c.src.RESTConfig(context.WithoutCancel(ctx), e.ref)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.configs[key] = cfg
	c.mu.Unlock()
	return cfg, nil
}

// IsGKEEndpoint reports whether an endpoint (a host, or an https URL) is
// a GKE control plane's DNS endpoint.
func IsGKEEndpoint(endpoint string) bool {
	host := strings.TrimPrefix(endpoint, "https://")
	if i := strings.IndexAny(host, ":/"); i >= 0 {
		host = host[:i]
	}
	return strings.HasSuffix(host, ".gke.goog") && len(host) > len(".gke.goog")
}

// canonicalName is the name that never changes with the set:
// project/location/name, or the bare name when the cluster has no
// project (a kubeconfig fleet's, whose context names are unique).
func canonicalName(r cloud.ClusterRef) string {
	if r.Project == "" {
		return r.Name
	}
	return r.Project + "/" + r.Location + "/" + r.Name
}

// nameClusters names a set: each cluster's short name, and every name
// that resolves to it.
func nameClusters(refs []cloud.ClusterRef) *clusterSet {
	bare := map[string]int{}
	byProject := map[string]int{}
	for _, r := range refs {
		bare[r.Name]++
		byProject[r.Name+"@"+r.Project]++
	}
	set := &clusterSet{byName: map[string]int{}}
	for _, r := range refs {
		e := clusterEntry{ref: r, canonical: canonicalName(r), setSize: len(refs)}
		if bare[r.Name] == 1 {
			e.storeName = r.Name
		}
		switch {
		case bare[r.Name] == 1 || r.Project == "":
			e.name = r.Name
		case byProject[r.Name+"@"+r.Project] == 1:
			e.name = r.Name + "@" + r.Project
		default:
			e.name = r.Name + "@" + r.Project + "/" + r.Location
		}
		set.entries = append(set.entries, e)
	}
	slices.SortFunc(set.entries, func(a, b clusterEntry) int { return strings.Compare(a.name, b.name) })
	for i, e := range set.entries {
		set.byName[e.name] = i
		set.byName[e.canonical] = i
	}
	return set
}
