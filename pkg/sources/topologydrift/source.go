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
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the source's stable config- and schema-facing name.
const Name = "topology-drift"

// DefaultEligibilitySweepInterval bounds how often a broad node change — a
// node going NotReady, a taint appearing, allocatable shrinking — causes every
// subject to be re-evaluated.
//
// §6.3 calls this the cheap half of the node-event asymmetry, and the interval
// is what makes it cheap: a rolling node upgrade touches thousands of nodes,
// and enqueuing 20,000 subjects per node touched is the one way this source
// could take a cluster down. The narrow half — a node's topology labels
// actually moving — is applied immediately, because it changes counts.
const DefaultEligibilitySweepInterval = 5 * time.Minute

// DefaultAlertInterval is how often §8.2's machine is advanced over every
// scored subject.
//
// It is the resolution of every dwell timer in the subsystem — a 10-minute
// dwell measured on a 30-second tick fires somewhere in [10:00, 10:30) — and
// against a `for` measured in minutes that is noise. Going faster buys nothing:
// the verdicts it reads are only recomputed when a pod or node event arrives,
// so a 5-second tick would advance the machine over the same numbers six times
// and reach the same answer.
const DefaultAlertInterval = 30 * time.Second

// DefaultReconcileGrace bounds how long a persisted alert record waits for its
// subject to be scored again after a restart (§9.3 step 6).
//
// A record can only be reconciled against a verdict, and at startup there are
// none: the informers have synced but nothing has been scored. Most subjects
// are scored within one coalesce window of the first re-apply; this is the
// allowance for the tail, after which an unclaimed record is taken to belong to
// a workload that was deleted while we were down. Generous on purpose — the
// cost of waiting is one row in a table, and the cost of dropping too early is
// a dwell timer restarted for a subject that is still drifting.
const DefaultReconcileGrace = 5 * time.Minute

// DefaultReadySampleInterval is how often §7.6's ready-node series is
// sampled.
//
// Thirty seconds gives a fifteen-minute outage window thirty samples, which is
// more resolution than a test with a 50% threshold needs — the point of the
// cadence is not precision but that a sample exists *at all* between the last
// change and the outage, because the peak the outage is measured against has
// to be somewhere in the window. Anything up to a couple of minutes would work;
// this matches the alert tick so that the two passes stay roughly in step.
const DefaultReadySampleInterval = 30 * time.Second

// DefaultBaselineSampleInterval is how often §7.5's estimators are fed.
//
// A minute against a twelve-hour half-life is 720 samples per half-life,
// which is far more than the estimate needs — the EWMA converges on elapsed
// time, not on sample count, and `dt` comes from the clock precisely so that
// the cadence is free to be wrong. What the cadence actually buys is the
// 200-sample maturity gate: at one a minute a subject clears it in 3h20m,
// comfortably inside the 6h age gate, so age is the binding constraint and
// maturity means "we watched this for six hours" rather than "we sampled it
// often enough".
//
// Going faster would make the sample count the binding gate on a cluster
// that restarts often, which is the wrong gate: 200 samples in ten minutes
// is a very good estimate of ten minutes.
const DefaultBaselineSampleInterval = time.Minute

// DefaultBaselineFlushInterval is §9.2's batched baseline write.
//
// Thirty seconds, straight from the design: losing it costs thirty seconds of
// a twelve-hour half-life, which is not a durability promise anybody made.
const DefaultBaselineFlushInterval = 30 * time.Second

// DefaultTopologyKeys are the axes tracked when none are configured (§10.2).
var DefaultTopologyKeys = []leeway.TopologyKey{
	corev1.LabelTopologyZone,
	corev1.LabelTopologyRegion,
}

// Config configures the source. Zero-valued fields take the shipped defaults.
type Config struct {
	// TopologyKeys are the axes counted, in order. Empty takes
	// DefaultTopologyKeys.
	TopologyKeys []leeway.TopologyKey

	// CoalesceWindow, RolloutCoalesceWindow and MaxCoalesceDelay configure
	// §6.4's evaluation queue.
	CoalesceWindow        time.Duration
	RolloutCoalesceWindow time.Duration
	MaxCoalesceDelay      time.Duration

	// EligibilitySweepInterval bounds the broad-node-change sweep.
	EligibilitySweepInterval time.Duration

	// VerifyInterval and VerifyShards configure §6.5's verifier, taking
	// DefaultVerifyInterval and DefaultVerifyShards when zero.
	//
	// Deliberately not flags. There is no operational decision here to
	// delegate: the defaults put a full pass at one hour, and an operator who
	// lengthened that would be choosing to detect a counter bug more slowly in
	// exchange for nothing measurable. They exist so tests can run a pass
	// without waiting five minutes.
	VerifyInterval time.Duration
	VerifyShards   int

	// ClusterDefaultConstraints is the cluster's PodTopologySpread
	// defaultConstraints (FR-9), and it is a pointer on purpose: nil is "we
	// have never been told", an empty slice is "an operator asserts there are
	// none", and collapsing those two is the silent-assumption bug spike S4
	// exists to stop. See ClusterDefaultIntents.
	ClusterDefaultConstraints *[]corev1.TopologySpreadConstraint

	// PerDomainSeries exports the per-subject, per-domain counts for *every*
	// tracked subject, not only the drifting ones. See PerDomainGate for why
	// that is a deliberate opt-in and PerDomainSeriesMinDrift for what a
	// deployment gets without it.
	PerDomainSeries bool

	// PerDomainSeriesMinDrift is §8.4's cardinality floor on the per-domain
	// series. Zero takes DefaultPerDomainSeriesMinDrift; negative admits every
	// scored subject.
	PerDomainSeriesMinDrift float64

	// Thresholds are §7.4's scoring thresholds. Nil takes
	// leeway.DefaultThresholds.
	//
	// A pointer rather than a value, because these are five fields that must
	// be set together: a caller that wanted a stricter drift threshold and set
	// the struct's one field would otherwise get a MinReplicasForScoring of
	// zero along with it, and score every single-replica workload in the
	// cluster. Take DefaultThresholds and amend it.
	Thresholds *leeway.Thresholds

	// CapacityRatioTrigger is §7.2's max/min allocatable-CPU ratio above
	// which a subject that declared no weighting is apportioned by capacity
	// instead of equally. Zero takes leeway.CapacityWeightingRatioTrigger.
	//
	// Not folded into Thresholds, which are §7.4's scoring cut-offs: this one
	// changes the expectation a score is taken against rather than how big a
	// score has to be to count.
	CapacityRatioTrigger float64

	// NodeGroupLabelKeys is FR-3's ordered precedence list: the node label
	// keys a node group's name is read from, first match wins. Empty takes
	// DefaultNodeGroupLabelKeys.
	NodeGroupLabelKeys []string

	// MaxNodeGroups bounds how many node-group subjects are tracked. Zero
	// takes DefaultMaxNodeGroups; a negative value turns node-group subjects
	// off entirely.
	MaxNodeGroups int

	// Dwell is §8.2's timing. The zero value takes leeway.DefaultDwell, and a
	// partial one is completed by the machine itself — see leeway.Dwell.
	Dwell leeway.Dwell

	// Transient is §7.6's windows and multiplier. The zero value takes
	// leeway.DefaultTransientConfig, field by field.
	Transient leeway.TransientConfig

	// ReadySampleInterval is how often each domain's ready-node count is
	// sampled for §7.6's outage test. Zero takes
	// DefaultReadySampleInterval.
	ReadySampleInterval time.Duration

	// AlertInterval is how often the §8.2 machine is advanced. Zero takes
	// DefaultAlertInterval.
	AlertInterval time.Duration

	// ReconcileGrace bounds how long a persisted alert record waits for its
	// subject to be scored again after a restart. Zero takes
	// DefaultReconcileGrace.
	ReconcileGrace time.Duration

	// Cause tunes §8.5's attribution rules. The zero value takes
	// leeway.DefaultCauseConfig, field by field.
	Cause leeway.CauseConfig

	// Baselines tunes §7.5's estimator. The zero value takes
	// leeway.DefaultBaselineConfig, field by field, and every gate in it
	// fails closed — a zero MinSamples does not mean "mature immediately".
	Baselines leeway.BaselineConfig

	// BaselineSampleInterval is how often each scored subject's placement is
	// fed to its estimator. Zero takes DefaultBaselineSampleInterval.
	//
	// On a timer for the same reason the dwell is, only more so: a half-life
	// is a statement about elapsed time, and sampling on the evaluation path
	// would make a subject whose pods churn reach maturity in minutes while a
	// quiet one that is equally wrong took the full six hours.
	BaselineSampleInterval time.Duration

	// BaselineFlushInterval is §9.2's batched write. Zero takes
	// DefaultBaselineFlushInterval.
	BaselineFlushInterval time.Duration

	// LearnBaselines is §7.5's off switch, and it defaults ON — the zero
	// value learns, which is why this is not spelled DisableBaselines.
	//
	// Learning is separate from *using* what was learned (TierCSignals).
	// A deployment that never wants a Tier C signal still wants the
	// baselines, because lookout_leeway_baseline_mature is how an operator
	// finds out whether turning them on would be quiet.
	LearnBaselines *bool

	// TierCSignals is §8.3's policy opt-in: Tier C findings are metrics-only
	// unless it is set. It defaults off, and that default is what makes the
	// source defensible as a default-on one — the overwhelming majority of
	// subjects that breach anything breach the distributional rule with no
	// declared intent behind it.
	TierCSignals bool

	// Cluster names this cluster in the persisted alert state, so one store
	// shared by a fleet keeps the episodes apart.
	Cluster string

	// Meter is where the §8.4 instruments are declared. Nil means no-op, so
	// the source is usable in tests and in a process with no telemetry.
	Meter metric.Meter
}

func (c Config) normalize() Config {
	if len(c.TopologyKeys) == 0 {
		c.TopologyKeys = DefaultTopologyKeys
	}
	if c.EligibilitySweepInterval <= 0 {
		c.EligibilitySweepInterval = DefaultEligibilitySweepInterval
	}
	if c.PerDomainSeriesMinDrift == 0 {
		c.PerDomainSeriesMinDrift = DefaultPerDomainSeriesMinDrift
	}
	if c.Thresholds == nil {
		t := leeway.DefaultThresholds()
		c.Thresholds = &t
	}
	if c.CapacityRatioTrigger <= 0 {
		c.CapacityRatioTrigger = leeway.CapacityWeightingRatioTrigger
	}
	if len(c.NodeGroupLabelKeys) == 0 {
		c.NodeGroupLabelKeys = DefaultNodeGroupLabelKeys
	}
	// Only the zero value defaults. A negative bound is the off switch and has
	// to survive normalization to mean anything.
	if c.MaxNodeGroups == 0 {
		c.MaxNodeGroups = DefaultMaxNodeGroups
	}
	if c.AlertInterval <= 0 {
		c.AlertInterval = DefaultAlertInterval
	}
	if c.ReconcileGrace <= 0 {
		c.ReconcileGrace = DefaultReconcileGrace
	}
	if c.ReadySampleInterval <= 0 {
		c.ReadySampleInterval = DefaultReadySampleInterval
	}
	if c.BaselineSampleInterval <= 0 {
		c.BaselineSampleInterval = DefaultBaselineSampleInterval
	}
	if c.BaselineFlushInterval <= 0 {
		c.BaselineFlushInterval = DefaultBaselineFlushInterval
	}
	c.Transient = c.Transient.Normalized()
	c.Cause = c.Cause.Normalized()
	c.Baselines = c.Baselines.Normalized()
	return c
}

// learnBaselines is LearnBaselines with its default applied: nil is on.
func (c Config) learnBaselines() bool { return c.LearnBaselines == nil || *c.LearnBaselines }

// readyRetention is how much ready-count history the two readers of the series
// need between them, which is the longer of their windows.
//
// §7.6's outage test wants twice the window it scans — twice rather than
// exactly, so that a sample taken just before the window opened is still there
// when it does, and a peak is never lost to the boundary between two ticks.
// §8.5's attribution wants its whole two-hour cause window, because the peak a
// finding reports a domain as having fallen from is read from the same samples.
//
// Keeping one series rather than two is what stops the two from disagreeing
// about when a zone lost its nodes. Retaining more than the outage test scans
// costs it nothing: it bounds its own scan and never reads the older samples.
func (c Config) readyRetention() time.Duration {
	return max(2*c.Transient.Normalized().OutageWindow, c.Cause.Normalized().Window)
}

// perDomainGate is §8.4's gate as the metrics layer takes it. A negative floor
// is the configured way to say "every scored subject", which the gate spells
// as a zero floor — drift is never negative, so passing it through unchanged
// would work by accident rather than by contract.
func (c Config) perDomainGate() PerDomainGate {
	g := PerDomainGate{All: c.PerDomainSeries, MinDrift: c.PerDomainSeriesMinDrift}
	if g.MinDrift < 0 {
		g.MinDrift = 0
	}
	return g
}

// Source implements sources.Source for the topology-drift row of §3.
//
// It maintains the §6.2 counters against live traffic, infers intent (§5.1),
// scores every subject on every eligible axis (§7.1–§7.4), withholds judgement
// while the cluster is mid-move (§7.6), runs the §8.2 dwell machine over the
// verdicts, exports the lot as §8.4 metrics — and, from Phase 4's last
// increment, emits a §8.5 finding for each episode that crosses its dwell.
//
// **What it emits is narrow by default, on purpose.** §8.3 routes Tier C to
// metrics unless a policy opts in, and Tier C is the overwhelming majority of
// subjects that breach anything: a workload with no declared or inferred intent
// is measured against an apportionment nobody promised. So the default-on
// deployment adds approximately zero agent sessions, which is the property that
// makes shipping this default-on defensible in the first place.
type Source struct {
	client kubernetes.Interface
	cfg    Config

	// factory, when set via WithFactory, is the externally owned shared
	// informer factory (§6.1: leeway adds no informers of its own to a
	// process that already watches pods and nodes).
	factory informers.SharedInformerFactory

	// nodeFactory, when set via WithNodeFactory, is where the Node informer
	// comes from. Unset means factory.
	nodeFactory informers.SharedInformerFactory

	// dyn, when set via WithDynamic, is the dynamic client the optional
	// LeewayPolicy informers are built on. Unset means no policy watch at all,
	// which is the same observable state as a cluster without the CRDs.
	dyn dynamic.Interface

	// rollouts, when set via WithRolloutOracle, answers §7.6's rollout row.
	// Nil means the row is unanswered — see RolloutOracle.
	rollouts RolloutOracle

	inv   *Inventory
	state *State
	queue *coalescer
	// scales is §7.6's recent-scale row. Always non-nil; it is simply empty
	// when the apps informers are not running.
	scales  *scaleLog
	metrics *instruments
	verify  *Verifier
	alerts  *Alerts
	// baselines is §7.5's live estimators. Always non-nil; a deployment with
	// LearnBaselines off simply never feeds it.
	baselines *baselineLog
	// store persists the §8.2 dwell timers. Nil means no persistence, which is
	// the no-`--store` deployment: the machine still runs, it just starts every
	// dwell from zero at each restart.
	store AlertStore
	// baselineStore is the same store when it can also carry §7.5's learned
	// normals, and nil when it cannot. Separate from store rather than a type
	// assertion at each use, so the capability is decided once, where the
	// store arrives.
	baselineStore BaselineStore
	// policies holds the LeewayPolicy objects the informers deliver. Always
	// non-nil so that policyFor needs no second nil check; an empty store is
	// the normal deployment.
	policies *PolicyStore

	mu sync.Mutex
	// emit is the pipeline callback, set at the top of Run. Nil outside Run,
	// which is the state every unit test that never calls Run is in — and the
	// reason emitFinding checks rather than assumes.
	emit func(sources.Signal)
	// armed flips true after every informer cache syncs and the initial
	// re-apply has run.
	armed bool
	// rollingOut is the last snapshot the rollout oracle gave us. Nil until
	// the first cluster sample, which reads as "nothing is rolling out".
	rollingOut map[leeway.SubjectRef]bool
	// baselineGraceUntil holds the baseline reap off until the first re-apply
	// has had time to score the subjects whose rows we just loaded. Zero
	// outside Run, which is every test that never starts one — and reaping
	// from the first tick is right there, because nothing was loaded either.
	baselineGraceUntil time.Time
	// replicaSets resolves the pod → ReplicaSet → Deployment hop. Set in Run.
	replicaSets appslisters.ReplicaSetLister
	// pods is the verifier's view of the cache. Set in Run.
	pods corelisters.PodLister
	// claims and volumes resolve FR-8's pod → PVC → PV hop. Set in Run.
	claims  corelisters.PersistentVolumeClaimLister
	volumes corelisters.PersistentVolumeLister
	// sweepPending records that some node's eligibility moved since the last
	// sweep. See DefaultEligibilitySweepInterval.
	sweepPending bool
	// warnedTies deduplicates the ambiguous-policy warning. Keyed by the set of
	// competing policies rather than by subject: the operator fixes a pair of
	// policies, not each of the subjects they both match, and keying it this
	// way bounds the map by the number of policies (tens) instead of by the
	// number of subjects (tens of thousands).
	warnedTies map[string]bool

	// nodeGroupsFound is how many distinct node groups the last pass resolved,
	// before MaxNodeGroups was applied. It is exported as its own gauge
	// precisely so that it can disagree with subjects_tracked{NodeGroup}:
	// "found 4,000, tracking 0" is the reading a misconfigured precedence list
	// produces, and it is unrecoverable from the tracked count alone.
	nodeGroupsFound int64
	// warnedNodeGroupBound records that the over-the-bound warning has been
	// logged. The pass runs every 30 s and the condition is a configuration
	// mistake, so the second line onwards would be noise.
	warnedNodeGroupBound bool

	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// logger returns the configured log sink.
func (s *Source) logger() func(string, ...any) {
	if s.logf != nil {
		return s.logf
	}
	return log.Printf
}

// New constructs the source.
func New(client kubernetes.Interface, cfg Config) *Source {
	cfg = cfg.normalize()
	s := &Source{client: client, cfg: cfg}
	s.policies = NewPolicyStore()
	s.warnedTies = map[string]bool{}
	s.scales = newScaleLog()
	s.inv = NewInventory(cfg.TopologyKeys)
	s.queue = newCoalescer(CoalesceOptions{
		Window:        cfg.CoalesceWindow,
		RolloutWindow: cfg.RolloutCoalesceWindow,
		MaxDelay:      cfg.MaxCoalesceDelay,
		Evaluate:      s.evaluate,
	})
	s.state = NewState(StateOptions{
		Inventory: s.inv,
		Owners:    s.ownerOf,
		Enqueue:   s.queue.Enqueue,
		Pinned:    VolumePins(s.boundVolume, cfg.TopologyKeys),
	})
	s.alerts = NewAlerts(cfg.Dwell)
	s.baselines = newBaselineLog()
	s.verify = NewVerifier(VerifyOptions{
		State:      s.state,
		Pods:       s.cachedPods,
		Interval:   cfg.VerifyInterval,
		Shards:     cfg.VerifyShards,
		OnMismatch: s.onMismatch,
		Logf:       func(f string, a ...any) { s.logger()(f, a...) },
	})
	return s
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the node inventory of §6.2 is built from a
// cluster-wide node watch, so a namespace-tier deployment gets the loud §11
// startup failure rather than a distribution over one visible domain.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// WithFactory directs Run to register its handlers on an externally owned
// shared factory. Call before Run; nil is ignored.
func (s *Source) WithFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.factory = f
	}
}

// WithNodeFactory directs Run to take the Node informer from a different
// factory than the namespaced ones. Call before Run; nil is ignored, and
// unset means "the same factory as everything else", which is the caller's
// normal case.
//
// It exists because a namespace deny list is applied as a field selector on the
// factory, and `metadata.namespace` is not selectable on a cluster-scoped
// resource — the API server rejects the node LIST outright rather than ignoring
// the term. A caller that scopes its namespaced watches therefore has to hand
// the node watch a factory that carries no selector.
func (s *Source) WithNodeFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.nodeFactory = f
	}
}

// WithDynamic gives Run a dynamic client to build the optional LeewayPolicy
// informers on. Call before Run; nil is ignored.
//
// Separate from New's signature, unlike the gateway source which takes its
// dynamic client as a constructor parameter, because there the CRD is the
// entire subject and a source without it has nothing to watch. Here the policy
// watch is an optional override on a source that works fully without it, so a
// caller that does not care — every unit test, and any embedder that has no
// dynamic client — should not have to name it.
func (s *Source) WithDynamic(dyn dynamic.Interface) {
	if dyn != nil {
		s.dyn = dyn
	}
}

// RolloutOracle answers §7.6's rollout row: which subjects are partway
// through a revision change right now. It is asked once per evaluation pass
// for the whole cluster rather than once per subject, because a pass covers
// tens of thousands of subjects and the mid-rollout set is a handful.
//
// A function rather than an interface, and it deals in leeway's own
// SubjectRef, so that neither this package nor the rollout source has to
// import the other: the adapter belongs at the composition root that already
// knows about both. Unset means the row goes unanswered, which is the state
// of any deployment that turned the rollout source off.
type RolloutOracle func() []leeway.SubjectRef

// WithRolloutOracle sets the callback that answers §7.6's rollout row. Call
// before Run; nil is ignored.
func (s *Source) WithRolloutOracle(fn RolloutOracle) {
	if fn != nil {
		s.rollouts = fn
	}
}

// AlertStore is §9.1's persistence seam, satisfied by *store.Store.
//
// An interface rather than the concrete type for the usual reason plus one
// specific to it: the store is optional (`--store` is opt-in), and a source
// that imported the package would still have to answer what it does without
// one. Three methods, named after what they are for, is a cheaper contract to
// state than "a *store.Store, or nil, and remember that two of its methods
// return an error on nil while the third does not".
type AlertStore interface {
	// LeewayAlertStates returns one cluster's persisted episodes.
	LeewayAlertStates(ctx context.Context, cluster string) ([]leeway.AlertRecord, error)
	// PutLeewayAlertState writes one episode.
	PutLeewayAlertState(ctx context.Context, rec leeway.AlertRecord) error
	// DeleteLeewayAlertState ends one episode.
	DeleteLeewayAlertState(ctx context.Context, cluster, subjectKey, topologyKey string) error
}

// BaselineStore is §9.1's second persistence seam, also satisfied by
// *store.Store.
//
// Separate from AlertStore because the two are written on deliberately
// opposite policies (§9.2) and it is worth being able to read one contract
// without the other: this one is a batch write and a bulk read, which is what
// a twelve-hour half-life is allowed to be, while an alert transition is
// written one at a time and synchronously because losing it loses a promise.
//
// Optional in the same way: a store that does not implement it — an older
// embedder's, or a fake in a test that only cares about dwells — simply
// learns baselines in memory and forgets them on restart, which costs six
// hours of relearning and no monitoring at all.
type BaselineStore interface {
	// LeewayBaselines returns one cluster's persisted baselines.
	LeewayBaselines(ctx context.Context, cluster string) ([]leeway.BaselineRecord, error)
	// PutLeewayBaselines writes a batch in one transaction.
	PutLeewayBaselines(ctx context.Context, recs []leeway.BaselineRecord) error
	// DeleteLeewayBaseline forgets one subject-axis's learned normal.
	DeleteLeewayBaseline(ctx context.Context, cluster, subjectKey, topologyKey string) error
}

// WithStore gives the source somewhere to persist its dwell timers, and — if
// the store also satisfies BaselineStore — its learned baselines.
//
// Optional, and running without one is a supported deployment rather than a
// degraded mode — it is what `watch` does with no `--store`. §9.2's rule is
// that a missing history means one lost dwell, never no monitoring, so the
// machine runs either way and a restart simply starts every episode's clock
// again.
func (s *Source) WithStore(st AlertStore, cluster string) {
	if st == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = st
	s.baselineStore, _ = st.(BaselineStore)
	s.cfg.Cluster = cluster
}

// warnAmbiguous logs a policy tie once per competing set. Never called under
// s.mu by its caller; it takes the lock itself.
func (s *Source) warnAmbiguous(sub leeway.SubjectRef, m PolicyMatch) {
	tie := strings.Join(m.Competing, ",")
	s.mu.Lock()
	seen := s.warnedTies[tie]
	s.warnedTies[tie] = true
	s.mu.Unlock()
	if seen {
		return
	}
	s.logger()("topologydrift: %d policies match the same subject (e.g. %s) and there is no defensible ordering between their selectors — applying %q; narrow one of %s to make this deterministic",
		len(m.Competing), sub, m.Policy.ref(), tie)
}

// WithMeter directs Run to declare its §8.4 instruments against an externally
// owned meter. Call before Run; nil is ignored.
//
// Separate from Config.Meter for the same reason WithFactory is separate from
// the client: the meter belongs to the process that serves the scrape endpoint,
// not to the configuration an operator writes, and the sentinel builds it one
// layer above the code that decides which sources to construct.
func (s *Source) WithMeter(m metric.Meter) {
	if m != nil {
		s.cfg.Meter = m
	}
}

// RequiredAccess implements sources.AccessDeclarer (§11).
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, r := range []struct{ group, resource string }{
		{"", "pods"},
		{"", "nodes"},
		{"apps", "replicasets"},
		// §7.6's recent-scale row reads spec.replicas. Declared rather than
		// treated as optional because §11's check is a coverage contract and a
		// missing grant here is a transient row that silently never fires —
		// exactly the kind of quiet degradation the contract exists to catch.
		// Both are already granted to the sentinel for the rollout source.
		{"apps", "deployments"},
		{"apps", "statefulsets"},
		// FR-8: a pod bound to a volume that cannot follow it is drift nobody
		// can act on, and saying so needs the claim's binding and the volume's
		// node affinity.
		{"", "persistentvolumeclaims"},
		{"", "persistentvolumes"},
	} {
		for _, verb := range []string{"list", "watch"} {
			reqs = append(reqs, sources.Requirement{Group: r.group, Resource: r.resource, Verb: verb})
		}
	}
	// FR-10's policies are deliberately absent. §11's access check is a
	// coverage contract — it fails startup when a grant the source needs to do
	// its job is missing — and the source does its whole job without ever
	// reading a policy. Declaring them would turn an optional override into a
	// startup prerequisite on every cluster, including the ones that never
	// install the CRD, which is exactly the coupling keeping the CRD out of
	// deploy/kustomization.yaml avoids. A missing grant is instead handled
	// where it happens: the discovery gate in Run logs and continues.
	return reqs
}

// HasSynced implements sources.SyncReporter. Reporting unsynced until the
// initial re-apply has run is the honest answer: until then the counters are a
// partial cluster, and a reader cannot tell a half-built distribution from a
// genuinely lopsided one.
func (s *Source) HasSynced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed
}

// Verifier exposes §6.5's auditor, for tests.
func (s *Source) Verifier() *Verifier { return s.verify }

// cachedPods implements PodSnapshot against the pod informer's cache. Before
// Run has a lister it reports an error rather than an empty cluster: a verifier
// handed zero pods would conclude that every tracked subject is a leak and
// "repair" the entire state to nothing.
func (s *Source) cachedPods() ([]*corev1.Pod, error) {
	s.mu.Lock()
	lister := s.pods
	s.mu.Unlock()
	if lister == nil {
		return nil, fmt.Errorf("topologydrift: pod cache not ready")
	}
	return lister.List(labels.Everything())
}

// onMismatch records the §6.5 SLI. Nothing here is attached to a request, so
// the background context is the honest one.
func (s *Source) onMismatch(kind leeway.SubjectKind) {
	s.metrics.recordMismatch(context.Background(), kind)
}

// State exposes the counters for the §6.5 verifier and for tests.
func (s *Source) State() *State { return s.state }

// Inventory exposes the node-side state for the §6.5 verifier and for tests.
func (s *Source) Inventory() *Inventory { return s.inv }

// ownerOf implements OwnerLookup against the ReplicaSet cache. Before Run
// populates the lister — and for any kind other than ReplicaSet, which the
// resolver never asks about — it reports no owner, so the pod goes uncounted
// and is retried on its next event.
func (s *Source) ownerOf(namespace, kind, name string) (*metav1.OwnerReference, bool) {
	if kind != "ReplicaSet" {
		return nil, false
	}
	s.mu.Lock()
	lister := s.replicaSets
	s.mu.Unlock()
	if lister == nil {
		return nil, false
	}
	rs, err := lister.ReplicaSets(namespace).Get(name)
	if err != nil {
		return nil, false
	}
	return controllerOf(rs.OwnerReferences), true
}

// boundVolume implements VolumeLookup against the PVC and PV caches.
//
// Before Run populates the listers it reports nothing bound, which reads as
// unpinned — the same "retried on the next event" shape as ownerOf, and for
// FR-8 a benign one: the first thing Run does after the caches sync is reapply,
// which re-counts every pod with the listers in place.
func (s *Source) boundVolume(namespace, claim string) (*corev1.PersistentVolume, bool) {
	s.mu.Lock()
	claims, volumes := s.claims, s.volumes
	s.mu.Unlock()
	if claims == nil || volumes == nil {
		return nil, false
	}
	pvc, err := claims.PersistentVolumeClaims(namespace).Get(claim)
	if err != nil {
		return nil, false
	}
	// An unbound claim has no VolumeName, and a claim that is Pending because
	// its class provisions on first consumer will get one the moment the pod is
	// scheduled — which is also the moment the pod starts occupying a domain.
	if pvc.Spec.VolumeName == "" {
		return nil, false
	}
	pv, err := volumes.Get(pvc.Spec.VolumeName)
	if err != nil {
		return nil, false
	}
	return pv, true
}

// policyCRDsServed asks discovery which of the two policy kinds the cluster
// serves. A discovery error — which is what an uninstalled CRD looks like,
// since the whole group is absent — reads as "neither", because that is the
// same observable state and the same correct behaviour.
func (s *Source) policyCRDsServed() (namespaced, clusterScoped bool) {
	resources, err := s.client.Discovery().ServerResourcesForGroupVersion(policyGV.String())
	if err != nil || resources == nil {
		return false, false
	}
	for _, r := range resources.APIResources {
		switch r.Name {
		case policyGVR.Resource:
			namespaced = true
		case clusterPolicyGVR.Resource:
			clusterScoped = true
		}
	}
	return namespaced, clusterScoped
}

// startPolicyWatch registers informers for whichever policy CRDs the cluster
// serves and returns their sync barriers, or nothing at all.
//
// **Absent CRDs log and continue.** This is the opposite of the gateway
// source, which returns an error when its CRD is missing, and the difference is
// which way the silence points. There, the source was named explicitly or by a
// discovery gate that would have skipped it, so an empty watch would be a
// coverage lie. Here the source is default-on and complete without any policy:
// refusing to start would make an optional override a prerequisite for a
// cluster-wide watcher, and the operator who installs the CRD later gets it at
// the next restart, which the manifest says out loud.
//
// The gate is evaluated once. Watching for the CRD itself to appear would mean
// a second watch on apiextensions — a grant we would then need everywhere — to
// save a restart on a one-off installation step.
func (s *Source) startPolicyWatch(ctx context.Context) ([]cache.InformerSynced, error) {
	if s.dyn == nil {
		return nil, nil
	}
	namespaced, clusterScoped := s.policyCRDsServed()
	if !namespaced && !clusterScoped {
		s.logger()("topologydrift: %s not installed — placement intent is inferred only (apply deploy/crds/leewaypolicies.yaml and restart to declare it)", policyGV)
		return nil, nil
	}

	factory := dynamicinformer.NewDynamicSharedInformerFactory(s.dyn, 0)
	var synced []cache.InformerSynced
	for _, w := range []struct {
		gvr           schema.GroupVersionResource
		serve         bool
		clusterScoped bool
	}{
		{policyGVR, namespaced, false},
		{clusterPolicyGVR, clusterScoped, true},
	} {
		if !w.serve {
			s.logger()("topologydrift: %s not served — %s policies ignored", w.gvr, w.gvr.Resource)
			continue
		}
		scoped := w.clusterScoped
		inf := factory.ForResource(w.gvr).Informer()
		h, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { s.onPolicy(obj, scoped) },
			UpdateFunc: func(_, obj any) { s.onPolicy(obj, scoped) },
			DeleteFunc: func(obj any) { s.onPolicyDelete(obj) },
		})
		if err != nil {
			return nil, fmt.Errorf("topologydrift: register %s handler: %w", w.gvr.Resource, err)
		}
		synced = append(synced, h.HasSynced)
	}
	factory.Start(ctx.Done())
	return synced, nil
}

// onPolicy decodes and stores one policy object.
//
// A policy that does not decode is dropped with a log line and the previous
// version of it, if any, is left in place. Replacing it with nothing would mean
// a typo in one field silently reverts a subject to inferred intent — the
// operator sees their policy stop applying and nothing tells them why. The
// structural schema rejects most of this at admission; what reaches here is
// what a schema cannot express, such as an all-zero expectedDistribution.
func (s *Source) onPolicy(obj any, clusterScoped bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	p, err := DecodePolicy(u, clusterScoped)
	if err != nil {
		s.logger()("topologydrift: ignoring %s %s/%s: %v", u.GetKind(), u.GetNamespace(), u.GetName(), err)
		return
	}
	s.policies.Upsert(p)
}

// onPolicyDelete removes a policy, resolving the tombstone the informer
// delivers when it missed the delete itself.
func (s *Source) onPolicyDelete(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	s.policies.Delete(u.GetNamespace(), u.GetName())
}

// evaluate is the coalesced per-subject callback.
//
// It resolves intent and eligibility, scores the subject on every axis and
// stores both results. It does not touch the §8.2 machine and it emits
// nothing: the machine runs on its own timer (see Run) so that a dwell
// measures how long a condition held rather than how often the subject's pods
// churned, and it is the alert pass — not this one — that emits.
func (s *Source) evaluate(ctx context.Context, sub leeway.SubjectRef) {
	start := time.Now()
	defer func() { s.metrics.recordEvaluation(ctx, sub.Kind, time.Since(start)) }()

	pod := s.subjectPod(sub)
	if pod == nil {
		// No pod to read means no evidence, and no evidence is not the same as
		// no intent: clearing what the last evaluation resolved would drop a
		// subject's intent_info every time its pods turned over faster than the
		// cache. The stale answer is the better one, and it is bounded — the
		// subject is forgotten outright once its last pod stops counting.
		return
	}
	res := Resolve(pod, s.inv, ResolveConfig{
		ClusterDefaults:      s.cfg.ClusterDefaultConstraints,
		Policy:               s.policyFor(sub, pod),
		Baselines:            s.baselineIntents(sub, start),
		CapacityRatioTrigger: s.cfg.CapacityRatioTrigger,
	})
	s.state.SetIntents(sub, res.Intents)

	// Snapshot and then score outside the index lock. Scoring is O(domains)
	// arithmetic and holding the lock across it would put every pod event in
	// the cluster behind whichever subject the queue happened to reach.
	s.state.SetEvaluations(sub, ScoreSubject(res, s.state.Snapshot(sub), *s.cfg.Thresholds, s.suppression(sub, start)))
}

// evaluateNodeGroups re-derives every node-group subject from the inventory
// and scores it (FR-3).
//
// It runs on a timer rather than through the §6.4 coalescer, which is the
// shape difference between this pass and evaluate: a node group has no
// representative pod to read intent from, no owner chain to resolve and no
// per-subject event to coalesce. What it has is the node inventory, and the
// cheapest correct thing to do with a small, already-indexed collection is to
// walk it whole. One pass is O(nodes), against O(pods) for the pod side.
//
// Three things about the scoring are decided here and nowhere else.
//
// **The intent is nil, deliberately.** A node group declared nothing that this
// process can read — FR-3's non-goal rules out the MIG and ASG APIs, so all
// leeway has is labels — and §8.1 reads a non-nil intent as the difference
// between Tier C and Tier B. Handing these subjects a synthetic intent to
// carry the weighting would quietly promote every pool in the estate to a
// tier that emits signals by default. The weighting travels as
// EligibilityOptions.UndeclaredWeighting instead, which is what that field is
// for.
//
// **Eligibility is cluster-wide.** A pool's configured zones are not in the
// labels, so the honest comparison is against the domains the cluster has:
// the finding says "this pool sits in one of the three zones this cluster
// spans", which is true, useful, and the only statement the available
// evidence supports. A deliberately zonal pool therefore reads as drifting —
// which is the other reason these subjects must stay Tier C, and why the
// precedence list is configurable down to nothing.
//
// **The weighting is Equal, and §7.2 is amended to say so.** It named
// AllocatableCPU as the default for node-group subjects; working the exit
// criterion through showed that backwards. Capacity weighting apportions
// objects over the cluster's allocatable CPU per domain, and a node group's
// objects *are* that capacity, so the expectation contains the thing it is
// meant to predict. Three zones of two nodes each plus a six-node pool wholly
// in zone-a weights [32 8 8], which apportions the lopsided pool [4 1 1] —
// partly excusing it — and apportions the *evenly spread* pool [4 1 1] as
// well, reporting it as drifting because a different pool is lopsided. Equal
// is the only expectation for a node group that is not circular, and it is
// pinned rather than defaulted: §7.2's ratio trigger would reintroduce exactly
// the same contamination on any cluster with unequal zones.
func (s *Source) evaluateNodeGroups(now time.Time) {
	if s.cfg.MaxNodeGroups < 0 {
		return
	}
	found := s.inv.NodeGroups(s.cfg.NodeGroupLabelKeys)
	s.mu.Lock()
	s.nodeGroupsFound = int64(len(found))
	warned := s.warnedNodeGroupBound
	over := len(found) > s.cfg.MaxNodeGroups
	s.warnedNodeGroupBound = over
	s.mu.Unlock()

	if over {
		// All of them, not the first MaxNodeGroups of them. Truncating would
		// pick an arbitrary subset by map order — a different subset every
		// pass — and score a cluster nobody configured. Dropping the lot
		// leaves node_groups_discovered as the one honest number, which is the
		// reading that sends somebody to the precedence list.
		s.state.SetGroups(nil)
		if !warned {
			s.logger()("topology-drift: %d node groups resolved from %v, over the --topology-max-node-groups bound of %d — tracking none of them. "+
				"This is usually a per-node label in the precedence list; lookout_leeway_node_groups_discovered keeps reporting the count",
				len(found), s.cfg.NodeGroupLabelKeys, s.cfg.MaxNodeGroups)
		}
		return
	}

	// One eligibility per axis for the whole pass. Every node group shares it
	// — the constraint set is empty, so the eligible domains are the cluster's
	// — and computing it per group would walk every node once per pool.
	eligible := make(map[leeway.TopologyKey]leeway.Eligibility, len(s.inv.Keys()))
	for _, key := range s.inv.Keys() {
		opts := leeway.DefaultEligibilityOptions()
		opts.CapacityRatioTrigger = s.cfg.CapacityRatioTrigger
		opts.UndeclaredWeighting = leeway.WeightEqual
		eligible[key] = leeway.EligibleDomains(s.inv.NodeViews(key, Constraints{}), opts)
	}

	groups := make(map[leeway.SubjectRef]map[leeway.TopologyKey]*leeway.Distribution, len(found))
	for name, byKey := range found {
		groups[leeway.SubjectRef{Kind: leeway.SubjectNodeGroup, Name: name}] = byKey
	}
	s.state.SetGroups(groups)

	res := Resolution{Eligible: eligible, Intents: map[leeway.TopologyKey]*leeway.Intent{}}
	for sub := range groups {
		// Snapshot rather than the map we just handed over: SetGroups adopts
		// the distributions, so reading them back through the lock is what
		// keeps this pass from racing the scrape callback walking the same
		// pointers.
		s.state.SetEvaluations(sub, ScoreSubject(res, s.state.Snapshot(sub), *s.cfg.Thresholds, s.nodeGroupSuppression(now)))
	}
}

// nodeGroupSuppression is §7.6 for a node group.
//
// Two of the five rows are answerable and three are not, which is worth
// stating rather than leaving to be inferred from a struct literal. Cluster
// warmup applies — an inventory that has not synced reports pools in the
// wrong places, the same as it reports pods there. Domain outage applies, and
// matters most here: a zone whose nodes have all gone NotReady is a zone
// every pool just emptied, and one domain finding beats one per pool. The
// drain row does not, because a cordoned node is still in its group and still
// counted; the rollout and recent-scale rows do not, because neither a
// Deployment rollout nor a replica change is an event in a node group's life.
// A pool being resized looks like a rollout and is not covered — that is
// §7.6's gap for FR-3, and the dwell is what absorbs it.
func (s *Source) nodeGroupSuppression(now time.Time) Suppressor {
	warming := !s.HasSynced()
	return func(key leeway.TopologyKey, eligible leeway.Eligibility) leeway.Suppression {
		t := leeway.Transients{
			Warming:      warming,
			DomainOutage: s.domainOutage(key, eligible.Domains, now),
		}
		return t.Classify(now, s.cfg.Transient)
	}
}

// suppression returns §7.6's Suppressor for one evaluation, closing over the
// instant the evaluation started so that every axis of one subject is judged
// against the same clock.
//
// Suppression is decided here, at scoring time, and then kept on the
// Evaluation rather than recomputed when the §8.2 machine reads it. That makes
// a suppression as stale as the evaluation carrying it: a subject scored during
// a zone outage stays suppressed until something re-evaluates it. The bound is
// acceptable and the alternative is worse. Re-judging on every alert tick would
// mean the verdict in a finding and the verdict behind the exported scores
// could disagree, and the staleness costs only latency — the end of an outage
// is itself a burst of node events, which bumps the generation and re-scores
// every subject that cared; and a subject that comes out of suppression still
// owes §8.2 a full dwell before it fires, which is longer than the staleness.
func (s *Source) suppression(sub leeway.SubjectRef, now time.Time) Suppressor {
	warming := !s.HasSynced()
	rolling := s.isRollingOut(sub)
	scaledAt := s.scales.ScaledAt(sub)
	return func(key leeway.TopologyKey, eligible leeway.Eligibility) leeway.Suppression {
		t := leeway.Transients{
			Warming:           warming,
			RolloutInProgress: rolling,
			ScaledAt:          scaledAt,
			DomainOutage:      s.domainOutage(key, eligible.Domains, now),
			DrainedAt:         s.inv.LastDrainIn(key, eligible.Domains),
		}
		return t.Classify(now, s.cfg.Transient)
	}
}

// sampleCluster refreshes the two §7.6 inputs that describe the cluster rather
// than a subject: the per-domain ready-count series and the set of workloads
// mid-rollout. Both are read by evaluate, which runs per subject, so both have
// to be gathered on a timer instead.
func (s *Source) sampleCluster(now time.Time) {
	s.inv.SampleReady(now, s.cfg.readyRetention())

	if s.rollouts == nil {
		return
	}
	refs := s.rollouts()
	set := make(map[leeway.SubjectRef]bool, len(refs))
	for _, ref := range refs {
		set[ref] = true
	}
	s.mu.Lock()
	s.rollingOut = set
	s.mu.Unlock()
}

// isRollingOut reports whether the last cluster sample found this subject
// mid-rollout. An unwired oracle, or a sample that has not run yet, reads
// false: §7.6's rows relax rather than suppress, so the absent answer costs a
// threshold that was not widened, not a finding that should not exist.
func (s *Source) isRollingOut(sub leeway.SubjectRef) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rollingOut[sub]
}

// onScalable records a Deployment's or StatefulSet's declared size for §7.6's
// recent-scale row. Anything else the informers might hand us is ignored
// rather than guessed at — a workload whose size we cannot read is one whose
// scale row stays unanswered, which is the same state as a DaemonSet.
func (s *Source) onScalable(obj any) {
	switch o := obj.(type) {
	case *appsv1.Deployment:
		s.scales.Observe(leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: o.Namespace, Name: o.Name},
			int32OrOne(o.Spec.Replicas), time.Now())
	case *appsv1.StatefulSet:
		s.scales.Observe(leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: o.Namespace, Name: o.Name},
			int32OrOne(o.Spec.Replicas), time.Now())
	}
}

// onScalableDelete drops a deleted workload's scale entry, resolving the
// tombstone the informer delivers when it missed the delete itself.
func (s *Source) onScalableDelete(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	switch o := obj.(type) {
	case *appsv1.Deployment:
		s.scales.Forget(leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: o.Namespace, Name: o.Name})
	case *appsv1.StatefulSet:
		s.scales.Forget(leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: o.Namespace, Name: o.Name})
	}
}

// int32OrOne dereferences a replicas pointer with the API's own default. An
// unset spec.replicas means one, not zero, and reading it as zero would make
// every default-sized workload's first explicit scale look like a change from
// nothing.
func int32OrOne(p *int32) int32 {
	if p != nil {
		return *p
	}
	return 1
}

// domainOutage reports whether any domain this subject could have used has
// lost more than half its ready nodes within §7.6's window.
//
// Any, not all: the question the row asks is whether the counts still describe
// the cluster the expectation was apportioned over, and one dead zone out of
// three is enough to make them describe something else. A subject eligible for
// no domains cannot be affected by one going down, and scores as a gate anyway.
func (s *Source) domainOutage(key leeway.TopologyKey, domains []leeway.Domain, now time.Time) bool {
	stats := s.inv.Stats(key)
	for _, d := range domains {
		if s.cfg.Transient.DomainOutage(s.inv.ReadyHistory(key, d), stats[d].Ready, now) {
			return true
		}
	}
	return false
}

// policyFor picks the LeewayPolicy governing sub, or nil when none does — the
// normal case, since the CRD is optional and most clusters never install it.
//
// The selector is matched against the *pod's* labels, not the workload's. The
// source resolves subjects from pods and never reads the owning object, so a
// label that exists only on the Deployment is not available to match; the pod
// carries the template's labels plus the controller's own, which is a superset
// of what a workload selector would offer anyway.
func (s *Source) policyFor(sub leeway.SubjectRef, pod *corev1.Pod) *Policy {
	m := s.policies.For(sub, pod.Labels)
	if m.Ambiguous {
		// Reported, not resolved: there is no honest ordering between two
		// selectors, so the operator has to be told which one won rather than
		// left to discover it from the intent it produced. Rate-limited,
		// because this runs on every coalesced evaluation of a matched subject
		// and the condition persists until someone edits a policy.
		s.warnAmbiguous(sub, m)
	}
	return m.Policy
}

// baselineIntents renders whatever §7.5 has learned about sub, for §5.1 to
// rank against everything the subject actually declared.
//
// The learnBaselines gate is redundant with the sampler's — with learning off
// nothing is ever observed, so the log stays empty and this walk finds
// nothing. It is here anyway because the two would stop being redundant the
// moment anything else populates the log, and of the two ways to get that
// wrong, "the off switch did not turn something off" is the one an operator
// finds out about from a page.
func (s *Source) baselineIntents(sub leeway.SubjectRef, now time.Time) map[leeway.TopologyKey]*leeway.Intent {
	if !s.cfg.learnBaselines() {
		return nil
	}
	return s.baselines.IntentsFor(sub, now, s.cfg.Baselines)
}

// subjectPod returns an admitted pod belonging to sub, for inference to read.
//
// The fast path is State's representative hint: one map get and one lister get,
// which is what keeps evaluation independent of cluster size. The hint is a
// hint, so a miss falls through to listing the subject's namespace and electing
// a new one — plain O(pods in namespace), taken rarely and never cached into a
// per-pod index.
//
// That the fallback is un-indexed is the decision, not an omission. A
// subject-keyed pod index has to run resolveSubject to place a pod, which reads
// the ReplicaSet cache — so a pod's index entry would depend on a *different*
// informer's state, and an informer re-indexes an object only when that object
// changes. With the resync period at zero, an entry computed before the
// ReplicaSet landed is never repaired. A hint that can be wrong and is checked
// on every read has no such failure mode.
//
// Misses also cluster in exactly the case where paying for them is wrong: a
// domain outage turns over every pod at once, and §7.6 suppresses workload
// findings for the duration and raises one domain finding instead. Skipping an
// evaluation whose output would be suppressed anyway is the right answer, so
// the fallback is allowed to be slow because it is allowed to be rare.
func (s *Source) subjectPod(sub leeway.SubjectRef) *corev1.Pod {
	s.mu.Lock()
	lister := s.pods
	s.mu.Unlock()
	if lister == nil {
		return nil
	}
	if name, ok := s.state.Representative(sub); ok {
		if pod, err := lister.Pods(sub.Namespace).Get(name); err == nil {
			return pod
		}
	}
	return s.electRepresentative(lister, sub)
}

// electRepresentative finds a pod for sub the hard way and records it.
//
// It picks the most recently created pod, breaking ties on name. Newest because
// during a rollout it is the one carrying the current template — inference that
// reads the pod being replaced describes intent the workload has already
// abandoned; deterministic because two evaluations of an unchanged subject that
// disagree would flap `intent_info` on nothing but map iteration order.
func (s *Source) electRepresentative(lister corelisters.PodLister, sub leeway.SubjectRef) *corev1.Pod {
	pods, err := lister.Pods(sub.Namespace).List(labels.Everything())
	if err != nil {
		return nil
	}
	var best *corev1.Pod
	for _, pod := range pods {
		got, ok := resolveSubject(pod, s.ownerOf)
		if !ok || got != sub {
			continue
		}
		if best == nil || newerPod(pod, best) {
			best = pod
		}
	}
	if best == nil {
		return nil
	}
	s.state.SetRepresentative(sub, best.UID, best.Name)
	return best
}

// newerPod reports whether a should be preferred over b as a representative.
func newerPod(a, b *corev1.Pod) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.After(b.CreationTimestamp.Time)
	}
	// Creation timestamps have one-second resolution, so a scale-out puts a
	// whole ReplicaSet on the same value. The name is the tiebreak that makes
	// the choice reproducible.
	return a.Name < b.Name
}

// Run implements sources.Source.
//
// emit is stored rather than threaded through, because the only caller is the
// §8.2 alert pass and it runs on a timer several call frames down.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	factory, owned := s.factory, false
	if factory == nil {
		factory = informers.NewSharedInformerFactory(s.client, 0)
		owned = true
	}

	nodeFactory := s.nodeFactory
	if nodeFactory == nil {
		nodeFactory = factory
	}

	podInformer := factory.Core().V1().Pods()
	nodeInformer := nodeFactory.Core().V1().Nodes()
	rsInformer := factory.Apps().V1().ReplicaSets()
	// §7.6's recent-scale row. Both kinds are already on this factory in the
	// sentinel — the rollout source watches all three of Deployment,
	// StatefulSet and ReplicaSet, and objectstate watches Deployments — so
	// this costs no new stream in the deployment that matters. A standalone
	// embedder that runs leeway alone pays two LISTs of objects an order of
	// magnitude smaller than the pod list it is already paying for.
	depInformer := factory.Apps().V1().Deployments()
	stsInformer := factory.Apps().V1().StatefulSets()
	pvcInformer := factory.Core().V1().PersistentVolumeClaims()
	// PersistentVolumes are cluster-scoped, so they go where the nodes go —
	// see WithNodeFactory for why a scoped caller's factory cannot list them.
	pvInformer := nodeFactory.Core().V1().PersistentVolumes()

	// The ReplicaSet, PVC and PV caches are read, not watched: the resolver and
	// the FR-8 pin predicate ask them for one object at a time and nothing here
	// reacts to any of the three changing. Touching the Lister before Start is
	// what gets the informer built and started with the rest.
	//
	// No handler on the volumes is a decision, not an omission. A pod whose
	// claim binds later is not a pod whose placement we have already got wrong:
	// until the claim binds the pod is unscheduled, so it holds no domain and
	// contributes to no distribution, and the event that schedules it is a pod
	// event we already act on. Watching the volumes instead would make a pod's
	// Pinned answer depend on a second informer's arrival order — the same trap
	// that ruled out a subject-keyed pod index in subjectPod.
	s.mu.Lock()
	s.replicaSets = rsInformer.Lister()
	s.pods = podInformer.Lister()
	s.claims = pvcInformer.Lister()
	s.volumes = pvInformer.Lister()
	s.mu.Unlock()

	podH, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onPod(ctx, obj) },
		UpdateFunc: func(_, obj any) { s.onPod(ctx, obj) },
		DeleteFunc: func(obj any) { s.onPodDelete(ctx, obj) },
	})
	if err != nil {
		return fmt.Errorf("topologydrift: register pod handler: %w", err)
	}
	nodeH, err := nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onNode(ctx, obj) },
		UpdateFunc: func(_, obj any) { s.onNode(ctx, obj) },
		DeleteFunc: func(obj any) { s.onNodeDelete(ctx, obj) },
	})
	if err != nil {
		return fmt.Errorf("topologydrift: register node handler: %w", err)
	}
	depH, err := depInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onScalable(obj) },
		UpdateFunc: func(_, obj any) { s.onScalable(obj) },
		DeleteFunc: func(obj any) { s.onScalableDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("topologydrift: register deployment handler: %w", err)
	}
	stsH, err := stsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onScalable(obj) },
		UpdateFunc: func(_, obj any) { s.onScalable(obj) },
		DeleteFunc: func(obj any) { s.onScalableDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("topologydrift: register statefulset handler: %w", err)
	}
	if s.rollouts == nil {
		// §13 S4's "say it out loud" rule, applied to a row that is simply
		// absent: an operator looking at lookout_leeway_transient_subjects and
		// seeing no rollout bucket should be able to find out here whether that
		// means no rollouts or no answer.
		s.logger()("topologydrift: no rollout source wired — §7.6 will not relax thresholds during a rollout")
	}

	// §13 S4's fourth mitigation, and the cheapest one: say once, out loud,
	// whose numbers the fleet is about to be scored against. An operator who
	// never configured anything should not have to read the source to find out
	// that a maxSkew is ours rather than theirs.
	s.logger()("topologydrift: %s", DescribeClusterDefaults(s.cfg.ClusterDefaultConstraints))

	policySynced, err := s.startPolicyWatch(ctx)
	if err != nil {
		return err
	}

	if err := s.startMetrics(); err != nil {
		return err
	}
	defer func() { _ = s.metrics.Close() }()

	factory.Start(ctx.Done())
	if nodeFactory != factory {
		// Start is idempotent per informer, so starting the same object twice
		// would be harmless — but these are two objects whenever the caller
		// scoped its namespaced watches, and the node informer lives only on
		// the second one.
		nodeFactory.Start(ctx.Done())
	}
	if owned {
		defer factory.Shutdown()
	}

	// The policy barriers join the rest rather than being awaited separately:
	// arming before they sync would run the first re-apply against inferred
	// intent and then quietly correct it, so every subject a policy governs
	// would publish one wrong intent_info sample at every restart.
	synced := append([]cache.InformerSynced{
		podH.HasSynced, nodeH.HasSynced, rsInformer.Informer().HasSynced,
		depH.HasSynced, stsH.HasSynced,
		pvcInformer.Informer().HasSynced, pvInformer.Informer().HasSynced,
	}, policySynced...)
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return fmt.Errorf("topologydrift: cache sync failed (informer stopped before initial list completed)")
	}

	if err := s.reapply(podInformer.Lister()); err != nil {
		return err
	}
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()

	// §9.3 step 6, read here and applied on the first alert pass: a persisted
	// episode can only be reconciled against a fresh verdict, and the queue has
	// not scored anything yet. Read after arming, because until the caches are
	// synced there is nothing to reconcile against either.
	s.loadAlertState(ctx, time.Now())
	// §9.3 step 1's other half, read at the same instant and for the same
	// reason: step 7 measures one gap for the whole set, and a loader that
	// read the clock per row would give two subjects loaded a second apart
	// two different answers about the same outage.
	s.loadBaselines(ctx, time.Now())

	go s.queue.Run(ctx)

	ticker := time.NewTicker(s.cfg.EligibilitySweepInterval)
	defer ticker.Stop()

	// §6.5. One shard per tick, on its own timer rather than folded into the
	// sweep: the sweep is conditional on a node having changed, and a counter
	// bug does not wait for one.
	verifyTick := time.NewTicker(s.verify.Interval())
	defer verifyTick.Stop()

	// §8.2, on a timer rather than on the evaluation path. Dwell measures how
	// long a condition has held, so advancing it per event would make it run
	// faster for a subject whose pods churn than for a quiet one that is
	// equally wrong; and a whole pass has to be judged against one instant.
	alertTick := time.NewTicker(s.cfg.AlertInterval)
	defer alertTick.Stop()

	// §7.6's outage test compares a domain against its peak over a window, so
	// something has to record the peak while the domain is still healthy. Node
	// events cannot: they carry the value *after* each change, and a zone that
	// dies all at once changes exactly once.
	//
	// The rollout set rides the same tick. It is the other §7.6 input that is a
	// property of the cluster rather than of the subject being scored, and the
	// oracle answers for the whole cluster in one call — asking it from
	// evaluate, which runs per subject, would make one pass over tens of
	// thousands of subjects into tens of thousands of scans of every Deployment
	// in the cluster. Snapshotting it on a timer bounds that at one scan per
	// interval, at the cost of a rollout being noticed up to one interval late,
	// which is nothing against a ten-minute dwell.
	//
	// FR-3's node-group pass rides it too, for a third reason: a node group is
	// a property of the cluster and not of any pod, so there is no event to
	// coalesce it behind, and the inventory it is derived from is exactly what
	// this tick already samples. It runs *after* the sample, because §7.6's
	// outage row for a pool reads the series the sample just extended.
	readyTick := time.NewTicker(s.cfg.ReadySampleInterval)
	defer readyTick.Stop()
	s.sampleCluster(time.Now())
	s.evaluateNodeGroups(time.Now())

	// §7.5, on its own timer and not on the evaluation path. A half-life is a
	// statement about elapsed time, so a subject whose pods churn must not
	// learn faster than a quiet one that is equally wrong — the same argument
	// as the alert tick, only sharper, because the dwell is minutes and this
	// is hours.
	baselineTick := time.NewTicker(s.cfg.BaselineSampleInterval)
	defer baselineTick.Stop()

	// §9.2's batched write, separate from the sample tick because the two
	// answer to different things: the sample rate is what the estimator needs,
	// and the flush rate is how much learning a crash is allowed to cost.
	baselineFlush := time.NewTicker(s.cfg.BaselineFlushInterval)
	defer baselineFlush.Stop()
	// A last flush on the way out. Not a promise — a killed process makes no
	// write at all — but a graceful stop is the common shutdown, and a clean
	// one turns "24 hours of downtime, mark everything stale" into "no
	// downtime at all" for a rolling restart that takes seconds.
	defer func() { s.flushBaselines(context.WithoutCancel(ctx)) }()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.sweep()
		case <-verifyTick.C:
			s.runVerifyPass()
		case <-alertTick.C:
			s.runAlertPass(ctx)
		case <-readyTick.C:
			now := time.Now()
			s.sampleCluster(now)
			s.evaluateNodeGroups(now)
		case <-baselineTick.C:
			s.sampleBaselines(time.Now())
		case <-baselineFlush.C:
			s.flushBaselines(ctx)
		}
	}
}

// runVerifyPass checks one shard and logs only when it found something. A
// component that logs a line every five minutes forever to say nothing happened
// is a component whose one important line gets scrolled past.
func (s *Source) runVerifyPass() {
	rep := s.verify.Tick()
	if rep.Err != nil || rep.Drifted > 0 {
		// Both are already logged in detail by the verifier itself; this is the
		// line that ties those per-subject reports to a shard.
		s.logger()("topology-drift: verify shard %d/%d: %d of %d subject(s) drifted",
			rep.Shard, s.verify.Shards(), rep.Drifted, rep.Subjects)
	}
}

// loadAlertState reads §9.1's persisted episodes and hands them to the machine
// for reconciliation on the first pass (§9.3 step 6).
//
// A read failure is logged and swallowed. §9.2 is explicit that a sentinel must
// never refuse to start over its history file — a store that cannot be read
// costs every open episode one dwell, and refusing to run costs the cluster its
// monitoring. The nil-store case is not an error at all: it is the deployment
// without `--store`.
func (s *Source) loadAlertState(ctx context.Context, now time.Time) {
	s.mu.Lock()
	st, cluster := s.store, s.cfg.Cluster
	s.mu.Unlock()
	if st == nil {
		return
	}

	records, err := st.LeewayAlertStates(ctx, cluster)
	if err != nil {
		s.logger()("topology-drift: could not read persisted alert state (%v) — every open episode restarts its dwell; monitoring is unaffected", err)
		return
	}
	if n := s.alerts.Load(records, now); n > 0 {
		s.logger()("topology-drift: restored %d open episode(s) from the store; each is reconciled against a fresh score as its subject is re-evaluated (within %s)",
			n, s.cfg.ReconcileGrace)
	}
}

// loadBaselines reads §7.5's persisted estimators and applies §9.3 step 7's
// downtime rules (§9.3 step 1, deferred to here for the same reason the alert
// state is: a baseline is only interpretable against a clock, and step 7 wants
// one instant for the whole set).
//
// Swallowed on failure, like the alert half and for the stronger version of
// the same reason: a baseline that cannot be read costs six hours of
// relearning during which Tier C is scored exactly as it was before Phase 5.
func (s *Source) loadBaselines(ctx context.Context, now time.Time) {
	s.mu.Lock()
	st, cluster := s.baselineStore, s.cfg.Cluster
	s.baselineGraceUntil = now.Add(s.cfg.ReconcileGrace)
	s.mu.Unlock()
	if st == nil || !s.cfg.learnBaselines() {
		return
	}

	records, err := st.LeewayBaselines(ctx, cluster)
	if err != nil {
		s.logger()("topology-drift: could not read persisted baselines (%v) — §7.5 relearns from scratch and Tier C is scored against an even apportionment until it does; monitoring is unaffected", err)
		return
	}
	n, verdicts := s.baselines.Load(records, now, s.cfg.Baselines)
	if n == 0 {
		return
	}
	// One line, and only when something came back. The verdict breakdown is
	// the part worth reading: "restored 12000" says the file was there, and
	// "11800 resumed, 200 widened" says what it is worth.
	s.logger()("topology-drift: restored %d learned baseline(s) — %d resumed, %d widened for one half-life after a gap, %d stale and relearning (§9.3 step 7)",
		n,
		verdicts[leeway.DowntimeResumed],
		verdicts[leeway.DowntimeWidened],
		verdicts[leeway.DowntimeStale])
}

// baselineSample is one subject-axis's placement as of one pass, lifted out
// of the index before anything is done with it.
//
// Collected first and applied second because State.EachEvaluation holds the
// index lock for the whole walk, and both of the things a sample needs next —
// the episode's phase and the estimator itself — live behind other locks. The
// same rule judgements() follows, for the same reason: two locks that are
// never held together cannot be taken in two orders.
type baselineSample struct {
	key        baselineKey
	domains    []leeway.Domain
	actual     []int64
	suppressed bool
}

// sampleBaselines feeds every scored subject-axis to its §7.5 estimator, and
// reaps the ones that are no longer scored.
//
// Freezing is decided here rather than inside the estimator because it is two
// facts from two different places. §7.6 suppression is already on the
// evaluation, carried from scoring time; the alert phase is the machine's, and
// AlertPhase.Firing() covers Resolving as well as Firing — a subject whose
// episode is still clearing has not yet been shown to be back to normal, and
// learning through the tail of an episode is learning from the drift.
//
// The reap is a sweep rather than a hook on every removal path, because the
// index already knows the answer: a subject-axis with no evaluation is one
// nothing is scoring, whether that is because the workload went away or
// because the last node carrying its topology label did. It is held off for
// ReconcileGrace after a load for the same reason the alert machine holds off
// its own: at startup nothing has been scored yet, and reaping on the first
// tick would delete every row we had just read.
func (s *Source) sampleBaselines(now time.Time) {
	if !s.cfg.learnBaselines() {
		return
	}

	samples := make([]baselineSample, 0, 64)
	s.state.EachEvaluation(func(sub leeway.SubjectRef, ev *Evaluation) {
		samples = append(samples, baselineSample{
			key:        baselineKey{Subject: sub, Key: ev.Key},
			domains:    ev.Scores.Domains,
			actual:     ev.Scores.Actual,
			suppressed: ev.Verdict.Suppressed,
		})
	})

	live := make(map[baselineKey]bool, len(samples))
	for _, sm := range samples {
		live[sm.key] = true
		frozen := sm.suppressed
		if st, open := s.alerts.StateOf(sm.key.Subject, sm.key.Key); open && st.Phase.Firing() {
			frozen = true
		}
		s.baselines.SetFrozen(sm.key, frozen)
		s.baselines.Observe(sm.key, sm.domains, sm.actual, now, s.cfg.Baselines)
	}

	s.mu.Lock()
	grace := s.baselineGraceUntil
	s.mu.Unlock()
	if now.Before(grace) {
		return
	}
	for _, k := range s.baselines.Keys() {
		if !live[k] {
			s.baselines.Forget(k)
		}
	}
}

// flushBaselines writes §9.2's batch: everything that moved since the last
// flush, in one transaction, plus the deletions that cannot ride in it.
//
// Errors are logged and dropped rather than retried. The batch has already
// been drained, so a failed flush loses at most one interval of a twelve-hour
// half-life and the next sample marks the same sets dirty again — which is the
// trade §9.2 makes for this table and explicitly refuses for the other one.
func (s *Source) flushBaselines(ctx context.Context) {
	s.mu.Lock()
	st, cluster := s.baselineStore, s.cfg.Cluster
	s.mu.Unlock()
	if st == nil || !s.cfg.learnBaselines() {
		return
	}

	recs, gone := s.baselines.Flush(cluster)
	if len(recs) > 0 {
		if err := st.PutLeewayBaselines(ctx, recs); err != nil {
			s.logger()("topology-drift: could not flush %d learned baseline(s) (%v) — they are still being learned in memory and will be written on the next tick", len(recs), err)
		}
	}
	for _, k := range gone {
		if err := st.DeleteLeewayBaseline(ctx, cluster, k.Subject.String(), string(k.Key)); err != nil {
			s.logger()("topology-drift: could not delete the learned baseline for %s on %s: %v", k.Subject, k.Key, err)
		}
	}
}

// judgements snapshots every scored subject-axis's verdict.
//
// Taken as a slice before the machine runs rather than advancing inside
// State's walk, for two reasons. It keeps the index lock off the machine's
// lock, so the two can never be taken in opposite orders by some later caller.
// And it is what makes the pass complete: Alerts.Pass reads the absence of a
// judgement as "this subject is gone", which is only true of a set that was
// consistent at one moment.
func (s *Source) judgements() []Judgement {
	var js []Judgement
	s.state.EachEvaluation(func(sub leeway.SubjectRef, ev *Evaluation) {
		js = append(js, Judgement{
			Subject:  sub,
			Key:      ev.Key,
			Breached: ev.Verdict.Breached,
			Tier:     ev.Verdict.Tier,
		})
	})
	return js
}

// runAlertPass advances §8.2's machine over every scored subject, emits what
// crossed its dwell and persists what moved.
//
// Persistence happens for every outcome and emission only for TransitionFiring.
// The other four moves are bookkeeping §8.2 owns: Pending and Clearing change
// nothing anybody outside has been told, Abandoned ends an episode that was
// never announced, and Resolved is answered by the clearance observer rather
// than by a second signal — DESIGN §7.4 owns outcome records, and a source that
// emitted its own would produce two.
//
// Persist first, emit second. A finding on the wire that the store does not
// know about turns into a duplicate the moment the process restarts, while a
// persisted episode nobody was told about is one tick late at worst.
func (s *Source) runAlertPass(ctx context.Context) {
	now := time.Now()
	for _, o := range s.alerts.Pass(s.judgements(), now, s.cfg.ReconcileGrace) {
		s.persistOutcome(ctx, o, now)
		if o.Transition == leeway.TransitionFiring {
			s.emitFinding(o, now)
		}
	}
}

// emitFinding builds and routes the §8.5 payload for one episode that has just
// crossed its dwell.
//
// The evaluation is re-read from the index rather than carried through the
// machine, because the machine's input is deliberately four fields wide
// (Judgement) and widening it would put the whole scoring result inside a lock
// the pass holds over every subject in the cluster. The re-read is bounded by
// the number of episodes that fired on this tick, which is normally none.
//
// A subject whose evaluation has gone — scored, then untracked between the
// judgement snapshot and here — is dropped silently. There is nothing to say
// about a workload that no longer exists, and the machine has already emitted
// the outcome that removes it.
func (s *Source) emitFinding(o Outcome, now time.Time) {
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit == nil {
		return
	}

	ev := axisOf(s.state.EvaluationsOf(o.Subject), o.Key)
	if ev == nil {
		return
	}
	f, delivery := s.findingFor(o.Subject, ev, o.State, s.state.Snapshot(o.Subject)[o.Key], now)
	if !delivery.Signal {
		// Metrics only (§8.3). Not logged: on a large estate most firing
		// subjects are Tier C, and a line per subject per episode would be a
		// log nobody reads about the decision that keeps the source quiet.
		// lookout_leeway_alert_state already shows the episode.
		return
	}
	emit(signalFor(f, now))
}

// axisOf picks one axis out of a subject's evaluations, or nil when the axis is
// no longer scored — a subject can lose an axis between two passes when the
// last node carrying that topology label goes away.
func axisOf(evals []Evaluation, key leeway.TopologyKey) *Evaluation {
	for i := range evals {
		if evals[i].Key == key {
			return &evals[i]
		}
	}
	return nil
}

// persistOutcome writes one episode's move through to the store.
//
// Only a move is written. An episode that sat in Pending for nine of its ten
// minutes produced no transition and changed none of the persisted fields, so
// writing it every tick would be twenty thousand redundant UPSERTs an hour to
// record that nothing happened. The fields §9.1 persists — the phase, the two
// timestamps and the flap history — only change when Advance says they did.
func (s *Source) persistOutcome(ctx context.Context, o Outcome, now time.Time) {
	s.mu.Lock()
	st, cluster := s.store, s.cfg.Cluster
	s.mu.Unlock()
	if st == nil {
		return
	}

	subjectKey, topologyKey := o.Subject.String(), string(o.Key)
	if o.Gone {
		if err := st.DeleteLeewayAlertState(ctx, cluster, subjectKey, topologyKey); err != nil {
			s.logger()("topology-drift: could not clear persisted alert state for %s on %s: %v", subjectKey, topologyKey, err)
		}
		return
	}
	if o.Transition == leeway.TransitionNone {
		return
	}
	if err := st.PutLeewayAlertState(ctx, leeway.AlertRecord{
		Cluster:     cluster,
		SubjectKey:  subjectKey,
		TopologyKey: topologyKey,
		AlertState:  o.State,
		UpdatedAt:   now,
	}); err != nil {
		s.logger()("topology-drift: could not persist alert state for %s on %s (%v) — the episode is still tracked in memory and will restart its dwell if this process does",
			subjectKey, topologyKey, err)
	}
}

// startMetrics declares the §8.4 instruments against the configured meter.
func (s *Source) startMetrics() error {
	in, err := newInstruments(metricsOptions{
		Meter:         s.cfg.Meter,
		PerDomain:     s.cfg.perDomainGate(),
		SubjectCounts: s.state.SubjectCounts,
		NodeGroups:    s.nodeGroupsDiscovered,
		DomainNodes:   s.domainNodes,
		DomainObjects: s.domainObjects,
		Evaluations:   s.state.EachEvaluation,
		Intents:       s.state.EachIntent,
		Alerts:        s.alerts.Each,
		Baselines:     func() baselineStats { return s.baselines.Stats(time.Now(), s.cfg.Baselines) },
	})
	if err != nil {
		return err
	}
	s.metrics = in
	return nil
}

// reapply rebuilds the counters from the synced pod cache.
//
// The handlers registered above already replayed the cache as adds, so most of
// this is a no-op — but not all of it, and the part that is not is the point.
// A pod whose ReplicaSet had not yet landed in its own cache resolved to no
// subject and went uncounted; there is no ordering guarantee between two
// informers' initial lists. Re-applying once both are synced closes that
// window, and OnPodAdd is idempotent by construction precisely so this is safe
// to do. Steady state has no equivalent race worth handling here — a pod is
// created after its ReplicaSet and gets several more events within seconds —
// and §6.5's verifier is the backstop for whatever slips through anyway.
func (s *Source) reapply(lister corelisters.PodLister) error {
	pods, err := lister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("topologydrift: initial pod list: %w", err)
	}
	for _, p := range pods {
		s.state.OnPodAdd(p)
	}
	s.logger()("topology-drift: counters built from %d pods across %d nodes: %d subjects tracked",
		len(pods), s.inv.Len(), s.state.Len())
	return nil
}

// sweep re-enqueues every subject after a broad node change. It is a no-op
// when nothing eligibility-relevant has happened since the last tick, which is
// the common case.
func (s *Source) sweep() {
	s.mu.Lock()
	pending := s.sweepPending
	s.sweepPending = false
	s.mu.Unlock()
	if !pending {
		return
	}
	subs := s.state.Subjects()
	for _, sub := range subs {
		s.queue.Enqueue(sub)
	}
	s.logger()("topology-drift: node eligibility changed, re-enqueuing %d subjects (inventory generation %d)",
		len(subs), s.inv.Generation())
}

func (s *Source) onPod(ctx context.Context, obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	s.state.OnPodAdd(pod)
	s.metrics.recordEvent(ctx, resourcePod, time.Now())
}

func (s *Source) onPodDelete(ctx context.Context, obj any) {
	s.state.OnPodDelete(obj)
	s.metrics.recordEvent(ctx, resourcePod, time.Now())
}

func (s *Source) onNode(ctx context.Context, obj any) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}
	s.noteChange(s.state.OnNodeUpsert(node))
	s.metrics.recordEvent(ctx, resourceNode, time.Now())
}

func (s *Source) onNodeDelete(ctx context.Context, obj any) {
	s.noteChange(s.state.OnNodeDelete(obj))
	s.metrics.recordEvent(ctx, resourceNode, time.Now())
}

// noteChange applies the §6.3 node-event asymmetry. The narrow half is already
// done by the time we get here — State.OnNodeUpsert re-maps the affected pods
// and enqueues their subjects synchronously, because those are counts and
// counts cannot wait. The broad half only changes which domains a subject
// *could* have used, which nothing reads until the next evaluation, so it is
// deferred to the sweep.
func (s *Source) noteChange(ch Change) {
	if !ch.EligibilityChanged {
		return
	}
	s.mu.Lock()
	s.sweepPending = true
	s.mu.Unlock()
}

// nodeGroupsDiscovered is how many distinct node groups the last FR-3 pass
// resolved, for lookout.leeway.node_groups_discovered.
func (s *Source) nodeGroupsDiscovered() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodeGroupsFound
}

// domainNodes reports usable nodes per domain, per axis, for
// lookout.leeway.domain_ready_nodes.
func (s *Source) domainNodes() map[leeway.TopologyKey]map[leeway.Domain]int64 {
	keys := s.inv.Keys()
	out := make(map[leeway.TopologyKey]map[leeway.Domain]int64, len(keys))
	for _, key := range keys {
		byDomain := make(map[leeway.Domain]int64)
		for domain, st := range s.inv.Stats(key) {
			byDomain[domain] = st.Usable
		}
		out[key] = byDomain
	}
	return out
}

// domainObjects walks every non-zero per-domain count for
// lookout.leeway.domain_objects.
//
// Snapshots subject by subject rather than holding the index lock across the
// whole walk: a scrape must not be able to stall the delta path. The cost is
// that two subjects in one scrape can be a few microseconds apart, which for a
// gauge of a moving count is not a cost at all.
// Node groups are walked alongside the pod subjects, through the same gate:
// a pool's nodes per zone is the same kind of row as a Deployment's pods per
// zone, and a reader looking at a drifting workload wants the pool underneath
// it in the same query.
func (s *Source) domainObjects(gate PerDomainGate, yield countObserver) {
	for _, sub := range append(s.state.Subjects(), s.state.Groups()...) {
		if !gate.Admits(s.state.DriftOf(sub)) {
			continue
		}
		for key, dist := range s.state.Snapshot(sub) {
			for _, domain := range dist.Domains() {
				c := dist.ByDomain[domain]
				for _, sc := range []struct {
					state leeway.CountState
					n     int64
				}{
					{leeway.StateRunning, c.Running},
					{leeway.StatePending, c.Pending},
					{leeway.StateUnschedulable, c.Unschedulable},
					{leeway.StateTerminating, c.Terminating},
				} {
					if sc.n != 0 {
						yield(sub, key, domain, sc.state, sc.n)
					}
				}
			}
		}
	}
}
