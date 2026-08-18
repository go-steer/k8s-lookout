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

// Package workload is the batch-workload signal source (post-M5
// roadmap B.1, issue #129): failed Jobs and CronJobs that stopped
// being scheduled. Before this source, a failed Job
// (BackoffLimitExceeded, DeadlineExceeded) or a silently dead CronJob
// was invisible to the sentinel unless its pods happened to crashloop
// into the k8s-events allow-list — batch work fails without leaving a
// crashlooping pod behind.
//
// Two kinds (§7.3, APPEND-ONLY):
//
//   - workload.job_failed — a Job's Failed condition went True. Fired
//     on the observed transition, once per Job (a re-run is a NEW Job
//     with its own UID, so CronJob children each get their own
//     verdict; the dedup key is the Job UID per issue #129).
//   - workload.cron_missed — an unsuspended CronJob passed a scheduled
//     activation (plus Config.Grace) without status.lastScheduleTime
//     advancing: the schedule is dead — controller wedged, schedule
//     starved by startingDeadlineSeconds, or concurrencyPolicy=Forbid
//     against a stuck predecessor. Fired once per missed activation
//     (dedup key: the CronJob UID, so repeats collapse into followups
//     on the same incident). Consecutive misses escalate to critical
//     at CriticalMisses — one miss is a hiccup, three is an outage of
//     the schedule.
//
// Schedules are parsed with github.com/robfig/cron/v3 — the same
// parser the upstream CronJob controller uses — honoring
// spec.timeZone via the CRON_TZ prefix exactly as upstream does. A
// spec the API server accepted but this parser rejects is skipped
// with one loud log line per CronJob generation (§2: never silent).
//
// Transition-based, arm-after-sync (the objectstate discipline): the
// initial LIST populates state without emitting, so Jobs that failed
// before the sentinel started are history (`triage delta` shows
// them), not replayed incidents. A CronJob's misses are judged only
// from activations expected AFTER arming — an as-it-happens source
// observes fresh misses, it does not backfill downtime.
//
// §7.4 clearance (each source that can observe a symptom observes its
// absence):
//
//   - job_failed on a CronJob-owned Job clears as recovered when a
//     SIBLING Job of the same CronJob is observed completing after the
//     failure — the schedule produced a good run. StableSince is the
//     observation of that success (M3 observation 4 discipline: the
//     stability window counts from when recovery was SEEN, never from
//     a historical completionTime).
//   - job_failed on a standalone Job is terminal: a Failed condition
//     never reverts, so the incident clears only as object_deleted
//     (the TTL controller or an operator removing the Job).
//   - cron_missed clears as recovered when lastScheduleTime advances
//     (the schedule fired again) or the CronJob is suspended (the
//     operator ended the "should have run" condition deliberately);
//     as object_deleted when the CronJob is gone. Incidents restored
//     from a previous sentinel's dedup snapshot carry no in-process
//     miss memory — for those, ANY run observed since arming proves
//     the schedule recovered (it necessarily postdates the old miss).
package workload

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "workload"

// The kinds this source emits (§7.3). APPEND-ONLY.
const (
	KindJobFailed  = "workload.job_failed"
	KindCronMissed = "workload.cron_missed"
)

// The dedup/fingerprint reasons (kind suffixes, the objectstate
// convention). Both map to themselves under CanonicalReason.
const (
	ReasonJobFailed  = "job_failed"
	ReasonCronMissed = "cron_missed"
)

// CriticalMisses is the consecutive-miss count that escalates
// cron_missed to critical: one miss is a hiccup (a slow control
// plane, a Forbid policy waiting out a long run), three consecutive
// is a dead schedule. Design-fixed like the expiry source's 72h
// critical window; the warning threshold (the first miss) is what
// Config.Grace tunes.
const CriticalMisses = 3

// Config are the source's thresholds. Zero values take the defaults.
type Config struct {
	// Grace is how far past a scheduled activation the source waits
	// for status.lastScheduleTime to advance before calling the
	// activation missed — absorbing controller latency and short
	// startingDeadlineSeconds windows. Default 5m.
	Grace time.Duration
	// TickInterval drives the miss sweep (a dead schedule produces
	// no informer updates — the clock has to notice). Default 30s.
	//
	// There is deliberately NO state TTL: the objects this source
	// exists for are quiet by definition (a terminally failed Job
	// never updates again; a dead schedule writes no status), so a
	// last-seen prune would evict LIVE objects and falsely resolve
	// their incidents as object_deleted. The mirror is bounded by the
	// cluster's Job/CronJob count like any informer cache; DeleteFunc
	// (with tombstone unwrapping) owns removal.
	TickInterval time.Duration
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{
		Grace:        5 * time.Minute,
		TickInterval: 30 * time.Second,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Grace <= 0 {
		c.Grace = d.Grace
	}
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	return c
}

// jobEntry is the per-Job state mirror.
type jobEntry struct {
	namespace string
	name      string
	// ownerCron is the controlling CronJob's UID ("" for standalone
	// Jobs) — the sibling-success clearance join key.
	ownerCron types.UID
	ownerName string
	failed    bool
	complete  bool
	// fired gates KindJobFailed to once per Job.
	fired bool
	// failedObservedAt stamps the not-failed→failed transition
	// observation (clearance compares sibling successes against it).
	failedObservedAt time.Time
}

// cronEntry is the per-CronJob state mirror.
type cronEntry struct {
	namespace string
	name      string
	schedule  string
	timeZone  string
	suspend   bool
	// lastSchedule mirrors status.lastScheduleTime (zero when the
	// CronJob has never scheduled).
	lastSchedule time.Time
	creation     time.Time
}

// cronTrack is the per-CronJob miss memory.
type cronTrack struct {
	// consecutive counts misses since the last observed run — the
	// CriticalMisses escalation input. NOT reset by a sibling Job
	// completing: an old run finishing proves the workload works, not
	// that the SCHEDULE is producing runs — only lastScheduleTime
	// advancing does that.
	consecutive int
	// lastMissed is the newest activation called missed. It doubles
	// as the miss-anchor (each activation is judged exactly once) and
	// the clearance reference (lastSchedule must advance past it).
	lastMissed time.Time
	// runObservedAt / suspendedAt stamp the respective clearance
	// transitions AS OBSERVED (M3 observation 4: StableSince counts
	// from observation, never from a historical status timestamp).
	// runObservedAt is the FIRST post-arm lastSchedule advance seen
	// after the latest miss (a new miss zeroes it), so a healthy
	// high-frequency schedule cannot keep re-stamping it and starve
	// the §7.4 stability window.
	runObservedAt time.Time
	suspendedAt   time.Time
	prevSuspend   bool
	// prevLastSchedule is the previous observation's lastSchedule —
	// the advance-transition edge detector.
	prevLastSchedule time.Time
	// parseWarned dedups the unparseable-schedule log line to once
	// per (schedule, timeZone) revision.
	parseWarned string
}

// cronSuccess records the newest observed sibling completion per
// CronJob — the job_failed clearance evidence.
type cronSuccess struct {
	observedAt time.Time
}

// Source implements sources.Source (and engine.ClearanceObserver) for
// the batch-workload row of the post-M5 roadmap.
type Source struct {
	client kubernetes.Interface
	cfg    Config
	// factory, when set via WithFactory, is the externally owned
	// shared informer factory (§6.3).
	factory informers.SharedInformerFactory

	mu sync.Mutex
	// armed flips true after both informer caches sync; armedAt
	// anchors the first expected activation per CronJob (misses are
	// judged forward from arming, never backfilled).
	armed   bool
	armedAt time.Time
	emit    func(engine.Signal)

	jobs      map[types.UID]*jobEntry
	crons     map[types.UID]*cronEntry
	tracks    map[types.UID]*cronTrack
	successes map[types.UID]*cronSuccess

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// New constructs the source. Zero-valued cfg fields take the shipped
// defaults.
func New(client kubernetes.Interface, cfg Config) *Source {
	return &Source{
		client:    client,
		cfg:       cfg.normalize(),
		jobs:      make(map[types.UID]*jobEntry),
		crons:     make(map[types.UID]*cronEntry),
		tracks:    make(map[types.UID]*cronTrack),
		successes: make(map[types.UID]*cronSuccess),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the informers list Jobs and
// CronJobs cluster-wide.
func (s *Source) Scope() sources.Scope { return sources.ScopeCluster }

// WithFactory directs Run to register its informers on an externally
// owned shared factory (§6.3). Call before Run; nil is ignored.
func (s *Source) WithFactory(f informers.SharedInformerFactory) {
	if f != nil {
		s.factory = f
	}
}

// ClearanceObserver returns the §7.4 clearance predicate for this
// source's incidents, backed by its informers.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

// RequiredAccess implements sources.AccessDeclarer (§11): list+watch
// on both informer targets. Matches deploy/12-clusterrole-watcher.yaml.
func (s *Source) RequiredAccess() []sources.Requirement {
	var reqs []sources.Requirement
	for _, resource := range []string{"jobs", "cronjobs"} {
		for _, verb := range []string{"list", "watch"} {
			reqs = append(reqs, sources.Requirement{Group: "batch", Resource: resource, Verb: verb})
		}
	}
	return reqs
}

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// HasSynced implements sources.SyncReporter — the sentinel's /readyz
// probe is not ready until every source with a barrier has crossed it.
func (s *Source) HasSynced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed
}

// send delivers signals to the pipeline. Never called under s.mu.
func (s *Source) send(sigs []engine.Signal) {
	if len(sigs) == 0 {
		return
	}
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit == nil {
		return // not running (unit tests drive handlers directly)
	}
	for _, sig := range sigs {
		emit(sig)
	}
}

// Run implements sources.Source: starts both informers, arms after
// the caches sync, then drives the miss sweep until ctx is cancelled.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	factory := s.factory
	if factory == nil {
		factory = informers.NewSharedInformerFactory(s.client, 0)
	}

	jobH, err := factory.Batch().V1().Jobs().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asJob(obj, s.onJob) },
		UpdateFunc: func(_, obj any) { s.asJob(obj, s.onJob) },
		DeleteFunc: func(obj any) { s.asJob(tombstoneObj(obj), s.onJobDelete) },
	})
	if err != nil {
		return fmt.Errorf("workload: register job handler: %w", err)
	}
	cronH, err := factory.Batch().V1().CronJobs().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.asCronJob(obj, s.onCronJob) },
		UpdateFunc: func(_, obj any) { s.asCronJob(obj, s.onCronJob) },
		DeleteFunc: func(obj any) { s.asCronJob(tombstoneObj(obj), s.onCronJobDelete) },
	})
	if err != nil {
		return fmt.Errorf("workload: register cronjob handler: %w", err)
	}

	factory.Start(ctx.Done())
	// Shutdown blocks until every handler goroutine exits, upholding
	// the Source contract that emit is never called after Run returns.
	defer factory.Shutdown()

	if !cache.WaitForCacheSync(ctx.Done(), jobH.HasSynced, cronH.HasSynced) {
		return fmt.Errorf("workload: cache sync failed (informer stopped before initial list completed)")
	}
	s.mu.Lock()
	s.armed = true
	s.armedAt = s.clock()
	s.mu.Unlock()

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.send(s.sweep(s.clock()))
		}
	}
}

// tombstoneObj unwraps cache.DeletedFinalStateUnknown tombstones.
func tombstoneObj(obj any) any {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return t.Obj
	}
	return obj
}

func (s *Source) asJob(obj any, fn func(*batchv1.Job)) {
	if j, ok := obj.(*batchv1.Job); ok {
		fn(j)
	}
}

func (s *Source) asCronJob(obj any, fn func(*batchv1.CronJob)) {
	if c, ok := obj.(*batchv1.CronJob); ok {
		fn(c)
	}
}

// jobCondition returns the status of the named condition and its
// reason ("" / false when absent).
func jobCondition(j *batchv1.Job, t batchv1.JobConditionType) (reason string, isTrue bool) {
	for _, c := range j.Status.Conditions {
		if c.Type == t {
			return c.Reason, c.Status == corev1.ConditionTrue
		}
	}
	return "", false
}

// controllingCron returns the controlling CronJob owner, if any.
func controllingCron(j *batchv1.Job) (types.UID, string) {
	for _, ref := range j.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.Kind == "CronJob" {
			return ref.UID, ref.Name
		}
	}
	return "", ""
}

// ---- informer handlers ----

func (s *Source) onJob(j *batchv1.Job) {
	now := s.clock()
	failedReason, failed := jobCondition(j, batchv1.JobFailed)
	_, complete := jobCondition(j, batchv1.JobComplete)
	ownerUID, ownerName := controllingCron(j)

	s.mu.Lock()
	e, known := s.jobs[j.UID]
	if !known {
		e = &jobEntry{}
		s.jobs[j.UID] = e
	}
	wasFailed, wasComplete := e.failed, e.complete
	e.namespace, e.name = j.Namespace, j.Name
	e.ownerCron, e.ownerName = ownerUID, ownerName
	e.failed, e.complete = failed, complete
	armed := s.armed

	var sig *engine.Signal
	if armed && failed && !wasFailed && !e.fired {
		// The not-failed→failed transition, observed live. A Job
		// already failed at sync recorded failed=true un-armed and
		// never reaches here with wasFailed=false again.
		e.fired = true
		e.failedObservedAt = now
		sig = s.newJobFailed(j, failedReason, ownerName, now)
	}
	if armed && complete && !wasComplete && ownerUID != "" {
		// A sibling success: the job_failed clearance evidence.
		// Deliberately NOT a cron_missed consecutive reset — see
		// cronTrack.consecutive.
		s.successes[ownerUID] = &cronSuccess{observedAt: now}
	}
	s.mu.Unlock()

	if sig != nil {
		s.send([]engine.Signal{*sig})
	}
}

func (s *Source) onJobDelete(j *batchv1.Job) {
	s.mu.Lock()
	delete(s.jobs, j.UID)
	s.mu.Unlock()
}

func (s *Source) onCronJob(c *batchv1.CronJob) {
	now := s.clock()
	s.mu.Lock()
	e, known := s.crons[c.UID]
	if !known {
		e = &cronEntry{}
		s.crons[c.UID] = e
	}
	e.namespace, e.name = c.Namespace, c.Name
	e.schedule = c.Spec.Schedule
	e.timeZone = ""
	if c.Spec.TimeZone != nil {
		e.timeZone = *c.Spec.TimeZone
	}
	e.suspend = c.Spec.Suspend != nil && *c.Spec.Suspend
	e.lastSchedule = time.Time{}
	if c.Status.LastScheduleTime != nil {
		e.lastSchedule = c.Status.LastScheduleTime.Time
	}
	e.creation = c.CreationTimestamp.Time

	// Track the suspend transition and the schedule-advance
	// transition here (informer-driven, so StableSince stamps the
	// observation): the sweep only judges misses.
	tr := s.trackFor(c.UID)
	if e.suspend && !tr.prevSuspend {
		tr.suspendedAt = now
	}
	tr.prevSuspend = e.suspend
	if s.armed && known && e.lastSchedule.After(tr.prevLastSchedule) && tr.runObservedAt.IsZero() {
		// The first observed run since the latest miss (or since
		// arming) — the clearance StableSince. Later advances leave
		// it alone so a healthy high-frequency schedule cannot keep
		// restarting the §7.4 stability window. Gated on `known`: the
		// first population of a pre-existing CronJob's mirror is not
		// an observed run, whatever lastSchedule it arrives with.
		tr.runObservedAt = now
	}
	tr.prevLastSchedule = e.lastSchedule
	if !tr.lastMissed.IsZero() && !e.lastSchedule.Before(tr.lastMissed) {
		tr.consecutive = 0 // the schedule fired past the missed activation
	}
	s.mu.Unlock()
}

func (s *Source) onCronJobDelete(c *batchv1.CronJob) {
	s.mu.Lock()
	delete(s.crons, c.UID)
	delete(s.tracks, c.UID)
	delete(s.successes, c.UID)
	s.mu.Unlock()
}

// trackFor returns (creating if needed) a CronJob's miss memory.
// Called under s.mu.
func (s *Source) trackFor(uid types.UID) *cronTrack {
	tr, ok := s.tracks[uid]
	if !ok {
		tr = &cronTrack{}
		s.tracks[uid] = tr
	}
	return tr
}

// ---- the miss sweep ----

// cronSpec renders the robfig parse input, honoring spec.timeZone via
// the CRON_TZ prefix exactly as the upstream controller does.
func cronSpec(schedule, timeZone string) string {
	if timeZone != "" {
		return "CRON_TZ=" + timeZone + " " + schedule
	}
	return schedule
}

// sweep judges every CronJob's schedule. Returns the signals to emit
// (the caller sends them outside the lock). There is no TTL prune —
// see Config.TickInterval: evicting quiet LIVE objects would falsely
// resolve their incidents as object_deleted, and DeleteFunc owns
// removal.
func (s *Source) sweep(now time.Time) []engine.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []engine.Signal
	if s.armed {
		for uid, e := range s.crons {
			if sig := s.evalCron(uid, e, now); sig != nil {
				out = append(out, *sig)
			}
		}
	}
	return out
}

// evalCron judges one CronJob's schedule. Called under s.mu (and only
// when armed); returns the signal to emit, if any.
func (s *Source) evalCron(uid types.UID, e *cronEntry, now time.Time) *engine.Signal {
	if e.suspend {
		return nil // deliberate operator state, never a miss
	}
	tr := s.trackFor(uid)
	spec := cronSpec(e.schedule, e.timeZone)
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		// The API server accepted a spec this parser rejects —
		// near-impossible (both use the same grammar), but §2 forbids
		// silence. One line per revision of the spec.
		if tr.parseWarned != spec {
			tr.parseWarned = spec
			log.Printf("workload: CronJob %s/%s schedule %q unparseable (%v) — cron_missed detection disabled for it", e.namespace, e.name, spec, err)
		}
		return nil
	}

	// The anchor is the newest of: the last actual run, the CronJob's
	// creation, arming, and the last activation already called missed
	// — misses are judged forward from what this process has observed
	// (never backfilled across sentinel downtime), and each reported
	// miss advances the anchor so the NEXT activation is judged, not
	// the same one forever.
	anchor := e.lastSchedule
	if e.creation.After(anchor) {
		anchor = e.creation
	}
	if s.armedAt.After(anchor) {
		anchor = s.armedAt
	}
	if tr.lastMissed.After(anchor) {
		anchor = tr.lastMissed
	}
	expected := sched.Next(anchor)
	if expected.IsZero() || now.Before(expected.Add(s.cfg.Grace)) {
		return nil // nothing due yet (inside grace)
	}
	if !e.lastSchedule.Before(expected) {
		return nil // it ran; the informer handler resets the streak
	}
	// One judgment per activation: lastMissed feeds the anchor above,
	// so expected is always strictly after everything already fired.
	tr.lastMissed = expected
	tr.consecutive++
	tr.runObservedAt = time.Time{} // a new miss invalidates the last recovery stamp

	severity := engine.SeverityWarning
	if tr.consecutive >= CriticalMisses {
		severity = engine.SeverityCritical
	}
	msg := fmt.Sprintf(
		"CronJob missed its scheduled run at %s (schedule %q): no job created within the %s grace window",
		expected.UTC().Format(time.RFC3339), e.schedule, s.cfg.Grace)
	if tr.consecutive > 1 {
		msg += fmt.Sprintf("; consecutive_missed=%d", tr.consecutive)
	}
	if e.lastSchedule.IsZero() {
		msg += " last_schedule=never"
	} else {
		msg += fmt.Sprintf(" last_schedule=%s", e.lastSchedule.UTC().Format(time.RFC3339))
	}
	msg += " — the schedule is not producing runs (controller wedged, startingDeadlineSeconds starvation, or concurrencyPolicy=Forbid behind a stuck run)"

	sig := engine.Signal{
		Kind:     KindCronMissed,
		Source:   engine.SourceSentinel,
		Severity: severity,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: string(uid), Reason: ReasonCronMissed},
			Namespace:    e.namespace,
			KindOfObject: "CronJob",
			Name:         e.name,
			Message:      msg,
			FirstSeen:    now,
			LastSeen:     now,
			Count:        1,
		},
	}
	return &sig
}

// newJobFailed composes the workload.job_failed Signal. Called under
// s.mu.
func (s *Source) newJobFailed(j *batchv1.Job, reason, ownerName string, ts time.Time) *engine.Signal {
	if reason == "" {
		reason = "JobFailed"
	}
	msg := fmt.Sprintf("Job failed: %s (failed=%d succeeded=%d)", reason, j.Status.Failed, j.Status.Succeeded)
	if ownerName != "" {
		msg += fmt.Sprintf(" — run of CronJob %s; its next successful run clears this incident (§7.4)", ownerName)
	} else {
		msg += " — standalone Job: a Failed condition is terminal, the incident clears only when the Job is deleted"
	}
	sig := engine.Signal{
		Kind:     KindJobFailed,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityWarning,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: string(j.UID), Reason: ReasonJobFailed},
			Namespace:    j.Namespace,
			KindOfObject: "Job",
			Name:         j.Name,
			Message:      msg,
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
	}
	return &sig
}

// ---- §7.4 clearance ----

// Clearance implements engine.ClearanceObserver for this source's
// incidents. See the package comment for the semantics per kind.
// ok=false for other sources' incidents, or before the caches synced
// (cannot judge against an empty mirror).
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	switch engine.CanonicalReason(inc.Key.Reason) {
	case ReasonJobFailed:
		return s.jobClearance(inc)
	case ReasonCronMissed:
		return s.cronClearance(inc)
	}
	return engine.Clearance{}, false
}

func (s *Source) jobClearance(inc engine.Incident) (engine.Clearance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed {
		return engine.Clearance{}, false
	}
	e, ok := s.jobs[types.UID(inc.Key.UID)]
	if !ok {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	if e.ownerCron != "" {
		if suc, ok := s.successes[e.ownerCron]; ok && suc.observedAt.After(e.failedObservedAt) {
			return engine.Clearance{
				Cleared:     true,
				StableSince: suc.observedAt,
				Resolution:  engine.ResolutionRecovered,
			}, true
		}
	}
	// Still failed (a Failed condition never reverts): standalone
	// Jobs are terminal until deletion; CronJob children wait for a
	// sibling success.
	return engine.Clearance{Cleared: false, Resolution: engine.ResolutionRecovered}, true
}

func (s *Source) cronClearance(inc engine.Incident) (engine.Clearance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed {
		return engine.Clearance{}, false
	}
	uid := types.UID(inc.Key.UID)
	e, ok := s.crons[uid]
	if !ok {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	tr := s.tracks[uid]
	if e.suspend {
		var since time.Time
		if tr != nil {
			since = tr.suspendedAt
		}
		return engine.Clearance{
			Cleared:     true,
			StableSince: since,
			Resolution:  engine.ResolutionRecovered,
		}, true
	}
	// Two recovery shapes: the miss this process observed has been
	// run past (lastMissed known), or — the restart posture — the
	// incident predates this process (no track memory), in which case
	// ANY run observed since arming postdates the old miss and proves
	// the schedule is producing runs again. Without the second arm a
	// restored cron_missed incident could never clear as recovered
	// (only suspend/delete would exit it).
	cleared := false
	if tr != nil && !tr.lastMissed.IsZero() {
		cleared = !e.lastSchedule.Before(tr.lastMissed)
	} else {
		cleared = e.lastSchedule.After(s.armedAt)
	}
	if cleared {
		var since time.Time
		if tr != nil {
			since = tr.runObservedAt
		}
		return engine.Clearance{
			Cleared:     true,
			StableSince: since,
			Resolution:  engine.ResolutionRecovered,
		}, true
	}
	return engine.Clearance{Cleared: false, Resolution: engine.ResolutionRecovered}, true
}
