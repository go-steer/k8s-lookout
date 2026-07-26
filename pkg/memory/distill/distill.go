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

// Package distill is the §9.2 distiller: the scheduled pass in the
// sentinel that converts recurring raw occurrences (pkg/store) into
// durable distilled facts (pkg/memory) — "us-east1-b nodegroup
// n2d-pool: 3 stockouts in 7d". It reads the occurrence window once
// per pass and writes through the memory.FactWriter interface;
// dedupe is the writer's job (same class + scope updates the
// existing fact).
//
// # Predicates
//
// Three patterns ship first, each documented at its matcher below:
//
//   - capacity.stockout.recurrence — the same (cluster, zone,
//     nodegroup) reported capacity.stockout at least StockoutMin
//     times inside the window. Every stored stockout row counts,
//     dedup-suppressed ones included: each row is one autoscaler
//     decision record, and the recurrence RATE is the fact.
//   - workload.crashloop.recurrence — the same workload re-opened at
//     least WorkloadMinIncidents crash-grade incidents (canonical
//     reasons CrashLoopBackOff / OOMKilling / OOMKilled; rows that
//     routed as fresh incidents, not dedup followups) totalling at
//     least WorkloadMinOccurrences occurrences inside the window.
//     Requiring multiple FRESH incidents is what separates "keeps
//     coming back" (a memory) from one long incident (a session).
//   - expiry.renewal_failure.recurrence — the same issuer produced at
//     least RenewalMinFailures cert-renewal-failure occurrences
//     (kind expiry.warning with the source's renewal=FAILED marker)
//     across at least RenewalMinObjects distinct certificates inside
//     the window. The distinct-object floor is the point: one
//     misconfigured Certificate is an incident; two certificates
//     failing under one issuer is a fact about the ISSUER.
package distill

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// Fact classes this distiller produces. Append-only.
const (
	ClassStockout       = "capacity.stockout.recurrence"
	ClassCrashloop      = "workload.crashloop.recurrence"
	ClassRenewalFailure = "expiry.renewal_failure.recurrence"
)

// Defaults for Config. The window is the design's "this week"; the
// thresholds are deliberately conservative — a distilled fact is a
// standing recommendation input, and a fact that fires on noise
// poisons every downstream consumer.
const (
	DefaultWindow                 = 7 * 24 * time.Hour
	DefaultStockoutMin            = 3
	DefaultWorkloadMinIncidents   = 2
	DefaultWorkloadMinOccurrences = 5
	DefaultRenewalMinFailures     = 3
	DefaultRenewalMinObjects      = 2
)

// Config tunes one distiller pass. The zero value takes every
// default.
type Config struct {
	// Window is the evidence lookback (default DefaultWindow, 7d).
	// Bounded above by the store's own TTL in practice.
	Window time.Duration
	// StockoutMin is the per-(cluster,zone,nodegroup) occurrence
	// floor for ClassStockout.
	StockoutMin int
	// WorkloadMinIncidents / WorkloadMinOccurrences are the
	// fresh-incident and total-occurrence floors for ClassCrashloop.
	WorkloadMinIncidents   int
	WorkloadMinOccurrences int
	// RenewalMinFailures / RenewalMinObjects are the occurrence and
	// distinct-certificate floors for ClassRenewalFailure.
	RenewalMinFailures int
	RenewalMinObjects  int
	// Now overrides the pass clock (window end). Nil = time.Now.
	Now func() time.Time
}

func (c Config) withDefaults() Config {
	if c.Window <= 0 {
		c.Window = DefaultWindow
	}
	if c.StockoutMin <= 0 {
		c.StockoutMin = DefaultStockoutMin
	}
	if c.WorkloadMinIncidents <= 0 {
		c.WorkloadMinIncidents = DefaultWorkloadMinIncidents
	}
	if c.WorkloadMinOccurrences <= 0 {
		c.WorkloadMinOccurrences = DefaultWorkloadMinOccurrences
	}
	if c.RenewalMinFailures <= 0 {
		c.RenewalMinFailures = DefaultRenewalMinFailures
	}
	if c.RenewalMinObjects <= 0 {
		c.RenewalMinObjects = DefaultRenewalMinObjects
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// OccurrenceSource is the slice of the store's query surface the
// distiller reads — satisfied by *store.Store (nil-safe: a disabled
// store yields nothing and the pass writes nothing).
type OccurrenceSource interface {
	Recent(ctx context.Context, since time.Time, limit int) iter.Seq2[store.Occurrence, error]
}

// Stats reports one pass.
type Stats struct {
	// Scanned is how many occurrences the window held.
	Scanned int
	// Written counts facts upserted, per class.
	Written map[string]int
}

// Total returns the pass's total facts written.
func (s Stats) Total() int {
	n := 0
	for _, v := range s.Written {
		n += v
	}
	return n
}

// Run executes one distiller pass: read the occurrence window from
// src, evaluate every predicate, and upsert the resulting facts
// through dst. A write failure aborts the pass (the next scheduled
// pass re-derives everything — the predicates are pure functions of
// the window, so a lost pass loses freshness, never facts).
func Run(ctx context.Context, src OccurrenceSource, dst memory.FactWriter, cfg Config) (Stats, error) {
	cfg = cfg.withDefaults()
	now := cfg.Now().UTC()
	since := now.Add(-cfg.Window)

	stockouts := map[string]*group{}
	workloads := map[string]*group{}
	renewals := map[string]*group{}

	stats := Stats{Written: map[string]int{}}
	for occ, err := range src.Recent(ctx, since, 0) {
		if err != nil {
			return stats, fmt.Errorf("distill: reading occurrences: %w", err)
		}
		stats.Scanned++
		switch {
		case occ.Kind == "capacity.stockout":
			bucket(stockouts, stockoutScope(occ)).add(occ, false)
		case crashloopReasons[occ.CanonicalReason] && occ.Kind != "resolved" && occ.Kind != "resolved.reverted":
			bucket(workloads, crashloopScope(occ)).add(occ, freshRoutes[occ.Route])
		case occ.Kind == "expiry.warning" && strings.Contains(occ.Message, "renewal=FAILED"):
			bucket(renewals, renewalScope(occ)).add(occ, false)
		}
	}

	write := func(class string, f memory.DistilledFact) error {
		f.Class = class
		f.WindowStart = since
		f.WindowEnd = now
		if _, err := dst.UpsertFact(ctx, f); err != nil {
			return fmt.Errorf("distill: writing %s fact for %s: %w", class, memory.ScopeKey(f.Scope), err)
		}
		stats.Written[class]++
		return nil
	}

	for _, g := range sorted(stockouts) {
		if g.count < cfg.StockoutMin {
			continue
		}
		if err := write(ClassStockout, g.fact(stockoutStatement(g, cfg.Window))); err != nil {
			return stats, err
		}
	}
	for _, g := range sorted(workloads) {
		if g.fresh < cfg.WorkloadMinIncidents || g.count < cfg.WorkloadMinOccurrences {
			continue
		}
		if err := write(ClassCrashloop, g.fact(crashloopStatement(g, cfg.Window))); err != nil {
			return stats, err
		}
	}
	for _, g := range sorted(renewals) {
		if g.count < cfg.RenewalMinFailures || len(g.objects) < cfg.RenewalMinObjects {
			continue
		}
		if err := write(ClassRenewalFailure, g.fact(renewalStatement(g, cfg.Window))); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

// crashloopReasons are the canonical (engine.CanonicalReason) crash-
// grade families ClassCrashloop watches. CrashLoopBackOff already
// absorbs BackOff and objectstate.restart_burst via the dedup
// canonical map; OOMKilling/OOMKilled are the kernel/kubelet OOM
// variants.
var crashloopReasons = map[string]bool{
	"CrashLoopBackOff": true,
	"OOMKilling":       true,
	"OOMKilled":        true,
}

// freshRoutes are the routing outcomes that mark a FRESH incident
// (post-dedup): the dedup window expired and the symptom came back.
// Suppressed rows still count as occurrences, but only fresh rows
// count as incidents.
var freshRoutes = map[store.RouteOutcome]bool{
	store.RouteInjected:    true,
	store.RouteStorm:       true,
	store.RouteStormMember: true,
	store.RouteWatchboard:  true,
}

// group accumulates one scope's evidence.
type group struct {
	scope        map[string]string
	count, fresh int
	first, last  time.Time
	objects      map[string]bool
	fingerprints map[string]bool
	// reason keeps the group's canonical reason for statements
	// (crashloop groups are homogeneous by construction: the reason
	// class is a scope dimension).
	reason string
}

func bucket(m map[string]*group, scope map[string]string) *group {
	key := memory.ScopeKey(scope)
	g, ok := m[key]
	if !ok {
		g = &group{scope: scope, objects: map[string]bool{}, fingerprints: map[string]bool{}}
		m[key] = g
	}
	return g
}

func (g *group) add(occ store.Occurrence, fresh bool) {
	g.count++
	if fresh {
		g.fresh++
	}
	at := occ.EmittedAt
	if g.first.IsZero() || at.Before(g.first) {
		g.first = at
	}
	if at.After(g.last) {
		g.last = at
	}
	if occ.UID != "" {
		g.objects[occ.UID] = true
	}
	if occ.Fingerprint != "" {
		g.fingerprints[occ.Fingerprint] = true
	}
	g.reason = occ.CanonicalReason
}

func (g *group) fact(statement string) memory.DistilledFact {
	fps := make([]string, 0, len(g.fingerprints))
	for fp := range g.fingerprints {
		fps = append(fps, fp)
	}
	sort.Strings(fps)
	return memory.DistilledFact{
		Scope:              g.scope,
		Statement:          statement,
		Occurrences:        g.count,
		DistinctObjects:    len(g.objects),
		FirstSeen:          g.first,
		LastSeen:           g.last,
		SourceFingerprints: fps,
	}
}

// sorted returns the groups in deterministic scope-key order so a
// pass writes facts (and tests observe writes) in a stable order.
func sorted(m map[string]*group) []*group {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*group, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}
	return out
}

// ---- scope + statement composition ----------------------------------------

// rawContext is the slice of the stored Signal JSON (the raw blob)
// the distiller needs beyond the flat columns. Field names are the
// engine.Signal Go names (the blob is a plain json.Marshal).
type rawContext struct {
	Zone          string            `json:"Zone"`
	Project       string            `json:"Project"`
	ControllerRef string            `json:"ControllerRef"`
	Labels        map[string]string `json:"Labels"`
}

func parseRaw(occ store.Occurrence) rawContext {
	var rc rawContext
	_ = json.Unmarshal(occ.Raw, &rc)
	return rc
}

func stockoutScope(occ store.Occurrence) map[string]string {
	rc := parseRaw(occ)
	return map[string]string{
		memory.ScopeProject:   rc.Project,
		memory.ScopeCluster:   occ.Cluster,
		memory.ScopeZone:      rc.Zone,
		memory.ScopeNodeGroup: occ.Name,
	}
}

func stockoutStatement(g *group, window time.Duration) string {
	where := g.scope[memory.ScopeZone]
	if where == "" {
		where = g.scope[memory.ScopeCluster]
	}
	prefix := "nodegroup " + g.scope[memory.ScopeNodeGroup]
	if where != "" {
		prefix = where + " " + prefix
	}
	return fmt.Sprintf("%s: %d stockouts in %s (first %s, last %s)",
		prefix, g.count, fmtWindow(window),
		g.first.UTC().Format(time.RFC3339), g.last.UTC().Format(time.RFC3339))
}

func crashloopScope(occ store.Occurrence) map[string]string {
	return map[string]string{
		memory.ScopeCluster:   occ.Cluster,
		memory.ScopeNamespace: occ.Namespace,
		memory.ScopeWorkload:  workloadKey(occ),
		memory.ScopeReason:    occ.CanonicalReason,
	}
}

func crashloopStatement(g *group, window time.Duration) string {
	subject := g.scope[memory.ScopeWorkload]
	if ns := g.scope[memory.ScopeNamespace]; ns != "" {
		subject = ns + "/" + subject
	}
	return fmt.Sprintf("%s: %d fresh incidents, %d %s occurrences across %d pods in %s",
		subject, g.fresh, g.count, g.reason, len(g.objects), fmtWindow(window))
}

// workloadKey derives the "per workload" grouping key for a
// crash-grade occurrence, best-signal first:
//
//  1. the Signal's ControllerRef when a source populated it;
//  2. the app.kubernetes.io/name or app label from the stored
//     labels;
//  3. the object name with generated pod suffixes stripped
//     (ReplicaSet pod hash + random suffix, StatefulSet ordinal) —
//     a documented heuristic, deterministic but best-effort;
//  4. the object name verbatim.
func workloadKey(occ store.Occurrence) string {
	rc := parseRaw(occ)
	if rc.ControllerRef != "" {
		return rc.ControllerRef
	}
	if v := rc.Labels["app.kubernetes.io/name"]; v != "" {
		return v
	}
	if v := rc.Labels["app"]; v != "" {
		return v
	}
	if occ.KindOfObject == "Pod" {
		return stripPodSuffix(occ.Name)
	}
	return occ.Name
}

var (
	// ordinalSuffix: StatefulSet pod ordinals ("payment-0").
	ordinalSuffix = regexp.MustCompile(`-[0-9]+$`)
	// randomSuffix: the kubelet's 5-char generated pod suffix
	// ("payment-7d5b9c6f4-x2x9k"). The generator's alphabet excludes
	// vowels and easily-confused characters.
	randomSuffix = regexp.MustCompile(`-[bcdfghjklmnpqrstvwxz0-9]{5}$`)
	// templateHash: the ReplicaSet pod-template-hash segment that
	// precedes the random suffix (same generator alphabet, longer).
	templateHash = regexp.MustCompile(`-[bcdfghjklmnpqrstvwxz0-9]{8,10}$`)
)

func stripPodSuffix(name string) string {
	if m := ordinalSuffix.FindString(name); m != "" && len(m) < len(name) {
		return name[:len(name)-len(m)]
	}
	if m := randomSuffix.FindString(name); m != "" && len(m) < len(name) {
		name = name[:len(name)-len(m)]
		if h := templateHash.FindString(name); h != "" && len(h) < len(name) {
			name = name[:len(name)-len(h)]
		}
	}
	return name
}

func renewalScope(occ store.Occurrence) map[string]string {
	return map[string]string{
		memory.ScopeCluster: occ.Cluster,
		memory.ScopeIssuer:  issuerOf(occ.Message),
	}
}

func renewalStatement(g *group, window time.Duration) string {
	return fmt.Sprintf("issuer %s: %d cert-renewal failures across %d certificates in %s",
		g.scope[memory.ScopeIssuer], g.count, len(g.objects), fmtWindow(window))
}

// issuerOf extracts the expiry source's "issuer=<CN>" message token.
// The source writes "-" for an unknown issuer; both that and a
// missing token normalize to "unknown" so the scope key is never
// empty.
func issuerOf(message string) string {
	const marker = "issuer="
	i := strings.Index(message, marker)
	if i < 0 {
		return "unknown"
	}
	rest := message[i+len(marker):]
	if j := strings.IndexAny(rest, " ;"); j >= 0 {
		rest = rest[:j]
	}
	if rest == "" || rest == "-" {
		return "unknown"
	}
	return rest
}

// fmtWindow renders the evidence window compactly: whole days as
// "Nd", otherwise Go duration syntax.
func fmtWindow(d time.Duration) string {
	if d > 0 && d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	}
	return d.String()
}
