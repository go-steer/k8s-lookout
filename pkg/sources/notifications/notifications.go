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

// Package notifications is the cluster-notification signal source
// (post-M5 roadmap C.1, issue #130): the provider's own announcements
// about the cluster — upgrades starting, upgrades becoming available,
// security bulletins — read through the §2 cloud boundary (GKE: a
// Pub/Sub subscription on the cluster's notificationConfig topic).
// The single most useful correlation in real GKE triage is "the
// incident started four minutes after the node-pool upgrade began";
// before this source lookout could not say it.
//
// Three kinds (§7.3, APPEND-ONLY), severity-routed so §7.7 does the
// issue's routing table with no special cases:
//
//   - notification.upgrade (info → store): an upgrade is starting or
//     in progress on the control plane or a node pool. Store-recorded
//     evidence for incident-window correlation.
//   - notification.upgrade_available (info → store): a new version is
//     offered for auto-upgrade — the "pre-warning" half.
//   - notification.security_bulletin (warning → watchboard): a
//     bulletin affecting this cluster; batched into the watchboard
//     digest, zero false-positive cost.
//
// Like the quota source, this is explicitly enabled, never auto: it
// needs a provider with the notifications capability AND an
// operator-created subscription (--notifications-subscription), and
// construction fails LOUDLY without them (§2/§11 — no degraded mode).
// Scope() is Project: one subscription serves the project's topic,
// and a topic may carry notifications for several clusters — every
// signal names its cluster.
//
// Staleness: a pre-existing subscription can hold a backlog.
// UPGRADE events older than Config.StaleAfter at receipt are dropped
// loudly — replaying last week's completed upgrade as a live signal
// would be worse than silence. Security bulletins are EXEMPT: a
// bulletin published while the sentinel was down is exactly what the
// Pub/Sub backlog exists to preserve, and durable awareness on the
// watchboard is the point — age rides in the message instead. Drops
// log immediately (first of each class, then every 100th) plus a
// Run-exit summary; a source built to run for months cannot save its
// reporting for shutdown (§2).
//
// No §7.4 clearance observer: these are point-in-time facts. Info
// signals route to the store without opening sessions, and the
// watchboard digest carries its own lifecycle.
package notifications

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "notifications"

// The kinds this source emits (§7.3). APPEND-ONLY.
const (
	KindUpgrade          = "notification.upgrade"
	KindUpgradeAvailable = "notification.upgrade_available"
	KindSecurityBulletin = "notification.security_bulletin"
)

// The dedup/fingerprint reasons (kind suffixes, the source
// convention). All map to themselves under CanonicalReason.
const (
	ReasonUpgrade          = "upgrade"
	ReasonUpgradeAvailable = "upgrade_available"
	ReasonSecurityBulletin = "security_bulletin"
)

// The provider event types this source understands (the tail of
// GKE's type_url). Unknown types are counted and dropped loudly —
// never silently (§2).
const (
	typeUpgrade          = "UpgradeEvent"
	typeUpgradeAvailable = "UpgradeAvailableEvent"
	typeSecurityBulletin = "SecurityBulletinEvent"
)

// Config are the source's thresholds. Zero values take the defaults.
type Config struct {
	// StaleAfter drops notifications older than this at receipt — a
	// pre-existing subscription's backlog is history, not live
	// signal. Default 1h.
	StaleAfter time.Duration
}

// DefaultConfig returns the shipped thresholds.
func DefaultConfig() Config {
	return Config{StaleAfter: time.Hour}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.StaleAfter <= 0 {
		c.StaleAfter = d.StaleAfter
	}
	return c
}

// Source implements sources.Source for the cluster-notification row.
type Source struct {
	api cloud.NotificationsAPI
	cfg Config

	// dropped counts stale + unknown-type messages for the Run-exit
	// summary line (§2: bounded coverage is reported, not implied).
	droppedStale   atomic.Int64
	droppedUnknown atomic.Int64

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// New constructs the source. Like the quota source, construction
// fails loudly when the provider cannot serve notifications — there
// is no degraded mode for an explicitly enabled project-tier source.
func New(provider cloud.Provider, cfg Config) (*Source, error) {
	if provider == nil {
		provider = cloud.NoProvider
	}
	api, ok := provider.Notifications()
	if !ok {
		u := cloud.Unavailable(provider, cloud.CapabilityNotifications)
		return nil, fmt.Errorf(
			"source %q requires a cloud provider with the %s capability (Scope()=%s — one subscription per project's notification topic, issue #130): %s; build with -tags gke/allproviders, configure --notifications-subscription, or drop %q from --sources",
			Name, cloud.CapabilityNotifications, sources.ScopeProject, u.Marker(), Name)
	}
	return &Source{api: api, cfg: cfg.normalize()}, nil
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: one subscription per project's
// notification topic; no Kubernetes RBAC at all.
func (s *Source) Scope() sources.Scope { return sources.ScopeProject }

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Run implements sources.Source: a blocking receive on the provider
// stream until ctx is cancelled.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	err := s.api.Receive(ctx, func(n cloud.ClusterNotification) {
		if sig, ok := s.translate(n); ok {
			emit(sig)
		}
	})
	if stale, unknown := s.droppedStale.Load(), s.droppedUnknown.Load(); stale > 0 || unknown > 0 {
		log.Printf("notifications: dropped %d stale (older than %s at receipt) and %d unknown-type messages over this run", stale, s.cfg.StaleAfter, unknown)
	}
	if ctx.Err() != nil {
		return nil // clean shutdown
	}
	return err
}

// translate maps one provider notification to its signal. ok=false
// drops it (stale upgrade or unknown/stray type), counted AND logged
// (first per class, then every 100th — §2, see the package comment).
func (s *Source) translate(n cloud.ClusterNotification) (engine.Signal, bool) {
	var zero engine.Signal
	now := s.clock()

	var kind, reason string
	severity := engine.SeverityInfo
	switch n.Type {
	case typeUpgrade:
		kind, reason = KindUpgrade, ReasonUpgrade
	case typeUpgradeAvailable:
		kind, reason = KindUpgradeAvailable, ReasonUpgradeAvailable
	case typeSecurityBulletin:
		kind, reason = KindSecurityBulletin, ReasonSecurityBulletin
		severity = engine.SeverityWarning
	default:
		if c := s.droppedUnknown.Add(1); c == 1 || c%100 == 0 {
			log.Printf("notifications: dropped unknown-type message %d (type %q from %s/%s) — a stray topic or an event type newer than this build", c, n.Type, n.Location, n.Cluster)
		}
		return zero, false
	}
	// Staleness applies to upgrade kinds only: a backlog bulletin is
	// preserved awareness, not replay (see the package comment).
	if kind != KindSecurityBulletin && !n.Time.IsZero() && now.Sub(n.Time) > s.cfg.StaleAfter {
		if c := s.droppedStale.Add(1); c == 1 || c%100 == 0 {
			log.Printf("notifications: dropped stale message %d (%s from %s/%s, published %s ago > %s)", c, n.Type, n.Location, n.Cluster, now.Sub(n.Time).Truncate(time.Second), s.cfg.StaleAfter)
		}
		return zero, false
	}

	ts := n.Time
	if ts.IsZero() {
		ts = now
	}
	return engine.Signal{
		Kind:     kind,
		Source:   engine.SourceSentinel,
		Severity: severity,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: signalUID(n, reason), Reason: reason},
			KindOfObject: "Cluster",
			Name:         n.Cluster,
			Message:      message(n),
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
	}, true
}

// signalUID is the dedup key. Each distinct provider event gets its
// own incident identity: bulletins by bulletin ID AND cluster — a
// bulletin affecting three clusters on a shared project topic is
// three watchboard entries, one per cluster, because collapsing them
// would leave only the race-winning cluster's name on the record and
// erase the others' exposure; redeliveries for the SAME cluster still
// dedup. Upgrades key by operation ID when the payload carries one,
// else by cluster+type+resource so concurrent control-plane and
// node-pool upgrades stay distinct.
func signalUID(n cloud.ClusterNotification, reason string) string {
	if id := n.Attributes["bulletinId"]; id != "" && reason == ReasonSecurityBulletin {
		return "bulletin:" + id + "/" + n.Cluster
	}
	if op := n.Attributes["operation"]; op != "" {
		return "upgrade-op:" + op
	}
	return fmt.Sprintf("notification:%s/%s/%s/%s", n.Location, n.Cluster, reason, n.Attributes["resourceType"])
}

// message renders the signal message: the provider's description
// first, then the payload fields that matter for triage, sorted for
// determinism. Provider-authored text — the §6.5 sanitizer and §7.8
// framing apply downstream like every other signal.
func message(n cloud.ClusterNotification) string {
	msg := n.Message
	if msg == "" {
		msg = n.Type
	}
	var kv []string
	for k, v := range n.Attributes {
		if v == "" {
			continue
		}
		switch k {
		case "resourceType", "operation", "currentVersion", "targetVersion", "version", "bulletinId", "severity", "cveIds", "bulletinUri", "resource":
			kv = append(kv, k+"="+v)
		}
	}
	sort.Strings(kv)
	if len(kv) > 0 {
		msg += " (" + strings.Join(kv, " ") + ")"
	}
	if n.Location != "" {
		msg += " location=" + n.Location
	}
	return msg
}
