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

package notifications

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

var testNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// notesProvider is cloud.NoProvider with the notifications capability
// grafted on (§13: embed the sentinel, override one getter).
type notesProvider struct {
	cloud.Provider
	api cloud.NotificationsAPI
}

func (p notesProvider) Notifications() (cloud.NotificationsAPI, bool) { return p.api, p.api != nil }

// fakeAPI replays canned notifications through Receive.
type fakeAPI struct {
	notes []cloud.ClusterNotification
}

func (f *fakeAPI) Receive(_ context.Context, handle func(cloud.ClusterNotification)) error {
	for _, n := range f.notes {
		handle(n)
	}
	return nil
}

func newTestSource(t *testing.T, api cloud.NotificationsAPI) *Source {
	t.Helper()
	s, err := New(notesProvider{Provider: cloud.NoProvider, api: api}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return testNow }
	return s
}

func run(t *testing.T, s *Source) []engine.Signal {
	t.Helper()
	var got []engine.Signal
	if err := s.Run(context.Background(), func(sig engine.Signal) { got = append(got, sig) }); err != nil {
		t.Fatal(err)
	}
	return got
}

func upgradeNote(cluster, op string) cloud.ClusterNotification {
	return cloud.ClusterNotification{
		Time: testNow.Add(-5 * time.Minute), Type: "UpgradeEvent",
		Cluster: cluster, Location: "us-east1",
		Attributes: map[string]string{
			"resourceType": "MASTER", "operation": op,
			"currentVersion": "1.31.9-gke.10", "targetVersion": "1.32.4-gke.1000",
		},
		Message: "Master is upgrading to version 1.32.4-gke.1000.",
	}
}

func TestTranslateUpgrade(t *testing.T) {
	s := newTestSource(t, &fakeAPI{notes: []cloud.ClusterNotification{upgradeNote("prod", "op-1")}})
	sigs := run(t, s)
	if len(sigs) != 1 {
		t.Fatalf("got %d signals, want 1", len(sigs))
	}
	sig := sigs[0]
	if sig.Kind != KindUpgrade || sig.Key.Reason != ReasonUpgrade {
		t.Errorf("identity = %s/%s", sig.Kind, sig.Key.Reason)
	}
	if sig.Severity != engine.SeverityInfo {
		t.Errorf("severity = %s, want info (store-only routing)", sig.Severity)
	}
	if sig.Key.UID != "upgrade-op:op-1" {
		t.Errorf("UID = %q, want the operation key", sig.Key.UID)
	}
	if sig.KindOfObject != "Cluster" || sig.Name != "prod" {
		t.Errorf("object = %s/%s", sig.KindOfObject, sig.Name)
	}
	for _, want := range []string{"Master is upgrading", "operation=op-1", "targetVersion=1.32.4-gke.1000", "location=us-east1"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message missing %q: %s", want, sig.Message)
		}
	}
	if !sig.FirstSeen.Equal(testNow.Add(-5 * time.Minute)) {
		t.Errorf("FirstSeen = %v, want the event time", sig.FirstSeen)
	}
}

func TestTranslateBulletinAndAvailable(t *testing.T) {
	s := newTestSource(t, &fakeAPI{notes: []cloud.ClusterNotification{
		{
			Time: testNow.Add(-time.Minute), Type: "SecurityBulletinEvent",
			Cluster: "prod", Location: "us-east1",
			Attributes: map[string]string{"bulletinId": "gcp-2026-777", "severity": "High", "cveIds": `["CVE-2026-1234"]`},
			Message:    "A new security bulletin affects this cluster.",
		},
		{
			Time: testNow.Add(-time.Minute), Type: "UpgradeAvailableEvent",
			Cluster: "prod", Location: "us-east1",
			Attributes: map[string]string{"resourceType": "MASTER", "version": "1.33.0-gke.100"},
		},
	}})
	sigs := run(t, s)
	if len(sigs) != 2 {
		t.Fatalf("got %d signals, want 2", len(sigs))
	}
	bulletin, avail := sigs[0], sigs[1]
	if bulletin.Kind != KindSecurityBulletin || bulletin.Severity != engine.SeverityWarning {
		t.Errorf("bulletin = %s/%s, want warning (watchboard routing)", bulletin.Kind, bulletin.Severity)
	}
	if bulletin.Key.UID != "bulletin:gcp-2026-777/prod" {
		t.Errorf("bulletin UID = %q", bulletin.Key.UID)
	}
	if avail.Kind != KindUpgradeAvailable || avail.Severity != engine.SeverityInfo {
		t.Errorf("available = %s/%s, want info", avail.Kind, avail.Severity)
	}
	if avail.Key.UID != "notification:us-east1/prod/upgrade_available/MASTER" {
		t.Errorf("available UID = %q (no operation → composite key)", avail.Key.UID)
	}
}

func TestStaleAndUnknownDropped(t *testing.T) {
	s := newTestSource(t, &fakeAPI{notes: []cloud.ClusterNotification{
		// A backlog upgrade from yesterday: stale, dropped.
		{Time: testNow.Add(-25 * time.Hour), Type: "UpgradeEvent", Cluster: "prod"},
		// An event type this source does not understand yet.
		{Time: testNow.Add(-time.Minute), Type: "FutureEvent", Cluster: "prod"},
		// A stray from the wrong topic (adapter passes it through
		// with an empty Type): counted as unknown.
		{Time: testNow.Add(-time.Minute), Type: "", Cluster: "prod"},
		upgradeNote("prod", "op-2"),
	}})
	sigs := run(t, s)
	if len(sigs) != 1 || sigs[0].Key.UID != "upgrade-op:op-2" {
		t.Fatalf("got %+v, want only the live upgrade", sigs)
	}
	if s.droppedStale.Load() != 1 || s.droppedUnknown.Load() != 2 {
		t.Errorf("drop counters = stale:%d unknown:%d, want 1/2", s.droppedStale.Load(), s.droppedUnknown.Load())
	}
}

func TestStaleBulletinStillDelivered(t *testing.T) {
	// A bulletin published during sentinel downtime is exactly what
	// the Pub/Sub backlog exists to preserve — the stale drop applies
	// to upgrade kinds only (the review finding on destroyed
	// bulletins).
	s := newTestSource(t, &fakeAPI{notes: []cloud.ClusterNotification{{
		Time: testNow.Add(-25 * time.Hour), Type: "SecurityBulletinEvent",
		Cluster: "prod", Location: "us-east1",
		Attributes: map[string]string{"bulletinId": "gcp-2026-778"},
		Message:    "A new security bulletin affects this cluster.",
	}}})
	sigs := run(t, s)
	if len(sigs) != 1 || sigs[0].Kind != KindSecurityBulletin {
		t.Fatalf("stale bulletin dropped: %+v", sigs)
	}
	if s.droppedStale.Load() != 0 {
		t.Errorf("stale counter = %d, want 0", s.droppedStale.Load())
	}
}

func TestDistinctUpgradesStayDistinct(t *testing.T) {
	// Concurrent control-plane and node-pool upgrades on one cluster
	// must not dedup into one incident identity.
	nodePool := upgradeNote("prod", "")
	nodePool.Attributes = map[string]string{"resourceType": "NODE_POOL", "resource": "projects/p/.../nodePools/default"}
	master := upgradeNote("prod", "")
	master.Attributes = map[string]string{"resourceType": "MASTER"}
	s := newTestSource(t, &fakeAPI{notes: []cloud.ClusterNotification{nodePool, master}})
	sigs := run(t, s)
	if len(sigs) != 2 {
		t.Fatalf("got %d signals, want 2", len(sigs))
	}
	if sigs[0].Key.UID == sigs[1].Key.UID {
		t.Errorf("node-pool and master upgrades share UID %q", sigs[0].Key.UID)
	}
}

func TestNewFailsLoudlyWithoutCapability(t *testing.T) {
	_, err := New(cloud.NoProvider, Config{})
	if err == nil {
		t.Fatal("New succeeded without the notifications capability")
	}
	for _, want := range []string{Name, "notifications", "--sources"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

func TestScopeAndName(t *testing.T) {
	s := newTestSource(t, &fakeAPI{})
	if s.Name() != "notifications" || s.Scope().String() != "project" {
		t.Errorf("identity = %s/%s", s.Name(), s.Scope())
	}
}
