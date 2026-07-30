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

//go:build gke || allproviders

package gke

import (
	"context"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

func TestSubscriptionID(t *testing.T) {
	for _, tc := range []struct {
		configured            string
		wantProject, wantName string
	}{
		{"projects/other-proj/subscriptions/gke-notes", "other-proj", "gke-notes"},
		{"gke-notes", "proj-1", "gke-notes"},
	} {
		p, n := subscriptionID("proj-1", tc.configured)
		if p != tc.wantProject || n != tc.wantName {
			t.Errorf("subscriptionID(%q) = (%s, %s), want (%s, %s)", tc.configured, p, n, tc.wantProject, tc.wantName)
		}
	}
}

// fakeNotificationReceiver replays canned messages.
type fakeNotificationReceiver struct {
	msgs []pubsubMessage
}

func (f *fakeNotificationReceiver) Receive(_ context.Context, handle func(pubsubMessage)) error {
	for _, m := range f.msgs {
		handle(m)
	}
	return nil
}

// upgradeMsg is the documented GKE UpgradeEvent shape (authored from
// the GKE cluster-notifications reference; §13: not captured live).
var upgradeMsg = pubsubMessage{
	PublishTime: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
	Data:        []byte("Master is upgrading to version 1.32.4-gke.1000."),
	Attributes: map[string]string{
		"type_url":         "type.googleapis.com/google.container.v1beta1.UpgradeEvent",
		"cluster_name":     "prod",
		"cluster_location": "us-east1",
		"project_id":       "1234567890",
		"payload":          `{"resourceType":"MASTER","operation":"operation-175h-abc","currentVersion":"1.31.9-gke.10","targetVersion":"1.32.4-gke.1000","resource":"projects/p/locations/us-east1/clusters/prod"}`,
	},
}

func TestNotificationsReceiveTranslation(t *testing.T) {
	api := &notificationsAPI{receiver: &fakeNotificationReceiver{msgs: []pubsubMessage{
		upgradeMsg,
		// A stray message from the wrong topic: no type_url → dropped.
		{Data: []byte("not a notification"), Attributes: map[string]string{"foo": "bar"}},
	}}}
	var got []cloud.ClusterNotification
	if err := api.Receive(context.Background(), func(n cloud.ClusterNotification) { got = append(got, n) }); err != nil {
		t.Fatal(err)
	}
	// The stray flows through with Type="" — the SOURCE counts it as
	// unknown; the adapter never classifies silently (§2).
	if len(got) != 2 {
		t.Fatalf("delivered %d notifications, want 2 (stray passes with empty Type): %+v", len(got), got)
	}
	if got[1].Type != "" {
		t.Errorf("stray Type = %q, want empty", got[1].Type)
	}
	n := got[0]
	if n.Type != "UpgradeEvent" {
		t.Errorf("Type = %q", n.Type)
	}
	if n.Cluster != "prod" || n.Location != "us-east1" {
		t.Errorf("identity = %s/%s", n.Location, n.Cluster)
	}
	if n.Attributes["operation"] != "operation-175h-abc" || n.Attributes["targetVersion"] != "1.32.4-gke.1000" {
		t.Errorf("payload fields = %+v", n.Attributes)
	}
	if n.Message != "Master is upgrading to version 1.32.4-gke.1000." {
		t.Errorf("Message = %q", n.Message)
	}
	if !n.Time.Equal(upgradeMsg.PublishTime) {
		t.Errorf("Time = %v", n.Time)
	}
}

func TestNotificationsCapabilityGates(t *testing.T) {
	withSub := &Provider{project: "proj-1", location: "us-east1", cluster: "prod", subscription: "gke-notes"}
	if _, ok := withSub.Notifications(); !ok {
		t.Error("subscription + project: Notifications() unavailable, want available")
	}
	noSub := &Provider{project: "proj-1", location: "us-east1", cluster: "prod"}
	if _, ok := noSub.Notifications(); ok {
		t.Error("no subscription: Notifications() available, want unavailable")
	}
	if s := noSub.capabilityStatus(cloud.CapabilityNotifications); s.Reason != reasonNoSubscription {
		t.Errorf("no-subscription reason = %q", s.Reason)
	}
	noProj := &Provider{subscription: "gke-notes"}
	if s := noProj.capabilityStatus(cloud.CapabilityNotifications); s.Reason != reasonNoProject {
		t.Errorf("no-project reason = %q", s.Reason)
	}
}
