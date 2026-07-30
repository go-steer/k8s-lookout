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

// NotificationsAPI implementation (post-M5 #130): the GKE cluster
// notification stream. GKE publishes UpgradeEvent /
// UpgradeAvailableEvent / SecurityBulletinEvent messages to the
// cluster's notificationConfig Pub/Sub topic; the operator creates a
// subscription on it and names it via --notifications-subscription.
// Message shape (GKE-documented): attributes carry `type_url` (the
// qualified event type), `cluster_name`, `cluster_location`, and
// `payload` (the event's JSON); the message data is the human-readable
// description.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// pubsubMessage is the §13 slice of *pubsub.Message the translation
// reads; production wraps the real message, tests feed literals.
type pubsubMessage struct {
	PublishTime time.Time
	Data        []byte
	Attributes  map[string]string
}

// notificationReceiver is the §13 small client interface over the one
// Pub/Sub operation the reader needs. The production adapter acks
// every delivered message after handle returns (the sentinel's store
// is the durable record; redelivery would duplicate signals);
// tests replay canned messages.
type notificationReceiver interface {
	Receive(ctx context.Context, handle func(pubsubMessage)) error
}

// notificationsAPI implements cloud.NotificationsAPI.
type notificationsAPI struct {
	receiver notificationReceiver
}

func newNotificationsAPI(p *Provider) *notificationsAPI {
	return &notificationsAPI{receiver: &pubsubReceiver{
		project:      p.project,
		subscription: p.subscription,
	}}
}

// pubsubReceiver is the production notificationReceiver: one
// subscription, client dialed on first Receive.
type pubsubReceiver struct {
	project      string
	subscription string
}

// subscriptionID resolves the configured subscription to (project,
// bare name): a full "projects/<p>/subscriptions/<name>" path wins
// over the provider project; anything else is a bare name in it.
func subscriptionID(project, configured string) (string, string) {
	if parts := strings.Split(configured, "/"); len(parts) == 4 && parts[0] == "projects" && parts[2] == "subscriptions" {
		return parts[1], parts[3]
	}
	return project, configured
}

func (r *pubsubReceiver) Receive(ctx context.Context, handle func(pubsubMessage)) error {
	project, name := subscriptionID(r.project, r.subscription)
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("pubsub client: %w", err)
	}
	defer func() { _ = client.Close() }()
	sub := client.Subscription(name)
	return sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		handle(pubsubMessage{
			PublishTime: m.PublishTime,
			Data:        m.Data,
			Attributes:  m.Attributes,
		})
		// Ack after handle: the handler is synchronous signal
		// emission into the sentinel pipeline, and the sentinel's
		// store is the durable record — redelivery would only
		// duplicate signals.
		m.Ack()
	})
}

// Receive implements cloud.NotificationsAPI.
func (a *notificationsAPI) Receive(ctx context.Context, handle func(cloud.ClusterNotification)) error {
	return a.receiver.Receive(ctx, func(m pubsubMessage) {
		if n, ok := parseNotification(m); ok {
			handle(n)
		}
	})
}

// parseNotification translates one Pub/Sub message. Messages without
// a type_url attribute are not cluster notifications (someone pointed
// the subscription at the wrong topic) and are dropped here — the
// consuming source counts and reports what it receives, and an ack'd
// drop beats infinite redelivery.
func parseNotification(m pubsubMessage) (cloud.ClusterNotification, bool) {
	var zero cloud.ClusterNotification
	typeURL := m.Attributes["type_url"]
	if typeURL == "" {
		return zero, false
	}
	n := cloud.ClusterNotification{
		Time:       m.PublishTime,
		Type:       typeURL[strings.LastIndexByte(typeURL, '.')+1:],
		Cluster:    m.Attributes["cluster_name"],
		Location:   m.Attributes["cluster_location"],
		Attributes: map[string]string{},
		Message:    strings.TrimSpace(string(m.Data)),
	}
	// The payload attribute carries the event's JSON; flatten its
	// top-level fields to strings for display. Non-JSON or nested
	// values render compactly rather than vanishing.
	if payload := m.Attributes["payload"]; payload != "" {
		var fields map[string]any
		if err := json.Unmarshal([]byte(payload), &fields); err == nil {
			for k, v := range fields {
				switch val := v.(type) {
				case string:
					n.Attributes[k] = val
				default:
					raw, err := json.Marshal(val)
					if err == nil {
						n.Attributes[k] = string(raw)
					}
				}
			}
		}
	}
	return n, true
}
