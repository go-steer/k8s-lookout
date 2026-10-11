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
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/inject"
)

type spyPlainSink struct {
	openedPayload   any
	appendedPayload any
}

func (s *spyPlainSink) OpenIncident(_ context.Context, payload any) (string, error) {
	s.openedPayload = payload
	return "inc-plain", nil
}

func (s *spyPlainSink) Append(_ context.Context, _ string, payload any) error {
	s.appendedPayload = payload
	return nil
}

type spyOpenerSink struct {
	spyPlainSink
}

func (s *spyOpenerSink) CreateSession(_ context.Context) (string, error) {
	return "sid-created", nil
}

type spyKeyedSink struct {
	spyOpenerSink
	keyedPayload any
}

func (s *spyKeyedSink) OpenIncidentKeyed(_ context.Context, _ inject.IncidentKey, payload any) (string, error) {
	s.keyedPayload = payload
	return "task-1/sid-keyed", nil
}

func (s *spyKeyedSink) CreateSessionKeyed(_ context.Context, _ inject.IncidentKey) (string, error) {
	return "task-1/sid-empty", nil
}

func TestMaskingSink_CapabilityTiersAndSanitization(t *testing.T) {
	if MaskingSink(nil) != nil {
		t.Fatal("MaskingSink(nil) must return nil")
	}

	// Tier 1: plain Sink (must NOT satisfy SessionOpener or KeyedOpener).
	plain := &spyPlainSink{}
	wPlain := MaskingSink(plain)
	if _, ok := wPlain.(inject.SessionOpener); ok {
		t.Fatal("wrapped plain Sink must not implement SessionOpener")
	}
	if _, ok := wPlain.(inject.KeyedOpener); ok {
		t.Fatal("wrapped plain Sink must not implement KeyedOpener")
	}
	if MaskingSink(wPlain) != wPlain {
		t.Fatal("MaskingSink must be idempotent when wrapping an already-masked sink")
	}

	raw := inject.Payload{
		Message: "failed auth to postgres://:s3cr3tP@ssw0rd@db.prod.svc:5432/app",
		Context: inject.PayloadContext{
			Labels: map[string]string{
				"app":      "payments",
				"password": "short",
				"token":    "ghp_1234567890abcdefghijklmnopqrstuvwx12",
			},
		},
	}
	if _, err := wPlain.OpenIncident(context.Background(), raw); err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	openWire, err := json.Marshal(plain.openedPayload)
	if err != nil {
		t.Fatalf("marshal open: %v", err)
	}
	for _, secret := range []string{"s3cr3tP@ssw0rd", "short", "ghp_1234567890"} {
		if strings.Contains(string(openWire), secret) {
			t.Errorf("OpenIncident leaked secret %q on wire: %s", secret, openWire)
		}
	}

	if err := wPlain.Append(context.Background(), "inc-plain", raw); err != nil {
		t.Fatalf("Append: %v", err)
	}
	appendWire, err := json.Marshal(plain.appendedPayload)
	if err != nil {
		t.Fatalf("marshal append: %v", err)
	}
	for _, secret := range []string{"s3cr3tP@ssw0rd", "short", "ghp_1234567890"} {
		if strings.Contains(string(appendWire), secret) {
			t.Errorf("Append leaked secret %q on wire: %s", secret, appendWire)
		}
	}

	// Tier 2: SessionOpener (not KeyedOpener).
	opener := &spyOpenerSink{}
	wOpener := MaskingSink(opener)
	if _, ok := wOpener.(inject.SessionOpener); !ok {
		t.Fatal("wrapped SessionOpener must satisfy inject.SessionOpener")
	}
	if _, ok := wOpener.(inject.KeyedOpener); ok {
		t.Fatal("wrapped SessionOpener must not satisfy inject.KeyedOpener")
	}

	// Tier 3: KeyedOpener + annotations/map payload on both OpenIncidentKeyed and Append.
	keyed := &spyKeyedSink{}
	wKeyed := MaskingSink(keyed)
	ko, ok := wKeyed.(inject.KeyedOpener)
	if !ok {
		t.Fatal("wrapped KeyedOpener must satisfy inject.KeyedOpener")
	}
	if unwrapSink(wKeyed) != keyed {
		t.Fatal("unwrapSink must return underlying inner sink")
	}
	mapPayload := map[string]any{
		"message": "Authorization: Bearer my-short-token",
		"annotations": map[string]string{
			"api_key": "shortkey",
			"safe":    "v1.2.3",
		},
		"labels": map[string]string{
			"client_secret": "tiny",
		},
	}
	ik := inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyIncident, ID: "uid-1/CrashLoopBackOff"}
	if _, err := ko.OpenIncidentKeyed(context.Background(), ik, mapPayload); err != nil {
		t.Fatalf("OpenIncidentKeyed: %v", err)
	}
	keyedWire, _ := json.Marshal(keyed.keyedPayload)
	for _, secret := range []string{"my-short-token", "shortkey", "tiny"} {
		if strings.Contains(string(keyedWire), secret) {
			t.Errorf("OpenIncidentKeyed leaked secret %q on wire: %s", secret, keyedWire)
		}
	}
	if !strings.Contains(string(keyedWire), "v1.2.3") {
		t.Errorf("OpenIncidentKeyed altered benign annotation value: %s", keyedWire)
	}
}
