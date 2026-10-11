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

package inject

import (
	"context"
	"strings"
	"testing"
)

// Only the ax sink holds reminders (issue #599). The core-agent and webhook
// sinks deliver a repeat_count > 0 payload at once, every time, like any
// other followup.
func TestReminderPassesStraightThroughNonAXSinks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	reminder := func(n int) Payload {
		return Payload{Kind: KindFollowup, Reason: "BackOff", UID: "u1", RepeatCount: n}
	}

	t.Run("core-agent", func(t *testing.T) {
		t.Parallel()
		base, injects, _ := newFakeDaemon(t)
		inj, err := NewInjector(Config{DaemonURL: base, BearerToken: "tok"})
		if err != nil {
			t.Fatal(err)
		}
		for n := 1; n <= 3; n++ {
			if err := inj.Append(ctx, "sess-x", reminder(n)); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if len(*injects) != 3 || !strings.Contains((*injects)[2], `repeat_count`) {
			t.Errorf("core-agent sink did not deliver every reminder at once: %v", *injects)
		}
	})

	t.Run("webhook", func(t *testing.T) {
		t.Parallel()
		base, calls := newFakeReceiver(t)
		s, err := NewWebhookSink(WebhookConfig{URL: base})
		if err != nil {
			t.Fatal(err)
		}
		for n := 1; n <= 3; n++ {
			if err := s.Append(ctx, "inc-1", reminder(n)); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if len(*calls) != 3 || !strings.Contains((*calls)[2].Body, `"repeat_count":3`) {
			t.Errorf("webhook sink did not deliver every reminder at once: %v", *calls)
		}
	})
}
