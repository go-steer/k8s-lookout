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
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// The digest path reaches the wire through Append, not openSession, so
// dispatcher.fitInject never saw it: before #337 a --watchboard-batch
// large enough to breach the daemon's per-inject ceiling made EVERY
// flush 400 and discard its whole buffer. pkg/inject covers FitTo
// itself; this is the wiring — that a real flush, through a real
// injector, puts a body on the wire that the daemon would accept.
func TestWatchboardDigest_FitsUnderInjectCeiling(t *testing.T) {
	t.Parallel()

	base, injects := newRoutingFakeDaemon(t)
	// A batch far past the ~22-entry breach point, flushed on count.
	d, _ := newBoardDispatcher(t, base, 200, time.Minute, 500)
	d.board.injectMaxBytes = inject.MaxInjectBytes

	ctx := context.Background()
	for i := range 200 {
		d.DispatchSignal(ctx, warningSignal(i))
	}

	var digest string
	for _, in := range *injects {
		if strings.Contains(messageOf(t, in.Body), `"kind":"watchboard.digest"`) {
			digest = in.Body
		}
	}
	if digest == "" {
		t.Fatalf("no watchboard digest reached the wire; got %d injects", len(*injects))
	}
	if len(digest) > inject.MaxInjectBytes {
		t.Errorf("digest body is %d bytes, over the %d ceiling the daemon would 400",
			len(digest), inject.MaxInjectBytes)
	}

	var p inject.WatchboardDigestPayload
	if err := json.Unmarshal([]byte(messageOf(t, digest)), &p); err != nil {
		t.Fatalf("unmarshal digest: %v", err)
	}
	if p.EntriesDropped == 0 {
		t.Errorf("entries_dropped = 0 on a digest that had to be cut")
	}
	if len(p.Entries)+p.EntriesDropped != 200 {
		t.Errorf("kept %d + dropped %d != the 200 buffered warnings",
			len(p.Entries), p.EntriesDropped)
	}
	if len(p.Entries) == 0 {
		t.Errorf("every entry dropped; the digest says nothing")
	}
	// Newest-wins: warningSignal(199) is the last buffered, so it must
	// be the one that survived.
	if last := p.Entries[len(p.Entries)-1]; last.Name != "cart-199" {
		t.Errorf("newest entry = %q, want cart-199 (the fit keeps the tail)", last.Name)
	}
	if got := testutil.ToFloat64(d.metrics.injectShrinks.WithLabelValues("watchboard_entries")); got != 1 {
		t.Errorf("inject_shrinks{shed=watchboard_entries} = %v, want 1", got)
	}
}

// A board with no ceiling configured (<= 0) must behave exactly as it
// did before #337 — this is what keeps every byte-exact digest wire pin
// in this package valid, since none of them set injectMaxBytes.
func TestWatchboardDigest_NoCeilingIsUnchanged(t *testing.T) {
	t.Parallel()

	base, injects := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 200, time.Minute, 500)

	ctx := context.Background()
	for i := range 200 {
		d.DispatchSignal(ctx, warningSignal(i))
	}

	var digest string
	for _, in := range *injects {
		if strings.Contains(messageOf(t, in.Body), `"kind":"watchboard.digest"`) {
			digest = in.Body
		}
	}
	if digest == "" {
		t.Fatalf("no watchboard digest reached the wire")
	}
	if strings.Contains(digest, "entries_dropped") {
		t.Errorf("unfitted digest carries a drop count; omitempty or the <= 0 guard is broken")
	}
	var p inject.WatchboardDigestPayload
	if err := json.Unmarshal([]byte(messageOf(t, digest)), &p); err != nil {
		t.Fatalf("unmarshal digest: %v", err)
	}
	if len(p.Entries) != 200 {
		t.Errorf("entries = %d, want all 200 (no ceiling means no fit)", len(p.Entries))
	}
}
