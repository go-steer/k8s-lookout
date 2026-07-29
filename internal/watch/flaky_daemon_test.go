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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// flakySessionDaemon is newRoutingFakeDaemon plus a scriptable
// outage: while down, POST /sessions is refused with 503 (the
// injector's single un-retried POST then reports sid="") — inject
// POSTs always succeed and are captured with the session id they
// targeted, INCLUDING one addressed to an empty id (path
// /sessions//inject), which must never reach the wire.
type flakySessionDaemon struct {
	mu      sync.Mutex
	down    bool
	injects []routedInject
	created int // successful POST /sessions
	refused int // POST /sessions rejected while down
}

func newFlakySessionDaemon(t *testing.T) (baseURL string, fd *flakySessionDaemon) {
	t.Helper()
	fd = &flakySessionDaemon{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fd.mu.Lock()
		defer fd.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			if fd.down {
				fd.refused++
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"daemon restarting"}`))
				return
			}
			fd.created++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"app":"core-agent","user":"alice","sessionID":"sess-%d","url":"http://x"}`, fd.created)
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sessions/") && strings.HasSuffix(r.URL.Path, "/inject"):
			sid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sessions/"), "/inject")
			body, _ := io.ReadAll(r.Body)
			fd.injects = append(fd.injects, routedInject{SessionID: sid, Body: string(body)})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, fd
}

func (fd *flakySessionDaemon) setDown(down bool) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.down = down
}

func (fd *flakySessionDaemon) injectLog() []routedInject {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	out := make([]routedInject, len(fd.injects))
	copy(out, fd.injects)
	return out
}

func (fd *flakySessionDaemon) sessionsCreated() int {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	return fd.created
}
