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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestSinkHTTPClient_OwnConnectionPool pins #759: the sink client keeps
// its connections in a pool of its own, so CloseIdleConnections on
// http.DefaultTransport (which httptest.Server.Close calls, and any
// other in-process user of the default transport may) cannot reach
// them. Sharing the default pool made an in-flight inject fail with
// "transport connection broken: http: CloseIdleConnections called"
// whenever a parallel test closed its server at the wrong moment.
//
// Deterministic proxy for that race: a connection left idle by request
// 1 must survive the default transport's CloseIdleConnections and
// carry request 2, so the server sees exactly one new connection.
func TestSinkHTTPClient_OwnConnectionPool(t *testing.T) {
	var newConns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	c := NewSinkHTTPClient()
	get := func() {
		t.Helper()
		resp, err := c.Get(srv.URL)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		// Read to EOF so the connection goes back to the idle pool.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	get()
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		dt.CloseIdleConnections()
	}
	get()

	if n := newConns.Load(); n != 1 {
		t.Fatalf("server saw %d new connections, want 1: the sink's idle connection was closed by http.DefaultTransport.CloseIdleConnections, so the sink shares the default pool", n)
	}
}
