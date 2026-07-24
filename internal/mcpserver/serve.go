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

package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Serve runs the server until ctx is canceled or the peer
// disconnects. An empty listen address serves the default stdio
// transport (newline-delimited JSON-RPC on stdin/stdout — stdout
// carries only protocol frames; command payloads travel inside tool
// results). A non-empty listen address serves streamable HTTP, and
// must be loopback (ValidateLoopback).
//
// ready, if non-nil, receives the bound address once an HTTP listener
// is accepting connections (tests bind port 0).
func Serve(ctx context.Context, server *mcp.Server, listen string, ready chan<- string) error {
	if listen == "" {
		err := server.Run(ctx, &mcp.StdioTransport{})
		if errors.Is(err, context.Canceled) || isPeerDisconnect(err) {
			return nil // clean shutdown on signal or client exit
		}
		return err
	}

	if err := ValidateLoopback(listen); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Handler: mcp.NewStreamableHTTPHandler(
			func(*http.Request) *mcp.Server { return server }, nil),
		// Loopback-only, but still bounded: a wedged local client
		// must not pin header reads open forever (gosec G112).
		ReadHeaderTimeout: 10 * time.Second,
	}
	if ready != nil {
		ready <- ln.Addr().String()
	}

	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		<-done
		return nil // clean shutdown on signal
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// codeServerClosing is the SDK's jsonrpc2 "server is closing" wire
// code (not exported alongside the standard codes in the jsonrpc
// package). The SDK wraps it around the write-side error when the
// client ends a stdio session with responses still in flight.
const codeServerClosing = -32004

// isPeerDisconnect reports whether err is the client tearing down
// the stdio session — EOF on a quiet connection is already treated
// as clean by the SDK; an abrupt close (the supervisor killing its
// MCP child mid-write, normal lifecycle for stdio servers) surfaces
// as the server-closing wire error instead. Neither is a lookout
// failure.
func isPeerDisconnect(err error) bool {
	var wire *jsonrpc.Error
	return errors.As(err, &wire) && wire.Code == codeServerClosing
}

// ValidateLoopback rejects any --listen address that is not
// unambiguously loopback. lookout mcp has no authentication or
// authorization story — the §4.3 transports are stdio and *localhost*
// HTTP — so binding a routable interface would expose every cluster
// read to the network. Until a deployment needs remote access badly
// enough to bring an auth design with it, we refuse loudly instead of
// serving quietly: a literal loopback IP or "localhost" only, no
// hostnames, no wildcard binds.
func ValidateLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("--listen=%q: %v (want host:port, e.g. 127.0.0.1:8383)", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("--listen=%q: refusing to bind a non-loopback address: "+
			"lookout mcp has no auth; only 127.0.0.1, ::1, or localhost are allowed (§4.3)", addr)
	}
	return nil
}
