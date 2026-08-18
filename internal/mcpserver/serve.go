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
	"io"
	"net"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ServeOptions is everything Serve needs beyond the server itself.
// The zero value is the default: stdio, no listener, no auth.
type ServeOptions struct {
	// Listen is the HTTP bind address; empty serves stdio.
	Listen string
	// AllowNonLoopback opts in to a routable bind. It is not enough on
	// its own — see BindPolicy.
	AllowNonLoopback bool
	// Auth, when non-nil, requires a bearer token on every HTTP
	// request. It is what makes AllowNonLoopback permissible.
	Auth *BearerAuth
	// Announce, when non-nil, receives the one-line startup banner for
	// an HTTP bind. Callers pass stderr; stdout carries protocol
	// frames on the stdio transport and must stay clean.
	Announce io.Writer
	// AccessLogPath is named in the banner only. The log itself is
	// wired into the server with WithAccessLog; this is here so the
	// banner can tell an operator where the record is going.
	AccessLogPath string
	// Ready, if non-nil, receives the bound address once the HTTP
	// listener is accepting connections (tests bind port 0).
	Ready chan<- string
}

// Serve runs the server until ctx is canceled or the peer
// disconnects. An empty listen address serves the default stdio
// transport (newline-delimited JSON-RPC on stdin/stdout — stdout
// carries only protocol frames; command payloads travel inside tool
// results). A non-empty listen address serves streamable HTTP, subject
// to BindPolicy.
func Serve(ctx context.Context, server *mcp.Server, opts ServeOptions) error {
	if opts.Listen == "" {
		err := server.Run(ctx, &mcp.StdioTransport{})
		if errors.Is(err, context.Canceled) || isPeerDisconnect(err) {
			return nil // clean shutdown on signal or client exit
		}
		return err
	}

	// Re-checked here rather than trusted from the caller: Serve is
	// the last place before a socket exists, and a policy enforced
	// only at the flag layer is one refactor away from not being
	// enforced at all.
	if err := (BindPolicy{
		Listen:           opts.Listen,
		AllowNonLoopback: opts.AllowNonLoopback,
		HasAuthToken:     opts.Auth != nil,
		HasAccessLog:     opts.AccessLogPath != "",
	}).Validate(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Handler: opts.Auth.Wrap(mcp.NewStreamableHTTPHandler(
			func(*http.Request) *mcp.Server { return server }, nil)),
		// Bounded even on loopback: a wedged local client must not pin
		// header reads open forever (gosec G112).
		ReadHeaderTimeout: 10 * time.Second,
	}
	announce(opts, ln.Addr().String())
	if opts.Ready != nil {
		opts.Ready <- ln.Addr().String()
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

// announce writes the startup banner for an HTTP bind. It is loud in
// proportion to the exposure: a loopback bind gets one factual line, a
// routable one says so in as many words, because the failure mode this
// guards against is an operator who did not realize what they opened.
func announce(opts ServeOptions, addr string) {
	if opts.Announce == nil {
		return
	}
	if err := ValidateLoopback(opts.Listen); err == nil {
		fmt.Fprintf(opts.Announce, "lookout mcp: serving MCP over HTTP on %s (loopback)\n", addr)
		return
	}
	fmt.Fprintf(opts.Announce,
		"lookout mcp: serving MCP over HTTP on %s — REACHABLE OFF-HOST.\n"+
			"lookout mcp: bearer-token authentication is REQUIRED; every call is recorded to %s.\n",
		addr, opts.AccessLogPath)
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

// BindPolicy decides whether an HTTP bind is permitted (issue #282).
//
// Loopback stays the default and needs nothing. A routable bind needs
// all three of the conditions below, and the reason there are three
// rather than one is that each is a separate mistake:
//
//   - AllowNonLoopback, because a token supplied for a localhost bind
//     must not silently change what interface gets opened;
//   - HasAuthToken, because a bind flag must not open an
//     unauthenticated cluster-read API;
//   - HasAccessLog, because a remotely reachable read API with no
//     record of who called what is not shippable. On loopback the log
//     is a debugging convenience; off-host it is the only evidence
//     that exists.
//
// Stated as a type rather than a chain of ifs at the flag layer so
// that the whole rule is one table-testable function, and so Serve can
// re-check it immediately before opening the socket.
type BindPolicy struct {
	Listen           string
	AllowNonLoopback bool
	HasAuthToken     bool
	HasAccessLog     bool
}

// Validate returns nil if the bind is permitted, or the refusal to
// print otherwise.
func (p BindPolicy) Validate() error {
	loopbackErr := ValidateLoopback(p.Listen)
	if loopbackErr == nil {
		return nil // loopback: allowed with or without the extras
	}
	// A malformed address is not an exposure decision; report it as
	// itself rather than as a policy refusal.
	if _, _, err := net.SplitHostPort(p.Listen); err != nil {
		return loopbackErr
	}
	if !p.AllowNonLoopback {
		return fmt.Errorf("--listen=%q: refusing to bind a non-loopback address. "+
			"To serve off-host, pass --allow-non-loopback together with --auth-token-file=<path> "+
			"and --access-log=<path>; without all three lookout mcp is loopback-only (§4.3)", p.Listen)
	}
	if !p.HasAuthToken {
		return fmt.Errorf("--listen=%q with --allow-non-loopback requires --auth-token-file=<path>: "+
			"an unauthenticated cluster-read API on a routable address is not something to open by accident", p.Listen)
	}
	if !p.HasAccessLog {
		return fmt.Errorf("--listen=%q with --allow-non-loopback requires --access-log=<path>: "+
			"a remotely reachable read API with no record of who called what is not shippable", p.Listen)
	}
	return nil
}

// ValidateLoopback reports whether an address is unambiguously
// loopback: a literal loopback IP or "localhost" only, no hostnames,
// no wildcard binds.
//
// It is the floor the §4.3 transports are built on — stdio and
// *localhost* HTTP — because binding a routable interface exposes
// every cluster read to the network. A deployment that needs remote
// access can have it, but only by bringing an auth token and an access
// log with it; BindPolicy is where that trade is made.
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
		return fmt.Errorf("--listen=%q: not a loopback address; "+
			"only 127.0.0.1, ::1, or localhost are loopback (§4.3)", addr)
	}
	return nil
}
