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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/steward/mcpauth"
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
	// Google, when non-nil, is Google-token mode (#699): every request
	// carries the caller's own Google token, checked by steward, and the
	// server is stateless (a Gemini Enterprise client opens a session per
	// interaction and closes none, so sessions would only accumulate).
	// The server must be built WithGoogleAuth with the same value. Like
	// Auth, it makes AllowNonLoopback permissible; the two are exclusive.
	Google *GoogleAuth
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
	// MTLSListen is the address of the mTLS listener for workload
	// callers (#701); Workloads must be set with it. It is a second
	// listener beside Listen (either may be served alone): purser's mTLS
	// profiles demand a client certificate in the handshake, which people
	// reaching Listen through a Gateway do not have.
	MTLSListen string
	// Workloads authenticates and authorizes workload callers on the
	// MTLSListen listener, and authorizes the client-credentials clients
	// of Google's Path B authorization server on Listen (#702). The server
	// must be built WithWorkloadAuth with the same value.
	Workloads *WorkloadAuth
	// MTLSReady, if non-nil, receives the mTLS listener's bound address.
	MTLSReady chan<- string
	// Stateless serves HTTP without sessions, as Google-token mode
	// always does: any replica can answer any request, so an endpoint
	// runs as several replicas behind a Service with no sticky sessions
	// (#700). lookout keeps no per-session state and sends no
	// server-to-client notifications, so it loses nothing.
	Stateless bool
}

// Serve runs the server until ctx is canceled or the peer
// disconnects. An empty listen address serves the default stdio
// transport (newline-delimited JSON-RPC on stdin/stdout — stdout
// carries only protocol frames; command payloads travel inside tool
// results). A non-empty listen address serves streamable HTTP, subject
// to BindPolicy.
func Serve(ctx context.Context, server *mcp.Server, opts ServeOptions) error {
	if opts.Google != nil && opts.Auth != nil {
		return errors.New("--google-client-id and --auth-token-file are exclusive: pick one way to authenticate callers")
	}
	if opts.Google != nil && opts.Google.server != server {
		// The HTTP layer would authenticate each caller and the tools
		// would then read as lookout: the one outcome this mode exists to
		// prevent. (The reverse mismatch, a server built WithGoogleAuth
		// served without Google, fails closed: no request carries a
		// caller, so every call is refused.)
		return errors.New("internal: Google-token mode served in front of a server not built WithGoogleAuth with the same GoogleAuth; " +
			"its tools would read as lookout, not as each caller")
	}
	if opts.Workloads != nil && opts.Workloads.server != server {
		// The listener would verify each workload and the tools would then
		// run without its rules, as lookout, for anyone the CA vouches for.
		return errors.New("internal: workload callers served in front of a server not built WithWorkloadAuth with the same WorkloadAuth; " +
			"its tools would not apply the --workload-rules")
	}
	if opts.MTLSListen != "" && (opts.Workloads == nil || opts.Workloads.tlsConfig == nil) {
		return errors.New("internal: --mtls-listen and the workload configuration go together")
	}
	if opts.Workloads != nil && opts.MTLSListen == "" && !opts.Google.HasAuthServer() {
		// Rules with no way in: neither the mTLS listener nor lookout's
		// authorization server's client-credentials clients.
		return errors.New("internal: workload rules served with neither --mtls-listen nor --oauth-issuer")
	}
	if opts.Listen == "" && opts.MTLSListen == "" {
		if opts.Google != nil {
			return errors.New("--google-client-id needs --listen: on stdio there is no HTTP request to carry a caller's token")
		}
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
	getServer := func(*http.Request) *mcp.Server { return server }
	var httpOpts *mcp.StreamableHTTPOptions
	if opts.Stateless {
		httpOpts = &mcp.StreamableHTTPOptions{Stateless: true}
	}
	var listeners []servedListener
	closeAll := func() {
		for _, l := range listeners {
			_ = l.ln.Close()
		}
	}
	if opts.Listen != "" {
		if err := (BindPolicy{
			Listen:           opts.Listen,
			AllowNonLoopback: opts.AllowNonLoopback,
			HasAuthToken:     opts.Auth != nil || opts.Google != nil,
			HasAccessLog:     opts.AccessLogPath != "",
		}).Validate(); err != nil {
			return err
		}
		ln, err := net.Listen("tcp", opts.Listen)
		if err != nil {
			return err
		}
		var handler http.Handler
		if g := opts.Google; g != nil {
			mux := http.NewServeMux()
			// RFC 9728 metadata, unauthenticated: a client reads it to learn
			// where to sign in before it has a token.
			mux.Handle(g.protector.MetadataPath(), g.protector.MetadataHandler())
			if g.as != nil {
				// Path B (#702): lookout's own authorization server, at the
				// paths its issuer implies (RFC 8414 metadata, authorize,
				// callback, token, and register with open registration).
				// Unauthenticated by nature: it is where a client gets a
				// token.
				g.as.Register(mux)
			}
			// MCP only at the --resource-url path, the resource the metadata
			// describes; anything else is a 404.
			mux.Handle(g.path, mcpauth.Middleware(g.protector)(mcp.NewStreamableHTTPHandler(getServer,
				&mcp.StreamableHTTPOptions{Stateless: true})))
			handler = wrapHTTPHandler(mux)
		} else {
			handler = opts.Auth.Wrap(wrapHTTPHandler(mcp.NewStreamableHTTPHandler(getServer, httpOpts)))
		}
		listeners = append(listeners, servedListener{ln: ln, handler: handler, ready: opts.Ready})
	}
	if opts.MTLSListen != "" {
		// The mTLS listener authenticates every connection by its client
		// certificate, so it needs no other authentication mode to bind
		// off-host; it still needs the opt-in and the access log.
		if err := (BindPolicy{
			Flag:             "--mtls-listen",
			Listen:           opts.MTLSListen,
			AllowNonLoopback: opts.AllowNonLoopback,
			HasAuthToken:     true,
			HasAccessLog:     opts.AccessLogPath != "",
		}).Validate(); err != nil {
			closeAll()
			return err
		}
		ln, err := net.Listen("tcp", opts.MTLSListen)
		if err != nil {
			closeAll()
			return err
		}
		handler := opts.Workloads.middleware(wrapHTTPHandler(mcp.NewStreamableHTTPHandler(getServer, httpOpts)))
		listeners = append(listeners, servedListener{ln: tls.NewListener(ln, opts.Workloads.tlsConfig), handler: handler, ready: opts.MTLSReady, mtls: true})
	}

	servers := make([]*http.Server, len(listeners))
	done := make(chan error, len(listeners))
	for i, l := range listeners {
		servers[i] = &http.Server{
			Handler: l.handler,
			// Bounded even on loopback: a wedged local client must not pin
			// header reads, request bodies, or keep-alive sockets open forever.
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		if l.mtls {
			// Why a handshake was refused (no certificate, an untrusted
			// one, a peer the profile did not admit) is reported only in
			// the server's error log, so it goes to the banner's stream
			// (stderr), labelled; the access log records calls, not
			// refused connections.
			out := opts.Announce
			if out == nil {
				out = io.Discard
			}
			servers[i].ErrorLog = log.New(out, "lookout mcp: --mtls-listen: ", 0)
		}
	}
	for i := range listeners {
		listeners[i].addr = listeners[i].ln.Addr().String()
	}
	announce(opts, listeners)
	for i, l := range listeners {
		if l.ready != nil {
			l.ready <- l.ln.Addr().String()
		}
		go func() { done <- servers[i].Serve(l.ln) }()
	}
	shutdown := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, s := range servers {
			_ = s.Shutdown(shutdownCtx)
		}
	}
	select {
	case <-ctx.Done():
		shutdown()
		for range listeners {
			<-done
		}
		return nil // clean shutdown on signal
	case err := <-done:
		shutdown()
		for range listeners[1:] {
			<-done
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// servedListener is one HTTP listener Serve runs.
type servedListener struct {
	ln      net.Listener
	handler http.Handler
	ready   chan<- string
	mtls    bool
	addr    string // the bound address, for the banner
}

// announce writes the startup banner for an HTTP bind. It is loud in
// proportion to the exposure: a loopback bind gets one factual line, a
// routable one says so in as many words, because the failure mode this
// guards against is an operator who did not realize what they opened.
func announce(opts ServeOptions, listeners []servedListener) {
	if opts.Announce == nil {
		return
	}
	if opts.Google != nil {
		fmt.Fprintf(opts.Announce, "lookout mcp: Google-token mode: every call reads as its caller, "+
			"with their own token (RFC 9728 metadata at %s)\n", opts.Google.protector.MetadataURL())
		if as := opts.Google.as; as != nil {
			fmt.Fprintf(opts.Announce, "lookout mcp: authorization server for generic MCP clients at %s "+
				"(the Google OAuth client must list %s as a redirect URI)\n", as.Issuer(), as.CallbackURL())
			if opts.Workloads != nil && opts.MTLSListen == "" {
				fmt.Fprintf(opts.Announce, "lookout mcp: workload callers: %d rule(s) in --workload-rules decide what each client-credentials client may call\n",
					opts.Workloads.RuleCount())
			}
		}
	}
	for _, l := range listeners {
		addr := l.addr
		listen, kind, how := opts.Listen, "HTTP", "bearer-token"
		if opts.Google != nil {
			how = "Google-token"
		}
		if l.mtls {
			listen, kind, how = opts.MTLSListen, "mTLS (workload callers)", "client-certificate"
			fmt.Fprintf(opts.Announce, "lookout mcp: workload callers: %d rule(s) in --workload-rules decide what each workload may call\n",
				opts.Workloads.RuleCount())
		}
		if err := ValidateLoopback(listen); err == nil {
			fmt.Fprintf(opts.Announce, "lookout mcp: serving MCP over %s on %s (loopback)\n", kind, addr)
			continue
		}
		fmt.Fprintf(opts.Announce,
			"lookout mcp: serving MCP over %s on %s — REACHABLE OFF-HOST.\n"+
				"lookout mcp: %s authentication is REQUIRED; every call is recorded to %s.\n",
			kind, addr, how, opts.AccessLogPath)
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
	// Flag names the listener's flag in refusals; empty is --listen.
	Flag             string
	Listen           string
	AllowNonLoopback bool
	HasAuthToken     bool
	HasAccessLog     bool
}

// Validate returns nil if the bind is permitted, or the refusal to
// print otherwise.
func (p BindPolicy) Validate() error {
	flag := p.Flag
	if flag == "" {
		flag = "--listen"
	}
	loopbackErr := validateLoopbackFlag(flag, p.Listen)
	if loopbackErr == nil {
		return nil // loopback: allowed with or without the extras
	}
	// A malformed address is not an exposure decision; report it as
	// itself rather than as a policy refusal.
	if _, _, err := net.SplitHostPort(p.Listen); err != nil {
		return loopbackErr
	}
	if !p.AllowNonLoopback {
		return fmt.Errorf("%s=%q: refusing to bind a non-loopback address. "+
			"To serve off-host, pass --allow-non-loopback together with an authentication mode "+
			"(--auth-token-file=<path> or --google-client-id=<id>) and --access-log=<path>; "+
			"without all three lookout mcp is loopback-only (§4.3)", flag, p.Listen)
	}
	if !p.HasAuthToken {
		return fmt.Errorf("%s=%q with --allow-non-loopback requires --auth-token-file=<path> or --google-client-id=<id>: "+
			"an unauthenticated cluster-read API on a routable address is not something to open by accident", flag, p.Listen)
	}
	if !p.HasAccessLog {
		return fmt.Errorf("%s=%q with --allow-non-loopback requires --access-log=<path>: "+
			"a remotely reachable read API with no record of who called what is not shippable", flag, p.Listen)
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
	return validateLoopbackFlag("--listen", addr)
}

func validateLoopbackFlag(flag, addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s=%q: %v (want host:port, e.g. 127.0.0.1:8383)", flag, addr, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%s=%q: not a loopback address; "+
			"only 127.0.0.1, ::1, or localhost are loopback (§4.3)", flag, addr)
	}
	return nil
}

type remoteAddrKey struct{}

func withRemoteAddr(ctx context.Context, addr string) context.Context {
	return context.WithValue(ctx, remoteAddrKey{}, addr)
}

func remoteAddrFromContext(ctx context.Context) string {
	v, _ := ctx.Value(remoteAddrKey{}).(string)
	return v
}

const maxHTTPBodyBytes = 1 << 20 // 1 MiB

func wrapHTTPHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxHTTPBodyBytes)
		if r.RemoteAddr != "" {
			r = r.WithContext(withRemoteAddr(r.Context(), r.RemoteAddr))
		}
		next.ServeHTTP(w, r)
	})
}
