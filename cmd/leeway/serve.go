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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// serve runs the /metrics + /healthz + /readyz listener until ctx is
// cancelled. It is a sibling of the sentinel's serveMetrics rather than
// a call into it: that one is unexported in internal/watch, and
// exporting it would put internal/watch on this binary's import path —
// the one thing §2.4 discipline 4 is about. Sixty lines of net/http is
// the cheaper half of that trade.
func serve(ctx context.Context, addr string, reg *prometheus.Registry, srcs []sources.Source, log io.Writer) error {
	if addr == "" {
		<-ctx.Done()
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	// Liveness does not consult the sources. The question it answers is
	// "is this process wedged", and a process whose informers cannot
	// reach the apiserver is not wedged — restarting it would not help.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	// Readiness does, and answers the different question: has this
	// process established its watches. Until the initial LISTs drain the
	// sources' handlers are suppressed and their state is half-built, so
	// every gauge on /metrics is an undercount rather than a reading.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		ok, unsynced := sources.AllSynced(srcs)
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "not ready: %s is still listing\n", unsynced)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	// Bind synchronously so a port already in use is a startup failure
	// rather than a process that runs happily and exports to nobody.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	// The resolved address, not the requested one: --metrics-addr=:0 is
	// how the smoke test and the kwok harness avoid fighting over a port,
	// and a bound port nothing prints is a bound port nothing can scrape.
	fmt.Fprintf(log, "leeway: serving /metrics /healthz /readyz on %s\n", ln.Addr())

	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("metrics: serve: %w", err)
	}
}
