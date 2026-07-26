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

// harvest-corpus is the §9.3 drill tool over pkg/corpus: it reads a
// captured inject stream (dev/drills/stub-daemon.py's log format —
// `kubectl logs` of the stub pod verbatim, optionally interleaved
// with exported §9.4 triage-status record JSON lines) and emits one
// labeled trajectory per incident session as JSON lines on stdout,
// complete trajectories first. A summary goes to stderr.
//
// Deliberately a dev tool, not a `lookout` subcommand: it validates
// the harvestability CONTRACT (outcome records are schema-stable
// structured injects, so trajectories extract without NLP); the
// product surface is the schema itself (docs/signal-schema-v1.md).
//
// Usage:
//
//	kubectl logs stub-daemon | go run ./dev/tools/harvest-corpus
//	go run ./dev/tools/harvest-corpus capture.log
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/go-steer/k8s-lookout/pkg/corpus"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "harvest-corpus:", err)
		os.Exit(1)
	}
}

func run() error {
	var in io.Reader = os.Stdin
	switch len(os.Args) {
	case 1:
	case 2:
		f, err := os.Open(os.Args[1]) // #nosec G304 G703 -- dev CLI opening the capture file the operator named
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		in = f
	default:
		return fmt.Errorf("usage: harvest-corpus [capture-file] (default: stdin)")
	}
	trajectories, err := corpus.Harvest(in)
	if err != nil {
		return err
	}
	if err := corpus.WriteJSONL(os.Stdout, trajectories); err != nil {
		return err
	}
	complete := 0
	for _, tr := range trajectories {
		if tr.Complete {
			complete++
		}
	}
	fmt.Fprintf(os.Stderr, "harvest-corpus: %d trajectories (%d complete: symptom→diagnosis→action→outcome)\n", len(trajectories), complete)
	return nil
}
