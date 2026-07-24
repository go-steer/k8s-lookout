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

	"github.com/go-steer/k8s-lookout/internal/watch"
)

func init() {
	register(command{
		name:    "watch",
		summary: "resident per-cluster sentinel (the moved k8s-event-watcher)",
		// watch manages its own signal handling and lifecycle —
		// moved verbatim; the root context is intentionally unused
		// until the M2 signal-engine generalization.
		run: func(_ context.Context, args []string) int {
			return watch.Main(args)
		},
	})
}
