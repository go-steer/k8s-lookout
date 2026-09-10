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

package sources

import (
	"context"
	"log"
	"sync"

	"k8s.io/apimachinery/pkg/util/runtime"
)

// errorHandlerOnce guards the one write to runtime.ErrorHandlers.
var errorHandlerOnce sync.Once

// InstallInformerErrorHandler routes client-go's internal errors
// ("unknown object type in cache" on shutdown ctx.Done races,
// reflector list/watch failures) through our logger. Safe to call from
// any source's Run; every call after the first is a no-op.
//
// It deliberately names NO source (issue #384). runtime.ErrorHandlers
// is process-global and client-go passes no informer identity to it, so
// a handler that prefixes lines with one source's name misattributes
// every other source's watch errors — and under the shared informer
// factory (§6.3) most watches belong to no single source at all. The
// msg/keysAndValues client-go supplies are logged instead: they are the
// only attribution that is actually true.
//
// Two properties matter and neither is obvious. APPEND, never replace:
// the slice is shared by every client-go consumer in this binary, and
// replacing it silently discards the default handlers. And append
// exactly ONCE per process: Run is restarted by the supervisor, so an
// unguarded append grows the global slice — and the number of log lines
// per informer error — without bound across a restart loop.
func InstallInformerErrorHandler() {
	errorHandlerOnce.Do(func() {
		runtime.ErrorHandlers = append(runtime.ErrorHandlers,
			func(_ context.Context, err error, msg string, kv ...any) {
				switch {
				case msg == "":
					log.Printf("informer error: %v", err)
				case len(kv) == 0:
					log.Printf("informer error: %v (%s)", err, msg)
				default:
					log.Printf("informer error: %v (%s %v)", err, msg, kv)
				}
			},
		)
	})
}
