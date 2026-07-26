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

//go:build gke || allproviders

package gke

// The ONLY GCP SDK import in the capacity path, tag-guarded with the
// rest of this package (DESIGN.md §2: go.mod carries the Logging SDK,
// the default build never links it — cmd/lookout's
// providers_default_test plus the nm check in CI keep that honest).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

// logadminLister is the production EntryLister: Cloud Logging via
// cloud.google.com/go/logging's read-path (logadmin) client.
type logadminLister struct {
	client *logadmin.Client
}

// newLogadminLister dials Cloud Logging for one project using
// Application Default Credentials (on GKE: the node/workload
// identity).
func newLogadminLister(ctx context.Context, project string) (EntryLister, error) {
	client, err := logadmin.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	return &logadminLister{client: client}, nil
}

// ListEntries implements EntryLister: fetch every entry matching
// filter, oldest first (the caller's poll windows are minutes wide;
// pagination is the iterator's problem).
func (l *logadminLister) ListEntries(ctx context.Context, filter string) ([]LogEntry, error) {
	it := l.client.Entries(ctx, logadmin.Filter(filter))
	var out []LogEntry
	for {
		e, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		payload, err := payloadJSON(e.Payload)
		if err != nil || payload == nil {
			continue // text/proto payloads are not visibility records
		}
		out = append(out, LogEntry{Timestamp: e.Timestamp, Payload: payload})
	}
}

// payloadJSON renders a logging entry payload as raw JSON. Visibility
// records arrive as jsonPayload, which logadmin decodes to
// *structpb.Struct.
func payloadJSON(payload any) (json.RawMessage, error) {
	switch p := payload.(type) {
	case nil:
		return nil, nil
	case *structpb.Struct:
		b, err := p.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("marshal jsonPayload: %w", err)
		}
		return b, nil
	default:
		return nil, nil
	}
}
