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

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Minimal GCE metadata-server client. Deliberately not
// cloud.google.com/go/compute/metadata: this PR establishes the
// provider boundary with zero GCP SDK linkage; the protocol is one
// GET with a Metadata-Flavor header.

// metadataHostEnv overrides the metadata host, mirroring the
// convention the official libraries honor (and what tests use).
const metadataHostEnv = "GCE_METADATA_HOST"

const defaultMetadataHost = "metadata.google.internal"

type metadataClient struct {
	base   string
	client *http.Client
}

func newMetadataClient() *metadataClient {
	host := os.Getenv(metadataHostEnv)
	if host == "" {
		host = defaultMetadataHost
	}
	return &metadataClient{
		base: "http://" + host + "/computeMetadata/v1/",
		// Off-GCE the connect fails fast or times out here; identity
		// detection is best-effort, so keep the budget small.
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

// lookup fetches one metadata path, returning "" on any failure —
// callers treat absence as "not detectable here".
func (m *metadataClient) lookup(ctx context.Context, path string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.base+path, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := m.client.Do(req)
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Metadata-Flavor") != "Google" {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}
