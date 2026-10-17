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
	"fmt"
	"net/http"

	"golang.org/x/oauth2/google"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

func init() {
	cloud.RegisterServiceClient(serviceClient)
}

// serviceClient is an HTTP client authenticated with Application Default
// Credentials, lookout's own identity. It is detached from ctx because it
// outlives the call that created it.
func serviceClient(ctx context.Context, scopes ...string) (*http.Client, error) {
	c, err := google.DefaultClient(context.WithoutCancel(ctx), scopes...)
	if err != nil {
		return nil, fmt.Errorf("application default credentials: %w", err)
	}
	return c, nil
}
