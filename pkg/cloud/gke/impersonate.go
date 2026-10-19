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

	"golang.org/x/oauth2"
	"google.golang.org/api/impersonate"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

func init() {
	cloud.RegisterImpersonator(impersonateServiceAccount)
}

// impersonateServiceAccount mints access tokens for serviceAccount, with
// scopes (none: cloud-platform), through the IAM Credentials API
// (generateAccessToken), with Application Default Credentials as the
// impersonating identity:
// lookout's own service account needs roles/iam.serviceAccountTokenCreator
// on serviceAccount. The source refreshes itself, and is detached from
// ctx because it outlives the call that created it.
//
// It never falls back to lookout's own credentials: a failed mint is the
// token source's error, which the caller's call reports.
func impersonateServiceAccount(ctx context.Context, serviceAccount string, scopes ...string) (oauth2.TokenSource, error) {
	if len(scopes) == 0 {
		scopes = []string{"https://www.googleapis.com/auth/cloud-platform"}
	}
	ts, err := impersonate.CredentialsTokenSource(context.WithoutCancel(ctx), impersonate.CredentialsConfig{
		TargetPrincipal: serviceAccount,
		Scopes:          scopes,
	})
	if err != nil {
		return nil, fmt.Errorf("impersonating %s: %w", serviceAccount, err)
	}
	return ts, nil
}
