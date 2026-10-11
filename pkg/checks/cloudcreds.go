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

package checks

import (
	"context"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// CredentialsUnavailableFinding is the cloud.unavailable record for a
// cloud read that failed because lookout's own identity could not get
// an access token (cloud.ErrCredentialsUnavailable, #614). Like
// CloudUnavailableKind it lives here because every cloud-backed
// command emits it; one builder keeps them saying the same thing. The
// message is built from fixed text and the provider's static remedy —
// never from the error, whose token-endpoint URLs and server prose
// stay off stdout.
func CredentialsUnavailableFinding(provider string, c cloud.Capability, what, fix string) emit.Finding {
	return emit.Finding{
		Kind:     "cloud.unavailable",
		Severity: emit.SeverityInfo,
		Reason:   cloud.ReasonCredentialsUnavailable,
		Message:  cloud.CredentialsUnavailableMessage(what, fix),
		Details: []emit.Field{
			{Key: "capability", Value: string(c)},
			{Key: "provider", Value: provider},
		},
	}
}

// DegradeCredentials is the read-path's handling of a refused token:
// when err is cloud.ErrCredentialsUnavailable it emits the
// CredentialsUnavailable record and the summary's unavailable= note
// and reports handled=true (the caller then exits 0 with whatever it
// had scanned); any other error, nil included, is left to the caller.
// Once ctx has ended nothing is handled: an expired --timeout stays
// the runtime error (exit 1) the output contract promises.
func DegradeCredentials(ctx context.Context, out *emit.Writer, provider string, c cloud.Capability, what string, err error) (handled bool, emitErr error) {
	fix, ok := cloud.DegradableCredentials(ctx, err)
	if !ok {
		return false, nil
	}
	if err := out.Emit(CredentialsUnavailableFinding(provider, c, what, fix)); err != nil {
		return true, err
	}
	return true, out.Note("unavailable", cloud.CredentialsUnavailableNote)
}
