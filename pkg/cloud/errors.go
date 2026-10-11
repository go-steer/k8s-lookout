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

package cloud

import (
	"context"
	"errors"
	"fmt"
)

// ErrPermissionDenied is the provider-neutral "the cloud identity is
// not allowed to make this read" answer. Providers wrap their own
// refusal (on GKE, an HTTP 403 / PERMISSION_DENIED) into a
// *PermissionDeniedError, which matches this sentinel under
// errors.Is, so pkg/checks can tell a forbidden read — reported as an
// explicit unavailable, the cloud twin of checks.ListForbidden —
// from a runtime failure, which stays fatal (§2, #546).
var ErrPermissionDenied = errors.New("cloud permission denied")

// PermissionDeniedError carries the permission a refused cloud read
// needed, when the provider knows it, so the degradation record can
// name exactly what to grant.
type PermissionDeniedError struct {
	// Permission is the provider permission the read needed (e.g.
	// "compute.addresses.list"); empty when unknown.
	Permission string
	// Err is the provider's original error.
	Err error
}

func (e *PermissionDeniedError) Error() string {
	if e.Permission == "" {
		return fmt.Sprintf("permission denied: %v", e.Err)
	}
	return fmt.Sprintf("permission denied (needs %s): %v", e.Permission, e.Err)
}

// Unwrap exposes the provider's original error.
func (e *PermissionDeniedError) Unwrap() error { return e.Err }

// Is makes every PermissionDeniedError match ErrPermissionDenied.
func (e *PermissionDeniedError) Is(target error) bool { return target == ErrPermissionDenied }

// PermissionDenied reports whether err is a refused cloud read and,
// when the provider named it, the permission the read needed.
func PermissionDenied(err error) (permission string, denied bool) {
	if !errors.Is(err, ErrPermissionDenied) {
		return "", false
	}
	var pd *PermissionDeniedError
	if errors.As(err, &pd) {
		return pd.Permission, true
	}
	return "", true
}

// ErrCredentialsUnavailable is the provider-neutral "lookout's own
// cloud identity could not get an access token" answer (#614). It is
// a different failure from ErrPermissionDenied: no request reached
// the API at all, because the credential source (on GKE, the
// metadata server minting a Workload Identity token) refused first.
// It is never about the resource a command reads, so every
// cloud-backed command reports it as an explicit unavailable — the
// same §2 record as an absent capability — instead of failing.
var ErrCredentialsUnavailable = errors.New("cloud credentials unavailable")

// ReasonCredentialsUnavailable is the cloud.unavailable reason a
// command emits for ErrCredentialsUnavailable.
const ReasonCredentialsUnavailable = "CredentialsUnavailable"

// CredentialsUnavailableNote is the summary-line unavailable= text
// for ErrCredentialsUnavailable. Fixed text: the provider's error
// carries token-endpoint URLs and server prose that have no place
// on stdout.
const CredentialsUnavailableNote = "lookout's own cloud identity could not obtain an access token (CredentialsUnavailable)" //nolint:gosec // operator prose about a missing token, not a credential (G101 false positive)

// CredentialsUnavailableError carries the provider's remedy for a
// refused token alongside the original error.
type CredentialsUnavailableError struct {
	// Fix names the likely remedy in the provider's terms (on GKE:
	// the KSA→GSA roles/iam.workloadIdentityUser binding). It is
	// static provider text, never derived from Err, so it is safe to
	// render on any output surface; empty when the provider cannot
	// say.
	Fix string
	// Err is the provider's original error. It may carry URLs and
	// server prose: logs only, never an output record.
	Err error
}

func (e *CredentialsUnavailableError) Error() string {
	return fmt.Sprintf("cloud credentials unavailable: %v", e.Err)
}

// Unwrap exposes the provider's original error.
func (e *CredentialsUnavailableError) Unwrap() error { return e.Err }

// Is makes every CredentialsUnavailableError match
// ErrCredentialsUnavailable.
func (e *CredentialsUnavailableError) Is(target error) bool {
	return target == ErrCredentialsUnavailable
}

// CredentialsUnavailable reports whether err is a refused token and,
// when the provider named one, the remedy.
func CredentialsUnavailable(err error) (fix string, unavailable bool) {
	if !errors.Is(err, ErrCredentialsUnavailable) {
		return "", false
	}
	var ce *CredentialsUnavailableError
	if errors.As(err, &ce) {
		return ce.Fix, true
	}
	return "", true
}

// DegradableCredentials is CredentialsUnavailable as every consumer
// that turns a refused token into a record or a log line must ask it:
// once ctx has ended, nothing degrades. A --timeout that expired or a
// cancellation that interrupted the token fetch is a runtime error
// (the §4.2 --timeout contract), never a missing grant with a remedy.
func DegradableCredentials(ctx context.Context, err error) (fix string, degradable bool) {
	if ctx.Err() != nil {
		return "", false
	}
	return CredentialsUnavailable(err)
}

// CredentialsUnavailableMessage is the operator-facing message of
// the CredentialsUnavailable record: what could not run, that the
// failure is lookout's own identity (not the resource being read),
// and the remedy when the provider named one. what is the command or
// read ("cloud quota", "state wi").
func CredentialsUnavailableMessage(what, fix string) string {
	msg := what + " could not run: lookout's own cloud identity could not obtain an access token, so nothing was read from the cloud"
	if fix == "" {
		return msg + " — check the credentials lookout runs with"
	}
	return msg + " — " + fix
}
