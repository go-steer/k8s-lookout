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
