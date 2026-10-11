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
	"testing"
)

func TestPermissionDenied(t *testing.T) {
	base := errors.New("googleapi: Error 403: Required 'compute.addresses.list' permission")
	wrapped := fmt.Errorf("listing addresses: %w", &PermissionDeniedError{Permission: "compute.addresses.list", Err: base})

	if !errors.Is(wrapped, ErrPermissionDenied) {
		t.Fatal("wrapped PermissionDeniedError does not match ErrPermissionDenied")
	}
	if !errors.Is(wrapped, base) {
		t.Error("PermissionDeniedError does not unwrap to the provider's error")
	}
	if perm, ok := PermissionDenied(wrapped); !ok || perm != "compute.addresses.list" {
		t.Errorf("PermissionDenied = %q, %v; want compute.addresses.list, true", perm, ok)
	}
	if perm, ok := PermissionDenied(fmt.Errorf("x: %w", ErrPermissionDenied)); !ok || perm != "" {
		t.Errorf("bare sentinel: PermissionDenied = %q, %v; want \"\", true", perm, ok)
	}
	if _, ok := PermissionDenied(errors.New("googleapi: Error 500: backend error")); ok {
		t.Error("a non-permission error classified as denied")
	}
	if _, ok := PermissionDenied(nil); ok {
		t.Error("nil classified as denied")
	}
}

func TestCredentialsUnavailable(t *testing.T) {
	base := errors.New(`Get "https://compute.googleapis.com/...": credentials: cannot fetch token: compute: Received 403`)
	wrapped := fmt.Errorf("listing disks: %w", &CredentialsUnavailableError{Fix: "grant the binding", Err: base})

	if !errors.Is(wrapped, ErrCredentialsUnavailable) {
		t.Fatal("wrapped CredentialsUnavailableError does not match ErrCredentialsUnavailable")
	}
	if !errors.Is(wrapped, base) {
		t.Error("CredentialsUnavailableError does not unwrap to the provider's error")
	}
	if errors.Is(wrapped, ErrPermissionDenied) {
		t.Error("a refused token matches ErrPermissionDenied")
	}
	if fix, ok := CredentialsUnavailable(wrapped); !ok || fix != "grant the binding" {
		t.Errorf("CredentialsUnavailable = %q, %v; want the fix, true", fix, ok)
	}
	if fix, ok := CredentialsUnavailable(fmt.Errorf("x: %w", ErrCredentialsUnavailable)); !ok || fix != "" {
		t.Errorf("bare sentinel: CredentialsUnavailable = %q, %v; want \"\", true", fix, ok)
	}
	for _, err := range []error{nil, base, &PermissionDeniedError{Err: base}} {
		if _, ok := CredentialsUnavailable(err); ok {
			t.Errorf("%v classified as credentials unavailable", err)
		}
	}
}

func TestCredentialsUnavailableMessage(t *testing.T) {
	const head = "cloud quota could not run: lookout's own cloud identity could not obtain an access token, so nothing was read from the cloud — "
	if got := CredentialsUnavailableMessage("cloud quota", "grant X"); got != head+"grant X" {
		t.Errorf("message = %q", got)
	}
	if got := CredentialsUnavailableMessage("cloud quota", ""); got != head+"check the credentials lookout runs with" {
		t.Errorf("no-fix message = %q", got)
	}
}
