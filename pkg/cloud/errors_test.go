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
