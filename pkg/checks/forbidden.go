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
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ListForbidden classifies an error from a read-path List call: when
// the API server refused it on authorization grounds it reports a
// short reason naming the refused read ("forbidden: list secrets",
// "forbidden: list nodes"), which a composition turns into an explicit
// unavailable answer instead of failing the whole command (#546). Any
// other error reports false and stays fatal — a broken API server is
// not a permission gap.
//
// It lives at the bottom of the checks tree so every group can use it
// without importing another group; state.ListForbidden is the same
// function.
func ListForbidden(err error) (string, bool) {
	if err == nil || !apierrors.IsForbidden(err) {
		return "", false
	}
	resource := ""
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		if d := status.Status().Details; d != nil && d.Kind != "" {
			resource = d.Kind
			if d.Group != "" {
				resource += "." + d.Group
			}
		}
	}
	if resource == "" {
		return "forbidden: list", true
	}
	return "forbidden: list " + resource, true
}
