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

// Error classification for the REST discovery clients: the one place
// a GCP refusal becomes the provider-neutral cloud.ErrPermissionDenied
// that pkg/checks degrades on (#231, the #546 "a forbidden read is
// reported, not fatal" rule carried to the cloud side).

import (
	"errors"
	"net/http"
	"regexp"

	"google.golang.org/api/googleapi"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// notPermissionReasons are 403 error-item reasons that are not a
// missing grant: rate/quota throttling and a disabled API. They stay
// runtime errors (exit 1) — degrading them would report "grant X"
// for a problem no grant fixes.
var notPermissionReasons = map[string]bool{
	"rateLimitExceeded":     true,
	"userRateLimitExceeded": true,
	"quotaExceeded":         true,
	"dailyLimitExceeded":    true,
	"accessNotConfigured":   true,
	"SERVICE_DISABLED":      true,
}

// permissionInMessage pulls the permission name out of the Compute
// API's refusal text ("Required 'compute.addresses.list' permission
// for 'projects/p'") and the IAM-style variant ("Permission
// 'compute.disks.list' denied on resource ...").
var permissionInMessage = regexp.MustCompile(`(?:Required|Permission) '([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+)'`)

// classifyPermission wraps err into a *cloud.PermissionDeniedError
// when it is an HTTP 403 permission refusal; any other error (nil
// included) is returned unchanged. The permission is read from the
// API's message when it names one, else hint (the permission the
// call is documented to need; "" when the caller cannot say).
func classifyPermission(err error, hint string) error {
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != http.StatusForbidden {
		return err
	}
	for _, item := range gerr.Errors {
		if notPermissionReasons[item.Reason] {
			return err
		}
	}
	perm := hint
	messages := []string{gerr.Message}
	for _, item := range gerr.Errors {
		messages = append(messages, item.Message)
	}
	for _, msg := range messages {
		if m := permissionInMessage.FindStringSubmatch(msg); m != nil {
			perm = m[1]
			break
		}
	}
	return &cloud.PermissionDeniedError{Permission: perm, Err: err}
}
