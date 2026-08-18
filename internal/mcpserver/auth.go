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

package mcpserver

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// minTokenLen is the shortest bearer token we will load. A token
// guarding cluster-wide read access is not a password a human types,
// it is generated once and pasted into a config; refusing a short one
// costs nothing and rules out the "temporary" token that outlives its
// afternoon.
const minTokenLen = 16

// BearerAuth authenticates HTTP requests against a single shared
// bearer token (issue #282).
//
// One token, no authorization: every caller that presents it gets the
// full advertised tool surface. This is the smallest thing that makes
// a routable bind defensible, not an access-control system. Per-token
// tool profiles are a plausible follow-up now that profiles exist;
// mTLS is the right answer for a production deployment and is
// deliberately out of scope here.
//
// The digest, not the token, is what is held and compared: comparing
// fixed-width SHA-256 sums in constant time removes both the timing
// side channel of a byte-by-byte compare and the length side channel
// that comparing the raw values would leave.
type BearerAuth struct {
	want [sha256.Size]byte
}

// LoadAuthToken reads a bearer token from path.
//
// File permissions are deliberately not checked. The obvious way to
// supply this in-cluster is a Secret volume, and those mount 0644 by
// default; refusing them would push operators toward baking the token
// into the image instead, which is worse.
func LoadAuthToken(path string) (*BearerAuth, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--auth-token-file=%s: %v", path, err)
	}
	// Trim rather than reject trailing whitespace: every way an
	// operator will produce this file (echo, a here-doc, an editor)
	// adds a newline, and a token that fails because of an invisible
	// byte is a bad afternoon.
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return nil, fmt.Errorf("--auth-token-file=%s: the file is empty", path)
	}
	if strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("--auth-token-file=%s: the token contains whitespace; it must be a single line", path)
	}
	if len(token) < minTokenLen {
		return nil, fmt.Errorf("--auth-token-file=%s: the token is %d characters; want at least %d "+
			"(generate one with: head -c 32 /dev/urandom | base64)", path, len(token), minTokenLen)
	}
	return &BearerAuth{want: sha256.Sum256([]byte(token))}, nil
}

// Authorize reports whether an Authorization header carries the
// expected bearer token. A nil *BearerAuth authorizes everything —
// there is no token configured, which is only reachable on a loopback
// bind or stdio.
func (a *BearerAuth) Authorize(header string) bool {
	if a == nil {
		return true
	}
	// RFC 7235 makes the scheme case-insensitive; clients send
	// "Bearer" and "bearer" about equally often.
	const scheme = "bearer "
	if len(header) <= len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return false
	}
	got := sha256.Sum256([]byte(strings.TrimSpace(header[len(scheme):])))
	return subtle.ConstantTimeCompare(got[:], a.want[:]) == 1
}

// Wrap returns next guarded by the bearer check; a nil *BearerAuth
// returns next unchanged, so the caller never branches on whether
// authentication is configured.
//
// The 401 body says nothing beyond the scheme. An unauthenticated
// caller has no business learning what this server is, which tools it
// advertises, or which cluster it reads.
func (a *BearerAuth) Wrap(next http.Handler) http.Handler {
	if a == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.Authorize(r.Header.Get("Authorization")) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="lookout mcp"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
