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

package emit

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// FuzzMaskString exercises MaskString across arbitrary inputs and planted
// credential templates, verifying absence of panics, UTF-8 validity preservation,
// complete redaction of planted secrets, and fixed-point idempotency.
func FuzzMaskString(f *testing.F) {
	seeds := []string{
		"",
		"normal k8s event: Back-off restarting failed container api in pod api-0",
		"redis://:secret@redis.prod.svc:6379/0",
		"postgres://:p%2Fw@db.internal:5432/payments",
		"postgres://user:pass/word@db.internal:5432/payments",
		"AI" + "zaSyD-1234567890abcdefABCDEF012345678",
		"ya" + "29.a0AfH6SMBx-token_example_value",
		"ghp_1234567890abcdef1234567890abcdef1234",
		"github_pat_11ABCDEF0123456789_secretvalue",
		"xoxb-123456-789012-abcdef123456",
		"sk_live_51Habcdef1234567890",
		"glpat-abcdef1234567890_xyz",
		"Authorization: Basic YTpi",
		"Authorization: Bearer short_tok",
		"DB_PASSWORD=hunter2 API_KEY=xyz CLIENT_SECRET=shh",
		"--db-password=s3cr3t --secret-name=db-credentials",
		"-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...\n-----END RSA PRIVATE KEY-----",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		once := MaskString(in)
		twice := MaskString(once)
		if once != twice {
			t.Fatalf("MaskString is not idempotent on %q:\n once:  %q\n twice: %q", in, once, twice)
		}
		if utf8.ValidString(in) && !utf8.ValidString(once) {
			t.Fatalf("MaskString corrupted UTF-8 on %q -> %q", in, once)
		}
		// Planted secret check when embedding known secret shapes around fuzz prefix/suffix.
		planted := in + " redis://:fuzzsecret99@db:6379"
		masked := MaskString(planted)
		if strings.Contains(masked, "fuzzsecret99") {
			t.Fatalf("planted DSN secret survived MaskString(%q) = %q", planted, masked)
		}
	})
}
