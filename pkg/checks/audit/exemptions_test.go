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

package audit_test

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var update = flag.Bool("update", false, "rewrite golden files")

// checktest's fake clock starts at 2026-01-01T00:00:00Z, which is the
// instant every exemption in these fixtures is judged against.
const fixture = `exemptions:
  - kind: audit.no_pdb
    namespace: batch
    reason: batch jobs are restartable by design
    expires: 2025-11-01
    owner: data-platform
  - kind: audit.no_pdb
    namespace: prod
    name: legacy-api
    reason: single-replica vendor appliance, replacement tracked in PLAT-8812
    expires: 2026-01-08
    owner: platform
  - kind: top.unrequested
    reason: cluster is single-tenant and deliberately unbounded
    expires: 2026-06-30
`

// write drops the fixture in a temp dir and returns its path.
func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "exemptions.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func TestExemptionsContract(t *testing.T) {
	checktest.VerifyContract(t, audit.ExemptionsCommand(), "--exemptions="+write(t, fixture))
}

func TestExemptionsGolden(t *testing.T) {
	res := checktest.Run(t, audit.ExemptionsCommand(), "--exemptions="+write(t, fixture))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	path := filepath.Join("testdata", "exemptions.golden")
	if *update {
		if err := os.WriteFile(path, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden (run 'go test ./pkg/checks/audit -update'): %v", err)
	}
	if !bytes.Equal([]byte(res.Stdout), want) {
		t.Errorf("output does not match %s:\ngot:\n%s\nwant:\n%s", path, res.Stdout, want)
	}
}

// The two claims are different and must not collapse into one kind: a
// lapsed entry is annotating NOTHING right now, an expiring one still
// is. Severity follows — the first is a warning, the second is FYI.
func TestExemptionsSeparatesExpiredFromExpiring(t *testing.T) {
	res := checktest.Run(t, audit.ExemptionsCommand(), "--exemptions="+write(t, fixture))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("want two findings and a summary, got:\n%s", res.Stdout)
	}
	// Entries() sorts soonest-expiry first, so the lapsed one leads.
	if !strings.HasPrefix(lines[0], "kind=audit.exemption_expired severity=warning ") {
		t.Errorf("first finding = %q, want the lapsed entry as a warning", lines[0])
	}
	if !strings.HasPrefix(lines[1], "kind=audit.exemption_expiring severity=info ") {
		t.Errorf("second finding = %q, want the soon-to-lapse entry as info", lines[1])
	}
	// The entry expiring in June is neither, and emits nothing: zero
	// nominal state applies here too.
	if strings.Contains(res.Stdout, "top.unrequested") {
		t.Errorf("a live, not-yet-due entry must emit nothing:\n%s", res.Stdout)
	}
	// scanned counts every entry examined, including the quiet one —
	// that is how a reader tells "file is clean" from "file is empty".
	if !strings.Contains(lines[2], "scanned=3 findings=2") {
		t.Errorf("summary = %q, want scanned=3 findings=2", lines[2])
	}
}

// The command reports on the file it was given; without one there is
// nothing to report, and guessing a path would be worse than asking.
func TestExemptionsRequiresAFile(t *testing.T) {
	res := checktest.Run(t, audit.ExemptionsCommand())
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d (usage)", res.Code, emit.ExitUsage)
	}
	if !strings.Contains(res.Stderr, "--exemptions") {
		t.Errorf("stderr should name the missing flag: %q", res.Stderr)
	}
	if res.Stdout != "" {
		t.Errorf("a usage error must leave stdout untouched, got %q", res.Stdout)
	}
}

// A malformed file fails the run rather than being partially applied.
// The alternative — scanning on with exemptions silently not in
// effect — is the failure mode the whole design exists to prevent.
func TestExemptionsRejectsBadFile(t *testing.T) {
	bad := write(t, "exemptions:\n  - kind: audit.no_pdb\n    reason: why\n")
	res := checktest.Run(t, audit.ExemptionsCommand(), "--exemptions="+bad)
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d (usage)", res.Code, emit.ExitUsage)
	}
	if !strings.Contains(res.Stderr, "no expires") {
		t.Errorf("stderr should say what is wrong: %q", res.Stderr)
	}
}

// --within widens or narrows only the FORWARD-looking half. Already
// lapsed entries are reported regardless, including at --within=0.
func TestExemptionsWithinWindow(t *testing.T) {
	for _, tc := range []struct {
		within       string
		wantExpiring int
		wantFindings string
	}{
		{within: "0s", wantExpiring: 0, wantFindings: "findings=1"},
		{within: "336h", wantExpiring: 1, wantFindings: "findings=2"},
		{within: "4400h", wantExpiring: 2, wantFindings: "findings=3"},
	} {
		t.Run(tc.within, func(t *testing.T) {
			res := checktest.Run(t, audit.ExemptionsCommand(),
				"--exemptions="+write(t, fixture), "--within="+tc.within)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}
			if got := strings.Count(res.Stdout, "kind=audit.exemption_expiring "); got != tc.wantExpiring {
				t.Errorf("--within=%s: %d expiring findings, want %d\n%s", tc.within, got, tc.wantExpiring, res.Stdout)
			}
			if !strings.Contains(res.Stdout, tc.wantFindings) {
				t.Errorf("--within=%s: summary should report %s\n%s", tc.within, tc.wantFindings, res.Stdout)
			}
			if !strings.Contains(res.Stdout, "kind=audit.exemption_expired ") {
				t.Errorf("--within=%s: a lapsed entry must be reported at any window\n%s", tc.within, res.Stdout)
			}
		})
	}
}

func TestExemptionsRejectsNegativeWithin(t *testing.T) {
	res := checktest.Run(t, audit.ExemptionsCommand(),
		"--exemptions="+write(t, fixture), "--within=-1h")
	if res.Code != emit.ExitUsage {
		t.Fatalf("exit %d, want %d (usage)", res.Code, emit.ExitUsage)
	}
}

// An empty file is a clean file, not an error: scanned=0 findings=0 is
// the explicit "nothing to report" the §4.2 contract requires.
func TestExemptionsEmptyFile(t *testing.T) {
	res := checktest.Run(t, audit.ExemptionsCommand(), "--exemptions="+write(t, "exemptions: []\n"))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.HasPrefix(res.Stdout, "scanned=0 findings=0 ") {
		t.Errorf("output = %q, want an explicit empty scan", res.Stdout)
	}
	// The exemption seam was active, so the summary says so even
	// though nothing matched.
	if !strings.Contains(res.Stdout, "exempt=0") {
		t.Errorf("output = %q, want exempt=0 — the file was in effect and matched nothing", res.Stdout)
	}
}

// The command's own findings pass through the same exemption seam as
// every other command's, so an entry covering audit.exemption_expired
// annotates this output. It is not a loop: the escape hatch has to
// carry an expiry too, so it expires as well.
func TestExemptionsAnnotatesItsOwnFindings(t *testing.T) {
	const selfReferential = `exemptions:
  - kind: audit.no_pdb
    namespace: batch
    reason: batch jobs are restartable by design
    expires: 2025-11-01
  - kind: audit.exemption_expired
    reason: mid-migration, the batch entries are being rewritten in PLAT-9001
    expires: 2026-03-01
`
	res := checktest.Run(t, audit.ExemptionsCommand(), "--exemptions="+write(t, selfReferential))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, `exempt_reason="mid-migration, the batch entries are being rewritten in PLAT-9001"`) {
		t.Errorf("the expired-entry finding should carry its own exemption annotation:\n%s", res.Stdout)
	}
	// Annotated, not hidden.
	if !strings.Contains(res.Stdout, "kind=audit.exemption_expired ") {
		t.Errorf("an exempted finding must still be emitted:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "findings=1") || !strings.Contains(res.Stdout, "exempt=1") {
		t.Errorf("summary should report findings=1 and exempt=1:\n%s", res.Stdout)
	}
}

// The posture recipe, exercised end to end: the fingerprint is a
// property of the CLASS, so two entries lapsing in two different
// namespaces share it.
func TestExemptionsFingerprintIsClassNotInstance(t *testing.T) {
	const twoLapsed = `exemptions:
  - kind: audit.no_pdb
    namespace: batch
    reason: restartable
    expires: 2025-11-01
  - kind: audit.no_pdb
    namespace: prod
    reason: behind a regional LB
    expires: 2025-12-01
`
	res := checktest.Run(t, audit.ExemptionsCommand(), "--exemptions="+write(t, twoLapsed))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	const want = "fingerprint=sha256:434e960e0f889a0bdacfb627694794044c0e90071520bcd15ab34592ce8f5c8b"
	if got := strings.Count(res.Stdout, want); got != 2 {
		t.Errorf("both lapsed entries should carry the pinned posture fingerprint (%d of 2):\n%s", got, res.Stdout)
	}
}
