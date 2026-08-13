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

// Package version centralizes build-identity reporting for
// cmd/lookout, ported from core-agent's internal/version (issue #146:
// a go-installed lookout reported the literal string "dev", with no
// way to correlate a binary with a release). The package vars are
// overridable at release time via -ldflags; plain `go build` /
// `go install` falls back to the VCS metadata Go embeds when
// -buildvcs=true (the default since Go 1.18), so dev builds still
// report a real SHA. (Known limit, shared with core-agent: Go embeds
// no vcs.* settings when building from a LINKED git worktree — such
// builds report "commit none".)
//
// Release process (docs/release-process.md):
//
//	go build -ldflags "\
//	  -X github.com/go-steer/k8s-lookout/internal/version.Version=v0.12.0 \
//	  -X github.com/go-steer/k8s-lookout/internal/version.Commit=$(git rev-parse HEAD) \
//	  -X github.com/go-steer/k8s-lookout/internal/version.Date=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
//	" ./cmd/lookout
//
// After cutting a tag, bump Version below to the next minor + "-dev"
// (e.g. v0.12.0 release → main becomes v0.13.0-dev) so post-release
// dev builds report their next-target version. Enforced by
// dev/ci/presubmits/verify-version-fallback, same as core-agent —
// where skipped bumps produced stale --version output silently
// until the check existed.
package version

import (
	"fmt"
	"runtime/debug"
)

// Build-time metadata. Defaults assume an in-development build off
// main; release-time -ldflags injection overrides them with the real
// tag, commit, and build date.
var (
	// Version is the semver tag for released builds, or vX.Y.Z-dev
	// for in-development builds. Bump this manually on main right
	// after cutting a release (verify-version-fallback enforces it).
	Version = "v0.20.0-dev"

	// Commit is the git SHA the binary was built from. Defaults to
	// "none" so the debug.BuildInfo fallback can detect that nothing
	// was injected; release builds get the full SHA via -ldflags.
	Commit = "none"

	// Date is the build timestamp in ISO 8601. Same default-sentinel
	// pattern as Commit.
	Date = "unknown"
)

// String renders the build identity for `lookout version`, the
// --version flag, and the sentinel's startup log line. prog is the
// binary name so the format starts with what the operator typed.
//
// Format:
//
//	<prog> <semver> (commit <8-char-sha>[, modified], built <date>)
//
// The leading two tokens are always (prog, version) so scripts can
// grep without parsing the parenthesized suffix.
func String(prog string) string {
	v, c, d, dirty := resolveBuildInfo(Version, Commit, Date)
	return formatVersion(prog, v, c, d, dirty)
}

// Semver returns the resolved version alone (the ReadBuildInfo
// fallback applied) for consumers that want the bare semver — the
// MCP server's advertised version.
func Semver() string {
	v, _, _, _ := resolveBuildInfo(Version, Commit, Date)
	return v
}

// resolveBuildInfo returns the version/commit/date/dirty tuple to
// report. ldflags-injected values are authoritative when present;
// when the defaults are still in place we fall back to the VCS
// metadata Go embeds via -buildvcs=true so a plain `go build` at
// least surfaces the SHA + commit time + dirty marker.
func resolveBuildInfo(ldVersion, ldCommit, ldDate string) (v, c, d string, dirty bool) {
	v, c, d = ldVersion, ldCommit, ldDate
	// Only consult ReadBuildInfo when nothing was injected — the
	// release-time ldflags win when set.
	if c != "none" {
		return v, c, d, false
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return v, c, d, false
	}
	// `go install module@version` carries the module version even
	// though no ldflags ran — prefer it over the -dev fallback so an
	// @latest install reports the release it actually is (#146).
	if mv := info.Main.Version; mv != "" && mv != "(devel)" {
		v = mv
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if s.Value != "" {
				c = s.Value
			}
		case "vcs.time":
			if s.Value != "" {
				d = s.Value
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = true
			}
		}
	}
	return v, c, d, dirty
}

// formatVersion is the deterministic string-building half, split out
// so tests can exercise format choices without juggling build-info
// state.
func formatVersion(prog, v, c, d string, dirty bool) string {
	short := c
	if len(short) > 8 {
		short = short[:8]
	}
	suffix := ""
	if dirty {
		suffix = ", modified"
	}
	return fmt.Sprintf("%s %s (commit %s%s, built %s)", prog, v, short, suffix, d)
}
