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

package findings

import "strings"

// generatedAlphabet is Kubernetes' own generated-suffix alphabet:
// k8s.io/apimachinery/pkg/util/rand's `alphanums`, the charset behind
// every `generateName` suffix and every ReplicaSet pod-template hash.
//
//	bcdfghjklmnpqrstvwxz2456789
//
// Note what is MISSING, because the omissions are what make this
// normalizer safe rather than a guess: no vowels (a, e, i, o, u) and
// no 0/1/3. Kubernetes drops them so a random suffix can never
// accidentally spell a word and never renders an ambiguous glyph
// (0/O, 1/l). The side effect we exploit: an ordinary English-ish name
// segment — "backend", "payment", "master", "frontend", "manager" —
// almost always contains a vowel, so it cannot be mistaken for a
// generated suffix. A normalizer built on "looks like base36" would
// have no such protection.
const generatedAlphabet = "bcdfghjklmnpqrstvwxz2456789"

// generatedSet is generatedAlphabet as a lookup set.
var generatedSet = func() map[rune]struct{} {
	m := make(map[rune]struct{}, len(generatedAlphabet))
	for _, r := range generatedAlphabet {
		m[r] = struct{}{}
	}
	return m
}()

// Suffix-shape bounds. A `generateName` random suffix is exactly 5
// characters; a ReplicaSet pod-template hash is SafeEncodeString of a
// 32-bit FNV hash, which lands in 6-10.
//
// maxGeneratedSuffixes is 2 because that is the deepest real chain:
// <deployment>-<rs-hash>-<pod>. The cap is not just an optimization —
// it protects names whose OWN tail is hash-shaped. A Helm release
// installed as `myapp-7f8bd` produces pods `myapp-7f8bd-<rs>-<pod>`;
// stripping exactly two keeps `myapp-7f8bd`, so two releases of the
// same chart stay distinct subjects. An uncapped strip would fold both
// to `myapp` and silently merge their findings. The price is that
// normalization is not idempotent above the cap (`a-<g>-<g>-<g>` loses
// one more segment per pass); nothing re-normalizes a composed key, and
// TestNormalizeNameCapIsNotIdempotent pins the trade-off.
const (
	minGeneratedLen      = 5
	maxGeneratedLen      = 10
	maxGeneratedSuffixes = 2
)

// NormalizeName strips Kubernetes' generated name suffixes so that a
// rescheduled pod reads as the SAME subject as the pod it replaced.
//
// This is the whole reason the diff surface needs its own key. A
// CrashLooping pod `payment-backend-7d9f8-x9k2l` that gets rescheduled
// as `payment-backend-7d9f8-q4m7p` is one ongoing finding, but under an
// exact-name key it reads as `resolved` + `new` — the single most
// common way a transition surface lies to an operator, and it lies
// every time a crash-looping pod is replaced, which is constantly.
//
//	payment-backend-7d9f8-x9k2l → payment-backend
//	payment-backend-7d9f8       → payment-backend
//	payment-backend             → payment-backend
//
// The rules, applied right-to-left over `-`-separated segments, at
// most maxGeneratedSuffixes times:
//
//   - the segment is minGeneratedLen..maxGeneratedLen characters, and
//   - every character is in generatedAlphabet (see that constant for
//     why the vowel exclusion carries the weight here), and
//   - stripping it would not consume the entire name.
//
// Deliberately conservative in three places:
//
//   - StatefulSet ordinals survive. `db-0`, `db-2` keep their suffix:
//     one character is below minGeneratedLen (and 0/1/3 are not even in
//     the alphabet). This is correct, not a gap — `db-0` and `db-1` are
//     durable, individually-addressable instances, and collapsing them
//     would hide a single-replica failure inside a healthy set.
//   - CronJob job names survive. `backup-28472940` keeps its suffix:
//     the unix-minute stamp normally contains 0/1/3. Consecutive runs
//     of one CronJob therefore read as resolved+new rather than
//     ongoing. Under-normalizing is the safe direction — it produces
//     noisier digests, never a silently-suppressed new failure — and
//     the fix, if it proves annoying in practice, is a CronJob-aware
//     rule that reads ownerReferences rather than a looser charset.
//   - Short segments survive. "svc", "db", "k8s" are below the length
//     floor even though they are vowel-free.
//
// Names that are entirely generated (a bare `x9k2l`) are returned
// unchanged: there is nothing left to identify the subject by, and an
// empty key is worse than an over-specific one.
func NormalizeName(name string) string {
	if name == "" {
		return ""
	}
	segs := strings.Split(name, "-")
	stripped := 0
	for stripped < maxGeneratedSuffixes && len(segs) > 1 {
		if !isGeneratedSegment(segs[len(segs)-1]) {
			break
		}
		segs = segs[:len(segs)-1]
		stripped++
	}
	return strings.Join(segs, "-")
}

// isGeneratedSegment reports whether seg has the shape of a
// Kubernetes-generated suffix: right length, and drawn entirely from
// the generated alphabet.
func isGeneratedSegment(seg string) bool {
	if len(seg) < minGeneratedLen || len(seg) > maxGeneratedLen {
		return false
	}
	for _, r := range seg {
		if _, ok := generatedSet[r]; !ok {
			return false
		}
	}
	return true
}
