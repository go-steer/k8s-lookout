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

// This file is the §6.5 sanitizer: the single place where secret
// material is masked and system metadata is stripped before anything
// reaches stdout, an MCP response, or an inject. It has two layers:
//
//   - SanitizeFinding — the finding-level sanitizer wired in as
//     DefaultSanitizer, so every Writer.Emit on every surface passes
//     through it. It masks value-shaped credential strings in
//     Message, Reason, and Detail values.
//
//   - SanitizeObject / SanitizeUnstructured — the spec sanitizer for
//     whole Kubernetes objects, called by anything that renders a
//     spec (`triage spec`, enrichment bundles). It strips system
//     metadata, masks Secret payloads and credential env vars, and
//     runs the value-shape heuristics over every string in the
//     object.
//
// Design stance (§1 principle 8, AGENTS.md hard rule): prefer
// precision — heuristics anchor on key name + value shape together
// wherever possible so image digests, uids, and resource *names*
// survive (the graph deliberately keeps secret names; only values
// are radioactive). Each heuristic documents what it matches and
// what it deliberately lets through.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"
)

// Redacted replaces every masked credential value on every surface.
// Secret payload values use RedactedLen instead so "is this secret
// empty?" stays answerable without leaking a single byte.
const Redacted = "[REDACTED]"

// RedactedLen renders a masked value that reports only its decoded
// content length. Never a prefix, never a hash of the value.
func RedactedLen(n int) string { return fmt.Sprintf("[REDACTED:%dB]", n) }

// ---------------------------------------------------------------------------
// Finding-level sanitizer (the DefaultSanitizer)
// ---------------------------------------------------------------------------

// SanitizeFinding masks credential material in a finding: the
// value-shape heuristics (MaskString) over Reason and Message, and
// key-anchored masking over Detail values (a detail whose key names
// a credential and whose value is secret-shaped is fully redacted; a
// detail like secret=db-credentials survives, because that is a
// resource *name* and `triage changes` must be able to say it).
func SanitizeFinding(f Finding) Finding {
	f.Reason = MaskString(f.Reason)
	f.Message = MaskString(f.Message)
	if len(f.Details) > 0 {
		details := make([]Field, len(f.Details))
		for i, d := range f.Details {
			details[i] = Field{Key: d.Key, Value: maskKeyedValue(d.Key, d.Value)}
		}
		f.Details = details
	}
	return f
}

// structuralKeys are detail keys whose value lookout builds itself
// out of object identity — cluster, namespace, kind, name and a
// canonical reason joined with "/". Nothing in them comes from
// Secret data, but they are long, slash-separated and mixed-class,
// so secretShaped matches them, and keyWords splits subject_key to
// {subject,key} where "key" is a credential word: the key-anchored
// branch redacted them outright (#246). These values exist to be
// copied back into a command (`lookout findings ack --subject=…`),
// so redacting them breaks the documented workflow. They still run
// through the value-shape heuristics below.
var structuralKeys = map[string]bool{
	"subject_key":  true, // findings diff / findings ack
	"resource_key": true, // triage status
}

// maskKeyedValue applies the key-anchored heuristics (credential key
// name AND secret-shaped value → fully redacted) and then the
// position-independent value-shape heuristics.
func maskKeyedValue(key, value string) string {
	if value != "" && !structuralKeys[key] && credentialKey(key) && secretShaped(value) {
		return Redacted
	}
	return MaskString(value)
}

// ---------------------------------------------------------------------------
// Value-shape heuristics
// ---------------------------------------------------------------------------

// The substring heuristics below run over every string on every
// surface. Each one matches a shape that is a credential with very
// high confidence on its own, so no key anchoring is needed.
var (
	// rePEMPrivateKey matches PEM private-key blocks (RSA, EC,
	// PKCS#8, OpenSSH, ENCRYPTED, …). (?s) lets the body span
	// newlines, and .*? also spans the literal `\n` escapes of a
	// PEM embedded in a JSON string (GCP service-account keys).
	// CERTIFICATE and PUBLIC KEY blocks are deliberately not
	// matched: they are public material and often triage-relevant.
	rePEMPrivateKey = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----`)

	// rePrivateKeyJSON masks the "private_key" field of a GCP
	// service-account JSON even if the PEM heuristic missed it
	// (e.g. a truncated or reformatted block).
	rePrivateKeyJSON = regexp.MustCompile(`("private_key"\s*:\s*")(?:[^"\\]|\\.)*(")`)

	// reJWT matches three dot-separated base64url segments starting
	// with eyJ (base64 of `{"`) — the JOSE compact serialization.
	// Signed JWTs are bearer credentials regardless of where they
	// appear.
	reJWT = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}`)

	// reAuthHeader masks the token of Bearer/Basic authorization
	// header values. The scheme word survives so "authenticated
	// with Bearer …" stays readable.
	reAuthHeader = regexp.MustCompile(`(?i)\b(bearer|basic)[ \t]+[A-Za-z0-9+/=_.~-]{16,}`)

	// reAWSKeyID matches AWS access key IDs (long-term AKIA…,
	// temporary ASIA…): fixed 4-letter prefix + 16 uppercase
	// alphanumerics. The ID alone is only half a credential, but it
	// pinpoints an account and often travels next to the secret.
	reAWSKeyID = regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`)

	// reURLPassword matches the password of URL userinfo
	// (scheme://user:password@host). Only the password is masked —
	// scheme, user, host, and path are triage signal.
	reURLPassword = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^/@:\s]+:)[^@/\s]+@`)

	// reCredentialFlag matches command-line flags whose name
	// contains a credential word (--db-password=x, --token x, …)
	// and masks the value. Flag names ending in -name/-ref/-file/
	// -path/-dir/-id are skipped by maskCredentialFlags: those
	// values are references, not secrets (--secret-name=db-creds).
	//
	// 'pass' / 'passphrase' (issue #106) are matched as whole hyphen-
	// delimited SEGMENTS (--pass, --db-pass, --ssl-passphrase), not as
	// substrings like the other words: the flag path uses substring
	// matching, so a naive `pass` alternative would over-mask --bypass=.
	// The `--?(?:[a-z0-9-]*-)?` prefix requires a dash immediately
	// before the word, so "bypass" (no dash before "pass") never matches.
	//
	// The leading (^|[^0-9A-Za-z_-]) group is load-bearing (#357): the
	// flag's dash must START a word. Without it `--?` happily matches
	// the interior hyphen of an ordinary object name, so
	// "secret edgy-absent-secret not found" reads `-absent-secret` as
	// a flag and redacts `not` — inverting the sentence. RE2 has no
	// lookbehind, so the boundary is captured and re-emitted by
	// maskCredentialFlags.
	reCredentialFlag = regexp.MustCompile(`(?i)(^|[^0-9A-Za-z_-])(--?[a-z0-9-]*(?:password|passwd|pwd|token|secret|credential|api-?key|access-?key|private-?key)[a-z0-9-]*|--?(?:[a-z0-9-]*-)?(?:passphrase|pass))(=|[ \t]+)(\S+)`)

	// reCredentialFlagName matches a bare credential flag with no
	// attached value, for args/command lists in the two-element
	// form (["--db-password", "hunter2"]) where the secret is the
	// NEXT list element. Same 'pass'/'passphrase' segment-anchoring as
	// reCredentialFlag (issue #106) so --bypass stays benign.
	reCredentialFlagName = regexp.MustCompile(`(?i)^(?:--?[a-z0-9-]*(?:password|passwd|pwd|token|secret|credential|api-?key|access-?key|private-?key)[a-z0-9-]*|--?(?:[a-z0-9-]*-)?(?:passphrase|pass))$`)
)

// credentialFlagRefSuffixes are flag-name suffixes that mark the
// value as a reference to a secret rather than the secret itself.
var credentialFlagRefSuffixes = []string{"-name", "-ref", "-file", "-path", "-dir", "-id"}

// MaskString applies every position-independent value-shape
// heuristic to s, replacing matched credential material with
// Redacted while preserving the surrounding text. It is intentionally
// surgical: a log line quoting a JWT keeps its prose, a DSN keeps
// everything but the password.
//
// Known recall gap (documented tradeoff): free-form `password:
// hunter2` prose inside opaque config blobs is NOT matched, because
// the same shape legitimately carries references (`secretName:
// db-credentials`) and names must survive.
func MaskString(s string) string {
	if s == "" {
		return s
	}
	s = rePEMPrivateKey.ReplaceAllString(s, Redacted)
	s = rePrivateKeyJSON.ReplaceAllString(s, "${1}"+Redacted+"${2}")
	s = reJWT.ReplaceAllString(s, Redacted)
	s = reAuthHeader.ReplaceAllString(s, "${1} "+Redacted)
	s = reAWSKeyID.ReplaceAllString(s, Redacted)
	s = reURLPassword.ReplaceAllString(s, "${1}"+Redacted+"@")
	s = maskCredentialFlags(s)
	return s
}

// maskCredentialFlags applies reCredentialFlag with the
// reference-suffix escape hatch.
func maskCredentialFlags(s string) string {
	return reCredentialFlag.ReplaceAllStringFunc(s, func(m string) string {
		parts := reCredentialFlag.FindStringSubmatch(m)
		// parts[1] is the word boundary the pattern had to consume
		// (#357) and is put back verbatim; the flag is parts[2].
		if credentialFlagIsRef(parts[2]) {
			return m // reference to a secret, not a secret
		}
		return parts[1] + parts[2] + parts[3] + Redacted
	})
}

func credentialFlagIsRef(name string) bool {
	name = strings.ToLower(name)
	for _, suffix := range credentialFlagRefSuffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// credentialWords are the key-name words that mark a field as
// credential-bearing. Matching is per-word after splitting the key
// on case transitions and non-alphanumerics, so `topologyKey` splits
// to {topology,key} — "key" matches there too; key-anchored
// heuristics therefore ALWAYS also require a secret-shaped value
// (except env names and annotation keys, where the spec position
// itself is credential-suggestive and name-match alone masks).
var credentialWords = map[string]bool{
	"password": true, "passwd": true, "pwd": true,
	// 'pass' / 'passphrase' (issue #106): matched per-word by
	// credentialKey (keyWords splits DB_PASS → {db,pass}), so BYPASS,
	// COMPASS, and passenger — where "pass" is only a substring — stay
	// clean. The flag path anchors them separately (reCredentialFlag).
	"pass": true, "passphrase": true,
	"secret": true, "secrets": true,
	"token": true, "tokens": true,
	"credential": true, "credentials": true, "creds": true,
	"apikey": true, "key": true, "keys": true,
	"auth": true, "authorization": true, "bearer": true,
}

// credentialKey reports whether any word of key is a credential
// word. Words split on non-alphanumerics and lower→upper case
// transitions: DB_PASSWORD → {db,password}, secretKeyRef →
// {secret,key,ref}, example.com/api-token → {example,com,api,token}.
func credentialKey(key string) bool {
	for _, w := range keyWords(key) {
		if credentialWords[w] {
			return true
		}
	}
	return false
}

func keyWords(key string) []string {
	var words []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			words = append(words, strings.ToLower(cur.String()))
			cur.Reset()
		}
	}
	runes := []rune(key)
	for i, r := range runes {
		switch {
		case !isAlnum(r):
			flush()
		case isUpper(r) && i > 0 && isLower(runes[i-1]):
			flush()
			cur.WriteRune(r)
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return words
}

func isAlnum(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}
func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }
func isLower(r rune) bool { return r >= 'a' && r <= 'z' }

var reHex = regexp.MustCompile(`^[0-9a-fA-F]{32,}$`)
var reBase64 = regexp.MustCompile(`^[A-Za-z0-9+/_-]{20,}={0,2}$`)

// secretShaped reports whether value, sitting under a
// credential-named key, looks like secret material itself rather
// than a reference to it:
//
//   - hex ≥ 32 chars (session secrets, HMAC keys). Image digests are
//     also 64-hex but live under keys like image/digest/checksum, so
//     the key anchor keeps them; uids contain dashes and fail the
//     charset.
//   - base64-ish ≥ 20 chars with at least 3 of {upper,lower,digit}
//     character classes AND Shannon entropy ≥ 3.0 bits/char. The
//     class+entropy gate is what lets resource names through:
//     "db-credentials-prod-v2" is single-class and low-entropy,
//     random key material is neither.
func secretShaped(value string) bool {
	if reHex.MatchString(value) {
		return true
	}
	return reBase64.MatchString(value) && charClasses(value) >= 3 && shannonEntropy(value) >= 3.0
}

// charClasses counts which of {upper, lower, digit} appear.
func charClasses(s string) int {
	var upper, lower, digit bool
	for _, r := range s {
		switch {
		case isUpper(r):
			upper = true
		case isLower(r):
			lower = true
		case r >= '0' && r <= '9':
			digit = true
		}
	}
	n := 0
	for _, b := range []bool{upper, lower, digit} {
		if b {
			n++
		}
	}
	return n
}

// shannonEntropy returns the per-character Shannon entropy of s in
// bits. Random base64 measures well above 4 at 20+ chars; English
// words and DNS-label names sit near 3 or below.
func shannonEntropy(s string) float64 {
	if s == "" {
		return 0
	}
	counts := make(map[rune]int, len(s))
	total := 0
	for _, r := range s {
		counts[r]++
		total++
	}
	var h float64
	for _, c := range counts {
		p := float64(c) / float64(total)
		h -= p * math.Log2(p)
	}
	return h
}

// ---------------------------------------------------------------------------
// Spec sanitizer
// ---------------------------------------------------------------------------

// SanitizeObject sanitizes a Kubernetes object of any type — a typed
// client-go struct, an unstructured map, anything JSON-shaped — and
// returns the sanitized unstructured form. The input is never
// mutated (it is round-tripped through JSON, which also normalizes
// typed structs to their wire field names).
func SanitizeObject(obj any) (map[string]any, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("sanitize: encoding object: %w", err)
	}
	var u map[string]any
	if err := json.Unmarshal(raw, &u); err != nil {
		return nil, fmt.Errorf("sanitize: object is not a JSON object: %w", err)
	}
	return SanitizeUnstructured(u), nil
}

// SanitizeUnstructured sanitizes an unstructured Kubernetes object
// in place (and returns it for chaining):
//
//   - strips system metadata: managedFields, resourceVersion, uid,
//     generation, creationTimestamp, selfLink, ownerReferences[].uid,
//     the last-applied-configuration annotation (a full unsanitized
//     copy of the object), and noisy status (observedGeneration,
//     condition probe/heartbeat timestamps, container status
//     imageID/containerID).
//   - masks all Secret data/stringData values: keys survive, every
//     value becomes [REDACTED:<decoded length>B].
//   - masks env var values by name (credential-named) or shape;
//     valueFrom.secretKeyRef stays untouched — a named reference is
//     exactly what we want rendered.
//   - masks values of credential-named annotations, and runs the
//     value-shape heuristics over every other string in the object.
//   - elides nil values and empty maps/lists (except emptyDir, whose
//     empty object is load-bearing). Full default-eliding is a later
//     refinement.
func SanitizeUnstructured(u map[string]any) map[string]any {
	if kind, _ := u["kind"].(string); kind == "Secret" {
		maskSecretPayload(u, "data", true)
		maskSecretPayload(u, "stringData", false)
	}
	sanitizeMap(u, "")
	return u
}

// maskSecretPayload replaces every value of a Secret data/stringData
// map with a length-only redaction. base64 selects decoding for the
// `data` form so the reported length is the content length, not the
// encoding length.
func maskSecretPayload(u map[string]any, field string, isBase64 bool) {
	m, ok := u[field].(map[string]any)
	if !ok {
		return
	}
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			m[k] = Redacted
			continue
		}
		n := len(s)
		if isBase64 {
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				n = len(decoded)
			}
		}
		m[k] = RedactedLen(n)
	}
}

// systemMetadataFields are stripped from every metadata map (object
// and template): API-server bookkeeping that burns tokens and, in
// the last-applied case, duplicates the entire unsanitized object.
var systemMetadataFields = []string{
	"managedFields", "resourceVersion", "uid", "generation",
	"creationTimestamp", "selfLink",
}

// lastAppliedAnnotation carries a full JSON copy of the object as
// the user applied it — including any secret env values — so it is
// removed outright rather than sanitized.
const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// containerStatusKeys are the list keys whose entries carry runtime
// identifiers (imageID, containerID) that are pure noise for triage.
var containerStatusKeys = map[string]bool{
	"containerStatuses":          true,
	"initContainerStatuses":      true,
	"ephemeralContainerStatuses": true,
}

// sanitizeMap walks one map level. parentKey is the key this map was
// found under ("" at the object root, "metadata", "annotations",
// "env" for env entries, …) and selects position-specific rules.
func sanitizeMap(m map[string]any, parentKey string) {
	switch {
	case parentKey == "metadata":
		for _, f := range systemMetadataFields {
			delete(m, f)
		}
	case parentKey == "annotations":
		delete(m, lastAppliedAnnotation)
	case parentKey == "status":
		delete(m, "observedGeneration")
	case parentKey == "conditions":
		delete(m, "lastProbeTime")
		delete(m, "lastHeartbeatTime")
	case parentKey == "ownerReferences":
		delete(m, "uid")
	case containerStatusKeys[parentKey]:
		delete(m, "imageID")
		delete(m, "containerID")
	}
	for k, v := range m {
		m[k] = sanitizeValue(parentKey, k, v)
		// emptyDir is the documented eliding exception:
		// `emptyDir: {}` is a complete, meaningful volume source.
		if elidable(m[k]) && k != "emptyDir" {
			delete(m, k)
		}
	}
}

func sanitizeValue(parentKey, key string, v any) any {
	switch val := v.(type) {
	case string:
		// Annotation values under credential-named keys are
		// masked on the key alone: the position is
		// credential-suggestive and annotation values are
		// free-form (no reference/value distinction to preserve).
		if parentKey == "annotations" && val != "" && credentialKey(key) {
			return Redacted
		}
		return maskKeyedValue(key, val)
	case map[string]any:
		sanitizeMap(val, key)
		return val
	case []any:
		if key == "env" {
			maskEnvEntries(val)
		}
		if key == "args" || key == "command" {
			maskFlagValuePairs(val)
		}
		for i, e := range val {
			switch ev := e.(type) {
			case map[string]any:
				sanitizeMap(ev, key)
			case string:
				// Lists of strings (args, command): shape
				// heuristics only; the list key names the
				// list, not any one value.
				val[i] = MaskString(ev)
			}
		}
		return val
	default:
		return v
	}
}

// maskEnvEntries applies the env-specific rule before the generic
// walk: a literal value is fully masked when the var NAME is
// credential-shaped (DB_PASSWORD, SIGNING_KEY, …) or the VALUE is
// secret-shaped; entries with valueFrom (secretKeyRef et al.) carry
// no value and stay as named references. Partial shapes inside
// surviving values (URL passwords, PEM blocks) are handled by the
// generic MaskString pass that follows.
func maskEnvEntries(env []any) {
	for _, e := range env {
		entry, ok := e.(map[string]any)
		if !ok {
			continue
		}
		value, ok := entry["value"].(string)
		if !ok || value == "" {
			continue
		}
		name, _ := entry["name"].(string)
		if credentialKey(name) || secretShaped(value) {
			entry["value"] = Redacted
		}
	}
}

// maskFlagValuePairs handles args/command lists in the two-element
// form: a bare credential-named flag followed by its value in the
// next element (["--db-password", "hunter2"]). The single-element
// --flag=value form is handled by MaskString on each string.
func maskFlagValuePairs(list []any) {
	for i := 0; i < len(list)-1; i++ {
		s, ok := list[i].(string)
		if !ok || !reCredentialFlagName.MatchString(s) || credentialFlagIsRef(s) {
			continue
		}
		if _, ok := list[i+1].(string); ok {
			list[i+1] = Redacted
			i++
		}
	}
}

// elidable reports values cheap to drop: nils and empty
// maps/lists say nothing an agent needs.
func elidable(v any) bool {
	switch val := v.(type) {
	case nil:
		return true
	case map[string]any:
		return len(val) == 0
	case []any:
		return len(val) == 0
	}
	return false
}
