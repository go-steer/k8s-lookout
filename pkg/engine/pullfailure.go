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

package engine

import (
	"strings"
	"sync"
	"time"
)

// PullClass is the failure class of an image-pull incident (issue
// #213): WILL this fix itself if kubelet just keeps retrying?
//
// The reason family cannot answer that. `manifest unknown` and
// `429 toomanyrequests` both canonicalize to ImagePullBackOff
// (CanonicalReasonForEvent) and are opposite incidents — one needs a
// human right now, the other needs sixty seconds of patience. Only the
// error text inside the message separates them.
type PullClass int

const (
	// PullClassNA: not an image-pull failure at all. The zero value,
	// so any signal nobody classified reads as "not applicable".
	PullClassNA PullClass = iota
	// PullClassTerminal: retrying cannot help. A bad tag, a denied
	// pull, a full disk. Fires on the first event — the #197 posture,
	// unchanged.
	PullClassTerminal
	// PullClassRetryable: a registry- or transport-side failure that
	// kubelet's own retry cycle is expected to clear — rate limits,
	// 5xx, timeouts, resets. The class the leading-edge debounce
	// (--imagepull-transient-min-count) holds.
	PullClassRetryable
	// PullClassUnknown: an image-pull failure whose cause we do not
	// recognize — including the messages that carry no cause at all
	// (kubelet's bare `Back-off pulling image "…"`, `Error:
	// ErrImagePull`). Treated as terminal for firing purposes: this
	// gate only ever suppresses what it positively recognizes.
	PullClassUnknown
)

// String renders the class for metric labels and logs.
func (c PullClass) String() string {
	switch c {
	case PullClassTerminal:
		return "terminal"
	case PullClassRetryable:
		return "retryable"
	case PullClassUnknown:
		return "unknown"
	}
	return "n/a"
}

// pullTerminalMarkers are substrings (lowercased) that prove retrying
// is pointless. Sourced from the containerd/distribution error strings
// kubelet surfaces verbatim in `Failed to pull image "…": <err>`.
var pullTerminalMarkers = []string{
	"manifest unknown",
	"not found", // `404 Not Found`, `manifest for … not found`
	"notfound",  // `rpc error: code = NotFound desc = …`
	"repository does not exist",
	"denied", // `denied: …`, `pull access denied for …`
	"unauthorized",
	"authentication required",
	"401 unauthorized",
	"403 forbidden",
	"invalid reference format",
	"invalidimagename",
	"errimageneverpull",
	// Transient-LOOKING but node-terminal: the pull will keep failing
	// until something frees disk. Not a wait-it-out condition.
	"no space left on device",
}

// pullRetryableMarkers are substrings (lowercased) that mean the
// registry or the path to it is temporarily unhappy — kubelet's own
// backoff-and-retry cycle is the correct response, and it does not
// need an agent session to supervise it.
//
// The motivating case (issue #213) is Artifact Registry's per-region
// per-minute request quota: the quota window rolls over and the pull
// succeeds, but the sentinel has already alerted.
//
// EVERY MARKER IS A PHRASE, never a bare status number (issue #216).
// `429` alone matched digits anywhere in the message — inside a sha256
// digest, a tag like `:v429`, an Artifact Registry
// `project_number:235545413903` — and a false retryable is the
// expensive direction: it holds a real failure for three events. The
// registries that rate-limit say so in words ("429 Too Many Requests",
// `toomanyrequests:`, `quota exceeded`), so the phrases carry the
// signal on their own. A naked `: 429` with no reason phrase now
// classifies Unknown and fires immediately, which is the safe miss.
var pullRetryableMarkers = []string{
	"toomanyrequests",
	"too many requests",
	"quota exceeded",
	"500 internal server error",
	"502 bad gateway",
	"503 service unavailable",
	"504 gateway timeout",
	"i/o timeout",
	"tls handshake timeout",
	"connection reset by peer",
	"connection refused",
	"unexpected eof",
	"context deadline exceeded",
	"temporary failure in name resolution",
}

// ClassifyPullFailure reads an image-pull failure's message and
// returns its class. Callers must already know the signal is in the
// image-pull family (canonical reason ImagePullBackOff); this function
// only reads the cause out of the text.
//
// TERMINAL WINS TIES. kubelet aggregates several attempts into one
// message, so a message can carry both a terminal and a retryable
// marker (a bad tag whose first attempt also timed out). Resolving
// those to terminal keeps the gate purely subtractive: it can delay
// only failures it recognizes as nothing-but-transient.
//
// The image reference is REMOVED before matching (issue #216). Its
// every component is operator-chosen text that kubelet echoes back
// several times — in the quoted ref, in `failed to resolve reference
// "…"`, in the registry URL — so a repository called `denied-team` or
// `not-found-yet` reads as a permission failure, and a tag or digest
// reads as whatever digits it happens to contain. Only the error the
// registry actually returned gets a vote.
//
// HONESTY NOTE — this is a dependency on containerd/registry error
// STRINGS, which are not API, and it is the same bargain
// CanonicalReasonForEvent already takes (see dedup.go). An unmatched
// message classifies Unknown, which fires immediately: a reworded
// error costs us a suppression we would have liked, never an incident
// we needed. The tests pin the real observed shapes so a matcher
// change is visible rather than silent.
func ClassifyPullFailure(message string) PullClass {
	m := classificationText(message)
	for _, marker := range pullTerminalMarkers {
		if strings.Contains(m, marker) {
			return PullClassTerminal
		}
	}
	for _, marker := range pullRetryableMarkers {
		if strings.Contains(m, marker) {
			return PullClassRetryable
		}
	}
	return PullClassUnknown
}

// classificationText lowercases the message and blanks out every
// occurrence of the image reference and of its parts, so that only the
// registry's own error wording is left for the markers to match.
//
// Blanking is a substring replace rather than a cut of the quoted span
// because kubelet quotes the reference once but repeats its pieces:
// `failed to resolve reference "HOST/repo@sha256:…"`, and the registry
// URL `https://HOST/v2/repo/manifests/sha256:…`, where the repository
// path and the digest appear again without the surrounding quotes.
// Replacing with a space (not "") keeps two words that straddled a
// removal from fusing into a marker that neither of them contained.
func classificationText(message string) string {
	m := strings.ToLower(message)
	ref, ok := quotedImageRef(message)
	if !ok {
		return m
	}
	for _, tok := range refTokens(strings.ToLower(ref)) {
		// Below three characters a token is more likely to shred
		// unrelated text than to hide a marker — and no marker is
		// reachable by a one- or two-character tag anyway.
		if len(tok) < 3 {
			continue
		}
		m = strings.ReplaceAll(m, tok, " ")
		// Registries that authenticate per-repository echo the
		// repository back inside a URL query, percent-encoded:
		// Artifact Registry's token fetch appends
		// `?scope=repository%3Aproj%2Fteam%2Fapp%3Apull`. Only the
		// delimiters are escaped, so the path SEGMENTS survive
		// verbatim and would still be matched — observed on a real
		// GKE 1.36 pull failure.
		if enc := refPathEscaper.Replace(tok); enc != tok {
			m = strings.ReplaceAll(m, enc, " ")
		}
	}
	return m
}

// refPathEscaper mirrors the percent-encoding a registry applies to a
// repository path it puts in a query parameter. Lowercase because the
// text being searched has already been lowercased.
var refPathEscaper = strings.NewReplacer("/", "%2f", ":", "%3a", "@", "%40")

// refTokens decomposes a lowercased image reference into the
// substrings that can show up on their own elsewhere in the message,
// longest first: the whole reference, it without the digest, it
// without the tag, the repository path with the host dropped, and the
// digest and tag by themselves.
func refTokens(ref string) []string {
	name, digest := ref, ""
	if i := strings.IndexByte(ref, '@'); i >= 0 {
		name, digest = ref[:i], ref[i+1:]
	}
	repo, tag := name, ""
	// A colon is the tag separator only after the last slash; before it
	// the colon belongs to a host:port.
	if i := strings.LastIndexByte(name, ':'); i > strings.LastIndexByte(name, '/') {
		repo, tag = name[:i], name[i+1:]
	}
	toks := []string{ref, name, repo}
	if i := strings.IndexByte(repo, '/'); i >= 0 {
		toks = append(toks, repo[i+1:])
	}
	if digest != "" {
		toks = append(toks, digest)
	}
	if tag != "" {
		toks = append(toks, tag)
	}
	return toks
}

// dockerHubHost is the canonical registry host for an image reference
// that names none (`nginx:1.25`, `library/nginx`) — the same
// normalization containerd applies.
const dockerHubHost = "docker.io"

// RegistryHost extracts the registry host from the image reference
// kubelet quotes in its pull messages: both `Failed to pull image
// "HOST/path@sha256:…": …` and the bare `Back-off pulling image
// "HOST/path:tag"` carry it. Returns "" when the message quotes no
// image reference.
//
// Host detection follows the docker reference rule: the segment before
// the first "/" is the registry only when it looks like a host (has a
// dot or a port colon, or is localhost). Otherwise the reference names
// a Docker Hub repository and the host is docker.io.
func RegistryHost(message string) string {
	ref, ok := quotedImageRef(message)
	if !ok {
		return ""
	}
	slash := strings.IndexByte(ref, '/')
	if slash < 0 {
		return dockerHubHost
	}
	head := ref[:slash]
	if head == "localhost" || strings.ContainsAny(head, ".:") {
		return head
	}
	return dockerHubHost
}

// quotedImageRef pulls the first double-quoted token following the
// word `image ` out of a kubelet pull message. Anchoring on `image "`
// rather than on the first quote in the message keeps a quoted
// container name or a quoted digest elsewhere in an aggregated error
// from being mistaken for the reference.
func quotedImageRef(message string) (string, bool) {
	const anchor = `image "`
	i := strings.Index(message, anchor)
	if i < 0 {
		return "", false
	}
	rest := message[i+len(anchor):]
	j := strings.IndexByte(rest, '"')
	if j <= 0 {
		return "", false
	}
	return rest[:j], true
}

// PullClassMemo resolves a signal's PullClass, remembering what the
// last CAUSE-BEARING message said — per object, and for retryable
// causes per registry host as well (see causeScope).
//
// The memo exists because kubelet states the cause exactly once and
// then keeps reporting the failure without it. One failed pull on GKE
// v1.36 (containerd) emits FOUR events, in order:
//
//  1. `Failed` / `Failed to pull image "…": <the actual error>` — the
//     only one that says WHY, and it is not in the shipped --reason
//     allow-list (filter.go defaultReasons). It still reaches the
//     pipeline: the k8s-events source emits every event and the engine
//     filter is what rejects it.
//  2. `Failed` / `Error: ErrImagePull` — causeless.
//  3. `BackOff` / `Back-off pulling image "…"` — allow-listed, and
//     causeless.
//  4. `Failed` / `Error: ImagePullBackOff` — causeless.
//
// Classifying only the message in hand would therefore gate nothing in
// a default deployment: a 429 would be held on event 1 and then fire
// seconds later on the first causeless one behind it. Joining them —
// which is precisely the "wait, why is this failing?" step a human
// does — is what makes the gate real.
//
// Bounded and self-expiring: every entry carries its scope's TTL and
// the map is capped, so a cluster churning through thousands of pods
// cannot grow it without limit. Safe for concurrent use.
type PullClassMemo struct {
	mu      sync.Mutex
	entries map[causeScope]pullMemoEntry
	max     int
	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// scopeKind names the breadth of one piece of cause evidence: how far
// beyond the object that reported it the cause is allowed to speak.
type scopeKind uint8

const (
	// scopeObject: evidence about ONE object, keyed on its UID. The
	// original behaviour — kubelet's causeless follow-on events for
	// the same pod inherit the cause its `Failed` event stated.
	scopeObject scopeKind = iota
	// scopeRegistryHost: evidence about a REGISTRY, keyed on the host
	// in the image reference. A per-region quota or a registry outage
	// is not a property of the pod that happened to report it — it is
	// a property of the host, and every pod pulling from that host is
	// experiencing the same fault. Without this scope the fault is
	// only visible on the events that happen to name a cause, which
	// on a real GKE cluster is a small minority of them (issue #225):
	// seven pods hit one Artifact Registry 429 and only the two that
	// emitted a `Failed …429` reached the correlator with a registry
	// key — below DefaultStormMin, so no storm formed and each dug
	// its own root cause.
	scopeRegistryHost
)

// causeScope is one memo key: a scope kind plus its identity. A struct
// key rather than a prefixed string so a UID that looks like a
// hostname can never collide with one.
type causeScope struct {
	kind scopeKind
	id   string
}

type pullMemoEntry struct {
	class PullClass
	at    time.Time
	// ttl is the scope's freshness bound, carried per entry because
	// the scopes expire at very different rates.
	ttl time.Duration
}

const (
	// defaultPullMemoTTL bounds how long a cause carries forward to
	// the causeless follow-on events for the same object. Comfortably
	// longer than kubelet's pull-backoff ceiling (300s) so a cause
	// stays attached for the whole retry cycle, short enough that a
	// pod which later fails for a DIFFERENT reason is not judged on
	// stale evidence.
	defaultPullMemoTTL = 10 * time.Minute
	// hostPullMemoTTL bounds how long a retryable cause observed on
	// ONE object speaks for OTHER objects pulling from the same
	// registry host. Deliberately far shorter than the per-object TTL:
	// this is the scope that can be wrong about an object that never
	// reported a cause of its own, so it is sized to cover a burst
	// (rate limits arrive in a clump and clear in a clump) and not
	// much more. A host that is genuinely still throttling keeps
	// re-arming it — every fresh `Failed …429` rewrites the entry.
	hostPullMemoTTL = 2 * time.Minute
	// maxPullMemoEntries caps the memo. Pull failures are a small
	// fraction of a cluster's objects; this is generous.
	maxPullMemoEntries = 4096
)

// NewPullClassMemo constructs a memo with the shipped TTLs and bound.
func NewPullClassMemo() *PullClassMemo {
	return &PullClassMemo{
		entries: make(map[causeScope]pullMemoEntry),
		max:     maxPullMemoEntries,
	}
}

func (m *PullClassMemo) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

// Resolve returns the PullClass for sig, recording the cause when this
// signal carries one. Non-image-pull signals answer PullClassNA
// without touching the memo. A nil memo resolves message-only (no
// carry-forward), so callers may leave it unset.
//
// Causeless messages inherit from the NARROWEST fresh scope that
// matches: the object's own evidence first, the registry host's only
// if the object has none. Evidence about this exact pod always beats
// evidence about the registry it pulls from.
//
// ONLY RETRYABLE CAUSES PROPAGATE HOST-WIDE. A terminal cause is a
// statement about one image reference — `manifest unknown` for a bad
// tag says nothing about the next pod's pull — whereas a rate limit or
// a 503 is a statement about the registry itself. Keeping the host
// scope retryable-only also keeps its blast radius bounded: the worst
// it can do is delay a firing by --imagepull-transient-min-count
// events, never suppress one outright.
//
// HONESTY NOTE — the exposure this buys is real and worth stating. A
// pod whose pull is failing TERMINALLY, whose own cause-bearing event
// we never saw, and which pulls from a host that is concurrently rate
// limiting, will inherit retryable and be held for the debounce. That
// needs all three to coincide; the cause-bearing event for the
// terminal failure normally arrives first and wins the object scope
// (terminal also wins ties inside ClassifyPullFailure). Weighed
// against the alternative — a cluster-wide registry fault arriving as
// N separate incidents, which is the thing that actually happens —
// this is the cheaper mistake.
func (m *PullClassMemo) Resolve(sig Signal) PullClass {
	if CanonicalReasonForEvent(sig.Key.Reason, sig.Message) != "ImagePullBackOff" {
		return PullClassNA
	}
	class := ClassifyPullFailure(sig.Message)
	if m == nil {
		return class
	}
	object := causeScope{kind: scopeObject, id: sig.Key.UID}
	host, hasHost := causeScope{}, false
	if h := RegistryHost(sig.Message); h != "" {
		host, hasHost = causeScope{kind: scopeRegistryHost, id: h}, true
	}
	now := m.clock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if class != PullClassUnknown {
		// This message names a cause: it is the new evidence of
		// record for the object, and — when the registry itself is
		// what is unhappy — for the host.
		m.evictIfFull(now)
		m.entries[object] = pullMemoEntry{class: class, at: now, ttl: defaultPullMemoTTL}
		if hasHost && class == PullClassRetryable {
			m.entries[host] = pullMemoEntry{class: class, at: now, ttl: hostPullMemoTTL}
		}
		return class
	}
	// Causeless message (the bare back-off, a sync-result error):
	// inherit from the narrowest fresh scope.
	if prior, ok := m.lookup(object, now); ok {
		return prior
	}
	if hasHost {
		if prior, ok := m.lookup(host, now); ok {
			return prior
		}
	}
	return PullClassUnknown
}

// lookup reads one scope's evidence, dropping it if it has aged out.
// Caller holds mu.
func (m *PullClassMemo) lookup(s causeScope, now time.Time) (PullClass, bool) {
	prior, ok := m.entries[s]
	if !ok {
		return PullClassNA, false
	}
	if now.Sub(prior.at) > prior.ttl {
		delete(m.entries, s)
		return PullClassNA, false
	}
	return prior.class, true
}

// evictIfFull is called under lock: drops expired entries, and if that
// did not get the map under its cap, the oldest entry.
func (m *PullClassMemo) evictIfFull(now time.Time) {
	if len(m.entries) < m.max {
		return
	}
	for s, e := range m.entries {
		if now.Sub(e.at) > e.ttl {
			delete(m.entries, s)
		}
	}
	if len(m.entries) < m.max {
		return
	}
	var oldest causeScope
	var oldestAt time.Time
	first := true
	for s, e := range m.entries {
		if first || e.at.Before(oldestAt) {
			oldest, oldestAt, first = s, e.at, false
		}
	}
	delete(m.entries, oldest)
}

// Len reports the memo size. Test helper.
func (m *PullClassMemo) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}
