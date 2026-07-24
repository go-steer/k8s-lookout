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

package logs

// Drain-style template clustering (DESIGN.md §5, `triage logs`): a
// fixed-depth parse tree over whitespace-tokenized lines, not a flat
// regex+hash. Lines are first bucketed by token count, then by their
// first treeDepth tokens, and only then compared token-by-token
// against the leaf's clusters — so the expensive similarity check
// runs against a handful of candidates, and near-identical templates
// land in the same leaf instead of hashing apart.
//
// Before a line enters the tree, obviously-variable tokens (numbers,
// hex ids, uuids, IPs, timestamps, durations, sizes) are pre-masked
// to the `<*>` wildcard. This is the single highest-leverage trick
// for clustering quality: "GET /api/user/8231 200 15ms" and
// "GET /api/user/9017 200 8ms" become identical before any
// similarity math, and the parameter positions can never fragment
// the tree's branch tokens. Remaining variable words (pod names,
// request ids that survive masking) are absorbed into `<*>` by the
// similarity merge instead.
//
// Dependency-free by design: hand-rolled from the Drain paper's idea
// (He et al., ICWS 2017), sized for a one-shot CLI pass, not a
// streaming daemon.

import (
	"regexp"
	"strings"
	"time"
)

const (
	// wildcard marks a parameter position in a template.
	wildcard = "<*>"
	// treeDepth is how many leading tokens branch the parse tree
	// below the token-count root.
	treeDepth = 2
	// maxChildren caps a node's branches; overflow tokens route to
	// the wildcard branch so high-cardinality first tokens (e.g.
	// bare request ids) cannot balloon the tree.
	maxChildren = 64
	// simThreshold is the fraction of positionally-matching tokens
	// required to merge a line into an existing cluster. 0.5 is the
	// Drain paper's neighborhood; pre-masking lets us sit at the
	// stricter end without fragmenting.
	simThreshold = 0.5
	// maxCountKey caps the token-count root key so very long lines
	// (giant JSON blobs) share one bucket instead of one bucket per
	// unique length. Clusters still only merge at equal length.
	maxCountKey = 40
	// maxSample bounds the representative raw line kept per
	// cluster; token density is the mission.
	maxSample = 200
)

// podKey identifies one contributing pod.
type podKey struct {
	namespace string
	pod       string
}

// entry is one log line after the kubelet timestamp prefix is split
// off. ts is zero when no timestamp was parseable.
type entry struct {
	ts   time.Time
	text string
}

// cluster is one merged template with its bookkeeping.
type cluster struct {
	template []string
	count    int
	pods     map[podKey]struct{}
	first    time.Time
	last     time.Time
	sample   string
	level    int
}

// drainTree is the fixed-depth parse tree.
type drainTree struct {
	roots map[int]*drainNode
	all   []*cluster
}

type drainNode struct {
	children map[string]*drainNode
	clusters []*cluster
}

func newDrainTree() *drainTree {
	return &drainTree{roots: map[int]*drainNode{}}
}

// add inserts one line, merging it into an existing cluster when the
// similarity threshold allows, creating a new cluster otherwise.
func (t *drainTree) add(pod podKey, en entry) {
	tokens := tokenizeMask(en.text)
	if len(tokens) == 0 {
		return
	}
	key := min(len(tokens), maxCountKey)
	node := t.roots[key]
	if node == nil {
		node = &drainNode{}
		t.roots[key] = node
	}
	for i := 0; i < treeDepth && i < len(tokens); i++ {
		tok := branchToken(tokens[i])
		if node.children == nil {
			node.children = map[string]*drainNode{}
		}
		child, ok := node.children[tok]
		if !ok {
			if len(node.children) >= maxChildren {
				tok = wildcard
				child = node.children[tok]
			}
			if child == nil {
				child = &drainNode{}
				node.children[tok] = child
			}
		}
		node = child
	}

	var best *cluster
	bestSim := 0.0
	for _, c := range node.clusters {
		if len(c.template) != len(tokens) {
			continue
		}
		if s := similarity(c.template, tokens); s > bestSim {
			best, bestSim = c, s
		}
	}
	lvl := guessLevel(en.text)
	if best != nil && bestSim >= simThreshold {
		merge(best.template, tokens)
		best.observe(pod, en, lvl)
		return
	}
	c := &cluster{
		template: append([]string(nil), tokens...),
		pods:     map[podKey]struct{}{},
		sample:   truncate(en.text, maxSample),
	}
	c.observe(pod, en, lvl)
	node.clusters = append(node.clusters, c)
	t.all = append(t.all, c)
}

// observe folds one line's bookkeeping into the cluster.
func (c *cluster) observe(pod podKey, en entry, lvl int) {
	c.count++
	c.pods[pod] = struct{}{}
	if lvl > c.level {
		c.level = lvl
	}
	if !en.ts.IsZero() {
		if c.first.IsZero() || en.ts.Before(c.first) {
			c.first = en.ts
		}
		if c.last.IsZero() || en.ts.After(c.last) {
			c.last = en.ts
		}
	}
}

// branchToken maps a token to its tree branch. Tokens containing
// digits route to the wildcard branch (the Drain paper's heuristic):
// a digit in a leading token usually means a variable that survived
// pre-masking (request ids, /api/user/u8231 paths), and branching on
// it would fragment one template across hundreds of leaves.
func branchToken(tok string) string {
	if strings.Contains(tok, wildcard) || strings.ContainsAny(tok, "0123456789") {
		return wildcard
	}
	return tok
}

// similarity is the fraction of positions where the template matches
// the line (a template wildcard matches anything). Lengths are equal
// by construction.
func similarity(tpl, tokens []string) float64 {
	same := 0
	for i := range tpl {
		if tpl[i] == wildcard || tpl[i] == tokens[i] {
			same++
		}
	}
	return float64(same) / float64(len(tpl))
}

// merge widens the template in place: every mismatching position
// becomes a wildcard. key=value tokens that disagree only in the
// value keep the key ("path=<*>", not "<*>") — the key name is
// structure, and structure is what the template exists to keep.
func merge(tpl, tokens []string) {
	for i := range tpl {
		if tpl[i] == wildcard || tpl[i] == tokens[i] {
			continue
		}
		if k1, _, ok := strings.Cut(tpl[i], "="); ok {
			if k2, _, ok := strings.Cut(tokens[i], "="); ok && k1 == k2 {
				tpl[i] = k1 + "=" + wildcard
				continue
			}
		}
		tpl[i] = wildcard
	}
}

// ---------------------------------------------------------------------------
// Tokenizing and pre-masking
// ---------------------------------------------------------------------------

// Variable-token shapes pre-masked to the wildcard before tree
// insertion. Each is a whole-token (post punctuation-trim) match.
var (
	reNum       = regexp.MustCompile(`^[+-]?\d+([.,]\d+)*%?$`)
	reDur       = regexp.MustCompile(`^\d+(\.\d+)?(ns|us|µs|ms|s|m|h)$`)
	reSize      = regexp.MustCompile(`(?i)^\d+(\.\d+)?[kmgtp]i?b$`)
	reHex       = regexp.MustCompile(`^(0[xX][0-9a-fA-F]+|[0-9a-fA-F]{8,})$`)
	reUUID      = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	reIP        = regexp.MustCompile(`^\d{1,3}(\.\d{1,3}){3}(:\d+)?$`)
	reISO       = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}.*)?$`)
	reWall      = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}([.,]\d+)?Z?$`)
	reDateSlash = regexp.MustCompile(`^\d{4}/\d{2}/\d{2}$`)
)

// tokenizeMask splits on whitespace and masks variable-shaped
// tokens. key=value tokens keep the key and mask only the value, so
// logfmt-style lines cluster on their keys.
func tokenizeMask(text string) []string {
	fields := strings.Fields(text)
	for i, f := range fields {
		fields[i] = maskToken(f, 0)
	}
	return fields
}

func maskToken(tok string, depth int) string {
	if depth < 2 {
		if k, v, ok := strings.Cut(tok, "="); ok && k != "" && v != "" {
			return k + "=" + maskToken(v, depth+1)
		}
	}
	pre, core, suf := trimPunct(tok)
	if core == "" {
		return tok
	}
	if isVariableToken(core) {
		return pre + wildcard + suf
	}
	return tok
}

// trimPunct peels wrapping punctuation so "(0x7f3a2b90):" still
// masks its hex core while keeping the wrapper in the template.
func trimPunct(tok string) (pre, core, suf string) {
	start, end := 0, len(tok)
	for start < end && strings.ContainsRune(`([{<"'`+"`", rune(tok[start])) {
		start++
	}
	for end > start && strings.ContainsRune(`)]}>"'.,;:`+"`", rune(tok[end-1])) {
		end--
	}
	return tok[:start], tok[start:end], tok[end:]
}

// isVariableToken reports whether a token core is variable-shaped:
// numbers, durations, sizes, hex ids (must contain a digit so plain
// words never match), uuids, IPs, and timestamps.
func isVariableToken(core string) bool {
	if reNum.MatchString(core) || reDur.MatchString(core) || reSize.MatchString(core) {
		return true
	}
	if reHex.MatchString(core) && strings.ContainsAny(core, "0123456789") {
		return true
	}
	return reUUID.MatchString(core) || reIP.MatchString(core) ||
		reISO.MatchString(core) || reWall.MatchString(core) || reDateSlash.MatchString(core)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Cut on a rune boundary so we never emit split UTF-8.
	cut := n
	for cut > 0 && !isRuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "..."
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
