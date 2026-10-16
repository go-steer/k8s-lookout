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

// Which cluster a call reads, when it does not say, or says something
// the caller's set does not hold (#700).
//
// Seeing a cluster is not reading it. Discovery asks which clusters a
// caller may list, which a folder- or organization-level role grants
// widely; reading inside one is Kubernetes RBAC (or IAM such as
// container.viewer) in that cluster. A live test showed the gap: asked
// about "my cluster", a model picked one of 48 the caller could list and
// got a scorecard of nothing but permission refusals. So each discovery
// also asks, once per cluster and as the same identity, whether it may
// list pods cluster-wide, and the set records the answer: readable
// clusters are listed first, are the only ones chosen on a caller's
// behalf, and are the ones suggested.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	authv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// What an identity may do in a cluster.
const (
	// accessRead: may list pods in every namespace.
	accessRead = "read"
	// accessLimited: may not, though it may still read some namespaces.
	accessLimited = "limited"
	// accessUnreachable: the check itself failed (no DNS endpoint, a
	// perimeter, a timeout).
	accessUnreachable = "unreachable"

	accessCheckTimeout     = 5 * time.Second
	accessCheckConcurrency = 16
	maxSuggestions         = 5
	maxElicitChoices       = 20
)

// accessFunc reports what the identity behind cfg may do in its cluster.
type accessFunc func(ctx context.Context, cfg *rest.Config) string

// reviewAccess asks the API server, as the identity in cfg, whether it
// may list pods cluster-wide: one SelfSubjectAccessReview, which needs no
// grant of its own.
func reviewAccess(ctx context.Context, cfg *rest.Config) string {
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return accessUnreachable
	}
	res, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authv1.ResourceAttributes{Verb: "list", Resource: "pods"},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return accessUnreachable
	}
	if res.Status.Allowed {
		return accessRead
	}
	return accessLimited
}

// checkAccess records each cluster's access in a fresh set, a few at a
// time, each bounded by a timeout, then orders the set: readable first,
// the default project's next, then by name.
func (c *Clusters) checkAccess(ctx context.Context, set *clusterSet) {
	if c.access != nil {
		var wg sync.WaitGroup
		slots := make(chan struct{}, accessCheckConcurrency)
		for i := range set.entries {
			wg.Add(1)
			slots <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				e := &set.entries[i]
				cfg, err := c.Config(ctx, *e)
				if err != nil {
					e.access = accessUnreachable
					return
				}
				cctx, cancel := context.WithTimeout(ctx, accessCheckTimeout)
				defer cancel()
				e.access = c.access(cctx, cfg)
			}()
		}
		wg.Wait()
	}
	slices.SortStableFunc(set.entries, func(a, b clusterEntry) int {
		if d := accessRank(a.access) - accessRank(b.access); d != 0 {
			return d
		}
		if d := c.projectRank(a) - c.projectRank(b); d != 0 {
			return d
		}
		return strings.Compare(a.name, b.name)
	})
	clear(set.byName)
	for i, e := range set.entries {
		set.byName[e.name] = i
		set.byName[e.canonical] = i
	}
}

func accessRank(a string) int {
	switch a {
	case accessRead:
		return 0
	case "":
		return 1 // unchecked: no reason to rank it below a refusal
	case accessLimited:
		return 2
	default:
		return 3
	}
}

func (c *Clusters) projectRank(e clusterEntry) int {
	if c.defaultProject != "" && e.ref.Project == c.defaultProject {
		return 0
	}
	return 1
}

// readable reports whether a call may be pointed at e on a caller's
// behalf: checked and readable, or unchecked.
func (e clusterEntry) readable() bool { return e.access == accessRead || e.access == "" }

// SetDefaults sets the cluster a call that names none reads (a name or
// canonical name in the caller's set), and the project preferred when a
// name is bare and repeats. Either may be empty.
func (c *Clusters) SetDefaults(cluster, project string) {
	c.defaultCluster, c.defaultProject = cluster, project
}

// choiceError is a call whose cluster could not be chosen: none named and
// none to default to, or a name the caller's set does not hold. It
// carries the clusters worth offering instead.
type choiceError struct {
	msg        string
	candidates []string
	unknown    bool
}

func (e *choiceError) Error() string { return e.msg }

// Is makes an unknown-name choiceError an errUnknownCluster.
func (e *choiceError) Is(target error) bool { return e.unknown && target == errUnknownCluster }

// Choose picks the cluster a call reads. A named cluster resolves as in
// Resolve, except that a bare name repeated across projects resolves to
// the default project's when there is exactly one there. With no name:
// the default cluster when the caller can read it, else the caller's only
// readable cluster in the default project, else their only readable
// cluster. Anything else is a choiceError naming the best candidates, so
// a model can choose rather than guess.
func (c *Clusters) Choose(ctx context.Context, caller, name string) (clusterEntry, error) {
	if name != "" {
		e, err := c.Resolve(ctx, caller, name)
		if !errors.Is(err, errUnknownCluster) {
			return e, err
		}
		set, serr := c.set(ctx, caller)
		if serr != nil {
			return clusterEntry{}, err
		}
		if e, ok := c.inDefaultProject(set, name); ok {
			return e, nil
		}
		cands := suggest(set, name)
		msg := fmt.Sprintf("unknown cluster %q", name)
		if len(cands) > 0 {
			msg += fmt.Sprintf("; did you mean %s?", strings.Join(cands, ", "))
		}
		return clusterEntry{}, &choiceError{
			msg:        msg + fmt.Sprintf(" k8s_list_clusters lists all %d you can pass", len(set.entries)),
			candidates: elicitChoices(set, cands),
			unknown:    true,
		}
	}
	set, err := c.set(ctx, caller)
	if err != nil {
		return clusterEntry{}, err
	}
	if c.defaultCluster != "" {
		if i, ok := set.byName[c.defaultCluster]; ok && set.entries[i].readable() {
			return set.entries[i], nil
		}
	}
	var readable, inProject []clusterEntry
	for _, e := range set.entries {
		if !e.readable() {
			continue
		}
		readable = append(readable, e)
		if c.defaultProject != "" && e.ref.Project == c.defaultProject {
			inProject = append(inProject, e)
		}
	}
	switch {
	case len(inProject) == 1:
		return inProject[0], nil
	case len(readable) == 1:
		return readable[0], nil
	}
	cands := suggest(set, "")
	msg := fmt.Sprintf("this endpoint serves %d clusters and this call names none", len(set.entries))
	if len(readable) == 0 {
		msg += "; you cannot read any of them cluster-wide"
	} else {
		msg += fmt.Sprintf("; pass cluster, e.g. %s (%d you can read)", strings.Join(cands, ", "), len(readable))
	}
	return clusterEntry{}, &choiceError{msg: msg + "; k8s_list_clusters lists them all", candidates: elicitChoices(set, cands)}
}

// inDefaultProject resolves a bare name repeated across projects to the
// default project's, when exactly one cluster there has it.
func (c *Clusters) inDefaultProject(set *clusterSet, name string) (clusterEntry, bool) {
	if c.defaultProject == "" || strings.ContainsAny(name, "@/") {
		return clusterEntry{}, false
	}
	var match []clusterEntry
	for _, e := range set.entries {
		if e.ref.Name == name && e.ref.Project == c.defaultProject {
			match = append(match, e)
		}
	}
	if len(match) == 1 {
		return match[0], true
	}
	return clusterEntry{}, false
}

// suggest names up to maxSuggestions clusters for a wrong or missing
// name, readable ones only: every cluster whose name contains it (which
// is how a bare name repeated across projects is offered), else the
// nearest names, else (no name at all) the set's first readable ones,
// already ordered default project first.
func suggest(set *clusterSet, name string) []string {
	var readable []clusterEntry
	for _, e := range set.entries {
		if e.readable() {
			readable = append(readable, e)
		}
	}
	pick := func(es []clusterEntry) []string {
		out := make([]string, 0, maxSuggestions)
		for _, e := range es {
			if len(out) == maxSuggestions {
				break
			}
			out = append(out, e.name)
		}
		return out
	}
	if name == "" {
		return pick(readable)
	}
	lower := strings.ToLower(name)
	var contains []clusterEntry
	for _, e := range readable {
		if strings.Contains(strings.ToLower(e.canonical), lower) || strings.Contains(strings.ToLower(e.name), lower) {
			contains = append(contains, e)
		}
	}
	if len(contains) > 0 {
		return pick(contains)
	}
	near := slices.Clone(readable)
	slices.SortStableFunc(near, func(a, b clusterEntry) int {
		return editDistance(lower, strings.ToLower(a.ref.Name)) - editDistance(lower, strings.ToLower(b.ref.Name))
	})
	var nearby []clusterEntry
	for _, e := range near {
		if editDistance(lower, strings.ToLower(e.ref.Name)) <= max(2, len(name)/3) {
			nearby = append(nearby, e)
		}
	}
	return pick(nearby)
}

// clusterInputID names lookout's one question to the person in a
// multi-round-trip call: which cluster.
const clusterInputID = "lookout-cluster"

// clusterAnswer is the cluster the person picked, when this call is the
// retry of one that asked (MCP multi-round-trip requests, SEP-2322).
// It is only a name: Choose checks it against the caller's own set.
func clusterAnswer(req *mcp.CallToolRequest) (picked string, answered bool) {
	resp, answered := req.Params.InputResponses[clusterInputID]
	if !answered {
		return "", false
	}
	if er, ok := resp.(*mcp.ElicitResult); ok && er.Action == "accept" {
		picked, _ = er.Content[clusterProperty].(string)
	}
	return picked, true
}

// clientCanBeAsked reports whether the session's client advertised
// elicitation. A stateless endpoint (Google-token mode, multi-cluster
// HTTP) has no initialize to read it from, so it never asks: a client
// that cannot answer would get an SDK error instead of the candidates.
func clientCanBeAsked(session *mcp.ServerSession) bool {
	if session == nil {
		return false
	}
	p := session.InitializeParams()
	return p != nil && p.Capabilities != nil && p.Capabilities.Elicitation != nil
}

// askForCluster is the result that asks the person which cluster to read,
// offering readable ones. The client answers and calls again; the SDK
// does the same over the session for clients that predate the
// multi-round-trip protocol.
func askForCluster(tool string, choice *choiceError) *mcp.CallToolResult {
	return &mcp.CallToolResult{InputRequests: mcp.InputRequestMap{clusterInputID: &mcp.ElicitParams{
		Message: fmt.Sprintf("lookout %s: which cluster should it read? (%s)", tool, choice.msg),
		RequestedSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				clusterProperty: map[string]any{
					"type":        "string",
					"title":       "Cluster",
					"description": "a cluster you can read",
					"enum":        choice.candidates,
				},
			},
			"required": []string{clusterProperty},
		},
	}}}
}

// elicitChoices is what to offer when the client can be asked: the
// suggestions first, then the rest of the readable set, capped.
func elicitChoices(set *clusterSet, first []string) []string {
	out := slices.Clone(first)
	for _, e := range set.entries {
		if len(out) >= maxElicitChoices {
			break
		}
		if e.readable() && !slices.Contains(out, e.name) {
			out = append(out, e.name)
		}
	}
	return out
}
