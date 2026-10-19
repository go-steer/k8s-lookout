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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/go-steer/steward/allow"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

const (
	groupsSA      = "lookout-groups@example-project.iam.gserviceaccount.com"
	groupsSAToken = "groups-sa-token-0123456789"
	sreGroup      = "sre@cymbal.example"
	writersGroup  = "writers@cymbal.example"
)

// fakeCloudIdentity answers the two Cloud Identity calls a group check
// makes on an edition with the transitive check (groups:lookup, then
// checkTransitiveMembership) from members (group -> member emails), and
// records the bearer token each request carried. While down is set it
// answers 500.
type fakeCloudIdentity struct {
	url     string
	down    atomic.Bool
	mu      sync.Mutex
	bearers []string
}

func newFakeCloudIdentity(t *testing.T, members map[string][]string) *fakeCloudIdentity {
	t.Helper()
	f := &fakeCloudIdentity{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.bearers = append(f.bearers, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		f.mu.Unlock()
		if f.down.Load() {
			http.Error(w, `{"error":{"code":500}}`, http.StatusInternalServerError)
			return
		}
		switch {
		case r.URL.Path == "/groups:lookup":
			g := r.URL.Query().Get("groupKey.id")
			if _, ok := members[g]; !ok {
				http.Error(w, `{"error":{"code":404}}`, http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"name": "groups/" + g})
		case strings.HasSuffix(r.URL.Path, "/memberships:checkTransitiveMembership"):
			g := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/groups/"), "/memberships:checkTransitiveMembership")
			q := r.URL.Query().Get("query") // member_key_id == '<email>'
			member := strings.TrimSuffix(strings.TrimPrefix(q, "member_key_id == '"), "'")
			_ = json.NewEncoder(w).Encode(map[string]bool{"hasMembership": slices.Contains(members[g], member)})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	f.url = srv.URL + "/"
	return f
}

func (f *fakeCloudIdentity) tokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.bearers...)
}

// groupsImpersonator stands in for IAM Credentials generateAccessToken.
// It records what it was asked for; with fail set, the token source it
// returns fails every mint (the token-creator grant missing).
type groupsImpersonator struct {
	mu    sync.Mutex
	asked []string // "<sa> <scopes>"
	fail  bool
}

func (g *groupsImpersonator) impersonate(_ context.Context, sa string, scopes ...string) (oauth2.TokenSource, error) {
	g.mu.Lock()
	g.asked = append(g.asked, sa+" "+strings.Join(scopes, ","))
	g.mu.Unlock()
	return tokenSourceFunc(func() (*oauth2.Token, error) {
		if g.fail {
			return nil, errors.New("iam: PERMISSION_DENIED: Permission 'iam.serviceAccounts.getAccessToken' denied")
		}
		return &oauth2.Token{AccessToken: groupsSAToken, Expiry: time.Now().Add(time.Hour)}, nil
	}), nil
}

type tokenSourceFunc func() (*oauth2.Token, error)

func (f tokenSourceFunc) Token() (*oauth2.Token, error) { return f() }

// withGroups configures group entries checked against ci as groupsSA.
func withGroups(ci *fakeCloudIdentity, imp *groupsImpersonator, allowList, writeList []string) func(*GoogleOptions) {
	return func(o *GoogleOptions) {
		o.Allow, o.AllowWrite = allowList, writeList
		o.GroupsServiceAccount = groupsSA
		o.Impersonate = imp.impersonate
		o.CloudIdentityEndpoint = ci.url
	}
}

// waitProbed waits for the startup mint to finish.
func waitProbed(t *testing.T, g *GoogleAuth) {
	t.Helper()
	select {
	case <-g.groupsProbed:
	case <-time.After(5 * time.Second):
		t.Fatal("the groups service account's startup mint never finished")
	}
}

// TestGoogleGroupsPathA: Gemini Enterprise callers are admitted by group
// membership, checked in Cloud Identity as the groups service account
// with the read-only groups scope; a group on --google-allow-write grants
// write. No token, the caller's or the service account's, reaches a
// response, the access log or the diagnostics, and no caller's token
// reaches Cloud Identity.
func TestGoogleGroupsPathA(t *testing.T) {
	ci := newFakeCloudIdentity(t, map[string][]string{
		sreGroup:     {"alice@cymbal.example", "bob@cymbal.example"},
		writersGroup: {"alice@cymbal.example"},
	})
	imp := &groupsImpersonator{}
	s := startGoogleServerOpts(t, true, withGroups(ci, imp, []string{"group:" + sreGroup}, []string{"group:" + writersGroup}))
	waitProbed(t, s.auth)

	// Members read with their own token; a non-member is refused before
	// any tool runs.
	for _, tok := range []string{"alice-token", "bob-token"} {
		resp, body := s.post(t, "/mcp", tok, callBody("k8s_whoami"))
		if resp.StatusCode != http.StatusOK || !strings.Contains(body, "caller-token-fingerprint="+tokenFingerprint(tok)) {
			t.Errorf("%s (a member of %s): %d %s", tok, sreGroup, resp.StatusCode, body)
		}
	}
	if resp, _ := s.post(t, "/mcp", "eve-token", callBody("k8s_whoami")); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("eve (no member): %d, want 401", resp.StatusCode)
	}

	// The write tool: alice is in the write group, bob is not.
	_, body := s.post(t, "/mcp", "bob-token", callBody("k8s_write"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, "google-allow-write") {
		t.Errorf("bob writing: %s", body)
	}
	_, body = s.post(t, "/mcp", "alice-token", callBody("k8s_write"))
	if strings.Contains(body, `"isError":true`) || !strings.Contains(body, tokenFingerprint("alice-token")) {
		t.Errorf("alice writing through %s: %s", writersGroup, body)
	}

	imp.mu.Lock()
	asked := slices.Clone(imp.asked)
	imp.mu.Unlock()
	if want := groupsSA + " " + allow.CloudIdentityGroupsScope; len(asked) != 1 || asked[0] != want {
		t.Errorf("impersonated %q, want exactly [%q]", asked, want)
	}
	bearers := ci.tokens()
	if len(bearers) == 0 {
		t.Fatal("no group check reached Cloud Identity")
	}
	for _, b := range bearers {
		if b != groupsSAToken {
			t.Errorf("a Cloud Identity request carried %q, want the groups service account's token", b)
		}
	}
	// Verdicts are cached: calling again asks Cloud Identity nothing.
	n := len(bearers)
	s.post(t, "/mcp", "bob-token", callBody("k8s_whoami"))
	s.post(t, "/mcp", "alice-token", callBody("k8s_write"))
	if got := len(ci.tokens()); got != n {
		t.Errorf("%d more Cloud Identity requests for verdicts already made, want 0", got-n)
	}

	s.mu.Lock()
	logText := s.log.String() + s.banner.String()
	s.mu.Unlock()
	for _, want := range []string{"caller=alice@cymbal.example", "caller=bob@cymbal.example"} {
		if !strings.Contains(logText, want) {
			t.Errorf("access log lacks %q:\n%s", want, logText)
		}
	}
	assertNoSecrets(t, "the access log and diagnostics", logText, groupsSAToken, "alice-token", "bob-token", "eve-token")
}

// TestGoogleGroupsEmailEntriesSkipTheDirectory: an account an email or
// domain entry admits costs no Cloud Identity call.
func TestGoogleGroupsEmailEntriesSkipTheDirectory(t *testing.T) {
	ci := newFakeCloudIdentity(t, map[string][]string{sreGroup: {"alice@cymbal.example"}})
	s := startGoogleServerOpts(t, true, withGroups(ci, &groupsImpersonator{}, []string{"bob@cymbal.example", "group:" + sreGroup}, nil))
	if resp, body := s.post(t, "/mcp", "bob-token", callBody("k8s_whoami")); resp.StatusCode != http.StatusOK {
		t.Fatalf("bob: %d %s", resp.StatusCode, body)
	}
	if n := len(ci.tokens()); n != 0 {
		t.Errorf("%d Cloud Identity requests for an account an email entry admits, want 0", n)
	}
}

// TestGoogleGroupsDirectoryDown: a group check that fails is not a
// refusal. A Path A call is turned away (purser's enforcing middleware
// answers 401 to any authentication error) without the "no valid
// credential" of a refusal, the operator's log says why, and the failure
// is not cached: once the directory answers, the caller gets in. The
// write gate tells the caller to retry and keeps the detail for the
// operator's log.
func TestGoogleGroupsDirectoryDown(t *testing.T) {
	ci := newFakeCloudIdentity(t, map[string][]string{writersGroup: {"alice@cymbal.example"}})
	s := startGoogleServerOpts(t, true, withGroups(ci, &groupsImpersonator{}, []string{"group:" + writersGroup}, []string{"group:" + writersGroup}))
	ci.down.Store(true)
	if resp, body := s.post(t, "/mcp", "alice-token", callBody("k8s_whoami")); resp.StatusCode == http.StatusOK || strings.Contains(body, "no valid credential") {
		t.Errorf("admission with the directory down: %d %s", resp.StatusCode, body)
	}
	s.mu.Lock()
	diag := s.banner.String()
	s.mu.Unlock()
	if !strings.Contains(diag, "allow-list group check") || !strings.Contains(diag, "HTTP 500") {
		t.Errorf("the operator's log lacks the failed admission:\n%s", diag)
	}
	ci.down.Store(false)
	if resp, body := s.post(t, "/mcp", "alice-token", callBody("k8s_whoami")); resp.StatusCode != http.StatusOK {
		t.Errorf("alice once the directory answers: %d %s", resp.StatusCode, body)
	}

	// The write gate, with admission already decided by an email entry.
	ci2 := newFakeCloudIdentity(t, map[string][]string{writersGroup: {"alice@cymbal.example"}})
	s2 := startGoogleServerOpts(t, true, withGroups(ci2, &groupsImpersonator{}, []string{"@cymbal.example"}, []string{"group:" + writersGroup}))
	ci2.down.Store(true)
	_, body := s2.post(t, "/mcp", "alice-token", callBody("k8s_write"))
	if !strings.Contains(body, `"isError":true`) || !strings.Contains(body, "retry") || strings.Contains(body, "HTTP 500") {
		t.Errorf("the write gate with the directory down: %s", body)
	}
	s2.mu.Lock()
	diag = s2.banner.String()
	s2.mu.Unlock()
	if !strings.Contains(diag, "--google-allow-write group check failed") || !strings.Contains(diag, "HTTP 500") {
		t.Errorf("the operator's log lacks the failed group check:\n%s", diag)
	}
	// Back up: the failure was not cached as a refusal.
	ci2.down.Store(false)
	_, body = s2.post(t, "/mcp", "alice-token", callBody("k8s_write"))
	if strings.Contains(body, `"isError":true`) {
		t.Errorf("alice writing once the directory answers: %s", body)
	}
}

// TestGoogleGroupsImpersonationFails: lookout cannot mint the groups
// service account's token (no token-creator grant yet). Startup warns,
// naming the account; a caller only a group admits is turned away (not
// as a refusal), and the operator's log says why; nothing reads as
// lookout.
func TestGoogleGroupsImpersonationFails(t *testing.T) {
	ci := newFakeCloudIdentity(t, map[string][]string{sreGroup: {"alice@cymbal.example"}})
	imp := &groupsImpersonator{fail: true}
	s := startGoogleServerOpts(t, true, withGroups(ci, imp, []string{"group:" + sreGroup}, nil))
	waitProbed(t, s.auth)
	s.mu.Lock()
	diag := s.banner.String()
	s.mu.Unlock()
	if !strings.Contains(diag, "cannot impersonate the groups service account") || !strings.Contains(diag, groupsSA) ||
		!strings.Contains(diag, "serviceAccountTokenCreator") {
		t.Errorf("no startup warning naming the account:\n%s", diag)
	}
	resp, body := s.post(t, "/mcp", "alice-token", callBody("k8s_whoami"))
	if resp.StatusCode == http.StatusOK || strings.Contains(body, "no valid credential") || strings.Contains(body, "caller-token-fingerprint") {
		t.Errorf("a group member while impersonation fails: %d %s", resp.StatusCode, body)
	}
	if n := len(ci.tokens()); n != 0 {
		t.Errorf("%d Cloud Identity requests went out without the account's token", n)
	}
	s.mu.Lock()
	diag = s.banner.String()
	s.mu.Unlock()
	if !strings.Contains(diag, "minting a token for --google-groups-service-account="+groupsSA) {
		t.Errorf("the failed admission does not say why:\n%s", diag)
	}
	assertNoSecrets(t, "the diagnostics", diag, "alice-token", groupsSAToken)
}

// TestGoogleGroupsPathB: people signing in through lookout's own
// authorization server are admitted by the same group entries, and a
// group on --google-allow-write grants write together with the
// lookout:write scope.
func TestGoogleGroupsPathB(t *testing.T) {
	ci := newFakeCloudIdentity(t, map[string][]string{
		sreGroup:     {"alice@cymbal.example", "bob@cymbal.example"},
		writersGroup: {"alice@cymbal.example"},
	})
	imp := &groupsImpersonator{}
	s := startPathBServer(t, pathBConfig{clusterRead: true,
		mutate: withGroups(ci, imp, []string{"group:" + sreGroup}, []string{"group:" + writersGroup})})

	bob := s.mustSignIn(t, "bob@cymbal.example", "")
	if _, body := s.post(t, "/mcp", bob, callBody("k8s_whoami")); !strings.Contains(body, tokenFingerprint(s.signIn.token("bob@cymbal.example"))) {
		t.Errorf("bob (a member) via Path B: %s", body)
	}
	if tok, err := s.signInAs(t, "dana@cymbal.example", ""); err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Errorf("dana (no member) signed in (%q, %v)", tok, err)
	}

	bobW := s.mustSignIn(t, "bob@cymbal.example", ScopeRead+" "+ScopeWrite)
	if _, body := s.post(t, "/mcp", bobW, callBody("k8s_write")); !strings.Contains(body, `"isError":true`) || !strings.Contains(body, "google-allow-write") {
		t.Errorf("bob writing (not in the write group): %s", body)
	}
	alice := s.mustSignIn(t, "alice@cymbal.example", "")
	if _, body := s.post(t, "/mcp", alice, callBody("k8s_write")); !strings.Contains(body, `"isError":true`) || !strings.Contains(body, ScopeWrite) {
		t.Errorf("alice writing without lookout:write: %s", body)
	}
	aliceW := s.mustSignIn(t, "alice@cymbal.example", ScopeRead+" "+ScopeWrite)
	if _, body := s.post(t, "/mcp", aliceW, callBody("k8s_write")); strings.Contains(body, `"isError":true`) {
		t.Errorf("alice writing through %s with lookout:write: %s", writersGroup, body)
	}

	for _, b := range ci.tokens() {
		if b != groupsSAToken {
			t.Errorf("a Cloud Identity request carried %q, want the groups service account's token", b)
		}
	}
	assertNoSecrets(t, "the access log and banner", s.logText(), groupsSAToken, bob, bobW, alice, aliceW,
		s.signIn.token("alice@cymbal.example"), s.signIn.token("bob@cymbal.example"))
}

func TestNewGoogleAuthGroupsRefuses(t *testing.T) {
	t.Parallel()
	imp := &groupsImpersonator{}
	base := func() GoogleOptions {
		return GoogleOptions{
			ClientIDs: []string{googleClientID}, Allow: []string{"group:" + sreGroup}, Resource: "https://l.example/mcp",
			GroupsServiceAccount: groupsSA, Impersonate: imp.impersonate,
		}
	}
	if _, err := NewGoogleAuth(base()); err != nil {
		t.Fatalf("valid options refused: %v", err)
	}
	noImpersonator := func(context.Context, string, ...string) (oauth2.TokenSource, error) {
		return nil, cloud.ErrNoImpersonator
	}
	for name, tc := range map[string]struct {
		mutate func(*GoogleOptions)
		want   string
	}{
		"group allow entry without the account": {func(o *GoogleOptions) { o.GroupsServiceAccount = "" }, "--google-allow: group: entries"},
		"group write entry without the account": {func(o *GoogleOptions) {
			o.Allow, o.AllowWrite, o.GroupsServiceAccount = []string{"@x.example"}, []string{"group:" + writersGroup}, ""
		}, "--google-allow-write: group: entries"},
		"the account without group entries": {func(o *GoogleOptions) { o.Allow = []string{"@x.example"} }, "neither --google-allow nor --google-allow-write"},
		"not a service account":             {func(o *GoogleOptions) { o.GroupsServiceAccount = "dana@cymbal.example" }, "not a service account email"},
		"a build that cannot impersonate":   {func(o *GoogleOptions) { o.Impersonate = noImpersonator }, "-tags gke"},
	} {
		o := base()
		tc.mutate(&o)
		_, err := NewGoogleAuth(o)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error naming %q", name, err, tc.want)
		}
	}
}

// TestNewGoogleAuthGroupsNeedTheGKEBuild: without the GKE provider, group
// entries are refused at startup rather than never matching.
func TestNewGoogleAuthGroupsNeedTheGKEBuild(t *testing.T) {
	if cloud.CanImpersonate() {
		t.Skip("this build can impersonate")
	}
	_, err := NewGoogleAuth(GoogleOptions{
		ClientIDs: []string{googleClientID}, Allow: []string{"group:" + sreGroup}, Resource: "https://l.example/mcp",
		GroupsServiceAccount: groupsSA,
	})
	if err == nil || !strings.Contains(err.Error(), "-tags gke") {
		t.Errorf("group entries in a build without the GKE provider: %v", err)
	}
}
