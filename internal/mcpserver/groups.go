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
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/go-steer/steward/allow"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// Group entries: "group:<group email>" in --google-allow and
// --google-allow-write admits every member of a Google group, directly or
// through nested groups. steward's allow.CloudIdentityGroups answers
// membership through the Cloud Identity Groups API, and the identity that
// asks needs the Groups Reader admin role in the group's Workspace. Admin
// roles are assigned to an email address, which lookout's Workload
// Identity principal does not have, so lookout asks as a dedicated
// service account (--google-groups-service-account) that it impersonates
// for those calls only, with the read-only groups scope and nothing else.
// The callers' own tokens never reach Cloud Identity.
//
// Verdicts are steward's to cache: a verdict for one account and group is
// reused for allow.GroupVerdictTTL (5 minutes), and so is a group's
// membership list on editions that walk it, so a removal from a group
// takes effect within about twice that.

// groupsTimeout bounds one Cloud Identity request, so a directory that
// hangs fails the admission (retryable) rather than holding the caller.
const groupsTimeout = 30 * time.Second

// serviceAccountRE is a Google service account's email.
var serviceAccountRE = regexp.MustCompile(`(?i)^[^@\s]+@[^@\s]+\.gserviceaccount\.com$`)

// groupsFlags names the flag(s) whose lists hold group entries.
func groupsFlags(allowList, writeList *allow.List) string {
	var flags []string
	if len(allowList.Groups()) > 0 {
		flags = append(flags, "--google-allow")
	}
	if len(writeList.Groups()) > 0 {
		flags = append(flags, "--google-allow-write")
	}
	return strings.Join(flags, " and ")
}

// groupChecker builds the Cloud Identity checker for the group entries of
// allowList and writeList, called as opts.GroupsServiceAccount. It returns
// nils when neither list has a group entry, and refuses the service
// account then: it would check nothing. probe is the account's token
// source, for the startup check.
func groupChecker(opts GoogleOptions, allowList, writeList *allow.List, logger *slog.Logger) (checker allow.GroupChecker, probe oauth2.TokenSource, err error) {
	sa := strings.TrimSpace(opts.GroupsServiceAccount)
	if len(allowList.Groups()) == 0 && len(writeList.Groups()) == 0 {
		if sa != "" {
			return nil, nil, fmt.Errorf("--google-groups-service-account=%s checks group: entries, and neither --google-allow nor --google-allow-write lists one", sa)
		}
		return nil, nil, nil
	}
	flags := groupsFlags(allowList, writeList)
	switch {
	case sa == "":
		return nil, nil, fmt.Errorf("%s: group: entries are checked in Cloud Identity as a service account holding the Groups Reader admin role; "+
			"pass --google-groups-service-account=<name>@<project>.iam.gserviceaccount.com (lookout's identity needs roles/iam.serviceAccountTokenCreator on it)", flags)
	case !serviceAccountRE.MatchString(sa):
		return nil, nil, fmt.Errorf("--google-groups-service-account=%s: not a service account email (<name>@<project>.iam.gserviceaccount.com)", sa)
	}
	impersonate := opts.Impersonate
	if impersonate == nil {
		impersonate = cloud.Impersonate
	}
	ts, err := impersonate(context.Background(), sa, allow.CloudIdentityGroupsScope)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: group: entries are checked as --google-groups-service-account=%s, and %w", flags, sa, err)
	}
	ts = groupsTokenSource{sa: sa, ts: oauth2.ReuseTokenSource(nil, ts)}
	return &allow.CloudIdentityGroups{
		Client:   &http.Client{Transport: &oauth2.Transport{Source: ts}, Timeout: groupsTimeout},
		Endpoint: opts.CloudIdentityEndpoint,
		Logger:   logger,
	}, ts, nil
}

// groupsTokenSource names the service account in a failed mint, so a
// group check that fails because lookout cannot impersonate it (no
// token-creator grant, or one still propagating) says so.
type groupsTokenSource struct {
	sa string
	ts oauth2.TokenSource
}

func (g groupsTokenSource) Token() (*oauth2.Token, error) {
	t, err := g.ts.Token()
	if err != nil {
		return nil, fmt.Errorf("minting a token for --google-groups-service-account=%s: %w", g.sa, err)
	}
	return t, nil
}

// probeGroupsAccount mints one token for the groups service account and
// warns if it cannot, so a missing grant shows at startup rather than as
// the first group member's refused call. Not fatal: a new grant can take
// minutes to propagate. It runs in the background, and closes done.
func probeGroupsAccount(ts oauth2.TokenSource, logger *slog.Logger, done chan<- struct{}) {
	defer close(done)
	if _, err := ts.Token(); err != nil {
		logger.Warn("lookout mcp: cannot impersonate the groups service account yet; callers admitted only through a group: entry are refused until it works "+
			"(lookout's identity needs roles/iam.serviceAccountTokenCreator on it)", "err", err)
	}
}
