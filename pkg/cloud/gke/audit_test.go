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

//go:build gke || allproviders

package gke

import (
	"context"
	"strings"
	"testing"
	"time"

	logging "google.golang.org/api/logging/v2"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

var auditWindow = cloud.TimeWindow{
	Start: time.Date(2026, 7, 25, 8, 25, 0, 0, time.UTC),
	End:   time.Date(2026, 7, 25, 8, 55, 0, 0, time.UTC),
}

var auditDeployRef = cloud.AuditRef{
	APIGroup: "apps", Version: "v1", Resource: "deployments",
	Namespace: "prod", Name: "api",
}

func TestAuditFilterShape(t *testing.T) {
	f := auditFilter("proj-1", "us-east1", "prod", auditDeployRef, auditWindow)
	for _, want := range []string{
		`logName="projects/proj-1/logs/cloudaudit.googleapis.com%2Factivity"`,
		`resource.type="k8s_cluster"`,
		`resource.labels.location="us-east1"`,
		`resource.labels.cluster_name="prod"`,
		`protoPayload.resourceName="apps/v1/namespaces/prod/deployments/api"`,
		`timestamp>="2026-07-25T08:25:00Z" AND timestamp<"2026-07-25T08:55:00Z"`,
	} {
		if !strings.Contains(f, want) {
			t.Errorf("filter missing %q:\n%s", want, f)
		}
	}
}

func TestAuditResourceNameCoreGroup(t *testing.T) {
	got := auditResourceName(cloud.AuditRef{
		APIGroup: "", Version: "v1", Resource: "configmaps",
		Namespace: "prod", Name: "app-config",
	})
	want := "core/v1/namespaces/prod/configmaps/app-config"
	if got != want {
		t.Errorf("core-group resourceName = %q, want %q", got, want)
	}
}

func TestAuditObjectWrites(t *testing.T) {
	var page logging.ListLogEntriesResponse
	loadJSON(t, "logging-entries-k8s-audit.json", &page)
	lister := &fakeEntryLister{pages: []*logging.ListLogEntriesResponse{&page}}
	api := &auditAPI{project: "proj-1", location: "us-east1", cluster: "prod", entries: lister}

	writes, err := api.ObjectWrites(context.Background(), auditDeployRef, auditWindow)
	if err != nil {
		t.Fatal(err)
	}
	// Three fixture entries; the principal-less one is dropped by the
	// belt (the filter is the contract, the parse is the belt).
	if len(writes) != 2 {
		t.Fatalf("got %d writes, want 2: %+v", len(writes), writes)
	}
	if writes[0].Principal != "alice@example.com" {
		t.Errorf("writes[0].Principal = %q", writes[0].Principal)
	}
	if writes[0].Method != "io.k8s.apps.v1.deployments.patch" {
		t.Errorf("writes[0].Method = %q", writes[0].Method)
	}
	if !strings.HasPrefix(writes[0].UserAgent, "kubectl/v1.31.0") {
		t.Errorf("writes[0].UserAgent = %q", writes[0].UserAgent)
	}
	if writes[1].Principal != "deployer@proj-1.iam.gserviceaccount.com" {
		t.Errorf("writes[1].Principal = %q", writes[1].Principal)
	}
	if writes[1].UserAgent != "" {
		t.Errorf("writes[1].UserAgent = %q, want empty (no callerSuppliedUserAgent recorded)", writes[1].UserAgent)
	}

	if len(lister.reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(lister.reqs))
	}
	req := lister.reqs[0]
	if len(req.ResourceNames) != 1 || req.ResourceNames[0] != "projects/proj-1" {
		t.Errorf("ResourceNames = %v", req.ResourceNames)
	}
	if req.OrderBy != "timestamp desc" {
		t.Errorf("OrderBy = %q", req.OrderBy)
	}
}

func TestAuditPaging(t *testing.T) {
	var page logging.ListLogEntriesResponse
	loadJSON(t, "logging-entries-k8s-audit.json", &page)
	// Every page claims a successor: the loop must stop at
	// auditMaxPages, not spin forever.
	withToken := page
	withToken.NextPageToken = "next"
	pages := make([]*logging.ListLogEntriesResponse, auditMaxPages+3)
	for i := range pages {
		pages[i] = &withToken
	}
	lister := &fakeEntryLister{pages: pages}
	api := &auditAPI{project: "proj-1", location: "us-east1", cluster: "prod", entries: lister}

	writes, err := api.ObjectWrites(context.Background(), auditDeployRef, auditWindow)
	if err != nil {
		t.Fatal(err)
	}
	if len(lister.reqs) != auditMaxPages {
		t.Errorf("made %d requests, want the %d-page cap", len(lister.reqs), auditMaxPages)
	}
	if len(writes) != 2*auditMaxPages {
		t.Errorf("got %d writes, want %d", len(writes), 2*auditMaxPages)
	}
}

func TestAuditCapabilityGates(t *testing.T) {
	full := &Provider{project: "proj-1", location: "us-east1", cluster: "prod"}
	if _, ok := full.Audit(); !ok {
		t.Error("full identity: Audit() unavailable, want available")
	}
	for name, p := range map[string]*Provider{
		"no-project":  {location: "us-east1", cluster: "prod"},
		"no-location": {project: "proj-1", cluster: "prod"},
		"no-cluster":  {project: "proj-1", location: "us-east1"},
	} {
		if _, ok := p.Audit(); ok {
			t.Errorf("%s: Audit() available, want unavailable", name)
		}
	}
}
