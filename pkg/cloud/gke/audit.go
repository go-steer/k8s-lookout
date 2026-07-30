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

// AuditAPI implementation (post-M5 #128): Kubernetes object write
// attribution from Cloud Audit Logs. GKE mirrors the API server's
// audit trail into Cloud Logging as admin-activity entries on the
// k8s_cluster monitored resource; the activity log carries WRITES
// only (reads go to the separate data_access log this reader never
// touches), so filtering by resourceName is already write-scoped.
// This is the identity read `stab drift --identity` uses to resolve
// a managedFields manager string to the principal who wrote it.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	logging "google.golang.org/api/logging/v2"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// auditMaxPages bounds the paging loop. Queries are per-object over
// narrow windows; an object with >5k writes in such a window is being
// hammered by a controller and the newest pages carry the answer.
const auditMaxPages = 5

// auditEntryLister is the §13 small client interface over the one
// Cloud Logging call the reader needs; production is entries.list,
// tests replay recorded response fixtures.
type auditEntryLister interface {
	ListEntries(ctx context.Context, req *logging.ListLogEntriesRequest) (*logging.ListLogEntriesResponse, error)
}

// auditAPI implements cloud.AuditAPI.
type auditAPI struct {
	project  string
	location string
	cluster  string
	entries  auditEntryLister
}

func newAuditAPI(p *Provider) *auditAPI {
	svc := lazyClient(func(ctx context.Context) (*logging.Service, error) {
		return logging.NewService(ctx)
	})
	return &auditAPI{
		project:  p.project,
		location: p.location,
		cluster:  p.cluster,
		entries:  &loggingEntryClient{svc: svc},
	}
}

// auditResourceName renders the protoPayload.resourceName of a
// Kubernetes audit entry: "<group>/<version>/namespaces/<ns>/
// <resource>/<name>", with the core group spelled "core". Namespaced
// objects only — GKE renders cluster-scoped objects WITHOUT the
// namespaces segment, a shape no current consumer needs; extend here
// (empty Namespace → drop the segment) when one does.
func auditResourceName(ref cloud.AuditRef) string {
	group := ref.APIGroup
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("%s/%s/namespaces/%s/%s/%s",
		group, ref.Version, ref.Namespace, ref.Resource, ref.Name)
}

// auditFilter is the Cloud Logging filter for object ref in window w:
// admin-activity entries on THIS cluster's k8s_cluster resource whose
// resourceName is exactly the object's request path.
func auditFilter(project, location, cluster string, ref cloud.AuditRef, w cloud.TimeWindow) string {
	return fmt.Sprintf(
		`logName="projects/%s/logs/cloudaudit.googleapis.com%%2Factivity"`+"\n"+
			`resource.type="k8s_cluster"`+"\n"+
			`resource.labels.location=%q`+"\n"+
			`resource.labels.cluster_name=%q`+"\n"+
			`protoPayload.resourceName=%q`+"\n"+
			`timestamp>=%q AND timestamp<%q`,
		project, location, cluster, auditResourceName(ref),
		w.Start.UTC().Format(time.RFC3339Nano), w.End.UTC().Format(time.RFC3339Nano))
}

// k8sAuditPayload is the slice of the audit-log proto payload the
// extraction reads (the payload arrives as JSON in the REST response).
type k8sAuditPayload struct {
	MethodName         string `json:"methodName"`
	AuthenticationInfo struct {
		PrincipalEmail string `json:"principalEmail"`
	} `json:"authenticationInfo"`
	RequestMetadata struct {
		CallerSuppliedUserAgent string `json:"callerSuppliedUserAgent"`
	} `json:"requestMetadata"`
}

// ObjectWrites implements cloud.AuditAPI.
func (a *auditAPI) ObjectWrites(ctx context.Context, ref cloud.AuditRef, w cloud.TimeWindow) ([]cloud.ObjectWrite, error) {
	req := &logging.ListLogEntriesRequest{
		ResourceNames: []string{"projects/" + a.project},
		Filter:        auditFilter(a.project, a.location, a.cluster, ref, w),
		OrderBy:       "timestamp desc",
		PageSize:      1000,
	}
	var out []cloud.ObjectWrite
	for page := 0; page < auditMaxPages; page++ {
		resp, err := a.entries.ListEntries(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("listing object write audit entries: %w", err)
		}
		for _, e := range resp.Entries {
			rec, ok := parseAuditEntry(e)
			if ok {
				out = append(out, rec)
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		req.PageToken = resp.NextPageToken
	}
	return out, nil
}

// parseAuditEntry extracts one write record; entries the filter let
// through but that lack a principal (or a parsable payload) are
// dropped — the filter is the contract, this is the belt.
func parseAuditEntry(e *logging.LogEntry) (cloud.ObjectWrite, bool) {
	var zero cloud.ObjectWrite
	if e == nil || len(e.ProtoPayload) == 0 {
		return zero, false
	}
	var p k8sAuditPayload
	if err := json.Unmarshal(e.ProtoPayload, &p); err != nil {
		return zero, false
	}
	if p.AuthenticationInfo.PrincipalEmail == "" {
		return zero, false
	}
	t, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil {
		return zero, false
	}
	return cloud.ObjectWrite{
		Time:      t,
		Principal: p.AuthenticationInfo.PrincipalEmail,
		Method:    p.MethodName,
		UserAgent: p.RequestMetadata.CallerSuppliedUserAgent,
	}, true
}
