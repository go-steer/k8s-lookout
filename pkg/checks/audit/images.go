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

package audit

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// kindImageFloatingTag: a container image reference that does not name
// specific bytes. Every restart, rescale, node replacement and
// preemption re-resolves the tag, so the pod that comes back can run
// different code from the one that went away — with no deploy, no
// manifest change, and nothing in the cluster recording it. A rollback
// resolves the same reference, so rolling back does not go back either.
const kindImageFloatingTag = "audit.image_floating_tag"

const (
	reasonNoTag       = "NoTag"
	reasonFloatingTag = "FloatingTag"
)

// floatingTags are the tag names that by convention move. A tag outside
// this list without a digest is still mutable in principle, but
// flagging `v1.2.3` would flag every fleet; the claim stays with the
// names whose whole purpose is to move. (A policy knob of the same name
// once pkg/policy exists.)
//
// Severity is info, not warning: an unpinned reference is everywhere in
// every fleet, and grading all of it warning would bury the findings
// that describe a workload already broken. The escalation — live pods
// of one workload running different digests under one reference — is a
// claim about pods, not the spec, and belongs to an incident-side kind.
var floatingTags = map[string]bool{
	"latest": true, "main": true, "master": true, "dev": true,
	"nightly": true, "stable": true, "edge": true,
}

// imageTagFindingsWithTags judges every container in the template, init
// containers included (an init container that pulls `:latest` changes
// what the pod boots into just as surely).
func imageTagFindingsWithTags(spec corev1.PodSpec, tags map[string]bool) []emit.Finding {
	if len(tags) == 0 {
		tags = floatingTags
	}
	var untagged, floating []string
	for _, c := range allContainers(spec) {
		switch classifyImageRefWithTags(c.Image, tags) {
		case reasonNoTag:
			untagged = append(untagged, c.Name)
		case reasonFloatingTag:
			floating = append(floating, c.Name)
		}
	}
	total := len(spec.InitContainers) + len(spec.Containers)
	var out []emit.Finding
	if len(floating) > 0 {
		out = append(out, emit.Finding{
			Kind:     kindImageFloatingTag,
			Severity: emit.SeverityInfo,
			Reason:   reasonFloatingTag,
			Message: fmt.Sprintf("%d of %d containers run an image on a tag that moves (%s) with no digest: what runs after the next pull is whatever the registry serves under that tag then, and a rollback resolves the same reference",
				len(floating), total, strings.Join(floating, ", ")),
			Details: []emit.Field{
				{Key: "containers", Value: itoa(len(floating))},
				{Key: "container_names", Value: cappedList(floating)},
				{Key: "total_containers", Value: itoa(total)},
			},
		})
	}
	if len(untagged) > 0 {
		out = append(out, emit.Finding{
			Kind:     kindImageFloatingTag,
			Severity: emit.SeverityInfo,
			Reason:   reasonNoTag,
			Message: fmt.Sprintf("%d of %d containers run an image with no tag at all (%s), which the runtime resolves to :latest: the reference names no particular bytes",
				len(untagged), total, strings.Join(untagged, ", ")),
			Details: []emit.Field{
				{Key: "containers", Value: itoa(len(untagged))},
				{Key: "container_names", Value: cappedList(untagged)},
				{Key: "total_containers", Value: itoa(total)},
			},
		})
	}
	return out
}

// classifyImageRefWithTags returns the reason an image reference is floating,
// or "" when it is pinned. A digest pins regardless of tag. The tag is
// the part after the last '/', because a registry host may carry a
// port (`registry:5000/app`).
func classifyImageRefWithTags(image string, tags map[string]bool) string {
	if image == "" || strings.Contains(image, "@sha256:") {
		return ""
	}
	last := image[strings.LastIndex(image, "/")+1:]
	i := strings.LastIndex(last, ":")
	if i < 0 {
		return reasonNoTag
	}
	if tags[last[i+1:]] {
		return reasonFloatingTag
	}
	return ""
}
