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
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const (
	kindPrivileged    = "audit.privileged_container"
	kindHostNamespace = "audit.host_namespace"
	kindHostPath      = "audit.hostpath_mount"
)

const (
	reasonPrivileged          = "PrivilegedContainer"
	reasonDangerousCapability = "DangerousCapability"
	reasonHostNetwork         = "HostNetwork"
	reasonHostPID             = "HostPID"
	reasonHostIPC             = "HostIPC"
	reasonWritableHostPath    = "WritableHostPath"
	reasonReadOnlyHostPath    = "ReadOnlyHostPath"
)

// allContainers returns init containers followed by regular ones. Both
// run with the securityContext they declare, and an init container
// holds whatever it asks for until it exits.
func allContainers(spec corev1.PodSpec) []corev1.Container {
	out := make([]corev1.Container, 0, len(spec.InitContainers)+len(spec.Containers))
	out = append(out, spec.InitContainers...)
	return append(out, spec.Containers...)
}

// nodeRootCapabilities are the two additions equivalent to
// privileged: true for this check's purposes. SYS_ADMIN is the
// capability the kernel uses as a catch-all for "can reconfigure the
// machine"; ALL is every capability including it.
var nodeRootCapabilities = map[string]bool{"ALL": true, "SYS_ADMIN": true}

// privilegeFindings judges the two ways a container asks for the node.
// They are separate claims because the remedies differ: one is a
// boolean to unset, the other a capability to drop.
func privilegeFindings(spec corev1.PodSpec) []emit.Finding {
	cs := allContainers(spec)
	if len(cs) == 0 {
		return nil
	}
	var privileged, capable []string
	caps := map[string]bool{}
	for _, c := range cs {
		sc := c.SecurityContext
		if sc == nil {
			continue
		}
		if sc.Privileged != nil && *sc.Privileged {
			privileged = append(privileged, c.Name)
		}
		if sc.Capabilities == nil {
			continue
		}
		var found []string
		for _, add := range sc.Capabilities.Add {
			if name := canonicalCapability(string(add)); nodeRootCapabilities[name] {
				found = append(found, name)
			}
		}
		if len(found) > 0 {
			capable = append(capable, c.Name)
			for _, name := range found {
				caps[name] = true
			}
		}
	}

	var out []emit.Finding
	if len(privileged) > 0 {
		out = append(out, emit.Finding{
			Kind:     kindPrivileged,
			Severity: emit.SeverityWarning,
			Reason:   reasonPrivileged,
			Message: fmt.Sprintf("securityContext.privileged on %d of %d %s: every device on the node is visible and writable, and a process that escapes the container is root on the host",
				len(privileged), len(cs), plural(len(cs), "container")),
			Details: []emit.Field{
				{Key: "containers", Value: itoa(len(privileged))},
				{Key: "container_names", Value: cappedList(privileged)},
				{Key: "total_containers", Value: itoa(len(cs))},
			},
		})
	}
	if len(capable) > 0 {
		out = append(out, emit.Finding{
			Kind:     kindPrivileged,
			Severity: emit.SeverityWarning,
			Reason:   reasonDangerousCapability,
			Message: fmt.Sprintf("capabilities.add %s on %d of %d %s: not privileged: true, and equivalent to it — a check reading only that flag reports this template as clean",
				sortedKeys(caps), len(capable), len(cs), plural(len(cs), "container")),
			Details: []emit.Field{
				{Key: "containers", Value: itoa(len(capable))},
				{Key: "container_names", Value: cappedList(capable)},
				{Key: "total_containers", Value: itoa(len(cs))},
				{Key: "capabilities", Value: sortedKeys(caps)},
			},
		})
	}
	return out
}

// canonicalCapability normalizes a capability name to the bare
// uppercase form the API uses ("SYS_ADMIN"), tolerating the CAP_
// prefix people write out of kernel-header habit.
func canonicalCapability(name string) string {
	return strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(name)), "CAP_")
}

func sortedKeys(set map[string]bool) string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// hostNamespaceFindings judges the three host namespaces a pod can
// join. One finding each, not one finding listing all three: they are
// asked for separately, granted separately and removed separately, and
// a rollup counting "how many workloads see every process on the node"
// should not have to parse a list to answer.
func hostNamespaceFindings(spec corev1.PodSpec) []emit.Finding {
	var out []emit.Finding
	if spec.HostNetwork {
		out = append(out, emit.Finding{
			Kind:     kindHostNamespace,
			Severity: emit.SeverityWarning,
			Reason:   reasonHostNetwork,
			Message:  "hostNetwork: the pod binds ports directly on the node, reaches anything the node can reach including the metadata server and the kubelet, and NetworkPolicy — which selects pods, not host interfaces — cannot constrain it",
		})
	}
	if spec.HostPID {
		out = append(out, emit.Finding{
			Kind:     kindHostNamespace,
			Severity: emit.SeverityWarning,
			Reason:   reasonHostPID,
			Message:  "hostPID: every process on the node is visible in /proc, including other containers' command lines and anything they were passed as an argument",
		})
	}
	if spec.HostIPC {
		out = append(out, emit.Finding{
			Kind:     kindHostNamespace,
			Severity: emit.SeverityWarning,
			Reason:   reasonHostIPC,
			Message:  "hostIPC: the pod shares the node's SysV IPC and POSIX message queues with every other process on it",
		})
	}
	return out
}

// hostPathFindings judges the template's hostPath volumes, split by
// whether anything can write through them.
//
// Only MOUNTED volumes are judged: a hostPath volume declared and
// never mounted grants no access to anything, and reporting it would
// be true and useless. A volume counts as writable when any one of its
// mounts is, since one writable mount is all it takes.
func hostPathFindings(spec corev1.PodSpec) []emit.Finding {
	hostPaths := map[string]string{} // volume name -> path on the node
	for _, v := range spec.Volumes {
		if v.HostPath != nil {
			hostPaths[v.Name] = v.HostPath.Path
		}
	}
	if len(hostPaths) == 0 {
		return nil
	}
	writable, readOnly := map[string]bool{}, map[string]bool{}
	for _, c := range allContainers(spec) {
		for _, m := range c.VolumeMounts {
			path, ok := hostPaths[m.Name]
			if !ok {
				continue
			}
			if m.ReadOnly {
				readOnly[path] = true
			} else {
				writable[path] = true
			}
		}
	}
	// A path mounted read-only somewhere and writable elsewhere is
	// writable; the weaker mount does not constrain the stronger one.
	for path := range writable {
		delete(readOnly, path)
	}

	var out []emit.Finding
	if len(writable) > 0 {
		out = append(out, hostPathFinding(emit.SeverityWarning, reasonWritableHostPath,
			"mounted read-write: anything the container writes lands on the node's filesystem, and a path like /var/run, /etc or / is a container escape rather than a mount",
			writable))
	}
	if len(readOnly) > 0 {
		// Info, not warning: a read-only hostPath still reads node state
		// the container was never given, but it cannot modify the node,
		// and the read-only forms in practice (a CA bundle, /etc/localtime)
		// are ordinary. The absence is worth reporting and is not, on its
		// own, a defect.
		out = append(out, hostPathFinding(emit.SeverityInfo, reasonReadOnlyHostPath,
			"mounted read-only: the node's filesystem is readable from inside the container, though not writable",
			readOnly))
	}
	return out
}

func hostPathFinding(severity, reason, consequence string, paths map[string]bool) emit.Finding {
	names := make([]string, 0, len(paths))
	for p := range paths {
		names = append(names, p)
	}
	sort.Strings(names)
	return emit.Finding{
		Kind:     kindHostPath,
		Severity: severity,
		Reason:   reason,
		Message:  fmt.Sprintf("%d hostPath %s %s", len(names), plural(len(names), "volume"), consequence),
		Details: []emit.Field{
			{Key: "host_paths", Value: itoa(len(names))},
			{Key: "host_path_names", Value: cappedList(names)},
		},
	}
}
