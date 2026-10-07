# In-pod network probe (`net probe-from`): decided

Issue #539. Decided 2026-10-07: build it as a separate, opt-in command
with its own RBAC. The default deployment stays strictly read-only.
This note is the design that DESIGN.md §5's amendment points to.

## Why

`lookout net probe` sends its packets from wherever lookout runs. For
most faults that is enough. For two it is the wrong place to stand:

- a one-way partition from a caller to its callee, and
- a DNS failure that only the caller sees.

From anywhere except the caller, the callee really is healthy, so a
probe from the lookout pod is evidence *against* a network fault. The
issue's evidence table shows this, and an agent run against the
partition drew exactly that wrong conclusion from a correct reading of
what it could see. The Dataplane V2 flow-log check (issue comments,
2026-10-02) covers the partition only on clusters that opt in to flow
observability, and does not cover the DNS case at all.

The only way to see these faults is to send the probe from inside the
caller pod's network namespace. That needs a write to the cluster,
which every other lookout command avoids. So it gets its own command,
its own grant, and its own safety rails.

## Shape

```
lookout net probe-from --pod=<namespace>/<name> \
    --image=<repo>@sha256:<digest> \
    [--dns=<names>] [--tcp=<host:port,...>] [--http=<urls>] \
    [--probe-timeout=5s] [--timeout=60s] [--format=logfmt|json]
```

MCP tool name: `k8s_net_probe_from`, **not served by default** (see
"MCP exposure").

## Mechanism: an ephemeral container, not exec

| | Ephemeral container (`pods/ephemeralcontainers`) | Exec (`pods/exec`) |
| --- | --- | --- |
| Depends on the target image | No. Brings its own image and filesystem. | Yes. Needs a shell or `tar` and a writable path to copy a binary in. Distroless and scratch images have neither; `readOnlyRootFilesystem` (Online Boutique sets it on every service) leaves nowhere to put it. |
| Runs code in the workload's own container | No. A separate container that shares only the pod's network namespace. | Yes. Our binary runs inside the application container, with its filesystem, mounts and environment. |
| Leaves a trace | Yes, permanently. The container stays in `pod.spec.ephemeralContainers` (with a terminated status) until the pod is deleted. | Nothing in the pod spec. Only the API server audit log. |
| Grant | `patch` on `pods/ephemeralcontainers` | `create` on `pods/exec` |
| Policy | Pod Security applies to the new container. A policy can inspect exactly what is being added (image, mounts, command). | Commonly denied outright. A policy sees only "exec into pod X" and the command line; it cannot constrain what the copied binary does. |

**Chosen: the ephemeral container.** Exec fails on most real targets
(distroless, read-only root filesystems), and when it works it runs our
code inside the workload's container. The ephemeral container works on
any running pod, keeps our code out of the workload's container, and is
something an admission policy can check field by field.

**Its cost, stated plainly:** every probe permanently adds an entry to
the pod's spec. It cannot be removed; it goes away when the pod is
replaced. Anyone who can read the pod can see it, including an agent
being evaluated on that cluster. The design limits this (below) but
cannot avoid it. Adding an ephemeral container does not restart the
pod or touch its other containers.

## Image

- `--image` is **required** and must be a digest reference,
  `<repo>@sha256:<64 hex>`. A tag (`:v0.33.0`, `:latest`) is a usage
  error. A digest pins exactly which bytes run inside someone else's
  pod, and it is what the admission policy checks.
- The image is a lookout release image (any flavor). The probe
  container runs `/lookout net probe ...`, the same code as the local
  command. Use the digest of the release you run, e.g.
  `crane digest ghcr.io/go-steer/lookout:<version>`.
- There is no built-in default. The binary cannot know its own image
  digest (the digest exists only after the image is built), and a
  silently chosen image is the wrong default for something that runs
  in another team's pod.
- Over MCP the image is **not** a tool argument. The operator fixes it
  when starting the server (`lookout mcp --probe-from-image=...`), so a
  model cannot choose what runs in the pod.

## What it probes, and output parity

The same three checks as `net probe`, with the same target syntax
(`--dns`, `--tcp`, `--http`, `--probe-timeout`) and the same finding
kinds and fields (`probe.dns`, `probe.tcp`, `probe.http`; `ips`,
`latency`, `status`, `content_length`, `error_class`). Parity holds by
construction: the probe container runs `lookout net probe
--format=json`, and `probe-from` reads its log and re-emits each record
through its own writer and sanitizer.

Every finding from either command gains a `vantage` field:

- `net probe`: `vantage=local` (wherever this lookout process runs).
  This is an additive field on an existing command.
- `net probe-from`: `vantage=pod:<namespace>/<name>`, plus
  `probe_container=<name>`, so a result can be traced to the record it
  left in the pod.

So the two outputs can be put side by side and differ only where the
network differs, and neither can be mistaken for the other.

Two target rules are stricter than `net probe`'s, because the targets
are written into the pod spec for the pod's lifetime: HTTP targets with
credentials (`https://user:pass@...`), a query string or a fragment are
usage errors.

## Authorization

- **A separate command, never composed.** `scan`, `health`, `bundle`
  and the sentinel never invoke it. `scan` lists it in its exclusion
  table, and a guard test fails if any composition or optional scan
  group would run a command marked privileged.
- **Marked as a write.** `Writes: true`, so wherever it is served over
  MCP it carries `ReadOnlyHint: false`. A new `Privileged` marker on
  the command keeps it off the default MCP surface entirely.
- **Refused without the grant.** Before changing anything the command
  asks the API server (a SelfSubjectAccessReview) whether this identity
  may `patch pods/ephemeralcontainers` and `get pods/log` on the named
  pod. If not, or if the patch itself is refused, nothing is changed
  and the answer is one `probe.refused` finding worded by the shared
  `checks.Refusal` (from #546), for example:

  ```
  kind=probe.refused severity=info namespace=shop name=frontend-7d9 reason=Forbidden
  message="forbidden: patch pods/ephemeralcontainers — namespaced, not granted by the built-in view role; grant patch on pods/ephemeralcontainers (core) via a ClusterRole or Role (deploy-probe/, or Helm rbac.probeFrom=true) — no probe ran and the pod was not changed"
  vantage=pod:shop/frontend-7d9
  ```

- **Exit code for a refusal: 0.** A missing grant is an answer about
  this identity, not a broken tool. This is the rule #546 set for every
  other command (a refused read is an explicit unavailable record, exit
  0), and the exact-`view` guard test holds this command to it too.
  Over MCP an exit 0 is ordinary content the model reads; an exit 1
  would arrive as a tool error, which agents tend to retry or report as
  lookout being broken. The answer cannot be confused with a result:
  it has no `probe.dns|tcp|http` records, its summary says `scanned=0`,
  and it never falls back to probing from the local vantage.
- The same `probe.refused` kind (with its own `reason`) covers the
  other "nothing was changed" outcomes: the pod is not Running, uses
  `hostNetwork`, already holds the per-pod limit of lookout probe
  containers, or an admission policy rejected the container
  (`reason=AdmissionDenied`, with the server's message).
- **Exit 1** is reserved for failures after the container was added
  (image pull failure, timeout, unreadable log). The diagnostic names
  the container and says it remains in the pod spec.

## RBAC: an optional overlay, never the default

The default `deploy/` and the chart's default values are unchanged:
no write verb anywhere.

`deploy-probe/` is a kustomize overlay on `deploy/` (used *instead of*
it, like `deploy-no-secrets/`). It adds:

1. ClusterRole `lookout-probe-from`: `patch` on `pods/ephemeralcontainers`.
   `get pods` and `get pods/log` are already in the base ClusterRole.
2. ClusterRoleBinding of that role to the `lookout-watch`
   ServiceAccount. To limit it to some namespaces, bind the same
   ClusterRole with RoleBindings instead; the doc shows how.
3. A **ValidatingAdmissionPolicy** (`admissionregistration.k8s.io/v1`,
   Kubernetes 1.30+) and its binding, matched to that ServiceAccount,
   that rejects any ephemeral container it adds unless:
   - its name starts with `lookout-probe-`;
   - its image is `ghcr.io/go-steer/lookout@sha256:...` (digest form,
     the official repository);
   - its command is exactly `/lookout net probe` and every argument is
     one of the probe flags;
   - it has no `targetContainerName` (no access to other containers'
     processes), no volume mounts, no `envFrom`, no stdin or TTY;
   - its security context is non-root, drops all capabilities, forbids
     privilege escalation and is not privileged.

   The policy is what turns "may patch ephemeral containers" (which on
   its own would allow running any image with any of the pod's volumes
   mounted) into "may add one specific, inert probe container".

Helm: `rbac.probeFrom: false` by default. `true` renders the same three
objects, and `dev/tools/verify-helm-parity` gains a third exact diff:
`helm template --set rbac.probeFrom=true` == `kustomize build deploy-probe/`.

A user running lookout under their own kubeconfig needs the same
`patch` grant from their cluster admin; the policy above covers only
the ServiceAccount the overlay binds, so the docs recommend copying it
for any other identity given the grant.

## MCP exposure

**Not served by default.** `lookout mcp` with no flags does not list
the tool, and neither `--tools=all` nor any profile adds it. It is
served only when the server is started with
`--probe-from-image=<repo>@sha256:<digest>`, which both opts in and
fixes the image (the tool's schema has no image argument).

Reasons:

- A tool that changes a pod is a different risk class from the
  read-only surface. Turning it on should be an operator decision made
  where the server is configured, not something a client can ask for.
- k8s-sre-agent's read-only guard (`internal/readonly`,
  `lookoutWriters`) starts `lookout mcp` with no flags and refuses to
  run if lookout advertises a write tool it has not classified. Keeping
  the tool off the default surface means shipping this does not stop
  that agent. An eval that wants to score "with caller probes" starts
  the server with the flag and classifies the tool on its side.
- When served it is marked `ReadOnlyHint: false`, so a convention-
  following client asks before calling it.

## Safety rails

- **Explicit target only.** `--pod=<namespace>/<name>` is required. No
  selectors, no `--namespace`, no `-A`, no `--workload`; all are usage
  errors. One invocation touches one pod.
- **No process access.** The container has no `targetContainerName`,
  so it shares the pod's network namespace only, not any container's
  process namespace.
- **Restricted security context**, valid under Pod Security
  `restricted`: `runAsNonRoot`, `runAsUser/runAsGroup: 65532` (the
  image's user is the non-numeric `nonroot`, which the kubelet cannot
  verify without a number), `allowPrivilegeEscalation: false`,
  `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault`,
  `readOnlyRootFilesystem: true`. No volume mounts.
- **Refuses** `hostNetwork` pods (the vantage would be the node, and
  such pods are usually privileged system components) and pods that
  are not Running.
- **Per-pod limit.** If the pod already holds 10 `lookout-probe-*`
  containers, the command refuses rather than grow the spec further.
  Replacing the pod clears them.
- **Arguments, not a shell.** Targets reach the container as separate
  argv entries (`--dns=a,b`), never through a shell, and are validated
  with `net probe`'s own parsers before anything is sent.
- **Bounded in time.** The probe container is given `--timeout` equal
  to the remaining budget of the outer command, so it exits on its own
  even if the caller disappears. `--timeout` defaults to 60s (image
  pull plus probes).
- **Sanitized output, no response bodies.** Re-emitted records pass the
  `pkg/emit` sanitizer like every other output. The probe never reads a
  response body (inherited from `net probe`). Records the probe
  container emits that are not `probe.*` kinds, or that carry fields
  outside the declared glossary, are dropped, not forwarded.
- **Audit trail.** Ephemeral containers cannot carry labels or
  annotations of their own, and annotating the pod would need a second
  write grant. So the record is the container itself: the
  `lookout-probe-<id>` name, the pinned image, the targets in its
  arguments, and environment entries `LOOKOUT_PROBE_REQUESTED_BY` (the
  caller's username from a SelfSubjectReview, or `unknown`),
  `LOOKOUT_PROBE_REQUESTED_AT` (RFC 3339) and `LOOKOUT_PROBE_CLIENT`
  (`lookout/<version>`). The API server audit log records the patch
  with the authenticated identity.

## Security review (design stage)

| Concern | Assessment |
| --- | --- |
| Privilege of the grant | `patch pods/ephemeralcontainers` alone is close to exec: it could run any image with the pod's volumes (and Secrets) mounted. **Mitigated** by the admission policy shipped with the grant, which pins the image repository, the command, and forbids mounts and process sharing. Without the policy (clusters older than 1.30, or a grant made by hand) the grant is as strong as exec, and the docs say so. |
| Injection through targets | Targets are validated by `net probe`'s parsers and passed as argv, never through a shell; the comma-separated list is one argument per flag, so a target cannot add a flag. The policy also rejects any argument that is not a probe flag. |
| Image trust | Digest-pinned; tags refused. Over MCP the image is fixed by the operator, not the caller. The policy limits the overlay's identity to the official repository. A mirror needs the policy edited to match. |
| Secrets | The container mounts nothing, and the probe never reads response bodies. Targets with credentials or query strings are refused because they would be stored in the pod spec. Output passes the sanitizer. |
| Lasting change | Inherent to the mechanism and accepted in the issue. Bounded by the per-pod limit; visible and attributable. |
| Network position | The probe can reach whatever the pod can, including endpoints a NetworkPolicy opens only to that pod. That is the point of the feature. It sends only DNS lookups, TCP connects and body-less `GET`s; a `GET` with side effects is the remaining exposure, same as `net probe`. |
| Accidental use | Off the default deploy, off the default MCP surface, never composed by scan/health/bundle/sentinel, and marked as a write where served. |

No risk found that needs a decision beyond the one already made; the
residual risks (the permanent record, and the grant's strength on
clusters that cannot run the policy) are documented for operators.

## Out of scope

- Any other mutation: exec, attach, port-forward, debug pods or node
  debugging, copying files, removing or editing ephemeral containers,
  annotating or labelling the pod.
- Spawning pods. The command adds a container to an existing pod only.
- Probing from a node (`kubectl debug node/...`).
- Running it from `scan`, `health`, `bundle`, enrichment or the
  sentinel.
- Changing `net probe`'s behaviour, beyond the additive `vantage`
  field.

## Testing

Hermetic, per DESIGN §13 (`fake.Clientset`): the patch body (name,
image, argv, security context, env audit fields, no mounts, no
target container); refusal from the SSAR and from a Forbidden patch,
both exit 0 with the `checks.Refusal` wording; admission denial;
the non-Running, `hostNetwork` and per-pod-limit refusals; output
parity with `net probe` (same kinds and fields, `vantage` differs);
the timeout path; sanitization of re-emitted records and dropping of
undeclared ones; the exact-`view` guard; MCP not advertising the tool
by default and advertising it, without an image argument, under
`--probe-from-image`; and a helm/kustomize parity diff for the overlay.
