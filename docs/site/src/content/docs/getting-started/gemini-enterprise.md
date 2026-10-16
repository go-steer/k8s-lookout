---
title: Gemini Enterprise
description: Serve lookout to a Gemini Enterprise app as a Custom MCP Server, with every tool call reading as the person who made it.
sidebar:
  order: 8
---

A Gemini Enterprise app can call lookout's tools through a **Custom MCP
Server** data store. In this setup every call reads the cluster and Google
Cloud **as the person who asked**, with their own Google access token:

- What a person sees is what their own Kubernetes RBAC and Cloud IAM allow.
  On GKE, IAM roles grant Kubernetes access too.
- lookout's own identity reads nothing. Its ServiceAccount has no RBAC at
  all.
- Each tool call in the access log names the person who made it.

How it works is on the [MCP setup](/getting-started/mcp/#each-caller-reads-as-themselves-google-token-mode)
page. This page is the walk-through, about an hour the first time, most of
it waiting for a certificate.

## What you need

- A **GKE** cluster. Callers' tokens are only ever sent to an `https` API
  server with verified TLS, which in-cluster lookout satisfies.
- A **Gemini Enterprise** app in the same Google Workspace organization,
  and permission to add data stores to it.
- A **hostname** you control (`lookout.example.com` below), for the
  load balancer's managed certificate.
- `gcloud` and `kubectl` pointed at the project and cluster.

```bash
export PROJECT=your-project
gcloud config set project "$PROJECT"
```

## 1. An OAuth client

In the console, **Google Auth Platform**:

1. The consent screen's audience must be **Internal**, so `cloud-platform`
   needs no app verification. An Internal app needs no scopes listed under
   Data access: the data store requests them.
2. **Clients → Create client**, type **Web application**, name `lookout`.
   Mark it as used by an AI-powered agent if the console asks.
3. **Authorized redirect URI:**
   `https://vertexaisearch.cloud.google.com/oauth-redirect`
4. **Create**, and note the client ID and secret. The secret only goes
   into the data store in step 5.

If your organization restricts Google Cloud scopes, sign-in will fail
with "Access blocked: your institution's admin needs to review" until a
Workspace admin marks this client **Trusted**.

## 2. A static IP and DNS

```bash
gcloud compute addresses create lookout-mcp-ip --global
gcloud compute addresses describe lookout-mcp-ip --global --format='value(address)'
```

Point an `A` record for your hostname at that address. The managed
certificate is only issued once the name resolves to it.

## 3. Deploy

Fetch the overlay, edit `config.yaml`, and apply it:

```bash
git clone --depth 1 --branch vX.Y.Z https://github.com/go-steer/k8s-lookout
cd k8s-lookout/deploy-mcp-gemini
$EDITOR config.yaml        # DOMAIN, GOOGLE_CLIENT_ID, GOOGLE_ALLOW, GOOGLE_ALLOW_WRITE
kubectl apply -k .
```

Or keep the overlay remote and patch `config.yaml` from your own
kustomization. Either way it creates, in `agent-triage`:

- a `lookout-mcp` ServiceAccount, with no RBAC and no Workload Identity;
- two stateless `lookout mcp` replicas in Google-token mode, serving the
  `triage` and `audit` tool profiles plus six incident reads (cluster
  health, recent changes, event timeline, resource spec, blast radius,
  resource top) — minus `findings diff`, which needs a store — with
  the access log on stdout;
- a GKE external HTTPS load balancer (Ingress, `ManagedCertificate`,
  `BackendConfig`) on `lookout-mcp-ip`, HTTPS only.

The certificate takes 15 to 60 minutes after DNS resolves. Watch it:

```bash
kubectl -n agent-triage get managedcertificate lookout-mcp -w   # wait for Active
curl -s https://lookout.example.com/.well-known/oauth-protected-resource/mcp
```

The metadata names `https://accounts.google.com` as the authorization
server. `curl -X POST https://lookout.example.com/mcp` gets a `401`: no
token.

## 4. Give people access

lookout reads as each person, so each person needs read access to what
they will ask about, granted as usual:

- **Kubernetes:** a `view` ClusterRole binding (or a namespaced Role) for
  their Google account or group, or an IAM role such as
  `roles/container.viewer`.
- **Cloud APIs** used by the cloud checks (Compute, Monitoring, Logging):
  their IAM roles in the project.

Someone with access to only some namespaces sees only those. Rows lookout
reads from its own files (triage records, history) are filtered the same
way, and the summary line counts the rest as `hidden=<n>`.

## 5. The Custom MCP Server data store

1. Gemini Enterprise → your app → **Data stores → Create data store →
   Custom MCP Server → Add MCP server**. Choose **OAuth 2.0**:

   | Field | Value |
   |---|---|
   | MCP Server URL | `https://lookout.example.com/mcp` |
   | Authorization URL | `https://accounts.google.com/o/oauth2/v2/auth` |
   | Authorization URL Parameters | `&access_type=offline&prompt=consent` |
   | Token URL | `https://oauth2.googleapis.com/token` |
   | Client ID / Client Secret | from step 1 |
   | Enable PKCE Support | on |
   | Scopes | `openid email https://www.googleapis.com/auth/cloud-platform` |

2. **Verify Auth**, and sign in with an account `GOOGLE_ALLOW` admits.
3. Location **global**, connector name `lookout`, and a description. The
   assistant uses the description to decide when to call lookout, so say
   what it is for:
   > lookout: read-only troubleshooting for our Kubernetes (GKE) cluster.
   > Use it for anything about workloads, pods, deployments, crash loops,
   > pending pods, recent changes, logs and cluster health. It reads as the
   > signed-in user, so answers reflect what they are allowed to see.
4. **Create** and wait for **Active**.
5. **Connect the data store to your app** first: until it is connected,
   **Reload custom actions** fails with "The service is currently
   unavailable" without contacting lookout.
6. **Actions → Reload custom actions**, then enable the connector in the
   chat.

## 6. Try it

Ask, for example, *"Using lookout, what is broken in the cluster right
now?"* The access log has one line per call with the caller's email:

```bash
kubectl -n agent-triage logs deploy/lookout-mcp | grep tool=
```

Gemini Enterprise refreshes people's tokens on its own; nobody signs in
again mid-conversation.

## Changing who can do what

- **Who may call lookout:** `GOOGLE_ALLOW` in `config.yaml`, then
  `kubectl apply -k .` and `kubectl -n agent-triage rollout restart
  deploy/lookout-mcp`.
- **Who may change state** (triage-status records, findings acks):
  `GOOGLE_ALLOW_WRITE`, the same way. Empty means nobody.
- **Which tools are offered:** the `--profile`/`--tools` arguments in
  `mcp.yaml` (see [MCP setup](/getting-started/mcp/)). Fewer tools means a
  cheaper, more accurate assistant.

## Not covered by this overlay

The sentinel store. Store-backed tools (`triage status`, history via
`--at`) need `--store` on a file the MCP pod can read. With the
sentinel's store on a ReadWriteOnce volume, that means running
`lookout mcp` as a second container in the sentinel's pod.
