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
page. This page is the walk-through, under an hour the first time.

## What you need

- A **GKE** cluster. Callers' tokens are only ever sent to an `https` API
  server with verified TLS, which in-cluster lookout satisfies.
- A **Gemini Enterprise** app in the same Google Workspace organization,
  and permission to add data stores to it.
- A **hostname** you control (`lookout.example.com` below), and a
  certificate for it in Certificate Manager (step 2 creates one).
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

## 2. A static IP, DNS and a certificate

lookout serves through a GKE Gateway (a global external Application
Load Balancer) whose TLS comes from a Certificate Manager map.

```bash
gcloud compute addresses create lookout-mcp-ip --global
gcloud compute addresses describe lookout-mcp-ip --global --format='value(address)'
```

Point an `A` record for your hostname at that address.

If you already have a certificate map covering the hostname (a wildcard
for the domain, say), use it and skip the rest of this step. Otherwise
create a Google-managed certificate, authorized through DNS, and a map
holding it:

```bash
gcloud certificate-manager dns-authorizations create lookout-mcp --domain=lookout.example.com
gcloud certificate-manager dns-authorizations describe lookout-mcp \
  --format='value(dnsResourceRecord.name,dnsResourceRecord.type,dnsResourceRecord.data)'
# add that CNAME record to your DNS, then:
gcloud certificate-manager certificates create lookout-mcp \
  --domains=lookout.example.com --dns-authorizations=lookout-mcp
gcloud certificate-manager maps create lookout-mcp
gcloud certificate-manager maps entries create lookout-mcp \
  --map=lookout-mcp --certificates=lookout-mcp --hostname=lookout.example.com
```

The certificate is issued once the CNAME resolves, usually within
minutes; `gcloud certificate-manager certificates describe lookout-mcp`
shows `ACTIVE`.

## 3. Deploy

Fetch the overlay, edit `config.yaml`, and apply it:

```bash
git clone --depth 1 --branch vX.Y.Z https://github.com/go-steer/k8s-lookout
cd k8s-lookout/deploy-mcp-gemini
$EDITOR config.yaml        # DOMAIN, GOOGLE_CLIENT_ID, GOOGLE_ALLOW, GOOGLE_ALLOW_WRITE
kubectl apply -k .
```

Two names in `gateway.yaml` are yours too, if they differ from the
defaults above: the static IP (`NamedAddress`, `lookout-mcp-ip`) and the
certificate map (the `networking.gke.io/certmap` annotation,
`lookout-mcp`). The cluster needs the Gateway API (Autopilot has it;
on Standard, `--gateway-api=standard`).

Or keep the overlay remote and set everything from your own
kustomization. Set the `config.yaml` values with a `configMapGenerator`
merge, not a patch, and list the `domain` component: the overlay copies
`DOMAIN` into the route's hostname when it is built, and only a merge
lands early enough for the component to copy your value instead.

```yaml
resources:
  - github.com/go-steer/k8s-lookout/deploy-mcp-gemini?ref=vX.Y.Z
components:
  - github.com/go-steer/k8s-lookout/deploy-mcp-gemini/domain?ref=vX.Y.Z
configMapGenerator:
  - name: lookout-mcp
    namespace: agent-triage
    behavior: merge
    literals:
      - DOMAIN=lookout.example.com
      - GOOGLE_CLIENT_ID=123456789012-abc.apps.googleusercontent.com
      - GOOGLE_ALLOW=@example.com
      - GOOGLE_ALLOW_WRITE=
patches:
  - target: {kind: Gateway, name: lookout-mcp}
    patch: |-
      - op: replace
        path: /metadata/annotations/networking.gke.io~1certmap
        value: my-cert-map
      - op: replace
        path: /spec/addresses/0/value
        value: my-static-ip
```

Either way it creates, in `agent-triage`:

- a `lookout-mcp` ServiceAccount, with no RBAC and no Workload Identity;
- two stateless `lookout mcp` replicas in Google-token mode, one per
  zone, serving the `triage` and `audit` tool profiles plus six
  incident reads (cluster health, recent changes, event timeline,
  resource spec, blast radius, resource top) — minus `findings diff`,
  which needs a store — with the access log on stdout;
- a `gke-l7-global-external-managed` Gateway on `lookout-mcp-ip`, HTTPS
  only, an HTTPRoute for `DOMAIN`, a health check on the metadata path
  and a 300 s backend timeout.

Once `kubectl -n agent-triage get gateway lookout-mcp` shows
`PROGRAMMED True`, allow a minute or two for the load balancer to start
answering HTTPS:

```bash
curl -s https://lookout.example.com/.well-known/oauth-protected-resource/mcp
```

The metadata names `https://accounts.google.com` as the authorization
server. `curl -X POST https://lookout.example.com/mcp` gets a `401`: no
token.

Rollouts keep a serving pod behind the load balancer (a `preStop` drain,
`maxUnavailable: 0`). One gap remains: the Gateway attaches a zone to
the load balancer only minutes after the first pod lands there, and
GKE counts a pod in a not-yet-attached zone as ready. If a rollout moves
every replica into zones the load balancer has never served, requests
fail until the attach completes. Spreading replicas across zones makes
that unlikely, and it stops once each zone has been attached.

### A different image

The overlay runs `ghcr.io/go-steer/lookout:vX.Y.Z-gke`, the build with
the Google Cloud provider: the cloud checks call Cloud APIs with the
caller's token, and the GCP-free `:vX.Y.Z` answers them
`read.unavailable`. To run another flavor, a mirror, or your own build,
add an `images:` entry to your kustomization:

```yaml
images:
  - name: ghcr.io/go-steer/lookout
    newName: us-docker.pkg.dev/my-project/mirror/lookout
    newTag: vX.Y.Z-gke        # or digest: sha256:...
```

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
