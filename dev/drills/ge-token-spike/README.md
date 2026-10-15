# Spike: a Gemini Enterprise token against GKE and Cloud APIs (#698)

Before lookout builds per-user identity for Gemini Enterprise (#699), this
spike answers one question with a real token: **does the Google access token
a Gemini Enterprise app sends for each person work for what lookout needs?**

It deploys a throwaway MCP server to Cloud Run. Gemini Enterprise calls it
through a Custom MCP Server data store, exactly as it would call lookout. Its
one tool, `spike_report`, runs every check with the caller's own token and
returns a table. The token is never shown, logged or stored.

The server is built on steward v0.1.0 (`googletoken`, `allow`, `httpauth`,
`mcpauth`), so this is also steward's first run against a real Gemini
Enterprise. Its own service account has **no roles**: anything that passes,
passed on the person's token.

Allow about 30 minutes, most of it console clicks in steps 2 and 4. Run it
in `gke-demos-345619`, where supercargo's Gemini Enterprise app already
lives, so the organization-policy work is mostly done.

## What it checks

| Check | #698 question | Pass means |
|---|---|---|
| `identity`, `token.scopes` | (a) does tokeninfo show our client, all three scopes, a verified email, and `hd`? | steward verified the token (it refuses otherwise); `token.scopes` is ACCEPTED when `cloud-platform` is present |
| `cloud.container.clusters.get` | (c) Cloud APIs | the Container API describes the cluster to this person |
| `gke.dns-endpoint.authn` | (b) GKE over the DNS endpoint | the API server's `SelfSubjectReview` names the person |
| `gke.dns-endpoint.rbac` | (b) RBAC is the person's | `SelfSubjectAccessReview` answers for the person (allowed or not) |
| `gke.ip-endpoint.authn` | (b) GKE over the IP endpoint, the path an in-cluster lookout uses | as above, over the cluster's IP endpoint with its CA |
| `cloud.compute`, `cloud.monitoring`, `cloud.logging` | (c) the APIs lookout's cloud-backed checks call | the API accepted the token |
| `token.fingerprint`, `token.lifetime` | how often Gemini Enterprise refreshes | compare across runs: a new fingerprint is a refreshed token |

Verdicts: **ACCEPTED** (200, or 403: the token worked but IAM or RBAC said
no, which still answers "does this token work as the person" with yes),
**REJECTED** (401: the token itself was refused), **UNREACHABLE** (no HTTP
answer, for example a private endpoint; says nothing about the token),
**SKIPPED** (a prerequisite failed).

The IP-endpoint check is the one most likely to come back UNREACHABLE from
Cloud Run if the cluster has authorized networks. That still leaves the
question open for an in-cluster lookout, so note it in #698.

## 0. Before you start

```bash
export PROJECT=gke-demos-345619
export REGION=us-central1
export CLUSTER=projects/$PROJECT/locations/<location>/clusters/<name>
gcloud config set project "$PROJECT"
```

The cluster needs its **DNS endpoint** enabled:

```bash
gcloud container clusters describe <name> --location <location> \
  --format='value(controlPlaneEndpointsConfig.dnsEndpointConfig.endpoint)'
# empty? enable it:
gcloud container clusters update <name> --location <location> --enable-dns-access
```

You don't need any RBAC binding in the cluster: the spike reports whatever
yours is. Giving yourself `view` makes the RBAC line more interesting.

## 1. The OAuth consent screen

The project's consent screen already exists (supercargo's, Internal). Under
**Data access**, add `https://www.googleapis.com/auth/cloud-platform` next to
`openid` and `userinfo.email`, then **Save**. For an Internal app that's all
it takes. It doesn't change what supercargo's client asks for.

## 2. A new OAuth client

Don't reuse supercargo's client: tokeninfo has to show the spike's own
client as `azp`.

1. **Google Auth Platform → Clients → Create client**, type **Web
   application**, name `lookout-ge-spike`.
2. **Authorized redirect URI:** `https://vertexaisearch.cloud.google.com/oauth-redirect`
3. **Create**. Copy the client ID and secret. The secret only goes into the
   data store in step 4.
4. If the organization restricts Google Cloud scopes, consent will fail with
   "Access blocked: your institution's admin needs to review" until a
   Workspace admin marks this client **Trusted**. Record whether that
   happened; it's one of #698's questions.

## 3. Deploy

```bash
export SPIKE_CLIENT_ID=<client ID from step 2>
export SPIKE_MEMBERS=you@example.com      # or @example.com
export SPIKE_CLUSTER=$CLUSTER
dev/drills/ge-token-spike/deploy.sh
```

It enables Cloud Run, Cloud Build and Artifact Registry, creates the
`ge-token-spike` service account with no roles, builds this directory with
Cloud Build, and deploys one instance with `--no-invoker-iam-check`
(Gemini Enterprise calls without a Cloud Run identity token; the server
checks each caller's Google token itself). It prints the MCP URL. Check it:

```bash
curl -s https://ge-token-spike-<project number>.$REGION.run.app/.well-known/oauth-protected-resource/mcp
```

The answer names `https://accounts.google.com` as the authorization server.

If `allowedEgressFqdns` is enforced for Gemini Enterprise (supercargo's
runbook §5), add `ge-token-spike-<project number>.<region>.run.app`.

## 4. The Custom MCP Server data store

1. https://console.cloud.google.com/gemini-enterprise/ → your app → **Data
   stores → Create data store → Custom MCP Server → Add MCP server**.
2. **Authentication settings: OAuth 2.0.**

   | Field | Value |
   |---|---|
   | MCP Server URL | the URL deploy.sh printed (ends in `/mcp`) |
   | Authorization URL | `https://accounts.google.com/o/oauth2/v2/auth` |
   | Authorization URL Parameters | `&access_type=offline&prompt=consent` |
   | Token URL | `https://oauth2.googleapis.com/token` |
   | Client ID / Client Secret | from step 2 |
   | Enable PKCE Support | on |
   | Scopes | `openid email https://www.googleapis.com/auth/cloud-platform` |

3. **Verify Auth**, and sign in with an account in `SPIKE_MEMBERS`.
4. Location **global**, connector name `lookout token spike`, description:
   > Runs the lookout token spike report (issue 698). Use only when the user
   > asks to run the spike report.
5. **Create**, wait for **Active**, then **Actions → Reload custom actions**,
   and enable the connector in the chat.

## 5. Run it

In the Gemini Enterprise chat:

> Run the spike report with the lookout token spike connector and show me
> the table exactly as returned.

Paste the table into #698. Then run it again after more than an hour,
in the same chat and in a new one, and compare `token.fingerprint` and
`token.lifetime`. That shows how Gemini Enterprise refreshes.

The service log (Cloud Run → `ge-token-spike` → Logs) has one line per run
with the caller's email and the verdicts, and a warning if steward could
not reach Google. It never contains the token.

## 6. Clean up

```bash
dev/drills/ge-token-spike/deploy.sh delete
```

Then delete the data store in Gemini Enterprise and the `lookout-ge-spike`
OAuth client. Remove `cloud-platform` from the consent screen's data access
if nothing else needs it.
