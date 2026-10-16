# Deploy demo

`.github/workflows/deploy-demo.yml` keeps the project's demo Gemini
Enterprise endpoint on the latest release. After **Release images**
succeeds for a tag, it applies `deploy-mcp-gemini-gateway/` at that tag
with the signed `:<tag>-gke` image, then checks the two answers Gemini
Enterprise depends on: the RFC 9728 metadata and a `401` for a call
without a token. Run it by hand (**Actions → Deploy demo → Run
workflow**, with a tag) to redeploy or to pick up a changed variable.

Nothing about the deployment is in the repository. The endpoint's
values are variables of the `demo` GitHub environment, and the workflow
signs in to Google Cloud through Workload Identity Federation: no key,
and only jobs of this repository running in that environment can use
the deploy service account. The account can fetch cluster credentials
and apply the overlay's objects in one namespace, nothing else.
lookout itself still has no RBAC: every tool call reads as its caller.

The workflow is skipped until the repository variable `DEMO_ENABLED` is
`true`, so a fork or an unconfigured clone does nothing.

## One-time setup

Prerequisites, the same as the "Gemini Enterprise" guide's: a GKE
cluster with the Gateway API, a reserved global static IP with the
endpoint's DNS name pointing at it, a Certificate Manager map holding a
certificate for that name, and the OAuth client the Gemini Enterprise
data store uses.

### 1. Google Cloud and the cluster

Needs permission to create the identity provider and the service
account and to grant IAM roles in the project, plus cluster admin for
the namespace and its Role:

```bash
PROJECT=my-project CLUSTER=my-cluster LOCATION=us-central1 NAMESPACE=lookout \
  dev/deploy-demo/setup.sh
```

It creates (or reuses):

- the `github-actions` Workload Identity pool, and a `k8s-lookout-demo`
  provider in it that accepts only this repository (by numeric ID) in
  the `demo` environment;
- the `lookout-demo-deploy` service account, which only that provider's
  `demo` subject may act as;
- `roles/container.clusterViewer` for it: cluster credentials, no
  Kubernetes object access;
- the namespace, and in it the `lookout-demo-deploy` Role and
  RoleBinding from `rbac.yaml`: get, create and update the overlay's
  object kinds; no delete, nothing cluster-scoped.

It prints the first six variables for step 2.

### 2. The GitHub environment

Create the `demo` environment (**Settings → Environments**). Restrict
its deployment branches to `main`: both triggers run the workflow from
the default branch. Add a required reviewer if each deploy should wait
for a person.

Its variables:

| Variable | Value |
|---|---|
| `WIF_PROVIDER` | printed by `setup.sh` |
| `DEPLOY_SERVICE_ACCOUNT` | printed by `setup.sh` |
| `GCP_PROJECT` | the project |
| `GKE_CLUSTER` | the cluster |
| `GKE_LOCATION` | its region or zone |
| `NAMESPACE` | as given to `setup.sh` |
| `DOMAIN` | the endpoint's hostname |
| `GOOGLE_CLIENT_ID` | the data store's OAuth client ID |
| `GOOGLE_ALLOW` | who may call: emails and `@domain`s, comma-separated |
| `GOOGLE_ALLOW_WRITE` | who may write; empty for no one |
| `STATIC_IP_NAME` | the reserved global static IP's name |
| `CERT_MAP` | the Certificate Manager map's name |

Or from a shell, with the GitHub CLI:

```bash
gh api -X PUT repos/go-steer/k8s-lookout/environments/demo \
  -F 'deployment_branch_policy[protected_branches]=false' \
  -F 'deployment_branch_policy[custom_branch_policies]=true'
gh api -X POST repos/go-steer/k8s-lookout/environments/demo/deployment-branch-policies \
  -f name=main -f type=branch
gh variable set DOMAIN --env demo --body lookout.example.com   # and so on
```

Then enable the workflow:

```bash
gh variable set DEMO_ENABLED --body true
```

### 3. First deploy

Run the workflow by hand with the current release tag. A release
earlier than the first one that ships `deploy-mcp-gemini-gateway/`
cannot be deployed this way.

## Changing a value

Edit the environment variable and run the workflow by hand with the
current release tag. A change to the static IP or the certificate map
reprograms the load balancer, which can drop requests for a few
seconds; a change to the allow-lists or the client ID only rolls the
pods, which the overlay does without dropping any.

## Taking it down

```bash
gh variable set DEMO_ENABLED --body false
kubectl delete namespace lookout
```

The service account, provider and IAM bindings can stay, or go with
`gcloud iam service-accounts delete` and `gcloud iam
workload-identity-pools providers delete`.
