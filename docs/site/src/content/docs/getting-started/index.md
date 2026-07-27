---
title: Getting started
description: Install lookout, run the first read-path commands against a kubeconfig, deploy the sentinel, connect it to core-agent, and wire up MCP.
sidebar:
  order: 0
---

Everything ships as one multicall binary, `lookout`, and the path in is
incremental — each step works without the next:

1. [Install](/getting-started/install/) — container images (including the
   `-gke` flavor), signature verification, `go install`, and the
   image-swap compatibility contract.
2. [First reads](/getting-started/first-run/) — the read-path against your
   current kubeconfig: `lookout health` and `lookout triage delta`, no
   deployment needed.
3. [Deploy the sentinel](/getting-started/deploy/) — `kubectl apply -f
   deploy/`, what each manifest is, the RBAC tiers, and the flags that
   matter.
4. [Connect to core-agent](/getting-started/connect-core-agent/) — the
   daemon contract: sessions, injects, per-incident vs shared routing.
5. [MCP setup](/getting-started/mcp/) — every read command as an MCP tool,
   for daemons that cannot shell out.
6. [Integrations](/getting-started/integrations/) — beyond core-agent:
   the read path from any MCP client or shell-capable agent, and the
   watch path into any webhook receiver.

The [Reference](/reference/) section is generated from the same
declarations that produce `--help` — when this section links a flag or a
command, the reference page is the authoritative surface.
