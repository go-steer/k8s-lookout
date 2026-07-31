---
title: Getting started
description: Install lookout, run the first read-path commands against a kubeconfig, deploy the sentinel, connect it to core-agent, and wire up MCP.
sidebar:
  order: 0
---

This section is for anyone starting from zero: you have a kubeconfig,
maybe an AI agent, and you have never run `lookout`. By the end you will
have the binary installed, real diagnostic output from a cluster you
already have access to, and — if you take the later steps — a sentinel
deployed in-cluster, opening incident sessions for your agent. The
first useful command needs nothing deployed at all.

Remember the shape: one binary, `lookout`, used three ways — the **CLI**
(one-shot diagnostic commands), the **MCP server** (`lookout mcp`, the
same commands as MCP tools), and the **sentinel** (`lookout watch`, the
optional in-cluster watcher). The path in is incremental — each step
works without the next:

1. [Install](/getting-started/install/) — get the `lookout` binary on
   your workstation, and know which container image flavor a cluster
   deployment needs.
2. [First reads](/getting-started/first-run/) — the CLI against your
   current kubeconfig: `lookout health` and `lookout triage delta`, no
   deployment needed.
3. [Deploy the sentinel](/getting-started/deploy/) — one
   `kubectl apply -k` from the shipped manifests, what each manifest
   is, the RBAC tiers, and the flags that matter.
4. [What the sentinel watches](/getting-started/what-the-sentinel-watches/)
   — the failure classes it monitors, what is on out of the box, and
   the flag line that turns the rest on.
5. [Connect to core-agent](/getting-started/connect-core-agent/) — the
   daemon contract: sessions, injects, per-incident vs shared routing.
6. [MCP setup](/getting-started/mcp/) — every read command as an MCP tool,
   for agent runtimes that cannot shell out.
7. [Integrations](/getting-started/integrations/) — beyond core-agent:
   the read path from any MCP client or shell-capable agent, and the
   watch path into any webhook receiver.

The [Reference](/reference/) section is generated from the same
declarations that produce `--help` — when this section links a flag or a
command, the reference page is the authoritative surface.
