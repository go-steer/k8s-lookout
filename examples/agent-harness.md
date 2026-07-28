# Testing lookout through agent harnesses

The scenarios in this directory double as fixtures for the *agent*
surface: inject a failure, hand the investigation to an LLM harness
armed with lookout, and judge whether it reaches the diagnosis the
scenario README documents. Three ways to arm a harness, in increasing
order of integration:

## 1. Plain CLI

Any harness that can run shell commands can use lookout directly —
every read command works against the current kubeconfig context, emits
token-dense logfmt findings, and always ends with an explicit summary
line (`scanned=N findings=N elapsed=D`), so "healthy" is never
ambiguous silence. Nothing to install beyond the binary.

## 2. Skills (the shipped workflow layer)

[`skills/`](../skills/) teaches the decision tree *across* commands —
changes-first vs bundle-first, when to trust events vs logs, the
pre-drain checklist. Skills version with tool flags, so install the
copy matching the deployed binary (skills/README.md):

```sh
# go-steer convention (core-agent, Antigravity, and anything else
# reading .agents/skills — project or user scope):
cp -r skills/k8s-triage skills/cluster-health skills/gitops-drift \
      skills/k8s-capacity skills/playbooks ~/.agents/skills/

# Claude Code (project scope; ~/.claude/skills for user scope):
cp -r skills/k8s-triage skills/cluster-health skills/gitops-drift \
      skills/k8s-capacity skills/playbooks .claude/skills/
```

`playbooks/` is shared reference material, not a skill — it must ride
along or the relative links break. The `lookout` binary must be on the
harness's PATH (or referenced by absolute path in your prompts).

## 3. MCP

`lookout mcp` serves every read command 1:1 as MCP tools (stdio by
default; schemas generated from the same command metadata as `--help`).
For Claude Code:

```sh
claude mcp add lookout -- lookout mcp
```

For other MCP clients, register `lookout mcp` as a stdio server the
same way. This is also how a distroless in-cluster daemon calls
lookout without a shell.

## The test loop

1. Deploy the stack (`kind/up`, `sentinel/up`, `workloads/`).
2. Inject a scenario: `examples/scenarios/<name>/inject`.
3. Prompt the harness with the scenario README's suggested prompt —
   each is written to be realistic (symptom, not solution):

   | Scenario | Prompt sketch |
   | --- | --- |
   | crashloop | "Something keeps restarting in lookout-demo — find it and tell me why." |
   | image-pull | "api isn't finishing its rollout. What changed; safe to roll back?" |
   | failed-mount | "A pod is stuck in ContainerCreating. Find the broken reference." |
   | oom | "The leaker pod keeps dying. Crash or resource problem?" |
   | pending | "Something's been Pending for a while. Will it ever schedule?" |
   | cert-expiry | "Audit for anything that breaks in the next two weeks untouched." |
   | pdb-gridlock | "We're about to upgrade nodes. Will anything block the drains?" |
   | endpoints-empty | "web.lookout-demo.svc stopped answering but pods look healthy." |
   | bad-rollout | "Sentinel opened an incident post-deploy but dashboards look fine. Real?" |

4. Judge against `examples/scenarios/<name>/verify` — the wire kinds
   and read-path findings it awaits are the ground truth the harness
   should have surfaced (the right *altitude* matters: e.g. for
   bad-rollout, `rollout.stall` on the Deployment, not just pod
   BackOffs).
5. `examples/scenarios/<name>/revert`.

## Watch-path with a real harness

The stub daemon only captures; to see per-incident sessions actually
drive an agent, point the sentinel at a real core-agent daemon —
change `--daemon-url`/`--token-env` in `examples/sentinel/up` (the
docs site's getting-started/connect-core-agent page covers tokens and
proxy identities), or use `--sink=webhook --sink-url=…` to feed any
receiver that implements the two webhook endpoints. The inject
payloads are identical either way — that's the frozen v1 schema the
stub log lets you diff against.
