# skills/ — workflow skills for lookout

Task-triggered skills teaching the *decision tree across* lookout commands
(DESIGN.md §4.4). Skills map to workflows, never to commands:

| Skill | Workflow |
| --- | --- |
| `k8s-triage/` | incident investigation: changes-first on sudden regressions, bundle first otherwise; logs vs events vs radius; when to net-probe |
| `cluster-health/` | on-demand & scheduled assessment: health, reading the scorecard, drilling down |
| `playbooks/` | per-symptom command sequences (CrashLoopBackOff, FailedMount, HPA thrash), referenced by the skills |

(`k8s-capacity/` arrives with M4, `gitops-drift/` with M5.)

Each skill is three-level progressive disclosure: frontmatter (~50 tokens,
always in context) answers "is lookout relevant now"; the SKILL.md body (on
trigger) teaches the workflow; `references/*.md` are per-command deep docs
loaded only when a command is actually being run. The references are
**generated** from `pkg/checks` command metadata — do not edit them; run
`dev/tools/gen-skill-refs` after changing command metadata (CI enforces
freshness, and validates every documented command line against the
registry).

## Installing

Skills version with tool flags and output formats, so they ship in this
repo and install into the consuming deployment — copy them next to the
`lookout` binary version you deploy:

```sh
# project scope (one deployment/checkout)
cp -r skills/k8s-triage skills/cluster-health skills/playbooks \
      /path/to/deployment/.agents/skills/

# user scope (all sessions of one user)
cp -r skills/k8s-triage skills/cluster-health skills/playbooks \
      "$HOME/.agents/skills/"
```

`playbooks/` is not a skill itself (no frontmatter) — it is shared
reference material the skills point at; install it alongside them so the
relative `../playbooks/` links resolve. When you upgrade the deployed
lookout image, reinstall the matching `skills/` from the same tag.
