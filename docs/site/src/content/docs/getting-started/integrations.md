---
title: Integrations
description: Beyond core-agent — the read path from any MCP client or shell-capable agent, skills portability, and receiving the watch path anywhere over the webhook sink.
sidebar:
  order: 6
---

core-agent is the first-class runtime and the default everywhere, but
neither data path is welded to it. The read path terminates at MCP,
a CLI, and plain-markdown skills — surfaces any agent runtime already
speaks. The watch path terminates at a
[two-verb sink contract](https://github.com/go-steer/k8s-lookout/blob/main/docs/agent-sink-design.md)
whose webhook implementation any HTTP endpoint can receive.

## Consuming the read path

### Any MCP client

`lookout mcp` is a standard MCP server (stdio, or loopback streamable
HTTP) — nothing about it is core-agent-specific. The config shape for
a generic MCP client:

```json
{
  "mcpServers": {
    "k8s-lookout": {
      "command": "lookout",
      "args": ["mcp"]
    }
  }
}
```

Claude Code users, same thing from the CLI:

```sh
claude mcp add k8s-lookout -- lookout mcp
```

The server reads the current kubeconfig context (the ServiceAccount
when in-cluster), and every tool result is the CLI's exact sanitized
payload, summary line included. Transports, the non-loopback refusal,
and the full tool↔command table are on the
[MCP setup page](/getting-started/mcp/).

### Shell-capable agents

Any agent with a shell tool needs no integration at all — the CLI
contract is designed for capture into a context window: token-dense
logfmt (or `--format=json`) on stdout, always terminated by the
`scanned=/findings=/elapsed=` summary line; diagnostics on stderr
only, so a captured stream never mixes streams; everything through
the sanitizer. `lookout --help` teaches the surface in one read, and
the [Reference](/reference/) section is generated from the same
declarations.

### Skills travel too

The workflow skills in
[`skills/`](https://github.com/go-steer/k8s-lookout/tree/main/skills)
are plain markdown with a `SKILL.md` + `references/` layout and no
runtime-specific hooks — they install into any runtime that loads
that shape (Claude Code's skills directory, `$HOME/.agents/skills/`
conventions, or a framework's own prompt-library mechanism) by
copying:

```sh
cp -r skills/k8s-triage skills/cluster-health skills/gitops-drift \
      skills/k8s-capacity skills/playbooks "$HOME/.agents/skills/"
```

Skills version with tool flags and output formats, so reinstall the
`skills/` matching the `lookout` tag you deploy. `playbooks/` is shared
reference material the skills link to — keep it alongside.

## Receiving the watch path anywhere

The sentinel's delivery side is a `Sink`: the core-agent daemon
client is the default (`--sink=core-agent`, wire-identical to every
release since M0), and `--sink=webhook` delivers the same
[schema-v1 signal payloads](/reference/signal-kinds/) to any HTTP
receiver:

```sh
lookout watch \
  --sink=webhook \
  --sink-url=https://receiver.example.com/lookout \
  --sink-token-env=SINK_TOKEN \
  --cluster-name=prod-east
```

The contract is two endpoints, mirroring the two verbs the watch-path
needs from any runtime — open an incident context, append to it:

| Verb | Request | Response |
| --- | --- | --- |
| Open | `POST <url>/incidents`, body = one signal payload | 2xx, `{"id":"<opaque>"}` |
| Append | `POST <url>/incidents/<id>/events`, same body shape | 2xx |

Every request carries `Authorization: Bearer <token>` from
`--sink-token-env`, and the body is the schema-v1 payload JSON
itself — what the core-agent sink wraps in its inject envelope, here
unwrapped. The exchange, curl-able against your own receiver (both
payloads below are captured ones, from the M0 wire capture and the
M2 fix-verify drill, abridged):

```sh
curl -s -X POST "https://receiver.example.com/lookout/incidents" \
  -H "Authorization: Bearer $SINK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"kind":"k8s-event","reason":"BackOff","namespace":"default","kind_of_object":"Pod","name":"crashloop-demo","container":"spec.containers{crasher}","uid":"7503ea47-d147-4342-92b2-743a1d88cd4b","message":"Back-off restarting failed container crasher in pod crashloop-demo_default(7503ea47-d147-4342-92b2-743a1d88cd4b)","count":1,"first_seen":"2026-07-24T17:12:00Z","last_seen":"2026-07-24T17:12:00Z","cluster":"local","context":{"node":"kl-m0-control-plane"}}'
```

```json
{"id":"inc-0001"}
```

Later, the sentinel observes the symptom clear and holds stable — the
closed-loop outcome record appends to the same context:

```sh
curl -s -X POST "https://receiver.example.com/lookout/incidents/inc-0001/events" \
  -H "Authorization: Bearer $SINK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"kind":"resolved","reason":"CrashLoopBackOff","namespace":"fixlab","kind_of_object":"Pod","name":"payment-869d9b5594-d6gtm","uid":"6b6d28f1-b811-49dd-86a7-9bb0c3c8468a","fingerprint":"sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b","cluster":"kl-m2","first_seen":"2026-07-25T00:54:07Z","resolved_at":"2026-07-25T00:56:53Z","cleared_after":"1m36s","observed_stable_for":"1m10s","resolution":"recovered","context":{}}'
```

Followups, storm records, and watchboard digests arrive the same
way — same endpoint, different `kind`.

What a receiver should know:

- **You may be stateless.** Ignore the ids you hand out if you like —
  every payload carries its own identity (`fingerprint`, object
  coordinates, `kind`), and `lookout` sequences opens before appends.
  Correlating payloads into per-incident threads is your opportunity,
  not your obligation.
- **A reference receiver already exists.**
  [`dev/drills/stub-daemon.py`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/stub-daemon.py)
  is the capture receiver every drill and milestone record uses —
  a page of stdlib Python implementing the open/append pattern
  (in the default sink's core-agent shape) and logging each body as
  one greppable line. Start there.
- **Delivery failures behave exactly as with core-agent**: logged,
  counted in `inject_errors_total`, re-fired through the dedup retry
  cooldown. The sink adds no retry semantics of its own.
- **`token-burn` needs core-agent.** That source reads the daemon's
  usage API; under `--sink=webhook` it idles with a loud startup
  message naming the source and the reason — never a silent empty
  watch.

core-agent remains the first-class default: enriched sessions an
agent wakes up inside of, the [closed loop](/concepts/closed-loop/),
and the usage-driven `token.burn` source all assume a runtime on the
other end — the webhook sink is the door for everyone else, not a
replacement. Wiring the daemon is
[Connect to core-agent](/getting-started/connect-core-agent/); the
settled design (and what is deliberately out of scope) is
[`docs/agent-sink-design.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/agent-sink-design.md).
