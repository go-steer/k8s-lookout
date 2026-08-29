# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fake core-agent daemon for lookout watch drills.

Implements the ONLY two endpoints the sentinel speaks (DESIGN.md §3:
POST /sessions and POST /sessions/<sid>/inject) and logs every request
body to stdout as single greppable lines, so `kubectl logs` of this pod
is the drill's wire-level evidence capture:

    SESSION-CREATE sid=stub-sess-0001 caller=<X-Asserted-Caller>
    INJECT sid=stub-sess-0001 kind=<payload kind> body=<full JSON body>

No auth validation beyond noting whether a bearer token was present —
this is a capture stub for staging drills, never a daemon substitute in
a real deployment. Used by docs/milestones/M2.md's kind drills and by
dev/drills/node-failure.md's GKE replay.

The ONE daemon behavior the stub does reproduce faithfully is the
per-inject body ceiling (issue #338), because a stub that accepts any
size makes every drill blind to the fit guard --inject-max-bytes exists
to provide.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

counter = 0

# The real daemon's per-inject request-body cap: core-agent's
# pkg/attach/handlers.go injectMaxBytes = 8 * 1024, applied to POST
# /sessions/<sid>/inject only (not /sessions). An over-limit body comes
# back 400 — NOT 413 — so it is permanent, non-retryable, and costs the
# whole inject; lookout's inject.MaxInjectBytes mirrors this value and
# the dispatcher fits payloads under it before POSTing.
INJECT_MAX_BYTES = 8 * 1024


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # silence default access log
        pass

    def _reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _reply_text(self, code, text):
        """Plain-text error, matching the daemon's http.Error shape."""
        body = (text + "\n").encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        global counter
        length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(length)
        raw = body_bytes.decode(errors="replace")
        token = "present" if self.headers.get("Authorization") else "MISSING"
        caller = self.headers.get("X-Asserted-Caller", "")
        if self.path == "/sessions":
            counter += 1
            sid = "stub-sess-%04d" % counter
            print("SESSION-CREATE sid=%s caller=%s token=%s" % (sid, caller, token), flush=True)
            self._reply(201, {"sessionID": sid})
            return
        if self.path.startswith("/sessions/") and self.path.endswith("/inject"):
            sid = self.path.split("/")[2]
            kind = ""
            try:
                # The inject envelope is {"message": "<payload JSON>"}.
                msg = json.loads(raw).get("message", "")
                kind = json.loads(msg).get("kind", "")
            except (ValueError, AttributeError):
                pass
            if len(body_bytes) > INJECT_MAX_BYTES:
                # Deliberately NOT an `INJECT` line, and `kind_rejected=`
                # rather than `kind=`: a rejected payload never reached
                # the session, so it must not satisfy a drill's
                # "delivered" grep. An assertion that counts injects
                # SHOULD go red when the fit guard lets one through.
                print("REJECT sid=%s bytes=%d limit=%d kind_rejected=%s head=%s" % (
                    sid, len(body_bytes), INJECT_MAX_BYTES, kind, raw[:200]), flush=True)
                self._reply_text(400, "request body too large (max %d bytes)" % INJECT_MAX_BYTES)
                return
            print("INJECT sid=%s kind=%s token=%s body=%s" % (sid, kind, token, raw), flush=True)
            self._reply(200, {})
            return
        print("UNEXPECTED %s %s body=%s" % (self.command, self.path, raw), flush=True)
        self._reply(404, {"error": "unknown path"})


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7777
    print("stub-daemon listening on :%d" % port, flush=True)
    HTTPServer(("", port), Handler).serve_forever()
