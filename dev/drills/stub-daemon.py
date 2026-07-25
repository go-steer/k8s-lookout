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
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

counter = 0


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

    def do_POST(self):
        global counter
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode(errors="replace")
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
            print("INJECT sid=%s kind=%s token=%s body=%s" % (sid, kind, token, raw), flush=True)
            self._reply(200, {})
            return
        print("UNEXPECTED %s %s body=%s" % (self.command, self.path, raw), flush=True)
        self._reply(404, {"error": "unknown path"})


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7777
    print("stub-daemon listening on :%d" % port, flush=True)
    HTTPServer(("", port), Handler).serve_forever()
