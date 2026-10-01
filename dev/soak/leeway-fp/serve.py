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

"""Serve dev/drills/stub-daemon.py's handler on a threaded server.

The drill stub runs a single-threaded HTTPServer with HTTP/1.1 keep-alive,
so one idle sentinel connection blocks every other: in the smoke run,
storm-member injects timed out client-side and were lost. A soak needs
every inject on the wire, so this serves the same Handler, unmodified, one
thread per connection. Requests are still handled one at a time — the
handler's session counter and its printed lines are not thread-safe.

Usage: serve.py <stub-daemon.py> <port>
"""

import importlib.util
import sys
import threading
from http.server import ThreadingHTTPServer

spec = importlib.util.spec_from_file_location("stub_daemon", sys.argv[1])
stub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stub)

lock = threading.Lock()


class Handler(stub.Handler):
    def do_POST(self):
        with lock:
            super().do_POST()


if __name__ == "__main__":
    port = int(sys.argv[2])
    print("stub daemon listening on :%d (threaded)" % port, flush=True)
    ThreadingHTTPServer(("", port), Handler).serve_forever()
