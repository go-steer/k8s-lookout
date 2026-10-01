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

"""Prefix every stdin line with a UTC timestamp, so the capture stub's
wire log can be lined up with the churn log. The stub itself
(dev/drills/stub-daemon.py) is shared with the drills and stays as is."""

import sys
import time

for line in sys.stdin:
    sys.stdout.write(time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + line)
    sys.stdout.flush()
