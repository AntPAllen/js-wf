#!/usr/bin/env python3
"""Render go test -json output while the original events are retained separately."""
import json
import sys

for line in sys.stdin:
    sys.stdout.write(json.loads(line).get("Output", ""))
    sys.stdout.flush()
