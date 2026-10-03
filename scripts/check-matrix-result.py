#!/usr/bin/env python3
"""Require the named sustained test to execute and pass for its full duration."""
import json
import math
import sys


def check(events, test, duration):
    if any(event.get("Action") in ("fail", "build-fail") for event in events):
        raise ValueError("failure event in sustained test results")
    target = [event for event in events if event.get("Test") == test and event.get("Action") in ("pass", "fail", "skip")]
    if len(target) != 1 or target[0]["Action"] != "pass":
        raise ValueError(f"expected exactly one passing {test}; terminal events={target}")
    seconds = {"35s": 35, "10m": 600, "24h": 86400}.get(duration)
    elapsed = target[0].get("Elapsed")
    if (seconds is None or type(elapsed) not in (int, float)
            or not math.isfinite(elapsed) or elapsed < seconds):
        raise ValueError(f"test elapsed={target[0].get('Elapsed')} does not establish duration={duration}")
    packages = [event for event in events if "Test" not in event
                and event.get("Action") in ("pass", "fail", "skip")]
    if (len(packages) != 1 or packages[0]["Action"] != "pass"
            or packages[0].get("Package") != target[0].get("Package")):
        raise ValueError("expected exactly one matching package completion")


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as source:
        events = [json.loads(line) for line in source if line.strip()]
    check(events, sys.argv[2], sys.argv[3])
    print(f"Verified executed sustained test {sys.argv[2]} duration={sys.argv[3]}")
