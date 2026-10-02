#!/usr/bin/env python3
"""Require the named sustained test to execute and pass for its full duration."""
import json
import sys


def check(events, test, duration):
    target = [event for event in events if event.get("Test") == test and event.get("Action") in ("pass", "fail", "skip")]
    if len(target) != 1 or target[0]["Action"] != "pass":
        raise ValueError(f"expected exactly one passing {test}; terminal events={target}")
    seconds = {"35s": 35, "10m": 600, "24h": 86400}.get(duration)
    if seconds is None or target[0].get("Elapsed", 0) < seconds:
        raise ValueError(f"test elapsed={target[0].get('Elapsed')} does not establish duration={duration}")
    if not any(event.get("Action") == "pass" and "Test" not in event for event in events):
        raise ValueError("package completion is missing")


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as source:
        events = [json.loads(line) for line in source if line.strip()]
    check(events, sys.argv[2], sys.argv[3])
    print(f"Verified executed sustained test {sys.argv[2]} duration={sys.argv[3]}")
