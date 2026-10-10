#!/usr/bin/env python3
"""Recompute the failing sample's dispatch timing without restoring native stores."""
import calendar
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parent


def ns(value):
    seconds, fraction = value.removesuffix("Z").split(".")
    return calendar.timegm(time.strptime(seconds, "%Y-%m-%dT%H:%M:%S")) * 10**9 + int(fraction.ljust(9, "0"))


def read(name):
    return json.loads((ROOT / name).read_text())


def main():
    review = read("independent-failure-review.json")
    sample = review["offending_sample"]
    dispatch = read("outlier-dispatch.json")
    operations = read("outlier-operations.json")
    for rows in (dispatch, operations):
        assert rows and all(x["Type"] == sample["type"] and x["ID"] == sample["id"] for x in rows)
        assert all(ns(a["At"]) <= ns(b["At"]) for a, b in zip(rows, rows[1:]))
    assert ns(sample["observed"]) - ns(sample["enabled"]) == sample["delay_ns"]
    source = review["source"]
    paths = ["provision/provision.go", "worker/worker.go", "integration/mixed_matrix_leader_test.go"]
    sources = {p: subprocess.check_output(["git", "show", f"{source}:{p}"], cwd=ROOT).decode() for p in paths}
    ttl = int(re.search(r"const LeaseTTL = (\d+) \* time.Second", sources[paths[0]])[1])
    nak = int(re.search(r"const heldLeaseNakDelay = (\d+) \* time.Second", sources[paths[1]])[1])
    assert "const DefaultAckWait = provision.LeaseTTL + time.Second" in sources[paths[1]]
    assert "WithAckWait" not in sources[paths[2]]
    a, b = max(zip(dispatch, dispatch[1:]), key=lambda pair: ns(pair[1]["At"]) - ns(pair[0]["At"]))
    assert (a["Stage"], a["RunSequence"], b["Stage"], b["RunSequence"]) == ("ack", 615, "fetched", 617)
    first_nak = next(x for x in dispatch if x["RunSequence"] == 617 and x["Stage"] == "nak")
    assert first_nak["Error"] == "" and b["Delivery"] == first_nak["Delivery"] + 1
    heal = ns(review["overlapping_majority_fault"]["healed"])
    due = ns(first_nak["At"]) + nak * 10**9
    longest = max(operations, key=lambda x: x["Duration"])
    result = {
        "source": source,
        "input_sha256": {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in ["independent-failure-review.json", "outlier-dispatch.json", "outlier-operations.json"]},
        "source_sha256": {p: hashlib.sha256(v.encode()).hexdigest() for p, v in sources.items()},
        "sample": sample,
        "source_defaults_seconds": {"lease_ttl": ttl, "ack_wait": ttl + 1, "held_lease_nak_delay": nak},
        "largest_dispatch_gap_ns": ns(b["At"]) - ns(a["At"]),
        "largest_dispatch_gap_from": a,
        "largest_dispatch_gap_to": b,
        "run617_nak_to_redelivery_ns": ns(b["At"]) - ns(first_nak["At"]),
        "expected_nak_due_at_ns": due,
        "nak_due_minus_partition_start_ns": due - ns(review["overlapping_majority_fault"]["killed"]),
        "redelivery_after_heal_ns": ns(b["At"]) - heal,
        "terminal_after_heal_ns": ns(sample["observed"]) - heal,
        "longest_recorded_operation": longest,
        "raw_gate_pass": sample["delay_ns"] < 30 * 10**9,
        "cause_confirmed": False,
        "scope": "Invocation projections only: no partition-wide slot occupancy, pull lifetime, server redelivery deadline, or NAK receipt proof. A successful client NAK call does not prove server processing. No native rerun or gate change.",
    }
    assert result["terminal_after_heal_ns"] == review["diagnostic_post_heal_ns"]
    assert not result["raw_gate_pass"]
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
