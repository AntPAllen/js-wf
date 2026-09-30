#!/usr/bin/env python3
"""Summarize timestamped strace disk injections; overlap is not RPC attribution."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
import re

PREFIX = re.compile(r"^(\d+)\s+(\d+\.\d+)\s+(.*)$")
RESUMED = re.compile(r"^<\.\.\. (\w+) resumed>(.*)$")
CALL = re.compile(r"^(\w+)\(")
DURATION = re.compile(r"<([0-9]+\.[0-9]+)>$")
PATH = re.compile(r"\d+<([^>]+)>")


def parse(lines):
    pending, records = {}, []
    unmatched = 0
    for line in lines:
        match = PREFIX.match(line.strip())
        if not match:
            continue
        thread, timestamp, body = match.groups()
        timestamp = float(timestamp)
        resumed = RESUMED.match(body)
        if resumed:
            previous = pending.pop(thread, None)
            if previous is None or previous[1] != resumed[1]:
                unmatched += 1
                continue
            timestamp, _, start = previous
            body = start + resumed[2]
        elif body.endswith("<unfinished ...>"):
            call = CALL.match(body)
            if call:
                if thread in pending:
                    unmatched += 1
                pending[thread] = (timestamp, call[1], body.removesuffix("<unfinished ...>"))
            continue
        call, duration, path = CALL.match(body), DURATION.search(body), PATH.search(body)
        if "(DELAYED)" not in body:
            continue
        if not (call and duration and path):
            unmatched += 1
            continue
        elapsed = float(duration[1])
        records.append({"thread": int(thread), "start": timestamp,
                        "end": timestamp + elapsed, "duration": elapsed,
                        "syscall": call[1], "path": path[1]})
    return records, {"unfinished_calls": len(pending), "unmatched_lines": unmatched}


def group(path, raft_groups=None):
    stream = re.search(r"/streams/([^/]+)/", path)
    if stream:
        return stream[1]
    if "/$SYS/_js_/" in path:
        match = re.search(r"/\$SYS/_js_/([^/]+)/", path)
        return (raft_groups or {}).get(match[1], "raft_group_unattributed") if match else "raft_group_unattributed"
    return "other"


def summarize(records, operations, raft_groups=None):
    grouped = defaultdict(list)
    for record in records:
        grouped[group(record["path"], raft_groups)].append(record)
    totals = {name: {"calls": len(values),
                     "duration_sum_seconds": sum(r["duration"] for r in values),
                     "duration_max_seconds": max(r["duration"] for r in values)}
              for name, values in sorted(grouped.items())}
    slow = []
    for operation in operations:
        update = operation.get("LeaseUpdateDuration", 0) / 1e9
        if not operation.get("LeaseUpdateAttempted") or update < 0.1:
            continue
        end = datetime.fromisoformat(operation["At"].replace("Z", "+00:00")).timestamp()
        # Update is the final measured part of renewal. Observer/return overhead
        # makes this an approximate client window, not a server RPC interval.
        start = end - update
        overlaps = Counter(group(r["path"], raft_groups) for r in records if r["start"] < end and r["end"] > start)
        slow.append({"type": operation["Type"], "id": operation["ID"],
                     "operation": operation["Operation"], "index": operation["JournalIndex"],
                     "update_seconds": update,
                     "gate_seconds": operation.get("LeaseGateWait", 0) / 1e9,
                     "overlapping_delayed_calls": dict(sorted(overlaps.items()))})
    return {"delayed_calls": len(records), "groups": totals,
            "slow_renewals": sorted(slow, key=lambda item: item["update_seconds"], reverse=True),
            "interpretation": "Concurrent syscall totals and temporal overlaps do not identify an RPC or establish causation."}


def raft_mapping(snapshots):
    result = {}
    for snapshot in snapshots:
        for account in snapshot.get("account_details", []):
            for stream in account.get("stream_detail", []):
                label = "raft:" + account["name"] + "/" + stream["name"]
                if stream.get("stream_raft_group"):
                    result[stream["stream_raft_group"]] = label
                for consumer in stream.get("consumer_raft_groups", []):
                    if consumer.get("raft_group"):
                        result[consumer["raft_group"]] = label + "/" + consumer["name"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--operations", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--jsz", type=Path, action="append", default=[], help="pre-fault JSz snapshot; may be repeated")
    args = parser.parse_args()
    records, incomplete = parse(args.trace.read_text().splitlines())
    operations = json.loads(args.operations.read_text()) if args.operations else []
    mapping = raft_mapping([json.loads(path.read_text()) for path in args.jsz])
    result = {**summarize(records, operations, mapping), **incomplete}
    if not records:
        parser.error("no timestamped, path-decoded delayed syscalls found")
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
