#!/usr/bin/env python3
"""Verify all sustained Tier 2 rows from completed Actions metadata and job logs.

This verifies recorded runtime checks, not an independent scan of raw stores.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import zipfile


def local_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


planner = local_module("matrix_planner", "matrix-campaign.py")
gate = local_module("matrix_row_gate", "check-matrix-campaign.py")
duration_gate = local_module("matrix_duration_gate", "check-matrix-result.py")
ROWS = {
    "journal": ("journal_leader", "TestMixedMatrixJournalLeaderEveryThirtySeconds"),
    "consumer": ("consumer_leader", "TestMixedMatrixConsumerLeaderEveryThirtySeconds"),
    "cluster": ("all_servers", "TestMixedMatrixAllServersKilledEveryThirtySeconds"),
    "partition": ("server_partition", "TestMixedMatrixServerPartitionEveryThirtySeconds"),
    "worker": ("worker_kill", "TestMixedMatrixRandomWorkerKilledEveryFiveSeconds"),
    "pause": ("worker_pause", "TestMixedMatrixWorkerPausedFortyFiveSeconds"),
    "isolation": ("worker_isolation", "TestMixedMatrixWorkerReplyIsolationFortyFiveSeconds"),
    "workerclock": ("worker_clock", "TestMixedMatrixWorkerClockSkew"),
    "serverclockplus": ("server_clock_plus", "TestMixedMatrixServerClockSkewPositive"),
    "serverclockminus": ("server_clock_minus", "TestMixedMatrixServerClockSkewNegative"),
    "fanoutrestart": ("fanout_restart", "TestMixedMatrixFanoutRestartEveryThirtySeconds"),
    "blockdisk": ("block_disk", "TestMixedMatrixBlockDiskStallEveryThirtySeconds"),
    "upgrade": ("rolling_upgrade", "TestMixedMatrixRollingServerUpgrade"),
}
HEADER = re.compile(r"Sustained matrix row=(\w+) seed=(\d+) duration=(\S+)")


def job_name(job):
    return f"leader ({job['row']}, {job['artifact_seed']})"


def check_full_matrix(metadata, logs, count):
    planned = planner.campaign("all", count, "10m")
    if set(ROWS) != set(planner.ROWS):
        raise ValueError("fault registry differs from campaign plan")
    if metadata.get("status") != "completed" or metadata.get("conclusion") != "success":
        raise ValueError("whole campaign is not terminal and successful")
    revision = metadata.get("headSha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("missing source revision")
    expected = {job_name(job) for job in planned}
    jobs = metadata.get("jobs", [])
    if len(jobs) != len(expected) + 1 or {job["name"] for job in jobs} != expected | {"seeds"}:
        raise ValueError("missing, duplicate or unexpected campaign jobs")
    if any(job.get("status") != "completed" or job.get("conclusion") != "success" for job in jobs):
        raise ValueError("unfinished, skipped or failed campaign job")
    if set(logs) != expected:
        raise ValueError("missing or unexpected job logs")
    rows = {row: [] for row in ROWS}
    for planned_job in planned:
        name = job_name(planned_job)
        log = logs[name]
        if revision not in log:
            raise ValueError(f"{name}: checkout revision missing or mismatched")
        headers = list(HEADER.finditer(log))
        actual = [(match[1], int(match[2]), match[3]) for match in headers]
        expected_headers = [(planned_job["row"], seed, "10m") for seed in range(planned_job["first"], planned_job["last"] + 1)]
        if actual != expected_headers:
            raise ValueError(f"{name}: missing, duplicate, wrong or shortened seed executions")
        runtime_row, test = ROWS[planned_job["row"]]
        for i, header in enumerate(headers):
            seed = int(header[2])
            segment = log[header.end():headers[i+1].start() if i+1 < len(headers) else len(log)]
            guard = f"Verified executed sustained test {test} duration=10m"
            if segment.count(guard) != 1:
                raise ValueError(f"{name} seed={seed}: missing or duplicate full-duration result guard")
            rows[planned_job["row"]].append(gate.check_seed(segment, runtime_row, seed, test))
    for row, seeds in rows.items():
        if [record["seed"] for record in seeds] != list(range(1, count+1)):
            raise ValueError(f"{row}: incomplete consecutive seed range")
    flattened = [seed for seeds in rows.values() for seed in seeds]
    return {
        "revision": revision,
        "scope": "whole three-node sustained fault matrix at the recorded revision",
        "consecutive_seeds_per_row": count,
        "duration_seconds_per_seed": 600,
        "fault_variants": len(rows),
        "executions": len(flattened),
        # Metadata/log checks are a preflight. Release qualification also needs
        # every uploaded named-test/package event sequence checked below.
        "clears_tier2_200_seed_gate": False,
        "raw_artifacts_verified": False,
        "clears_tier3_24_hour_soak": False,
        "invocations": sum(seed["invocations"] for seed in flattened),
        "faults": sum(seed["faults"] for seed in flattened),
        "worst_terminal_p99_seconds": max(seed["terminal_p99_seconds"] for seed in flattened),
        "worst_cell_terminal_p99_seconds": max(cell["terminal_p99_seconds"] for seed in flattened for cell in seed["cells"].values()),
        "worst_progress_p99_seconds": max(event["p99_seconds"] for seed in flattened for event in seed["progress"].values()),
        "rows": rows,
    }


def check_artifacts(report, root):
    """Cross-check raw Go JSON against every log-derived row verdict."""
    hashes = {}
    indexed = {}
    for path in root.rglob("*-test.jsonl"):
        indexed.setdefault(path.name, []).append(path)
    for row, seeds in report["rows"].items():
        runtime_row, test = ROWS[row]
        for expected in seeds:
            seed = expected["seed"]
            matches = indexed.get(f"matrix-{row}-{seed}-test.jsonl", [])
            if len(matches) != 1 or not matches[0].is_file():
                raise ValueError(f"{row}/{seed}: missing or duplicate raw events")
            path = matches[0]
            raw = path.read_bytes()
            events = [json.loads(line) for line in raw.splitlines() if line.strip()]
            duration_gate.check(events, test, "10m")
            if any(e.get("Action") in ("fail", "build-fail") for e in events):
                raise ValueError(f"{row}/{seed}: raw failure event")
            actual = gate.check_seed("".join(e.get("Output", "") for e in events), runtime_row, seed, test)
            if actual != expected:
                raise ValueError(f"{row}/{seed}: raw events disagree with job log verdict")
            hashes[str(path.relative_to(root))] = hashlib.sha256(raw).hexdigest()
    return hashes


def qualify_artifacts(report, root):
    """Promote a metadata preflight only after all raw event checks succeed."""
    hashes = check_artifacts(report, root)
    return dict(report, raw_event_sha256=hashes, raw_artifacts_verified=True,
                clears_tier2_200_seed_gate=report["consecutive_seeds_per_row"] == 200)


def load_logs(path):
    logs = {}
    with zipfile.ZipFile(path) as archive:
        for filename in archive.namelist():
            match = re.fullmatch(r"\d+_(leader \(\w+, [\d-]+\))\.txt", filename)
            if match:
                name = match[1]
                if name in logs:
                    raise ValueError(f"duplicate job log {name}")
                logs[name] = archive.read(filename).decode("utf-8")
    return logs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", required=True, type=Path)
    parser.add_argument("--logs", required=True, type=Path)
    parser.add_argument("--seeds", required=True, type=int, choices=(1, 20, 200))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--artifacts", type=Path, help="matching raw Go JSON for every row/seed; required for 200 seeds")
    args = parser.parse_args()
    if args.seeds == 200 and args.artifacts is None:
        parser.error("the 200-seed release gate requires --artifacts")
    report = check_full_matrix(json.loads(args.jobs.read_text()), load_logs(args.logs), args.seeds)
    if args.artifacts is not None:
        report = qualify_artifacts(report, args.artifacts)
    report["input_sha256"] = {label: hashlib.sha256(path.read_bytes()).hexdigest() for label, path in (("jobs", args.jobs), ("logs_zip", args.logs))}
    args.output.write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps({key: value for key, value in report.items() if key != "rows"}, indent=2))
