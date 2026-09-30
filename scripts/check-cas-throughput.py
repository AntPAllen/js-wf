#!/usr/bin/env python3
"""Compare fixed CAS workloads on one runner; preserve every measurement."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys


def validate(report):
    expected = {"replicas": 3, "storage": "file", "parallel_workers": 1000,
                "journal_messages": 110000, "journal_subjects": 1001}
    for name, value in expected.items():
        if report.get(name) != value:
            raise ValueError(f"invalid benchmark {name}: {report.get(name)!r}")
    placement = report.get("hot_placement", "pinned")
    if placement not in ("pinned", "leader", "follower"):
        raise ValueError("invalid hot placement")
    if placement != "pinned":
        before, after = report.get("hot_topology_before"), report.get("hot_topology_after")
        if not isinstance(before, dict) or before != after:
            raise ValueError("hot topology missing or changed across sample")
        leader, client = before.get("leader"), before.get("client_server")
        if not isinstance(leader, str) or not leader or not isinstance(client, str) or not client or before.get("client_is_leader") is not (placement == "leader") or (leader == client) != (placement == "leader"):
            raise ValueError("hot placement does not match topology")
    for name, invocations, each in (("hot", 1, 10000), ("parallel", 1000, 100)):
        sample = report[name]
        for field, expected_value in (("invocations", invocations), ("entries_each", each),
                                      ("appends", invocations * each)):
            if sample.get(field) != expected_value:
                raise ValueError(f"invalid {name} {field}: {sample.get(field)!r}")
        rate, elapsed = sample["appends_per_second"], sample["elapsed_ms"]
        if not all(isinstance(x, (int, float)) and math.isfinite(x) and x > 0
                   for x in (rate, elapsed)):
            raise ValueError(f"invalid {name} timing")
        if not math.isclose(rate, sample["appends"] * 1000 / elapsed, rel_tol=1e-6):
            raise ValueError(f"inconsistent {name} rate and elapsed time")
    if not report.get("go_version") or not report.get("nats_server_version"):
        raise ValueError("missing runtime versions")


def compare(baselines, candidates, minimum=0.8):
    if len(baselines) != len(candidates) or len(baselines) < 3 or len(baselines) % 2 == 0:
        raise ValueError("require equal odd sample counts, at least three")
    reference = baselines[0]
    for report in baselines + candidates:
        validate(report)
        if report.get("hot_placement", "pinned") != reference.get("hot_placement", "pinned"):
            raise ValueError("hot placement mismatch")
        for field in ("go_version", "nats_server_version"):
            if report[field] != reference[field]:
                raise ValueError(f"runtime mismatch: {field}")
    if not 0 < minimum <= 1:
        raise ValueError("invalid minimum throughput ratio")
    results = {}
    for name in ("hot", "parallel"):
        baseline = statistics.median(r[name]["appends_per_second"] for r in baselines)
        candidate = statistics.median(r[name]["appends_per_second"] for r in candidates)
        results[name] = {"baseline_median_appends_per_second": baseline,
                         "candidate_median_appends_per_second": candidate,
                         "ratio": candidate / baseline,
                         "passed": candidate >= baseline * minimum}
    return results


def measurement(binary, output, log, timeout, placement="pinned"):
    # A successful process that emits no report must not reuse an earlier file.
    output.unlink(missing_ok=True)
    # Separate process groups let a timeout stop the benchmark's children too.
    with log.open("wb") as stream:
        process = subprocess.Popen([str(binary), "-output", str(output), "-hot-placement", placement],
                                   stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise RuntimeError(f"benchmark timed out; see {log}") from None
    if code:
        raise RuntimeError(f"benchmark exited {code}; see {log}")
    report = json.loads(output.read_text())
    validate(report)
    if report.get("hot_placement", "pinned") != placement:
        raise ValueError("benchmark ignored requested hot placement")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hot-placement", choices=("pinned", "leader", "follower"), default="pinned")
    parser.add_argument("--harness-sha256")
    parser.add_argument("--rounds", type=int, default=3, choices=(3, 5))
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    binaries = {"baseline": args.baseline_binary.resolve(), "candidate": args.candidate_binary.resolve()}
    summary = {"minimum_ratio": 0.8, "hot_placement": args.hot_placement,
               "harness_sha256": args.harness_sha256, "rounds": args.rounds,
               "baseline_revision": args.baseline_revision,
               "candidate_revision": args.candidate_revision,
               "platform": platform.platform(), "cpu_count": os.cpu_count(),
               "gomaxprocs_env": os.environ.get("GOMAXPROCS"),
               "binary_sha256": {k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in binaries.items()},
               "order": [], "passed": False}
    samples = {"baseline": [], "candidate": []}
    code = 1
    try:
        for round_number in range(1, args.rounds + 1):
            order = ("baseline", "candidate") if round_number % 2 else ("candidate", "baseline")
            for name in order:
                label = f"{name}-{round_number}"
                summary["order"].append(label)
                samples[name].append(measurement(binaries[name], args.output / f"{label}.json",
                                                 args.output / f"{label}.log", args.timeout, args.hot_placement))
                print(f"completed {label}", flush=True)
        summary["results"] = compare(samples["baseline"], samples["candidate"])
        summary["passed"] = all(r["passed"] for r in summary["results"].values())
        code = 0 if summary["passed"] else 1
    except (ValueError, KeyError, OSError, RuntimeError) as error:
        summary["error"] = str(error)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return code


if __name__ == "__main__":
    sys.exit(main())
