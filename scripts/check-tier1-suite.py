#!/usr/bin/env python3
"""Check source inventory and terminal Go JSON evidence for a Tier 1 campaign."""
import argparse
import hashlib
import json
from pathlib import Path
import re

PACKAGE = "js-wf/sim"
TRACE_SKIPS = {
    "TestReplayFaultTrace": "set FAULT_TRACE to replay a saved Tier 1 trace",
    "TestMinimizeFaultTrace": "set FAULT_TRACE to a failing saved Tier 1 trace",
}


def check(events, inventory, seeds, source, regressions):
    if not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("missing exact source commit")
    if not isinstance(seeds, int) or not 1000 <= seeds <= 1_000_000:
        raise ValueError("invalid configured seed count")
    names = inventory.splitlines()
    if not names or len(set(names)) != len(names) or any(
        not re.fullmatch(r"Test\w+", name) or name == "TestMain" for name in names
    ):
        raise ValueError("invalid or empty compiled test inventory")
    if not set(TRACE_SKIPS).issubset(names):
        raise ValueError("compiled inventory omits trace replay/minimization tests")
    traces = regressions.splitlines()
    if not traces or len(set(traces)) != len(traces) or any(
        not re.fullmatch(r"sim/testdata/regressions/[A-Za-z0-9_.-]+\.json", name) for name in traces
    ) or "TestPinnedRegressionCorpus" not in names:
        raise ValueError("missing or invalid source regression inventory")
    required_pins = {"TestPinnedRegressionCorpus/" + Path(name).name for name in traces}
    runs, terminals, output, coverage, package_results = {}, {}, {}, [], []
    for event in events:
        if event.get("Package") != PACKAGE:
            raise ValueError("unexpected package in suite evidence")
        action, name = event.get("Action"), event.get("Test")
        if action == "fail":
            raise ValueError(f"failed test/package: {name}")
        if name:
            if name.split("/", 1)[0] not in names:
                raise ValueError(f"test absent from compiled inventory: {name}")
            if action == "run":
                runs[name] = runs.get(name, 0) + 1
            if action in ("pass", "skip"):
                terminals.setdefault(name, []).append(action)
            if action == "output":
                output[name] = output.get(name, "") + event.get("Output", "")
        elif action in ("pass", "skip"):
            package_results.append(action)
        if action == "output":
            for line in event.get("Output", "").splitlines():
                if line.startswith("TIER1_COVERAGE "):
                    coverage.append(line)
    if package_results != ["pass"]:
        raise ValueError("expected exactly one successful package completion")
    if set(runs) != set(terminals) or not set(names).issubset(runs):
        raise ValueError("missing test execution or terminal evidence")
    for name, actions in terminals.items():
        expected = "skip" if name in TRACE_SKIPS else "pass"
        if runs.get(name) != 1 or actions != [expected]:
            raise ValueError(f"duplicate execution or unexpected result for {name}: {actions}")
        if expected == "skip" and TRACE_SKIPS[name] not in output.get(name, ""):
            raise ValueError(f"missing documented trace-only skip reason: {name}")
    observed_pins = {name for name in terminals if name.startswith("TestPinnedRegressionCorpus/")}
    if observed_pins != required_pins:
        raise ValueError("executed pinned regressions differ from source inventory")
    if len(coverage) != 1:
        raise ValueError("expected exactly one aggregate coverage summary")
    fields = {}
    for token in coverage[0].split()[1:]:
        key, sep, value = token.partition("=")
        if not sep or key in fields:
            raise ValueError("malformed or duplicate coverage field")
        fields[key] = value
    required = ("model_version", "seeds_per_workload", "generated_schedules",
                "scheduler_choices", "transport_events", "virtual_ms_max",
                "virtual_buckets_zero", "under_1s", "under_1m", "at_least_1m")
    try:
        counts = {key: int(fields[key]) for key in required}
    except (KeyError, ValueError) as error:
        raise ValueError("missing or invalid aggregate coverage counts") from error
    if any(value < 0 for value in counts.values()):
        raise ValueError("negative aggregate coverage count")
    if counts["seeds_per_workload"] != seeds:
        raise ValueError("coverage seed configuration differs from requested campaign")
    if any(counts[key] == 0 for key in (
        "model_version", "generated_schedules", "scheduler_choices", "transport_events"
    )):
        raise ValueError("empty aggregate campaign coverage")
    if sum(counts[key] for key in ("virtual_buckets_zero", "under_1s", "under_1m", "at_least_1m")) != counts["generated_schedules"]:
        raise ValueError("virtual-time buckets do not account for all generated schedules")
    return {
        "source": source,
        "package": PACKAGE,
        "inventory_sha256": hashlib.sha256(inventory.encode()).hexdigest(),
        "top_level_pass": len(names) - len(TRACE_SKIPS),
        "trace_only_skips": sorted(TRACE_SKIPS),
        "pinned_regressions_pass": len(required_pins),
        "regression_inventory_sha256": hashlib.sha256(regressions.encode()).hexdigest(),
        "aggregate_counts": counts,
        "scope": "Compiled test inventory completed; seed count is configuration, not independent per-workload seed coverage proof.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--regressions", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--seeds", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.events.open() as stream:
        events = (json.loads(line) for line in stream if line.strip())
        report = check(events, args.inventory.read_text(), args.seeds, args.source.read_text().strip(), args.regressions.read_text())
    report["events_sha256"] = hashlib.sha256(args.events.read_bytes()).hexdigest()
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Verified Tier 1 inventory: {report['top_level_pass']} passes, two trace-only skips")


if __name__ == "__main__":
    main()
