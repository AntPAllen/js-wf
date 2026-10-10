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


def check(events, inventory, seeds, source, regressions, seeded_inventory=None, *, package_count=1):
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
    seeded_names = None
    if seeded_inventory is not None:
        seeded_names = seeded_inventory.splitlines()
        if not seeded_names or len(set(seeded_names)) != len(seeded_names) or any(
            not re.fullmatch(r"Test\w+", name) for name in seeded_names
        ) or not set(seeded_names).issubset(names):
            raise ValueError("invalid or empty source seeded-test inventory")
    seed_records = {}
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
            if "WARNING: DATA RACE" in event.get("Output", ""):
                raise ValueError("race detector warning in suite evidence")
            for line in event.get("Output", "").splitlines():
                if "TIER1_SEEDS " in line:
                    tokens = line.split("TIER1_SEEDS ", 1)[1].split()
                    fields = {}
                    for token in tokens:
                        key, sep, value = token.partition("=")
                        if not sep or key in fields:
                            raise ValueError("malformed or duplicate per-workload seed field")
                        fields[key] = value
                    if set(fields) != {"test", "first", "last", "completed", "requested"} or fields["test"] != name:
                        raise ValueError("seed evidence identity or schema mismatch")
                    seed_records.setdefault(name, []).append(fields)
                if line.startswith("TIER1_COVERAGE "):
                    coverage.append(line)
    if package_results != ["pass"] * package_count:
        raise ValueError("expected one successful completion for every package process")
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
    if len(coverage) != package_count:
        raise ValueError("expected one aggregate coverage summary per package process")
    process_counts = [check_coverage(line, seeds) for line in coverage]
    counts = dict(process_counts[0])
    for current in process_counts[1:]:
        if current["model_version"] != counts["model_version"]:
            raise ValueError("processes use different model versions")
        for key in counts:
            if key == "virtual_ms_max":
                counts[key] = max(counts[key], current[key])
            elif key not in ("model_version", "seeds_per_workload"):
                counts[key] += current[key]
    seed_proof = None
    if seeded_names is not None:
        if set(seed_records) != set(seeded_names):
            raise ValueError("seed evidence differs from source seeded-test inventory")
        for name, records in seed_records.items():
            if len(records) != 1:
                raise ValueError(f"duplicate per-workload seed evidence: {name}")
            fields = records[0]
            try:
                counts_for_test = {key: int(fields[key]) for key in ("first", "last", "completed", "requested")}
            except ValueError as error:
                raise ValueError("invalid per-workload seed count") from error
            if counts_for_test != {"first": 1, "last": seeds, "completed": seeds, "requested": seeds}:
                raise ValueError(f"incomplete per-workload seed range: {name}")
        seed_proof = {
            "inventory_sha256": hashlib.sha256(seeded_inventory.encode()).hexdigest(),
            "workloads": len(seeded_names), "first": 1, "last": seeds,
            "completed_bodies": seeds * len(seeded_names),
            "tests": sorted(seeded_names),
        }
    return {
        "source": source,
        "package": PACKAGE,
        "inventory_sha256": hashlib.sha256(inventory.encode()).hexdigest(),
        "top_level_pass": len(names) - len(TRACE_SKIPS),
        "trace_only_skips": sorted(TRACE_SKIPS),
        "pinned_regressions_pass": len(required_pins),
        "regression_inventory_sha256": hashlib.sha256(regressions.encode()).hexdigest(),
        "aggregate_counts": counts,
        "per_workload_seed_proof": seed_proof,
        "scope": ("Compiled inventory and every source-inventoried seeded loop completed the exact requested contiguous seed range."
                  if seed_proof else "Compiled test inventory completed; seed count is configuration, not independent per-workload seed coverage proof."),
    }


def check_coverage(line, seeds):
    fields = {}
    for token in line.split()[1:]:
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
    return counts


def check_parts(parts, inventory, seeds, source, regressions, seeded_inventory):
    """Check disjoint actual processes; never synthesize a package completion."""
    names = set(inventory.splitlines())
    if not 2 <= len(parts) <= 32:
        raise ValueError("invalid package process count")
    assigned = set()
    for local_inventory, _ in parts:
        local = local_inventory.splitlines()
        if not local or len(local) != len(set(local)) or set(local) - names or assigned.intersection(local):
            raise ValueError("invalid or overlapping process inventory")
        assigned.update(local)
    if assigned != names:
        raise ValueError("process union omits compiled tests")

    def events():
        for local_inventory, rows in parts:
            local = set(local_inventory.splitlines())
            roots, completed, coverage = set(), [], 0
            for event in rows:
                name, action = event.get("Test"), event.get("Action")
                if name:
                    if name.split("/", 1)[0] not in local:
                        raise ValueError("test ran in the wrong process")
                    if action == "run" and "/" not in name:
                        roots.add(name)
                elif action in ("pass", "skip", "fail"):
                    completed.append(action)
                coverage += sum(line.startswith("TIER1_COVERAGE ") for line in event.get("Output", "").splitlines())
                yield event
            if roots != local or completed != ["pass"] or coverage != 1:
                raise ValueError("incomplete process execution, completion or coverage")

    report = check(events(), inventory, seeds, source, regressions, seeded_inventory, package_count=len(parts))
    report["package_processes"] = len(parts)
    report["disjoint_compiled_inventory_union"] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    evidence = parser.add_mutually_exclusive_group(required=True)
    evidence.add_argument("--events", type=Path)
    evidence.add_argument("--parts", type=Path, help="JSON manifest of per-process inventory/event paths")
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--regressions", type=Path, required=True)
    parser.add_argument("--seeded-inventory", type=Path)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--seeds", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from contextlib import ExitStack
    with ExitStack() as stack:
        common = (args.inventory.read_text(), args.seeds, args.source.read_text().strip(), args.regressions.read_text(), args.seeded_inventory.read_text() if args.seeded_inventory else None)
        if args.parts:
            if not args.seeded_inventory:
                parser.error("partitioned evidence requires source seeded inventory")
            manifest = json.loads(args.parts.read_text())
            if not isinstance(manifest, list) or any(set(row) != {"inventory", "events"} for row in manifest):
                raise ValueError("invalid process manifest")
            parts = []
            for row in manifest:
                stream = stack.enter_context(Path(row["events"]).open())
                parts.append((Path(row["inventory"]).read_text(), (json.loads(line) for line in stream if line.strip())))
            report = check_parts(parts, *common)
            report["parts_manifest_sha256"] = hashlib.sha256(args.parts.read_bytes()).hexdigest()
            report["part_events_sha256"] = [hashlib.sha256(Path(row["events"]).read_bytes()).hexdigest() for row in manifest]
        else:
            stream = stack.enter_context(args.events.open())
            report = check((json.loads(line) for line in stream if line.strip()), *common)
            report["events_sha256"] = hashlib.sha256(args.events.read_bytes()).hexdigest()
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Verified Tier 1 inventory: {report['top_level_pass']} passes, two trace-only skips")


if __name__ == "__main__":
    main()
