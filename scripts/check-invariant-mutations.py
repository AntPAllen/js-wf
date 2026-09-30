#!/usr/bin/env python3
"""Require semantic detection of six isolated production mutations.

Go overlays leave the checkout untouched. Each fixture must pass without the
mutation before its intentional failure can count. These focused contracts do
not replace the implementation plan's full mixed-chaos mutation campaign.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
MUTATIONS = [
    dict(name="missing_cas", file="journal/journal.go",
         before="p.js.Publish(ctx, subject, data, jetstream.WithExpectLastSequencePerSubject(expected))",
         after="p.js.Publish(ctx, subject, data)",
         package="./integration", test="TestJournalCASTenThousandRacesWithLeaderRestarts",
         markers=["wins=2 stales=0"]),
    dict(name="independent_worker_leases", file="lease/lease.go",
         before="key := identity.Key(typ, id)",
         after='key := identity.Key(typ, id) + "." + worker',
         package="./integration", test="TestLeaseFenceAndStartRepair",
         markers=["second acquire: <nil>"]),
    dict(name="invocation_purged_first", file="retention/purge.go",
         before='\tif err := stage("marker"); err != nil {',
         after='\tif err := port.PurgeSubject(ctx, "WF_INV", identity.InvocationSubject(typ, id), input.Sequence+1); err != nil {\n\t\treturn err\n\t}\n\tif err := stage("marker"); err != nil {',
         package="./retention", test="TestPurgeResumesAfterEveryStage",
         markers=["resume after marker: invocation not found"]),
    dict(name="missing_determinism_guard", file="wf/context.go",
         before='if (got.Kind != kind && !(kind == "run" && got.Kind == "")) || got.Name != name || got.InputHash != want.InputHash {',
         after="if false {", package="./sim", test="TestSeededWorkflowReplay",
         markers=["escaped guard: effects=3 err=<nil>"]),
    dict(name="missing_run_message_id", file="client/client.go",
         before="p.js.Publish(ctx, subject, data, jetstream.WithMsgID(dedupID))",
         after="p.js.Publish(ctx, subject, data)",
         package="./integration", test="TestConcurrentEnqueueSameMessageID",
         markers=["retained run messages=", "State:{Msgs:64 ", "err=<nil>"]),
    dict(name="skipped_start_reconciler", file="reconcile/starts.go",
         before="func (s *StartScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {",
         after="func (s *StartScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {\n\tif true { return ScanResult{NextSequence: next}, nil }",
         package="./sim", test="TestSkippedStartReconcilerMutationIsDetected",
         markers=["repair missing start:", "Reenqueued:0", "err=<nil>"]),
]


def run_fixture(mutation, output, phase, overlay=None):
    command = ["go", "test", "-json", mutation["package"],
               "-run", "^" + mutation["test"] + "$", "-count=1", "-timeout=3m"]
    if overlay:
        command.insert(2, "-overlay=" + str(overlay))
    environment = dict(os.environ, FAULT_SEED="42", SIM_SEEDS="1000",
                       WF_CAS_RACE_ROUNDS="1000",
                       FAULT_TRACE_OUT=str(output / (mutation["name"] + "-" + phase + "-trace.json")))
    # A pinned replay override must not replace the selected seeded fixture.
    environment.pop("FAULT_TRACE", None)
    log = output / (mutation["name"] + "-" + phase + ".jsonl")
    started = time.monotonic()
    with log.open("w") as stream:
        process = subprocess.Popen(command, cwd=ROOT, env=environment,
                                   stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            process.wait(timeout=200)
        except subprocess.TimeoutExpired:
            # Go can have a live test executable after its parent is killed.
            # Terminate this isolated fixture's group before starting another.
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    events = []
    for line in log.read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    top_level = [event.get("Action") for event in events
                 if event.get("Test") == mutation["test"]
                 and event.get("Action") in ("pass", "fail", "skip")]
    failed_output = "".join(event.get("Output", "") for event in events
                            if event.get("Test", "").split("/")[0] == mutation["test"])
    passed = process.returncode == 0 and top_level == ["pass"]
    detected = (process.returncode != 0 and top_level == ["fail"]
                and all(marker in failed_output for marker in mutation["markers"])
                and "panic: test timed out" not in log.read_text())
    return dict(returncode=process.returncode, passed=passed, detected=detected,
                elapsed_seconds=round(time.monotonic() - started, 3), log=str(log))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mutation", choices=[m["name"] for m in MUTATIONS])
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    selected = [m for m in MUTATIONS if not args.mutation or m["name"] == args.mutation]
    report = dict(scope="focused production mutations; full mixed-chaos gate remains open",
                  head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  cas_rounds=1000, seeds_per_modeled_workload=1000, mutations=[])
    success = True
    with tempfile.TemporaryDirectory(prefix="js-wf-mutations-") as temporary:
        # A build error and a different test failure must never count as a kill.
        control = MUTATIONS[1]
        controls = [
            ("compile_error", ROOT / control["file"], "package lease\ninvalid Go syntax\n"),
            ("unrelated_failure", ROOT / "integration/core_test.go",
             (ROOT / "integration/core_test.go").read_text().replace(
                 "func TestLeaseFenceAndStartRepair(t *testing.T) {",
                 'func TestLeaseFenceAndStartRepair(t *testing.T) {\n\tt.Fatal("unrelated runner control")', 1)),
        ]
        report["negative_controls"] = []
        for name, source, contents in controls:
            changed = Path(temporary) / (name + ".go")
            changed.write_text(contents)
            overlay = Path(temporary) / (name + ".json")
            overlay.write_text(json.dumps({"Replace": {str(source): str(changed)}}))
            result = run_fixture(control, args.output, name, overlay)
            report["negative_controls"].append(dict(name=name, **result))
            if result["returncode"] == 0 or result["detected"]:
                raise RuntimeError("runner counted or missed its negative control: " + name)
        for mutation in selected:
            source = ROOT / mutation["file"]
            original = source.read_text()
            entry = dict(name=mutation["name"], file=mutation["file"], test=mutation["test"],
                         source_sha256=hashlib.sha256(original.encode()).hexdigest())
            report["mutations"].append(entry)
            try:
                if original.count(mutation["before"]) != 1:
                    raise RuntimeError("mutation anchor must occur exactly once")
                entry["baseline"] = run_fixture(mutation, args.output, "baseline")
                if not entry["baseline"]["passed"]:
                    raise RuntimeError("unmodified fixture did not pass; detection unproven")
                changed = Path(temporary) / (mutation["name"] + ".go")
                changed.write_text(original.replace(mutation["before"], mutation["after"], 1))
                overlay = Path(temporary) / (mutation["name"] + ".json")
                overlay.write_text(json.dumps({"Replace": {str(source): str(changed)}}))
                entry["mutated"] = run_fixture(mutation, args.output, "mutated", overlay)
                if not entry["mutated"]["detected"]:
                    raise RuntimeError("expected semantic failure missing; mutation not detected")
                print(mutation["name"] + ": baseline passed; production mutation detected", flush=True)
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                entry["error"] = str(error)
                success = False
                print(mutation["name"] + ": " + str(error), file=sys.stderr, flush=True)
            (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
