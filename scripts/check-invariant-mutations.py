#!/usr/bin/env python3
"""Require semantic detection of six isolated production mutations.

Go overlays leave the checkout untouched. Each fixture must pass without the
mutation before its intentional failure can count. These focused contracts do
not replace the implementation plan's full mixed-chaos mutation campaign.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
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


def check_sustained_evidence(events, mutation, prefix):
    """Require the completed original row even when the guard later fails."""
    output = "".join(e.get("Output", "") for e in events if e.get("Test") == mutation["test"])
    admission = re.findall(r"MIXED_SUSTAINED_ADMISSION (\{[^\n]+\})", output)
    if len(admission) != 1:
        raise ValueError("missing or duplicate same-store sustained admission")
    admission = json.loads(admission[0])
    seconds = {"35s": 35, "10m": 600}[mutation["sustained_duration"]]
    terminals = [event for event in events if event.get("Test") == mutation["test"] and event.get("Action") in ("pass", "fail", "skip")]
    if len(terminals) != 1 or terminals[0]["Action"] not in ("pass", "fail") or terminals[0].get("Elapsed", 0) < seconds:
        raise ValueError("named sustained test did not actually execute for its duration")
    if (admission["seed"] != 42 or admission["mode"] != mutation["environment"]["WF_SUSTAINED_MUTATION"]
            or admission["duration_ns"] != seconds * 10**9 or admission["elapsed_ns"] < seconds * 10**9
            or admission["release_duration"] is not (seconds == 600) or admission["same_stores"] is not True):
        raise ValueError("incorrect source cohort, seed, mode or actual sustained duration")
    batches, faults = admission["batches"], admission["faults"]
    retained = admission["retained"]
    if batches <= 0 or faults != (seconds - 1) // 30 or admission["invocation_cutoff"] <= 0:
        raise ValueError("incomplete sustained batches or repeated leader kills")
    if any(retained[k] != batches * 28 for k in ("Invocations", "Journals", "Terminal")) or retained["Entries"] <= batches * 28:
        raise ValueError("incorrect sustained retained cohort")
    if len(re.findall(r"MIXED_SUSTAINED_PRESERVED cutoff=" + str(admission["invocation_cutoff"]) + r"\b", output)) != 1:
        raise ValueError("original sustained cohort was not preserved after challenge")
    # The actual row checks remain responsible for their physical drain and
    # histories; admission occurs only after those gates and fleet join. Match
    # all six measured class counts and both p99 budgets in the original log.
    def duration_seconds(value):
        parts = re.findall(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|h|m|s)", value)
        if "".join(number + unit for number, unit in parts) != value or not parts:
            raise ValueError("invalid measured duration")
        units = dict(ns=1e-9, us=1e-6, ms=.001, s=1, m=60, h=3600)
        units["µs"] = 1e-6
        return sum(float(number) * units[unit] for number, unit in parts)
    cells = {}
    for typ, each in zip(("matrixshort", "matrixtimer", "matrixsignal", "matrixfanout", "matrixchild", "matrixgrandchild"), (4, 3, 2, 1, 6, 12)):
        cell = re.findall(r"MATRIX_CELL type=" + typ + r" invocations=(\d+) terminal_p99=(\S+)", output)
        progress = re.findall(r"MATRIX_PROGRESS type=" + typ + r" enabled_events=(\d+) progress_p99=(\S+)", output)
        if len(cell) != 1 or len(progress) != 1 or int(cell[0][0]) != batches * each or int(progress[0][0]) <= 0:
            raise ValueError("missing or incorrect sustained workload " + typ)
        terminal, next_entry = duration_seconds(cell[0][1]), duration_seconds(progress[0][1])
        if terminal >= 30 or next_entry >= 30:
            raise ValueError("sustained workload exceeds the unchanged p99 gate")
        cells[typ] = dict(invocations=int(cell[0][0]), terminal_p99_seconds=terminal, progress_p99_seconds=next_entry)
        cells[typ]["progress_events"] = int(progress[0][0])
    original_faults = json.loads(Path(prefix + "-faults.json").read_text())
    if original_faults["seed"] != 42 or duration_seconds(original_faults["duration"]) != seconds or len(original_faults["faults"]) != faults:
        raise ValueError("original fault artifact disagrees with admission")
    for fault in original_faults["faults"]:
        if fault.get("error") or not fault.get("killed") or not fault.get("healed") or fault["healed"] < fault["killed"]:
            raise ValueError("unconfirmed original leader heal")
    for suffix in ("-history.jsonl", "-dispatch.jsonl"):
        records = [json.loads(line) for line in Path(prefix + suffix).read_text().splitlines() if line.strip()]
        if not records or any(not isinstance(record, dict) for record in records):
            raise ValueError("missing or malformed original sustained artifact " + suffix)
    def timestamp_ns(value):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("artifact timestamp has no timezone")
        delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
        fraction = re.search(r"\.(\d+)", value)
        remainder = int((fraction[1] + "000000000")[6:9]) if fraction else 0
        return (delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000 + remainder
    samples = json.loads(Path(prefix + "-latencies.json").read_text())
    if not isinstance(samples, list) or not samples:
        raise ValueError("missing original latency samples")
    raw = {typ: {"terminal": [], "progress": [], "ids": set()} for typ in cells}
    for sample in samples:
        typ, ident, delay = sample["type"], sample["id"], sample["delay_ns"]
        if typ not in raw or not ident or not isinstance(delay, int) or delay < 0 or timestamp_ns(sample["observed"]) - timestamp_ns(sample["enabled"]) != delay:
            raise ValueError("raw latency sample disagrees with its enabling/observed times")
        kind = "terminal" if sample["event"] == "terminal" else "progress"
        raw[typ][kind].append(delay)
        if kind == "terminal":
            raw[typ]["ids"].add(ident)
    for typ, cell in cells.items():
        values = raw[typ]
        if len(values["terminal"]) != cell["invocations"] or len(values["ids"]) != cell["invocations"] or len(values["progress"]) != cell["progress_events"]:
            raise ValueError("raw sustained cohort disagrees with measured counts")
        for kind, key in (("terminal", "terminal_p99_seconds"), ("progress", "progress_p99_seconds")):
            ordered = sorted(values[kind])
            p99 = ordered[(len(ordered)*99+99)//100-1]
            if p99 >= 30*10**9 or abs(p99 - cell[key]*10**9) > 1:
                raise ValueError("raw sustained p99 disagrees with measured gate")
    return dict(accepted=True, admission=admission, cells=cells)


def run_fixture(mutation, output, phase, overlay=None):
    command = ["go", "test", "-p=1", "-json", mutation["package"],
               "-run", "^" + mutation["test"] + "$", "-count=1", "-timeout=" + mutation.get("timeout", "3m")]
    if overlay:
        command.insert(2, "-overlay=" + str(overlay))
    environment = dict(os.environ, FAULT_SEED="42", SIM_SEEDS="1000",
                       WF_CAS_RACE_ROUNDS="1000",
                       FAULT_TRACE_OUT=str(output / (mutation["name"] + "-" + phase + "-trace.json")))
    environment.update(mutation.get("environment", {}))
    if mutation.get("sustained_duration"):
        environment["MATRIX_ARTIFACT_PREFIX"] = str(output / (mutation["name"] + "-" + phase + "-matrix"))
    # A pinned replay override must not replace the selected seeded fixture.
    environment.pop("FAULT_TRACE", None)
    log = output / (mutation["name"] + "-" + phase + ".jsonl")
    started = time.monotonic()
    with log.open("w") as stream:
        process = subprocess.Popen(command, cwd=ROOT, env=environment,
                                   stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            process.wait(timeout=mutation.get("process_timeout", 200))
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
    unrelated_control_failed = (process.returncode != 0 and top_level == ["fail"]
                                and "unrelated runner control" in failed_output
                                and "panic: test timed out" not in log.read_text())
    sustained = None
    if mutation.get("sustained_duration"):
        try:
            sustained = check_sustained_evidence(events, mutation, environment["MATRIX_ARTIFACT_PREFIX"])
        except (ValueError, OSError, KeyError, TypeError) as error:
            sustained = {"accepted": False, "error": str(error)}
        passed = passed and sustained["accepted"]
        detected = detected and sustained["accepted"]
    return dict(returncode=process.returncode, passed=passed, detected=detected,
                sustained=sustained,
                unrelated_control_failed=unrelated_control_failed,
                elapsed_seconds=round(time.monotonic() - started, 3), log=str(log))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mutation", choices=[m["name"] for m in MUTATIONS])
    parser.add_argument("--mixed-determinism", action="store_true", help="challenge I4 through a live mixed worker/leader-kill workload")
    parser.add_argument("--mixed-leases", action="store_true", help="challenge lease exclusion after a real leader kill in the live mixed cohort")
    parser.add_argument("--mixed-cas", action="store_true", help="race two gated production journal appends in the live mixed leader-kill cohort")
    parser.add_argument("--mixed-enqueue", action="store_true", help="challenge retained dispatch deduplication in the live mixed leader-kill cohort")
    parser.add_argument("--mixed-start-repair", action="store_true", help="challenge missing-start repair in the live mixed leader-kill cohort")
    parser.add_argument("--mixed-purge", action="store_true", help="challenge retirement ordering and recovery in the live mixed leader-kill cohort")
    parser.add_argument("--sustained", choices=["35s", "10m"], help="run the selected mixed challenge after the same-store sustained journal-leader row; 35s is smoke only")
    args = parser.parse_args()
    if args.sustained and sum((args.mixed_leases,args.mixed_determinism,args.mixed_cas,args.mixed_enqueue,args.mixed_start_repair,args.mixed_purge)) != 1:
        parser.error("--sustained requires exactly one mixed category")
    if sum((args.mixed_leases,args.mixed_determinism,args.mixed_cas,args.mixed_enqueue,args.mixed_start_repair,args.mixed_purge))>1:
        parser.error("select one mixed challenge")
    if args.mixed_purge and args.mutation not in (None,"invocation_purged_first"):
        parser.error("--mixed-purge selects only invocation_purged_first")
    if args.mixed_start_repair and args.mutation not in (None,"skipped_start_reconciler"):
        parser.error("--mixed-start-repair selects only skipped_start_reconciler")
    if args.mixed_enqueue and args.mutation not in (None,"missing_run_message_id"):
        parser.error("--mixed-enqueue selects only missing_run_message_id")
    if args.mixed_cas and args.mutation not in (None,"missing_cas"):
        parser.error("--mixed-cas selects only missing_cas")
    if args.mixed_leases and args.mutation not in (None, "independent_worker_leases"):
        parser.error("--mixed-leases selects only independent_worker_leases")
    if args.mixed_determinism and args.mutation not in (None, "missing_determinism_guard"):
        parser.error("--mixed-determinism selects only missing_determinism_guard")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    selected = [m for m in MUTATIONS if not args.mutation or m["name"] == args.mutation]
    if args.mixed_determinism:
        mutation = dict(next(m for m in MUTATIONS if m["name"] == "missing_determinism_guard"))
        mutation.update(package="./integration", test="TestMixedDeterminismMutationAfterJournalLeaderKill",
                        fixture_file="integration/mixed_determinism_mutation_test.go",
                        environment={"WF_MIXED_DETERMINISM_MUTATION": "1"},
                        markers=["MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1", "MIXED_GUARD_FAULT", "signal=SIGKILL", "MIXED_GUARD_COHORT shorts=4 timers=3 signals=2 fanout=1 children=6 grandchildren=12 terminal=28", "MIXED_MUTATION_ESCAPE invariant=I4 effects=1 result=42 error=<nil> terminal=Completed"])
        selected = [mutation]
    if args.mixed_leases:
        mutation = dict(next(m for m in MUTATIONS if m["name"] == "independent_worker_leases"))
        mutation.update(package="./integration", test="TestMixedLeaseMutationAfterJournalLeaderKill",
                        fixture_file="integration/mixed_determinism_mutation_test.go",
                        environment={"WF_MIXED_LEASE_MUTATION": "1"},
                        markers=["MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1", "MIXED_GUARD_FAULT", "signal=SIGKILL", "MIXED_LEASE_ADMISSION replacement_shorts_pending=4 owner=mixed-guard-after", "MIXED_GUARD_COHORT shorts=4 timers=3 signals=2 fanout=1 children=6 grandchildren=12 terminal=28", "MIXED_MUTATION_ESCAPE category=independent_worker_leases admitted=1 rival_epoch=", "terminal=Completed"])
        selected = [mutation]
    if args.mixed_cas:
        mutation = dict(next(m for m in MUTATIONS if m["name"] == "missing_cas"))
        mutation.update(package="./integration", test="TestMixedCASMutationAfterJournalLeaderKill",
                        fixture_files=["integration/mixed_determinism_mutation_test.go", "integration/mixed_cas_mutation_test.go", "integration/journal_cas_race_scale_test.go"],
                        environment={"WF_MIXED_CAS_MUTATION": "1"},
                        markers=["MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1", "MIXED_GUARD_FAULT", "signal=SIGKILL", "MIXED_CAS_ADMISSION contenders=2", "MIXED_CAS_RETAINED", "MIXED_CAS_CHECKER_REJECTED", "MIXED_MUTATION_ESCAPE category=missing_cas acknowledged_winners=2 stale_rejections=0 same_index=2 retained_duplicates=2 checker_rejected=true"])
        selected = [mutation]
    if args.mixed_enqueue:
        mutation = dict(next(m for m in MUTATIONS if m["name"] == "missing_run_message_id"))
        mutation.update(package="./integration", test="TestMixedEnqueueMutationAfterJournalLeaderKill",
                        fixture_files=["integration/mixed_determinism_mutation_test.go", "integration/mixed_enqueue_mutation_test.go"],
                        environment={"WF_MIXED_ENQUEUE_MUTATION": "1"},
                        markers=["MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1", "MIXED_GUARD_FAULT", "signal=SIGKILL", "MIXED_ENQUEUE_RAW_RECEIPT", "MIXED_ENQUEUE_ADMISSION acknowledged_calls=64", "MIXED_ENQUEUE_RETAINED_DUPLICATES acknowledged_calls=64 retained_at_least64=true id_headers=0", "MIXED_GUARD_COHORT shorts=4 timers=3 signals=2 fanout=1 children=6 grandchildren=12 terminal=28", "MIXED_MUTATION_ESCAPE category=missing_run_message_id acknowledged_calls=64 retained_at_least64=true terminal=28 retained="])
        selected = [mutation]
    if args.mixed_start_repair:
        mutation = dict(next(m for m in MUTATIONS if m["name"] == "skipped_start_reconciler"))
        mutation.update(package="./integration", test="TestMixedStartRepairMutationAfterJournalLeaderKill",
                        fixture_files=["integration/mixed_determinism_mutation_test.go", "integration/mixed_start_repair_mutation_test.go", "integration/start_retry_repair_test.go"],
                        environment={"WF_MIXED_START_REPAIR_MUTATION": "1"},
                        markers=["MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1", "MIXED_START_GAP_ADMISSION", "MIXED_GUARD_FAULT", "signal=SIGKILL", "MIXED_START_RAW_INVOCATION", "MIXED_START_SCAN inspected=0 reenqueued=0 retained_dispatch=0", "MIXED_GUARD_COHORT shorts=4 timers=3 signals=2 fanout=1 children=6 grandchildren=12 terminal=28", "MIXED_MUTATION_ESCAPE category=skipped_start_reconciler retained_invocations=29 original_terminal=28 orphan_journal=absent orphan_terminal=absent"])
        selected = [mutation]
    if args.mixed_purge:
        mutation = dict(next(m for m in MUTATIONS if m["name"] == "invocation_purged_first"))
        mutation.update(package="./integration", test="TestMixedPurgeMutationAfterJournalLeaderKill",
                        fixture_files=["integration/mixed_determinism_mutation_test.go", "integration/mixed_purge_mutation_test.go"],
                        environment={"WF_MIXED_PURGE_MUTATION": "1"},
                        markers=["MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1", "MIXED_GUARD_FAULT", "signal=SIGKILL", "MIXED_GUARD_COHORT shorts=4 timers=3 signals=2 fanout=1 children=6 grandchildren=12 terminal=28", "MIXED_PURGE_RAW_INVOCATION", "MIXED_PURGE_RETAINED_JOURNAL", "MIXED_PURGE_RETAINED_STATE", "MIXED_MUTATION_ESCAPE category=invocation_purged_first cut_before=signals invocation=absent journal=unchanged terminal=unchanged purge_marker=retained retry=invocation_not_found original_terminal=28"])
        selected = [mutation]
    if args.sustained:
        mode = next(name for name, enabled in (("determinism",args.mixed_determinism), ("leases",args.mixed_leases), ("cas",args.mixed_cas), ("enqueue",args.mixed_enqueue), ("start-repair",args.mixed_start_repair), ("purge",args.mixed_purge)) if enabled)
        mutation = selected[0]
        mutation["test"] = "TestSustainedMixedMutationAfterJournalLeaderKills"
        mutation["environment"] = {"WF_SUSTAINED_MUTATION": mode, "WF_MATRIX_CHAOS": "1", "WF_MATRIX_DURATION": args.sustained}
        mutation["sustained_duration"] = args.sustained
        mutation["timeout"] = "19m" if args.sustained == "10m" else "10m"
        mutation["process_timeout"] = 1160 if args.sustained == "10m" else 620
        fixture_files = mutation.get("fixture_files", [mutation.get("fixture_file")])
        mutation.pop("fixture_file", None)
        mutation["fixture_files"] = sorted(set(fixture_files + ["integration/sustained_mixed_mutation_test.go", "integration/mixed_matrix_leader_test.go", "integration/mixed_matrix_metadata_contract_test.go", "integration/mixed_matrix_queue_diagnostics_test.go", "integration/mixed_matrix_stall_diagnostics_test.go"]))
    report = dict(scope="focused production mutations; full mixed-chaos gate remains open",
                  head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  cas_rounds=1000, seeds_per_modeled_workload=1000, mutations=[])
    if args.sustained:
        report["source_inventory_sha256s"] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                               for path in sorted(ROOT.rglob("*.go")) if ".git" not in path.parts}
        for path in (ROOT / "go.mod", ROOT / "go.sum", Path(__file__).resolve(), ROOT / ".github/workflows/invariant-mutations-sustained.yml"):
            report["source_inventory_sha256s"][str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    if args.mixed_leases:
        report["scope"] = "one mixed production lease mutation; remaining mixed categories and full release remain open"
    if args.mixed_determinism:
        report["scope"] = "one mixed production I4 mutation; remaining mixed categories and full release remain open"
    if args.mixed_cas:
        report["scope"] = "one mixed production CAS mutation; mutant stops on retained journal corruption; remaining categories and full release remain open"
    if args.mixed_enqueue:
        report["scope"] = "one mixed production enqueue mutation; remaining mixed categories and full release remain open"
    if args.mixed_start_repair:
        report["scope"] = "one mixed production start repair mutation; mutant leaves admitted orphan unstarted; remaining purge category and full release remain open"
    if args.mixed_purge:
        report["scope"] = "one mixed production purge-order mutation; six focused mixed categories have fixtures; full sustained mixed-chaos release gate remains open"
    if args.mixed_determinism or args.mixed_leases or args.mixed_cas or args.mixed_enqueue or args.mixed_start_repair or args.mixed_purge:
        report.pop("cas_rounds")
        report.pop("seeds_per_modeled_workload")
    if args.sustained:
        report["scope"] = "same-store sustained mixed journal-leader chaos plus one controlled live mutation; all six accepted categories and independent full-matrix/soak gates required"
        report["duration"] = args.sustained
        report["release_duration"] = args.sustained == "10m"
    success = True
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
        changed = args.output / (name + ".go")
        changed.write_text(contents)
        overlay = args.output / (name + "-overlay.json")
        overlay.write_text(json.dumps({"Replace": {str(source): str(changed)}}))
        result = run_fixture(control, args.output, name, overlay)
        report["negative_controls"].append(dict(name=name, **result))
        if result["returncode"] == 0 or result["detected"] or (name == "unrelated_failure" and not result["unrelated_control_failed"]):
            raise RuntimeError("runner counted or missed its negative control: " + name)
    for mutation in selected:
        source = ROOT / mutation["file"]
        original = source.read_text()
        entry = dict(name=mutation["name"], file=mutation["file"], test=mutation["test"],
                     source_sha256=hashlib.sha256(original.encode()).hexdigest())
        if mutation.get("fixture_files"):
            entry["fixture_source_sha256s"] = {path: hashlib.sha256((ROOT/path).read_bytes()).hexdigest() for path in mutation["fixture_files"]}
        if mutation.get("fixture_file"):
            entry["fixture_file"] = mutation["fixture_file"]
            entry["fixture_source_sha256"] = hashlib.sha256((ROOT / mutation["fixture_file"]).read_bytes()).hexdigest()
        report["mutations"].append(entry)
        try:
            if original.count(mutation["before"]) != 1:
                raise RuntimeError("mutation anchor must occur exactly once")
            entry["baseline"] = run_fixture(mutation, args.output, "baseline")
            if not entry["baseline"]["passed"]:
                raise RuntimeError("unmodified fixture did not pass; detection unproven")
            changed = args.output / (mutation["name"] + ".go")
            changed.write_text(original.replace(mutation["before"], mutation["after"], 1))
            entry["mutated_source_sha256"] = hashlib.sha256(changed.read_bytes()).hexdigest()
            overlay = args.output / (mutation["name"] + "-overlay.json")
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
