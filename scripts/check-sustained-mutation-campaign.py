#!/usr/bin/env python3
"""Verify all six sustained mutation pairs from original Actions artifacts."""
import argparse
import ast
import base64
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

spec = importlib.util.spec_from_file_location("mutation_runner", Path(__file__).with_name("check-invariant-mutations.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

TEST = "TestSustainedMixedMutationAfterJournalLeaderKills"
MODES = {
    "determinism": ("missing_determinism_guard", "MIXED_GUARD_REJECTED", "MIXED_MUTATION_ESCAPE invariant=I4 effects=1 result=42 error=<nil> terminal=Completed"),
    "leases": ("independent_worker_leases", "MIXED_LEASE_REJECTED admitted=0", "MIXED_MUTATION_ESCAPE category=independent_worker_leases admitted=1 rival_epoch="),
    "cas": ("missing_cas", "MIXED_CAS_REJECTED acknowledged_winners=1 stale_rejections=1", "MIXED_MUTATION_ESCAPE category=missing_cas acknowledged_winners=2 stale_rejections=0 same_index=2 retained_duplicates=2 checker_rejected=true"),
    "enqueue": ("missing_run_message_id", "MIXED_ENQUEUE_REJECTED acknowledged_calls=64 retained=1", "MIXED_MUTATION_ESCAPE category=missing_run_message_id acknowledged_calls=64 retained_at_least64=true terminal=28 retained="),
    "start-repair": ("skipped_start_reconciler", "MIXED_START_REPAIR_REJECTED retained_invocations=29 terminal=29", "MIXED_MUTATION_ESCAPE category=skipped_start_reconciler retained_invocations=29 original_terminal=28 orphan_journal=absent orphan_terminal=absent"),
    "purge": ("invocation_purged_first", "MIXED_PURGE_REJECTED terminal=28", "MIXED_MUTATION_ESCAPE category=invocation_purged_first cut_before=signals invocation=absent journal=unchanged terminal=unchanged purge_marker=retained retry=invocation_not_found original_terminal=28"),
}


def check_metadata(metadata):
    revision = metadata.get("headSha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or (metadata.get("status"), metadata.get("conclusion")) != ("completed", "success"):
        raise ValueError("campaign must be terminal and successful at an exact revision")
    expected = {"categories"} | {f"sustained ({mode})" for mode in MODES}
    jobs = metadata.get("jobs", [])
    if len(jobs) != 7 or {job.get("name") for job in jobs} != expected:
        raise ValueError("missing, duplicate or unexpected category job")
    if any((job.get("status"), job.get("conclusion")) != ("completed", "success") for job in jobs):
        raise ValueError("a required category job did not pass")
    return revision


def git_sources(revision):
    paths = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", "-z", revision], cwd=runner.ROOT).decode().split("\0")
    extra = {"go.mod", "go.sum", "scripts/check-invariant-mutations.py", ".github/workflows/invariant-mutations-sustained.yml"}
    return {path: subprocess.check_output(["git", "show", revision + ":" + path], cwd=runner.ROOT)
            for path in paths if path.endswith(".go") or path in extra}


def read_events(path):
    events = []
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith("go: downloading "):
            continue
        event = json.loads(line)
        if not isinstance(event, dict):
            raise ValueError("non-object Go event")
        events.append(event)
    return events


def output_for(events, test=TEST):
    return "".join(event.get("Output", "") for event in events if event.get("Test", "").split("/")[0] == test)


def require_execution(events, test, action):
    terminal = [event.get("Action") for event in events if event.get("Test") == test and event.get("Action") in ("pass", "fail", "skip")]
    packages = [event.get("Action") for event in events if event.get("Package") == "js-wf/integration" and "Test" not in event and event.get("Action") in ("pass", "fail", "skip")]
    if terminal != [action] or packages != [action] or any(event.get("Action") == "build-fail" or "panic: test timed out" in event.get("Output", "") for event in events):
        raise ValueError("required actual semantic execution is missing or substituted")


def raw_receipts(output, marker):
    return [json.loads(value) for value in re.findall(re.escape(marker) + r" (\{[^\n]+\})", output)]


def decode_proofs(mode, baseline, mutated):
    proof = {}
    if mode == "cas":
        raw = raw_receipts(mutated, "MIXED_CAS_RAW_RECEIPT")
        bodies = [json.loads(base64.b64decode(item["Data"], validate=True)) for item in raw]
        if len(raw) != 2 or len({item["Sequence"] for item in raw}) != 2 or len({item["Subject"] for item in raw}) != 1:
            raise ValueError("missing distinct physical CAS receipts")
        if any(body["index"] != 2 or body["kind"] != "StepCompleted" or body["payload"]["result"] != 42 for body in bodies) or bodies[0] != bodies[1]:
            raise ValueError("CAS receipts do not prove the same logical entry")
        if "index 2 at position 3" not in mutated:
            raise ValueError("raw-state checker did not reject the duplicate")
        proof["cas"] = dict(receipts=raw, decoded=bodies)
    if mode == "enqueue":
        for phase, output in (("baseline", baseline), ("mutated", mutated)):
            raw = raw_receipts(output, "MIXED_ENQUEUE_RAW_RECEIPT")
            if (phase == "baseline" and len(raw) != 1) or (phase == "mutated" and len(raw) < 64) or len({item["Sequence"] for item in raw}) != len(raw):
                raise ValueError("missing distinct retained enqueue receipts")
            if any(base64.b64decode(item["Data"], validate=True) != b"guardshort.guardshort-0-0" for item in raw):
                raise ValueError("enqueue receipts contain the wrong invocation")
            headers = [(item.get("Header") or {}).get("Nats-Msg-Id", []) for item in raw]
            if (phase == "baseline" and headers != [["mixed-enqueue:guardshort.guardshort-0-0"]]) or (phase == "mutated" and any(headers)):
                raise ValueError("enqueue receipts do not establish message-ID isolation")
            proof[phase] = raw
    if mode == "start-repair":
        for phase, output in (("baseline", baseline), ("mutated", mutated)):
            raw = raw_receipts(output, "MIXED_START_RAW_INVOCATION")
            if len(raw) != 1:
                raise ValueError("missing retained orphan invocation receipt")
            inv = raw[0]
            payload = base64.b64decode(inv["Data"], validate=True)
            if inv["Subject"] != "wf.inv.guardshort.repair-orphan" or payload != b"0" or inv["Header"].get("Wf-Input-SHA256") != [hashlib.sha256(payload).hexdigest()]:
                raise ValueError("orphan invocation receipt has the wrong generation/input")
            dispatch = raw_receipts(output, "MIXED_START_RAW_DISPATCH")
            if phase == "baseline":
                if len(dispatch) != 1 or base64.b64decode(dispatch[0]["Data"], validate=True) != b"guardshort.repair-orphan" or dispatch[0]["Header"].get("Nats-Msg-Id") != [f"start:guardshort.repair-orphan:{inv['Sequence']}"]:
                    raise ValueError("start repair dispatch is not bound to the original generation")
            elif dispatch:
                raise ValueError("disabled scanner unexpectedly retained a dispatch")
            proof[phase] = dict(invocation=inv, dispatch=dispatch)
    if mode == "purge":
        old = raw_receipts(baseline, "MIXED_PURGE_RAW_INVOCATION")
        # The journal proof is an array rather than a RawStreamMsg object.
        arrays = re.findall(r"MIXED_PURGE_REUSED_JOURNAL (\[[^\n]+\])", baseline)
        if len(old) != 1 or len(arrays) != 1:
            raise ValueError("missing purge/reuse generation receipts")
        reused = json.loads(arrays[0])
        prior_arrays = re.findall(r"MIXED_PURGE_RETAINED_JOURNAL (\[[^\n]+\])", baseline)
        if len(prior_arrays) != 1:
            raise ValueError("missing prior retained journal")
        prior = json.loads(prior_arrays[0])
        generation = reused[-1]["payload"]["inv_seq"]
        if len(reused) != 4 or [entry["index"] for entry in reused] != list(range(4)) or generation <= old[0]["Sequence"] or any(entry["sequence"] <= prior[-1]["sequence"] for entry in reused) or base64.b64decode(reused[-1]["payload"]["result"], validate=True) != b"42":
            raise ValueError("purge reuse mixed old entries or generations")
        proof["reuse"] = dict(old=old, prior=prior, reused=reused)
    return proof


def recorded_mutations(source):
    """Read literal mutation definitions at the executed revision, without eval."""
    tree = ast.parse(source)
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "MUTATIONS"
                           for target in node.targets)]
    if len(assignments) != 1 or not isinstance(assignments[0].value, ast.List):
        raise ValueError("missing or ambiguous recorded mutation registry")
    result = {}
    for item in assignments[0].value.elts:
        if (not isinstance(item, ast.Call) or not isinstance(item.func, ast.Name)
                or item.func.id != "dict" or item.args
                or any(keyword.arg is None for keyword in item.keywords)):
            raise ValueError("recorded mutation registry is not literal")
        definition = {keyword.arg: ast.literal_eval(keyword.value) for keyword in item.keywords}
        if any(not isinstance(definition.get(field), str)
               for field in ("name", "file", "before", "after")):
            raise ValueError("invalid recorded mutation definition")
        name = definition["name"]
        if name in result:
            raise ValueError("duplicate recorded mutation definition")
        result[name] = definition
    if set(result) != {mode[0] for mode in MODES.values()}:
        raise ValueError("recorded mutation categories differ from required six")
    return result


def check_category(root, mode, duration, revision, sources):
    report = json.loads((root / "report.json").read_text())
    expected_hashes = {path: hashlib.sha256(contents).hexdigest() for path, contents in sources.items()}
    if report.get("head") != revision or report.get("duration") != duration or report.get("release_duration") is not (duration == "10m") or report.get("source_inventory_sha256s") != expected_hashes:
        raise ValueError("original source inventory/revision/duration mismatch")
    name, positive, escape = MODES[mode]
    entries = report.get("mutations", [])
    if len(entries) != 1 or entries[0].get("name") != name or entries[0].get("test") != TEST or entries[0].get("error"):
        raise ValueError("wrong category fixture or runner failure")
    entry = entries[0]
    if entry.get("baseline", {}).get("passed") is not True or entry.get("baseline", {}).get("returncode") != 0 or entry.get("mutated", {}).get("detected") is not True or entry.get("mutated", {}).get("returncode", 0) == 0:
        raise ValueError("original runner did not accept the required pair")
    mutation = recorded_mutations(sources["scripts/check-invariant-mutations.py"])[name]
    if entry.get("file") != mutation["file"]:
        raise ValueError("reported production mutation file mismatch")
    source = sources[mutation["file"]]
    original = source.decode()
    if original.count(mutation["before"]) != 1 or entry.get("source_sha256") != hashlib.sha256(source).hexdigest():
        raise ValueError("mutation does not match the exact production source")
    changed = (root / (name + ".go")).read_bytes()
    if changed != original.replace(mutation["before"], mutation["after"], 1).encode() or entry.get("mutated_source_sha256") != hashlib.sha256(changed).hexdigest():
        raise ValueError("overlay is not the exact single production mutation")
    overlay = json.loads((root / (name + "-overlay.json")).read_text()).get("Replace", {})
    if len(overlay) != 1 or not next(iter(overlay)).endswith("/" + mutation["file"]) or Path(next(iter(overlay.values()))).name != name + ".go":
        raise ValueError("unexpected mutation overlay mapping")
    for path, digest in entry.get("fixture_source_sha256s", {}).items():
        if expected_hashes.get(path) != digest:
            raise ValueError("fixture source hash mismatch")
    phases, output = {}, {}
    for phase, action in (("baseline", "pass"), ("mutated", "fail")):
        events = read_events(root / f"{name}-{phase}.jsonl")
        require_execution(events, TEST, action)
        output[phase] = output_for(events)
        for marker in ("MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1", "MIXED_GUARD_FAULT", "signal=SIGKILL", positive if phase == "baseline" else escape):
            if marker not in output[phase]:
                raise ValueError("required live category evidence missing: " + marker)
        phases[phase] = runner.check_sustained_evidence(events, dict(test=TEST, sustained_duration=duration, environment={"WF_SUSTAINED_MUTATION": mode}), str(root / f"{name}-{phase}-matrix"))
        if entry[phase].get("sustained") != phases[phase]:
            raise ValueError("uploaded phase report disagrees with original artifact verification")
        admission = phases[phase]["admission"]
        checkpoints = list(map(int, re.findall(r"checkpoint audit batch=(\d+) complete report=", output[phase])))
        if checkpoints != list(range(10, admission["batches"] + 1, 10)):
            raise ValueError("missing or duplicated completed checkpoint audit")
        faults = json.loads((root / f"{name}-{phase}-matrix-faults.json").read_text())["faults"]
        scheduled = []
        for fault in faults:
            times = [datetime.fromisoformat(fault[key].replace("Z", "+00:00")) for key in ("scheduled", "killed", "healed")]
            if any(value.tzinfo is None or value.year < 2020 for value in times) or times != sorted(times) or fault.get("node") not in (0, 1, 2):
                raise ValueError("unconfirmed original leader fault chronology/identity")
            scheduled.append(times[0])
        if any((later - earlier).total_seconds() != 30 for earlier, later in zip(scheduled, scheduled[1:])):
            raise ValueError("original fault schedule is incomplete")
        report_pattern = r"MIXED_SUSTAINED_PRESERVED cutoff=(\d+) report=\{Invocations:(\d+) Journals:(\d+) Entries:(\d+) Terminal:(\d+)\}"
        retained = re.findall(report_pattern, output[phase])
        expected = [admission["invocation_cutoff"]] + [admission["retained"][key] for key in ("Invocations", "Journals", "Entries", "Terminal")]
        if len(retained) != 1 or list(map(int, retained[0])) != expected:
            raise ValueError("original cohort preservation counts disagree")
    controls = {control.get("name"): control for control in report.get("negative_controls", [])}
    if set(controls) != {"compile_error", "unrelated_failure"} or len(report["negative_controls"]) != 2:
        raise ValueError("missing actual runner negative controls")
    for control_name, control in controls.items():
        if control.get("returncode", 0) == 0 or control.get("detected") is not False:
            raise ValueError("runner incorrectly accepted a negative control")
        events = read_events(root / f"independent_worker_leases-{control_name}.jsonl")
        control_file = "lease/lease.go" if control_name == "compile_error" else "integration/core_test.go"
        control_source = b"package lease\ninvalid Go syntax\n" if control_name == "compile_error" else sources[control_file].replace(
            b"func TestLeaseFenceAndStartRepair(t *testing.T) {",
            b'func TestLeaseFenceAndStartRepair(t *testing.T) {\n\tt.Fatal("unrelated runner control")', 1)
        if (root / (control_name + ".go")).read_bytes() != control_source:
            raise ValueError("negative control source was substituted")
        control_overlay = json.loads((root / (control_name + "-overlay.json")).read_text()).get("Replace", {})
        if len(control_overlay) != 1 or not next(iter(control_overlay)).endswith("/" + control_file) or Path(next(iter(control_overlay.values()))).name != control_name + ".go":
            raise ValueError("unexpected negative control overlay mapping")
        if control_name == "unrelated_failure":
            require_execution(events, "TestLeaseFenceAndStartRepair", "fail")
            if "unrelated runner control" not in output_for(events, "TestLeaseFenceAndStartRepair"):
                raise ValueError("unrelated failure control did not actually execute")
        elif not any(event.get("Action") == "build-fail" for event in events) or any(event.get("Test") for event in events):
            raise ValueError("compilation control is not an actual rejected build failure")
    return dict(mode=mode, phases=phases, raw_proofs=decode_proofs(mode, output["baseline"], output["mutated"]))


def check_campaign(metadata, artifacts, duration):
    revision = check_metadata(metadata)
    sources = git_sources(revision)
    categories = [check_category(artifacts / ("sustained-production-" + mode), mode, duration, revision, sources) for mode in MODES]
    return dict(source=revision, duration=duration, shortened_smoke=duration == "35s", categories=categories,
                actual_pairs=6, clears_sustained_six_mutation_gate=duration == "10m", clears_full_release=False,
                scope="Six source mutations caught after same-store mixed journal-leader chaos; independent full matrix and 24-hour soak remain required.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--duration", choices=("35s", "10m"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = check_campaign(json.loads(args.metadata.read_text()), args.artifacts, args.duration)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "categories"}, indent=2))
