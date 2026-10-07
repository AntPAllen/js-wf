#!/usr/bin/env python3
"""Review a closed original bulk journal soak; never start or restore brokers."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile


TEST = "TestFiveContainerMixedJournalLeaderEveryThirtySeconds"
PROFILE = {"GOMAXPROCS": "4", "GOGC": "500", "GOMEMLIMIT": "4GiB",
           "WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT": "1",
           "WF_MATRIX_BULK_FINAL_LATENCY": "1", "WF_MATRIX_BULK_POINT_COMPARE": "1"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path):
    return json.loads(Path(path).read_bytes())


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def birth(stat):
    pieces = stat.rsplit(")", 1)
    require(len(pieces) == 2, "invalid process birth record")
    fields = pieces[1].split()
    require(len(fields) >= 20 and fields[19].isdigit(), "invalid process birth record")
    return fields[19]


def process_closed(record, proc_root=Path("/proc")):
    """Prove the observed incarnation is gone, even when its PID was reused."""
    pid = record["pid"]
    require(type(pid) is int and pid > 0, "invalid observed PID")
    require(record["stat"].split(" ", 1)[0] == str(pid), "PID/stat mismatch")
    original_birth = birth(record["stat"])
    try:
        current_birth = birth((proc_root / str(pid) / "stat").read_text())
    except FileNotFoundError:
        return dict(pid=pid, original_birth=original_birth, state="absent")
    require(current_birth != original_birth, "observed process incarnation is still live")
    return dict(pid=pid, original_birth=original_birth, current_birth=current_birth,
                state="PID reused; original incarnation closed")


def validate_profile(state, env, parent, original_root, duration, revision, sdk_pid):
    require(duration in ("10m", "24h"), "unsupported original duration")
    require(state["source"] == revision and state["duration"] == duration
            and state["row"] == "journal" and state["seed"] == 1
            and state["race"] is False and state["clears_full_tier3_release"] is False,
            "wrong source, row, seed, duration, race or release scope")
    for key in ("chunked_state_retained_audit", "bulk_final_latency", "compare_bulk_point"):
        require(state[key] is True, "required explicit profile missing: " + key)
    for key in ("concurrent_state_retained_audit", "streaming_state_retained_audit",
                "batched_retained_audit", "cached_latency_metadata", "upgrade_start_gap"):
        require(state[key] is False, "incompatible profile: " + key)
    require(state["journal_rollout"] == "none", "unexpected wire rollout")
    require((state["memory_limit"], state["gomaxprocs"], state["gc_percent"])
            == ("4GiB", "4", "500"), "wrong producer resource profile")
    require(all(env.get(k) == v for k, v in PROFILE.items()), "wrong exported profile")
    require(env.get("WF_TIER3_MATRIX_DURATION") == duration
            and env.get("WF_TIER3_SYNC_INTERVAL") == "2m"
            and env.get("FAULT_SEED") == "1"
            and env.get("TIER3_MATRIX_ARTIFACT_ROOT") == str(original_root / "fixture"),
            "wrong native duration, sync, seed or artifact root")
    if duration == "24h":
        for state_key, env_key in (("retained_audit_trace", "WF_TIER3_RETAINED_AUDIT_TRACE"),
                                  ("audit_wait_stack", "WF_TIER3_AUDIT_WAIT_STACK"),
                                  ("explicit_route_seeds", "WF_TIER3_EXPLICIT_ROUTE_SEEDS")):
            require(state[state_key] is True and env.get(env_key) == "1",
                    "original 24h diagnostic/route profile missing")
        admission = state["disk_admission"]
        require(admission["additional_reserve_bytes"] >= 1024**3
                and admission["minimum_free_bytes"] >= 17 * 1024**3
                and admission["free_bytes"] >= admission["minimum_free_bytes"],
                "original 24h disk admission was not satisfied")
    timeout = "24h20m" if duration == "24h" else "20m"
    expected = [str(original_root / "integration.test"), "-test.run=^" + TEST + "$",
                "-test.v=test2json", "-test.count=1", "-test.timeout=" + timeout]
    require(parent["pid"] == sdk_pid and parent["args"] == expected,
            "wrong actual SDK identity or original argv/deadline")
    require(parent["profile_environment"] == PROFILE, "wrong observed SDK profile")


def verify_legacy_archive(root):
    manifest = read(root / "archive-manifest.json")
    require(sha(root / "originals.tar.gz") == manifest["archive_sha256"], "legacy archive SHA mismatch")
    seen = set()
    with tarfile.open(root / "originals.tar.gz") as archive:
        for member in archive:
            path = Path(member.name)
            require(member.isfile() and not path.is_absolute() and ".." not in path.parts
                    and path.as_posix() == member.name
                    and member.name not in seen and member.name in manifest["files"],
                    "unsafe, duplicate or unexpected legacy member")
            with archive.extractfile(member) as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            require(digest == manifest["files"][member.name] == sha(root / member.name),
                    "legacy member differs from original: " + member.name)
            seen.add(member.name)
    require(seen == set(manifest["files"]), "incomplete legacy archive")
    return dict(files=len(seen), sha256=manifest["archive_sha256"])


def native_terminal(events, duration):
    limits = {"10m": (600, 1200), "24h": (86400, 87600)}
    require(duration in limits, "unsupported original duration")
    terminal = [r for r in events if r.get("Test") == TEST and r["Action"] in ("pass", "fail")]
    require(len(terminal) == 1 and terminal[0]["Action"] == "pass", "missing/duplicate/failed native terminal")
    elapsed = terminal[0].get("Elapsed")
    minimum, maximum = limits[duration]
    require(type(elapsed) in (int, float) and math.isfinite(elapsed)
            and minimum <= elapsed <= maximum,
            "native duration shortened or exceeds original SDK deadline")
    return terminal[0]


def review(root, watch, repo, duration, revision, sdk_pid, original_root=None):
    root, watch, repo = Path(root), Path(watch), Path(repo)
    original_root = Path(original_root or root)
    state = read(root / "execution.json")
    require(state["status"] == "row_verified" and state["test_exit_code"] == 0,
            "native/producer failed or incomplete; preserve without acceptance")
    observed = [json.loads(line) for line in (watch / "sdks.jsonl").read_text().splitlines() if line]
    parents = [r for r in observed if r["pid"] == sdk_pid]
    require(len(parents) == 1, "missing/duplicate observed original SDK")
    parent = parents[0]
    validate_profile(state, read(root / "test-environment.json"), parent,
                     original_root, duration, revision, sdk_pid)
    watcher = read(watch / "watch-result.json")
    require(watcher["parent_pid"] == sdk_pid and watcher["parent_gone"] is True
            and watcher["deadline_exhausted"] is False, "observer incomplete/expired; not native terminal")
    closed = [process_closed(r) for r in observed]
    binary = read(root / "binary.json")
    require(binary["race"] is False and "-race=true" not in binary["build_info"], "race profile mismatch")
    require(sha(root / "integration.test") == binary["sha256"], "retained SDK hash mismatch")
    for actual in observed:
        require(actual["sha256"] == binary["sha256"], "actual SDK differs from retained binary")
        require(actual["build_info"].splitlines()[1:] == binary["build_info"].splitlines()[1:],
                "actual SDK build metadata differs")
    require("vcs.revision=" + revision in parent["build_info"]
            and "vcs.modified=false" in parent["build_info"], "actual SDK source provenance mismatch")
    info = subprocess.check_output(["go", "version", "-m", str(root / "integration.test")], text=True)
    require(info.splitlines()[1:] == binary["build_info"].splitlines()[1:], "retained SDK build differs")
    before = read(root / "source-before.json")
    require(before == read(root / "source-after.json") and before["revision"] == revision, "source changed during run")
    names = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", revision], cwd=repo, text=True).splitlines()
    expected = {n for n in names if n.endswith((".go", ".py", ".yml"))
                or n in ("go.mod", "go.sum") or n.startswith("sim/testdata/")}
    require(expected == set(before["files"]), "incomplete source inventory")
    for name, digest in before["files"].items():
        blob = subprocess.check_output(["git", "cat-file", "blob", revision + ":" + name], cwd=repo)
        require(sha(root / "source" / name) == hashlib.sha256(blob).hexdigest() == digest,
                "source differs from Git/current inventory: " + name)
    servers = [json.loads(line) for line in (watch / "servers.jsonl").read_text().splitlines() if line]
    require({r["inspect"]["Name"].rsplit("-n", 1)[-1] for r in servers} == {"0", "1", "2", "3", "4"}, "incomplete R5 server identities")
    server_sha = sha(root / "fixture/cluster/nats-server")
    require(sha(watch / "server-executables" / server_sha) == server_sha,
            "captured server executable differs")
    for actual in servers:
        require(actual["sha256"] == server_sha,
                "observed/captured server executable differs")
        require("\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0" in actual["build_info"], "server module differs")
        closed.append(process_closed(actual))
    events = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines() if line]
    terminal = native_terminal(events, duration)
    archive = verify_legacy_archive(root)
    # Independent current checker and compiled-source checker must reproduce
    # the complete original report. Never drop checkpoint, bulk or point gates.
    with tempfile.TemporaryDirectory(prefix="js-wf-bulk-terminal-check-") as temp:
        # Compile imported checkers from the Git-bound source, without reading
        # old cached bytecode or adding files to the verified fixture tree.
        checker_env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
                           PYTHONPYCACHEPREFIX=str(Path(temp) / "isolated-pycache"))
        for scripts in (root / "source/scripts", repo / "scripts"):
            output = Path(temp) / "result.json"
            command = ["python3", str(scripts / "check-tier3-journal-row.py"), "--root", str(root / "fixture"),
                       "--events", str(root / "events.jsonl"), "--row", "journal", "--duration", duration,
                       "--expected-seed", "1", "--output", str(output), "--require-checkpoint-audits",
                       "--require-bulk-final-latency", "--require-bulk-point-equivalence"]
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL, env=checker_env)
            require(read(output) == read(root / "result.json"), "complete row report did not reproduce")
        for script, filename in (("explain-tier3-events.py", "event-explanations.json"),
                                 ("review-tier3-fencing.py", "fencing-timeline-review.json")):
            output = Path(temp) / filename
            subprocess.run(["python3", str(root / "source/scripts" / script), "--root", str(root / "fixture"),
                            "--output", str(output)], check=True, stdout=subprocess.DEVNULL, env=checker_env)
            require(read(output) == read(root / "fixture" / filename), "event/fencing report did not reproduce")
    bulk = read(root / "fixture/bulk-latency-audit.json")
    require(bulk["repair_writers_stopped"] is True
            and bulk["repair_stop_requested_at"] <= bulk["repair_writers_joined_at"], "repair writers not joined")
    return dict(accepted_component=True, execution=state, native_terminal=terminal,
                source_files_exact_git_before_after_current=len(expected), original_archive=archive,
                sdk_sha256=binary["sha256"], actual_sdk_pid=sdk_pid, server_sha256=server_sha,
                observed_server_incarnations=len(servers), observed_incarnation_closure=closed,
                complete_row_and_event_reports_regenerate=True, row_result=read(root / "result.json"),
                bulk_latency=bulk, qualifies_24h_component=duration == "24h",
                clears_full_tier3_release=False, stores_reopened=False,
                scope="Executed-source original journal component only. Periodic observations are not exhaustive process lifetime proof. Full matrix/all-row24h/default adoption remain separate.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "watch", "repo", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--original-root", type=Path)
    p.add_argument("--duration", choices=("10m", "24h"), required=True)
    p.add_argument("--expected-source", required=True)
    p.add_argument("--expected-sdk-pid", type=int, required=True)
    a = p.parse_args()
    result = review(a.root, a.watch, a.repo, a.duration, a.expected_source, a.expected_sdk_pid, a.original_root)
    with a.output.open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print("ACCEPTED_ORIGINAL_COMPONENT", a.duration)
