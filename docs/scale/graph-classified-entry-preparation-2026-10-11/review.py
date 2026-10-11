#!/usr/bin/env python3
"""Review preparation only; actual production-cap execution remains unproven."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import sys

here = Path(__file__).resolve().parent
repo = here.parents[2]
sys.path.insert(0,str(repo/"scripts"))
from graph_limit_profile import audit_profile
state = json.loads((here / "preparation.json").read_text())
assert state["prepared"] and state["campaign_started"] is False
checkout, root = Path(state["checkout"]), Path(state["root"])
assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip() == state["source"]
assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=checkout)
before = json.loads((root / "source-before.json").read_text())
assert before == json.loads((root / "source-after.json").read_text())
assert before["source"] == state["source"]
tracked = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", state["source"]], cwd=checkout, text=True).splitlines()
names = [n for n in tracked if n.split("/")[0] not in ("docs", "scripts", ".github")
         and (n.endswith(".go") or n in ("go.mod", "go.sum") or "/testdata/" in n)]
assert set(names) == set(before["files"]) and len(names) == state["inputs"]
objects = subprocess.check_output(["git", "cat-file", "--batch"], cwd=checkout,
                                 input="".join(state["source"] + ":" + n + "\n" for n in names).encode())
stream = io.BytesIO(objects)
for name in names:
    header = stream.readline().decode().split()
    assert header[1] == "blob"
    data = stream.read(int(header[2]))
    assert stream.read(1) == b"\n"
    assert hashlib.sha256(data).hexdigest() == before["files"][name]
    assert hashlib.sha256((checkout / name).read_bytes()).hexdigest() == before["files"][name]
assert state["compile_environment"] == {"GOWORK": "off", "GOFLAGS": ""}
assert len(state["commands"]) == 2
for command, mode in zip(state["commands"], ("normal", "race")):
    binary = root / ("worker-" + mode + ".test")
    expected = ["/usr/local/bin/go", "test"] + (["-race"] if mode == "race" else []) + ["-c", "-o", str(binary), "./worker"]
    assert command["command"] == expected and command["exit"] == 0 and command["ended_utc"]
    info = state["binaries"][mode]
    assert info["path"] == str(binary) and info["bytes"] == binary.stat().st_size
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == info["sha256"]
    assert info["build_info"] == subprocess.check_output(["/usr/local/bin/go", "version", "-m", str(binary)], text=True)
    assert ("-race=true" in info["build_info"]) == (mode == "race")
control = (here / "retained-control.log").read_text()
assert re.search(r"^PASS$", control, re.M) and "--- FAIL:" not in control
assert "GRAPH_CONTINUATION_LIMIT budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19 prefix_stage_calls=1/1 production_cap=false padding_operations=2" in control
windows = re.findall(r'GRAPH_LIMIT_PORT_PROFILE from=(\d+) to=(\d+) wall_ns=(\d+) operations=(.*)',control)
assert len(windows)==2
assert all(audit_profile(json.loads(row[3]))['accepted'] for row in windows)
receipt = json.loads((here / "retained-control.json").read_text())
assert hashlib.sha256(control.encode()).hexdigest() == receipt["log_sha256"]
store = Path(receipt["retained_root"])
assert receipt["files"] and store.is_dir()
for name, item in receipt["files"].items():
    data = (store / name).read_bytes()
    assert len(data) == item["bytes"] and hashlib.sha256(data).hexdigest() == item["sha256"]
frozen = gzip.decompress((here / "worker_graph_continuation_limit_test.go.txt.gz").read_bytes())
assert hashlib.sha256(frozen).hexdigest() == json.loads((here / "source-control.json").read_text())["worker/graph_continuation_limit_test.go"]
assert (checkout / "worker/graph_continuation_limit_test.go").read_bytes() == frozen
assert (repo / "worker/graph_continuation_limit_test.go").read_bytes() == frozen
result = dict(prepared=True, source=state["source"], git_verified_inputs=len(names),
              retained_control_files=len(receipt["files"]), binaries=["normal", "race"],
              actual_campaign_started=False, actual_100000_entries_qualified=False)
(here / "review.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
