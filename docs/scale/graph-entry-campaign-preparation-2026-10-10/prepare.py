#!/usr/bin/env python3
"""Freeze module inputs and build retained binaries; never run the campaign."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

here = Path(__file__).resolve().parent
repo = here.parents[2]
source = sys.argv[1]
checkout = Path("/home/exedev/js-wf-indexed-entry100000-qualification-20261010")
root = Path("/home/exedev/js-wf-indexed-entry100000-20261010")
assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip() == source
assert not checkout.exists() and not root.exists(), "never overwrite a prior campaign"
root.mkdir()
state = dict(source=source, checkout=str(checkout), root=str(root), campaign_started=False,
             prepared=False, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), commands=[],
             compile_environment={"GOWORK": "off", "GOFLAGS": ""})
def save():
    (here / "preparation.json").write_text(json.dumps(state, indent=2) + "\n")
save()
subprocess.run(["git", "worktree", "add", "--detach", "--no-checkout", str(checkout), source], cwd=repo, check=True)
directories = subprocess.check_output(["git", "ls-tree", "-d", "--name-only", source], cwd=repo, text=True).splitlines()
directories = [d for d in directories if d not in ("docs", "scripts", ".github")]
subprocess.run(["git", "sparse-checkout", "set", "--cone", *directories], cwd=checkout, check=True)
tracked = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", source], cwd=checkout, text=True).splitlines()
names = [n for n in tracked if n.split("/")[0] not in ("docs", "scripts", ".github")
         and (n.endswith(".go") or n in ("go.mod", "go.sum") or "/testdata/" in n)]
def inventory():
    return dict(source=source, files={n: hashlib.sha256((checkout / n).read_bytes()).hexdigest() for n in names})
before = inventory()
(root / "source-before.json").write_text(json.dumps(before, indent=2) + "\n")
state["inputs"] = len(names)
state["toolchain"] = subprocess.check_output(["/usr/local/bin/go", "version"], text=True).strip()
state["binaries"] = {}
env = dict(os.environ, GOWORK="off", GOFLAGS="")
for mode in ("normal", "race"):
    binary = root / ("worker-" + mode + ".test")
    args = ["/usr/local/bin/go", "test"] + (["-race"] if mode == "race" else []) + ["-c", "-o", str(binary), "./worker"]
    row = dict(mode=mode, command=args, cwd=str(checkout), started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
    state["commands"].append(row)
    save()
    with (root / ("compile-" + mode + ".log")).open("w") as output:
        row["exit"] = subprocess.call(args, cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT)
    row["ended_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
    assert row["exit"] == 0, "binary compile failed"
    state["binaries"][mode] = dict(path=str(binary), bytes=binary.stat().st_size,
        sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        build_info=subprocess.check_output(["/usr/local/bin/go", "version", "-m", str(binary)], text=True))
    save()
after = inventory()
(root / "source-after.json").write_text(json.dumps(after, indent=2) + "\n")
assert after == before
assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=checkout)
state.update(prepared=True, ended_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
save()
print(json.dumps(state))
