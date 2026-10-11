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
checkout = Path("/home/exedev/js-wf-classified-entry100000-qualification-20261011")
root = Path("/home/exedev/js-wf-classified-entry100000-20261011")
assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip() == source
resume = len(sys.argv) == 3 and sys.argv[2] == "--resume-empty-checkout"
if resume:
    state = json.loads((here / "preparation.json").read_text())
    assert state["source"] == source and state["checkout"] == str(checkout) and state["root"] == str(root)
    assert not state["prepared"] and not state["campaign_started"] and not state["commands"]
    assert root.is_dir() and not list(root.iterdir())
    assert {p.name for p in checkout.iterdir()} == {".git"}
    assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip() == source
    state["resumed_empty_checkout"] = True
else:
    assert not checkout.exists() and not root.exists(), "never overwrite a prior campaign"
    root.mkdir()
    state = dict(source=source, checkout=str(checkout), root=str(root), campaign_started=False,
                 prepared=False, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), commands=[],
                 compile_environment={"GOWORK": "off", "GOFLAGS": ""})
def save():
    (here / "preparation.json").write_text(json.dumps(state, indent=2) + "\n")
save()
if not resume:
    subprocess.run(["git", "worktree", "add", "--detach", "--no-checkout", str(checkout), source], cwd=repo, check=True)
directories = subprocess.check_output(["git", "ls-tree", "-d", "--name-only", source], cwd=repo, text=True).splitlines()
directories = [d for d in directories if d not in ("docs", "scripts", ".github")]
subprocess.run(["git", "sparse-checkout", "set", "--cone", *directories], cwd=checkout, check=True)
subprocess.run(["git", "checkout", "--force", "--detach", source], cwd=checkout, check=True)
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
# A retained normal control uses the exact compiled binary and bounded fixture.
store = root/'native-control'
assert not store.exists()
control_env = dict(os.environ,GOMAXPROCS='4',GOMEMLIMIT='8GiB',WF_GRAPH_CONTINUATION_LIMIT_BUDGET='20',WF_GRAPH_CONTINUATION_OWNER_INDEX='1',WF_GRAPH_CONTINUATION_DURABLE='1',WF_GRAPH_CONTINUATION_COMPACTION_TTL='3h',WF_GRAPH_LIMIT_PORT_PROFILE='1',WF_GRAPH_LIMIT_STORE_ROOT=str(store))
control_command=[str(root/'worker-normal.test'),'-test.v','-test.run=^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/^R1$/^archive=true$','-test.count=1','-test.timeout=5m']
with (here/'retained-control.log').open('wb') as output:
    control = subprocess.Popen(control_command,cwd=checkout/'worker',env=control_env,stdout=output,stderr=subprocess.STDOUT)
    control_pid=control.pid
    control_exit=control.wait()
assert control_exit==0
files={str(p.relative_to(store)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in store.rglob('*') if p.is_file()}
assert files
(here/'retained-control.json').write_text(json.dumps(dict(actual_child_exit=control_exit,actual_child_pid=control_pid,command=control_command,retained_root=str(store),files=files,log_sha256=hashlib.sha256((here/'retained-control.log').read_bytes()).hexdigest()),indent=2)+'\n')
frozen=(checkout/'worker/graph_continuation_limit_test.go').read_bytes()
(here/'worker_graph_continuation_limit_test.go.txt.gz').write_bytes(__import__('gzip').compress(frozen,mtime=0))
(here/'source-control.json').write_text(json.dumps({'worker/graph_continuation_limit_test.go':hashlib.sha256(frozen).hexdigest()},indent=2)+'\n')
state.update(prepared=True, ended_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
save()
print(json.dumps(state))
