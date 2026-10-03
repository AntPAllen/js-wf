#!/usr/bin/env python3
"""Capture exact committed source before/after the R5 worker-clock execution."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--stage',choices=['before','after'],required=True)
a=p.parse_args()
def git(*args): return subprocess.check_output(['git',*args]).decode().strip()
files=git('ls-files').splitlines()
names=[name for name in files if name.endswith(('.go','.py','.yml')) or name in ('go.mod','go.sum')]
proof=dict(revision=git('rev-parse','HEAD'),clean=not git('status','--porcelain'),
           files={name:hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in names})
if not proof['clean']: raise SystemExit('worker-clock execution requires a clean committed source')
a.root.mkdir(parents=True,exist_ok=True)
path=a.root/f'worker-clock-source-{a.stage}.json'
path.write_text(json.dumps(proof,indent=2)+'\n')
if a.stage=='after' and proof!=json.loads((a.root/'worker-clock-source-before.json').read_text()):
    raise SystemExit('worker-clock source changed during execution')
