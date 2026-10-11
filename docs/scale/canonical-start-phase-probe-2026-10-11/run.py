"""Capture one prepared helper execution; refuse to overwrite its receipt."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

root, out = map(lambda x: Path(x).resolve(), sys.argv[1:])
assert not (out / 'run-command.json').exists()
repo = Path.cwd()
names = subprocess.check_output(['git', 'ls-files'], text=True).splitlines()
inputs = {name: hashlib.sha256((repo / name).read_bytes()).hexdigest()
          for name in names if name.endswith('.go') or name in ('go.mod', 'go.sum')}
(out / 'source-inputs.json').write_text(json.dumps(inputs, indent=2) + '\n')
(out / 'binary-build-info.txt').write_bytes(subprocess.check_output(
    ['go', 'version', '-m', str(root / 'probe')]))
receipt = dict(command=[str(root / 'probe'), str(root), str(out / 'result.json')],
               start_ns=time.time_ns(),
               revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
               binary_sha256=hashlib.sha256((root / 'probe').read_bytes()).hexdigest(),
               helper_sha256=hashlib.sha256((out / 'probe.go.txt').read_bytes()).hexdigest(),
               environment={'GOMAXPROCS': '2', 'GOMEMLIMIT': '512MiB'})
with (out / 'run.log').open('wb') as log:
    child = subprocess.Popen(receipt['command'], stdout=log, stderr=subprocess.STDOUT,
                             env=dict(os.environ, **receipt['environment']))
    receipt.update(pid=child.pid, actual_exit=child.wait())
receipt.update(end_ns=time.time_ns(), source_unchanged=all(
    hashlib.sha256((repo / name).read_bytes()).hexdigest() == sha
    for name, sha in inputs.items()))
(out / 'run-command.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
