#!/usr/bin/env python3
"""Read-only privileged reference check for the archived temporary traces."""
import json
import os
from pathlib import Path
import subprocess

roots = json.loads(Path(__file__).with_name('selection.json').read_text())['roots']
blocked, errors = [], []
def check(value, source):
    for root in roots:
        if root in value:
            blocked.append({'root': root, 'source': source})

for proc in Path('/proc').glob('[0-9]*'):
    if int(proc.name) == os.getpid():
        continue
    try:
        for name in ('cwd', 'exe', 'root'):
            try:
                check(os.readlink(proc / name), str(proc / name))
            except (FileNotFoundError, ProcessLookupError):
                pass
        for name in ('cmdline', 'maps', 'mountinfo'):
            try:
                check((proc / name).read_bytes().decode(errors='replace'), str(proc / name))
            except (FileNotFoundError, ProcessLookupError):
                pass
        for fd in (proc / 'fd').iterdir():
            try:
                check(os.readlink(fd), str(fd))
            except (FileNotFoundError, ProcessLookupError):
                pass
    except (FileNotFoundError, ProcessLookupError):
        pass
    except PermissionError as exc:
        errors.append(str(exc))
ids = subprocess.check_output(['docker', 'ps', '-aq'], text=True).split()
containers = json.loads(subprocess.check_output(['docker', 'inspect', *ids])) if ids else []
for container in containers:
    for mount in container['Mounts']:
        source = mount['Source'].rstrip('/')
        for root in roots:
            if root == source or root.startswith(source + '/') or source.startswith(root + '/'):
                blocked.append({'root': root, 'source': 'docker:' + container['Id']})
print(json.dumps({'roots': roots, 'blocked': blocked, 'permission_errors': errors}, indent=2))
assert not blocked and not errors
