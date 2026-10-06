"""Capture owned matrix worker executable bytes at observed /proc points.

The caller also binds matrix_process_observer.py, which supplies all-task-thread
ancestry traversal. Polling can miss short-lived generations.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import os
import re
import shutil
import subprocess

from matrix_process_observer import descendants


def worker_identity(argv, environment, originals):
    if argv[1:] != ['-test.run=^TestMixedMatrixWorkerProcessChild$']:
        return None
    if environment.get('WF_MATRIX_WORKER_CHILD') != '1':
        return None
    identity = environment.get('WF_MATRIX_WORKER_ID', '')
    match = re.fullmatch(r'matrix-process-([0-2])-generation-(\d+)', identity)
    base = Path(environment.get('WF_MATRIX_WORKER_BASE', ''))
    parent_root = Path(environment.get('WF_MATRIX_PROCESS_ROOT', ''))
    if (match is None or not base.is_absolute() or base.name != identity
            or not parent_root.is_absolute() or parent_root.resolve() != originals.resolve()):
        return None
    return dict(worker_id=identity, index=int(match[1]), generation=int(match[2]),
                base=str(base), process_root=str(parent_root))


def observe_workers(sdk_pid, root, seen, records, executable_cache=None):
    for pid in descendants(sdk_pid):
        proc = Path(f'/proc/{pid}')
        temporary = None
        try:
            start = proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19]
            token = (pid, start)
            if token in seen:
                continue
            argv = proc.joinpath('cmdline').read_bytes().decode().rstrip('\0').split('\0')
            # Read only the fixture identity fields, retaining no other environment.
            fields = {}
            for value in proc.joinpath('environ').read_bytes().split(b'\0'):
                key, separator, data = value.partition(b'=')
                if separator and key in {b'WF_MATRIX_WORKER_CHILD', b'WF_MATRIX_WORKER_ID',
                                         b'WF_MATRIX_WORKER_BASE', b'WF_MATRIX_PROCESS_ROOT'}:
                    fields[key.decode()] = data.decode()
            identity = worker_identity(argv, fields, root/'originals')
            if identity is None:
                continue
            temporary = root/f'observed-worker-{pid}-{start}.tmp'
            with proc.joinpath('exe').open('rb') as source, temporary.open('wb') as target:
                shutil.copyfileobj(source, target)
            after = proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19]
            if after != start:
                continue
            with temporary.open('rb') as file:
                digest = hashlib.file_digest(file, 'sha256').hexdigest()
            destination = root/'worker-executables'/f'{digest}.bin'
            destination.parent.mkdir(exist_ok=True)
            if not destination.exists():
                if executable_cache is None:
                    os.replace(temporary, destination)
                else:
                    from retained_input_cache import retain
                    retain(temporary, destination, executable_cache, executable=True)
            info = subprocess.check_output(['go', 'version', '-m', str(destination)], text=True)
            records.append(dict(identity, pid=pid, start_ticks=start, argv=argv,
                                actual_executable_sha256=digest,
                                captured=str(destination.relative_to(root)), build_info=info,
                                observed_at=datetime.now(timezone.utc).isoformat()))
            seen.add(token)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
