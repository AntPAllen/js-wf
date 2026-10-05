"""Point-in-time /proc observations of owned matrix NATS descendants.

Polling scans every task thread for child processes and can miss short-lived processes. Never infer a server's executable from
its command name; retain bytes read from its live /proc executable instead.
"""
from pathlib import Path
import hashlib
import os
import shutil
import subprocess
from datetime import datetime, timezone


def server_identity(argv, originals):
    if argv.count('-n') != 1 or argv.count('-sd') != 1:
        return None
    try:
        name = argv[argv.index('-n') + 1]
        store = Path(argv[argv.index('-sd') + 1])
    except IndexError:
        return None
    if name not in {f'wf-process-{node}' for node in range(3)}:
        return None
    node = int(name[-1])
    if not store.is_absolute() or store.name != f'node-{node}':
        return None
    if not store.resolve().is_relative_to(originals.resolve()):
        return None
    return node, str(store)


def descendants(pid):
    found, pending = set(), [pid]
    while pending:
        parent = pending.pop()
        try:
            children = []
            for task in Path(f'/proc/{parent}/task').iterdir():
                try:
                    children.extend(task.joinpath('children').read_text().split())
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    continue
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        for child in map(int, children):
            if child not in found:
                found.add(child)
                pending.append(child)
    return found


def observe_servers(sdk_pid, root, seen, records):
    for pid in descendants(sdk_pid):
        proc = Path(f'/proc/{pid}')
        try:
            argv = proc.joinpath('cmdline').read_bytes().decode().rstrip('\0').split('\0')
            identity = server_identity(argv, root/'originals')
            if identity is None:
                continue
            # starttime is field22; splitting after comm's closing ')' avoids
            # spaces/parentheses in the executable name corrupting field offsets.
            start = proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19]
            token = (pid, start)
            if token in seen:
                continue
            temporary = root/f'observed-server-{pid}-{start}.tmp'
            with proc.joinpath('exe').open('rb') as source, temporary.open('wb') as target:
                shutil.copyfileobj(source, target)
            # Reject PID reuse across this observation before naming the bytes.
            after = proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19]
            if start != after:
                temporary.unlink()
                continue
            with temporary.open('rb') as file:
                digest = hashlib.file_digest(file, 'sha256').hexdigest()
            destination = root/'server-executables'/f'{digest}.bin'
            destination.parent.mkdir(exist_ok=True)
            if destination.exists():
                temporary.unlink()
            else:
                os.replace(temporary, destination)
            info = subprocess.check_output(['go', 'version', '-m', str(destination)], text=True)
            records.append(dict(pid=pid, start_ticks=start, node=identity[0], store=identity[1],
                                argv=argv, actual_executable_sha256=digest,
                                captured=str(destination.relative_to(root)), build_info=info,
                                observed_at=datetime.now(timezone.utc).isoformat()))
            seen.add(token)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            # Exit/restart races are expected; absence is not a success claim.
            continue
