"""Share captured immutable input bytes before native execution.

Original broker media are never deduplicated or modified. Each fixture still
contains ordinary files, so a complete fixture archive is independently usable.
"""
import hashlib
import os
from pathlib import Path
import shutil


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def retain(source, destination, cache=None, *, executable=False):
    source, destination = Path(source), Path(destination)
    if source.is_symlink() or not source.is_file() or destination.exists():
        raise ValueError('require a regular input and fresh destination')
    destination.parent.mkdir(parents=True, exist_ok=True)
    if cache is None:
        shutil.copy2(source, destination)
        return
    cache = Path(cache)
    if not cache.is_absolute() or cache.is_symlink():
        raise ValueError('cache must be an absolute real directory')
    cache.mkdir(parents=True, exist_ok=True)
    before = source.stat()
    sha = digest(source)
    suffix = str(before.st_mode & 0o777)
    if not executable:
        suffix += '.' + str(before.st_mtime_ns)
    captured = cache / (sha + '.' + suffix)
    if captured.exists():
        if captured.is_symlink() or not captured.is_file() or digest(captured) != sha:
            raise ValueError('existing captured input differs')
        stat = captured.stat()
        if stat.st_mode & 0o777 != before.st_mode & 0o777:
            raise ValueError('captured input mode differs')
        if not executable and stat.st_mtime_ns != before.st_mtime_ns:
            raise ValueError('captured input timestamp differs')
    else:
        # The campaign is sequential. Refuse competing creation rather than
        # replacing any previous captured bytes.
        with captured.open('xb') as target, source.open('rb') as stream:
            shutil.copyfileobj(stream, target)
        shutil.copystat(source, captured)
        if digest(captured) != sha:
            raise ValueError('input changed while retaining bytes')
    after = source.stat()
    if (before.st_size, before.st_mtime_ns, before.st_mode) != (after.st_size, after.st_mtime_ns, after.st_mode):
        raise ValueError('input changed during capture')
    os.link(captured, destination)
