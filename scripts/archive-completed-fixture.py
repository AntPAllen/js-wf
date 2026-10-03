#!/usr/bin/env python3
"""Losslessly archive completed broker stores with durable per-file readback."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--accepted-report', type=Path, required=True)
a = p.parse_args()
root = a.root.resolve()
report_bytes = (root/'report.json').read_bytes()
assert report_bytes == gzip.decompress(a.accepted_report.read_bytes()), 'accepted report differs'
report = json.loads(report_bytes)
assert report['status'] == 'completed'
for fd in Path('/proc').glob('[0-9]*/fd/*'):
    try: target = os.readlink(fd)
    except OSError: continue
    assert not target.startswith(str(root)+'/'), (fd, target)
files = sorted((p for d in root.glob('node-*') if d.is_dir() for p in d.rglob('*') if p.is_file()), key=lambda p:(p.stat().st_size,str(p)))
assert files and not any(p.is_symlink() for p in files)
archive = root/'broker-store-archive'
archive.mkdir(exist_ok=False)
def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
count = raw = compressed = 0
manifest = archive/'manifest.jsonl'
with manifest.open('x') as ledger:
    for source in files:
        before = source.stat()
        relative = source.relative_to(root)
        dest = archive/(str(relative)+'.gz')
        dest.parent.mkdir(parents=True,exist_ok=True)
        temporary = dest.with_name(dest.name+'.tmp')
        digest = hashlib.sha256()
        with source.open('rb') as reader, temporary.open('xb') as writer:
            with gzip.GzipFile(filename='',mode='wb',compresslevel=1,mtime=0,fileobj=writer) as zipper:
                while chunk := reader.read(1024*1024):
                    digest.update(chunk)
                    zipper.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        readback = hashlib.sha256()
        size = 0
        with gzip.open(temporary,'rb') as reader:
            while chunk := reader.read(1024*1024):
                readback.update(chunk)
                size += len(chunk)
        after = source.stat()
        assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
        assert size==before.st_size and readback.digest()==digest.digest()
        temporary.rename(dest)
        sync_dir(dest.parent)
        entry = dict(original=str(relative),archive=str(dest.relative_to(root)),
                     sha256=digest.hexdigest(),bytes=size,archive_bytes=dest.stat().st_size,
                     archive_sha256=hashlib.sha256(dest.read_bytes()).hexdigest(),
                     mode=stat.S_IMODE(before.st_mode),mtime_ns=before.st_mtime_ns)
        ledger.write(json.dumps(entry,separators=(',',':'))+'\n')
        ledger.flush()
        os.fsync(ledger.fileno())
        sync_dir(archive)
        source.unlink()
        sync_dir(source.parent)
        count += 1
        raw += size
        compressed += entry['archive_bytes']
        if count%300==0:
            print(json.dumps(dict(files=count,original_bytes=raw,compressed_bytes=compressed)),flush=True)
assert (root/'report.json').read_bytes()==report_bytes
summary = dict(root=str(root),revision=report['revision'],report_sha256=hashlib.sha256(report_bytes).hexdigest(),
               files=count,original_bytes=raw,compressed_bytes=compressed,freed_bytes=raw-compressed,
               manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
               all_decompressed_bytes_verified=True,status='complete')
(archive/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary),flush=True)
