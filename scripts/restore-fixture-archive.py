#!/usr/bin/env python3
"""Restore a byte-verified fixture archive into a new directory."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path


def relative_path(value):
    path = Path(value)
    if path.is_absolute() or not path.parts or '..' in path.parts:
        raise ValueError('archive path must be relative without parent traversal')
    return path


def restore(source, manifest, destination):
    # Never overwrite a live store or an existing restoration.
    destination.mkdir(parents=True, exist_ok=False)
    count = total = 0
    for line in manifest.read_text().splitlines():
        entry = json.loads(line)
        original = relative_path(entry['original'])
        archive = relative_path(entry['archive'])
        compressed = (source/archive).resolve()
        compressed.relative_to(source.resolve())
        output = destination/original
        output.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        with gzip.open(compressed, 'rb') as reader, output.open('xb') as writer:
            while chunk := reader.read(1024*1024):
                digest.update(chunk)
                size += len(chunk)
                writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        if size != entry['bytes'] or digest.hexdigest() != entry['sha256']:
            output.unlink()
            raise ValueError(f'archive bytes differ for {original}')
        output.chmod(entry['mode'])
        os.utime(output, ns=(entry['mtime_ns'], entry['mtime_ns']))
        count += 1
        total += size
    return dict(files=count, restored_bytes=total, all_sha256_verified=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(restore(args.source_root, args.manifest, args.destination)))


if __name__ == '__main__':
    main()
