#!/usr/bin/env python3
"""Restore referenced worker-clock executables from a pinned, verified Git proof."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import tempfile


MEMBER = re.compile(r'raw/tier3-matrix-range/seed-([0-9]+)/tier3-mixed-journal/'
                    r'(clock-unshifted\.test|clock-[01]/integration-clock\.test)')


def digest(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def relative(value):
    if (not isinstance(value, str) or not value or PurePosixPath(value).is_absolute()
            or '..' in PurePosixPath(value).parts or str(PurePosixPath(value)) != value):
        raise ValueError('invalid relative proof path')
    return value


def member(value):
    match = MEMBER.fullmatch(relative(value))
    if match is None or not 1 <= int(match[1]) <= 200:
        raise ValueError('invalid worker-clock executable member')
    return match[2]


def safe_target(root, name):
    path = root / relative(name)
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink():
            raise ValueError('symlink in executable destination')
    return path


def restore(references, repository, destination, temporary_root=None):
    provider = references['provider']
    revision = provider['revision']
    if not isinstance(revision, str) or not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('provider must be pinned to a full Git revision')
    directory = relative(provider['directory'])

    def blob(name):
        return subprocess.check_output(['git', '-C', str(repository), 'show',
                                        revision + ':' + directory + '/' + relative(name)])

    manifest_bytes = blob('manifest.json')
    if hashlib.sha256(manifest_bytes).hexdigest() != provider['manifest_sha256']:
        raise ValueError('provider manifest digest differs')
    manifest = json.loads(manifest_bytes)
    summary_bytes = blob('summary.json')
    summary = json.loads(summary_bytes)
    if (hashlib.sha256(summary_bytes).hexdigest() != manifest['files']['summary.json']['sha256']
            or summary['source'] != provider['source'] or summary['job_id'] != provider['job']
            or manifest['archive_sha256'] != provider['archive_sha256']):
        raise ValueError('provider source, job, summary or archive binding differs')
    if not references['files']:
        raise ValueError('no executable references')
    needed = {}
    targets = {}
    destination = Path(destination).absolute()
    for name, record in references['files'].items():
        asset = member(name)
        original = record['provider_member']
        if member(original) != asset or manifest['files'][original] != {
                'sha256': record['sha256'], 'bytes': record['bytes']}:
            raise ValueError('executable reference differs from provider member')
        if type(record['bytes']) is not int or record['bytes'] <= 0:
            raise ValueError('invalid executable size')
        path = safe_target(destination, name)
        if path.exists() and (not path.is_file() or path.stat().st_size != record['bytes']
                              or digest(path) != record['sha256']):
            raise ValueError('existing executable differs; refusing overwrite')
        needed[original] = record
        targets[name] = path
    with tempfile.TemporaryDirectory(prefix='worker-clock-provider-', dir=temporary_root) as temporary:
        temporary = Path(temporary)
        archive = temporary / 'provider.tar.gz'
        combined = hashlib.sha256()
        total = 0
        with archive.open('wb') as output:
            for part in manifest['parts']:
                command = ['git', '-C', str(repository), 'show',
                           revision + ':' + directory + '/' + relative(part['path'])]
                process = subprocess.Popen(command, stdout=subprocess.PIPE)
                part_hash = hashlib.sha256()
                size = 0
                with process.stdout:
                    for block in iter(lambda: process.stdout.read(1024 * 1024), b''):
                        output.write(block)
                        part_hash.update(block)
                        combined.update(block)
                        size += len(block)
                if (process.wait() != 0 or size != part['bytes']
                        or part_hash.hexdigest() != part['sha256']):
                    raise ValueError('provider archive part failed verification')
                total += size
        if total != manifest['archive_bytes'] or combined.hexdigest() != provider['archive_sha256']:
            raise ValueError('provider canonical archive failed verification')
        captured = {}
        with tarfile.open(archive, 'r:gz') as tar:
            for item in tar:
                if item.name not in needed:
                    continue
                if item.name in captured or not (item.isfile() or item.islnk()):
                    raise ValueError('invalid or duplicate provider executable')
                path = temporary / ('executable-' + str(len(captured)))
                size = 0
                hashed = hashlib.sha256()
                with tar.extractfile(item) as source, path.open('xb') as output:
                    for block in iter(lambda: source.read(1024 * 1024), b''):
                        output.write(block)
                        hashed.update(block)
                        size += len(block)
                record = needed[item.name]
                if size != record['bytes'] or hashed.hexdigest() != record['sha256']:
                    raise ValueError('provider executable content failed verification')
                captured[item.name] = path
        if set(captured) != set(needed):
            raise ValueError('provider archive is missing an executable')
        # All provider bytes and all existing destinations are checked before writes.
        restored = 0
        aliases = {}
        for name, path in targets.items():
            safe_target(destination, name)
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                original = references['files'][name]['provider_member']
                if original in aliases:
                    os.link(aliases[original], path)
                else:
                    with captured[original].open('rb') as source, path.open('xb') as output:
                        for block in iter(lambda: source.read(1024 * 1024), b''):
                            output.write(block)
                path.chmod(0o755)
                restored += 1
            record = references['files'][name]
            if path.stat().st_size != record['bytes'] or digest(path) != record['sha256']:
                raise ValueError('restored executable failed readback verification')
            aliases[record['provider_member']] = path
    return dict(provider_revision=revision, provider_source=provider['source'],
                canonical_sha256=provider['archive_sha256'], destinations_verified=len(targets),
                distinct_provider_members=len(needed), files_created=restored,
                all_part_canonical_member_and_destination_hashes_verified=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--references', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--temporary-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(restore(json.loads(args.references.read_text()), args.repository,
                             args.destination, args.temporary_root), indent=2))
