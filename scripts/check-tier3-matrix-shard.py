#!/usr/bin/env python3
"""Review one complete R5 shard without qualifying its parent or full matrix."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


full = load('shard_full_matrix', 'check-tier3-full-matrix.py')
rows = load('shard_rows', 'check-tier3-journal-row.py')
CAPTURED = {'worker_clock', 'rolling_upgrade', 'server_clock_ahead', 'server_clock_behind'}
REPO = Path(__file__).resolve().parent.parent


def bind(run, job, artifact, log, row, first, last):
    if row not in rows.TESTS or type(first) is not int or type(last) is not int or not 1 <= first <= last <= 200:
        raise ValueError('invalid release row or seed range')
    source = run.get('head_sha', '')
    if not isinstance(source, str) or not re.fullmatch('[0-9a-f]{40}', source):
        raise ValueError('missing exact executed source')
    if (type(run.get('id')) is not int or type(job.get('id')) is not int
            or job.get('run_id') != run['id'] or job.get('head_sha') != source
            or (job.get('status'), job.get('conclusion')) != ('completed', 'success')):
        raise ValueError('required job is not terminal successful at the bound source/run')
    span = str(first) if first == last else f'{first}-{last}'
    names = {f'journal ({row}, {span})'}
    if first == last:
        names.add(f'journal ({first})')
    if job.get('name') not in names or source not in log:
        raise ValueError('job name or exact checkout differs from requested shard')
    expected_names = {f'tier3-{row}-seed-{span}'}
    if row in CAPTURED:
        expected_names.add(f'rolling-original-stores-{row}-seed-{span}')
    origin = artifact.get('workflow_run', {})
    if (type(artifact.get('id')) is not int or artifact.get('name') not in expected_names
            or artifact.get('expired') is not False or origin.get('id') != run['id']
            or origin.get('head_sha') != source):
        raise ValueError('artifact identity, expiry, source or parent run differs')
    headers = re.findall(r'TIER3_CAMPAIGN row=(\w+) seed=(\d+) duration=(\S+)', log)
    if headers != [(row, str(seed), '10m') for seed in range(first, last+1)]:
        raise ValueError('missing, duplicated or substituted actual ten-minute executions')
    return source


def source_hashes(source):
    """Hash the exact inventory captured by capture-tier3-clock-source.py."""
    listing = subprocess.check_output(['git', 'ls-tree', '-rz', '--full-tree', source], cwd=REPO)
    selected = []
    for record in listing.split(b'\0'):
        if not record:
            continue
        description, raw_path = record.split(b'\t', 1)
        mode, kind, oid = description.split()
        path = raw_path.decode()
        if path.endswith(('.go', '.py', '.yml')) or path in ('go.mod', 'go.sum'):
            if kind != b'blob' or mode not in (b'100644', b'100755'):
                raise ValueError('nonregular captured source')
            selected.append((path, oid))
    if not {'go.mod', 'go.sum'} <= {name for name, _ in selected}:
        raise ValueError('incomplete source inventory')
    data = subprocess.check_output(['git', 'cat-file', '--batch'],
                                   input=b''.join(oid+b'\n' for _, oid in selected), cwd=REPO)
    position, hashes = 0, {}
    for name, oid in selected:
        end = data.index(b'\n', position)
        actual, kind, size = data[position:end].split()
        size = int(size)
        if actual != oid or kind != b'blob':
            raise ValueError('Git source readback differs')
        position = end+1
        body = data[position:position+size]
        if len(body) != size or data[position+size:position+size+1] != b'\n':
            raise ValueError('truncated Git source readback')
        hashes[name] = hashlib.sha256(body).hexdigest()
        position += size+1
    if position != len(data):
        raise ValueError('unexpected Git source readback tail')
    return hashes


def inventory(root):
    if root.is_symlink():
        raise ValueError('symlink artifact root')
    files = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError('symlink in original evidence')
        if path.is_file():
            digest = hashlib.sha256()
            with path.open('rb') as file:
                while block := file.read(1024*1024):
                    digest.update(block)
            files[str(path.relative_to(root))] = digest.hexdigest()
    return files


def review(run, job, artifact, log, root, row, first, last,
           require_clock=False, require_upgrade_start_gap=False, upgrade_shutdown=None):
    source = bind(run, job, artifact, log, row, first, last)
    if row.startswith('server_clock_') and not require_clock:
        raise ValueError('release clock shards require positive cuts and five common-clock probes')
    if row == 'rolling_upgrade' and (not require_upgrade_start_gap or upgrade_shutdown not in ('sigkill', 'ldm')):
        raise ValueError('release upgrade shards require explicit forced-gap and shutdown profiles')
    if require_clock and not row.startswith('server_clock_'):
        raise ValueError('clock admission requested for a non-clock shard')
    if (require_upgrade_start_gap or upgrade_shutdown is not None) and row != 'rolling_upgrade':
        raise ValueError('upgrade profile requested for a non-upgrade shard')
    before = inventory(root)
    events = list(root.rglob('tier3-mixed-journal-events.jsonl'))
    if len(events) != last-first+1:
        raise ValueError('missing, duplicate or unexpected seed evidence')
    locations = {}
    for path in events:
        match = re.fullmatch(r'seed-(\d+)', path.parent.name)
        seed = int(match[1]) if match else first if first == last else None
        if seed not in range(first, last+1) or seed in locations:
            raise ValueError('ambiguous or unexpected seed layout')
        locations[seed] = path
    sources = source_hashes(source) if row in CAPTURED else None
    reports = []
    with tempfile.TemporaryDirectory(prefix='tier3-shard-review-') as temporary:
        for seed in range(first, last+1):
            path = locations[seed]
            fixture = path.parent/'tier3-mixed-journal'
            flat_store_archive = False
            if not fixture.is_dir():
                # Focused original-store archives place the fixture's contents
                # at their root. Raw uploads and range archives use the nested
                # layout; missing nested data there must still be rejected.
                span = str(first) if first == last else f'{first}-{last}'
                if (row not in CAPTURED or first != last or path.parent != root
                        or artifact['name'] != f'rolling-original-stores-{row}-seed-{span}'):
                    raise ValueError('missing expected nested fixture directory')
                fixture = root
                flat_store_archive = True
            if sources is not None:
                prefix = row.replace('_', '-')
                proofs = [json.loads((fixture/f'{prefix}-source-{stage}.json').read_text())
                          for stage in ('before', 'after')]
                expected = dict(revision=source, clean=True, files=sources)
                if proofs != [expected, expected]:
                    raise ValueError('captured pre/post inputs differ from exact Git source inventory')
            reports.append(full.verify_seed(path, Path(temporary)/'review.json', row, seed, '10m',
                                            require_clock, require_upgrade_start_gap, upgrade_shutdown,
                                            **({'fixture_root': fixture} if flat_store_archive else {})))
    if inventory(root) != before:
        raise ValueError('original evidence changed during review')
    return dict(source=source, run_id=run['id'], job_id=job['id'], artifact_id=artifact['id'],
                row=row, first_seed=first, last_seed=last, seeds=len(reports), duration_seconds=600,
                reports=reports, artifact_sha256=before, verified_source_sha256=sources,
                invocations=sum(item['invocations'] for item in reports),
                journal_entries=sum(item['journal_entries'] for item in reports),
                confirmed_faults=sum(item['confirmed_faults'] for item in reports),
                shard_qualified=True, parent_conclusion=run.get('conclusion'),
                qualifies_parent_campaign=False, qualifies_full_row=False,
                clears_full_tier3_release=False,
                scope='Complete requested ten-minute shard only. Parent failure/remaining seeds, full matrix and 24-hour gates remain unchanged. Any provided physical stores are hashed, not reopened; history/integrity/drain assertions retain their named-test provenance boundary, with no independent Porcupine rerun.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('run', 'job', 'artifact-metadata', 'job-log', 'artifact-root', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--row', choices=tuple(rows.TESTS), required=True)
    parser.add_argument('--first', type=int, required=True)
    parser.add_argument('--last', type=int, required=True)
    parser.add_argument('--require-admitted-clock', action='store_true')
    parser.add_argument('--require-upgrade-start-gap', action='store_true')
    parser.add_argument('--expected-upgrade-shutdown', choices=('sigkill', 'ldm'))
    args = parser.parse_args()
    if args.output.exists() or args.output.resolve().is_relative_to(args.artifact_root.resolve()):
        parser.error('use a fresh output outside the original artifact root')
    result = review(json.loads(args.run.read_text()), json.loads(args.job.read_text()),
                    json.loads(args.artifact_metadata.read_text()), args.job_log.read_text(),
                    args.artifact_root, args.row, args.first, args.last,
                    args.require_admitted_clock, args.require_upgrade_start_gap,
                    args.expected_upgrade_shutdown)
    result['input_sha256'] = {name: hashlib.sha256(path.read_bytes()).hexdigest()
                              for name, path in [('run', args.run), ('job', args.job),
                                                 ('artifact_metadata', args.artifact_metadata), ('job_log', args.job_log)]}
    result['reviewer_sha256'] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in Path(__file__).parent.glob('*.py')}
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({key: value for key, value in result.items()
                      if key not in ('reports', 'artifact_sha256', 'verified_source_sha256', 'reviewer_sha256')}, indent=2))
