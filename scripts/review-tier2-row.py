#!/usr/bin/env python3
"""Qualify one complete 200-seed Tier2 singleton row from provider/raw evidence.

Input is a fresh collection directory, with manifest.json containing `run`
(REST run metadata path), `generator` (REST generator job path), `jobs` and
`artifacts` (complete paginated REST listings), and 200 `seeds`.
Each seed has: seed, job, artifact, source_artifact, log, zip, source_zip,
root and source_root. Paths are collection-relative. ZIP bodies and extracted
files must match provider digests. This never substitutes stored shard reports
for independently reviewing every original raw fault/latency/history corpus.
"""
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import zipfile

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('tier2_row_shard', Path(__file__).with_name('check-tier2-journal-shard.py'))
shard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shard)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def terminal_job(job, run, name):
    require(type(job.get('id')) is int and job['id'] > 0
            and job.get('run_id') == run['id'] and job.get('head_sha') == run['head_sha']
            and job.get('name') == name
            and (job.get('status'), job.get('conclusion')) == ('completed', 'success'),
            'job source, identity, name or terminal success differs')


def artifact_identity(artifact, run, name):
    origin = artifact.get('workflow_run', {})
    require(type(artifact.get('id')) is int and artifact['id'] > 0
            and artifact.get('expired') is False and artifact.get('name') == name
            and origin.get('id') == run['id'] and origin.get('head_sha') == run['head_sha'],
            'artifact source, identity, name or expiry differs')


def bind(run, generator, seeds, run_id, revision, row):
    shard.row_contract(row)
    require(type(run_id) is int and run_id > 0 and re.fullmatch('[0-9a-f]{40}', revision),
            'expected run and exact source are required')
    require(type(run.get('id')) is int and run['id'] == run_id and run.get('head_sha') == revision
            and (run.get('status'), run.get('conclusion')) == ('completed', 'success')
            and run.get('event') == 'workflow_dispatch'
            and run.get('path') == '.github/workflows/tier2-matrix-journal.yml',
            'parent workflow identity, source or terminal success differs')
    terminal_job(generator, run, 'seeds')
    require(len(seeds) == 200 and all(type(s.get('seed')) is int for s in seeds)
            and sorted(s['seed'] for s in seeds) == list(range(1, 201)),
            'requires every seed 1 through 200 exactly once')
    jobs, artifacts = {generator['id']}, set()
    for seed in seeds:
        n = seed['seed']
        terminal_job(seed['job'], run, f'leader ({n})')
        require(seed['job']['id'] not in jobs, 'duplicate provider job identity')
        jobs.add(seed['job']['id'])
        for field, name in [('artifact', f'matrix-{row}-{n}-10m'),
                            ('source_artifact', f'workload-source-{row}-{n}')]:
            artifact_identity(seed[field], run, name)
            require(seed[field]['id'] not in artifacts, 'duplicate provider artifact identity')
            artifacts.add(seed[field]['id'])
        shard.bind(run, seed['job'], seed['artifact'], seed['log'], n, n, row, 'single')


def input_path(root, name):
    require(isinstance(name, str) and name and not PurePosixPath(name).is_absolute()
            and all(part not in ('', '.', '..') for part in name.split('/')), 'invalid collection path')
    path = root / name
    require(path.resolve().is_relative_to(root.resolve()), 'collection path escapes root')
    require(path.exists() and not any(p.is_symlink() for p in [path, *path.parents]),
            'missing or symlink collection input')
    return path


def check_listings(jobs, artifacts, generator, seeds):
    expected_jobs = [generator, *(s['job'] for s in seeds)]
    expected_artifacts = [a for s in seeds for a in (s['artifact'], s['source_artifact'])]
    for listing, field, expected, keys in [
            (jobs, 'jobs', expected_jobs, ('id', 'run_id', 'head_sha', 'name', 'status', 'conclusion')),
            (artifacts, 'artifacts', expected_artifacts, ('id', 'name', 'expired', 'digest', 'workflow_run'))]:
        rows = listing.get(field, [])
        require(type(listing.get('total_count')) is int and listing['total_count'] == len(expected)
                and len(rows) == len(expected), 'incomplete or extra provider listing')
        normalize = lambda row: {k:row.get(k) for k in keys}
        by_id = {row.get('id'):normalize(row) for row in rows}
        require(len(by_id) == len(rows) and by_id == {row['id']:normalize(row) for row in expected},
                'provider listing differs from individual metadata')


def zip_inventory(archive, artifact):
    with archive.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    require(artifact.get('digest') == 'sha256:' + digest, 'provider ZIP digest differs or is missing')
    files = {}
    with zipfile.ZipFile(archive) as provider:
        for member in provider.infolist():
            name = member.filename
            require(not member.flag_bits & 1 and not stat.S_ISLNK(member.external_attr >> 16),
                    'encrypted or symlink provider member')
            require(name and not name.startswith('/') and '\\' not in name
                    and all(p not in ('', '.', '..') for p in name.rstrip('/').split('/')),
                    'unsafe provider member')
            if member.is_dir():
                continue
            require(name not in files, 'duplicate provider member')
            with provider.open(member) as stream:
                files[name] = hashlib.file_digest(stream, 'sha256').hexdigest()
    require(files, 'empty provider ZIP')
    return files


def check_provider(archive, artifact, root):
    files = zip_inventory(archive, artifact)
    require(files == shard.shared.inventory(root), 'extracted corpus differs from provider ZIP')
    return files


def expected_source(revision, repo=REPO):
    names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', '-z', revision], cwd=repo).decode().split('\0')[:-1]
    selected = [n for n in names if not n.startswith('docs/') or n.endswith(('.go', '.py', '.yml'))]
    require(all('\n' not in n and '\r' not in n for n in selected), 'unsupported source path')
    requests = ''.join(revision + ':' + n + '\n' for n in selected).encode()
    body = subprocess.check_output(['git', 'cat-file', '--batch'], cwd=repo, input=requests)
    stream = io.BytesIO(body)
    inputs = {}
    for name in selected:
        header = stream.readline().split()
        require(len(header) == 3 and header[1] == b'blob', 'selected source is not a blob')
        size = int(header[2]); data = stream.read(size)
        require(len(data) == size and stream.read(1) == b'\n', 'incomplete Git source body')
        inputs[name] = dict(bytes=size, sha256=hashlib.sha256(data).hexdigest())
    require(not stream.read(), 'unexpected Git source body')
    return dict(schema='js-wf-workload-source-checkout-v1', revision=revision,
                tracked_paths=len(names), materialized_paths=len(selected),
                materialized_bytes=sum(v['bytes'] for v in inputs.values()),
                omitted_docs_paths=len(names)-len(selected), inputs=inputs)


def check_source(actual, expected):
    require(all(actual.get(k) == v for k, v in expected.items()),
            'source selection does not match complete executed Git inputs')
    for key in ('tracked_paths', 'materialized_paths', 'materialized_bytes', 'omitted_docs_paths'):
        require(type(actual.get(key)) is int, 'source counter must be an integer')
    require(all(type(v.get('bytes')) is int for v in actual['inputs'].values()),
            'source input size must be an integer')


def review(root, run_id, revision, row, model_root=None, temporary_root=None):
    root = Path(root).resolve()
    before = shard.shared.inventory(root)
    manifest = json.loads((root/'manifest.json').read_text())
    read = lambda name: json.loads(input_path(root, name).read_text())
    run, generator = read(manifest['run']), read(manifest['generator'])
    seeds = []
    for item in manifest['seeds']:
        seed = dict(item)
        for field in ('job', 'artifact', 'source_artifact'):
            seed[field] = read(item[field])
        seed['log'] = input_path(root, item['log']).read_text()
        seeds.append(seed)
    bind(run, generator, seeds, run_id, revision, row)
    check_listings(read(manifest['jobs']), read(manifest['artifacts']), generator, seeds)
    source = expected_source(revision)
    reports = []
    used_paths = set()
    for item, seed in sorted(zip(manifest['seeds'], seeds), key=lambda pair: pair[0]['seed']):
        for field in ('job', 'artifact', 'source_artifact', 'log', 'zip', 'source_zip', 'root', 'source_root'):
            path = input_path(root, item[field])
            require(path not in used_paths, 'reused collection evidence path')
            used_paths.add(path)
        raw_root, source_root = input_path(root, item['root']), input_path(root, item['source_root'])
        check_provider(input_path(root, item['zip']), seed['artifact'], raw_root)
        provider_source = check_provider(input_path(root, item['source_zip']), seed['source_artifact'], source_root)
        require(set(provider_source) == {'workload-source.json'}
                and provider_source == shard.shared.inventory(source_root),
                'extracted source selection differs from provider ZIP')
        check_source(json.loads((source_root/'workload-source.json').read_text()), source)
        n = seed['seed']
        report = shard.review(run, seed['job'], seed['artifact'], seed['log'], raw_root, n, n,
                              temporary_root, model_root, row=row, job_layout='single')
        require(report['shard_qualified'] is True and report['first'] == report['last'] == n
                and report['revision'] == revision, 'raw seed reviewer did not qualify requested evidence')
        reports.append(report)
        print(f'RAW_SEED_REVIEWED row={row} seed={n}', flush=True)
    require(shard.shared.inventory(root) == before, 'collection changed during full row review')
    return dict(schema='js-wf-tier2-full-row-review-v1', run=run_id, revision=revision, row=row,
                first=1, last=200, seed_count=200, duration_seconds_per_seed=600,
                full_row_qualified=True, clears_selected_tier2_200_seed_row=True,
                qualifies_full_matrix=False, clears_tier3_24_hour_soak=False,
                input_sha256=before, seeds=reports,
                reviewer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                scope='One complete recorded-source singleton row only; every raw seed, provider ZIP and source selection independently checked. No captured workload binary or physical stores are provided by this workflow.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-id', type=int, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--row', choices=tuple(shard.ROWS), required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--model-root', type=Path)
    parser.add_argument('--temporary-root', type=Path)
    args = parser.parse_args()
    require(not args.output.exists() and not args.output.resolve().is_relative_to(args.root.resolve()),
            'output must be fresh and outside original collection')
    result = review(args.root, args.run_id, args.revision, args.row, args.model_root, args.temporary_root)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps({k:result[k] for k in ('run', 'revision', 'row', 'seed_count', 'full_row_qualified')}))
