#!/usr/bin/env python3
"""Collect a terminal 200-seed singleton Tier2 row into a fresh review directory.

Read-only GitHub REST access; no workflow dispatch/cancellation or native run.
Incomplete/failed collection is retained and never qualifies. The separate
review-tier2-row.py performs complete source/raw/history/latency qualification.
"""
import argparse
import datetime
import importlib.util
import json
from pathlib import Path
import subprocess
import zipfile

spec = importlib.util.spec_from_file_location('tier2_row_review', Path(__file__).with_name('review-tier2-row.py'))
reviewer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reviewer)
REPOSITORY = 'AntPAllen/js-wf'


def api(path, pages=False):
    command = ['gh', 'api', f'repos/{REPOSITORY}/'+path]
    if pages:
        command += ['--paginate', '--slurp']
    return json.loads(subprocess.check_output(command))


def listing(path, field):
    pages = api(path, pages=True)
    reviewer.require(pages and all(p.get('total_count') == pages[0].get('total_count') for p in pages),
                     'provider pagination count changed')
    items = [item for page in pages for item in page[field]]
    reviewer.require(type(pages[0].get('total_count')) is int and len(items) == pages[0]['total_count'],
                     'provider pagination is incomplete')
    return dict(total_count=len(items), **{field:items})


def extract_verified(archive, artifact, destination):
    # Check complete digest, duplicate/unsafe paths and member bytes before writes.
    expected = reviewer.zip_inventory(archive, artifact)
    reviewer.require(not destination.exists(), 'extracted destination must be fresh')
    destination.mkdir()
    with zipfile.ZipFile(archive) as provider:
        for name in expected:
            path = destination/name
            path.parent.mkdir(parents=True, exist_ok=True)
            with provider.open(name) as source, path.open('xb') as target:
                while block := source.read(1 << 20):
                    target.write(block)
    reviewer.check_provider(archive, artifact, destination)


def collect(root, run_id, revision, row):
    reviewer.require(not root.exists(), 'collection root must be fresh')
    run = api(f'actions/runs/{run_id}')
    reviewer.require(run.get('id') == run_id and run.get('head_sha') == revision
                     and (run.get('status'),run.get('conclusion')) == ('completed','success')
                     and run.get('event') == 'workflow_dispatch'
                     and run.get('path') == '.github/workflows/tier2-matrix-journal.yml',
                     'requested original run is not terminal successful at pinned source; no restart is authorized')
    jobs = listing(f'actions/runs/{run_id}/jobs?filter=all&per_page=100', 'jobs')
    artifacts = listing(f'actions/runs/{run_id}/artifacts?per_page=100', 'artifacts')
    job_by_name = {j['name']:j for j in jobs['jobs']}
    artifact_by_name = {a['name']:a for a in artifacts['artifacts']}
    reviewer.require(len(job_by_name) == 201 and len(jobs['jobs']) == 201
                     and len(artifact_by_name) == len(artifacts['artifacts']) == 400,
                     'requires exactly 201 distinct jobs and 400 distinct source/raw artifacts')
    reviewer.require(set(job_by_name) == {'seeds', *(f'leader ({n})' for n in range(1,201))}
                     and set(artifact_by_name) == {name for n in range(1,201)
                         for name in (f'matrix-{row}-{n}-10m',f'workload-source-{row}-{n}')},
                     'provider coverage does not match requested singleton row')
    for j in jobs['jobs']:
        reviewer.terminal_job(j,run,j['name'])
    for a in artifacts['artifacts']:
        reviewer.artifact_identity(a,run,a['name'])
    root.mkdir(parents=True)
    write = lambda name, obj: (root/name).write_text(json.dumps(obj,indent=2)+'\n')
    manifest = dict(run='run.json', generator='generator.json',jobs='jobs.json',artifacts='artifacts.json',seeds=[])
    write('run.json',run);write('generator.json',job_by_name['seeds']);write('jobs.json',jobs);write('artifacts.json',artifacts)
    try:
        for n in range(1,201):
            directory = root/f'seed-{n}';directory.mkdir()
            job = job_by_name[f'leader ({n})']
            paths = dict(seed=n,job=f'seed-{n}/job.json',artifact=f'seed-{n}/artifact.json',
                         source_artifact=f'seed-{n}/source-artifact.json',log=f'seed-{n}/job.log',
                         zip=f'seed-{n}/raw.zip',source_zip=f'seed-{n}/source.zip',
                         root=f'seed-{n}/raw',source_root=f'seed-{n}/source')
            write(paths['job'],job)
            with (root/paths['log']).open('xb') as log:
                subprocess.run(['gh','api','--allow-escape-sequences',f'repos/{REPOSITORY}/actions/jobs/{job["id"]}/logs'],
                               stdout=log,check=True)
            for field,name,zip_field,destination in [
                    ('artifact',f'matrix-{row}-{n}-10m','zip','root'),
                    ('source_artifact',f'workload-source-{row}-{n}','source_zip','source_root')]:
                artifact = artifact_by_name[name];write(paths[field],artifact)
                with (root/paths[zip_field]).open('xb') as body:
                    subprocess.run(['gh','api','--allow-escape-sequences',
                                    f'repos/{REPOSITORY}/actions/artifacts/{artifact["id"]}/zip'],stdout=body,check=True)
                extract_verified(root/paths[zip_field],artifact,root/paths[destination])
            manifest['seeds'].append(paths)
            write('manifest.json',manifest)
            print(f'COLLECTED_PROVIDER_SEED row={row} seed={n}',flush=True)
        write('collection.json',dict(complete=True,qualified=False,run=run_id,revision=revision,row=row,
                                    observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                    scope='Complete provider collection only; independent raw row reviewer remains required.'))
    except BaseException as error:
        write('collection.json',dict(complete=False,qualified=False,run=run_id,revision=revision,row=row,
                                    error=str(error),scope='Partial collection retained; no native or row qualification.'))
        raise


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--run-id',type=int,required=True)
    parser.add_argument('--revision',required=True)
    parser.add_argument('--row',choices=tuple(reviewer.shard.ROWS),required=True)
    args=parser.parse_args()
    reviewer.require(type(args.run_id) is int and args.run_id > 0, 'invalid run identity')
    reviewer.require(len(args.revision)==40 and all(c in '0123456789abcdef' for c in args.revision),'invalid exact source')
    collect(args.root.resolve(),args.run_id,args.revision,args.row)
