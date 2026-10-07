#!/usr/bin/env python3
"""Independently verify a local R5 producer archive without restoring broker stores."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

import parallel_soak_profile


def sha(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):value.update(block)
    return value.hexdigest()


def review(root, repo):
    manifest=json.loads((root/'archive-manifest.json').read_text())
    archive=root/'originals.tar.gz'
    if sha(archive)!=manifest['archive_sha256']:
        raise ValueError('original archive SHA mismatch')
    with tarfile.open(archive) as tar:
        members=tar.getmembers()
        names={m.name for m in members}
        if len(names)!=len(members) or names!=set(manifest['files']):
            raise ValueError('duplicate/missing/unexpected archive member')
        for member in members:
            path=Path(member.name)
            if not member.isfile() or path.is_absolute() or '..' in path.parts:
                raise ValueError('unsafe archive member')
            digest=hashlib.sha256()
            with tar.extractfile(member) as stream:
                for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
            if digest.hexdigest()!=manifest['files'][member.name]:
                raise ValueError('member SHA mismatch:'+member.name)
        def read(name):return json.load(tar.extractfile(name))
        state=read('execution.json')
        if state['status']!='row_verified' or state['test_exit_code']!=0 or state['clears_full_tier3_release'] is not False:
            raise ValueError('row is failed/incomplete or claims incorrect release scope')
        environment=read('test-environment.json')
        events=[json.loads(line) for line in tar.extractfile('events.jsonl').read().decode().splitlines() if line.strip()]
        parallel_profile=parallel_soak_profile.validate(state,environment,events)
        before=read('source-before.json');after=read('source-after.json')
        revision=state['source']
        if before!=after or before['revision']!=revision:
            raise ValueError('compiled source changed during run')
        tracked=subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines()
        expected=[n for n in tracked if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
        if set(expected)!=set(before['files']):
            raise ValueError('incomplete compiled source inventory')
        for name in expected:
            original=subprocess.check_output(['git','show',revision+':'+name],cwd=repo)
            if hashlib.sha256(original).hexdigest()!=before['files'][name] or manifest['files'].get('source/'+name)!=before['files'][name]:
                raise ValueError('compiled checkout does not match Git:'+name)
        binary=read('binary.json')
        if manifest['files']['integration.test']!=binary['sha256'] or binary['race'] is not state['race']:
            raise ValueError('executable/provenance mismatch')
        commands=read('commands.json')
        executed=[c for c in commands if 'test2json' in c['command']]
        builds=[c for c in commands if c['command'][:2]==['go','test']]
        if len(executed)!=1 or len(builds)!=1:
            raise ValueError('missing/duplicate executable build or execution')
        built=builds[0]['command'][builds[0]['command'].index('-o')+1]
        if executed[0]['command'][executed[0]['command'].index('js-wf/integration')+1]!=built:
            raise ValueError('executed binary differs from retained build')
        if ('-race' in builds[0]['command']) is not state['race']:
            raise ValueError('requested race mode differs from actual build')
        with tempfile.TemporaryDirectory() as directory:
            temporary=Path(directory)
            # Restore only verification inputs and executable; stores stay archived.
            for member in members:
                name=member.name
                needed=(name=='integration.test' or name in ('events.jsonl','result.json') or
                        (name.startswith('fixture/') and name.endswith(('.json','.jsonl'))) or
                        (name.startswith('source/scripts/') and name.endswith('.py')))
                if needed:
                    target=temporary/name;target.parent.mkdir(parents=True,exist_ok=True)
                    with tar.extractfile(member) as original,target.open('wb') as out:
                        for block in iter(lambda:original.read(1024*1024),b''):out.write(block)
            info=subprocess.check_output(['go','version','-m',str(temporary/'integration.test')],text=True)
            if info.splitlines()[1:]!=binary['build_info'].splitlines()[1:] or ('-race=true' in info) is not binary['race']:
                raise ValueError('retained executable build settings mismatch')
            scripts=temporary/'source/scripts'
            output=temporary/'regenerated.json'
            args=['python3',str(scripts/'check-tier3-journal-row.py'),'--root',str(temporary/'fixture'),
                  '--events',str(temporary/'events.jsonl'),'--row',state['row'],'--duration',state['duration'],
                  '--expected-seed',str(state['seed']),'--output',str(output),'--require-checkpoint-audits']
            if state.get('bulk_final_latency', False):
                args+=['--require-bulk-final-latency']
                if state.get('compare_bulk_point', False):
                    args+=['--require-bulk-point-equivalence']
            if state['row'].startswith('server_clock_'):
                args+=['--require-clock-timer-cut','--require-common-timer-clock']
            if state['row']=='rolling_upgrade':
                args+=['--expected-upgrade-shutdown',state['upgrade_shutdown']]
                if state['upgrade_start_gap']:args+=['--require-upgrade-start-gap','--require-start-scan-progress']
            subprocess.run(args,check=True,stdout=subprocess.DEVNULL)
            if output.read_bytes()!=(temporary/'result.json').read_bytes():
                raise ValueError('raw row report does not reproduce')
            report=json.loads(output.read_text())
            for script,name in [('explain-tier3-events.py','event-explanations.json'),('review-tier3-fencing.py','fencing-timeline-review.json')]:
                subprocess.run(['python3',str(scripts/script),'--root',str(temporary/'fixture'),'--output',str(output)],check=True,stdout=subprocess.DEVNULL)
                if output.read_bytes()!=(temporary/'fixture'/name).read_bytes():
                    raise ValueError('raw event/fencing report does not reproduce')
            explanations=json.loads((temporary/'fixture/event-explanations.json').read_text())
        return dict(accepted_row=True,parallel_profile=parallel_profile,source=revision,row=state['row'],seed=state['seed'],
                    duration=state['duration'],archive_members_verified=len(members),source_files_verified=len(expected),
                    binary_sha256=binary['sha256'],race=binary['race'],invocations=report['invocations'],
                    journal_entries=report['journal_entries'],confirmed_faults=report['confirmed_faults'],
                    fencing_records=explanations['fencing_records'],all_reports_regenerate_identically=True,
                    scope='Requested single R5 row duration; stores hash-verified but not independently reopened. No full-matrix or full-release qualification.',
                    clears_full_tier3_release=False)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=review(args.root.resolve(),args.repo.resolve())
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
