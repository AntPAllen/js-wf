#!/usr/bin/env python3
"""Review all six closed 500-child combined cuts, including local physical queues."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

SCRIPTS=Path(__file__).resolve().parent
REPO=SCRIPTS.parent

def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

physical=load('combined_physical',SCRIPTS/'review-tier2-copied-audit.py')
gate=load('combined_gate',SCRIPTS/'check-fanout-boundary-matrix.py')
require,read,sha,ns=physical.require,physical.read,physical.sha,physical.timestamp_ns


def review_drain(drain,healed):
    require(drain['workers_joined'] is True and type(drain['elapsed_ns']) is int and
            0<drain['elapsed_ns']<300_000_000_000, 'workers not joined or original drain budget exceeded')
    peers=drain['all_three_peers']
    require(len(peers)==3 and all(isinstance(p.get('physical'),dict) for p in peers),
            'incomplete API and local physical peer census')
    physical.review_physical_peers(dict(physical_queue_peers=[p['physical'] for p in peers]),True)
    deadline=ns(drain['case_deadline'])
    for peer in peers:
        local=peer['physical']
        require(ns(local['started'])>=ns(healed) and ns(local['finished'])<=deadline,
                'local physical witness outside original post-fault case interval')
        info=peer['queue'];state=info['state'];cluster=info['cluster']
        require(type(state['messages']) is int and state['messages']==0 and state['consumer_count']==64 and
                info['config']['num_replicas']==3 and len(cluster['replicas'])==2 and
                all(p['current'] and not p.get('offline',False) for p in cluster['replicas']) and
                ns(info['ts'])<=deadline, 'API drain/replica/deadline state invalid')
        consumers=peer['consumers']
        require(len(consumers)==64 and {c['name'] for c in consumers}=={f'WF_P_{part:02d}' for part in range(64)} and
                all(type(c['num_pending']) is int and c['num_pending']==0 and
                    type(c['num_ack_pending']) is int and c['num_ack_pending']==0 and ns(c['ts'])<=deadline
                    for c in consumers), 'durable census, pending state or original deadline invalid')
    return True


def review(root):
    e=read(root/'execution.json');binary=read(root/'binary.json');revision=e['source']
    require(e['status']=='passed' and e['exit_code']==0 and not Path('/proc',str(e['pid'])).exists(),
            'native SDK failed or remains live')
    require(e['full_six_boundary_selection'] is True and e['selected_case'] is None and
            sha(root/'integration.test')==e['sha256']==binary['sha256'] and
            e['build_info'].splitlines()[1:]==binary['build_info'].splitlines()[1:] and
            'vcs.modified=false' in e['build_info'] and '-race=true' in e['build_info'] and
            'vcs.revision='+revision in e['build_info'], 'require full six-case original race SDK')
    git=lambda name:subprocess.check_output(['git','show',revision+':'+name],cwd=REPO)
    before=read(root/'source-before.json')
    require(before==read(root/'source-after.json') and before['revision']==revision, 'native source inventory changed')
    for name,digest in before['files'].items():
        require(sha(root/'source'/name)==digest==hashlib.sha256(git(name)).hexdigest(), 'captured native source differs from Git')
    external=read(root/'external-source-before.json');paths=read(root/'external-captured-paths.json')
    require(external==read(root/'external-source-after.json') and set(external)==set(paths), 'external source inventory changed')
    for name,digest in external.items():require(sha(root/paths[name])==digest, 'external captured input changed')
    producer=read(root/'producer-source.json')
    require(producer['revision']==revision and producer['path']=='scripts/run-fanout-combined-test.py' and
            sha(root/'executed-producer.py')==producer['sha256']==hashlib.sha256(git(producer['path'])).hexdigest(),
            'executed producer differs from original Git')
    for name in ('check-fanout-boundary-matrix.py','review-tier2-copied-audit.py'):
        require((SCRIPTS/name).read_bytes()==git('scripts/'+name), 'review dependency differs from executed source')
    commands=read(root/'commands.json');env=commands['environment']
    require(commands['full_six_boundary_selection'] is True and commands['selected_case'] is None and
            env['WF_FANOUT_PHYSICAL_DRAIN']=='1' and env['GOMAXPROCS']=='2' and env['GOMEMLIMIT']=='2GiB',
            'wrong original full-case selection or execution profile')
    log=(root/'native.log').read_text()
    conversion=subprocess.check_output(['go','tool','test2json','-p','js-wf/integration'],input=log,text=True)
    accepted=gate.check([json.loads(line) for line in conversion.splitlines()],combined=True,physical_drain=True)
    cases=[]
    for item in accepted['cases']:
        phase,position=item['phase'],item['position'];leaf=root/'originals'/gate.COMBINED_TEST/phase/position
        parent=read(leaf/'actual-parent-sdk.json');restart=read(leaf/'journal-restart.json');prefix=parent['durable_prefix']
        require(parent['actual_sdk_sha256']==e['sha256'] and not Path('/proc',str(parent['pid'])).exists() and
                parent['actual_sdk_build_info'].splitlines()[1:]==binary['build_info'].splitlines()[1:] and
                (leaf/'parent-cut').read_text()==str(item['cut']), 'actual killed parent SDK or boundary differs')
        require(restart['phase']==phase and restart['library_restart_not_server_sigkill'] is True and
                restart['old_server_id']!=restart['new_server_id'] and
                type(restart['node']) is int and 0<=restart['node']<3 and
                restart['captured_prefix_tail_sequence']==prefix[-1]['sequence'] and
                ns(parent['observed_before_kill'])<ns(restart['started'])<ns(restart['healed']),
                'restart or preserved prefix cut invalid')
        for label in ('before','after'):
            info=restart[label]
            require(info['config']['storage']=='file' and info['config']['num_replicas']==3 and
                    len(info['cluster']['replicas'])==2 and all(p['current'] and not p.get('offline',False) for p in info['cluster']['replicas']) and
                    info['state']['last_seq']>=prefix[-1]['sequence'], 'journal replica/count/prefix state invalid')
        require(restart['before']['state']['messages']==restart['after']['state']['messages'] and
                restart['before']['state']['last_seq']==restart['after']['state']['last_seq'], 'journal changed across quiet restart')
        final=read(leaf/'final-proof.json');report=final['retained_integrity']
        require(final['creation_prefix' if phase=='create' else 'result_prefix']==prefix and
                final['child_count']==500 and final['parent_result']=='249500' and
                set(report)=={'Invocations','Journals','Entries','Terminal'} and
                all(type(value) is int for value in report.values()) and report['Entries']>=501 and
                report['Invocations']==report['Journals']==report['Terminal']==501,
                'final original prefix/results/cohort differs')
        drain=read(leaf/'physical-drain.json');review_drain(drain,restart['healed'])
        cases.append(dict(**item,retained_report=report,physical_drain_elapsed_ns=drain['elapsed_ns'],
                          distinct_local_physical_queues_drained=True,original_case_deadline=drain['case_deadline']))
    return dict(source=revision,actual_sdk_pid=e['pid'],actual_sdk_sha256=e['sha256'],
                source_files=len(before['files']),external_files=len(external),cases=cases,
                six_combined_boundaries_and_local_physical_drains_verified=True,
                scope='Full six original R3 500-child parent SIGKILL + library journal restart cuts; copied audits/full fault matrix/24h remain separate')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();require(args.root.is_absolute(),'require absolute closed fixture')
    result=review(args.root.resolve());args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if __name__=='__main__':main()
