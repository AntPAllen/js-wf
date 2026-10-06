#!/usr/bin/env python3
"""Review a closed ordinary Tier2 copied audit; never start NATS or write stores."""
import argparse
from datetime import datetime
import hashlib
import json
import re
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parent.parent


def require(value, message):
    if not value:
        raise ValueError(message)


def read(path):
    return json.loads(path.read_text())


def sha(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file,'sha256').hexdigest()


def review_counts(result, expected):
    require(set(expected)=={'Invocations','Journals','Entries','Terminal'} and
            all(type(value) is int and value>0 for value in expected.values()) and
            expected['Invocations']==expected['Journals']==expected['Terminal'] and
            expected['Entries']>=expected['Invocations'], 'invalid complete expected cohort')
    require(result['report']==expected and all(type(value) is int for value in result['report'].values()),
            'retained counts differ from complete native cohort')
    for name in ('whole_review_ns','audit_ns'):
        require(type(result[name]) is int and 0<result[name]<20_000_000_000,
                'copied review exceeds original twenty-second budget')
    require(result['audit_ns']<=result['whole_review_ns'], 'whole review shorter than integrity audit')
    require(type(result['drained_partition_consumers']) is int and result['drained_partition_consumers']==64,
            'not all sixty-four durable consumers drained')
    peers=result['queue_all_three_peers']
    require(len(peers)==3, 'missing three leader-routed API views')
    for peer in peers:
        require(type(peer['state']['messages']) is int and peer['state']['messages']==0 and
                type(peer['state']['consumer_count']) is int and peer['state']['consumer_count']==64,
                'API queue or durable count differs from original drain gate')



def timestamp_ns(value):
    require(isinstance(value,str) and re.fullmatch(
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})',value),
        'invalid physical monitoring timestamp')
    instant=datetime.fromisoformat(value.replace('Z','+00:00'))
    fraction=re.search(r'\.(\d+)',value)
    return int(instant.replace(microsecond=0).timestamp())*1_000_000_000 + int(
        (fraction[1] if fraction else '').ljust(9,'0'))


def review_physical_peers(result, required=False):
    peers=result.get('physical_queue_peers')
    if peers is None:
        require(not required, 'required pinned physical monitoring absent')
        return False
    require(isinstance(peers,list) and len(peers)==3 and
            all(type(peer['node']) is int for peer in peers) and
            {peer['node'] for peer in peers}=={0,1,2}, 'missing or duplicated physical nodes')
    identities=set()
    for peer in peers:
        started=timestamp_ns(peer['started']);finished=timestamp_ns(peer['finished'])
        require(started<=finished,
                'invalid physical monitoring interval')
        snapshot=peer['state'];identity=snapshot.get('server_id')
        require(not peer.get('error') and isinstance(identity,str) and bool(identity) and
                identity not in identities, 'missing or duplicate physical server identity')
        identities.add(identity)
        streams=[stream for account in snapshot.get('account_details',[])
                 for stream in account.get('stream_detail',[]) if stream['name']=='WF_RUN']
        require(len(streams)==1, 'local physical WF_RUN missing or duplicated')
        state=streams[0]['state']
        require(type(state.get('messages')) is int and state['messages']==0 and
                type(state.get('consumer_count')) is int and state['consumer_count']==64,
                'local physical queue or durable count differs from original drain gate')
    return True


def review(root, canonical_required=False, physical_required=False):
    execution=read(root/'execution.json')
    require(execution['status']=='passed' and execution['exit_code']==0 and
            not Path('/proc',str(execution['pid'])).exists(), 'copied SDK failed or remains live')
    require(sha(root/'review-sdk')==execution['actual_sha256']==read(root/'binary.json')['sha256'],
            'copied SDK executable differs from actual process')
    inputs=read(root/'selected-inputs.json');helper=inputs['helper_git_source'];production=inputs['production_source']
    git=lambda revision,name:subprocess.check_output(['git','show',revision+':'+name],cwd=REPO)
    require((root/'helper.go').read_bytes()==git(helper,'scripts/matrix-retained-review.go.txt') and
            sha(root/'helper.go')==inputs['helper_git_sha256'], 'copied helper differs from recorded Git source')
    require((root/'executed-producer.py').read_bytes()==git(helper,'scripts/run-tier2-copied-audit.py'),
            'executed copied producer differs from Git')
    for record in inputs['files'].values():
        captured=root/record['captured']
        require(sha(captured)==record['sha256'], 'captured selected input bytes changed')
        if Path(record['captured']).is_relative_to('selected-source'):
            name=str(Path(record['captured']).relative_to('selected-source'))
            require(captured.read_bytes()==git(production,name), 'runtime source differs from original native Git')
    for name,digest in inputs['module_files'].items():
        require(sha(root/'selected-source'/name)==digest==hashlib.sha256(git(production,name)).hexdigest(),
                'module metadata differs from original native Git')
    before=read(root/'copy-before.json');after=read(root/'original-after.json')
    require(after['unchanged'] and before['files']==after['original_closed_files'], 'original files changed during copied audit')
    source=Path(before['original_root'])
    require(source.is_absolute() and source.parent.name=='originals' and not source.is_symlink(), 'unsupported original store scope')
    require(not any(path.is_symlink() for path in source.rglob('*')), 'block-image donor requires dedicated copied-media review')
    require({str(path.relative_to(source)):sha(path) for path in source.rglob('*') if path.is_file()}==before['files'],
            'current original file set or bytes changed')
    native=read(source.parent.parent/'execution.json')
    require(native['source']==production==before['original_source'] and native['sha256']==before['original_sdk_sha256'] and
            native['status']==before['original_native_status']=='passed' and native['exit_code']==0 and
            not Path('/proc',str(native['pid'])).exists(), 'original native identity or closure differs from copied origin')
    expected=read(root/'expected-original-report.json');result=read(root/'retained-review.json')
    review_counts(result,expected)
    physical_verified=review_physical_peers(result,physical_required)
    native_histories=list(source.parent.parent.glob('matrix-*-history.jsonl'))
    require(len(native_histories)==1 and (root/'history.jsonl').read_bytes()==native_histories[0].read_bytes(),
            'copied history differs from complete original native history')
    command=read(root/'commands.json')['run']
    require(command[0]==str(root/'review-sdk') and command[1]==str(root/'originals'/'cluster') and
            command[4]==str(root/'history.jsonl') and command[5]==str(root/'retained-review.json'),
            'executed helper paths differ from reviewed copied fixture')
    require(int(command[2])==expected['Invocations'] and int(command[3])==expected['Entries'],
            'copied helper arguments differ from complete expected cohort')
    servers=read(root/'observed-servers.json')
    require({record['node'] for record in servers}=={0,1,2} and len(servers)==3, 'actual copied servers missing or duplicated')
    require((root/'executed-observer.py').read_bytes()==git(helper,'scripts/matrix_process_observer.py'),
            'executed process observer differs from helper Git')
    for server in servers:
        require(not Path('/proc',str(server['pid'])).exists() and sha(root/server['captured'])==server['actual_executable_sha256'] and
                Path(server['store']).is_relative_to(root/'originals'), 'copied server identity, closure or scope invalid')
    canonical=root/'precopy-verification.json'
    require(not canonical_required or canonical.exists(), 'required canonical pre-copy verification absent')
    canonical_bound=False
    if (root/'executed-precopy-verifier.py').exists():
        proof=read(canonical)
        require((root/'executed-precopy-verifier.py').read_bytes()==git(helper,'scripts/verify-tier2-closed-originals.py') and
                proof['head']==helper and proof['files']==len(before['files']) and proof['canonical_parts_and_current_archive_match'] and
                proof['all_current_original_store_files_match_preserved_manifest'] and proof['sdk_workers_observed_servers_closed'] and
                proof['all_visible_task_fds_checked'], 'canonical pre-copy verification differs from executed helper or origin')
        canonical_bound=True
    require(not canonical_required or canonical_bound, 'canonical verifier was not captured from executed helper Git')
    return dict(actual_sdk_pid=execution['pid'],actual_sdk_sha256=execution['actual_sha256'],
                original_production_source=production,helper_git_source=helper,selected_inputs=len(inputs['files']),
                selected_runtime_source_matches_native_git=True,executed_copied_producer_matches_helper_git=True,
                original_files_unchanged=len(before['files']),retained_report=result['report'],
                audit_ns=result['audit_ns'],whole_review_ns=result['whole_review_ns'],
                drained_three_clients_and_64_consumers=True,server_observations=len(servers),
                pinned_local_physical_queues_verified=physical_verified,
                original_native_duration=native['duration'],original_smoke_only=native['duration']!='10m',
                canonical_precopy_bound_to_executed_helper=canonical_bound,
                scope='Independent copied integrity/history/drain only; original native fault/duration and full matrix qualification remain separate')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--require-canonical-verification',action='store_true')
    parser.add_argument('--require-physical-peers',action='store_true')
    args=parser.parse_args()
    require(args.root.is_absolute() and not args.output.resolve().is_relative_to(args.root.resolve()/'originals'),
            'require absolute closed root and output outside copied stores')
    result=review(args.root.resolve(),args.require_canonical_verification,args.require_physical_peers)
    args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))

if __name__=='__main__':main()
