#!/usr/bin/env python3
"""Require frozen inputs, real service/child exits and full native assertions."""
import argparse, datetime, gzip, hashlib, json, re, subprocess, sys
from pathlib import Path
here=Path(__file__).resolve().parent
repo=here.parents[2]
sys.path.insert(0,str(repo/"scripts"))
from graph_limit_profile import audit_profile
parser=argparse.ArgumentParser();parser.add_argument('root',type=Path);parser.add_argument('--native-root',type=Path,help='Fresh restored native fixture, checked against the original full file census');parser.add_argument('--output',type=Path,help='Separate review output; preserve the original review receipt');a=parser.parse_args()
native_root=a.native_root if a.native_root is not None else a.root/'native'
review_output=a.output if a.output is not None else a.root/'review.json'
state=json.loads((a.root/'state.json').read_text())
props=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',state['unit'],
    '-p','LoadState','-p','MainPID','-p','InvocationID','-p','RemainAfterExit','-p','ExecMainStatus',
    '-p','ExecMainExitTimestamp','-p','ActiveState','-p','SubState'],text=True).splitlines())
result=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    root=str(a.root),source=state['source'],mode=state['mode'],kind=state['kind'],properties=props,
    accepted=False,actual_100000_entries_qualified=False)
def save():
    review_output.write_text(json.dumps(result,indent=2)+'\n')
assert props['LoadState']=='loaded' and props['InvocationID']==state['invocation']
assert props['RemainAfterExit']=='yes'
if not state['terminal'] or props['MainPID']!='0' or not props['ExecMainExitTimestamp']:
    save();print(json.dumps(result));raise SystemExit(0)
assert state['child_terminal'] and state['child_exit']==0 and state['supervisor_exit']==0
assert props['ExecMainStatus']=='0' and state['phase']=='closed'
assert state['inputs_unchanged'] and state['binary_unchanged']
prep_dir=repo/'docs/scale/graph-entry-campaign-preparation-2026-10-10'
subprocess.run(['python3',str(prep_dir/'review.py')],check=True,stdout=subprocess.DEVNULL)
prep=json.loads((prep_dir/'preparation.json').read_text())
assert state['source']==prep['source'] and state['binary']==prep['binaries'][state['mode']]
assert gzip.decompress((a.root/'supervisor.py.txt.gz').read_bytes())==(here/'supervisor.py').read_bytes()
before=json.loads(gzip.decompress((a.root/'source-before.json.gz').read_bytes()))
assert before==json.loads(gzip.decompress((a.root/'source-after.json.gz').read_bytes()))
assert before==json.loads((Path(prep['root'])/'source-before.json').read_text())['files']
for name,digest in before.items():
    assert hashlib.sha256((Path(state['checkout'])/name).read_bytes()).hexdigest()==digest,name
budget=100000 if state['kind']=='production' else 20
assert state['budget']==budget
expected_command=[state['binary']['path'],'-test.v',
    '-test.run=^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/^R1$/^archive=true$',
    '-test.count=1','-test.timeout='+('310m' if budget==100000 else '5m')]
assert state['command']==expected_command
assert state['configuration']==dict(GOMAXPROCS='4',GOMEMLIMIT='8GiB',
    WF_GRAPH_CONTINUATION_LIMIT_BUDGET=str(budget),WF_GRAPH_CONTINUATION_OWNER_INDEX='1',
    WF_GRAPH_CONTINUATION_DURABLE='1',WF_GRAPH_CONTINUATION_COMPACTION_TTL=state['configuration']['WF_GRAPH_CONTINUATION_COMPACTION_TTL'],
    WF_GRAPH_LIMIT_PORT_PROFILE='1',WF_GRAPH_LIMIT_STORE_ROOT=str(a.root/'native'))
if budget==100000:
    policy=json.loads((a.root/'policy.json').read_text())
    native_dir=repo/'docs/scale/graph-native-owned100000-2026-10-10'
    subprocess.run(['python3',str(native_dir/'review.py')],check=True,stdout=subprocess.DEVNULL)
    native=json.loads((native_dir/'review.json').read_text())
    assert native['accepted'] and native['log_sha256']==policy['native_log_sha256']
    assert policy['prepared_source']==state['source'] and policy['actual_entry_cap']==budget
    assert policy['request_bounds_unchanged'] is True
    assert policy['compaction_ttl']==state['configuration']['WF_GRAPH_CONTINUATION_COMPACTION_TTL']
else:
    assert json.loads((a.root/'policy.json').read_text()) is None
    assert state['configuration']['WF_GRAPH_CONTINUATION_COMPACTION_TTL']=='3h'
data=(a.root/'native.log').read_text()
assert hashlib.sha256(data.encode()).hexdigest()==state['log_sha256']
assert re.search(r'^PASS$',data,re.M) and '--- FAIL:' not in data and 'WARNING: DATA RACE' not in data
assert len(re.findall(r'--- PASS: TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R1/archive=true ',data))==1
assert 'GRAPH_LIMIT_DURABLE storage=file whole_handoff=delivery_context request_bounds_unchanged=true' in data
assert 'GRAPH_LIMIT_CONFIG owner_index=true compaction_ttl='+state['configuration']['WF_GRAPH_CONTINUATION_COMPACTION_TTL'] in data
expected=f'GRAPH_CONTINUATION_LIMIT budget={budget} entries={budget} archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot={budget-1} prefix_stage_calls=1/1 production_cap={str(budget==100000).lower()} padding_operations={(budget-16)//2}'
assert expected in data
windows=re.findall(r'GRAPH_LIMIT_PORT_PROFILE from=(\d+) to=(\d+) wall_ns=(\d+) operations=(.*)',data)
assert len(windows)==2
padding=(budget-16)//2
assert [(int(w[0]),int(w[1])) for w in windows]==[(0,padding//2),(padding//2,padding)]
profile_errors = []
unexpected_errors = []
for row in windows:
    audited = audit_profile(json.loads(row[3]))
    for key,target in (('errors',profile_errors),('unexpected',unexpected_errors)):
        target.extend(dict(from_padding=int(row[0]),to_padding=int(row[1]),**value) for value in audited[key])
result['profile_errors'] = profile_errors
result['unexpected_profile_errors'] = unexpected_errors
result['zero_profile_errors_gate'] = not profile_errors
result['no_unexpected_profile_errors_gate'] = not unexpected_errors
files=json.loads(gzip.decompress((a.root/'native-files.json.gz').read_bytes()))
assert files and len(files)==state['native_files']
assert sum(x['bytes'] for x in files.values())==state['native_bytes']
assert native_root.is_dir() and not native_root.is_symlink()
assert not any(p.is_symlink() for p in native_root.rglob('*'))
assert set(files)=={str(p.relative_to(native_root)) for p in native_root.rglob('*') if p.is_file()}
for name,item in files.items():
    path=native_root/name
    assert path.stat().st_size==item['bytes']
    digest=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1<<20),b''):digest.update(block)
    assert digest.hexdigest()==item['sha256'],name
result.update(accepted=not unexpected_errors,functional_limit_assertions_verified=True,source_inputs=len(before),budget=budget,entries=budget,
    checkpoints=2,terminal_slot=budget-1,padding_operations=padding,native_files=len(files),
    native_bytes=state['native_bytes'],native_storage_path=str(native_root),fresh_restored_storage=a.native_root is not None,child_wall_seconds=state['child_wall_seconds'],
    actual_100000_entries_qualified=budget==100000 and not unexpected_errors,log_sha256=state['log_sha256'])
if unexpected_errors:
    result['remaining_gate'] = 'Unclassified/uncertain/non-CAS errors remain unaccepted; only fully accounted definite CAS conflicts are eligible. Functional assertions/source/binary/native-file verification passed separately.'
save();print(json.dumps(result))
