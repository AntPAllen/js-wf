#!/usr/bin/env python3
"""Supervise one frozen native entry campaign and preserve actual exits/stores."""
import argparse, datetime, gzip, hashlib, json, os, shutil, subprocess, sys, time
from pathlib import Path
here=Path(__file__).resolve().parent
repo=here.parents[2]
parser=argparse.ArgumentParser()
parser.add_argument('--mode',choices=('normal','race'),required=True)
parser.add_argument('--kind',choices=('control','production'),required=True)
parser.add_argument('--root',type=Path,required=True)
parser.add_argument('--unit',required=True)
parser.add_argument('--ttl',required=True)
parser.add_argument('--policy',type=Path)
parser.add_argument('--normal-root',type=Path)
a=parser.parse_args()
assert a.root.is_absolute() and not a.root.exists()
assert a.unit.endswith('.service') and os.environ.get('INVOCATION_ID')
prep_dir=repo/'docs/scale/graph-classified-entry-preparation-2026-10-11'
subprocess.run([sys.executable,str(prep_dir/'review.py')],check=True,stdout=subprocess.DEVNULL)
prep=json.loads((prep_dir/'preparation.json').read_text())
assert prep['prepared'] and not prep['campaign_started']
checkout=Path(prep['checkout'])
binary=Path(prep['binaries'][a.mode]['path'])
assert hashlib.sha256(binary.read_bytes()).hexdigest()==prep['binaries'][a.mode]['sha256']
assert a.ttl.endswith('h') and a.ttl[:-1].isdigit() and 1<=int(a.ttl[:-1])<=12
policy=None
if a.kind=='production':
    assert a.policy is not None
    policy=json.loads(a.policy.read_text())
    assert policy['compaction_ttl']==a.ttl and policy['actual_entry_cap']==100000
    native_dir=repo/'docs/scale/graph-native-owned100000-2026-10-10'
    subprocess.run([sys.executable,str(native_dir/'review.py')],check=True,stdout=subprocess.DEVNULL)
    native_review=json.loads((native_dir/'review.json').read_text())
    assert native_review['accepted'] and native_review['log_sha256']==policy['native_log_sha256']
    assert policy['prepared_source']==prep['source'] and policy['request_bounds_unchanged'] is True
else:
    assert a.policy is None and a.ttl=='3h'
if a.mode=='race':
    assert a.normal_root is not None
    normal=json.loads((a.normal_root/'review.json').read_text())
    prior=json.loads((a.normal_root/'state.json').read_text())
    assert normal['accepted'] and normal['mode']=='normal' and normal['source']==prep['source'] and normal['kind']==a.kind
    terminal=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',prior['unit'],'-p','MainPID','-p','InvocationID','-p','ExecMainStatus','-p','RemainAfterExit'],text=True).splitlines())
    assert terminal['MainPID']=='0' and terminal['ExecMainStatus']=='0' and terminal['RemainAfterExit']=='yes' and terminal['InvocationID']==prior['invocation']
assert shutil.disk_usage(a.root.parent).free >= 5*(1<<30)
a.root.mkdir(mode=0o700)
store=a.root/'native';store.mkdir(mode=0o700)
budget=100000 if a.kind=='production' else 20
command=[str(binary),'-test.v','-test.run=^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/^R1$/^archive=true$',
         '-test.count=1','-test.timeout='+('310m' if a.kind=='production' else '5m')]
configuration=dict(GOMAXPROCS='4',GOMEMLIMIT='8GiB',WF_GRAPH_CONTINUATION_LIMIT_BUDGET=str(budget),
    WF_GRAPH_CONTINUATION_OWNER_INDEX='1',WF_GRAPH_CONTINUATION_DURABLE='1',
    WF_GRAPH_CONTINUATION_COMPACTION_TTL=a.ttl,WF_GRAPH_LIMIT_PORT_PROFILE='1',WF_GRAPH_LIMIT_STORE_ROOT=str(store))
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')}
env.update(configuration)
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def hash_file(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1<<20),b''):h.update(block)
    return h.hexdigest()
def props():
    return dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',a.unit,
        '-p','LoadState','-p','MainPID','-p','InvocationID','-p','RemainAfterExit','-p','RuntimeMaxUSec'],text=True).splitlines())
state=dict(unit=a.unit,invocation=os.environ['INVOCATION_ID'],supervisor_pid=os.getpid(),
    source=prep['source'],checkout=str(checkout),root=str(a.root),mode=a.mode,kind=a.kind,budget=budget,
    binary=prep['binaries'][a.mode],command=command,configuration=configuration,
    started_utc=stamp(),phase='preflight',terminal=False,child_terminal=False,child_exit=None,accepted=False,
    actual_100000_entries_qualified=False)
def save():
    tmp=a.root/'state.tmp';tmp.write_text(json.dumps(state,indent=2)+'\n');tmp.replace(a.root/'state.json')
save()
exit_code=1
try:
    initial=props()
    assert initial['LoadState']=='loaded' and initial['RemainAfterExit']=='yes'
    assert initial['InvocationID']==state['invocation'] and int(initial['MainPID'])==os.getpid()
    state['initial_service']=initial
    (a.root/'policy.json').write_text(json.dumps(policy,indent=2)+'\n')
    original=json.loads((Path(prep['root'])/'source-before.json').read_text())
    names=original['files']
    def inventory():
        assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==prep['source']
        assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
        return {n:hash_file(checkout/n) for n in names}
    before=inventory();assert before==names
    (a.root/'source-before.json.gz').write_bytes(gzip.compress(json.dumps(before,sort_keys=True).encode(),mtime=0))
    (a.root/'supervisor.py.txt.gz').write_bytes(gzip.compress(Path(__file__).read_bytes(),mtime=0))
    state['phase']='running';start=time.monotonic()
    with (a.root/'native.log').open('xb') as log:
        child=subprocess.Popen(command,cwd=checkout/'worker',env=env,stdout=log,stderr=subprocess.STDOUT)
        state['child_pid']=child.pid;save()
        exit_code=child.wait()
    state.update(child_terminal=True,child_exit=exit_code,child_ended_utc=stamp(),child_wall_seconds=time.monotonic()-start,phase='retaining')
    save()
    after=inventory()
    (a.root/'source-after.json.gz').write_bytes(gzip.compress(json.dumps(after,sort_keys=True).encode(),mtime=0))
    state['inputs_unchanged']=before==after
    state['binary_unchanged']=hash_file(binary)==state['binary']['sha256']
    assert state['inputs_unchanged'] and state['binary_unchanged']
    files={str(p.relative_to(store)):dict(bytes=p.stat().st_size,sha256=hash_file(p)) for p in sorted(store.rglob('*')) if p.is_file()}
    assert files
    (a.root/'native-files.json.gz').write_bytes(gzip.compress(json.dumps(files,sort_keys=True).encode(),mtime=0))
    state['native_files']=len(files);state['native_bytes']=sum(x['bytes'] for x in files.values())
    state['log_sha256']=hash_file(a.root/'native.log')
    state.update(terminal=True,phase='closed',ended_utc=stamp(),supervisor_exit=exit_code)
    save()
except BaseException as error:
    state.update(terminal=True,phase='supervision_failure',ended_utc=stamp(),supervisor_exit=1,error=repr(error))
    save();raise
print(json.dumps(state),flush=True)
raise SystemExit(exit_code if exit_code>=0 else 128-exit_code)
