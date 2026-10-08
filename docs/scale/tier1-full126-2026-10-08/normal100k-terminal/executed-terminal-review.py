import sys,json,hashlib,subprocess,datetime,shutil,io
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier1-full126-normal100k-20261008');run=root/'run';source=root/'source';base=repo/'docs/scale/tier1-full126-2026-10-08/normal100k-terminal'
assert not base.exists();base.mkdir()
state=json.loads((root/'execution.json').read_text());launch=json.loads((repo/'docs/scale/tier1-full126-2026-10-08/normal100k-launch/launch.json').read_text())
assert state['status']=='passed' and state['exit_code']==0 and state['source']=='074bcfcc50c5b000a2364a808bf970f31b9cc522'
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show','js-wf-tier1-full126-normal100k-20261008.service','--property=ActiveState,SubState,MainPID,ExecMainPID,Result,ExecMainStatus,InvocationID,Restart,RuntimeMaxUSec,CPUWeight,Nice,MemoryMax'],text=True).splitlines())
assert unit['SubState']=='exited' and unit['Result']=='success' and unit['ExecMainStatus']=='0' and unit['InvocationID']==launch['unit']['InvocationID']=='b2521868d0e34dc49e70a618dcd2b424'
assert not Path('/proc',str(state['actual_sdk']['pid'])).exists()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
binary=json.loads((run/'binary.json').read_text());assert sha(run/'sim.test')==binary['binary_sha256']==state['actual_sdk']['exe_sha256']==launch['actual_sdk_sha256']
assert not binary['race_instrumented'] and state['configured_original_timeout']=='300m'
assert state['actual_sdk']['environment']==launch['actual_sdk']['environment']==dict(GOMEMLIMIT='512MiB',GOMAXPROCS='2',SIM_SEEDS='100000',SIM_COVERAGE_SUMMARY='1')
for field in ('pid','start_ticks','args','environment'):
    assert state['actual_sdk'][field]==launch['actual_sdk'][field]
before=json.loads((run/'source-before.json').read_text());after=json.loads((run/'source-after.json').read_text());assert before==after
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==state['source']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
names=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=repo,text=True).splitlines()
expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(expected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(state['source']+':'+n+'\n' for n in expected).encode()))
for n in expected:
    header=stream.readline().decode().split();assert header[1]=='blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
    assert hashlib.sha256(data).hexdigest()==before['files'][n]==sha(source/n)
assert not stream.read()
command=['python3',str(source/'scripts/check-tier1-suite.py'),'--events',str(run/'tier1-events.jsonl'),'--inventory',str(run/'tier1-inventory.txt'),'--regressions',str(run/'tier1-regression-inventory.txt'),'--source',str(run/'tier1-source.txt'),'--seeded-inventory',str(run/'tier1-seeded-inventory.txt'),'--seeds','100000','--output',str(base/'independent-result.json')]
review=subprocess.run(command,cwd=source,capture_output=True,text=True);(base/'review.stdout').write_text(review.stdout);(base/'review.stderr').write_text(review.stderr);assert review.returncode==0
result=json.loads((base/'independent-result.json').read_text());assert result==json.loads((run/'tier1-result.json').read_text())
assert result['source']==state['source'] and result['top_level_pass']==184 and result['pinned_regressions_pass']==433
assert result['trace_only_skips']==['TestMinimizeFaultTrace','TestReplayFaultTrace']
proof=result['per_workload_seed_proof'];assert proof['workloads']==126 and proof['completed_bodies']==12600000 and proof['first']==1 and proof['last']==100000
for name in ('binary.json','commands.json','execution-contexts.json','source-before.json','source-after.json','stderr.log','tier1-events.jsonl','tier1-inventory.txt','tier1-list.log','tier1-regression-inventory.txt','tier1-seeded-inventory.txt','tier1-source.txt','tier1-time.txt','tier1-result.json'):
    shutil.copy2(run/name,base/name)
shutil.copy2(root/'execution.json',base/'execution.json');shutil.copy2(root/'producer.log',base/'producer.log');shutil.copy2('/tmp/js-wf-tier1-full126-normal100k-20261008-supervisor.log',base/'supervisor.log');shutil.copy2(__file__,base/'executed-terminal-review.py')
report=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=state['source'],unit=unit,original_sdk=state['actual_sdk'],original_live_admission='docs/scale/tier1-full126-2026-10-08/normal100k-launch/launch.json',terminal_binary_sha256=sha(run/'sim.test'),all_selected_inputs_equal_to_git_before_after_and_now=len(expected),independent_check_command=command,result=result,scope='Complete full126 normal100000 Tier1 qualification only at frozen074bcfcc. Original count1/nonrace/300m/twoGoCPU/512MiB,433pins and12600000 bodies. Does not qualify later current-source graph reader/catalog/native/runtime changes, default dependency or native matrices/24h/million-drain/online GC.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(dict(source=state['source'],selected_inputs=len(expected),workloads=126,pins=433,bodies=12600000)),flush=True)
