import sys,json,hashlib,subprocess,datetime,shutil,io
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier1-full123-normal100k-20261007');run=root/'run';source=root/'source'
state=json.loads((root/'execution.json').read_text());assert state['status']=='running' and state['actual_sdk']
unit=dict(row.split('=',1) for row in subprocess.check_output(['systemctl','--user','show','js-wf-tier1-full123-normal100k-20261007.service','--property=ActiveState,SubState,MainPID,Result,ExecMainStatus,InvocationID,Restart,RuntimeMaxUSec,CPUWeight,Nice,MemoryMax'],text=True).splitlines())
assert unit['ActiveState']=='active' and unit['SubState']=='running' and int(unit['MainPID'])>0 and unit['Restart']=='no'
sys.path.insert(0,str(source/'scripts'));from live_process_admission import snapshot
profile=state['actual_sdk']['environment'];pid=state['actual_sdk']['pid'];first=snapshot(pid,profile)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
digest=sha(Path('/proc',str(pid),'exe'));second=snapshot(pid,profile)
keys=['pid','start_ticks','args','exe','working_directory','environment']
assert {k:first[k] for k in keys}=={k:second[k] for k in keys}=={k:state['actual_sdk'][k] if k!='exe' else str(run/'sim.test') for k in keys}
binary=json.loads((run/'binary.json').read_text());assert digest==sha(run/'sim.test')==binary['binary_sha256']==state['actual_sdk']['exe_sha256']
assert not binary['race_instrumented'] and '-race=true' not in binary['build_info']
assert profile==dict(GOMEMLIMIT='512MiB',GOMAXPROCS='2',SIM_SEEDS='100000',SIM_COVERAGE_SUMMARY='1')
before=json.loads((run/'source-before.json').read_text());assert before['revision']==state['source']
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==state['source']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
names=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=repo,text=True).splitlines()
expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(expected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],input=''.join(state['source']+':'+n+'\n' for n in expected).encode(),cwd=repo))
for n in expected:
 header=stream.readline().decode().split();assert header[1]=='blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(data).hexdigest()==before['files'][n]==sha(source/n)
assert not stream.read()
seeded=(run/'tier1-seeded-inventory.txt').read_text().splitlines();pins=(run/'tier1-regression-inventory.txt').read_text().splitlines()
assert len(seeded)==123 and len(pins)==395
assert 'TestSeededOnlineBlobBoundaryReplay' in seeded
assert (run/'tier1-events.jsonl').stat().st_size>0
live24={u:subprocess.check_output(['systemctl','show',u,'--property=ActiveState,SubState,MainPID'],text=True) for u in ['js-wf-parallel-recovery-journal-24h-20261007.service','js-wf-parallel-recovery-journal-watch-24h-20261007.service','js-wf-parallel-recovery-journal-review-24h-20261007.service']}
assert all('ActiveState=active' in v for v in live24.values())
out=repo/'docs/scale/tier1-full123-2026-10-07/normal100k-launch';out.mkdir(parents=True)
for src,name in [(Path(__file__),'executed-verifier.py'),(root/'executed-launch.py','executed-launch.py'),(run/'source-before.json','source-before.json'),(run/'tier1-inventory.txt','compiled-tests.txt'),(run/'tier1-seeded-inventory.txt','seeded-tests.txt'),(run/'tier1-regression-inventory.txt','regressions.txt')]:shutil.copyfile(src,out/name)
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),execution=state,unit=unit,actual_sdk=second,actual_sdk_sha256=digest,binary=binary,selected_inputs_equal_to_git=len(expected),seeded_workloads=len(seeded),pinned_regressions=len(pins),original24h_units=live24,scope='Verified live full123-workload normal100000 qualifier with all395 pins at exact isolated4a04e00, unchanged300m SDK/twoGoCPU/512MiB. No terminal/full-suite/default-matrix/24h acceptance inferred. Source after, complete raw events, terminal retained unit and independent full-suite review remain mandatory.')
(out/'launch.json').write_text(json.dumps(report,indent=2)+'\n');print('ACTUAL_NORMAL100K_SDK_AND_EXACT_FULL_SOURCE_VERIFIED',pid,len(expected),len(seeded),len(pins),flush=True)
