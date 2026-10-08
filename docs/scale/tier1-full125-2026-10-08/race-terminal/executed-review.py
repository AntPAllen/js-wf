import copy,sys,json,hashlib,subprocess,shutil,importlib.util,datetime,io
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive,repo
root=Path('/tmp/js-wf-tier1-full125-race-20261008');run=root/'run';source=root/'source'
unit=dict(row.split('=',1) for row in subprocess.check_output(['systemctl','--user','show','js-wf-tier1-full125-race-v2-20261008.service','--property=LoadState,ActiveState,SubState,MainPID,ExecMainPID,ExecMainStatus,Result,InvocationID,Restart,MemoryMax,MemoryPeak'],text=True).splitlines())
assert unit['LoadState']=='loaded' and unit['ActiveState']=='active' and unit['SubState']=='exited' and unit['MainPID']=='0' and unit['ExecMainStatus']=='0' and unit['Result']=='success' and unit['Restart']=='no'
state=json.loads((root/'execution.json').read_text());assert state['status']=='passed' and state['exit_code']==0 and state['race'] and state['seeds']==1000 and state['configured_original_timeout']=='60m'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
canonical=Path('docs/scale/tier1-full125-2026-10-08/race-launch/launch.json')
launchbytes=subprocess.check_output(['git','cat-file','blob',head+':'+str(canonical)],cwd=repo);assert launchbytes==(repo/canonical).read_bytes();launch=json.loads(launchbytes)
assert state['source']==launch['execution']['source']=='c13c8a647053dcaeb2312c26fa58e7e15c100945'
assert unit['InvocationID']==launch['unit']['InvocationID'] and unit['ExecMainPID']==launch['unit']['MainPID']
assert state['actual_sdk']==launch['execution']['actual_sdk']
assert unit['MemoryMax']==launch['unit']['MemoryMax']=='3221225472'
pids=[int(unit['ExecMainPID']),state['producer_pid'],state['actual_sdk']['pid']]
assert all(not Path('/proc',str(p)).exists() for p in pids)
binary=json.loads((run/'binary.json').read_text());assert binary==launch['binary']==state['binary']
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(run/'sim.test')==binary['binary_sha256']==state['actual_sdk']['exe_sha256']==launch['actual_sdk_sha256']
assert binary['race_instrumented'] and '-race=true' in binary['build_info']
assert binary['environment']==state['actual_sdk']['environment']==dict(GOMEMLIMIT='512MiB',GOMAXPROCS='2',SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1')
before=json.loads((run/'source-before.json').read_text());assert before==json.loads((run/'source-after.json').read_text()) and before['revision']==state['source']
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==state['source'] and not subprocess.check_output(['git','status','--porcelain'],cwd=source)
names=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(state['source']+':'+n+'\n' for n in expected).encode()))
for n in expected:
 header=stream.readline().split();assert header[1]==b'blob';b=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(b).hexdigest()==before['files'][n]==sha(source/n)
assert not stream.read()
spec=importlib.util.spec_from_file_location('checker',source/'scripts/check-tier1-suite.py');checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
events=[json.loads(line) for line in (run/'tier1-events.jsonl').read_text().splitlines() if line.strip()]
report=checker.check(events,(run/'tier1-inventory.txt').read_text(),1000,state['source'],(run/'tier1-regression-inventory.txt').read_text(),(run/'tier1-seeded-inventory.txt').read_text());report['events_sha256']=sha(run/'tier1-events.jsonl')
assert report==json.loads((run/'tier1-result.json').read_text()) and report['per_workload_seed_proof']['workloads']==125 and report['per_workload_seed_proof']['completed_bodies']==125000 and report['pinned_regressions_pass']==419
inventory=(run/'tier1-inventory.txt').read_text();pins=(run/'tier1-regression-inventory.txt').read_text();seeded=(run/'tier1-seeded-inventory.txt').read_text()
terminal=[x for x in events if 'Test' not in x and x['Action']=='pass'];assert len(terminal)==1 and terminal[0]['Elapsed']<3600
controls=[]
def reject(label,ev=events,inv=inventory,reg=pins,seed=seeded):
 try:checker.check(ev,inv,1000,state['source'],reg,seed)
 except (ValueError,KeyError,TypeError):controls.append(label);return
 raise AssertionError('invalid proof accepted: '+label)
reject('missing_package_terminal',[x for x in events if x not in terminal]);reject('duplicate_package_terminal',events+terminal)
reject('native_failure',events+[dict(Package='js-wf/sim',Action='fail')])
reject('missing_seeded_inventory_name',seed='\n'.join(seeded.splitlines()[1:])+'\n')
reject('missing_compiled_test',inv='\n'.join(inventory.splitlines()[1:])+'\n')
reject('missing_pin_inventory',reg='\n'.join(pins.splitlines()[1:])+'\n')
pin=next(x['Test'] for x in events if x.get('Test','').startswith('TestPinnedRegressionCorpus/'))
reject('missing_pin_terminal',[x for x in events if not(x.get('Test')==pin and x['Action']=='pass')])
reject('duplicate_pin_terminal',events+[x for x in events if x.get('Test')==pin and x['Action']=='pass'])
seed_event=next(x for x in events if 'TIER1_SEEDS ' in x.get('Output',''))
for field in ['last=1000','completed=1000','requested=1000']:
 changed=copy.deepcopy(events);idx=events.index(seed_event);changed[idx]['Output']=changed[idx]['Output'].replace(field,field.split('=')[0]+'=999');reject('short_'+field.split('=')[0],changed)
reject('missing_body_counter',[x for x in events if x is not seed_event]);reject('duplicate_body_counter',events+[seed_event])
coverage=next(x for x in events if x.get('Output','').startswith('TIER1_COVERAGE '));reject('missing_coverage',[x for x in events if x is not coverage]);reject('duplicate_coverage',events+[coverage])
initial=closure(root);out=repo/'docs/scale/tier1-full125-2026-10-08/race-terminal';raw=Path('/tmp/js-wf-tier1-full125-race-complete-20261008.tar.gz');full=fixture_archive.capture(root,raw,out,compresslevel=1);final=closure(root)
for n in ['tier1-result.json','binary.json','source-before.json','source-after.json','tier1-inventory.txt','tier1-seeded-inventory.txt','tier1-regression-inventory.txt','tier1-time.txt']:shutil.copyfile(run/n,out/n)
shutil.copyfile(__file__,out/'executed-review.py')
(out/'independent-review.json').write_text(json.dumps(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=state['source'],unit=unit,execution=state,actual_pids_closed=pids,selected_inputs_equal_git=len(expected),suite=report,actual_positive_proof_mutations_rejected=controls,elapsed=(run/'tier1-time.txt').read_text().strip(),complete_archive=full,closure_before=initial,closure_after=final,scope='Complete125-workload race1000 graph,125000 bodies and419 source pins qualified at isolatedc13c8a6. Original counts/deadline retained. Selected repository inputs exact; exhaustive/hermetic compiler provenance is not claimed. No full125 normal100k, new runtime source, safe onlineGC, real-cluster matrices, million physical drain or actual24h acceptance.'),indent=2)+'\n')
print('FULL125_RACE_TERMINAL_INDEPENDENTLY_REVIEWED',len(expected),report['per_workload_seed_proof']['completed_bodies'],report['pinned_regressions_pass'],full['members'],flush=True)
