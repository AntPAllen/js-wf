import sys,json,pathlib,subprocess,hashlib,shutil,importlib.util,copy,datetime
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp');from storage_review_common import closure,fixture_archive
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-tier1-full123-normal100k-20261007');run=root/'run';source=root/'source';out=repo/'docs/scale/tier1-full123-2026-10-07/normal100k-accepted';out.mkdir(parents=True,exist_ok=True)
read=lambda p:json.loads(p.read_text())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
execution=read(root/'execution.json');revision=execution['source'];assert revision=='4a04e000cd75d591752d8588cc36afd405818c51';assert execution['status']=='passed' and execution['exit_code']==0
before=read(run/'source-before.json');assert before==read(run/'source-after.json');assert before['revision']==revision
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
for name,digest in before['files'].items():
 assert sha(source/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)).hexdigest()==digest
unit=subprocess.check_output(['systemctl','--user','show','js-wf-tier1-full123-normal100k-20261007.service','-p','Id','-p','MainPID','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID','-p','ActiveState','-p','SubState'],text=True)
fields=dict(x.split('=',1) for x in unit.splitlines());assert fields['MainPID']=='0' and fields['ExecMainPID']=='2039197' and fields['ExecMainStatus']=='0' and fields['Result']=='success' and fields['InvocationID']=='c232dbf42fcf4d9ca69c01c57c52b176'
for pid in [2039197,execution['producer_pid'],execution['actual_sdk']['pid']]:assert not pathlib.Path('/proc',str(pid)).exists()
binary=read(run/'binary.json');assert sha(run/'sim.test')==binary['binary_sha256']==execution['actual_sdk']['exe_sha256'];assert not binary['race_instrumented'] and '-race=true' not in binary['build_info']
assert execution['actual_sdk']['args']==[str(run/'sim.test'),'-test.v=test2json','-test.count=1','-test.timeout=300m'];assert execution['actual_sdk']['environment']=={'GOMEMLIMIT':'512MiB','GOMAXPROCS':'2','SIM_SEEDS':'100000','SIM_COVERAGE_SUMMARY':'1'}
checker=source/'scripts/check-tier1-suite.py';spec=importlib.util.spec_from_file_location('checker',checker);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
events=[json.loads(x) for x in (run/'tier1-events.jsonl').read_text().splitlines()];inventory=(run/'tier1-inventory.txt').read_text();pins=(run/'tier1-regression-inventory.txt').read_text();seeded=(run/'tier1-seeded-inventory.txt').read_text()
result=m.check(events,inventory,100000,revision,pins,seeded);result['events_sha256']=sha(run/'tier1-events.jsonl');assert result==read(run/'tier1-result.json');assert result['per_workload_seed_proof']['workloads']==123 and result['per_workload_seed_proof']['completed_bodies']==12300000;assert result['pinned_regressions_pass']==395
terminal=[x for x in events if 'Test' not in x and x['Action']=='pass'];assert len(terminal)==1 and terminal[0]['Elapsed']<18000
controls=[]
def reject(label,ev=events,inv=inventory,reg=pins,seed=seeded):
 try:m.check(ev,inv,100000,revision,reg,seed)
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
for field in ['last=100000','completed=100000','requested=100000']:
 changed=copy.deepcopy(events);idx=events.index(seed_event);changed[idx]['Output']=changed[idx]['Output'].replace(field,field.split('=')[0]+'=99999');reject('short_'+field.split('=')[0],changed)
reject('missing_body_counter',[x for x in events if x is not seed_event]);reject('duplicate_body_counter',events+[seed_event])
coverage=next(x for x in events if x.get('Output','').startswith('TIER1_COVERAGE '));reject('missing_coverage',[x for x in events if x is not coverage]);reject('duplicate_coverage',events+[coverage])
review={'reviewed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':revision,'source_inputs_verified':len(before['files']),'actual_sdk':execution['actual_sdk'],'actual_binary_sha256':binary['binary_sha256'],'original_unit':fields,'package_terminal':terminal[0],'result':result,'actual_positive_proof_mutations_rejected':controls,'checker_sha256':sha(checker),'scope':'Full frozen123 normal100k suite at 4a04e0; not current124 graph, native matrices, actual24h, online GC or dependency adoption.'}
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');(root/'original-terminal-unit.txt').write_text(unit);(out/'original-terminal-unit.txt').write_text(unit)
shutil.copy2(__file__,root/'executed-terminal-review.py');shutil.copy2(__file__,out/'executed-terminal-review.py');shutil.copy2(checker,root/'independent-checker.py')
for n in ['tier1-result.json','binary.json','tier1-time.txt','source-before.json','source-after.json']:shutil.copy2(run/n,out/n)
(out/'closure.json').write_text(json.dumps(closure(root),indent=2)+'\n')
fixture_archive.capture(root,pathlib.Path('/tmp/js-wf-tier1-full123-normal100k-complete-20261007.tar.gz'),out,compresslevel=1)
print(json.dumps({'full123_normal100k_accepted':True,'elapsed':terminal[0]['Elapsed'],'mutation_controls':len(controls),'verified_source_inputs':len(before['files'])}),flush=True)
