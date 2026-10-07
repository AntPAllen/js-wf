import sys,os,json,pathlib,subprocess,hashlib,shutil,time,importlib.util,datetime
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-blob-publication-tier1-20261007')
assert root.exists();sys.path.insert(0,str(repo/'scripts'))
import fixture_archive,live_process_admission
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();before=shared.source_inventory(revision)
save=lambda n,v:(root/n).write_text(json.dumps(v,indent=2)+'\n')
shutil.copy2(__file__,root/'executed-continuation.py');assert before==json.loads((root/'source-before.json').read_text())
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_COVERAGE_SUMMARY='1',FAULT_TRACE_OUT=str(root/'failure-trace.json'))
regex='^(TestSeededBlobPublicationReplay|TestBlobPublicationTransportAuthorityCopies|TestPinnedRegressionCorpus)$/^blob-publication-'
results={'normal100k':json.loads((root/'normal100k/execution.json').read_text())}
assert results['normal100k']['exit_code']==0 and results['normal100k']['body_complete']
save('original-producer-format-failure.json',dict(original_producer_exit_code=1,original_normal_sdk_exit_code=0,reason='Old log check incorrectly required PASS to follow the Tier1 TestMain coverage summary; native normal body and all pins passed. Original producer and original native artifacts retained; no normal rerun.'))
for profile,seeds,race in [('race1000',1000,True)]:
 run=root/profile;run.mkdir();binary=run/'sim.test';profile_env=dict(env,SIM_SEEDS=str(seeds))
 build=['go','test','-p=1','-buildvcs=true',*(['-race'] if race else []),'-c','-o',str(binary),'./sim']
 with (run/'build.log').open('w') as log:subprocess.run(build,cwd=repo,env=profile_env,stdout=log,stderr=subprocess.STDOUT,check=True)
 info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert 'vcs.revision='+revision in info and 'vcs.modified=false' in info;assert ('-race=true' in info)==race
 fingerprint=shared.sha(binary);(run/'binary.json').write_text(json.dumps(dict(sha256=fingerprint,build_info=info,race=race),indent=2)+'\n')
 command=[str(binary),'-test.v','-test.run='+regex,'-test.count=1','-test.timeout=3m'];started=time.monotonic()
 with (run/'actual.log').open('w') as log:
  child=subprocess.Popen(command,cwd=repo/'sim',env=profile_env,stdout=log,stderr=subprocess.STDOUT)
  admission=live_process_admission.admit(child,command,{k:profile_env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','SIM_COVERAGE_SUMMARY','SIM_SEEDS','FAULT_TRACE_OUT']},repo/'sim',binary,fingerprint)
  (run/'actual-sdk.json').write_text(json.dumps(admission,indent=2)+'\n');print('ADMITTED',profile,child.pid,flush=True);code=child.wait()
 log=(run/'actual.log').read_text();result=dict(source=revision,exit_code=code,elapsed_seconds=time.monotonic()-started,profile=profile,seeds=seeds,race=race,command=command,build=build,body_complete=f'TIER1_SEEDS test=TestSeededBlobPublicationReplay first=1 last={seeds} completed={seeds} requested={seeds}' in log,scope='Focused shared Tier1 workload, 15 pins and transport authority controls; not full124 graph or native transport qualification.')
 (run/'execution.json').write_text(json.dumps(result,indent=2)+'\n');results[profile]=result
 assert code==0 and result['body_complete'];assert '\nPASS\n' in log and log.splitlines()[-1].startswith('TIER1_COVERAGE ');assert 'DATA RACE' not in log and '--- FAIL:' not in log and '--- SKIP:' not in log
 assert log.count('--- PASS: TestSeededBlobPublicationReplay (')==1 and log.count('--- PASS: TestBlobPublicationTransportAuthorityCopies (')==1
 assert log.count('--- PASS: TestPinnedRegressionCorpus/blob-publication-')==15
 assert shared.sha(binary)==fingerprint
 print('COMPLETE',profile,result['elapsed_seconds'],flush=True)
after=shared.source_inventory(revision);assert after==before;save('source-after.json',after);save('execution.json',results);save('closure.json',shared.closure(root))
fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print('COMPLETE_ARCHIVE',flush=True)
