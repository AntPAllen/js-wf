import sys,json,hashlib,subprocess,shutil,re,copy
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/authority-read-witness-tier1-2026-10-08/accepted';root=Path('/tmp/js-wf-authority-witness-tier1-20261008');native=Path('/tmp/js-wf-shared-authority-witness-native-20261008')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def validate_root(root):
 before=read(root/'source-before.json');assert before==read(root/'source-after.json');rev=before['revision']
 names=list(before['files']);raw=subprocess.check_output(['git','cat-file','--batch'],input=''.join(rev+':'+n+'\n' for n in names).encode(),cwd=repo)
 import io
 stream=io.BytesIO(raw)
 for name in names:
  header=stream.readline().decode().split();assert header[1]=='blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
  p=root/'selected-source'/name
  if p.suffix=='.go':p=p.with_suffix('.go.txt')
  assert hashlib.sha256(data).hexdigest()==before['files'][name]==sha(p)
 assert not stream.read()
 proof=root.with_name(root.name+'-proof');meta=read(proof/'archive-verification.json');inv=read(proof/'fixture-inventory.json')
 assert fixture_archive.inventory(root)==inv['files']
 with root.with_suffix('.tar.gz').open('rb') as f:declared,actual=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
 assert declared==inv
 return rev,len(names),actual
rev,count,archive=validate_root(root);native_rev,native_count,native_archive=validate_root(native);assert native_rev==rev
out.mkdir(parents=True,exist_ok=True)
controls=[];profiles={}
modes={'fresh_value','fresh_absence','stale_value','stale_absence','speculative_value','persistent_stale','replacement','lost_ack','no_quorum','snapshot_error','cancel_before','cancel_after','zero_ack','same_ack'}
def model_check(log,seeds):
 assert 'PASS' in log.splitlines() and not any(x in log for x in ['--- FAIL:','--- SKIP:','DATA RACE'])
 assert f'TIER1_SEEDS test=TestSeededAuthorityReadWitnessReplay first=1 last={seeds} completed={seeds} requested={seeds}' in log
 assert log.count('--- PASS: TestSeededAuthorityReadWitnessReplay (')==1 and log.count('--- PASS: TestPinnedRegressionCorpus/authority-witness-')==14
 m=re.search(r'authority witness modes=map\[([^\]]+)\]',log);assert m
 values={k:int(v) for k,v in re.findall(r'(\w+):(\d+)',m[1])};assert set(values)==modes and sum(values.values())==seeds and all(n>0 for n in values.values())
 assert f'generated_schedules={seeds} scheduler_choices={seeds*3}' in log
 return values
for label,seeds,race in [('normal100k',100000,False),('race1000',1000,True)]:
 run=root/label;log=(run/'actual.log').read_text();e=read(run/'execution.json');a=read(run/'actual-sdk.json');b=read(run/'binary.json')
 assert e['source']==rev and e['exit_code']==0 and e['body_complete'] and e['seeds']==seeds and e['race']==race
 assert a['args']==e['command'] and a['exe_sha256']==b['sha256']==sha(run/'sim.test') and a['admission']['stable_identity_observed_twice'] and not Path('/proc',str(a['pid'])).exists()
 assert 'vcs.revision='+rev in b['build_info'] and 'vcs.modified=false' in b['build_info'] and ('-race=true' in b['build_info'])==race
 assert a['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_SEEDS=str(seeds),SIM_COVERAGE_SUMMARY='1',FAULT_TRACE_OUT=str(root/'failure-trace.json'))
 counts=model_check(log,seeds)
 for name,bad in [('short_count',log.replace(f'completed={seeds}',f'completed={seeds-1}',1)),('missing_pin',log.replace('--- PASS: TestPinnedRegressionCorpus/authority-witness-zero_ack','--- OMIT: TestPinnedRegressionCorpus/authority-witness-zero_ack',1)),('race',log+'\nDATA RACE\n'),('missing_choices',log.replace(f'scheduler_choices={seeds*3}',f'scheduler_choices={seeds*3-1}',1)),('no_ack_mode',log.replace('lost_ack:','lost_mode:')),('failed',log+'\n--- FAIL: witness\n')]:
  try:model_check(bad,seeds)
  except AssertionError:controls.append(label+'/'+name)
  else:raise AssertionError('bad model log accepted')
 target=out/label;target.mkdir()
 for name in ['actual.log','actual-sdk.json','binary.json','execution.json']:shutil.copy2(run/name,target/name)
 profiles[label]=dict(execution=e,mode_counts=counts,actual_pid=a['pid'])
# Bind the same helper to the actual five retained native proofs.
a=read(native/'blob-actual-sdk.json');b=read(native/'blob-binary.json');e=read(native/'blob-execution.json');c=read(native/'blob-commands.json')['run'];log=(native/'blob-actual.log').read_text()
assert e['source']==rev and e['exit_code']==0 and a['args']==c and a['exe_sha256']==b['sha256']==sha(native/'blob-race.test') and a['admission']['stable_identity_observed_twice'] and not Path('/proc',str(a['pid'])).exists()
assert 'vcs.revision='+rev in b['build_info'] and 'vcs.modified=false' in b['build_info'] and '-race=true' in b['build_info']
assert a['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_BLOB_AUTHORITY_ROOT=str(native/'blob-stores'))
assert log.rstrip().endswith('PASS') and not any(x in log for x in ['--- FAIL:','--- SKIP:','DATA RACE'])
for name in ['TestNativeAuthorityReadWitness','TestNativeAuthorityWitnessRacesCommittedReplacement','TestNativeAuthorityReadRequiresQuorum']:assert re.search(r'^--- PASS: '+name+r' \(',log,re.M)
target=out/'native';target.mkdir(exist_ok=True)
for name in ['blob-actual-sdk.json','blob-binary.json','blob-execution.json','blob-commands.json','blob-actual.log']:shutil.copy2(native/name,target/name)
for r,target in [(root,out/'model-archive'),(native,out/'native')]:
 target.mkdir(exist_ok=True)
 for name in ['source-before.json','source-after.json','executed-producer.py','closure.json']:shutil.copy2(r/name,target/name)
 for name in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(r.with_name(r.name+'-proof')/name,target/name)
report={'source':rev,'source_inputs':count,'native_source_inputs':native_count,'model_archive':archive,'native_archive':native_archive,'profiles':profiles,'native_execution':e,'actual_positive_log_mutations_rejected':controls,'scope':'Focused production shared decision helper model normal100k/race1000, fourteen exact common regression replays per profile and five native cases. Not full126/433 final-source qualification or broad native/release/production online GC.'}
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copy2(__file__,out/'executed-review.py')
print('SHARED_MODEL_AND_NATIVE_SOURCE_REVIEW',count,len(controls))
