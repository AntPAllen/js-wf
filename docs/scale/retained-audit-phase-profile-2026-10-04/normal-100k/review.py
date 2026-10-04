"""Verify actual retained reader run source, binary and native results."""
import argparse, hashlib, json, subprocess, re
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--repo',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args();root=a.root.resolve()
def read(name):return json.loads((root/name).read_text())
def sha(path):
 with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=read('execution.json');before=read('source-before.json');after=read('source-after.json');paths=read('captured-paths.json')
assert before==after and e['source_before_after_identical'] and e['binary_unchanged']
assert len(before)==e['selected_inputs'] and set(paths)==set(before)
local=0
for original,digest in before.items():
 capture=paths[original];assert sha(root/capture)==digest,capture
 if capture.startswith('selected-source/local/'):
  name=capture.removeprefix('selected-source/local/')
  assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=a.repo)).hexdigest()==digest,name
  local+=1
assert local==e['local_git_inputs']
assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':scripts/run-tier3-retained-test.py'],cwd=a.repo)).hexdigest()==sha(root/'runner.py')
commands=read('commands.json');assert commands['package']=='js-wf/integrity'
binary=root/'integrity.test';assert sha(binary)==e['actual_binary_sha256']
actual=subprocess.check_output(['go','version','-m',str(binary)],text=True).splitlines()
recorded=(root/'binary-build-info.txt').read_text().splitlines()
assert actual[0].split(': ',1)[1]==recorded[0].split(': ',1)[1] and actual[1:]==recorded[1:]
race='-race' in commands['build']
assert any('-race=true' in line for line in actual)==race
events=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
results={event['Test']:dict(status=event['Action'],seconds=event.get('Elapsed')) for event in events if event['Action'] in ('pass','fail','skip') and 'Test' in event}
logs=''.join(event.get('Output','') for event in events)
assert e['exit_code']==1 and e['source']=='6a38a7c41d040cbf6d259f98dcd0a7fc4a9704b7'
assert set(results)=={'TestBatchedInvariantAuditNativePhaseProfile'} and all(r['status']=='fail' for r in results.values())
assert commands['environment']['WF_AUDIT_BATCH_PROFILE_INVOCATIONS']=='100000'
assert not race
live=read('actual-sdk.json');assert live['sha256']==sha(binary)
info=live['build_info'].splitlines();assert info[0].split(': ',1)[1]==actual[0].split(': ',1)[1] and info[1:]==actual[1:]
phases=read('profile/audit-phases.json')
assert phases['error']=='context deadline exceeded' and phases['report']==dict(Invocations=100000,Journals=0,Entries=0,Terminal=0)
assert 20_000_000_000<=phases['elapsed_ns']<21_000_000_000
assert [p['stream'] for p in phases['phases']]==['WF_INV','WF_JRN']
assert [p['records'] for p in phases['phases']]==[100000,1200000]
assert [p['bytes'] for p in phases['phases']]==[500000,84700000]
assert all(0<p['visit_ns']<=p['scan_ns'] for p in phases['phases'])
assert sum(p['scan_ns'] for p in phases['phases'])<phases['elapsed_ns']
assert 'profiled audit report={Invocations:100000 Journals:0 Entries:0 Terminal:0}' in logs and 'context deadline exceeded' in logs
profile=root/'profile/audit-cpu.pprof'
assert profile.stat().st_size>0
for name,args in [('cpu-top.txt',['-top','-nodecount=16']),('cpu-tags.txt',['-tags'])]:
 command=['go','tool','pprof',*args,str(binary),str(profile)]
 value=subprocess.check_output(command,text=True)
 (root/'profile'/name).write_text(value)
write_commands=[['go','tool','pprof','-top','-nodecount=16',str(binary),str(profile)],['go','tool','pprof','-tags',str(binary),str(profile)]]
(root/'profile/derived-commands.json').write_text(json.dumps(write_commands,indent=2)+'\n')
store_files=[p for p in (root/'stores').rglob('*') if p.is_file()]
summary=dict(source=e['source'],exit_code=e['exit_code'],race=race,selected_inputs=len(before),local_git_inputs=local,all_captured_source_hashes_verified=True,all_local_inputs_match_executed_git=True,actual_runner_matches_executed_git=True,actual_binary_sha256=sha(binary),actual_binary_retained=True,all_actual_build_info_fields_match=True,live_proc_identity_captured=True,named_results=results,native_files=len(store_files),native_file_bytes=sum(p.stat().st_size for p in store_files),phases=phases,cpu_profile_sha256=sha(profile),raw_result_log=logs,physical_stores_reopened=False,qualifies_24h_row=False,qualifies_full_matrix=False,scope='Failed native 100k/1.2M audit profile reads all records then hits original20s cap; includes embedded servers and client. Selected inputs/runner/SDK match retained execution; live proc identity matches; no store reopening, fault capacity or release qualification.')
a.output.write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:v for k,v in summary.items() if k!='raw_result_log'},indent=2))
