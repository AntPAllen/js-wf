import gzip,hashlib,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent
state=json.loads((out/'state.json').read_text())
root=Path(state['root']);checkout=Path(state['checkout'])
assert json.loads((root/'state.json').read_text())==state
props=dict(line.split('=',1) for line in (out/'terminal-unit.txt').read_text().splitlines())
assert props['LoadState']=='loaded' and props['MainPID']=='0'
assert props['InvocationID']==state['invocation']
assert props['ExecMainStatus']=='1' and props['ActiveState']=='failed' and props['Result']=='exit-code'
assert state['terminal'] and state['child_terminal'] and state['phase']=='closed'
assert state['child_exit']==state['supervisor_exit']==1 and not state['accepted']
assert not state['actual_100000_entries_qualified']
assert props['ExecMainPID']==str(state['supervisor_pid'])
for pid in (state['supervisor_pid'],state['child_pid']):
 assert not Path('/proc/'+str(pid)).exists(),pid
assert state['inputs_unchanged'] and state['binary_unchanged']
before=json.loads(gzip.decompress((out/'source-before.json.gz').read_bytes()))
assert before==json.loads(gzip.decompress((out/'source-after.json.gz').read_bytes()))
for name,sha in before.items():assert hashlib.sha256((checkout/name).read_bytes()).hexdigest()==sha,name
assert hashlib.sha256(Path(state['binary']['path']).read_bytes()).hexdigest()==state['binary']['sha256']
log=(out/'native.log').read_text()
assert hashlib.sha256(log.encode()).hexdigest()==state['log_sha256']
assert (root/'native.log').read_bytes()==(out/'native.log').read_bytes()
assert 'delivery failed commit graph append index 40959: journal append outcome unknown: context deadline exceeded' in log
assert 'CONTINUATION_FAILURE_STATUS count=40959 tail=40959 kind=StepCompleted checkpoint=<nil>' in log
assert '--- FAIL: TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R1/archive=true ' in log
assert 'GRAPH_CONTINUATION_LIMIT budget=100000 entries=100000' not in log
windows=re.findall(r'GRAPH_LIMIT_PORT_PROFILE from=(\d+) to=(\d+) wall_ns=(\d+) operations=(.*)',log)
assert len(windows)==1
start,end,wall,raw=windows[0];profile=json.loads(raw)
assert (int(start),int(end))==(0,24996)
assert profile['CASRoot']['errors']==472 and profile['CASRoot']['error_kinds']=={'conflict':472}
assert profile['ReadBlob']['errors']==1 and profile['ReadBlob']['error_kinds']=={'deadline':1}
assert all(row['errors']==0 for name,row in profile.items() if name not in ('CASRoot','ReadBlob'))
files=json.loads(gzip.decompress((out/'native-files.json.gz').read_bytes()))
native=root/'native';assert native.is_dir() and not native.is_symlink()
assert len(files)==state['native_files']==866
assert sum(item['bytes'] for item in files.values())==state['native_bytes']==685671380
assert set(files)=={str(p.relative_to(native)) for p in native.rglob('*') if p.is_file()}
for name,item in files.items():
 p=native/name;assert not p.is_symlink() and p.stat().st_size==item['bytes']
 sha=hashlib.sha256()
 with p.open('rb') as f:
  while block:=f.read(1024*1024):sha.update(block)
 assert sha.hexdigest()==item['sha256'],name
refusal=json.loads((out/'acceptance-refusal-command.json').read_text())
assert refusal['actual_exit_code']==1
result={'failure_evidence_verified':True,'accepted':False,'actual_100000_entries_qualified':False,'source':state['source'],'child_exit':1,'supervisor_exit':1,'child_wall_seconds':state['child_wall_seconds'],'failed_append_index':40959,'observed_journal_count':40959,'completed_checkpoints':0,'profile_definite_cas_conflicts':472,'profile_read_blob_deadlines':1,'complete_source_inputs':len(before),'native_files':len(files),'native_bytes':state['native_bytes'],'native_census_hash_verified':True,'acceptance_review_actual_refusal':1,'server_side_cause_confirmed':False}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
