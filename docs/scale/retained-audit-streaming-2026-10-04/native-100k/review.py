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
assert e['exit_code']==0 and e['source']=='1d43a2b2cdf2fe82e3d1fcf5bbabf0c42ad38986'
assert set(results)=={'TestBatchedInvariantAuditNativeStreamingComparison'} and all(r['status']=='pass' for r in results.values())
assert not race and commands['environment']['WF_AUDIT_BATCH_PROFILE_INVOCATIONS']=='100000'
assert commands['environment']['GOMEMLIMIT']=='2GiB' and commands['environment']['GOMAXPROCS']=='2'
live=read('actual-sdk.json');assert live['sha256']==sha(binary)
info=live['build_info'].splitlines();assert info[0].split(': ',1)[1]==actual[0].split(': ',1)[1] and info[1:]==actual[1:]
observed=re.findall(r'streaming-audit reader=(\w+) elapsed=([0-9.]+)s report=\{Invocations:100000 Journals:100000 Entries:1200000 Terminal:100000\} err=<nil> allocated_bytes=(\d+) gc_cycles=(\d+)',logs)
assert [v[0] for v in observed]==['baseline','streaming','streaming_state','baseline_recheck']
measurements={name:dict(seconds=float(seconds),allocated_bytes=int(allocated),gc_cycles=int(gc)) for name,seconds,allocated,gc in observed}
assert all(0<v['seconds']<20 for v in measurements.values())
assert live['environment']['GOMEMLIMIT']=='2GiB' and live['environment']['GOMAXPROCS']=='2' and live['environment']['WF_AUDIT_BATCH_PROFILE_INVOCATIONS']=='100000'
store_files=[p for p in (root/'stores').rglob('*') if p.is_file()]
summary=dict(source=e['source'],exit_code=e['exit_code'],race=race,selected_inputs=len(before),local_git_inputs=local,all_captured_source_hashes_verified=True,all_local_inputs_match_executed_git=True,actual_runner_matches_executed_git=True,actual_binary_sha256=sha(binary),actual_binary_retained=True,all_actual_build_info_fields_match=True,live_proc_identity_captured=True,actual_live_environment_verified=True,named_results=results,native_files=len(store_files),native_file_bytes=sum(p.stat().st_size for p in store_files),raw_result_log=logs,measurements=measurements,all_reports_match=True,physical_stores_reopened=False,qualifies_24h_row=False,qualifies_full_matrix=False,scope='Four same-store full100k/1.2M reads agree under20s. Combined mode faster in this fixture; streaming alone is close to baseline. Allocation/GC covers embedded servers and client, not peak/checker-only memory. No fault/production/five-container/race/24h qualification.')
a.output.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({k:v for k,v in summary.items() if k!='raw_result_log'},indent=2))
