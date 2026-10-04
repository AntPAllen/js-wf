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
commands=read('commands.json');assert commands['package']=='js-wf/integrity' and '-race' in commands['build']
binary=root/'integrity.test';assert sha(binary)==e['actual_binary_sha256']
actual=subprocess.check_output(['go','version','-m',str(binary)],text=True).splitlines()
recorded=(root/'binary-build-info.txt').read_text().splitlines()
assert actual[0].split(': ',1)[1]==recorded[0].split(': ',1)[1] and actual[1:]==recorded[1:]
assert any('-race=true' in line for line in actual)
events=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
results={event['Test']:dict(status=event['Action'],seconds=event.get('Elapsed')) for event in events if event['Action'] in ('pass','fail','skip') and 'Test' in event}
logs=''.join(event.get('Output','') for event in events)
assert e['exit_code']==0 and e['source']=='ddd5396065c3d1fbe41ef7720d6b14948283059d'
assert set(results)=={'TestBatchedInvariantAuditNativeConsumerReplication'} and all(r['status']=='pass' for r in results.values())
match=re.search(r'replication-audit invocations=12000 entries=144000 terminal=12000 stream_replicas=3 replicated_consumer_seconds=([0-9.]+)s replicated_error=<nil> single_consumer_seconds=([0-9.]+)s single_error=<nil> replicated_recheck_seconds=([0-9.]+)s replicated_recheck_error=<nil> actual_single_consumers=WF_INV,WF_JRN consumers=0',logs)
assert match
baseline_seconds,single_seconds,recheck_seconds=map(float,match.groups());assert all(0<x<20 for x in [baseline_seconds,single_seconds,recheck_seconds])
live=read('actual-sdk.json');assert live['sha256']==sha(binary)
info=live['build_info'].splitlines();assert info[0].split(': ',1)[1]==actual[0].split(': ',1)[1] and info[1:]==actual[1:]
store_files=[path for path in (root/'stores').rglob('*') if path.is_file()]
summary=dict(source=e['source'],exit_code=e['exit_code'],selected_inputs=len(before),local_git_inputs=local,all_captured_source_hashes_verified=True,all_local_inputs_match_executed_git=True,actual_runner_matches_executed_git=True,actual_binary_sha256=sha(binary),actual_binary_retained=True,all_actual_build_info_fields_match=True,actual_live_sdk_identity_verified=True,named_results=results,native_files=len(store_files),native_file_bytes=sum(p.stat().st_size for p in store_files),raw_result_log=logs,physical_stores_reopened=False,production_consumer_replication_changed=False,fixture_invocations=12000,fixture_entries=144000,source_stream_replicas=3,actual_single_replica_consumers=['WF_INV','WF_JRN'],replicated_baseline_seconds=baseline_seconds,single_replica_seconds=single_seconds,replicated_recheck_seconds=recheck_seconds,material_speedup_demonstrated=False,qualifies_consumer_fault_recovery=False,qualifies_24h_row=False,qualifies_full_matrix=False,scope='Same-store three reads agree at12k/144k, single-replica actual consumer config verified and source streams remain3. No clear speedup or candidate fault qualification. Production unchanged; no soak restart.')
a.output.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({k:v for k,v in summary.items() if k!='raw_result_log'},indent=2))
