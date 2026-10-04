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
if e['exit_code']==1:
 assert e['source']=='7daea560dfa68b4b270d6b43d920f24aabcef652'
 assert results['TestAuditBatchScanCandidateNativeLeaderLossAndCancellation/leader-loss']['status']=='fail'
 assert 'leader recovery err=nats: no responders available for request count=512' in logs
 assert results['TestAuditBatchScanCandidateNativeLeaderLossAndCancellation/cancellation']['status']=='pass'
 assert results['TestAuditBatchScanCandidateNativeLeaderLossAndCancellation/consumer-deletion']['status']=='pass'
 assert results['TestAuditBatchScanCandidateNativeLegacy211']['status']=='pass'
else:
 assert e['exit_code']==0
 assert e['source']=='097c2756444c56002cfe67ce19eab5c636da2d40'
 assert all(r['status']=='pass' for r in results.values())
 for name in ['TestAuditBatchScanCandidateNativeLeaderLossAndCancellation/leader-loss','TestAuditBatchScanCandidateNativeLeaderLossAndCancellation/cancellation','TestAuditBatchScanCandidateNativeLeaderLossAndCancellation/consumer-deletion','TestAuditBatchScanCandidateNativeLegacy211','TestAuditBatchScanCandidateNativeSparseHundredThousandSpan','TestAuditBatchScanCandidateTransportFallbackResumesUnvisitedTail']:
  assert results[name]['status']=='pass',name
 for fault in ['leader-loss','cancellation','consumer-deletion']:
  match=re.search(r'fault='+fault+r' leader=(\d) client=(\d) visited=(\d+) elapsed=([^ ]+) gap_reads=(\d+) consumer_replicas=3 scan_error=(.*?) consumers=0',logs)
  assert match,fault
  leader,client,count,elapsed,gap,error=match.groups()
  assert leader!=client and int(count)==(128 if fault=='cancellation' else 3000)
  assert error==('context canceled' if fault=='cancellation' else '<nil>')
 assert re.search(r'version=2.11.17 replicas=3 count=997 elapsed=[^ ]+ gap_reads=3 consumer_replicas=3 digest=[a-f0-9]{64} consumers=0',logs)
 assert re.search(r'span=100002 retained=2 deleted=100000 elapsed=[^ ]+ leader_gap_reads=1 digest=[a-f0-9]{64} consumers=0',logs)
store_files=[path for path in (root/'native').rglob('*') if path.is_file()]
summary=dict(source=e['source'],exit_code=e['exit_code'],selected_inputs=len(before),local_git_inputs=local,all_captured_source_hashes_verified=True,all_local_inputs_match_executed_git=True,actual_runner_matches_executed_git=True,actual_binary_sha256=sha(binary),actual_binary_retained=True,all_actual_build_info_fields_match=True,named_results=results,native_files=len(store_files),native_file_bytes=sum(p.stat().st_size for p in store_files),raw_result_log=logs,physical_stores_reopened=False,production_reader_changed=False,qualifies_full_audit=False,qualifies_24h_row=False,qualifies_full_matrix=False)
a.output.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({k:v for k,v in summary.items() if k!='raw_result_log'},indent=2))
