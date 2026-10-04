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
assert e['source']=='b3c64f185b49b270391beb730a12e39263c64910' and e['exit_code']==0
assert results.keys()=={'TestStreamingAuditNativeLegacy211'} and results['TestStreamingAuditNativeLegacy211']['status']=='pass' and race
nodes=re.findall(r'legacy-node node=(\d+) pid=(\d+) version=(\S+) actual_sha256=(\w+)',logs)
assert len(nodes)==3 and {int(n[0]) for n in nodes}=={0,1,2}
legacy=next((root/'stores').rglob('nats-server-2.11.17'))
for node,pid,version,digest in nodes:
 assert version=='2.11.17' and digest==sha(legacy) and not Path('/proc',pid,'exe').exists()
assert 'legacy-full-audit version=2.11.17 replicas=3 compaction/cohort/fresh-terminal/tombstone/snapshot-corruption/orphan/public-apis matched' in logs
assert 'matched_error=journal wf.jrn.audit.legacy-orphan has no invocation' in logs
for required in ['terminal state differs','terminal state missing: nats: key not found','snapshot SHA256 differs','cohort=false report={Invocations:2 Journals:2 Entries:8 Terminal:2} matched_error=<nil>']:
 assert required in logs,required
assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':integrity/snapshot.go'],cwd=a.repo)).hexdigest()==hashlib.sha256(subprocess.check_output(['git','show','283ba32:integrity/snapshot.go'],cwd=a.repo)).hexdigest()
store_files=[p for p in (root/'stores').rglob('*') if p.is_file()]
summary=dict(source=e['source'],exit_code=e['exit_code'],race=race,selected_inputs=len(before),local_git_inputs=local,all_captured_source_hashes_verified=True,all_local_inputs_match_executed_git=True,actual_runner_matches_executed_git=True,actual_binary_sha256=sha(binary),actual_binary_retained=True,all_actual_build_info_fields_match=True,live_proc_identity_captured=False,actual_legacy_server_sha256=sha(legacy),actual_legacy_nodes=nodes,named_results=results,native_files=len(store_files),native_file_bytes=sum(p.stat().st_size for p in store_files),raw_result_log=logs,physical_stores_reopened=False,qualifies_24h_row=False,qualifies_full_matrix=False,scope='Native legacy compatibility control passes exact point/batched/state/streaming reports and errors, compaction/cohort/fresh corruption/snapshot and orphan controls; both public combined APIs pass. Actual legacy process digests verified, SDK retained but external live proc identity not captured. Stores not reopened; no scale/fault/full-matrix/24h qualification.')
a.output.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({k:v for k,v in summary.items() if k!='raw_result_log'},indent=2))
