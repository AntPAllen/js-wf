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
assert e['exit_code']==0 and e['source']=='4616d4795300fbfcba8396d6da001760273773f1'
expected={'TestStreamingAuditNativeJournalFaults'}|{'TestStreamingAuditNativeJournalFaults/state-'+state+'/'+fault for state in ['false','true'] for fault in ['consumer-leader-loss','cancellation']}
assert set(results)==expected and all(r['status']=='pass' for r in results.values())
assert race
fault_rows=[]
for event in events:
 line=event.get('Output','')
 if 'streaming-fault state_snapshot=' not in line: continue
 m=re.search(r'state_snapshot=(true|false) fault=(\S+) consumer=(\S*) leader=(-?\d+) pending_at_kill=(\d+) consumer_replicas=(\d+) visited=(\d+) elapsed=(\S+) report=\{Invocations:(\d+) Journals:(\d+) Entries:(\d+) Terminal:(\d+)\} err=(.*?) reconnects=(\d+)',line)
 assert m,line
 state,fault,consumer,leader,pending,replicas,visited,elapsed,inv,jrn,entries,terminal,error,reconnects=m.groups()
 assert int(inv)==500
 if fault=='consumer-leader-loss':
  assert consumer and 0<=int(leader)<3 and int(pending)>0 and int(replicas)==3
  assert (int(visited),int(jrn),int(entries),int(terminal),error)==(2000,500,2000,500,'<nil>')
 else:
  assert fault=='cancellation' and (int(visited),int(jrn),int(entries),int(terminal),error)==(128,0,0,0,'context canceled')
 unit='ms' if elapsed.endswith('ms') else 's'
 seconds=float(elapsed[:-len(unit)])/(1000 if unit=='ms' else 1)
 assert seconds<20
 fault_rows.append(dict(snapshot=state=='true',fault=fault,leader=int(leader),pending=int(pending),visited=int(visited),seconds=seconds))
assert len(fault_rows)==4 and len({(r['snapshot'],r['fault']) for r in fault_rows})==4
assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':integrity/snapshot.go'],cwd=a.repo)).hexdigest()==hashlib.sha256(subprocess.check_output(['git','show','283ba32:integrity/snapshot.go'],cwd=a.repo)).hexdigest()
store_files=[p for p in (root/'stores').rglob('*') if p.is_file()]
summary=dict(source=e['source'],exit_code=e['exit_code'],race=race,selected_inputs=len(before),local_git_inputs=local,all_captured_source_hashes_verified=True,all_local_inputs_match_executed_git=True,actual_runner_matches_executed_git=True,actual_binary_sha256=sha(binary),actual_binary_retained=True,all_actual_build_info_fields_match=True,live_proc_identity_captured=False,named_results=results,fault_rows=fault_rows,native_files=len(store_files),native_file_bytes=sum(p.stat().st_size for p in store_files),raw_result_log=logs,physical_stores_reopened=False,qualifies_24h_row=False,qualifies_full_matrix=False,scope='Native full streaming audits recover exact 500/2000/500 reports after actual pending R3 consumer leader library shutdown; cancellation stops at128. Consumer cleanup asserted by named test. State-watch interruption, OS SIGKILL, large-population recovery, five-container/full-matrix/24h qualification absent; live proc identity not captured.')
a.output.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({k:v for k,v in summary.items() if k!='raw_result_log'},indent=2))
