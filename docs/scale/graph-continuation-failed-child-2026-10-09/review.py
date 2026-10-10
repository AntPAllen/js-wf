import hashlib,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
repo=base.parents[2]
source='b7d3a14'
sources=json.loads((base/'final-source.json').read_text())['files']
for name,digest in sources.items():
 raw=subprocess.check_output(['git','show',source+':'+name],cwd=repo)
 assert hashlib.sha256(raw).hexdigest()==digest,name
 assert (base/('final-source-'+Path(name).name+'.txt')).read_bytes()==raw
for row in json.loads((base/'actual-binaries.json').read_text()):
 assert hashlib.sha256(Path(row['binary']).read_bytes()).hexdigest()==row['sha256']
 assert '-race=true' in row['build_info']
launch=json.loads((base/'final-command.json').read_text())
assert launch['exit']==1
assert launch['source_hashes']=={k:v for k,v in sources.items() if k!='journal/graph_full_range_test.go'}
log=(base/'final-race.log').read_text()
assert 'WARNING: DATA RACE' not in log
cases={f'{domain}/archive={archive}/buffered={buffered}' for domain in ('R1','R3Domain') for archive in ('false','true') for buffered in ('false','true')}
passed={name:float(elapsed) for name,elapsed in re.findall(r'--- PASS: TestNativeGraphContinuationFailedChildPromise/(\S+) \(([0-9.]+)s\)',log)}
failed='R3Domain/archive=true/buffered=true'
assert set(passed)==cases-{failed}
assert re.findall(r'--- FAIL: TestNativeGraphContinuationFailedChildPromise/(\S+) ',log)==[failed]
assert log.count('GRAPH_FAILED_CHILD ')==7
assert log.count('child_calls=1 preserved_error=planned_child_failure parent_result=43')==7
assert log.count('ARCHIVE_CHILD_RECLAIMED terminal_receipts=1 result_bytes=0')==3
assert log.count('ARCHIVE_COLLECTION stage=next original_entry_receipts_removed=10 live_records=3 logical_records=10')==3
assert log.count('ARCHIVE_COLLECTION stage=finish original_entry_receipts_removed=13 live_records=3 logical_records=20')==1
assert log.count('ARCHIVE_COLLECTION stage=finish original_entry_receipts_removed=15 live_records=3 logical_records=22')==2
assert log.count('effects=2 records=25 result=43')==7
journal=(base/'final-journal-race.log').read_text()
assert json.loads((base/'journal-command-result.json').read_text())['actual_exit_code']==0
assert '--- FAIL:' not in journal and 'WARNING: DATA RACE' not in journal
assert 'FULL_HISTORY_RANGE records=33 range_gets=97 point_gets=226 simulated_elapsed=19.4s pin_ttl=4s' in journal
assert len(re.findall(r'--- PASS: TestGraphCheckpointArchiveLogicalHistoryAndCollection/\d+ ',journal))==16
control=json.loads((base/'point-read-control-command.json').read_text())
assert control['actual_exit_code']==1
assert (base/'point-read-control.go.txt').read_bytes()==subprocess.check_output(['git','show','1a129b7:journal/graph.go'],cwd=repo)
assert 'range/point object GET census 226 226' in (base/'point-read-control.log').read_text()
for name in ('command.json','heartbeat-command.json','phase-command.json','boundary-command.json'):
 assert json.loads((base/name).read_text())['exit']==1
review=dict(verdict='INCOMPLETE native failed-child matrix: seven pass, one fails; journal controls PASS',native_matrix_accepted=False,failed_case=failed,source=source,native_cases=passed,journal_race_seconds=52.405,archive_model_cases=16,full_history_records=33,range_object_gets=97,point_object_gets=226,simulated_renewal_seconds=19.4,pin_ttl_seconds=4,negative_control_required_failure=True,scope='Eight failed-child controls executed: seven pass, R3 buffered archive fails its first delivery with an internal append deadline; cause unconfirmed. No full matrix acceptance. Failed-child archive fixture watchdog explicitly changed from inherited60s to120s. Existing30s kill-to-exact-ACK gate unchanged. Post-launch source snapshots and actual retained binaries bind the modified files; not a pre-build immutable full dependency manifest or complete current/extended/native fault/release qualification. Public continuation admission and production collection remain closed.')
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
