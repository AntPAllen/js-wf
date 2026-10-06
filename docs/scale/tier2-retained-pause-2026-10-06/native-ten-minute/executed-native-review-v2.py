from pathlib import Path
import subprocess,json,hashlib,sys,shutil
root=Path('/tmp/js-wf-tier2-retained-pause-ten-minute-20261006')
def digest(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
a=json.loads((root/'execution.json').read_text());revision=a['source']
assert a['status']=='passed' and a['exit_code']==0 and a['server_observer_errors']==0
assert not Path(f"/proc/{a['pid']}").exists()
assert digest(root/'integration.test')==a['sha256']
assert f'vcs.revision={revision}' in a['build_info'] and 'vcs.modified=false' in a['build_info']
assert a['duration']=='10m' and not a['race'] and a['sustained_ten_minutes']
b=json.loads((root/'source-before.json').read_text());c=json.loads((root/'source-after.json').read_text());assert b==c
for n,h in b['files'].items():
 assert digest(root/'source'/n)==h
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+n])).hexdigest()==h
before=json.loads((root/'external-source-before.json').read_text());assert before==json.loads((root/'external-source-after.json').read_text())
paths=json.loads((root/'external-captured-paths.json').read_text());assert set(paths)==set(before)
for n,h in before.items():assert digest(root/paths[n])==h
for path,captured in [('scripts/run-tier2-retained-row.py','executed-producer.py'),('scripts/matrix_process_observer.py','matrix_process_observer.py'),('scripts/check-matrix-result.py','executed-checker.py')]:
 assert (root/captured).read_bytes()==subprocess.check_output(['git','show',revision+':'+path])
servers=json.loads((root/'observed-servers.json').read_text());assert {v['node'] for v in servers}=={0,1,2}
for v in servers:
 assert digest(root/v['captured'])==v['actual_executable_sha256']
 assert not Path(f"/proc/{v['pid']}").exists()
 assert Path(v['store']).is_relative_to(root/'originals')
assert len({(v['pid'],v['start_ticks']) for v in servers})==len(servers)
acceptance=json.loads((root/'acceptance.json').read_text());assert acceptance['exit_code']==0 and acceptance['native_exit_code']==0
native=(root/'native.log').read_text();assert 'worker_pause fault node=' in native and 'PASS' in native
sys.path.insert(0,'scripts')
import importlib.util
spec=importlib.util.spec_from_file_location('matrix_check','scripts/check-matrix-result.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
module.check([json.loads(s) for s in (root/'converted-events.jsonl').read_text().splitlines()],a['test'],'10m')
spec=importlib.util.spec_from_file_location('campaign_check','scripts/check-matrix-campaign.py');campaign=importlib.util.module_from_spec(spec);spec.loader.exec_module(campaign)
seed_report=campaign.check_seed(native,'worker_pause',1,a['test']);assert seed_report['faults']==10
(root/'seed-review.json').write_text(json.dumps(seed_report,indent=2)+'\n')
count=seed_report['invocations']
(root/'expected-original-report.json').write_text(json.dumps({'Invocations':count,'Journals':count,'Entries':seed_report['journal_entries'],'Terminal':count},indent=2)+'\n')
clock_path=Path('scripts/tier3-worker-clock-evidence.py')
clock_bytes=subprocess.check_output(['git','show',revision+':scripts/tier3-worker-clock-evidence.py']);assert clock_path.read_bytes()==clock_bytes
(root/'executed-timestamp-helper.py').write_bytes(clock_bytes)
spec=importlib.util.spec_from_file_location('pause_timestamps',root/'executed-timestamp-helper.py');clock=importlib.util.module_from_spec(spec);spec.loader.exec_module(clock)
ns=clock.timestamp_ns
shutil.copy2(__file__,root/'executed-native-review-v2.py')
workers=json.loads((root/'observed-workers.json').read_text());assert len(workers)==3
by_pid={v['pid']:v for v in workers};assert len(by_pid)==3
for v in workers:
 assert v['actual_executable_sha256']==a['sha256']==digest(root/v['captured'])
 assert not Path(f"/proc/{v['pid']}").exists()
 assert v['argv'][1:]==['-test.run=^TestMixedMatrixWorkerProcessChild$']
 assert Path(v['process_root'])==root/'originals'
faults=json.loads((root/'matrix-pause-1-faults.json').read_text())
assert faults['seed']==1 and campaign.seconds(faults['duration'])==600 and len(faults['faults'])==10
for f in faults['faults']:
 assert not f.get('error') and f['active_leases']>0 and f['fencing_events']>0
 assert len(f['paused_leases'])==f['active_leases']
 assert f['pid'] in by_pid and by_pid[f['pid']]['worker_id']==f['worker']
 paused=ns(f['paused']);resumed=ns(f['resumed'])
 assert resumed-paused>=45_000_000_000
 assert all(l['worker_id']==f['worker'] and l['epoch']>0 and l['revision']>0 for l in f['paused_leases'])
 assert resumed-paused>=45_000_000_000
 fence_path=root/('matrix-pause-1-'+f['worker']+'-fencing.jsonl')
 data=fence_path.read_bytes();assert data.endswith(b'\n')
 records=[json.loads(line) for line in data.splitlines()]
 held={(l['key'],l['epoch']) for l in f['paused_leases']};matches=0
 for i,record in enumerate(records,1):
  event=record['event'];assert record['pid']==f['pid'] and record['sequence']==i and event['Worker']==f['worker']
  at=ns(event['At'])
  if at>=resumed and (event['Type']+'.'+event['ID'],event['Epoch']) in held:matches+=1
 assert matches>=f['fencing_events']>0
assert 'worker faults=10 active_worker_faults=10 row=worker_pause' in native
report={'source':revision,'actual_sdk_pid':a['pid'],'actual_sdk_sha256':a['sha256'],'source_files':len(b['files']),'external_files':len(before),'server_observations':len(servers),'server_nodes':[0,1,2],'native_fault_admitted':True,'native_duration_accepted':True,'closed_sdk_and_observed_servers':True,'smoke_only':False,'independent_store_audit':False,'scope':'Original ten-minute active-pause row checked; independent copied-store and full-matrix qualification remain separate.'}
(root/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
