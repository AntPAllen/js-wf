from pathlib import Path
import subprocess,json,hashlib,sys
root=Path('/tmp/js-wf-hosted-partition-seed3-download-20261005/tier2-retained-partition-3-37347246576-1')
def digest(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
a=json.loads((root/'execution.json').read_text());revision=a['source']
assert a['status']=='passed' and a['exit_code']==0 and a['server_observer_errors']==0

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

 assert Path(v['store']).is_relative_to(Path(a['exe']).parent/'originals')
assert len({(v['pid'],v['start_ticks']) for v in servers})==len(servers)
acceptance=json.loads((root/'acceptance.json').read_text());assert acceptance['exit_code']==0 and acceptance['native_exit_code']==0
native=(root/'native.log').read_text();assert 'server_partition fault node=' in native and 'PASS' in native
sys.path.insert(0,'scripts')
import importlib.util
spec=importlib.util.spec_from_file_location('matrix_check','scripts/check-matrix-result.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
module.check([json.loads(s) for s in (root/'converted-events.jsonl').read_text().splitlines()],a['test'],'10m')
spec=importlib.util.spec_from_file_location('campaign_check','scripts/check-matrix-campaign.py');campaign=importlib.util.module_from_spec(spec);spec.loader.exec_module(campaign)
seed_report=campaign.check_seed(native,'server_partition',3,a['test'])
faults=json.loads((root/'matrix-partition-3-faults.json').read_text())
assert faults['seed']==3 and campaign.seconds(faults['duration'])==600 and len(faults['faults'])==19==seed_report['faults']
assert all(f['node']==2 and f['partition_routes'][2]==0 and f['partition_routes'][0]>0 and f['partition_routes'][1]>0 and f['healed']>f['killed']>=f['scheduled'] for f in faults['faults'])
(root/'seed-review.json').write_text(json.dumps(seed_report,indent=2)+'\n')
report={'source':revision,'actual_sdk_pid':a['pid'],'actual_sdk_sha256':a['sha256'],'source_files':len(b['files']),'external_files':len(before),'server_observations':len(servers),'server_nodes':[0,1,2],'native_fault_admitted':True,'native_duration_accepted':True,'hosted_native_exit_zero':True,'local_proc_not_used_to_infer_remote_pid_lifetime':True,'smoke_only':False,'independent_store_audit':False,'scope':'Original ten-minute partition row checked; independent copied-store and full-matrix qualification remain separate.'}
(root/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
