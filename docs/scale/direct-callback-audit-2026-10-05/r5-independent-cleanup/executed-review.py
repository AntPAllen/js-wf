from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-direct-r1-r5-independent-cleanup-20261005');out=repo/'docs/scale/direct-callback-audit-2026-10-05/r5-independent-cleanup'
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+15*60
while not (root/'source-after.json').exists():
 assert time.monotonic()<end,'Observer timeout; inspect same native handle'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status'] in ('passed','failed') and not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256'];assert '-race=true' in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
env=json.loads((root/'commands.json').read_text())['env'];assert env['GOMAXPROCS']=='4' and env['GOGC']=='500' and env['GOMEMLIMIT']=='4GiB' and env['WF_TIER3_EXPLICIT_ROUTE_SEEDS']=='1'
records=json.loads((root/'actual-containers/actual-servers.json').read_text())
assert len(records)>=10
for r in records:
 assert not Path('/proc/'+str(r['host_pid'])).exists()
 assert sha(root/'actual-containers'/r['sha256'])==r['sha256']
 assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in r['actual_proc_build_info']
 assert any(m['Destination']=='/data' and str(root/'originals')+'/' in m['Source'] for m in r['container']['Mounts'])
log=(root/'native.log').read_text();qualified=e['status']=='passed' and e['exit_code']==0
cases=[]
for restart in (False,True):
 name='TestDirectR1AuditR5ProcessOwnerLoss/restart-'+str(restart).lower()
 assert '=== RUN   '+name in log
 if qualified:assert '--- PASS: '+name in log
 p=root/'originals'/name.replace('/','-')/'fault-result.json'
 if p.exists():
  result=json.loads(p.read_text());assert result['restart']==restart
  target=result['target'];kill=result['kill']
  if target:
   assert target['config']['num_replicas']==1 and target['config']['mem_storage'] and target['num_pending']>0
   owners=[r for r in records if r['container']['Name'].lstrip('/')==kill['container']]
   if kill['source_stopped']!='0001-01-01T00:00:00Z':assert owners and kill['state'] in ('absent','exited','dead')
   if qualified and restart:assert len({r['host_pid'] for r in owners})>=2
  if qualified:
   assert result['error']=='' and result['visited']==6000 and result['elapsed_ns']<20_000_000_000
  cases.append(result)
if qualified:assert len(cases)==2 and '\nPASS\n' in log and '--- FAIL:' not in log
else:assert '--- FAIL: TestDirectR1AuditR5ProcessOwnerLoss' in log
snapshots={};cleanup={};deletions={}
for restart in (False,True):
 name='TestDirectR1AuditR5ProcessOwnerLoss/restart-'+str(restart).lower()
 fixture=root/'originals'/name.replace('/','-')
 for stream in ('WF_INV','WF_JRN'):
  p=fixture/(stream+'-cursor-snapshots.json')
  if p.exists():snapshots[name+'/'+stream]=json.loads(p.read_text())
 for stream in ('WF_INV','WF_JRN'):
  p=fixture/(stream+'-delete-observations.json')
  if p.exists():deletions[name+'/'+stream]=json.loads(p.read_text())
 p=fixture/'cleanup-observations.json'
 if p.exists():
  observations=json.loads(p.read_text());cleanup[name]=observations
  if '--- PASS: '+name in log:
   for stream in ('WF_INV','WF_JRN'):
    assert any(o['stream']==stream and o['consumers']==0 and not o['error'] and o['elapsed_ns']<20_000_000_000 for o in observations)
individual_passes=re.findall(r'^    --- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)
review={'r5_process_owner_loss_pass':qualified,'execution':e,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_external_server_processes':len(records),'external_server_sha256':sorted({r['sha256'] for r in records}),'all_observed_processes_closed':True,'cases':cases,'cursor_api_snapshots':snapshots,'cleanup_observations':cleanup,'delete_observations':deletions,'individual_native_passes':individual_passes,'source_capture_scope':'Selected Git Go/module and actual SDK/external server metadata; not exhaustive compiler/toolchain inputs','candidate_adopted':False,'qualifies_large_fault_capacity':False,'qualifies_24h':False}
out.mkdir(parents=True)
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('R5_PROCESS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
