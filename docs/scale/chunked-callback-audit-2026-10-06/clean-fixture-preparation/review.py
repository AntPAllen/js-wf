from pathlib import Path
import json,hashlib,subprocess,shutil,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-chunked-r1-full400k-clean-fixture-20261006');out=repo/'docs/scale/chunked-callback-audit-2026-10-06/clean-fixture'
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+8*60
while True:
 try:
  e=json.loads((root/'execution.json').read_text())
  if e['status'] in ('passed','failed') and (root/'original-after-verification.json').exists():break
 except (FileNotFoundError,json.JSONDecodeError):pass
 assert time.monotonic()<end,'Observer timeout; inspect same handle before restarting'
 time.sleep(1)
out.mkdir(parents=True)
assert not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256'];assert 'vcs.modified=false' in e['build_info'] and 'vcs.revision='+e['source'] in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text())
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5;nodes=set()
server_binary=root/'originals/TestConcurrentStateR5CopiedCapacityProfile/cluster/nats-server';server_sha=sha(server_binary)
for x in servers:
 node=int(x['container']['Name'].rsplit('-n',1)[-1]);nodes.add(node)
 assert x['sha256']==server_sha==sha(root/'actual-containers'/x['sha256']) and not Path('/proc/'+str(x['host_pid'])).exists()
 assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in x['actual_proc_build_info']
 assert [m['Source'] for m in x['container']['Mounts'] if m['Destination']=='/data']==[str(root/'copied-stores'/f'node-{node}')]
assert nodes==set(range(5))
copy=json.loads((root/'original-to-copy-verification.json').read_text());assert copy['canonical_git_manifest_verified']
donor=Path(copy['original']);original_manifest=json.loads((donor/'archive-manifest.json').read_text());after=json.loads((root/'original-after-verification.json').read_text());assert after['all_original_store_bytes_unchanged'] and after['files']==len(copy['files'])
for name,digest in copy['files'].items():assert sha(donor/name)==digest==original_manifest[name]['sha256']
env=json.loads((root/'commands.json').read_text())['env'];assert env['GOMAXPROCS']=='4' and env['GOGC']=='500' and env['GOMEMLIMIT']=='4GiB' and env['WF_AUDIT_CAPACITY_R1_PROCESS_FAULT']=='owner-down'
assert env['WF_AUDIT_CAPACITY_R1_CPU_PROFILE']=='1' and env['WF_AUDIT_CHUNKED_CALLBACK']=='1'
assert env['WF_AUDIT_CAPACITY_CURSOR_NAMES']=='1'
initial=json.loads((root/'originals/TestConcurrentStateR5CopiedCapacityProfile/initial-consumer-inventory.json').read_text())
assert {x['stream'] for x in initial}=={'WF_INV','WF_JRN'}
assert all('names' in x and 'names_error' in x for x in initial)
preflight_failure=any(x['error'] for x in initial)
assert env['WF_AUDIT_CAPACITY_PREPARE_RESTORED']=='1'
prep_path=root/'originals/TestConcurrentStateR5CopiedCapacityProfile/restored-cursor-preparation.json'
prep=json.loads(prep_path.read_text()) if prep_path.exists() else None
if prep is None:
 assert preflight_failure
else:
 assert prep['initial']==initial
 preflight_failure=preflight_failure or bool(prep['error'])
if prep is not None and not prep['error']:
 assert prep['before']==prep['after']
 assert {x['stream']:x['messages'] for x in prep['after']}=={'WF_INV':400000,'WF_JRN':4800000,'KV_WF_STATE':400000}
 assert {x['name'] for x in prep['deletions']}=={'wf-audit-mweCABktZmjhL89Y7euEqi','wf-audit-mweCABktZmjhL89Y7euF5i'} and all(not x['error'] for x in prep['deletions'])
 assert {x['stream'] for x in prep['final']}=={'WF_INV','WF_JRN'}
 assert all(x['count']==0 and not x['names'] and not x['consumers'] and not x['error'] for x in prep['final'])

result_path=root/'originals/TestConcurrentStateR5CopiedCapacityProfile/direct-r1-capacity-fault.json'
results=json.loads(result_path.read_text()) if result_path.exists() else []
if preflight_failure:
 assert not results and e['status']=='failed' and ('initial consumer inventory:' in (root/'native.log').read_text() or prep is not None and prep['error'])
 assert not list(result_path.parent.glob('*-cpu.pprof'))
else:
 assert 1<=len(results)<=2 and results[0]['kind']=='baseline'
if len(results)==2:assert results[1]['kind']=='owner-down'
want={'Invocations':400000,'Journals':400000,'Entries':4800000,'Terminal':400000}
for x in results:
 assert x['chunked_callback'] and x['cpu_profile'] and x['phases'] and x['heap_before']>=0 and x['heap_after']>=0 and x['gc_pause_ns']>=0
 assert (result_path.parent/(x['kind']+'-cpu.pprof')).stat().st_size>0
 assert x['memory_limit_bytes']==4*1024**3 and x['gomaxprocs']==4
 assert x['cursors'] and all(c['replicas']==1 and c['start']>=1 for c in x['cursors'])
 if not x['error']:
  assert x['report']==want and x['journal_visits']==4800000 and x['audit_ns']<=x['total_ns']<20_000_000_000
  for name in ('WF_INV','WF_JRN'):
   assert any(o['stream']==name and o['consumers']==0 and not o['error'] and o['elapsed_ns']<20_000_000_000 for o in x['cleanup'])
 if x['kind']=='owner-down' and x['target']:
  target=x['target'];kill=x['kill'];assert target['stream_name']=='WF_JRN' and target['config']['num_replicas']==1 and target['config']['mem_storage'] and target['num_pending']>0
  if kill['source_stopped']!='0001-01-01T00:00:00Z':
   assert kill['state'] in ('absent','exited','dead')
   assert target['cluster']['leader']==copy['identity']+'-n'+str(kill['node'])
   assert any(x['container']['Name'].lstrip('/')==kill['container'] for x in servers)
qualified=len(results)==2 and all(not x['error'] for x in results)
assert (e['status']=='passed')==qualified
memory=json.loads((root/'sdk-memory-observations.json').read_text());assert memory and all(x['pid']==e['pid'] for x in memory)
r={'restored_cursor_preparation':prep,'initial_consumer_inventory':initial,'sdk_memory_observations':memory,'explicit_configuration':env,'execution':e,'selected_git_source_inputs_verified':len(a['files']),'actual_server_observations':len(servers),'server_sha256':server_sha,'observed_processes_closed':True,'copied_data_mounts_verified':True,'original_store_files_unchanged':len(copy['files']),'canonical_original_archive_sha256':copy['canonical_archive_sha256'],'results':results,'direct_single_replica_400k_owner_down_fault_capacity_pass':qualified,'qualifies_24h':False,'candidate_adopted':False,'original_20s_limits_retained':True,'source_capture_scope':'Selected Git Go/module source and observed executables; not exhaustive external toolchain/compiler inputs'}
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(root/'originals/TestConcurrentStateR5CopiedCapacityProfile/initial-consumer-inventory.json',out/'initial-consumer-inventory.json');
if result_path.exists():shutil.copyfile(result_path,out/'results.json')
shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
# Bind the executed verifier to the same pinned Git source as the SDK.
if prep_path.exists():shutil.copyfile(prep_path,out/'restored-cursor-preparation.json')
module_path=root/'executed-delta-module.py'
module_path.write_bytes(subprocess.check_output(['git','show',e['source']+':scripts/fixture_delta.py'],cwd=repo))
import importlib.util
spec=importlib.util.spec_from_file_location('executed_fixture_delta',module_path)
delta=importlib.util.module_from_spec(spec);spec.loader.exec_module(delta)
proof=delta.capture(root,out,repo,e['source'],'docs/scale/concurrent-state-audit-2026-10-05/capacity-400k/archive-verification.json','copied-stores/','originals/TestConcurrentStateR5PopulationCapacity/cluster/')
assert proof['lossless_base_plus_delta_verified']
print('LOSSLESS_DELTA_VERIFIED',proof['logical_files'],proof['aliased_files'],proof['archive_bytes'],flush=True)
print('PLAIN400K_COMPARISON_REVIEW_AND_ARCHIVE_COMPLETE',qualified,flush=True)
