from pathlib import Path
import json,hashlib,subprocess,shutil,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-callback-400k-four-core-copied-comparison-20261005');out=repo/'docs/scale/callback-audit-delivery-2026-10-05/capacity-400k'
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
env=json.loads((root/'commands.json').read_text())['env'];assert env['GOMAXPROCS']=='4' and env['GOGC']=='200' and env['GOMEMLIMIT']=='2GiB' and env['WF_AUDIT_CAPACITY_CALLBACK_COMPARISON']=='1'
result_path=root/'originals/TestConcurrentStateR5CopiedCapacityProfile/compact-capacity-comparison.json';results=json.loads(result_path.read_text());assert [x['Mode'] for x in results]==['sdk_concurrent','compact_concurrent','callback_concurrent','sdk_recheck']
want={'Invocations':400000,'Journals':400000,'Entries':4800000,'Terminal':400000}
for x in results:
 if x['Error']=='<nil>':assert x['Report']==want
compact=results[2];qualified=compact['Error']=='<nil>' and compact['Report']==want and compact['ElapsedNS']<20_000_000_000
assert (e['status']=='passed')==qualified
r={'explicit_configuration':env,'execution':e,'selected_git_source_inputs_verified':len(a['files']),'actual_server_observations':len(servers),'server_sha256':server_sha,'observed_processes_closed':True,'copied_data_mounts_verified':True,'original_store_files_unchanged':len(copy['files']),'canonical_original_archive_sha256':copy['canonical_archive_sha256'],'results':results,'callback_400k_quiet_capacity_pass':qualified,'qualifies_24h':False,'candidate_adopted':False,'original_20s_limits_retained':True,'source_capture_scope':'Selected Git Go/module source and observed executables; not exhaustive external toolchain/compiler inputs'}
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(result_path,out/'comparison.json');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('PLAIN400K_COMPARISON_REVIEW_AND_ARCHIVE_COMPLETE',qualified,flush=True)
