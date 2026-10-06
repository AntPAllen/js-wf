from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,io,shutil,re
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
r=Path('/tmp/js-wf-partition-seed6-quiescent-corrected-20261006');proof=r.with_name(r.name+'-proof');raw=r.with_suffix('.tar.gz')
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-partition-seed6-quiescent-corrected-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines());assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0')
e=json.loads((r/'execution.json').read_text());rev=e['source'];assert rev==subprocess.check_output(['git','rev-parse','623ab82'],cwd=repo,text=True).strip() and e['exit_code']==0 and e['original_native_failure_unchanged'] and e['restored_baseline_unchanged']
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
data=subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode());stream=io.BytesIO(data)
for n in expected:
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][n]==shared.sha(r/'selected-source'/n)==shared.sha(repo/n)
assert not stream.read()
external=json.loads((r/'external-source-before.json').read_text());assert external==json.loads((r/'external-source-after.json').read_text())
for name,record in external.items():assert shared.sha(name)==shared.sha(r/record['captured'])==record['sha256']
original_manifest=json.loads((repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-failure/fixture-inventory.json').read_text());assert fixture_archive.inventory(r/'restored')==original_manifest['files']
original_execution=json.loads((r/'restored'/'execution.json').read_text());assert original_execution['exit_code']==1 and original_execution['source']==e['original_source'] and original_execution['status']=='failed'
copy=json.loads((r/'copy-before.json').read_text());assert copy['all_copy_bytes_modes_mtimes_equal'] and copy['files']==fixture_archive.inventory(r/'restored'/'originals'/original_execution['test'])
command=json.loads((r/'commands.json').read_text());assert command['source']==rev and command['run']==[str(r/'diagnostic'),str(r/'originals'/'cluster'),str(r/'exact-nats-server'),str(r/'diagnostics')]
assert shared.sha(r/'helper.go')==command['helper_sha256']==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',rev+':scripts/partition-recovery-diagnostic.go.txt'],cwd=repo)).hexdigest()
helper=json.loads((r/'actual-helper.json').read_text());assert helper['argv']==command['run'] and helper['exe_sha256']==shared.sha(r/'diagnostic') and not Path('/proc',str(helper['pid'])).exists()
assert helper['stat'].split()[0]==str(helper['pid']) and helper['stat'].rsplit(')',1)[1].split()[19].isdigit()
servers=json.loads((r/'observed-servers.json').read_text());assert len(servers)==3 and sorted(s['node'] for s in servers)==[0,1,2] and len({s['pid'] for s in servers})==3
original_servers=json.loads((r/'restored'/'observed-servers.json').read_text());assert {s['actual_executable_sha256'] for s in original_servers}=={command['exact_nats_sha256']}
for server in servers:
 assert server['actual_executable_sha256']==command['exact_nats_sha256']==shared.sha(r/'exact-nats-server')==shared.sha(r/server['captured'])
 assert server['start_ticks'].isdigit() and not Path('/proc',str(server['pid'])).exists()
 argv=server['argv'];assert argv[0]==str(r/'exact-nats-server') and argv[argv.index('-sd')+1]==server['store']==str(r/'originals'/'cluster'/('node-'+str(server['node'])))
 assert re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s',server['build_info'])
result=json.loads((r/'diagnostics'/'result.json').read_text());assert result['all_ten_streams_current'] and len(result['server_ids'])==3
observations=[json.loads(l) for l in (r/'diagnostics'/'observations.jsonl').read_text().splitlines()]
names={'WF_INV','WF_JRN','WF_RUN','WF_SIG','KV_WF_LEASE','KV_WF_STATE','WF_PURGE','KV_WF_VIEW','KV_WF_ASSIGN','OBJ_WF_BLOB'}
assert len(observations)==result['rounds']*10 and len({(o['round'],o['stream']) for o in observations})==len(observations)
for round in range(1,result['rounds']+1):assert {o['stream'] for o in observations if o['round']==round}==names
for o in observations:
 assert o['elapsed_ns']>0 and o['elapsed_ns']<=result['elapsed_ns']
 if o['round']==result['rounds']:
  assert 'error' not in o and o['info']['cluster']['leader'] and len(o['info']['cluster']['replicas'])==2
  assert all(p['current'] and not p.get('offline',False) for p in o['info']['cluster']['replicas'])
public_ids=set()
for node in range(3):
 public=json.loads((r/'diagnostics'/f"jsz-round-{result['rounds']:03d}-node-{node}.json").read_text());public_ids.add(public['server_id'])
 accounts=[a for a in public['account_details'] if a['name']=='$G'];assert len(accounts)==1
 assert names.issubset({s['name'] for s in accounts[0]['stream_detail']})
assert public_ids==set(result['server_ids'])
meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(r)==manifest['files']
with raw.open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
closure=shared.closure(r)
out=repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-quiescent-restart/corrected';out.mkdir(exist_ok=True)
for name in ['execution.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','actual-helper.json','observed-servers.json','commands.json','restore.json','copy-before.json','closure.json']:shutil.copyfile(r/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copytree(r/'diagnostics',out/'diagnostics');shutil.copyfile(__file__,out/'executed-review.py')
report=dict(source=rev,unit=unit,source_files=len(expected),external_files=len(external),source_git_current_retained_before_after_equal=True,full_S3_archive_restored_baseline_unchanged=True,initial_mutable_copy_bytes_modes_mtimes_equal=True,three_live_captured_exact_original_NATS_executables_argv_birth_closed=True,public_local_jsz_server_ids_and_all_ten_store_names_verified=True,result=result,closure=closure,complete_archive=meta,qualifies_original_partition_row=False,scope='Quiescent all-server fresh-copy restart, with no workflow writers. Original seed6 replica-heal failure and200 row remain failed; original cause and required Tier1 reproduction remain open.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source=rev,files=len(expected),external=len(external),result=result,members=meta['members'])))
