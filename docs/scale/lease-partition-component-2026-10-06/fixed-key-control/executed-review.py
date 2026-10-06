from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,io,shutil,re
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
r=Path('/tmp/js-wf-lease-partition-component-admitted-20261006');proof=r.with_name(r.name+'-proof');raw=r.with_suffix('.tar.gz')
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-lease-partition-component-admitted-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines());assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0')
e=json.loads((r/'execution.json').read_text());rev=e['source'];assert rev==subprocess.check_output(['git','rev-parse','46772ba'],cwd=repo,text=True).strip() and e['exit_code']==0
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
data=subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode());stream=io.BytesIO(data)
for n in expected:
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][n]==shared.sha(r/'selected-source'/n)==shared.sha(repo/n)
assert not stream.read()
external=json.loads((r/'external-source-before.json').read_text());assert external==json.loads((r/'external-source-after.json').read_text())
for name,record in external.items():assert shared.sha(name)==shared.sha(r/record['captured'])==record['sha256']

command=json.loads((r/'commands.json').read_text());assert command['run']==[str(r/'diagnostic'),str(r)] and command['source']==rev
assert shared.sha(r/'helper.go')==command['helper_sha256']==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',rev+':scripts/lease-partition-component.go.txt'],cwd=repo)).hexdigest()
helper=json.loads((r/'actual-helper.json').read_text());assert helper['argv']==command['run'] and helper['exe_sha256']==shared.sha(r/'diagnostic') and not Path('/proc',str(helper['pid'])).exists()
servers=json.loads((r/'observed-servers.json').read_text());assert len(servers)==3 and sorted(s['node'] for s in servers)==[0,1,2]
original=json.loads((repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-live-diagnostics/native-failure/observed-servers.json').read_text());assert {s['actual_executable_sha256'] for s in servers}=={s['actual_executable_sha256'] for s in original}
for server in servers:
 assert not Path('/proc',str(server['pid'])).exists() and server['start_ticks'].isdigit() and shared.sha(r/server['captured'])==server['actual_executable_sha256']
 argv=server['argv'];assert argv[argv.index('-sd')+1]==server['store']==str(r/'originals'/'cluster'/('node-'+str(server['node'])))
 assert re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s',server['build_info'])
config=json.loads((r/'configuration.json').read_text());assert config['name']=='KV_WF_LEASE' and config['num_replicas']==3 and config['max_age']==12000000000 and config['max_msgs_per_subject']==1 and config['storage']=='file'
result=json.loads((r/'result.json').read_text());assert result['recovered'] is True and result['majority_probe_revision']>0 and result['acknowledged_transactions']>0
observations=[json.loads(l) for l in (r/'observations.jsonl').read_text().splitlines()];assert [o['stage'] for o in observations[:3]]==['admitted','before-cut','isolated'] and all(o['stage']=='recovering' for o in observations[3:])
ids={node:set() for node in range(3)}
for o in observations:
 assert [p['node'] for p in o['peers']]==[0,1,2]
 for peer in o['peers']:
  assert not peer.get('error') and peer['jetstream']['server_id'];ids[peer['node']].add(peer['jetstream']['server_id'])
  streams=[s for a in peer['jetstream']['account_details'] if a['name']=='$G' for s in a['stream_detail'] if s['name']=='KV_WF_LEASE'];assert len(streams)==1
  if o['stage']=='admitted':assert streams[0]['cluster']['leader']=='wf-process-2'
  if o is observations[-1] and streams[0]['cluster']['leader']==f"wf-process-{peer['node']}":assert len(streams[0]['cluster']['replicas'])==2 and all(p['current'] and not p.get('offline',False) for p in streams[0]['cluster']['replicas'])
assert all(len(v)==1 for v in ids.values()) and len(set().union(*ids.values()))==3
assert [p['routes'] for p in observations[2]['peers']]==[4,4,0] and [p['routes'] for p in observations[-1]['peers']]==[8,8,8]
meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(r)==manifest['files']
with raw.open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
closure=shared.closure(r)
out=repo/'docs/scale/lease-partition-component-2026-10-06/fixed-key-control';out.mkdir()
for name in ['execution.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','actual-helper.json','observed-servers.json','commands.json','configuration.json','observations.jsonl','result.json','closure.json','native.log']:shutil.copyfile(r/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-review.py')
report=dict(source=rev,source_files=len(expected),external_files=len(external),source_Git_current_retained_before_after_equal=True,actual_helper_verified=True,three_live_NATS_executables_equal_original_matrix_bytes=True,three_public_local_peer_ids_routes_bucket_state_verified=True,config=config,result=result,closure=closure,complete_archive=meta,component_reproduced=False,qualifies_matrix=False,scope='Fixed192-key bare KV reduction recovered. It does not reproduce the matrix stall, establish its cause, prove all KV interleavings or provide causalTier1 reproduction.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source=rev,files=len(expected),external=len(external),result=result,members=meta['members'])))
