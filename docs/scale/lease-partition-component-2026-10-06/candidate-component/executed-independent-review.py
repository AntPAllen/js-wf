from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,io,shutil,re,datetime
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
r=Path('/tmp/js-wf-lease-partition-component-candidate-20261006');proof=r.with_name(r.name+'-proof');raw=r.with_suffix('.tar.gz')
e=json.loads((r/'execution.json').read_text());rev=e['source'];assert rev==subprocess.check_output(['git','rev-parse','e815c3b'],cwd=repo,text=True).strip() and e['exit_code']==0 and e['server_profile']=='obsolete-catchup-candidate'
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode()))
for n in expected:
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][n]==shared.sha(r/'selected-source'/n)==shared.sha(repo/n)
assert not stream.read()
external=json.loads((r/'external-source-before.json').read_text());assert external==json.loads((r/'external-source-after.json').read_text())
for name,record in external.items():assert shared.sha(name)==shared.sha(r/record['captured'])==record['sha256']
command=json.loads((r/'commands.json').read_text());assert command['run']==[str(r/'diagnostic'),str(r),'fresh','production','debug','production',str(r/'candidate-server')] and command['source']==rev and command['server_profile']=='obsolete-catchup-candidate'
assert shared.sha(r/'helper.go')==command['helper_sha256']==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',rev+':scripts/lease-partition-component.go.txt'],cwd=repo)).hexdigest()
assert (r/'executed-producer.py').read_bytes()==subprocess.check_output(['git','cat-file','blob',rev+':scripts/run-lease-partition-component.py'],cwd=repo)
helper=json.loads((r/'actual-helper.json').read_text());assert helper['argv']==command['run'] and helper['exe_sha256']==shared.sha(r/'diagnostic') and not Path('/proc',str(helper['pid'])).exists() and helper['stat'].rsplit(')',1)[1].split()[19].isdigit()
build=json.loads((r/'candidate-build.json').read_text());assert build['source']==rev and build['executable_sha256']==shared.sha(r/'candidate-server')
assert '-mod=readonly' in build['build'] and '-overlay='+str(r/'candidate-overlay.json') in build['build'] and build['build'][-1]=='github.com/nats-io/nats-server/v2'
inputs=json.loads((r/'candidate-module-inputs.json').read_text());assert inputs=={'mod_sha256':shared.sha(r/'candidate.mod'),'sum_sha256':shared.sha(r/'candidate.sum')}
assert (r/'candidate.mod').read_text()==(repo/'go.mod').read_text()+'\nreplace github.com/nats-io/nats-server/v2 => '+str(r/'candidate-nats-source')+'\n' and (r/'candidate.sum').read_bytes()==(repo/'go.sum').read_bytes()
module_before=json.loads((r/'candidate-nats-source-before.json').read_text());assert module_before==json.loads((r/'candidate-nats-source-after.json').read_text())==fixture_archive.inventory(r/'candidate-nats-source')
module_root=Path(subprocess.check_output(['go','list','-m','-f','{{.Dir}}','github.com/nats-io/nats-server/v2'],cwd=repo,text=True).strip());assert fixture_archive.inventory(module_root)==module_before
spec=importlib.util.spec_from_file_location('patcher',r/'candidate-patcher.py');patcher=importlib.util.module_from_spec(spec);spec.loader.exec_module(patcher)
assert (r/'candidate-patcher.py').read_bytes()==subprocess.check_output(['git','cat-file','blob',rev+':scripts/raft-obsolete-catchup-candidate.py'],cwd=repo)
base=(r/'candidate-nats-source/server/raft.go').read_text();assert base.count(patcher.OLD)==1 and (r/'candidate-raft.go').read_text()==base.replace(patcher.OLD,patcher.NEW)
assert build['original_raft_sha256']==shared.sha(r/'candidate-nats-source/server/raft.go') and build['candidate_raft_sha256']==shared.sha(r/'candidate-raft.go')
reference_path='docs/scale/local-tier2-partition-2026-10-06/seed-006-failure/external-source-before.json'
reference_bytes=subprocess.check_output(['git','cat-file','blob',rev+':'+reference_path],cwd=repo);assert (repo/reference_path).read_bytes()==reference_bytes;original=json.loads(reference_bytes)
assert build['original_raft_sha256']==original[str(module_root/'server/raft.go')]
deps=json.loads((r/'candidate-dependencies-before.json').read_text());assert deps==json.loads((r/'candidate-dependencies-after.json').read_text())
selected=set();server_files=0
for line in (r/'candidate-dependencies.txt').read_text().splitlines():
 directory,*groups=line.split('|')
 for name in ' '.join(groups).split():
  path=(Path(directory)/name).resolve()
  if path.is_relative_to(r/'candidate-nats-source/server'):
   assert shared.sha(path)==original[str(module_root/path.relative_to(r/'candidate-nats-source'))];server_files+=1
  if path==r/'candidate-nats-source/server/raft.go':path=r/'candidate-raft.go'
  selected.add(str(path))
assert selected==set(deps) and str(r/'candidate-nats-source/main.go') in deps
for path,row in deps.items():assert shared.sha(path)==row['sha256']==shared.sha(r/row['captured'])
servers=json.loads((r/'observed-servers.json').read_text());assert len(servers)==3 and sorted(s['node'] for s in servers)==[0,1,2]
original_servers=json.loads((repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-live-diagnostics/native-failure/observed-servers.json').read_text());assert build['executable_sha256'] not in {s['actual_executable_sha256'] for s in original_servers}
for server in servers:
 assert not Path('/proc',str(server['pid'])).exists() and server['start_ticks'].isdigit() and shared.sha(r/server['captured'])==server['actual_executable_sha256']==build['executable_sha256']
 argv=server['argv'];assert argv[0]==str(r/'candidate-server') and '-D' in argv and argv[argv.index('-sd')+1]==server['store']==str(r/'originals/cluster'/('node-'+str(server['node'])))
 assert re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s',server['build_info'])
config=json.loads((r/'configuration.json').read_text());assert config==json.loads((repo/'docs/scale/lease-partition-component-2026-10-06/raft-debug-reproduction/configuration.json').read_text())
assert config['name']=='KV_WF_LEASE' and config['num_replicas']==3 and config['max_age']==12000000000 and config['max_msgs_per_subject']==1 and config['storage']=='file' and config['subject_delete_marker_ttl']==60000000000
result=json.loads((r/'result.json').read_text());assert result['recovered'] is True and result['key_profile']=='fresh' and result['expiry_profile']=='production' and result['ttl_ns']==12000000000 and result['marker_profile']=='production' and result['marker_ttl_ns']==60000000000 and result['majority_probe_revision']>0 and result['acknowledged_transactions']>0 and result['debug_profile']=='debug'
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
final_streams=[next(s for a in peer['jetstream']['account_details'] if a['name']=='$G' for s in a['stream_detail'] if s['name']=='KV_WF_LEASE') for peer in observations[-1]['peers']]
heads=[s['state']['last_seq'] for s in final_streams];assert len(set(heads))==1
stamp=lambda value:datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
cut=stamp(result['killed']);heal=stamp(result['routes_heal_requested']);finished=stamp(result['finished']);assert 10<=(heal-cut).total_seconds()<10.1 and (stamp(observations[-1]['at'])-cut).total_seconds()<35
logs={f.name:f.read_text().splitlines() for f in (r/'originals/cluster').glob('node-*.log')}
repairs=[l for l in logs['node-2.log'] if 'S-R3F-' in l and 'Truncating and repairing WAL to Term' in l];assert len(repairs)>1
meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(r)==manifest['files']
with raw.open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
closure=shared.closure(r)
out=repo/'docs/scale/lease-partition-component-2026-10-06/candidate-component';out.mkdir()
for name in ['execution.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','actual-helper.json','observed-servers.json','commands.json','configuration.json','observations.jsonl','result.json','closure.json','native.log','candidate-build.json','candidate-module-inputs.json','candidate-overlay.json','candidate-nats-source-before.json','candidate-nats-source-after.json','candidate-dependencies-before.json','candidate-dependencies-after.json','candidate-raft.go','candidate-patcher.py','executed-producer.py']:shutil.copyfile(r/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-independent-review.py')
patterns=['Our first entry','Running catchup','Need to send snapshot','AppendEntry detected','Truncating and repairing WAL','Expected first catchup entry','Ignoring append','Snapshot sent']
selected_logs={n:[dict(line=i+1,text=l) for i,l in enumerate(lines) if 'S-R3F-' in l and any(p in l for p in patterns)] for n,lines in logs.items()}
(out/'raft-log-excerpts.json').write_text(json.dumps(selected_logs,indent=2)+'\n')
report=dict(source=rev,source_files=len(expected),helper_external_files=len(external),server_dependency_files=len(deps),original_server_files_equal_failed_matrix=server_files,unchanged_upstream_module_files=len(module_before),original_reference_input=dict(path=reference_path,Git_sha256=hashlib.sha256(reference_bytes).hexdigest()),source_Git_current_retained_before_after_equal=True,actual_helper_verified=True,three_live_candidate_executable_argv_birth_captures_verified=True,candidate_sha256=build['executable_sha256'],three_public_local_peer_ids_routes_bucket_state_verified=True,all_local_heads=heads,recovery_cut_seconds=(finished-cut).total_seconds(),recovery_heal_seconds=(finished-heal).total_seconds(),minority_WAL_repairs=len(repairs),minority_first_repair=repairs[0],minority_last_repair=repairs[-1],configuration_equal_prior_failed_production_component=True,result=result,closure=closure,complete_archive=meta,qualifies_matrix=False,qualifies_workflow_Tier1=False,scope='Source-overlay candidate real lease component recovers within unchanged35s whole-cut bound, with production TTL/markers and fresh keys. Direct diagnostic only; original SDK seed6, full matrices, broader upstream safety and production dependency adoption remain unqualified.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ('result','closure','complete_archive','original_reference_input')},indent=2))
