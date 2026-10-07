import sys,json,subprocess,hashlib,importlib.util,io,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
root=Path('/tmp/js-wf-lease-component-official-rc2-v2-20261007');proof=root.with_name(root.name+'-proof');raw=root.with_suffix('.tar.gz')
unit=dict(row.split('=',1) for row in subprocess.check_output(['systemctl','show','js-wf-lease-component-official-rc2-v2-20261007.service','--property=ActiveState,SubState,MainPID,ExecMainPID,ExecMainStatus,Result,InvocationID,Restart'],text=True).splitlines())
assert unit['ActiveState']=='active' and unit['SubState']=='exited' and unit['MainPID']=='0' and unit['ExecMainStatus']=='0' and unit['Result']=='success' and unit['Restart']=='no'
e=json.loads((root/'execution.json').read_text());rev=e['source'];assert e['exit_code']==0 and e['server_profile']=='official-release'
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode()))
for n in expected:
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(body).hexdigest()==before['files'][n]==shared.sha(root/'selected-source'/n)==shared.sha(repo/n)
assert not stream.read()
external=json.loads((root/'external-source-before.json').read_text());assert external==json.loads((root/'external-source-after.json').read_text())
for name,row in external.items():assert shared.sha(name)==shared.sha(root/row['captured'])==row['sha256']
deps=json.loads((root/'official-dependencies-before.json').read_text());assert deps==json.loads((root/'official-dependencies-after.json').read_text())
for name,row in deps.items():assert shared.sha(name)==shared.sha(root/row['captured'])==row['sha256']
module=json.loads((root/'official-nats-source-before.json').read_text());assert module==json.loads((root/'official-nats-source-after.json').read_text())==fixture_archive.inventory(root/'official-nats-source')
download=json.loads((root/'official-download.json').read_text());assert fixture_archive.inventory(Path(download['Dir']))==module
assert download['Version']=='v2.15.1-RC.2' and download['Sum']=='h1:5dyJEGdG+OswDdiEvw06W7BukgvHbJEW8OrikvMbbIs='
tag=json.loads((root/'official-tag.json').read_text());assert tag['resolved_commit']==download['Origin']['Hash']=='d564fd6982a44cc47c4228b12f7a9b6c9f722a8c'
assert (root/'official-tag.json').read_bytes()==subprocess.check_output(['git','cat-file','blob',rev+':docs/scale/scheduler-upstream-rc2-2026-10-07/official-tag.json'],cwd=repo)
build=json.loads((root/'official-build.json').read_text());digest=shared.sha(root/'official-server')
assert build['executable_sha256']==digest and build['module_version']==download['Version'] and build['advertised_version']=='nats-server: v2.15.1-RC.2'
assert '-mod=readonly' in build['build'] and '-buildvcs=false' in build['build'] and not any('overlay' in a for a in build['build'])
commands=json.loads((root/'commands.json').read_text());assert commands['run']==[str(root/'diagnostic'),str(root),'fresh','production','debug','production',str(root/'official-server')]
assert shared.sha(root/'helper.go')==commands['helper_sha256']==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',rev+':scripts/lease-partition-component.go.txt'],cwd=repo)).hexdigest()
helper=json.loads((root/'actual-helper.json').read_text());assert helper['argv']==commands['run'] and helper['exe_sha256']==shared.sha(root/'diagnostic') and helper['admission']['stable_identity_observed_twice']
servers=json.loads((root/'observed-servers.json').read_text());assert len(servers)==3 and sorted(s['node'] for s in servers)==[0,1,2]
for server in servers:
 assert not Path('/proc',str(server['pid'])).exists() and server['start_ticks'].isdigit() and shared.sha(root/server['captured'])==server['actual_executable_sha256']==digest
 argv=server['argv'];assert argv[0]==str(root/'official-server') and '-D' in argv and argv[argv.index('-sd')+1]==server['store']==str(root/'originals/cluster'/('node-'+str(server['node'])))
assert not Path('/proc',str(helper['pid'])).exists() and not Path('/proc',unit['ExecMainPID']).exists()
config=json.loads((root/'configuration.json').read_text());reference=json.loads((repo/'docs/scale/lease-partition-component-2026-10-06/contiguous-component/configuration.json').read_text());assert config==reference
assert config['name']=='KV_WF_LEASE' and config['num_replicas']==3 and config['max_age']==12000000000 and config['max_msgs_per_subject']==1 and config['storage']=='file'
result=json.loads((root/'result.json').read_text());assert result['key_profile']=='fresh' and result['expiry_profile']=='production' and result['marker_profile']=='production' and result['debug_profile']=='debug' and result['ttl_ns']==12000000000 and result['marker_ttl_ns']==60000000000 and result['majority_probe_revision']>0 and result['acknowledged_transactions']>0
observations=[json.loads(line) for line in (root/'observations.jsonl').read_text().splitlines()];assert [o['stage'] for o in observations[:3]]==['admitted','before-cut','isolated'] and all(o['stage']=='recovering' for o in observations[3:])
ids={node:set() for node in range(3)}
for o in observations:
 assert [p['node'] for p in o['peers']]==[0,1,2]
 for p in o['peers']:
  assert not p.get('error');ids[p['node']].add(p['jetstream']['server_id'])
assert all(len(v)==1 for v in ids.values()) and len(set().union(*ids.values()))==3
assert [p['routes'] for p in observations[2]['peers']]==[4,4,0] and [p['routes'] for p in observations[-1]['peers']]==[8,8,8]
streams=[next(s for a in p['jetstream']['account_details'] if a['name']=='$G' for s in a['stream_detail'] if s['name']=='KV_WF_LEASE') for p in observations[-1]['peers']]
heads=[s['state']['last_seq'] for s in streams]
leaders=[s for p,s in zip(observations[-1]['peers'],streams) if s['cluster']['leader']==f"wf-process-{p['node']}"];assert len(leaders)==1
current=all(p['current'] and not p.get('offline',False) for p in leaders[0]['cluster']['replicas'])
assert len(leaders[0]['cluster']['replicas'])==2 and current==result['recovered']
parse=lambda s:datetime.datetime.fromisoformat(s.replace('Z','+00:00'))
whole=(parse(result['finished'])-parse(result['killed'])).total_seconds();postheal=(parse(result['finished'])-parse(result['routes_heal_requested'])).total_seconds()
assert (parse(result['routes_heal_requested'])-parse(result['killed'])).total_seconds()>=10
if result['recovered']:assert whole<35 and len(set(heads))==1
else:assert whole>=35
meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(root)==manifest['files']
with raw.open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
closure=shared.closure(root)
out=repo/'docs/scale/lease-partition-component-2026-10-06/official-rc2-component';out.mkdir()
for name in ['execution.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','actual-helper.json','observed-servers.json','commands.json','configuration.json','observations.jsonl','result.json','closure.json','native.log','official-build.json','official-download.json','official-tag.json','official-nats-source-before.json','official-nats-source-after.json','official-dependencies-before.json','official-dependencies-after.json']:shutil.copyfile(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-review.py')
report=dict(source=rev,unit=unit,selected_git_inputs=len(expected),helper_dependency_inputs=len(external),official_dependency_inputs=len(deps),unchanged_official_source_files=len(module),actual_helper_pid=helper['pid'],actual_three_server_ids=ids,official_server_sha256=digest,result=result,whole_cut_seconds=whole,post_heal_seconds=postheal,final_heads=heads,configuration_exactly_equals_reference=True,closure=closure,complete_archive=meta,scope='One actual unchanged official RC2 R3 fresh-key production lease component. Native recovery verdict preserved. No original workflow seed/matrix/Tier1/server-protocol cause/default adoption/million/24h qualification.')
# Sets are represented deterministically, retaining every observed identity.
report['actual_three_server_ids']={str(k):sorted(v) for k,v in ids.items()}
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print('OFFICIAL_RC2_COMPONENT_REVIEWED',result['recovered'],whole,postheal,len(expected),len(external),len(deps),meta['members'],flush=True)
