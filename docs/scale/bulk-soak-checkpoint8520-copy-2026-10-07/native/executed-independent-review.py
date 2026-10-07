from pathlib import Path
import sys,json,hashlib,subprocess,io,re,importlib.util,shutil
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-bulk-soak-checkpoint8520-copy-20261007');proof=Path(str(root)+'-proof')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
sha=lambda p:hashlib.file_digest(Path(p).open('rb'),'sha256').hexdigest()
read=lambda p:json.loads(Path(p).read_text())
meta=read(proof/'archive-verification.json');inventory=read(proof/'fixture-inventory.json');archive=root.with_suffix('.tar.gz')
assert sha(proof/'fixture-inventory.json')==meta['inventory_sha256']
assert fixture_archive.verify(archive)==inventory and fixture_archive.inventory(root)==inventory['files']
with archive.open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
before=read(root/'source-before.json');assert before==read(root/'source-after.json');revision=before['revision']
names=subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines()
selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(selected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(revision+':'+n+'\n' for n in selected).encode()))
for n in selected:
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(body).hexdigest()==before['files'][n]==sha(root/'selected-source'/n)==sha(repo/n)
assert not stream.read()
external=read(root/'external-source-before.json');assert external==read(root/'external-source-after.json')
paths=read(root/'external-captured-paths.json');assert set(paths)==set(external)
for n,digest in external.items():assert sha(root/paths[n])==digest
original=read(root/'donor-reference.json');canonical=original['canonical'];donor=Path(original['root'])
blob=lambda n:subprocess.check_output(['git','cat-file','blob',revision+':'+canonical+'/'+n],cwd=repo)
original_meta=json.loads(blob('archive-verification.json'));original_inventory_bytes=blob('fixture-inventory.json');original_inventory=json.loads(original_inventory_bytes)
assert original['archive']==original_meta and hashlib.sha256(original_inventory_bytes).hexdigest()==original_meta['inventory_sha256']
assert original['receipt']==json.loads(blob('s3-readback.json')) and original['receipt']['archive']['full_readback']==dict(bytes=original_meta['archive_bytes'],sha256=original_meta['archive_sha256'])
restore=read(root/'restore-verification.json')
assert restore['all_bytes_modes_mtimes_verified'] and restore['destination']==str(root/'restored-original')
assert restore['files']==len(original_inventory['files']) and restore['restored_bytes']==sum(x['bytes'] for x in original_inventory['files'].values())
assert restore['archive']==dict(bytes=original_meta['archive_bytes'],sha256=original_meta['archive_sha256'])
admission=read(root/'copied-store-admission.json');prefix=admission['stores_prefix'];assert prefix=='fixture/cluster/' and admission['cutoff']==238560
expected_stores={n:row for n,row in original_inventory['files'].items() if any(n.startswith(prefix+f'node-{i}/') for i in range(5))}
assert expected_stores==admission['files'] and len({n.split('/')[2] for n in expected_stores})==5
assert original['original_execution']==read(donor/'execution.json') and original['original_execution']['status']=='failed' and original['original_execution']['test_exit_code']==1
assert fixture_archive.inventory(donor)==original_inventory['files'] and read(root/'original-after-verification.json')['all_original_bytes_modes_mtimes_unchanged']
old=[json.loads(x) for x in (root/'restored-original/watch-servers.jsonl').read_text().splitlines()]
old_ids={x['inspect']['Id'] for x in old};identity=admission['identity']
assert {x['inspect']['Args'][x['inspect']['Args'].index('-n')+1].rsplit('-n',1)[0] for x in old}=={identity}
assert {x['sha256'] for x in old}=={admission['original_server_sha256']}
servers=read(root/'actual-servers.json');assert len(servers)==5
nodes=set()
for s in servers:
 assert s['container']['State']['Running'] and s['container']['Id'] not in old_ids and not Path('/proc',str(s['pid'])).exists()
 assert s['sha256']==admission['original_server_sha256']==sha(root/'actual-containers'/s['sha256'])
 assert re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s',s['build_info'])
 assert s['args']==[s['container']['Path'],*s['container']['Args']]
 name=s['args'][s['args'].index('-n')+1];assert name.startswith(identity+'-n');node=int(name[len(identity+'-n'):]);assert node in range(5) and node not in nodes;nodes.add(node)
 mounts=[m for m in s['container']['Mounts'] if m['Destination']=='/data'];assert len(mounts)==1 and mounts[0]['RW'] and mounts[0]['Source']==str(root/'restored-original/fixture/cluster'/f'node-{node}')
 assert int(s['stat'].split(') ',1)[1].split()[19])>0
assert nodes==set(range(5))
execution=read(root/'execution.json');sdk=read(root/'actual-sdk.json');binary=read(root/'binary.json');command=read(root/'commands.json')
test='TestRetainedAuditBulkSoakCheckpoint8520VerifiedCopy'
assert execution['source']==revision and execution['exit_code'] in (0,1) and execution['status']==('passed' if execution['exit_code']==0 else 'failed')
assert sdk['args']==command['test']==[str(root/'integration.test'),'-test.run=^'+test+'$','-test.count=1','-test.v','-test.timeout=6m']
assert sdk['exe']==str(root/'integration.test') and sdk['exe_sha256']==binary['sha256']==sha(root/'integration.test')
assert 'vcs.revision='+revision in binary['build_info'] and 'vcs.modified=false' in binary['build_info'] and '-race=true' not in binary['build_info']
assert sdk['working_directory']==command['working_directory']==str(repo) and not Path('/proc',str(sdk['pid'])).exists()
assert sdk['environment']==command['environment'] and {k:sdk['environment'][k] for k in ('GOMAXPROCS','GOGC','GOMEMLIMIT','GOWORK','GOFLAGS')}==dict(GOMAXPROCS='4',GOGC='500',GOMEMLIMIT='4GiB',GOWORK='off',GOFLAGS='')
assert sdk['environment']['WF_AUDIT_BULK_SOAK_STORES']==str(root/'restored-original/fixture/cluster') and sdk['environment']['WF_AUDIT_BULK_SOAK_IDENTITY']==identity
assert sdk['environment']['WF_TIER3_SYNC_INTERVAL']=='2m' and sdk['environment']['WF_TIER3_EXPLICIT_ROUTE_SEEDS']=='1'
results=list((root/'originals').rglob('copied-checkpoint-audit.json'));assert len(results)==1
result=read(results[0]);assert result['cutoff']==238560 and not result['qualifies_24h']
for n in ['WF_INV','WF_JRN','KV_WF_STATE','WF_PURGE']:
 row=result['readiness'][n];assert row['config']['num_replicas']==5 and row['config']['storage']=='file'
 assert row['cluster']['leader'] in {identity+'-n'+str(i) for i in range(5)} and len(row['cluster']['replicas'])==4
 assert all(x['current'] and not x.get('offline',False) for x in row['cluster']['replicas'])
log=(root/'native.log').read_text();assert 'DATA RACE' not in log and '--- SKIP:' not in log
passed=re.findall(r'^--- PASS: (\w+) \(',log,re.M);failed=re.findall(r'^--- FAIL: (\w+) \(',log,re.M)
assert passed==([test] if execution['exit_code']==0 else []) and failed==([test] if execution['exit_code']==1 else [])
assert 'full original checkpoint8520 copied audit cutoff=238560' in log
qualified=(result['error']=='<nil>' and result['elapsed_ns']<20_000_000_000 and result['report']['Invocations']==result['report']['Journals']==result['report']['Terminal']==238560 and result['report']['Entries']>0)
if qualified:
 for n in ['WF_STATE.WatchAll','WF_STATE.WatchStop']:
  row=result['trace']['counts'][n];assert row['started']==row['completed']==1 and row['errors']==0
assert (execution['exit_code']==0)==qualified
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
closure=shared.closure(root);assert fixture_archive.inventory(root)==inventory['files']
report=dict(source=revision,selected_git_inputs=len(selected),selected_external_inputs=len(external),restore=restore,exact_original_store_files=len(expected_stores),original_server_hash=admission['original_server_sha256'],actual_servers=5,original_complete_files_unchanged=len(original_inventory['files']),execution=execution,result=result,qualified_quiescent_copied_capacity=qualified,qualifies_24h=False,closure=closure,archive=meta,scope='Entire original checkpoint8520 cohort on fresh verified complete restore: actual identical stock NATS2.15 server binaries/five R5 sources/profile4CPU/GOGC500/4GiB/original20s attempt. No concurrent workload/fault reproduction, historical cause attribution, original24h acceptance or release adoption.')
(proof/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,proof/'executed-independent-review.py')
print(json.dumps(dict(qualified_quiescent_copied_capacity=qualified,result=result['report'],elapsed_ns=result['elapsed_ns'],actual_servers=5,full_archive_members=meta['members'],qualifies_24h=False)))
