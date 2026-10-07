from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,io,re,shutil
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-raft-snapshot-reservation-controls-20261007');out=Path(str(root)+'-proof')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
sha=lambda p:hashlib.file_digest(Path(p).open('rb'),'sha256').hexdigest()
read=lambda n:json.loads((root/n).read_text())
result=read('result.json');revision=result['source'];assert result['expected_outcomes_matched']
before=read('source-before.json');assert before==read('source-after.json') and before['revision']==revision
names=subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(selected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(revision+':'+n+'\n' for n in selected).encode()))
for name in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(data).hexdigest()==before['files'][name]==sha(root/'selected-source'/name)==sha(repo/name)
assert not stream.read()
for retained,name in [('reservation.py','scripts/raft-snapshot-test-reservation.py'),('patcher.py','scripts/raft-obsolete-catchup-candidate.py')]:
 assert (root/retained).read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)
def imported(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
patcher=imported('patcher',root/'patcher.py');helper=imported('reservation',root/'reservation.py')
base=(root/'nats-source/server/raft.go').read_text();assert base.count(patcher.OLD)==1
assert (root/'contiguous-raft.go').read_text()==base.replace(patcher.OLD,patcher.CONTIGUOUS)
assert (root/'contiguous-raft.go').read_bytes()==(repo/'docs/scale/lease-partition-component-2026-10-06/contiguous-component/candidate-raft.go.txt').read_bytes()
inputs=read('module-inputs.json');assert sha(root/'reference.mod')==inputs['mod_sha256'] and sha(root/'reference.sum')==inputs['sum_sha256']
assert (root/'reference.mod').read_text()==(root/'selected-source/go.mod').read_text()+'\nreplace github.com/nats-io/nats-server/v2 => '+str(root/'nats-source')+'\n'
assert (root/'reference.sum').read_bytes()==(root/'selected-source/go.sum').read_bytes()
module=read('nats-source-before.json');assert module==read('nats-source-after.json')==fixture_archive.inventory(root/'nats-source')
assert module==json.loads((repo/'docs/scale/lease-partition-component-2026-10-06/peer-locked-safety170/fixture-source-before.json').read_text())
deps=read('dependencies-before.json');assert deps==read('dependencies-after.json')
for path,row in deps.items():assert sha(path)==sha(root/row['captured'])==row['sha256']
original=(root/'nats-source/server/raft_test.go').read_text()
profiles={'original-late':(False,True),'reserved-late':(True,True),'reserved':(True,False)}
for profile,(reserved,late) in profiles.items():
 changed=helper.transform(original,reserved,late);path=root/(profile+'-raft_test.go')
 assert changed==path.read_text()
 change=read(profile+'-change.json');assert change['original_sha256']==sha(root/'nats-source/server/raft_test.go') and change['modified_sha256']==sha(path) and change['reserve_all']==reserved and change['late_return']==late
 assert deps[str(path)]['sha256']==sha(path)
 # Check reversibility independently and preserve all existing assertions/deadlines.
 reversed=changed.replace(helper.INJECT,'').replace(helper.OBSERVE,'') if late else changed
 reversed=reversed.replace(helper.RESERVE,helper.DRAIN) if reserved else reversed
 assert reversed==original
first='TestNRGCheckpointInstallSnapshotAbortDuringWrite';cases=[first+'/RemoveOrphan',first+'/KeepAdoptedSnapshot'];expected_names=[first,*cases]
observed={};environment=dict(GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='',TMPDIR=str(root/'test-tmp'));assert read('environment.json')==environment
for production in ['upstream','contiguous']:
 for profile,(reserved,late) in profiles.items():
  label=production+'-'+profile;scope='snapshot-controls';negative=not reserved
  overlay=read(label+'-overlay.json')['Replace'];wanted={str(root/'nats-source/server/raft_test.go'):str(root/(profile+'-raft_test.go'))}
  if production=='contiguous':wanted[str(root/'nats-source/server/raft.go')]=str(root/'contiguous-raft.go')
  assert overlay==wanted
  binary=read(label+'-binary.json');assert '-race' in binary['build'] and '-mod=readonly' in binary['build'] and '-overlay='+str(root/(label+'-overlay.json')) in binary['build'] and sha(root/(label+'.test'))==binary['executable_sha256']
  e=read(label+'-'+scope+'-execution.json');log=(root/(label+'-'+scope+'.log')).read_text()
  assert e['command']==e['actual']['argv']==[str(root/(label+'.test')),'-test.run=^'+first+'$','-test.v','-test.count=1','-test.timeout=3m']
  assert e['actual']['cwd']==str(root/'nats-source/server') and e['actual']['actual_executable_sha256']==binary['executable_sha256']
  assert int(e['actual']['stat'].split(') ',1)[1].split()[19])>0
  assert e['exit_code']==result['results'][label+'-'+scope]==result['expected'][label+'-'+scope]==(1 if negative else 0)
  assert 'WARNING: DATA RACE' not in log and '--- SKIP:' not in log
  started=re.findall(r'^=== RUN   (\S+)$',log,re.M);passed=re.findall(r'^\s*--- PASS: (\S+) \(',log,re.M);failed=re.findall(r'^\s*--- FAIL: (\S+) \(',log,re.M)
  assert started==expected_names
  if negative:assert set(failed)==set(expected_names) and len(failed)==3 and not passed and log.count('writer returned before dios refill: <nil>')==2
  else:assert set(passed)==set(expected_names) and len(passed)==3 and not failed
  records=[tuple(map(int,m)) for m in re.findall(r'DIAGNOSTIC_LATE_DIOS borrowed=1 waited=true drained=(\d+) capacity=(\d+)',log)]
  if late:assert len(records)==2 and all(a==b if reserved else 0<a<b for a,b in records)
  else:assert not records
  assert e['actual']['environment']==environment and '-race=true' in e['actual']['build_info']
  assert not Path('/proc',str(e['actual']['pid'])).exists()
  observed[label]=dict(exit_code=e['exit_code'],expected_native_failure=negative,started=started,passed=passed,failed=failed,late_permit_observations=records,actual_sdk_sha256=binary['executable_sha256'])
metadata=json.loads((out/'archive-verification.json').read_text());inventory=json.loads((out/'fixture-inventory.json').read_text());archive=Path(str(root)+'.tar.gz')
assert sha(out/'fixture-inventory.json')==metadata['inventory_sha256'] and archive.stat().st_size==metadata['archive_bytes'] and sha(archive)==metadata['archive_sha256']
assert fixture_archive.verify(archive)==inventory and fixture_archive.inventory(root)==inventory['files']
shared=imported('shared',repo/'scripts/run-domain-runtime-controls.py');closure=shared.closure(root);assert fixture_archive.inventory(root)==inventory['files']
report=dict(source=revision,selected_git_sources=len(selected),selected_compiled_dependencies=len(deps),unchanged_module_files=len(module),original_test_cases=1,original_subcases=2,results=observed,closure=closure,archive=metadata,scope='Independent focused native race/count1/3m/2CPU/2GiB controls. Original available-only drain fails with a controlled late I/O holder release on both upstream and contiguous code; full reservation passes the identical original assertions/subcases with and without that same injection. Historical full170 cause untraced; no full170/default adoption/matrix/Tier1/24h acceptance.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-independent-review.py');print(json.dumps(report,indent=2))
