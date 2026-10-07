from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,io,re,shutil
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-raft-synchronized-setup-controls-20261007');out=Path(str(root)+'-proof')
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
for retained,name in [('locking.py','scripts/raft-peer-test-locking.py'),('reservation.py','scripts/raft-snapshot-test-reservation.py'),('patcher.py','scripts/raft-obsolete-catchup-candidate.py')]:
 assert (root/retained).read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)
spec=importlib.util.spec_from_file_location('patcher',root/'patcher.py');patcher=importlib.util.module_from_spec(spec);spec.loader.exec_module(patcher)
base=(root/'nats-source/server/raft.go').read_text();assert base.count(patcher.OLD)==1
assert (root/'contiguous-raft.go').read_text()==base.replace(patcher.OLD,patcher.CONTIGUOUS)
inputs=read('module-inputs.json');assert sha(root/'reference.mod')==inputs['mod_sha256'] and sha(root/'reference.sum')==inputs['sum_sha256']
module=read('nats-source-before.json');assert module==read('nats-source-after.json')==fixture_archive.inventory(root/'nats-source')
deps=read('dependencies-before.json');assert deps==read('dependencies-after.json')
for path,row in deps.items():assert sha(path)==sha(root/row['captured'])==row['sha256']
spec=importlib.util.spec_from_file_location('locking',root/'locking.py');locking=importlib.util.module_from_spec(spec);spec.loader.exec_module(locking)
original=(root/'nats-source/server/raft_test.go').read_text();modified,changes=locking.transform(original)
assert modified==(root/'locked-raft_test.go').read_text()
change=read('locking-change.json');assert change['changes']==changes and change['original_sha256']==sha(root/'nats-source/server/raft_test.go') and change['modified_sha256']==sha(root/'locked-raft_test.go')
assert sum(row['groups'] for row in changes)==8 and sum(row['calls'] for row in changes)==17
spec=importlib.util.spec_from_file_location('reservation',root/'reservation.py');reservation=importlib.util.module_from_spec(spec);spec.loader.exec_module(reservation)
synchronized=reservation.transform(modified,True,False)
assert synchronized==(root/'synchronized-raft_test.go').read_text()
reserve=read('snapshot-reservation-change.json');assert reserve['original_sha256']==sha(root/'locked-raft_test.go') and reserve['modified_sha256']==sha(root/'synchronized-raft_test.go') and reserve['reserve_all'] and not reserve['late_return']
assert deps[str(root/'synchronized-raft_test.go')]['sha256']==sha(root/'synchronized-raft_test.go')
assert synchronized.replace(reservation.RESERVE,reservation.DRAIN)==modified

assert module==json.loads((repo/'docs/scale/lease-partition-component-2026-10-06/contiguous-safety170/fixture-source-before.json').read_text())
assert (root/'contiguous-raft.go').read_bytes()==(repo/'docs/scale/lease-partition-component-2026-10-06/raft-contiguous-controls/contiguous-raft.go.txt').read_bytes()
first='TestNRGTruncateWALRevertsUncommittedRemovePeer';second='TestNRGEvictPeers'
cases=[first+'/'+suffix for suffix in ['Leader','Follower','Restart']]+[second+'/'+suffix for suffix in ['Guards','RefusesWhenQuorumPossible','RefusesForUncommittedRemovePeer','RevertsUncommittedRemovePeer','RevertsUncommittedSelfRemoval','RevertsUncommittedAddPeer','RevertedAddPeerCommitsAsNoopWhenLeader']]
third='TestNRGCheckpointInstallSnapshotAbortDuringWrite';cases.extend([third+'/RemoveOrphan',third+'/KeepAdoptedSnapshot'])
observed={}
assert read('environment.json')==dict(GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='',TMPDIR=str(root/'test-tmp'))
for label in ['upstream','contiguous']:
 binary=read(label+'-binary.json');assert '-race' in binary['build'] and '-mod=readonly' in binary['build'] and sha(root/(label+'.test'))==binary['executable_sha256']
 for scope in ['synchronized-controls']:
  e=read(label+'-'+scope+'-execution.json');log=(root/(label+'-'+scope+'.log')).read_text()
  assert e['command']==e['actual']['argv'] and e['command'][0]==str(root/(label+'.test'))
  assert e['command'][2:]==['-test.v','-test.count=1','-test.timeout=3m'] and e['actual']['cwd']==str(root/'nats-source/server')
  assert e['actual']['actual_executable_sha256']==binary['executable_sha256'] and int(e['actual']['stat'].split(') ',1)[1].split()[19])>0
  assert e['exit_code']==result['results'][label+'-'+scope]==result['expected'][label+'-'+scope]
  assert 'WARNING: DATA RACE' not in log and '--- SKIP:' not in log
  started=re.findall(r'^=== RUN   (\S+)$',log,re.M);passed=re.findall(r'^\s*--- PASS: (\S+) \(',log,re.M);failed=re.findall(r'^\s*--- FAIL: (\S+) \(',log,re.M)
  expected=[first,second,third,*cases]
  assert e['command'][1]=='-test.run=^TestNRG(TruncateWALRevertsUncommittedRemovePeer|EvictPeers|CheckpointInstallSnapshotAbortDuringWrite)$'
  assert read(label+'-overlay.json')['Replace'][str(root/'nats-source/server/raft_test.go')]==str(root/'synchronized-raft_test.go')
  assert set(started)==set(passed)==set(expected) and len(started)==len(passed)==len(expected) and not failed
  assert e['actual']['environment']==read('environment.json') and '-race=true' in e['actual']['build_info']
  assert not Path('/proc',str(e['actual']['pid'])).exists()
  observed[label+'-'+scope]=dict(exit_code=e['exit_code'],started=started,passed=passed,failed=failed)
metadata=json.loads((out/'archive-verification.json').read_text());inventory=json.loads((out/'fixture-inventory.json').read_text());archive=Path(str(root)+'.tar.gz')
assert sha(out/'fixture-inventory.json')==metadata['inventory_sha256'] and archive.stat().st_size==metadata['archive_bytes'] and sha(archive)==metadata['archive_sha256']
assert fixture_archive.verify(archive)==inventory and fixture_archive.inventory(root)==inventory['files']
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
closure=shared.closure(root);assert fixture_archive.inventory(root)==inventory['files']
report=dict(source=revision,selected_git_sources=len(selected),selected_compiled_dependencies=len(deps),unchanged_module_files=len(module),original_test_cases=3,snapshot_reserves_all=True,original_subcases=len(cases),locking_groups=8,locked_peer_additions=17,results=observed,closure=closure,archive=metadata,scope='Independent composed setup qualification: eight lock pairs/17 peer additions in two original tests plus complete I/O-permit reservation in a third original snapshot test, no injection. Both upstream/contiguous actual race binaries/count1/3m/2CPU/2GiB pass all three top-level tests/twelve original subcases. Original assertions/sleeps/deadlines unchanged; historical full170 failures remain failed, no full derived170/default adoption/matrix/24h qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-independent-review.py')
print(json.dumps(report,indent=2))
