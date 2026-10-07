from pathlib import Path
import sys,json,hashlib,subprocess,re,io,importlib.util,shutil
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-raft-safety170-synchronized-durable-20261007');proof=Path(str(root)+'-proof')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
sha=lambda p:hashlib.file_digest(Path(p).open('rb'),'sha256').hexdigest()
read=lambda p:json.loads(Path(p).read_text())
meta=read(proof/'archive-verification.json');inventory=read(proof/'fixture-inventory.json')
assert sha(proof/'fixture-inventory.json')==meta['inventory_sha256']
archive=Path(str(root)+'.tar.gz')
assert archive.stat().st_size==meta['archive_bytes'] and sha(archive)==meta['archive_sha256']
assert fixture_archive.verify(archive)==inventory and fixture_archive.inventory(root)==inventory['files']
before=read(root/'source-before.json');assert before==read(root/'source-after.json');rev=before['revision']
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines()
selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(selected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in selected).encode()))
for name in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(data).hexdigest()==before['files'][name]==sha(root/'selected-source'/name)==sha(repo/name)
assert not stream.read()
parent=read(root/'parent-reference.json');canonical=Path(parent['canonical'])
for name,digest in parent['records'].items():
 data=subprocess.check_output(['git','cat-file','blob',rev+':'+str(canonical/name)],cwd=repo)
 assert hashlib.sha256(data).hexdigest()==digest==sha(root/'parent-proof'/name)
assert parent['guard_variant']=='contiguous' and parent['candidate_profile']=='contiguous'
component=Path(parent['component_canonical'])
component_build_data=subprocess.check_output(['git','cat-file','blob',rev+':'+str(component/'candidate-build.json')],cwd=repo)
assert hashlib.sha256(component_build_data).hexdigest()==parent['component_build_sha256']
component_build=json.loads(component_build_data)
body=subprocess.check_output(['git','cat-file','blob',rev+':'+str(component/'candidate-raft.go.txt')],cwd=repo)
assert hashlib.sha256(body).hexdigest()==parent['component_candidate_raft_sha256']==component_build['candidate_raft_sha256']
assert component_build['guard_variant']=='contiguous' and component_build['source']==parent['component_source']
overlay=read(root/'parent-proof/contiguous-overlay.json')['Replace']
guard_paths=[v for k,v in overlay.items() if k.endswith('/server/raft.go')];assert len(guard_paths)==1
assert body==(root/'selected-parent-dependencies'/guard_paths[0].lstrip('/')).read_bytes()==Path(guard_paths[0]).read_bytes()
parent_meta=read(root/'parent-proof/archive-verification.json');receipt=read(root/'parent-proof/s3-readback.json')
assert receipt['archive']['full_readback']==dict(bytes=parent_meta['archive_bytes'],sha256=parent_meta['archive_sha256'])
deps=read(root/'parent-proof/dependencies-before.json');assert deps==read(root/'parent-proof/dependencies-after.json')
for path,row in deps.items():assert sha(path)==sha(root/'selected-parent-dependencies'/str(path).lstrip('/'))==row['sha256']
module=read(root/'fixture-source-before.json');assert module==read(root/'fixture-source-after.json')==read(root/'parent-proof/nats-source-before.json')==read(root/'parent-proof/nats-source-after.json')
assert fixture_archive.inventory(root/'fixture-source')==module
assert parent['test_profile']=='synchronized-setup'
change=read(root/'parent-proof/locking-change.json');reserve=read(root/'parent-proof/snapshot-reservation-change.json')
assert sha(root/'synchronized-raft_test.go')==reserve['modified_sha256']==parent['test_overlay_sha256']
assert sha(root/'fixture-source/server/raft_test.go')==change['original_sha256']
spec=importlib.util.spec_from_file_location('locking',repo/'scripts/raft-peer-test-locking.py');locking=importlib.util.module_from_spec(spec);spec.loader.exec_module(locking)
locked,changes=locking.transform((root/'fixture-source/server/raft_test.go').read_text())
assert hashlib.sha256(locked.encode()).hexdigest()==change['modified_sha256']==reserve['original_sha256'] and changes==change['changes']
spec=importlib.util.spec_from_file_location('reservation',repo/'scripts/raft-snapshot-test-reservation.py');reservation=importlib.util.module_from_spec(spec);spec.loader.exec_module(reservation)
assert reserve['reserve_all'] and not reserve['late_return']
assert reservation.transform(locked,True,False)==(root/'synchronized-raft_test.go').read_text()
assert sum(row['groups'] for row in changes)==8 and sum(row['calls'] for row in changes)==17
assert any(row['sha256']==reserve['modified_sha256'] and path.endswith('/synchronized-raft_test.go') for path,row in deps.items())
assert read(root/'parent-proof/independent-review.json')['snapshot_reserves_all'] and read(root/'parent-proof/result.json')['expected_outcomes_matched']

listing=read(root/'selected-tests.json');expected=listing['upstream']['tests'];assert len(expected)==len(set(expected))==170 and listing['contiguous']['tests']==expected
assert set(re.findall(r'^func (TestNRG\w+)\(', (root/'fixture-source/server/raft_test.go').read_text(),re.M))==set(expected)
results={}
for profile in ['upstream','contiguous']:
 execution=read(root/(profile+'-execution.json'));live=read(root/(profile+'-live-execution.json'));binary=read(root/'parent-proof'/(profile+'-binary.json'));log=(root/(profile+'.log')).read_text()
 assert '-race' in binary['build']
 assert sha(root/(profile+'.test'))==binary['executable_sha256']==execution['actual']['actual_executable_sha256']==listing[profile]['executable_sha256']
 command=[str(root/(profile+'.test')),'-test.run=^TestNRG','-test.v','-test.count=1','-test.timeout=20m']
 assert execution['command']==live['command']==command
 assert execution['actual']['argv']==live['actual']['argv']==command
 assert execution['actual']['environment']==live['environment'] and '-race=true' in execution['actual']['build_info']
 assert not Path('/proc',str(execution['actual']['pid'])).exists()
 assert execution['actual']==live['actual'] and execution['actual']['cwd']==str(root/'fixture-source/server')
 assert int(execution['actual']['stat'].split(') ',1)[1].split()[19])>0
 assert live['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='2GiB',TMPDIR=str(root/'test-tmp'),GOWORK='off',GOFLAGS='')
 r=execution['result'];assert execution['exit_code']==r['exit_code']
 for field,pattern in [('started',r'^=== RUN   (TestNRG\w+)$'),('passed',r'^--- PASS: (TestNRG\w+) \('),('failed',r'^--- FAIL: (TestNRG\w+) \('),('skipped',r'^--- SKIP: (TestNRG\w+) \(')]:assert r[field]==re.findall(pattern,log,re.M)
 assert r['started']==expected and set(r['passed'])|set(r['failed'])==set(expected) and not set(r['passed'])&set(r['failed']) and not r['skipped']
 assert r['race_report']==('WARNING: DATA RACE' in log)
 assert r['qualified']==(r['exit_code']==0 and len(r['passed'])==170 and not r['failed'] and not r['skipped'] and not r['race_report'])
 results[profile]=dict(started=len(r['started']),passed=len(r['passed']),failed=r['failed'],race_report=r['race_report'],native_exit_code=execution['exit_code'],qualified=r['qualified'])
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
closure=shared.closure(root)
assert fixture_archive.inventory(root)==inventory['files']
report=dict(orchestrator_source=rev,compiled_server_source=parent['source'],guard_variant=parent['guard_variant'],candidate_profile=parent['candidate_profile'],component_source=parent['component_source'],candidate_guard_body_sha256=parent['component_candidate_raft_sha256'],selected_git_sources=len(selected),selected_parent_dependencies=len(deps),unchanged_module_files=len(module),results=results,closure=closure,archive=meta,test_profile=parent['test_profile'],locking_groups=8,locked_peer_additions=17,snapshot_reserves_all=True,actual_live_argv_birth_executable_and_environment_observations_verified=True,scope='Independent complete170 comparison with eight peer-addition setup lock/unlock pairs/seventeen calls in two tests and complete snapshot I/O-permit reservation in a third test, no diagnostic injection. Every original assertion/case/sleep/deadline unchanged. Exact source-bound upstream/contiguous race binaries/count1/20m, actual live argv/birth/bytes/environment verified. Original unmodified-source failed verdicts remain unchanged. No production adoption, workflow matrix or Tier1 qualification.')
(proof/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,proof/'executed-independent-review.py')
print(json.dumps(report,indent=2))
