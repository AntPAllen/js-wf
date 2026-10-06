from pathlib import Path
import sys,json,hashlib,subprocess,re,io,importlib.util,shutil
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-raft-safety170-20261006');proof=Path(str(root)+'-proof')
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
 assert hashlib.sha256(data).hexdigest()==before['files'][name]==sha(root/'selected-source'/name)
assert not stream.read()
parent=read(root/'parent-reference.json');canonical=Path(parent['canonical'])
for name,digest in parent['records'].items():
 data=subprocess.check_output(['git','cat-file','blob',rev+':'+str(canonical/name)],cwd=repo)
 assert hashlib.sha256(data).hexdigest()==digest==sha(root/'parent-proof'/name)
parent_meta=read(root/'parent-proof/archive-verification.json');receipt=read(root/'parent-proof/s3-readback.json')
assert receipt['archive']['full_readback']==dict(bytes=parent_meta['archive_bytes'],sha256=parent_meta['archive_sha256'])
deps=read(root/'parent-proof/dependencies-before.json');assert deps==read(root/'parent-proof/dependencies-after.json')
for path,row in deps.items():assert sha(root/'selected-parent-dependencies'/str(path).lstrip('/'))==row['sha256']
module=read(root/'fixture-source-before.json');assert module==read(root/'fixture-source-after.json')==read(root/'parent-proof/nats-source-before.json')==read(root/'parent-proof/nats-source-after.json')
assert fixture_archive.inventory(root/'fixture-source')==module
listing=read(root/'selected-tests.json');expected=listing['upstream']['tests'];assert len(expected)==len(set(expected))==170 and listing['candidate']['tests']==expected
assert set(re.findall(r'^func (TestNRG\w+)\(', (root/'fixture-source/server/raft_test.go').read_text(),re.M))==set(expected)
results={}
for profile in ['upstream','candidate']:
 execution=read(root/(profile+'-execution.json'));live=read(root/(profile+'-live-execution.json'));binary=read(root/'parent-proof'/(profile+'-binary.json'));log=(root/(profile+'.log')).read_text()
 assert '-race' in binary['build']
 assert sha(root/(profile+'.test'))==binary['executable_sha256']==execution['actual']['actual_executable_sha256']==listing[profile]['executable_sha256']
 command=[str(root/(profile+'.test')),'-test.run=^TestNRG','-test.v','-test.count=1','-test.timeout=20m']
 assert execution['command']==execution['actual']['argv']==live['command']==live['actual']['argv']==command
 assert execution['actual']==live['actual'] and execution['actual']['cwd']==str(root/'fixture-source/server')
 assert int(execution['actual']['stat'].split(') ',1)[1].split()[19])>0
 assert live['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='2GiB',TMPDIR=str(root/'test-tmp'),GOWORK='off',GOFLAGS='')
 r=execution['result'];assert execution['exit_code']==r['exit_code']==1
 for field,pattern in [('started',r'^=== RUN   (TestNRG\w+)$'),('passed',r'^--- PASS: (TestNRG\w+) \('),('failed',r'^--- FAIL: (TestNRG\w+) \('),('skipped',r'^--- SKIP: (TestNRG\w+) \(')]:assert r[field]==re.findall(pattern,log,re.M)
 assert r['started']==expected and set(r['passed'])|set(r['failed'])==set(expected) and not set(r['passed'])&set(r['failed']) and not r['skipped']
 assert r['race_report']==('WARNING: DATA RACE' in log) and not r['qualified']
 results[profile]=dict(started=len(r['started']),passed=len(r['passed']),failed=r['failed'],race_report=r['race_report'],native_exit_code=execution['exit_code'],qualified=False)
assert results['upstream']['passed']==169 and results['candidate']['passed']==166
terminal=read(proof/'terminal-storage-capture.json');assert terminal['original_producer_exit_code']==1 and terminal['original_collector_failure']=='live fixture process'
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
closure=shared.closure(root)
assert fixture_archive.inventory(root)==inventory['files']
report=dict(orchestrator_source=rev,compiled_server_source=parent['source'],selected_git_sources=len(selected),selected_parent_dependencies=len(deps),unchanged_module_files=len(module),results=results,closure=closure,archive=meta,original_collector_failure_retained=True,scope='Independent failed-run audit: actual original170 test bodies, source-bound race binaries and exact count1/20m scope. Native failures retained. Secondary collector closure failure retained; later complete storage capture does not repair native qualification. No production adoption, workflow matrix or Tier1 qualification.')
(proof/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,proof/'executed-independent-review.py')
print(json.dumps(report,indent=2))
