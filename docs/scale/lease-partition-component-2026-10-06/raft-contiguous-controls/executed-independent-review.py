from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,io,re,shutil
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-raft-contiguous-controls-20261006');out=Path(str(root)+'-proof')
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
for retained,name in [('fixture.go','scripts/fixtures/nats-raft-obsolete-catchup_test.go.txt'),('patcher.py','scripts/raft-obsolete-catchup-candidate.py')]:
 assert (root/retained).read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)
spec=importlib.util.spec_from_file_location('patcher',root/'patcher.py');patcher=importlib.util.module_from_spec(spec);spec.loader.exec_module(patcher)
base=(root/'nats-source/server/raft.go').read_text();assert base.count(patcher.OLD)==1
assert (root/'strict-raft.go').read_text()==base.replace(patcher.OLD,patcher.NEW)
assert (root/'contiguous-raft.go').read_text()==base.replace(patcher.OLD,patcher.CONTIGUOUS)
inputs=read('module-inputs.json');assert sha(root/'reference.mod')==inputs['mod_sha256'] and sha(root/'reference.sum')==inputs['sum_sha256']
module=read('nats-source-before.json');assert module==read('nats-source-after.json')==fixture_archive.inventory(root/'nats-source')
deps=read('dependencies-before.json');assert deps==read('dependencies-after.json')
for path,row in deps.items():assert sha(path)==sha(root/row['captured'])==row['sha256']
controls=['TestNRG'+name for name in ['IgnoreEntryAfterCanceledCatchup','ParallelCatchupRollback','RejectAppendEntryDuringCatchupFromPreviousLeader','DelayedMessagesAfterCatchupDontCountTowardQuorum','CatchupAcksProgressWhenGivenProgressInbox','CatchupSendsProgressInboxAndRefillsWindow','CatchupCanTruncateMultipleEntriesWithoutQuorum','CatchupDoesNotTruncateCommittedEntriesDuringRedelivery','TermDoesntRollBackToPtermOnCatchup','CatchupFromNewLeaderWithIncorrectPterm','NewEntriesFromOldLeaderResetsWALDuringCatchup']]
first='TestDiagnosticRaftObsoleteCatchupDoesNotCreateOrCancelState';second='TestDiagnosticRaftObsoleteCatchupTrailingEntries'
cases=[first+'/'+suffix for suffix in ['legacy/canceled','legacy/replaced','modern/canceled','modern/replaced']]+[second+'/'+suffix for suffix in ['contiguous','ahead','behind','wrong_previous_term','wrong_leader','higher_leader_term','older_leader_term','legacy']]
observed={}
assert read('environment.json')==dict(GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='',TMPDIR=str(root/'test-tmp'))
for label in ['upstream','strict','contiguous']:
 binary=read(label+'-binary.json');assert '-race' in binary['build'] and '-mod=readonly' in binary['build'] and sha(root/(label+'.test'))==binary['executable_sha256']
 for scope in ['regression','controls']:
  e=read(label+'-'+scope+'-execution.json');log=(root/(label+'-'+scope+'.log')).read_text()
  assert e['command']==e['actual']['argv'] and e['command'][0]==str(root/(label+'.test'))
  assert e['command'][2:]==['-test.v','-test.count=1','-test.timeout=3m'] and e['actual']['cwd']==str(root/'nats-source/server')
  assert e['actual']['actual_executable_sha256']==binary['executable_sha256'] and int(e['actual']['stat'].split(') ',1)[1].split()[19])>0
  assert e['exit_code']==result['results'][label+'-'+scope]==result['expected'][label+'-'+scope]
  assert 'WARNING: DATA RACE' not in log and '--- SKIP:' not in log
  started=re.findall(r'^=== RUN   (\S+)$',log,re.M);passed=re.findall(r'^\s*--- PASS: (\S+) \(',log,re.M);failed=re.findall(r'^\s*--- FAIL: (\S+) \(',log,re.M)
  control_cases=[name for name in re.findall(r'^=== RUN   (\S+)$',(root/'upstream-controls.log').read_text(),re.M) if '/' in name]
  assert set(control_cases)=={'TestNRGRejectAppendEntryDuringCatchupFromPreviousLeader/'+suffix for suffix in ['new-entry','new-entry-backward-compatible','catchup','catchup-backward-compatible']}
  expected=[first,second,*cases] if scope=='regression' else [*controls,*control_cases]
  assert set(started)==set(expected) and len(started)==len(expected)
  assert set(passed)|set(failed)==set(expected) and not set(passed)&set(failed)
  if label=='contiguous':assert not failed and len(passed)==len(expected)
  if label=='upstream' and scope=='controls':assert not failed
  if label=='strict' and scope=='regression':assert set(failed)=={second,second+'/contiguous'}
  if label=='strict' and scope=='controls':assert set(failed)=={'TestNRGNewEntriesFromOldLeaderResetsWALDuringCatchup'}
  if label=='upstream' and scope=='regression':
   assert first+'/modern/canceled' in failed and first+'/modern/replaced' in failed and second+'/contiguous' in passed and second+'/ahead' in failed
  observed[label+'-'+scope]=dict(exit_code=e['exit_code'],started=started,passed=passed,failed=failed)
metadata=json.loads((out/'archive-verification.json').read_text());inventory=json.loads((out/'fixture-inventory.json').read_text());archive=Path(str(root)+'.tar.gz')
assert sha(out/'fixture-inventory.json')==metadata['inventory_sha256'] and archive.stat().st_size==metadata['archive_bytes'] and sha(archive)==metadata['archive_sha256']
assert fixture_archive.verify(archive)==inventory and fixture_archive.inventory(root)==inventory['files']
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
closure=shared.closure(root);assert fixture_archive.inventory(root)==inventory['files']
report=dict(source=revision,selected_git_sources=len(selected),selected_compiled_dependencies=len(deps),unchanged_module_files=len(module),direct_cases=len(cases),unchanged_upstream_controls=len(controls),results=observed,closure=closure,archive=metadata,scope='Independent direct-state and eleven-control qualification of contiguous experimental overlay only. Strict and upstream negative outcomes retained. Original170 failed comparison, production adoption, SDK matrix, full matrices and24h remain unqualified by this artifact.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-independent-review.py')
print(json.dumps(report,indent=2))
