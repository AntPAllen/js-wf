import sys,json,hashlib,subprocess,shutil,copy,re,io
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-matrix-terminal-cohort-20261008');out=repo/'docs/scale/matrix-terminal-cohort-2026-10-08/accepted'
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
before=read(root/'source-before.json');assert before==read(root/'source-after.json');rev=before['revision'];assert rev=='8878b4779159f3695dc5b6dd6a93699d883250e8'
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(selected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],input=''.join(rev+':'+n+'\n' for n in selected).encode(),cwd=repo))
for n in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 p=root/'selected-source'/n
 if p.suffix=='.go':p=p.with_suffix('.go.txt')
 assert hashlib.sha256(data).hexdigest()==before['files'][n]==sha(p)
assert not stream.read()
a=read(root/'integration-actual-sdk.json');b=read(root/'integration-binary.json');e=read(root/'integration-execution.json');commands=read(root/'integration-commands.json');log=(root/'integration-actual.log').read_text()
assert e['source']==rev and e['exit_code']==0 and a['args']==commands['run'] and a['exe_sha256']==b['sha256']==sha(root/'integration-race.test') and a['admission']['stable_identity_observed_twice'] and not Path('/proc',str(a['pid'])).exists()
assert commands['run']==[str(root/'integration-race.test'),'-test.run=^(TestMatrixTerminalCohort.*|TestNativeMatrixTerminalCapturedPopulation|TestMatrixRetainedAudit.*)$','-test.v','-test.count=1','-test.timeout=3m']
assert 'vcs.revision='+rev in b['build_info'] and 'vcs.modified=false' in b['build_info'] and '-race=true' in b['build_info'] and 'github.com/nats-io/nats-server/v2\tv2.15.0' in b['build_info']
assert a['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_MATRIX_TERMINAL_COHORT_ROOT=str(root/'native-stores'))
assert a['working_directory']==str(repo)
tests=['TestMatrixTerminalCohortUsesCapturedPopulation','TestMatrixTerminalCohortRejectsMissingAndInvalidInvocations','TestNativeMatrixTerminalCapturedPopulation','TestMatrixRetainedAuditTransientCadence','TestMatrixRetainedAuditStopsOnInvariantOrExhaustion','TestMatrixRetainedAuditHonorsCancellationDuringWait','TestMatrixRetainedAuditRejectsConflictingReaders']
def check_log(value):
 assert value.rstrip().endswith('PASS') and not any(s in value for s in ['--- FAIL:','--- SKIP:','DATA RACE'])
 for test in tests:
  assert len(re.findall(r'^--- PASS: '+re.escape(test)+r' \(',value,re.M))==1
check_log(log)
controls=[]
def check(d):
 assert d['replicas']==3 and d['parent_budget_seconds']==30 and d['acknowledged_witness']==822 and d['captured_cutoff']==840
 assert d['visited']=={'stale':840,'empty':840} and d['weak_visited']==812 and d['missing_tail_visited']==839
 assert d['missing_tail_error']=='terminal invocation cohort visited=839 expected=840 cutoff=840'
 peers=d['peers'];assert len(peers)==len({p['id'] for p in peers})==3
 assert {p['name'] for p in peers}=={'wf-test-0','wf-test-1','wf-test-2'}
 assert all(p['version']=='2.15.0' and p['embedding_commit']==rev[:7] for p in peers)
p=root/'native-stores'/'TestNativeMatrixTerminalCapturedPopulation'/'terminal-cohort-proof.json';d=read(p);check(d)
changes=[('wrong_replica',lambda x:x.update(replicas=1)),('budget_relaxed',lambda x:x.update(parent_budget_seconds=60)),('witness_wrong',lambda x:x.update(acknowledged_witness=0)),('stale_cut',lambda x:x.update(captured_cutoff=812)),('stale_population',lambda x:x['visited'].update(stale=812)),('empty_population',lambda x:x['visited'].update(empty=0)),('no_weak_control',lambda x:x.update(weak_visited=840)),('missing_tail_ignored',lambda x:x.update(missing_tail_error='')),('missing_tail_population',lambda x:x.update(missing_tail_visited=840)),('duplicate_peer',lambda x:x['peers'][1].update(id=x['peers'][0]['id'])),('foreign_version',lambda x:x['peers'][0].update(version='foreign')),('foreign_commit',lambda x:x['peers'][0].update(embedding_commit='foreign'))]
for name,change in changes:
 bad=copy.deepcopy(d);change(bad)
 try:check(bad)
 except (AssertionError,KeyError):controls.append(name)
 else:raise AssertionError('accepted '+name)
for name,bad in [('missing_native',log.replace('--- PASS: TestNativeMatrixTerminalCapturedPopulation','--- OMIT: TestNativeMatrixTerminalCapturedPopulation')),('missing_retry_control',log.replace('--- PASS: TestMatrixRetainedAuditTransientCadence','--- OMIT: TestMatrixRetainedAuditTransientCadence')),('race_report',log+'\nDATA RACE\n')]:
 try:check_log(bad)
 except AssertionError:controls.append(name)
 else:raise AssertionError('bad log accepted')
staging=root.with_name(root.name+'-proof');meta=read(staging/'archive-verification.json');inv=read(staging/'fixture-inventory.json');assert fixture_archive.inventory(root)==inv['files']
with root.with_suffix('.tar.gz').open('rb') as f:declared,actual=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inv
out.mkdir(parents=True)
for n in ['source-before.json','source-after.json','integration-actual-sdk.json','integration-binary.json','integration-execution.json','integration-commands.json','integration-actual.log','executed-producer.py','closure.json']:shutil.copy2(root/n,out/n)
for n in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(staging/n,out/n)
shutil.copy2(p,out/'terminal-cohort-proof.json')
shutil.copy2(__file__,out/'executed-review.py')
report=dict(source=rev,source_inputs=len(selected),actual_sdk=a['pid'],execution=e,archive=actual,actual_positive_mutations_rejected=controls,scope='Terminal cohort scan with native R3 840/812/822 stale and empty metadata control, missing-tail rejection and original retry envelope at frozen source. Embedded SDK build/version is bound; no independent per-peer varz/process identity ledger or hermetic module cache provenance. Not workflow handler/latency/p99 execution, full200 campaign or other bulk/24h terminal metadata paths; runtime migration, final-source graph and full native/release/online GC remain open.')
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source_inputs=len(selected),mutations=len(controls),execution=e)),flush=True)
