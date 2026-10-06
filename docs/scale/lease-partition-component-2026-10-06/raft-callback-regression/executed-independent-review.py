from pathlib import Path
import json,subprocess,hashlib,io,importlib.util,sys,re,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-raft-callback-regression-20261006');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
result=json.loads((root/'result.json').read_text());rev=subprocess.check_output(['git','rev-parse','f090279'],cwd=repo,text=True).strip();assert result['source']==rev
inputs=json.loads((root/'module-inputs.json').read_text());assert shared.sha(root/'reference.mod')==inputs['mod_sha256'] and shared.sha(root/'reference.sum')==inputs['sum_sha256']
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(selected)==set(before['files'])
s=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in selected).encode()))
for n in selected:
 header=s.readline().split();assert header[1]==b'blob';data=s.read(int(header[2]));assert s.read(1)==b'\n';assert hashlib.sha256(data).hexdigest()==before['files'][n]==shared.sha(root/'selected-source'/n)==shared.sha(repo/n)
assert not s.read()
for filename,name in [('fixture.go','scripts/fixtures/nats-raft-obsolete-catchup_test.go.txt'),('patcher.py','scripts/raft-obsolete-catchup-candidate.py')]:assert (root/filename).read_bytes()==subprocess.check_output(['git','cat-file','blob',rev+':'+name],cwd=repo)
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('patcher',root/'patcher.py');patcher=importlib.util.module_from_spec(spec);spec.loader.exec_module(patcher)
base=(root/'nats-source/server/raft.go').read_text();assert base.count(patcher.OLD)==1 and (root/'candidate-raft.go').read_text()==base.replace(patcher.OLD,patcher.NEW)
original=json.loads((repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-failure/external-source-before.json').read_text());assert shared.sha(root/'nats-source/server/raft.go')==original['/home/exedev/go/pkg/mod/github.com/nats-io/nats-server/v2@v2.15.0/server/raft.go']
module=json.loads((root/'nats-source-before.json').read_text());assert module==json.loads((root/'nats-source-after.json').read_text())==fixture_archive.inventory(root/'nats-source')
deps=json.loads((root/'dependencies-before.json').read_text());assert deps==json.loads((root/'dependencies-after.json').read_text())
for path,row in deps.items():assert shared.sha(path)==shared.sha(root/row['captured'])==row['sha256']
expected_codes={'upstream-regression':1,'candidate-regression':0,'upstream-controls':0,'candidate-controls':0};assert result['results']==expected_codes
for label in ['upstream','candidate']:
 binary=json.loads((root/(label+'-binary.json')).read_text());assert binary['executable_sha256']==shared.sha(root/(label+'.test')) and '-race' in binary['build']
 for scope in ['regression','controls']:
  execution=json.loads((root/(label+'-'+scope+'-execution.json')).read_text());actual=execution['actual'];assert execution['exit_code']==expected_codes[label+'-'+scope] and actual['argv']==execution['command'] and actual['actual_executable_sha256']==binary['executable_sha256'] and not Path('/proc',str(actual['pid'])).exists()
  assert actual['stat'].split(') ',1)[1].split()[19].isdigit()
  assert '-test.count=1' in execution['command'] and '-test.timeout=3m' in execution['command']
  log=(root/(label+'-'+scope+'.log')).read_text();assert 'WARNING: DATA RACE' not in log
  if scope=='regression':
   top='TestDiagnosticRaftObsoleteCatchupDoesNotCreateOrCancelState'
   for profile in ['legacy/canceled','legacy/replaced','modern/canceled','modern/replaced']:
    verdict='FAIL' if label=='upstream' and profile.startswith('modern') else 'PASS'
    assert len(re.findall(r'^\s+--- '+verdict+': '+re.escape(top+'/'+profile)+r' \(',log,re.M))==1
   assert log.count('obsolete callback changed catchup state:')==(2 if label=='upstream' else 0)
  else:
   started=re.findall(r'^=== RUN   (TestNRG\w+)$',log,re.M);passed=re.findall(r'^--- PASS: (TestNRG\w+) \(',log,re.M);assert len(started)==len(passed)==10 and set(started)==set(passed)
proof=root.with_name(root.name+'-proof');meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(root.with_suffix('.tar.gz'))==manifest and fixture_archive.inventory(root)==manifest['files']
with root.with_suffix('.tar.gz').open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
closure=shared.closure(root)
out=repo/'docs/scale/lease-partition-component-2026-10-06/raft-callback-regression';out.mkdir()
for name in ['source-before.json','source-after.json','nats-source-before.json','nats-source-after.json','dependencies-before.json','dependencies-after.json','module-inputs.json','generated-testmain-limits.json','upstream-overlay.json','candidate-overlay.json','upstream-binary.json','candidate-binary.json','upstream-regression-execution.json','candidate-regression-execution.json','upstream-controls-execution.json','candidate-controls-execution.json','upstream-regression.log','candidate-regression.log','upstream-controls.log','candidate-controls.log','result.json','closure.json','executed-producer.py']:shutil.copyfile(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-independent-review.py')
report=dict(source=rev,source_files=len(selected),dependency_files=len(deps),original_module_files=len(module),four_live_SDK_executable_argv_birth_captures_verified=True,Git_current_retained_before_after_source_equal=True,patch_exactly_obsolete_subscription_guard=True,upstream_modern_failures=2,candidate_all_four_pass=True,ten_existing_controls_upstream_and_candidate_pass=True,race_reports_absent=True,generated_testmain_limit=json.loads((root/'generated-testmain-limits.json').read_text()),archive=meta,closure=closure,native_matrix_qualified=False,workflow_Tier1_qualified=False,scope='Direct NATS state-transition regressions and ten focused existing controls under race. Candidate overlay only; real lease component and original SDK seed6 recovery not yet tested with candidate.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ('archive','closure','generated_testmain_limit')}))
