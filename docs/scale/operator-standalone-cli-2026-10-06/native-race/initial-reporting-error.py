from pathlib import Path
import sys,json,hashlib,subprocess,importlib.util,io,shutil,datetime,re
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-operator-domain-controls.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
root=Path('/tmp/js-wf-operator-standalone-commands-20261006');proof=root.with_name(root.name+'-proof');raw=root.with_suffix('.tar.gz')
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-operator-standalone-commands-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines());assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0')
e=json.loads((root/'execution.json').read_text());rev=e['source'];assert rev==subprocess.check_output(['git','rev-parse','f6056cf'],cwd=repo,text=True).strip() and e['exit_code']==0
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
data=subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode());stream=io.BytesIO(data)
for n in expected:
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][n]==shared.sha(root/'selected-source'/n)==shared.sha(repo/n)
assert not stream.read()
sdk=json.loads((root/'actual-sdk.json').read_text());binary=json.loads((root/'binary.json').read_text());commands=json.loads((root/'commands.json').read_text())
assert sdk['args']==commands['run'] and sdk['args'][2]=='-test.run=^('+'|'.join(native.STANDALONE_TESTS)+')$' and sdk['args'][-2:]==['-test.count=1','-test.timeout=3m']
assert sdk['working_directory']==commands['run_working_directory']==str(repo/'cmd/wf') and commands['build_working_directory']==str(repo)
assert sdk['exe_sha256']==binary['sha256']==shared.sha(root/'operator-race.test') and '-race=true' in binary['build_info'] and 'v2.15.0' in binary['build_info'];assert not Path('/proc',str(sdk['pid'])).exists()
assert sdk['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',GOWORK='off',GOFLAGS='',WF_OPERATOR_TEST_ROOT=str(root/'stores'),WF_OPERATOR_STANDALONE='1')
plugins=json.loads((root/'plugins.json').read_text());assert len(plugins)==1
assert '-race=true' in plugins[0]['build_info'] and shared.sha(plugins[0]['path'])==plugins[0]['sha256']
children=json.loads((root/'standalone-processes.json').read_text());assert len(children)==45 and len({c['pid'] for c in children})==45
assert len({c['exe_sha256'] for c in children})==1
logged=re.findall(r'operator standalone process domain="(WFOPS)?" pid=(\d+) exit=(-?\d+)',(root/'native.log').read_text())
assert {(d,int(pid),int(code)) for d,pid,code in logged}=={(c['domain'],c['pid'],c['exit_code']) for c in children}
for domain,expected,failures in [('',22,4),('WFOPS',23,5)]:
 group=[c for c in children if c['domain']==domain];assert len(group)==expected and sum(c['exit_code']==1 for c in group)==failures
 for child in group:
  assert child['exit_code'] in (0,1) and not Path('/proc',str(child['pid'])).exists()
  assert child['stat'].split()[0]==str(child['pid']) and child['stat'].rsplit(')',1)[1].split()[19].isdigit()
  argv=child['argv'];binary_path=Path(argv[0]);assert binary_path.is_relative_to(root/'stores') and binary_path.name=='wf'
  assert shared.sha(binary_path)==child['exe_sha256'] and 'vcs.revision='+rev in child['build_info'] and 'vcs.modified=false' in child['build_info'] and '-race=true' in child['build_info']
  info=subprocess.check_output(['go','version','-m',str(binary_path)],text=True);assert info==child['build_info'] and '\tpath\tjs-wf/cmd/wf\n' in info
  if '-domain' in argv:assert argv[argv.index('-domain')+1] in (domain,'MISSING') and domain=='WFOPS'
  elif '-replay-bundle' not in argv:assert domain==''
  if child['exit_code']==1:assert child['stderr'].strip()
  if '-replay-bundle' in argv:assert argv[argv.index('-url')+1]=='nats://127.0.0.1:1'
  if '-domain' in argv and argv[argv.index('-domain')+1]=='MISSING':assert child['exit_code']==1 and '-timeout' in argv
records=list((root/'stores').rglob('standalone.process.json'));assert len(records)==45
for path in records:
 child=json.loads(path.read_text());assert child in children and (path.parent/'stdout').read_text()==child['stdout'] and (path.parent/'stderr').read_text()==child['stderr']
clusters=[p for p in (root/'stores').iterdir() if (p/'node-0').is_dir()];assert len(clusters)==2 and sorted(len(list(p.glob('node-*'))) for p in clusters)==[1,3]
qualification=native.verify_standalone_log((root/'native.log').read_text());assert json.loads((root/'row-review.json').read_text())==dict(qualification=qualification,rejection=None)
closure=shared.closure(root);meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(root)==manifest['files']
with raw.open('rb') as stream:assert fixture_archive.digest(stream)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
out=repo/'docs/scale/operator-standalone-cli-2026-10-06/native-race';out.mkdir()
for name in ['native.log','execution.json','source-before.json','source-after.json','actual-sdk.json','binary.json','commands.json','plugins.json','standalone-processes.json','row-review.json','closure.json']:shutil.copyfile(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-review.py')
report=dict(unit=unit,execution=e,source_files=len(expected),source_git_before_after_current_retained_equal=True,actual_sdk_binary_args_environment_cwd_verified=True,actual_sdk_closed=True,forty_five_actual_cli_child_executables_birth_args_stdout_stderr_exit_and_closure_verified=True,retained_real_store_topology=[1,3],fresh_closure=closure,independent_log_review=qualification,complete_archive=meta,scope='Actual standalone wf default/domain command boundaries, including plugin/offline replay and expected exit1 diagnostics. No outgoing wire trace, standalone daemon/SQL, server fault, leaf, fullmatrix, million drain or actual24h qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source=rev,files=len(expected),elapsed=e['elapsed_seconds'],members=meta['members'])))
