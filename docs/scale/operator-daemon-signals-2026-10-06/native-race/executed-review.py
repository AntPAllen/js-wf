from pathlib import Path
import sys,json,hashlib,subprocess,importlib.util,io,shutil,datetime,re
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-operator-domain-controls.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
root=Path('/tmp/js-wf-operator-daemon-signals-instrumented-20261006');proof=root.with_name(root.name+'-proof');raw=root.with_suffix('.tar.gz')
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-operator-daemon-signals-instrumented-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines());assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0')
e=json.loads((root/'execution.json').read_text());rev=e['source'];assert rev==subprocess.check_output(['git','rev-parse','4a1f1bf'],cwd=repo,text=True).strip() and e['exit_code']==0
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
data=subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode());stream=io.BytesIO(data)
for n in expected:
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][n]==shared.sha(root/'selected-source'/n)==shared.sha(repo/n)
assert not stream.read()
sdk=json.loads((root/'actual-sdk.json').read_text());binary=json.loads((root/'binary.json').read_text());commands=json.loads((root/'commands.json').read_text())
assert sdk['args']==commands['run'] and sdk['args'][2]=='-test.run=^('+'|'.join(native.DAEMON_TESTS)+')$' and sdk['args'][-2:]==['-test.count=1','-test.timeout=3m']
assert sdk['working_directory']==commands['run_working_directory']==str(repo/'cmd/wf') and commands['build_working_directory']==str(repo)
assert sdk['exe_sha256']==binary['sha256']==shared.sha(root/'operator-race.test') and '-race=true' in binary['build_info'] and 'v2.15.0' in binary['build_info'];assert not Path('/proc',str(sdk['pid'])).exists()
assert sdk['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',GOWORK='off',GOFLAGS='',WF_OPERATOR_TEST_ROOT=str(root/'stores'))
plugins=json.loads((root/'plugins.json').read_text());assert plugins==[]
children=json.loads((root/'daemon-processes.json').read_text());assert len(children)==8 and len({c['pid'] for c in children})==8
assert all(c['exe_sha256']==binary['sha256'] and c['argv']==[str(root/'operator-race.test'),'-test.run=^TestOperatorDaemonProcessHelper$'] and not Path('/proc',str(c['pid'])).exists() for c in children)
assert len({(c['command'],c['stage'],c['domain']) for c in children})==8
logged=re.findall(r'operator daemon stopped command=(project|tombstone-loop) stage=(startup|running) domain="(WFOPS)?" signal=(terminated|interrupt) exit=0 pid=(\d+)',(root/'native.log').read_text())
assert {(c,s,d,int(pid)) for c,s,d,_,pid in logged}=={(c['command'],c['stage'],c['domain'],c['pid']) for c in children}

for child in children:
 assert child['command'] in ('project','tombstone-loop') and child['stage'] in ('startup','running') and child['domain'] in ('','WFOPS')
 assert child['operator_args'][-1]==child['command']
 if child['domain']:assert child['operator_args'][child['operator_args'].index('-domain')+1]==child['domain']
 else:assert '-domain' not in child['operator_args']

clusters=[p for p in (root/'stores').iterdir() if (p/'node-0').is_dir()];assert len(clusters)==2 and sorted(len(list(p.glob('node-*'))) for p in clusters)==[1,3]
qualification=native.verify_daemon_log((root/'native.log').read_text());assert json.loads((root/'row-review.json').read_text())==dict(qualification=qualification,rejection=None)
closure=shared.closure(root);meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(root)==manifest['files']
with raw.open('rb') as stream:assert fixture_archive.digest(stream)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
out=repo/'docs/scale/operator-daemon-signals-2026-10-06/native-race';out.mkdir()
for name in ['native.log','execution.json','source-before.json','source-after.json','actual-sdk.json','binary.json','commands.json','plugins.json','daemon-processes.json','row-review.json','closure.json']:shutil.copyfile(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-review.py')
report=dict(unit=unit,execution=e,source_files=len(expected),source_git_before_after_current_retained_equal=True,actual_sdk_binary_args_environment_cwd_verified=True,actual_sdk_closed=True,eight_actual_signal_child_executables_birth_args_and_closure_verified=True,retained_real_store_topology=[1,3],fresh_closure=closure,independent_log_review=qualification,complete_archive=meta,scope='Operator real signal startup/steady daemon lifecycle and unrelated fatal startup only. Controlled client-trace startup delay; no SQL daemon, server SIGKILL, leaf, fullmatrix, million drain or actual24h acceptance.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source=rev,files=len(expected),elapsed=e['elapsed_seconds'],members=meta['members'])))
