from pathlib import Path
import sys,json,hashlib,subprocess,importlib.util,io,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-domain-runtime-controls.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
root=Path('/tmp/js-wf-domain-legacy-weak-expiry-20261006');proof=root.with_name(root.name+'-proof');raw=root.with_suffix('.tar.gz')
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-domain-legacy-weak-expiry-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
assert unit==dict(ActiveState='failed',MainPID='0',ExecMainStatus='1'),unit
execution=json.loads((root/'execution.json').read_text());revision=execution['source'];assert revision==subprocess.check_output(['git','rev-parse','01dee30'],cwd=repo,text=True).strip()
assert execution['exit_code']==1 and execution['case']=='legacy-retirement-weak-frame-expiry'
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==revision
names=subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
body=subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(revision+':'+name+'\n' for name in expected).encode());stream=io.BytesIO(body)
for name in expected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n';sha=hashlib.sha256(data).hexdigest();assert sha==before['files'][name]==native.sha(root/'selected-source'/name)==native.sha(repo/name)
assert not stream.read()
sdk=json.loads((root/'actual-sdk.json').read_text());binary=json.loads((root/'binary.json').read_text());command=json.loads((root/'commands.json').read_text())
assert sdk['args']==command['run'] and sdk['args'][-2:]==['-test.count=1','-test.timeout=3m'];assert sdk['args'][2]=='-test.run=^('+native.LEGACY_WEAK_EXPIRY+')$'
assert sdk['exe_sha256']==binary['sha256']==native.sha(root/'integration-race.test') and '-race=true' in binary['build_info']
assert sdk['environment']['GOMAXPROCS']=='2' and sdk['environment']['GOMEMLIMIT']=='1GiB'
assert not Path('/proc',str(sdk['pid'])).exists()
legacy=json.loads((root/'legacy-binary.json').read_text());assert legacy['sha256']==native.sha(root/'inputs'/'nats-server') and 'v2.11.17' in legacy['build_info'];assert sdk['environment']['WF_NATS_SERVER_BIN']==str(root/'inputs'/'nats-server')
servers=json.loads((root/'actual-servers.json').read_text());assert len(servers)==6
for server in servers:
 assert not Path('/proc',str(server['pid'])).exists();assert server['exe_sha256']==native.sha(server['exe'])==legacy['sha256'] and 'v2.11.17' in server['build_info'];assert all(str(root/'stores') in arg for arg in server['args'] if arg.endswith('domain.conf'))
log=(root/'native.log').read_text();assert '--- FAIL: '+native.LEGACY_WEAK_EXPIRY in log and 'completed server fault batches=0' in log;qualification={'qualified':False,'rejection':'native test failed'};assert json.loads((root/'row-review.json').read_text())['rejection']=='native test failed'
closed=native.closure(root)
meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.verify(raw)==manifest;assert fixture_archive.inventory(root)==manifest['files']
with raw.open('rb') as f:assert fixture_archive.digest(f)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
out=repo/'docs/scale/domain-retirement-legacy-weak-frame-expiry-2026-10-06/initial-failure';out.mkdir()
for name in ['native.log','execution.json','actual-sdk.json','actual-servers.json','commands.json','binary.json','source-before.json','source-after.json','row-review.json','closure.json','legacy-binary.json']:shutil.copyfile(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-review.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),unit=unit,execution=execution,source_files_exactly_bound_to_git=len(expected),source_git_before_after_current_retained_equal=True,actual_sdk_pid=sdk['pid'],actual_sdk_binary_profile_args_verified=True,actual_native_server_incarnations=6,all_actual_sdk_server_pids_closed=True,independent_log_check=qualification,closure=closed,complete_archive=meta,scope='Preserved FAILED focused actual domain retirement/reuse, manifest reply loss, all-server SIGKILL held beyond production12s TTL, higher terminal epoch and controlled weak fresh-frame absence. Native leader oracle/payload; original budgets/strict integrity/GC retained. Actual2.11.17 peer admission/restart and retained fallback backend verified. No natural follower-lag, active-writer GC, other legacy combinations, fullmatrix or actual24h acceptance.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source=revision,files=len(expected),sdk_elapsed=execution['elapsed_seconds'],members=meta['members'],native_incarnations=6)))
