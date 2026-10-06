from pathlib import Path
import json,subprocess,hashlib,io,os,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-postgres-domain-projection-corrected-50000-20261006')
sha=lambda p:hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());a=json.loads((root/'actual-sdk.json').read_text());b=json.loads((root/'binary.json').read_text());commands=json.loads((root/'commands.json').read_text());before=json.loads((root/'source-before.json').read_text())
assert e['status']=='running' and before['revision']==e['source']
proc=Path('/proc',str(a['pid']));assert proc.exists()
assert (proc/'stat').read_text().split()[21]==a['stat'].split()[21]
assert str((proc/'exe').resolve())==a['exe']==str(root/'integration.test') and sha(proc/'exe')==a['exe_sha256']==b['sha256']
argv=[os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v];assert argv==a['args']==commands['test']
assert argv[1:]==['-test.run=^TestPostgresProjectionCrashAndSessionLossFiftyThousandInvocationsInJetStreamDomain$','-test.count=1','-test.v','-test.timeout=22m']
env=dict(v.split(b'=',1) for v in (proc/'environ').read_bytes().split(b'\0') if b'=' in v)
assert b'WF_PROJECTION_COUNT' not in env and not a['actual_WF_PROJECTION_COUNT_present']
for name,value in a['environment'].items():assert os.fsdecode(env[name.encode()])==value
assert a['environment']['GOMAXPROCS']=='2' and a['environment']['GOMEMLIMIT']=='2GiB' and a['environment']['GOWORK']=='off' and a['environment']['GOFLAGS']==''
assert '-race=true' not in b['build_info'] and 'vcs.revision='+e['source'] in b['build_info'] and 'vcs.modified=false' in b['build_info']
assert str((proc/'cwd').resolve())==a['working_directory']==str(repo)
inputs=before['files'];data=subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(e['source']+':'+n+'\n' for n in inputs).encode());stream=io.BytesIO(data)
for name,digest in inputs.items():
 header=stream.readline().split();assert header[1]==b'blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(body).hexdigest()==digest==sha(repo/name)==sha(root/'selected-source'/name)
assert not stream.read()
pg=json.loads((root/'actual-postgres.json').read_text());current=json.loads(subprocess.check_output(['docker','inspect',pg['container']['Id']]))[0]
assert current['State']['Running'] and current['State']['Pid']==pg['actual_host_pid'] and sha(root/'actual-postgres')==pg['actual_executable_sha256']
log=(root/'native.log').read_text();assert 'real domain admitted node=0 domain=WFVIEW' in log and 'real domain admitted node=1 domain=WFVIEW' in log and 'real domain admitted node=2 domain=WFVIEW' in log
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=e['source'],actual_sdk_pid=a['pid'],source_files=len(inputs),exact_live_sdk_birth_exe_args_environment_cwd_and_source_verified=True,actual_postgres_pid=current['State']['Pid'],actual_postgres_version=pg['version'],all_three_native_domain_admissions_observed=True,scope='Original full50000 domain projection launch only. Original20m fixture/22m SDK live; no terminal recovery/rebuild/domain gate qualified. Keep same handle and checkout unchanged.')
Path('/tmp/js-wf-pg-domain-live-launch-20261006.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
