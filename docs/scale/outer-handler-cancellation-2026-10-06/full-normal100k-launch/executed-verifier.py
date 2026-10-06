from pathlib import Path
import subprocess,json,hashlib,datetime,os,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier1-handler-boundary-normal100k-20261006');run=root/'run';source=root/'source'
state=json.loads((root/'execution.json').read_text());assert state['status']=='running' and state['seeds']==100000 and state['race'] is False and state['configured_original_timeout']=='300m'
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-tier1-handler-boundary-normal100k-20261006.service','-p','ActiveState','-p','MainPID'],text=True).splitlines());assert unit['ActiveState']=='active' and int(unit['MainPID'])>0
assert Path('/proc',str(state['producer_pid'])).exists()
binary=json.loads((run/'binary.json').read_text());assert binary['race_instrumented'] is False and '-race=true' not in binary['build_info']
assert binary['environment']==dict(GOMEMLIMIT='512MiB',GOMAXPROCS='2',SIM_SEEDS='100000',SIM_COVERAGE_SUMMARY='1')
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
assert sha(run/'sim.test')==binary['binary_sha256']
actual=[]
for proc in Path('/proc').glob('[0-9]*'):
 try:
  if (proc/'exe').resolve()==run/'sim.test':actual.append(proc)
 except (PermissionError,FileNotFoundError,ProcessLookupError):pass
assert len(actual)==1
proc=actual[0];args=[a.decode() for a in (proc/'cmdline').read_bytes().split(b'\0') if a]
assert args==[str(run/'sim.test'),'-test.v=test2json','-test.count=1','-test.timeout=300m']
assert sha(proc/'exe')==binary['binary_sha256']
env=dict(e.decode().split('=',1) for e in (proc/'environ').read_bytes().split(b'\0') if b'=' in e);profile={k:env[k] for k in binary['environment']};assert profile==binary['environment']
before=json.loads((run/'source-before.json').read_text());assert before['revision']==state['source']
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==state['source'] and not subprocess.check_output(['git','status','--porcelain'],cwd=source)
for name,h in before['files'].items():
 assert sha(source/name)==h
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',state['source']+':'+name],cwd=repo)).hexdigest()==h
assert len((run/'tier1-seeded-inventory.txt').read_text().splitlines())==122 and len((run/'tier1-regression-inventory.txt').read_text().splitlines())==392
assert not subprocess.check_output(['git','diff','--name-only','3e35e7c',state['source'],'--','*.go','go.mod','go.sum','sim/testdata'],cwd=repo)
assert (run/'tier1-events.jsonl').stat().st_size>0
out=repo/'docs/scale/outer-handler-cancellation-2026-10-06/full-normal100k-launch';out.mkdir()
shutil.copyfile(__file__,out/'executed-verifier.py');shutil.copyfile(root/'executed-launch.py',out/'executed-launch.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),execution=state,live_unit=unit,actual_sdk_pid=int(proc.name),actual_sdk_stat=(proc/'stat').read_text(),actual_sdk_args=args,binary=binary,actual_profile=profile,source_files_verified=len(before['files']),source_exactly_bound_to_git=True,runtime_dependency_and_pin_inputs_identical_to_accepted_race_3e35e7c=True,seeded_workloads=122,pinned_inventory=392,scope='Verified live original full100000-seed normal qualifier under existing300m budget at executed4f93039. Native terminal, complete-suite checker and unchanged before/after source remain mandatory; no normal/full real-matrix/24h verdict.')
(out/'launch.json').write_text(json.dumps(report,indent=2)+'\n');print('LIVE_NORMAL100K_SDK_SOURCE_PROFILE_VERIFIED',proc.name,len(before['files']),flush=True)
