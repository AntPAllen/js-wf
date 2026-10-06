from pathlib import Path
import json,subprocess,hashlib,datetime,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-bulk-journal-24h-joined-20261006');out=repo/'docs/scale/bulk-journal-24h-2026-10-06/launch'
launch=json.loads((out/'launch.json').read_text());sdk=launch['actual_sdk'];pid=sdk['pid'];proc=Path('/proc')/str(pid)
assert proc.exists() and (proc/'stat').read_text().rsplit(')',1)[1].split()[19]==sdk['stat'].rsplit(')',1)[1].split()[19]
args=(proc/'cmdline').read_text().split('\0')[:-1]
assert args==sdk['args'] and '-test.timeout=24h20m' in args and '-test.count=1' in args and '-test.run=^TestFiveContainerMixedJournalLeaderEveryThirtySeconds$' in args
with (proc/'exe').open('rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==sdk['sha256']
assert '-race=true' not in sdk['build_info']
env=dict(e.decode().split('=',1) for e in (proc/'environ').read_bytes().split(b'\0') if b'=' in e)
expected=dict(WF_TIER3_MATRIX_DURATION='24h',FAULT_SEED='1',WF_TIER3_MATRIX='1',TIER3_MATRIX_ROW='journal',WF_TIER3_EXPLICIT_ROUTE_SEEDS='1',WF_TIER3_SYNC_INTERVAL='2m')
assert {k:env[k] for k in expected}==expected
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-bulk-journal-24h-joined-20261006.service','-p','ActiveState','-p','MainPID','-p','MemoryMax','-p','CPUQuotaPerSecUSec'],text=True).splitlines())
assert unit['ActiveState']=='active' and int(unit['MainPID'])==launch['execution']['supervisor_pid'] and int(unit['MemoryMax'])==6*1024**3 and unit['CPUQuotaPerSecUSec']=='4s'
state=json.loads((root/'execution.json').read_text());assert state['status']=='running' and state['source']==launch['execution']['source']
assert state['disk_admission']['minimum_free_bytes']==17*1024**3 and state['disk_admission']['additional_reserve_bytes']==1024**3
assert (proc/'stat').read_text().rsplit(')',1)[1].split()[19]==sdk['stat'].rsplit(')',1)[1].split()[19]
shutil.copyfile(__file__,out/'executed-profile-supplement.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),actual_sdk_pid=pid,actual_original_argv=args,actual_native_environment=expected,live_unit=unit,execution=state,scope='Same live actual SDK additionally binds exact24h native duration/seed/journal row/production2m sync/explicit routes, original24h20m timeout/count1/nonrace,6GiB cgroup/4CPU quota and strict17GiB launch admission. No source/runtime/test restart, original fault/audit/p99/history/drain/final/terminal24h acceptance still pending.')
(out/'profile-supplement.json').write_text(json.dumps(report,indent=2)+'\n');print('LIVE_ACTUAL_24H_ARGS_ENV_CGROUP_ADMISSION_VERIFIED',pid,flush=True)
