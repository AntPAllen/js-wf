from pathlib import Path
import os,subprocess,json,datetime,hashlib
root=Path(__file__).resolve().parent
binary=root/'race.test'
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_PROGRESS='1',SIM_COVERAGE_SUMMARY='1',SIM_SEEDS='1000',FAULT_TRACE_OUT=str(root/'race-failure.json'))
command=[str(binary),'-test.run=^TestSeededGraphSignalOperationActorsReplay$','-test.count=1','-test.timeout=90m','-test.v']
record={'command':command,'working_directory':'/home/exedev/js-wf/sim','binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'environment':{k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','SIM_PROGRESS','SIM_COVERAGE_SUMMARY','SIM_SEEDS']}}
(root/'race-launch.json').write_text(json.dumps(record,indent=2)+'\n')
with (root/'race.log').open('w') as out:r=subprocess.run(command,cwd=record['working_directory'],env=env,stdout=out,stderr=subprocess.STDOUT)
record['exit_code']=r.returncode;record['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(root/'race-launch.json').write_text(json.dumps(record,indent=2)+'\n')
print('actual focused race exit',r.returncode)
raise SystemExit(r.returncode)
