from pathlib import Path
import json,hashlib,subprocess,os,datetime
root=Path(__file__).resolve().parent
repo=Path('/home/exedev/js-wf')
source='e971d01'
binary=root/'cartesian-race.test'
observed=json.loads((root/'observed-source.json').read_text())
for name,digest in observed['files'].items():
 assert hashlib.sha256((repo/name).read_bytes()).hexdigest()==digest,name
 assert hashlib.sha256(subprocess.check_output(['git','show',f'{source}:{name}'],cwd=repo)).hexdigest()==digest,name
build=json.loads((root/'cartesian-race-binary.json').read_text())
assert '-race=true' in build['build_info']
assert hashlib.sha256(binary.read_bytes()).hexdigest()==build['sha256']
normal=(root/'normal1000.log').read_text()
assert 'TIER1_SEEDS test=TestSeededGraphSignalExpiryActorsReplay first=1 last=1000 completed=1000 requested=1000' in normal
assert '--- PASS: TestSeededGraphSignalExpiryActorsReplay' in normal
command=[str(binary),'-test.run=^TestSeededGraphSignalExpiryActorsReplay$','-test.count=1','-test.timeout=300m','-test.v']
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1',SIM_PROGRESS='1',FAULT_TRACE_OUT=str(root/'race1000-failure.json'))
record={'source_go_module_original_trace_inputs_byte_matched_to':source,'input_count':len(observed['files']),'binary_sha256':build['sha256'],'command':command,'working_directory':str(repo/'sim'),'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'watchdog_minutes':300,'scope':'focused expiry actor family x1000; not whole current suite/all pins'}
(root/'race1000-launch.json').write_text(json.dumps(record,indent=2)+'\n')
with (root/'race1000.log').open('w') as out:r=subprocess.run(command,cwd=repo/'sim',env=env,stdout=out,stderr=subprocess.STDOUT)
record['actual_exit_code']=r.returncode;record['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(root/'race1000-launch.json').write_text(json.dumps(record,indent=2)+'\n')
print('actual focused expiry race exit',r.returncode)
raise SystemExit(r.returncode)
