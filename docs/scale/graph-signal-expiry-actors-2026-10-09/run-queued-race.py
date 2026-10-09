from pathlib import Path
import subprocess,json,hashlib,os,time,datetime
base=Path(__file__).resolve().parent
repo=Path('/home/exedev/js-wf')
normal_root=Path('/home/exedev/js-wf-signal-expiry-actors-20261009')
root=Path('/home/exedev/js-wf-signal-expiry-race1000-20261009')
binary=normal_root/'cartesian-race.test'
source='e971d01'
assert not root.exists()
build=json.loads((normal_root/'cartesian-race-binary.json').read_text());assert '-race=true' in build['build_info']
assert hashlib.sha256(binary.read_bytes()).hexdigest()==build['sha256']
observed=json.loads((normal_root/'observed-source.json').read_text())
for name,digest in observed['files'].items():assert hashlib.sha256(subprocess.check_output(['git','show',f'{source}:{name}'],cwd=repo)).hexdigest()==digest,name
def identity(pid):
 p=Path('/proc')/str(pid)
 try:
  fields=(p/'stat').read_text().split(') ',1)[1].split()
  if fields[0]=='Z':return None
  return {'pid':pid,'start_ticks':fields[19],'arguments':[a.decode() for a in (p/'cmdline').read_bytes().split(b'\0') if a]}
 except FileNotFoundError:return None
wanted={'/home/exedev/js-wf-tier1-full151-race1000-after-reboot-20261009/sim.test','/home/exedev/js-wf-signal-actors-qualified-20261009/race.test'}
live=[]
for p in Path('/proc').iterdir():
 if p.name.isdigit():
  try:i=identity(int(p.name))
  except (PermissionError,ProcessLookupError):continue
  if i and i['arguments'] and i['arguments'][0] in wanted:live.append(i)
assert len(live)==2,live
record={'source_go_and_original_trace_byte_matched_to':source,'binary_sha256':build['sha256'],'queued_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'wait_until_one_observed_process_is_terminal':live,'observed_cpu_count':int(subprocess.check_output(['nproc'],text=True)),'watchdog_minutes':300,'root':str(root),'scope':'focused expiry actor family x1000; no current whole-suite/all-pin acceptance'}
(base/'queued-race-launch.json').write_text(json.dumps(record,indent=2)+'\n')
while all(identity(i['pid'])==i for i in live):time.sleep(5)
normal=(normal_root/'normal1000.log').read_text();exit_record=json.loads((normal_root/'normal-exit.json').read_text())
assert exit_record['exit_code']==0
assert 'TIER1_SEEDS test=TestSeededGraphSignalExpiryActorsReplay first=1 last=1000 completed=1000 requested=1000' in normal
assert hashlib.sha256(binary.read_bytes()).hexdigest()==build['sha256']
root.mkdir()
command=[str(binary),'-test.run=^TestSeededGraphSignalExpiryActorsReplay$','-test.count=1','-test.timeout=300m','-test.v']
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_SEEDS='1000',SIM_PROGRESS='1',SIM_COVERAGE_SUMMARY='1',FAULT_TRACE_OUT=str(root/'failure.json'))
record.update(command=command,working_directory=str(repo/'sim'),started_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
(base/'queued-race-launch.json').write_text(json.dumps(record,indent=2)+'\n')
with (root/'race.log').open('w') as out:r=subprocess.run(command,cwd=repo/'sim',env=env,stdout=out,stderr=subprocess.STDOUT)
record['actual_exit_code']=r.returncode;record['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(base/'queued-race-launch.json').write_text(json.dumps(record,indent=2)+'\n')
print('queued focused expiry race actual exit',r.returncode)
raise SystemExit(r.returncode)
