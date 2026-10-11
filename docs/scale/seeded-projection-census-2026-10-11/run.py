import hashlib,json,subprocess,time,os,sys
from pathlib import Path
out=Path(__file__).resolve().parent
mode=sys.argv[1]
command=['go','test']+(['-race'] if mode=='race' else [])+['./sim','-run','^(TestSeededGraphProjectionCensusReplay|TestGraphStartHeaderFailureTracePersistence|TestPinnedRegressionCorpus)$/^(graph_projection_census_|default|explicit)','-count=1','-v','-timeout=6m']
start=time.time_ns();captured=False
with (out/(mode+'.log')).open('w') as log:
 p=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1',FAULT_TRACE_OUT='/home/exedev/js-wf-projection-census-model-20261011/'+mode+'-failure.json'))
 while p.poll() is None:
  if not captured:
   for d in Path('/proc').iterdir():
    if not d.name.isdigit():continue
    try:
     cmd=(d/'cmdline').read_bytes().replace(b'\0',b' ').decode()
     if cmd.startswith('/tmp/go-build') and '/sim.test ' in cmd and 'TestSeededGraphProjectionCensusReplay' in cmd:
      exe=d/'exe'
      r={'pid':int(d.name),'sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'command':cmd,'build':subprocess.check_output(['go','version','-m',str(exe)],text=True),'captured_ns':time.time_ns()}
      (out/(mode+'-executable.json')).write_text(json.dumps(r,indent=2)+'\n');captured=True;break
    except (OSError,subprocess.CalledProcessError):pass
  time.sleep(.1)
r={'command':command,'mode':mode,'driver_pid':p.pid,'actual_exit_code':p.returncode,'start_ns':start,'end_ns':time.time_ns(),'executable_captured':captured}
(out/(mode+'-command.json')).write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r));raise SystemExit(p.returncode)
