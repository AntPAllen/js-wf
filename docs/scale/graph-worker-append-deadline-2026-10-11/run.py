import hashlib,json,os,subprocess,time,sys
from pathlib import Path
out=Path(__file__).resolve().parent;mode=sys.argv[1];assert mode in ('normal','race')
pattern='^(TestSeededGraphWorkerAppendDeadlineReplay|TestGraphStartHeaderFailureTracePersistence|TestPinnedRegressionCorpus)$/^(graph_worker_append_deadline_|default|explicit)'
command=['go','test']+(['-race'] if mode=='race' else [])+['./sim','-run',pattern,'-count=1','-v','-timeout=6m']
env=dict(os.environ,SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1',GOMAXPROCS='2',GOMEMLIMIT='512MiB',FAULT_TRACE_OUT=str(out/(mode+'-failure.json')))
start=time.time_ns();captured=None
with (out/(mode+'.log')).open('w') as log:
 p=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
 while p.poll() is None:
  if captured is None:
   for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():continue
    try:
     cmd=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode()
     if not cmd.startswith('/tmp/go-build') or '/sim.test ' not in cmd:continue
     parent=int((proc/'status').read_text().split('PPid:')[1].splitlines()[0].strip())
     if parent!=p.pid:continue
     captured=dict(pid=int(proc.name),parent_pid=parent,command=cmd,sha256=hashlib.sha256((proc/'exe').read_bytes()).hexdigest(),build=subprocess.check_output(['go','version','-m',str(proc/'exe')],text=True),captured_ns=time.time_ns())
     (out/(mode+'-executable.json')).write_text(json.dumps(captured,indent=2)+'\n')
     break
    except (OSError,subprocess.CalledProcessError):pass
  time.sleep(.01)
receipt=dict(command=command,actual_exit=p.returncode,driver_pid=p.pid,start_ns=start,end_ns=time.time_ns(),executable_captured=captured is not None,environment={k:env[k] for k in ('SIM_SEEDS','SIM_COVERAGE_SUMMARY','GOMAXPROCS','GOMEMLIMIT','FAULT_TRACE_OUT')})
(out/(mode+'-command.json')).write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt));raise SystemExit(p.returncode)
