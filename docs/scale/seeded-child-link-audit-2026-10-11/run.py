import hashlib,json,os,subprocess,sys,time
from pathlib import Path
out=Path(__file__).resolve().parent
mode=sys.argv[1];assert mode in ('normal','race')
command=['go','test']+(['-race'] if mode=='race' else [])+['./sim','-run','^(TestSeededGraphChildLinkAuditReplay|TestGraphStartHeaderFailureTracePersistence|TestPinnedRegressionCorpus)$/^(graph_child_link_audit_|default|explicit)','-count=1','-v','-timeout=6m']
env=dict(os.environ,SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1',GOMAXPROCS='2',GOMEMLIMIT='512MiB',FAULT_TRACE_OUT='/home/exedev/js-wf-child-link-model-20261011/'+mode+'-failure.json')
start=time.time_ns();captured=False
with (out/(mode+'.log')).open('w') as log:
 p=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env)
 while p.poll() is None:
  if not captured:
   for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():continue
    try:
     cmd=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode()
     if cmd.startswith('/tmp/go-build') and '/sim.test ' in cmd and 'TestSeededGraphChildLinkAuditReplay' in cmd:
      exe=proc/'exe';receipt={'pid':int(proc.name),'sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'command':cmd,'build':subprocess.check_output(['go','version','-m',str(exe)],text=True),'captured_ns':time.time_ns()}
      (out/(mode+'-executable.json')).write_text(json.dumps(receipt,indent=2)+'\n');captured=True;break
    except (OSError,subprocess.CalledProcessError):pass
  time.sleep(.1)
receipt={'command':command,'driver_pid':p.pid,'actual_exit_code':p.returncode,'start_ns':start,'end_ns':time.time_ns(),'executable_captured':captured,'environment':{k:env[k] for k in ('SIM_SEEDS','SIM_COVERAGE_SUMMARY','GOMAXPROCS','GOMEMLIMIT','FAULT_TRACE_OUT')}}
(out/(mode+'-command.json')).write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt));raise SystemExit(p.returncode)
