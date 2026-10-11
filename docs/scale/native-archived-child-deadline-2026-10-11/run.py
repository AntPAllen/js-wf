import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent;root=Path('/home/exedev/js-wf-native-archived-child-deadline-20261011');root.mkdir(exist_ok=False)
files=sorted(p for p in Path('.').rglob('*') if p.is_file() and (p.suffix=='.go' or str(p) in ('go.mod','go.sum')))
inputs={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files};(out/'source-before.json').write_text(json.dumps(inputs,indent=2)+'\n')
command=['go','test','-race','./worker','-run','^TestNativeGraphContinuationPromiseSelectionPriority$/^(R1|R3Domain)$/^archive=true$/^failed=false$','-count=1','-v','-timeout=6m']
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_GRAPH_SDK_DIAGNOSTIC_ROOT=str(root/'native'),WF_GRAPH_SDK_RAW_AUDIT='1')
start=time.time_ns();captured=None
with (out/'native.log').open('w') as log:
 p=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
 while p.poll() is None:
  if captured is None:
   for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():continue
    try:
     cmd=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode()
     if not cmd.startswith('/tmp/go-build') or '/worker.test ' not in cmd:continue
     parent=int((proc/'status').read_text().split('PPid:')[1].splitlines()[0].strip())
     if parent!=p.pid:continue
     data=(proc/'exe').read_bytes();(root/'worker-race.test').write_bytes(data);(root/'worker-race.test').chmod(0o700)
     captured=dict(pid=int(proc.name),parent_pid=parent,command=cmd,sha256=hashlib.sha256(data).hexdigest(),build=subprocess.check_output(['go','version','-m',str(proc/'exe')],text=True),captured_ns=time.time_ns(),retained_path=str(root/'worker-race.test'))
     (out/'executable.json').write_text(json.dumps(captured,indent=2)+'\n');break
    except (OSError,subprocess.CalledProcessError):pass
  time.sleep(.02)
receipt=dict(source_revision=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),command=command,root=str(root),actual_exit=p.returncode,driver_pid=p.pid,start_ns=start,end_ns=time.time_ns(),executable_captured=captured is not None,environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','WF_GRAPH_SDK_DIAGNOSTIC_ROOT','WF_GRAPH_SDK_RAW_AUDIT')})
(out/'command.json').write_text(json.dumps(receipt,indent=2)+'\n')
(out/'source-after.json').write_text(json.dumps({name:hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in inputs},indent=2)+'\n')
census={}
for path in sorted((root/'native').rglob('*')):
 if path.is_file():census[str(path.relative_to(root/'native'))]=dict(bytes=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest())
(out/'native-files.json').write_text(json.dumps(census,indent=2)+'\n');print(json.dumps(receipt));raise SystemExit(p.returncode)
