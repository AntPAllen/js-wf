import hashlib,json,subprocess,time
from pathlib import Path
base=Path(__file__).resolve().parent
checkout=json.loads((base/'checkout.json').read_text());source=checkout['source']
unit='js-wf-main163-tier1-normal-20261011-prepared.service'
props=subprocess.check_output(['systemctl','--user','show',unit,'-p','LoadState'],text=True)
assert 'LoadState=not-found\n' in props
assert not Path('/home/exedev/js-wf-main163-tier1-20261011').exists()
command=['systemd-run','--user','--unit',unit,'--property=CPUQuota=50%','--property=MemoryMax=2G','--property=RuntimeMaxSec=145200','--property=RemainAfterExit=yes','--property=WorkingDirectory=/home/exedev/js-wf','--property=Environment=PYTHONUNBUFFERED=1','/usr/bin/python3',str(base/'qualification-run.py'),source,'normal']
start=time.time_ns();result=subprocess.run(command,capture_output=True,text=True);assert result.returncode==0,result.stderr
for _ in range(100):
 state_path=base/'normal-state.json'
 if state_path.exists():
  state=json.loads(state_path.read_text())
  if state.get('child_pid'):break
 time.sleep(.1)
else:raise RuntimeError('launch state did not bind child process')
assert state['phase']=='running' and state['source']==source
for pid in (state['pid'],state['child_pid']):assert Path('/proc/'+str(pid)).exists()
props=subprocess.check_output(['systemctl','--user','show',unit,'-p','LoadState','-p','MainPID','-p','ExecMainPID','-p','ExecStart','-p','ExecMainStatus','-p','InvocationID','-p','CPUQuotaPerSecUSec','-p','MemoryMax','-p','RuntimeMaxUSec','-p','RemainAfterExit','-p','ActiveState','-p','SubState'],text=True)
service=dict(line.split('=',1) for line in props.splitlines())
assert service['LoadState']=='loaded' and service['ActiveState']=='active' and service['SubState']=='running'
assert int(service['MainPID'])==state['pid'] and service['InvocationID']==state['invocation']
assert service['CPUQuotaPerSecUSec']=='500ms' and service['MemoryMax']=='2147483648' and service['RuntimeMaxUSec']=='1d 16h 20min' and service['RemainAfterExit']=='yes'
receipt=dict(source=source,invocation=state['invocation'],supervisor_pid=state['pid'],child_pid=state['child_pid'],command=state['command'],service=service,supervisor_sha256=hashlib.sha256((base/'qualification-run.py').read_bytes()).hexdigest(),launch_command=command,actual_launch_exit=0,start_ns=start,end_ns=time.time_ns(),accepted=False)
(base/'normal-launch.json').write_text(json.dumps(receipt,indent=2)+'\n');(base/'normal-loaded-unit.txt').write_text(props)
print(json.dumps(receipt))
