import hashlib,json,os,shutil,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-native-active-population-inspection-20261011')
source=root/'source';probe=source/'probe';probe.mkdir(exist_ok=True)
shutil.copyfile(out/'probe.go.txt',probe/'main.go')
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
inputs=json.loads((out/'source-inputs.json').read_text())
inputs['helper']={'path':'probe/main.go','sha256':digest(probe/'main.go')}
(out/'source-inputs.json').write_text(json.dumps(inputs,indent=2)+'\n')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
steps=[]
for label,cmd in [('build',['go','build','-o',str(root/'probe'),'./probe']),('probe',[str(root/'probe'),str(root),str(out/'observations.json')])]:
 start=time.time_ns()
 with (out/(label+'.log')).open('w') as f:
  p=subprocess.Popen(cmd,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT);code=p.wait()
 end=time.time_ns()
 receipt=dict(label=label,argv=cmd,pid=p.pid,start_ns=start,end_ns=end,actual_exit=code)
 if label=='probe':receipt.update(binary_sha256=digest(root/'probe'),binary_bytes=(root/'probe').stat().st_size)
 steps.append(receipt);(out/'commands.json').write_text(json.dumps(steps,indent=2)+'\n')
 if code!=0:break
print(json.dumps(steps))
