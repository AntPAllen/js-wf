from pathlib import Path
import os,json,hashlib,subprocess,time,datetime
r=Path('/tmp/js-wf-raft-safety170-contiguous-admitted-20261007');out=Path('/tmp/full-contiguous-actual-captures-20261007');out.mkdir()
sha=lambda f:hashlib.file_digest(Path(f).open('rb'),'sha256').hexdigest()
for profile in ['upstream','contiguous']:
 target=r/(profile+'-live-execution.json')
 while not target.exists():
  if not Path('/proc/2186898').exists():raise RuntimeError('producer stopped before '+profile+' native observation')
  time.sleep(.1)
 initial=json.loads(target.read_text());pid=initial['actual']['pid'];p=Path('/proc')/str(pid);expected=r/(profile+'.test');deadline=time.monotonic()+5
 while True:
  stat=(p/'stat').read_text();args=[os.fsdecode(v) for v in (p/'cmdline').read_bytes().split(b'\0') if v];digest=sha(p/'exe')
  if args==initial['command'] and digest==sha(expected):break
  if time.monotonic()>deadline:raise RuntimeError('actual process did not admit expected argv/executable')
  time.sleep(.01)
 assert stat.split(') ',1)[1].split()[19]==initial['actual']['stat'].split(') ',1)[1].split()[19]
 environment={os.fsdecode(v.split(b'=',1)[0]):os.fsdecode(v.split(b'=',1)[1]) for v in (p/'environ').read_bytes().split(b'\0') if b'=' in v}
 actual=dict(pid=pid,stat=stat,argv=args,actual_executable_sha256=digest,cwd=os.readlink(p/'cwd'),environment={k:environment[k] for k in initial['environment']},build_info=subprocess.check_output(['go','version','-m',str(p/'exe')],text=True))
 assert actual['environment']==initial['environment'] and actual['cwd']==initial['actual']['cwd'] and actual['actual_executable_sha256']==initial['actual']['actual_executable_sha256']
 (out/(profile+'.json')).write_text(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),actual=actual,original_live_record=initial,scope='Independent live SDK birth/argv/byte/profile observation. Original immediate sample retained even if pre-exec stat/argv. Point-in-time identity only, not exhaustive lifetime coverage or native qualification.'),indent=2)+'\n')
 print('ACTUAL_CAPTURED',profile,pid,flush=True)
