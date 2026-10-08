import sys
sys.dont_write_bytecode=True
from pathlib import Path
import subprocess,json,shutil,time,hashlib,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier1-full126-race-20261008');source=root/'source';run=root/'run'
assert not root.exists() and not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert revision==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert shutil.disk_usage(root.parent).free>=128*1024**2,'Insufficient checkout/binary/log headroom'
root.mkdir();(root/'executed-launch.py').write_bytes(Path(__file__).read_bytes())
state=dict(source=revision,status='preparing',started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),seeds=1000,race=True,configured_original_timeout='60m')
def save():(root/'execution.json').write_text(json.dumps(state,indent=2)+'\n')
save()
try:
 subprocess.run(['git','worktree','add','--detach','--no-checkout',str(source),revision],cwd=repo,check=True)
 names=subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines()
 included=[n for n in names if not n.startswith('docs/') or n.endswith(('.go','.py','.yml'))]
 (root/'checkout-files.json').write_text(json.dumps(included,indent=2)+'\n')
 subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],cwd=source,input=''.join('/'+n+'\n' for n in included),text=True,check=True)
 subprocess.run(['git','read-tree','-mu','HEAD'],cwd=source,check=True)
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 command=['python3',str(source/'scripts/check-tier1-race.py'),'--root',str(run),'--seeds','1000']
 state.update(status='running',command=command);save()
 with (root/'producer.log').open('w') as log:
  child=subprocess.Popen(command,cwd=source,stdout=log,stderr=subprocess.STDOUT)
  state['producer_pid']=child.pid;save()
  observed=False
  while child.poll() is None:
   proof=run/'binary.json'
   if not observed and proof.exists():
    expected=json.loads(proof.read_text())
    for proc in Path('/proc').glob('[0-9]*'):
     try:
      if (proc/'exe').resolve()!=run/'sim.test':continue
      env=dict(row.split(b'=',1) for row in (proc/'environ').read_bytes().split(b'\0') if b'=' in row)
      profile={k:env[k.encode()].decode() for k in expected['environment']}
      args=[row.decode() for row in (proc/'cmdline').read_bytes().split(b'\0') if row]
      birth=lambda:(proc/'stat').read_text().rsplit(') ',1)[1].split()[19]
      raw_args=(proc/'cmdline').read_bytes();raw_env=(proc/'environ').read_bytes()
      first=birth();digest=hashlib.sha256((proc/'exe').read_bytes()).hexdigest()
      assert birth()==first and digest==expected['binary_sha256']
      assert (proc/'cmdline').read_bytes()==raw_args and (proc/'environ').read_bytes()==raw_env
      assert profile==expected['environment'] and args==[str(run/'sim.test'),'-test.v=test2json','-test.count=1','-test.timeout=60m']
      assert (proc/'cwd').resolve()==source/'sim'
      state['actual_sdk']=dict(pid=int(proc.name),start_ticks=first,args=args,exe_sha256=digest,environment=profile,working_directory=str((proc/'cwd').resolve()),observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
      state['binary']=expected;save();observed=True;break
     except (FileNotFoundError,ProcessLookupError,PermissionError,KeyError):pass
   time.sleep(.25)
  rc=child.wait()
  assert observed,'native terminal without actual SDK admission' 
 state.update(status='passed' if rc==0 else 'failed',exit_code=rc,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==revision
 print('TIER1_TERMINAL',state['status'],rc,flush=True)
except Exception as e:
 state.update(status='failed',error=str(e));save();raise
