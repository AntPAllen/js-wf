from pathlib import Path
import subprocess,json,shutil,time,hashlib,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier1-handler-boundary-normal100k-20261006');source=root/'source';run=root/'run'
race_state=json.loads(Path('/tmp/js-wf-tier1-handler-boundary-race-20261006/execution.json').read_text());assert race_state['status']=='passed' and race_state['exit_code']==0
assert Path('/home/exedev/js-wf/docs/scale/outer-handler-cancellation-2026-10-06/full-race-terminal/independent-review.json').is_file()
assert not root.exists() and not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert revision==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert shutil.disk_usage(root.parent).free>=128*1024**2,'Insufficient checkout/binary/log headroom'
root.mkdir();(root/'executed-launch.py').write_bytes(Path(__file__).read_bytes())
state=dict(source=revision,status='preparing',started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),seeds=100000,race=False,configured_original_timeout='300m')
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
 command=['python3',str(source/'scripts/check-tier1-race.py'),'--root',str(run),'--seeds','100000','--no-race']
 state.update(status='running',command=command);save()
 with (root/'producer.log').open('w') as log:
  child=subprocess.Popen(command,cwd=source,stdout=log,stderr=subprocess.STDOUT)
  state['producer_pid']=child.pid;save()
  rc=child.wait()
 state.update(status='passed' if rc==0 else 'failed',exit_code=rc,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==revision
 print('TIER1_TERMINAL',state['status'],rc,flush=True)
except Exception as e:
 state.update(status='failed',error=str(e));save();raise
