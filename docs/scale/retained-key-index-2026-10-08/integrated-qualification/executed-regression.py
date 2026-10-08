import sys,os,json,hashlib,subprocess,time,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/retained-key-index-2026-10-08/integrated-qualification';base.mkdir(parents=True)
assert not (base/'source-before.json').exists()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
names=[n for n in subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines() if n.endswith(('.go','.yml')) or n in ('go.mod','go.sum')]
def inventory():return {n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in names}
before=inventory()
for name,digest in before.items():assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)).hexdigest()==digest,name
(base/'source-before.json').write_text(json.dumps(dict(revision=revision,files=before,selected_inputs_match_git=True),indent=2)+'\n');shutil.copyfile(__file__,base/'executed-regression.py')
results=[]
try:
 for mode in ('normal','race'):
  command=['go','test','-p=1']+(['-race'] if mode=='race' else [])+['./internal/retainedindex','-count=1','-timeout=5m','-json']
  env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB');started=time.monotonic();print('START '+mode,flush=True)
  with (base/(mode+'.jsonl')).open('w') as out,(base/(mode+'.stderr')).open('w') as err:process=subprocess.run(command,cwd=repo,env=env,stdout=out,stderr=err)
  rows=[json.loads(line) for line in (base/(mode+'.jsonl')).read_text().splitlines()];passes=[r for r in rows if r.get('Action')=='pass' and 'Test' not in r]
  results.append(dict(mode=mode,command=command,environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT')},exit_code=process.returncode,package_passes=passes,wall_seconds=time.monotonic()-started));(base/'results.json').write_text(json.dumps(dict(source=revision,runs=results),indent=2)+'\n')
  print('END '+mode+' '+str(process.returncode),flush=True);assert process.returncode==0 and passes
finally:
 after=inventory();(base/'source-after.json').write_text(json.dumps(dict(revision=revision,files=after,unchanged=before==after),indent=2)+'\n');assert before==after
