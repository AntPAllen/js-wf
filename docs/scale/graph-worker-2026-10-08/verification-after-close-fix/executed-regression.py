import sys,os,json,hashlib,subprocess,time,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-worker-2026-10-08/verification-after-close-fix';assert not base.exists();base.mkdir()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
names=[n for n in subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines() if n.endswith(('.go','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory():return {n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in names}
before=inventory()
for n,h in before.items():assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+n],cwd=repo)).hexdigest()==h,n
(base/'source-before.json').write_text(json.dumps(dict(revision=revision,files=before,selected_inputs_match_git=True),indent=2)+'\n');shutil.copyfile(__file__,base/'executed-regression.py')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_COVERAGE_SUMMARY='1')
specs=[]
for mode in ('normal','race'):
 flags=['-race'] if mode=='race' else []
 seeds='1000'
 for label,package,pattern in (('worker','./worker','^TestNativeGraphWorkerReplayInputsSignalsAndResults$'),('journal','./journal','^(TestGraphJournal|TestNativeGraphJournal)'),('client','./client',None)):
  command=['go','test','-p=1',*flags,package]
  if pattern:command+=['-run',pattern]
  specs.append((label+'-'+mode,command+['-count=1','-timeout=5m','-json'],seeds))
 specs.append(('worker-sim-'+mode,['go','test','-p=1',*flags,'./sim','-run','^(TestSeededGraphWorkerReplay|TestGraphPublicationTransportCopiesFaultsAndIndependentCensus|TestPinnedRegressionCorpus)$','-count=1','-timeout=5m','-json'],'10000' if mode=='normal' else '1000'))
results=[]
try:
 for name,command,seeds in specs:
  callenv=dict(env,SIM_SEEDS=seeds,FAULT_TRACE_OUT='/tmp/js-wf-graph-worker-'+name+'-failure-20261008.json')
  print('START '+name,flush=True);started=time.monotonic()
  with (base/(name+'.jsonl')).open('w') as out,(base/(name+'.stderr')).open('w') as err:result=subprocess.run(command,cwd=repo,env=callenv,stdout=out,stderr=err)
  rows=[json.loads(l) for l in (base/(name+'.jsonl')).read_text().splitlines()]
  passes=[dict(package=r['Package'],elapsed=r.get('Elapsed')) for r in rows if r.get('Action')=='pass' and 'Test' not in r]
  tops=[r['Test'] for r in rows if r.get('Action')=='pass' and r.get('Test') and '/' not in r['Test']]
  item=dict(name=name,command=command,working_directory=str(repo),environment={k:callenv[k] for k in ('GOMAXPROCS','GOMEMLIMIT','SIM_COVERAGE_SUMMARY','SIM_SEEDS','FAULT_TRACE_OUT')},exit_code=result.returncode,wall_seconds=time.monotonic()-started,package_passes=passes,top_level_passes=tops)
  results.append(item);(base/'results.json').write_text(json.dumps(dict(source=revision,runs=results),indent=2)+'\n')
  print('END '+name+' '+json.dumps(dict(exit=result.returncode,passes=passes,top_groups=len(tops))),flush=True)
  assert result.returncode==0 and passes,item
finally:
 after=inventory();(base/'source-after.json').write_text(json.dumps(dict(revision=revision,files=after,unchanged=before==after),indent=2)+'\n');assert before==after
