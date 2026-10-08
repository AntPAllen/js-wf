import sys,os,json,hashlib,subprocess,time,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-purge-2026-10-08'
before=json.loads((base/'source-before.json').read_text());names=list(before['files']);revision=before['revision']
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==revision
assert all(hashlib.sha256((repo/n).read_bytes()).hexdigest()==h for n,h in before['files'].items())
resultfile=base/'results.json';r=json.loads(resultfile.read_text());assert len(r['runs'])==12
shutil.copyfile(__file__,base/'executed-boundary-regression.py')
for mode in ('normal','race'):
 name='boundary-'+mode;root='/tmp/js-wf-graph-purge-'+name+'-20261008';assert not Path(root).exists()
 env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_COVERAGE_SUMMARY='1',SIM_SEEDS='1000',FAULT_TRACE_OUT='/tmp/js-wf-graph-purge-'+name+'-failure-20261008.json',WF_BLOB_BOUNDARY_ROOT=root)
 command=['go','test','-p=1',*(['-race'] if mode=='race' else []),'./retention','-run','^TestBlobSweepConcurrentRefreshContract$','-count=1','-timeout=5m','-json'];print('START '+name,flush=True);started=time.monotonic()
 with (base/(name+'.jsonl')).open('w') as out,(base/(name+'.stderr')).open('w') as err:result=subprocess.run(command,cwd=repo,env=env,stdout=out,stderr=err)
 rows=[json.loads(l) for l in (base/(name+'.jsonl')).read_text().splitlines()]
 passes=[dict(package=x['Package'],elapsed=x.get('Elapsed')) for x in rows if x.get('Action')=='pass' and 'Test' not in x]
 tops=[x['Test'] for x in rows if x.get('Action')=='pass' and x.get('Test') and '/' not in x['Test']]
 r['runs'].append(dict(name=name,command=command,working_directory=str(repo),environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','SIM_COVERAGE_SUMMARY','SIM_SEEDS','FAULT_TRACE_OUT','WF_BLOB_BOUNDARY_ROOT')},exit_code=result.returncode,wall_seconds=time.monotonic()-started,package_passes=passes,top_level_passes=tops));resultfile.write_text(json.dumps(r,indent=2)+'\n')
 print('END '+name+' '+json.dumps(dict(exit=result.returncode,passes=passes)),flush=True)
 assert result.returncode==0 and passes
 for shape in ('quiescent','refresh_after_census'):
  proof=Path(root)/shape/'boundary-proof.json';assert proof.exists();shutil.copyfile(proof,base/(name+'-'+shape+'-proof.json'))
after={n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in names};assert before['files']==after
(base/'source-after-boundary.json').write_text(json.dumps(dict(revision=revision,files=after,unchanged=True),indent=2)+'\n')
