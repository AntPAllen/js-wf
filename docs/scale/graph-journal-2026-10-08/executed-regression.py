import sys,os,json,hashlib,subprocess,time,datetime,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf')
base=repo/'docs/scale/graph-journal-2026-10-08'
assert not base.exists();base.mkdir()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
names=subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines()
names=[n for n in names if n.endswith(('.go','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory():return {n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in names}
before=inventory()
for n,sha in before.items():
    assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+n],cwd=repo)).hexdigest()==sha,n
(base/'source-before.json').write_text(json.dumps(dict(revision=revision,files=before,selected_inputs_match_git=True),indent=2)+'\n')
shutil.copyfile(__file__,base/'executed-regression.py')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_COVERAGE_SUMMARY='1')
specs=[('native-normal',['go','test','-p=1','./journal','-run','^(TestGraphJournal|TestNativeGraphJournal)','-count=1','-timeout=5m','-json'],'1000'),('journal-normal100k',['go','test','-p=1','./sim','-run','^(TestSeededGraphJournalReplay|TestGraphPublicationTransportCopiesFaultsAndIndependentCensus|TestPinnedRegressionCorpus)$/^(graph-publication-|graph-readers-|graph-reader-resume-|graph-catalog-|graph-application-|graph-journal-)','-count=1','-timeout=10m','-json'],'100000'),('native-race',['go','test','-p=1','-race','./journal','-run','^(TestGraphJournal|TestNativeGraphJournal)','-count=1','-timeout=5m','-json'],'1000'),('journal-race1k',['go','test','-p=1','-race','./sim','-run','^(TestSeededGraphJournalReplay|TestGraphPublicationTransportCopiesFaultsAndIndependentCensus|TestPinnedRegressionCorpus)$/^(graph-publication-|graph-readers-|graph-reader-resume-|graph-catalog-|graph-application-|graph-journal-)','-count=1','-timeout=5m','-json'],'1000')]
results=[]
try:
    for name,command,seeds in specs:
        callenv=dict(env,SIM_SEEDS=seeds,FAULT_TRACE_OUT='/tmp/js-wf-graph-journal-'+name+'-failure-20261008.json')
        print('START '+name,flush=True);started=time.monotonic()
        with (base/(name+'.jsonl')).open('w') as out,(base/(name+'.stderr')).open('w') as err:
            result=subprocess.run(command,cwd=repo,env=callenv,stdout=out,stderr=err)
        rows=[json.loads(l) for l in (base/(name+'.jsonl')).read_text().splitlines()]
        package_passes=[dict(package=r['Package'],elapsed=r.get('Elapsed')) for r in rows if r.get('Action')=='pass' and 'Test' not in r]
        top_passes=[r['Test'] for r in rows if r.get('Action')=='pass' and 'Test' in r and '/' not in r['Test']]
        item=dict(name=name,command=command,working_directory=str(repo),environment={k:callenv[k] for k in ('GOMAXPROCS','GOMEMLIMIT','SIM_COVERAGE_SUMMARY','SIM_SEEDS','FAULT_TRACE_OUT')},exit_code=result.returncode,wall_seconds=time.monotonic()-started,package_passes=package_passes,top_level_passes=top_passes)
        results.append(item);(base/'results.json').write_text(json.dumps(dict(source=revision,runs=results),indent=2)+'\n')
        print('END '+name+' '+json.dumps(dict(exit=result.returncode,passes=package_passes,top_groups=len(top_passes))),flush=True)
        assert result.returncode==0 and package_passes,item
        assert ('TestNativeGraphJournalGenerationAndDrain' if name.startswith('native') else 'TestSeededGraphJournalReplay') in top_passes
finally:
    after=inventory();(base/'source-after.json').write_text(json.dumps(dict(revision=revision,files=after,unchanged=after==before),indent=2)+'\n')
    assert before==after
