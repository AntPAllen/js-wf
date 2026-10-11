import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
repo=out.parents[2]
root=Path('/home/exedev/js-wf-signal-phases-20261011')
inputs=json.loads((out/'source-inputs.json').read_text())
def check():
    return all(hashlib.sha256((repo/n).read_bytes()).hexdigest()==h for n,h in inputs.items())
assert check()
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
for mode in ('normal','race'):
    for package,pattern in [('client','^(TestCanonicalSignalMutationErrorPhases|TestGraphCanonicalSignalBindingBudgetRecovery|TestGraphCanonicalSignalBindingActualDeadlineRecovery|TestCanonicalSignalConflictIsNotGenerationReplacement)$'),('sim','^TestPinnedRegressionCorpus$')]:
        label=f'{package}-{mode}'
        before=set(root.glob('*-execution.json'))
        cmd=['go','test']+(['-race'] if mode=='race' else [])+['-p=1','-exec',f'python3 {root}/capture.py',f'./{package}','-run',pattern,'-count=1','-v','-timeout=6m']
        start=time.time_ns()
        with (out/(label+'.log')).open('w') as f:
            p=subprocess.Popen(cmd,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT)
            code=p.wait()
        end=time.time_ns()
        executions={x.name:json.loads(x.read_text()) for x in set(root.glob('*-execution.json'))-before}
        (out/(label+'-command.json')).write_text(json.dumps(dict(argv=cmd,pid=p.pid,start_ns=start,end_ns=end,actual_exit=code,executions=executions,source_unchanged=check()),indent=2)+'\n')
        assert code==0 and check() and len(executions)==1,label
