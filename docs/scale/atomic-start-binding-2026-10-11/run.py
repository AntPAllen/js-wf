import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent;repo=out.parents[2];root=Path('/home/exedev/js-wf-application-reader-release-20261011')
names=subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines()+['internal/graphpublication/application_release_test.go','sim/graph_start_atomic_binding_test.go']
names=[n for n in names if n.endswith(('.go','.py','.yml','.yaml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/regressions/')]
inputs={n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in sorted(set(names))}
(out/'source-inputs.json').write_text(json.dumps(inputs,indent=2)+'\n')
def check():return all(hashlib.sha256((repo/n).read_bytes()).hexdigest()==h for n,h in inputs.items())
for mode in ('normal','race'):
 for label,package,pattern in [('protocol','internal/graphpublication','^(TestGraphPublication.*|TestGraphApplicationAndReaderReleaseAtomic|TestGraphApplicationLifecycle|TestGraphReleaseReaderCurrent|TestGraphAppendPreparationErrorPhases|TestGraphAppendCommitErrorPhases|TestGraphAppendRequestDeadlineRecovery)$'),('client','client','^(TestCanonicalStart.*|TestCanonicalBoundStart.*|TestCanonicalSignalMutationErrorPhases|TestCanonicalSignalRecoveryAfterDedupExpiry|TestGraphCanonicalSignalBindingBudgetRecovery|TestGraphCanonicalSignalBindingActualDeadlineRecovery|TestCanonicalSignalConflictIsNotGenerationReplacement)$'),('journal','journal','^TestGraphCanonicalStart.*$'),('pins','sim','^(TestPinnedRegressionCorpus|TestGraphStartBindingAndReaderReleaseAtomic|TestGraphStartReadErrorPhases|TestGraphViewCloseSingleAuthorityObservation)$')]:
  name=label+'-'+mode;before=set(root.glob('*-execution.json'));start=time.time_ns()
  cmd=['go','test']+(['-race'] if mode=='race' else [])+['-p=1','-exec',f'python3 {root}/capture.py','./'+package,'-run',pattern,'-count=1','-v','-timeout=6m']
  with (out/(name+'.log')).open('w') as f:
   p=subprocess.Popen(cmd,cwd=repo,env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB'),stdout=f,stderr=subprocess.STDOUT);code=p.wait()
  end=time.time_ns()
  receipt=dict(argv=cmd,pid=p.pid,start_ns=start,end_ns=end,actual_exit=code,source_unchanged=check(),executions={x.name:json.loads(x.read_text()) for x in set(root.glob('*-execution.json'))-before})
  (out/(name+'-command.json')).write_text(json.dumps(receipt,indent=2)+'\n')
  assert code==0 and check() and len(receipt['executions'])==1,name
