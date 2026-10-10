import datetime,hashlib,json,os,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
repo=base.parents[2]
artifact=Path('/home/exedev/js-wf-failed-child-native-20261010/corpus-race.test')
names=subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines()
names=[name for name in names if name.endswith('.go') or name in ('go.mod','go.sum') or name.startswith('sim/testdata/regressions/')]
names.append('sim/graph_caller_admission_fault_test.go')
names=sorted(set(names))
def inventory():return {name:hashlib.sha256((repo/name).read_bytes()).hexdigest() for name in names}
before=inventory();(base/'corpus-source-before.json').write_text(json.dumps(before,indent=2)+'\n')
record=dict(head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),started=datetime.datetime.now(datetime.timezone.utc).isoformat(),env=dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB'),commands=[])
def save(): (base/'corpus-race-command.json').write_text(json.dumps(record,indent=2)+'\n')
env={**os.environ,**record['env']}
compile=['go','test','-c','-race','./sim','-o',str(artifact)]
with (base/'corpus-race-build.log').open('w') as out:
 result=subprocess.run(compile,cwd=repo,env=env,stdout=out,stderr=subprocess.STDOUT)
record['commands'].append(dict(command=compile,actual_exit_code=result.returncode));save()
result.check_returncode()
record['binary_sha256']=hashlib.sha256(artifact.read_bytes()).hexdigest()
record['build_info']=subprocess.check_output(['go','version','-m',str(artifact)],text=True)
assert '-race=true' in record['build_info']
command=[str(artifact),'-test.run=^(TestPinnedRegressionCorpus|TestGraphSignalCallerRetryAfterContention)$','-test.count=1','-test.timeout=5m','-test.v=true']
with (base/'corpus-race.log').open('w') as out:
 result=subprocess.run(command,cwd=repo/'sim',env=env,stdout=out,stderr=subprocess.STDOUT)
record['commands'].append(dict(command=command,actual_exit_code=result.returncode));record['finished']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
after=inventory();(base/'corpus-source-after.json').write_text(json.dumps(after,indent=2)+'\n');assert before==after
result.check_returncode()
print(json.dumps({key:record[key] for key in ('binary_sha256','started','finished')}))
