from pathlib import Path
import subprocess,os,json,hashlib,shutil,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-delayed-expiry-model-20261005');root.mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
names=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum','sim/testdata'],cwd=repo,text=True).splitlines();before={n:sha(repo/n) for n in names}
for name in names:
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+name],cwd=repo)).hexdigest()==before[name]
 p=root/'source'/name;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/name,p)
(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n')
base=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOCACHE='/tmp/js-wf-go-build-cache-20261004');executions=[]
for race,seeds,pattern,label in [(False,'100000','^TestDelayedExpirySuccessiveWorkerKills$','normal-100k'),(True,'1000','^Test(DelayedExpirySuccessiveWorkerKills|KVExpiryVisibilityDelayRequiresUnusedTransport|PinnedRegressionCorpus)$','race-1k-and-pins')]:
 binary=root/('sim-race.test' if race else 'sim.test');build=['go','test','-p=1','-buildvcs=true','-c','-o',str(binary),'./sim'];
 if race:build.insert(2,'-race')
 with (root/(label+'-build.log')).open('w') as log:subprocess.run(build,cwd=repo,env=base,stdout=log,stderr=subprocess.STDOUT,check=True)
 info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert f'vcs.revision={revision}' in info and 'vcs.modified=false' in info
 env=dict(base,SIM_SEEDS=seeds,SIM_COVERAGE_SUMMARY='1');args=[str(binary),'-test.run='+pattern,'-test.count=1','-test.v','-test.timeout=10m'];row={'label':label,'build':build,'args':args,'seeds':int(seeds),'race':race,'sha256':sha(binary),'build_info':info,'source':revision}
 with (root/(label+'.log')).open('w') as log:
  p=subprocess.Popen(args,cwd=repo/'sim',env=env,stdout=log,stderr=subprocess.STDOUT);exe=Path(f'/proc/{p.pid}/exe');row.update(pid=p.pid,actual_executable_sha256=sha(exe),actual_build_info=subprocess.check_output(['go','version','-m',str(exe)],text=True));assert row['actual_executable_sha256']==row['sha256'];code=p.wait();row['exit_code']=code
 executions.append(row);(root/'execution.json').write_text(json.dumps(executions,indent=2)+'\n');print(label,code,flush=True);assert code==0
 text=(root/(label+'.log')).read_text();assert f'completed={seeds} requested={seeds}' in text
 if race:assert text.count('--- PASS: TestPinnedRegressionCorpus/')==392
assert {n:sha(repo/n) for n in names}==before
(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n');shutil.copy2(__file__,root/'executed-producer.py')
print('SOURCE_VERIFIED',len(names),flush=True)
