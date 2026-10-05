from pathlib import Path
import subprocess,os,json,hashlib,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-held-observation-20261005');root.mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
names=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum','sim/testdata'],cwd=repo,text=True).splitlines();before={n:sha(repo/n) for n in names}
for n in names:
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+n],cwd=repo)).hexdigest()==before[n]
 p=root/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/n,p)
(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n');shutil.copytree('/tmp/js-wf-held-metadata-preliminary-failure-20261005',root/'preliminary-unqualified-failure')
base=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOCACHE='/tmp/js-wf-go-build-cache-20261004');rows=[]
for package,race,pattern,label in [('lease',False,'^Test(HeldObservationPreservesAcquisitionAndRequestCounts|HeldObservationNativeRenewalMetadata)$','native-r3-and-lease-controls'),('worker',True,'^Test(HeldLeaseDispatchReportsExistingEntryWithoutExtraRead|HeldLeaseRunUsesBoundedRedeliveryDelay)$','race-worker-controls'),('sim',True,'^Test(SeededTerminalHeldReplay|PinnedRegressionCorpus)$','race-model-1k-and-pins')]:
 binary=root/(package+'.test');build=['go','test','-p=1','-buildvcs=true','-c','-o',str(binary),'./'+package]
 if race:build.insert(2,'-race')
 with (root/(label+'-build.log')).open('w') as log:subprocess.run(build,cwd=repo,env=base,stdout=log,stderr=subprocess.STDOUT,check=True)
 info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert f'vcs.revision={revision}' in info and 'vcs.modified=false' in info
 env=dict(base,SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1')
 if package=='lease':env['WF_HELD_LEASE_NATIVE_ROOT']=str(root/'native')
 args=[str(binary),'-test.run='+pattern,'-test.count=1','-test.v','-test.timeout=6m'];row={'label':label,'source':revision,'build':build,'test':args,'race':race,'sha256':sha(binary),'build_info':info}
 with (root/(label+'.log')).open('w') as log:
  p=subprocess.Popen(args,cwd=repo/package,env=env,stdout=log,stderr=subprocess.STDOUT);exe=Path(f'/proc/{p.pid}/exe');row.update(pid=p.pid,actual_sha256=sha(exe),actual_build_info=subprocess.check_output(['go','version','-m',str(exe)],text=True));assert row['actual_sha256']==row['sha256'];row['exit_code']=p.wait()
 rows.append(row);(root/'execution.json').write_text(json.dumps(rows,indent=2)+'\n');print(label,row['exit_code'],flush=True)
 assert row['exit_code']==0
 if package=='sim':
  log=(root/(label+'.log')).read_text();assert 'completed=1000 requested=1000' in log;assert log.count('--- PASS: TestPinnedRegressionCorpus/')==392
assert {n:sha(repo/n) for n in names}==before;(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n');shutil.copy2(__file__,root/'executed-producer.py');print('ALL_FINISHED',len(names),flush=True)
