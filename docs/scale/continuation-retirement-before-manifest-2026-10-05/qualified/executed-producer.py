from pathlib import Path
import subprocess,json,hashlib,os,datetime,time,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-retirement-before-manifest-repair-race-20261005');root.mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
names=sorted(set(subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum'],cwd=repo,text=True).splitlines()))
before={n:sha(repo/n) for n in names};(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n')
for n in names:
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+n],cwd=repo)).hexdigest()==before[n]
 p=root/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(repo/n,p)
env=dict(os.environ,GOCACHE='/tmp/js-wf-go-build-cache-20261004',GOMAXPROCS='2',GOMEMLIMIT='2GiB')
command=['go','test','-p=1','-race','-buildvcs=true','-c','-o',str(root/'integrity.test'),'./integration']
with (root/'build.log').open('w') as log:subprocess.run(command,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(root/'integrity.test')],text=True);assert f'vcs.revision={revision}' in info and 'vcs.modified=false' in info
(root/'binary.json').write_text(json.dumps({'sha256':sha(root/'integrity.test'),'build_info':info},indent=2)+'\n')
env.update(WF_RETIREMENT_KILL_ROOT=str(root/'originals'),WF_TIER3_SYNC_INTERVAL='2m')
args=[str(root/'integrity.test'),'-test.run=^TestContinuationRetirementReuseBeforeManifestWorkerSIGKILL$','-test.count=1','-test.v','-test.timeout=12m']
(root/'commands.json').write_text(json.dumps({'build':command,'test':args,'cwd':str(repo),'environment':{k:v for k,v in env.items() if k.startswith('WF_') or k in ['GOMEMLIMIT','GOMAXPROCS','GOCACHE']}},indent=2)+'\n')
with (root/'native.log').open('w') as log:
 child=subprocess.Popen(args,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
 exe=Path(f'/proc/{child.pid}/exe');actual={'pid':child.pid,'exe':os.readlink(exe),'exe_sha256':sha(exe),'revision':revision,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'running'};assert actual['exe_sha256']==sha(root/'integrity.test')
 (root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n');print('NATIVE_STARTED',child.pid,revision,flush=True)
 status=child.wait();actual.update(status='passed' if status==0 else 'failed',exit_code=status,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-producer.py')
after={n:sha(repo/n) for n in names};assert before==after
(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':after},indent=2)+'\n')
print('NATIVE_FINISHED',status,'selected_inputs',len(names),flush=True)
raise SystemExit(status)
