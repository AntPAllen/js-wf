import pathlib,subprocess,json,hashlib,os,shutil,time,datetime
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-state-snapshot-copied-20261005');root.mkdir()
original=pathlib.Path('/tmp/js-wf-continuous-byte-journal-24h-20261005');manifest=json.loads((original/'archive-manifest.json').read_text())['files']
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
clone=root/'copied-stores';clone.mkdir();original_hashes={}
for node in range(5):
 source=original/'fixture/cluster'/f'node-{node}'
 for p in source.rglob('*'):
  if p.is_file():
   key=str(p.relative_to(original));digest=sha(p);assert digest==manifest[key],key;original_hashes[key]=digest
 shutil.copytree(source,clone/f'node-{node}')
 for p in (clone/f'node-{node}').rglob('*'):
  if p.is_file():assert sha(p)==original_hashes['fixture/cluster/'+str(p.relative_to(clone))]
(root/'original-to-copy-verification.json').write_text(json.dumps({'original':str(original),'original_archive_sha256':json.loads((original/'archive-manifest.json').read_text())['archive_sha256'],'files':original_hashes,'all_original_and_copy_bytes_match_verified_archive':True},indent=2)+'\n')
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
names=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum'],cwd=repo,text=True).splitlines();before={n:sha(repo/n) for n in names}
for n in names:
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+n],cwd=repo)).hexdigest()==before[n]
 p=root/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(repo/n,p)
(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n')
env=dict(os.environ,GOCACHE='/tmp/js-wf-go-build-cache-20261004',GOMAXPROCS='2',GOMEMLIMIT='2GiB')
build=['go','test','-p=1','-buildvcs=true','-c','-o',str(root/'integrity.test'),'./integrity']
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(root/'integrity.test')],text=True);assert f'vcs.revision={revision}' in info and 'vcs.modified=false' in info
(root/'binary.json').write_text(json.dumps({'sha256':sha(root/'integrity.test'),'build_info':info},indent=2)+'\n')
env.update(WF_AUDIT_STATE_PROFILE_STORES=str(clone),WF_AUDIT_STATE_PROFILE_IDENTITY='js-wf-route-6821-1791186805419314937',WF_AUDIT_BATCH_ROOT=str(root/'originals'),WF_TIER3_EXPLICIT_ROUTE_SEEDS='1',WF_TIER3_SYNC_INTERVAL='2m')
args=[str(root/'integrity.test'),'-test.run=^TestRetainedStateSnapshotCopiedStoreDiagnostic$','-test.count=1','-test.v','-test.timeout=6m']
(root/'commands.json').write_text(json.dumps({'build':build,'test':args,'environment':{k:v for k,v in env.items() if k.startswith('WF_') or k in ['GOMEMLIMIT','GOMAXPROCS','GOCACHE']}},indent=2)+'\n')
with (root/'native.log').open('w') as log:
 p=subprocess.Popen(args,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT);exe=pathlib.Path(f'/proc/{p.pid}/exe');actual={'pid':p.pid,'sha256':sha(exe),'exe':os.readlink(exe),'build_info':subprocess.check_output(['go','version','-m',str(exe)],text=True),'source':revision,'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};assert actual['sha256']==sha(root/'integrity.test');(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n');print('NATIVE_STARTED',p.pid,revision,flush=True)
 captured=False
 while p.poll() is None:
  if not captured:
   ids=subprocess.check_output(['docker','ps','--filter',f'name=js-wf-route-{p.pid}-','--format','{{.ID}}'],text=True).splitlines()
   if len(ids)==5:
    out=root/'actual-containers';out.mkdir();records=[]
    for i,cid in enumerate(ids):
     inspect=json.loads(subprocess.check_output(['docker','inspect',cid]))[0];hostpid=inspect['State']['Pid'];dest=out/f'server-{i}'
     with dest.open('wb') as f:subprocess.run(['sudo','cat',f'/proc/{hostpid}/exe'],stdout=f,check=True)
     records.append({'container':inspect,'host_pid':hostpid,'sha256':sha(dest),'actual_proc_build_info':subprocess.check_output(['sudo','go','version','-m',f'/proc/{hostpid}/exe'],text=True)})
    (out/'actual-servers.json').write_text(json.dumps(records,indent=2)+'\n');captured=True;print('SERVERS_CAPTURED',flush=True)
  time.sleep(.5)
 code=p.wait();actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),live_servers_captured=captured);(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
after={n:sha(repo/n) for n in names};assert before==after;(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':after},indent=2)+'\n')
for key,digest in original_hashes.items():assert sha(original/key)==digest,key
(root/'original-after-verification.json').write_text(json.dumps({'all_original_store_bytes_still_match':True,'files':len(original_hashes)},indent=2)+'\n');shutil.copy2(__file__,root/'executed-producer.py');print('NATIVE_FINISHED',code,flush=True)
raise SystemExit(code)
