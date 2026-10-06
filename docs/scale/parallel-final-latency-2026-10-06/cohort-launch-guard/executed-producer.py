from pathlib import Path
import subprocess,json,hashlib,os,shutil,time,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-parallel-latency-cohort-r5-20261006')
assert not root.exists()
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
assert shutil.disk_usage(repo).free>256*1024**2,'Need 256MiB free before finite native controls'
root.mkdir();source=root/'source'
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def save(name,data):(root/name).write_text(json.dumps(data,indent=2)+'\n')
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
names=[n for n in subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines() if n.endswith('.go') or n in ('go.mod','go.sum')]
subprocess.run(['git','worktree','add','--detach','--no-checkout',str(source),revision],cwd=repo,check=True)
subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],cwd=source,input=''.join('/'+n+'\n' for n in names),text=True,check=True)
subprocess.run(['git','checkout','--detach',revision],cwd=source,check=True)
def inventory():
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 return {'revision':revision,'files':{n:sha(source/n) for n in names}}
before=inventory();save('source-before.json',before)
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')}
original=Path('/tmp/js-wf-watch-observed-journal-24h-20261005')
origin=json.loads((original/'execution.json').read_text());assert origin['status']=='failed'
for pid in (162125,161783,origin['test_pid']):assert not Path('/proc/'+str(pid)).exists()
assert not subprocess.check_output(['docker','ps','-q','--filter','name=js-wf-route-162125-'],text=True).strip()
manifest=json.loads((original/'archive-manifest.json').read_text());assert sha(original/'originals.tar.gz')==manifest['archive_sha256']
import tarfile
selected={k:v for k,v in manifest['files'].items() if k.startswith('fixture/cluster/node-') or k=='fixture/faults.json'}
verified=set()
with tarfile.open(original/'originals.tar.gz','r|gz') as archive:
 for member in archive:
  name=member.name.removeprefix('./')
  if name in selected:
   with archive.extractfile(member) as f:assert hashlib.file_digest(f,'sha256').hexdigest()==selected[name]
   assert sha(original/name)==selected[name];verified.add(name)
assert verified==set(selected)
clone=root/'copied-stores';clone.mkdir()
needed=sum((original/k).stat().st_size for k in selected)
assert shutil.disk_usage(repo).free>needed+1024**3,'Need full copy plus1GiB proof/source/executable reserve'
for node in range(5):
 shutil.copytree(original/'fixture/cluster'/('node-'+str(node)),clone/('node-'+str(node)))
for p in clone.rglob('*'):
 if p.is_file():assert sha(p)==selected['fixture/cluster/'+str(p.relative_to(clone))]
shutil.copy2(original/'fixture/faults.json',root/'original-faults.json')
save('original-to-copy-verification.json',{'original':str(original),'original_archive_sha256':manifest['archive_sha256'],'files':selected,'all_original_and_copy_bytes_match_verified_archive':True,'original_sdk_and_owned_container_scope_closed':True})
env.update(GOMAXPROCS='4',GOGC='500',GOMEMLIMIT='4GiB',WF_MATRIX_LATENCY_COHORT_ROOT=str(root/'fixture'),WF_MATRIX_LATENCY_COHORT_STORES=str(clone),WF_MATRIX_LATENCY_COHORT_IDENTITY='js-wf-route-162125-1791200829958762326',WF_MATRIX_LATENCY_COHORT_FAULTS=str(root/'original-faults.json'),WF_TIER3_EXPLICIT_ROUTE_SEEDS='1',WF_TIER3_SYNC_INTERVAL='2m')
build=['go','test','-p=1','-buildvcs=true','-c','-o',str(root/'integration.test'),'./integration']
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(root/'integration.test')],text=True)
assert 'vcs.revision='+revision in info and 'vcs.modified=false' in info
save('binary.json',{'sha256':sha(root/'integration.test'),'build_info':info})
args=[str(root/'integration.test'),'-test.run=^TestMatrixParallelInvocationAuditsRetainedCohort$','-test.v','-test.count=1','-test.timeout=12m']
save('commands.json',{'build':build,'test':args,'env':{k:v for k,v in env.items() if k.startswith('WF_') or k in ('GOMAXPROCS','GOMEMLIMIT','GOGC')},'source_capture_scope':'Selected Git Go/module source, retained isolated worktree and executable build metadata; not exhaustive external toolchain/compiler input capture'})
(root/'actual-containers').mkdir();seen=set();records=[]
with (root/'native.log').open('w') as log:
 p=subprocess.Popen(args,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT)
 exe=Path(f'/proc/{p.pid}/exe')
 actual={'pid':p.pid,'source':revision,'sha256':sha(exe),'build_info':subprocess.check_output(['go','version','-m',str(exe)],text=True),'args':args,'stat':Path(f'/proc/{p.pid}/stat').read_text(),'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Normal current2.15.0 actual R5 verified disposable real87920 cohort point latency checks under6m; original donor never reopened, no original24h/full400k/currentmatrix qualification'}
 assert actual['sha256']==sha(root/'integration.test');save('execution.json',actual)
 print('NATIVE_STARTED',p.pid,revision,flush=True)
 while p.poll() is None:
  ids=subprocess.check_output(['docker','ps','-q','--filter',f'name=js-wf-route-{p.pid}-'],text=True).split()
  for cid in ids:
   try:
    item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0];pid=item['State']['Pid']
    if not pid or (cid,pid) in seen:continue
    path=Path(f'/proc/{pid}/exe');digest=sha(path)
    binary=root/'actual-containers'/digest
    if not binary.exists():shutil.copyfile(path,binary)
    assert sha(binary)==digest
    record={'container':item,'host_pid':pid,'sha256':digest,'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actual_proc_build_info':subprocess.check_output(['go','version','-m',str(path)],text=True)}
    assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in record['actual_proc_build_info']
    records.append(record);seen.add((cid,pid));save('actual-containers/actual-servers.json',records)
   except (FileNotFoundError,ProcessLookupError,subprocess.CalledProcessError):continue
  time.sleep(1)
 code=p.wait();actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save('execution.json',actual)
after=inventory();save('source-after.json',after);assert before==after
for name,digest in selected.items():assert sha(original/name)==digest
save('original-after-verification.json',{'files':len(selected),'all_original_bytes_unchanged':True})
assert sha(root/'integration.test')==actual['sha256']
assert not Path(f'/proc/{p.pid}').exists()
for r in records:assert not Path('/proc/'+str(r['host_pid'])).exists()
shutil.copyfile(__file__,root/'executed-producer.py')
print('NATIVE_FINISHED',code,flush=True)
raise SystemExit(code)
