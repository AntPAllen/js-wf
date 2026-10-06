from pathlib import Path
import subprocess,json,hashlib,io,tarfile,shutil,os,time,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-chunked-r1-full400k-owner-restart-20261006');donor=Path('/tmp/js-wf-concurrent-state-400k-capacity-bounded-preparation-20261005')
assert not root.exists() and not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
clone_bytes=sum(p.stat().st_size for p in (donor/'originals/TestConcurrentStateR5PopulationCapacity/cluster').rglob('*') if p.is_file())
assert shutil.disk_usage(repo).free>clone_bytes+320*1024**2,'Need full clone bytes plus320MiB for source/executables/delta/parts/Git and reserve'
root.mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def save(n,d):(root/n).write_text(json.dumps(d,indent=2)+'\n')
e=json.loads((donor/'execution.json').read_text());assert e['status']=='failed' and not Path('/proc/'+str(e['pid'])).exists()
original_servers=json.loads((donor/'actual-containers/actual-servers.json').read_text());assert len(original_servers)==5
for x in original_servers:assert not Path('/proc/'+str(x['host_pid'])).exists()
identity=original_servers[0]['container']['Name'].lstrip('/').rsplit('-n',1)[0]
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
base='docs/scale/concurrent-state-audit-2026-10-05/capacity-400k/'
meta=json.loads(subprocess.check_output(['git','show',revision+':'+base+'archive-verification.json'],cwd=repo))
class CanonicalParts(io.RawIOBase):
 def __init__(self):self.index=0;self.block=b'';self.offset=0;self.hash=hashlib.sha256()
 def readable(self):return True
 def readinto(self,target):
  if self.offset==len(self.block):
   if self.index==len(meta['parts']):return 0
   part=meta['parts'][self.index];self.block=subprocess.check_output(['git','cat-file','blob',revision+':'+base+part['file']],cwd=repo)
   assert len(self.block)==part['bytes'] and hashlib.sha256(self.block).hexdigest()==part['sha256']
   self.hash.update(self.block);self.index+=1;self.offset=0
  count=min(len(target),len(self.block)-self.offset);target[:count]=self.block[self.offset:self.offset+count];self.offset+=count;return count
raw=CanonicalParts();reader=io.BufferedReader(raw);manifest=None
with tarfile.open(fileobj=reader,mode='r|gz') as t:
 for m in t:
  if m.name=='archive-manifest.json':manifest=json.load(t.extractfile(m))
while reader.read(1024*1024):pass
assert raw.hash.hexdigest()==meta['archive_sha256'] and manifest==json.loads((donor/'archive-manifest.json').read_text())
clone=root/'copied-stores';clone.mkdir();original_hashes={}
for n in range(5):
 source=donor/'originals/TestConcurrentStateR5PopulationCapacity/cluster'/f'node-{n}'
 for p in source.rglob('*'):
  if p.is_file():
   key=str(p.relative_to(donor));digest=sha(p);assert digest==manifest[key]['sha256'];original_hashes[key]=digest
 shutil.copytree(source,clone/f'node-{n}')
 for p in (clone/f'node-{n}').rglob('*'):
  if p.is_file():assert sha(p)==original_hashes['originals/TestConcurrentStateR5PopulationCapacity/cluster/'+str(p.relative_to(clone))]
save('original-to-copy-verification.json',{'original':str(donor),'canonical_archive_sha256':meta['archive_sha256'],'canonical_git_manifest_verified':True,'files':original_hashes,'identity':identity})
source=root/'source';names=[n for n in subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines() if n.endswith('.go') or n in ('go.mod','go.sum')]
subprocess.run(['git','worktree','add','--detach','--no-checkout',str(source),revision],cwd=repo,check=True)
subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],cwd=source,input=''.join('/'+n+'\n' for n in names),text=True,check=True)
subprocess.run(['git','checkout','--detach',revision],cwd=source,check=True)
def inventory():
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 return {'revision':revision,'files':{n:sha(source/n) for n in names}}
before=inventory();save('source-before.json',before)
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')}
env.update(WF_AUDIT_CAPACITY_PREPARE_RESTORED='1',WF_AUDIT_CAPACITY_CURSOR_NAMES='1',WF_AUDIT_CHUNKED_CALLBACK='1',GOMAXPROCS='4',GOGC='500',GOMEMLIMIT='4GiB',WF_AUDIT_CAPACITY_R1_PROCESS_FAULT='owner-restart',WF_AUDIT_CAPACITY_R1_CPU_PROFILE='1',WF_AUDIT_CAPACITY_PROFILE_STORES=str(clone),WF_AUDIT_CAPACITY_PROFILE_IDENTITY=identity,WF_AUDIT_BATCH_ROOT=str(root/'originals'),WF_TIER3_EXPLICIT_ROUTE_SEEDS='1',WF_TIER3_SYNC_INTERVAL='2m')
build=['go','test','-p=1','-buildvcs=true','-c','-o',str(root/'integrity.test'),'./integrity']
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(root/'integrity.test')],text=True);assert 'vcs.revision='+revision in info and 'vcs.modified=false' in info
save('binary.json',{'sha256':sha(root/'integrity.test'),'build_info':info})
args=[str(root/'integrity.test'),'-test.run=^TestConcurrentStateR5CopiedCapacityProfile$','-test.v','-test.count=1','-test.timeout=6m']
save('commands.json',{'build':build,'test':args,'env':{k:v for k,v in env.items() if k.startswith('WF_') or k in ('GOMAXPROCS','GOMEMLIMIT','GOGC')},'source_capture_scope':'Selected Git Go/module files and executable metadata, not exhaustive compiler/toolchain inputs'})
(root/'actual-containers').mkdir();records=[];seen=set();memory_observations=[]
with (root/'native.log').open('w') as log:
 p=subprocess.Popen(args,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT);exe=Path(f'/proc/{p.pid}/exe')
 actual={'pid':p.pid,'source':revision,'sha256':sha(exe),'build_info':subprocess.check_output(['go','version','-m',str(exe)],text=True),'args':args,'stat':Path(f'/proc/{p.pid}/stat').read_text(),'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Explicit four-core GOGC500 4GiB verified full400k/4.8M copied-store R1 chunked baseline plus actual R5 cursor-owner SIGKILL plus same-store restart; original20s per baseline/fault includes cleanup; all verdicts retained'}
 assert actual['sha256']==sha(root/'integrity.test');save('execution.json',actual);print('NATIVE_STARTED',p.pid,revision,flush=True)
 while p.poll() is None:
  try:
   status=Path(f'/proc/{p.pid}/status').read_text().splitlines()
   memory_observations.append({'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':p.pid,'proc_status_memory':[line for line in status if line.startswith(('VmRSS:','VmHWM:','VmSize:'))]})
   save('sdk-memory-observations.json',memory_observations)
  except FileNotFoundError:pass
  ids=subprocess.check_output(['docker','ps','-q','--filter',f'name=js-wf-route-{p.pid}-'],text=True).split()
  for cid in ids:
   try:
    item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0];pid=item['State']['Pid']
    if not pid or (cid,pid) in seen:continue
    path=Path(f'/proc/{pid}/exe');digest=sha(path);binary=root/'actual-containers'/digest
    if not binary.exists():shutil.copyfile(path,binary)
    assert sha(binary)==digest
    records.append({'container':item,'host_pid':pid,'sha256':digest,'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actual_proc_build_info':subprocess.check_output(['go','version','-m',str(path)],text=True)})
    seen.add((cid,pid));save('actual-containers/actual-servers.json',records)
   except (FileNotFoundError,ProcessLookupError,subprocess.CalledProcessError):continue
  time.sleep(.5)
 code=p.wait();actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save('execution.json',actual)
after=inventory();save('source-after.json',after);assert before==after
for key,digest in original_hashes.items():assert sha(donor/key)==digest
save('original-after-verification.json',{'files':len(original_hashes),'all_original_store_bytes_unchanged':True})
assert not Path('/proc/'+str(p.pid)).exists()
for x in records:assert not Path('/proc/'+str(x['host_pid'])).exists()
shutil.copyfile(__file__,root/'executed-producer.py');print('NATIVE_FINISHED',code,flush=True)
raise SystemExit(code)
