from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 for c in running:
  for m in c['Mounts']:
   source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
 loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
 for device in loops['loopdevices']:
  assert not Path(device['back-file']).resolve().is_relative_to(root)
 mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
 def walk(rows):
  for row in rows:
   assert not Path(row['target']).resolve().is_relative_to(root)
   assert not row['source'].startswith(str(root))
   walk(row.get('children',[]))
 walk(mounts['filesystems'])
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids)
config=s3.credentials()
def get(url,verify):
 command=['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url]
 with subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as process:
  process.stdin.write(config);process.stdin.close()
  try:result=verify(process.stdout)
  except BaseException:process.kill();process.wait();raise
  error=process.stderr.read();code=process.wait();assert code==0,('S3 GET failed',code,error.decode())
 return result
import tarfile
from fixture_delta import safe_name
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
root=Path('/tmp')/'js-wf-tier3-clock131-143-37164231641';archive=root/'original-stores/rolling-originals.tar.gz'
canonical='docs/scale/tmp-storage-review-2026-10-06/worker-clock-131-143'
blob=lambda name:subprocess.check_output(['git','show',head+':'+canonical+'/'+name],cwd=repo)
meta_bytes=blob('archive-verification.json');meta=json.loads(meta_bytes);receipt=json.loads(blob('s3-readback.json'))
member_bytes=blob('member-inventory.json');members=json.loads(member_bytes)
root_bytes=blob('root-inventory.json');current=json.loads(root_bytes)
assert hashlib.sha256(member_bytes).hexdigest()==meta['member_inventory_sha256']
assert hashlib.sha256(root_bytes).hexdigest()==meta['root_inventory_sha256']
assert meta['archive_parts_count']==0 and meta['archive_parts']==[]
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert receipt['archive']['full_readback']==expected
assert receipt['metadata']['full_readback']==dict(bytes=len(meta_bytes),sha256=hashlib.sha256(meta_bytes).hexdigest())
assert fixture_archive.inventory(root)==current
with archive.open('rb') as stream:assert s3.digest(stream)==expected
initial=closure(root)
assert get(receipt['metadata']['url'],s3.digest)==receipt['metadata']['full_readback']
def verify_remote(stream):
 class Hashed:
  def __init__(self):self.hash=hashlib.sha256();self.bytes=0
  def read(self,n=-1):
   data=stream.read(n);self.hash.update(data);self.bytes+=len(data);return data
 hashed=Hashed();actual={}
 with tarfile.open(fileobj=hashed,mode='r|gz') as tar:
  for m in tar:
   n=safe_name(m.name);assert m.isfile() and n not in actual
   r=s3.digest(tar.extractfile(m));assert r['bytes']==m.size
   r.update(mode=m.mode,mtime=m.mtime);actual[n]=r
 while hashed.read(1<<20):pass
 fingerprint=dict(bytes=hashed.bytes,sha256=hashed.hash.hexdigest())
 assert fingerprint==expected and actual==members
 return dict(fingerprint=fingerprint,members_verified=len(actual))
remote=get(receipt['archive']['url'],verify_remote)
assert fixture_archive.inventory(root)==current
final=closure(root)
relative=archive.relative_to(root).as_posix();assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
out=repo/'docs/scale/tmp-storage-review-2026-10-06/worker-clock-131-143/primary-offload';out.mkdir()
shutil.copyfile(__file__,out/'executed-offload.py');allocated=archive.stat().st_blocks*512
archive.unlink();remaining={n:r for n,r in current.items() if n!=relative}
assert fixture_archive.inventory(root)==remaining
report=dict(head=head,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),offloaded_primary_archive=str(archive),archive_url=receipt['archive']['url'],complete_remote_readback=remote,remote_metadata_full_readback=True,full_current_root_census_verified_before=True,remaining_bytes_modes_mtimes_unchanged=True,allocated_bytes_recovered=allocated,free_bytes=shutil.disk_usage(repo).free,closure_before=initial,closure_after_remote_readback=final,scope='User-requested /tmp cleanup: sole local primary rolling-store tar moved to committed verified S3 archive after fresh compressed-body/every51,519 member/current-root/visible closure checks. Full source/native/provider/model/raw reports and rolling decompression manifest retained locally; any future physical audit downloads a verified fresh archive copy. Existing accepted executed-source worker-clock131-143 scope unchanged; no new native/current/fullmatrix/24h or provider durability claim.')
(out/'offload.json').write_text(json.dumps(report,indent=2)+'\n');print('PRIMARY_ROLLING_ARCHIVE_OFFLOADED',allocated,report['free_bytes'],flush=True)
