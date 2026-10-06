from pathlib import Path
import os,json,hashlib,subprocess,datetime
protected={'js-wf-bulk-latency-full400k-valid-20261006','js-wf-concurrent-state-400k-capacity-bounded-preparation-20261005','js-wf-tier1-handler-boundary-normal100k-20261006','js-wf-bulk-journal-24h-joined-20261006'}
roots={str(p):p for p in Path('/tmp').glob('js-wf-*') if p.is_dir() and not p.is_symlink() and p.name not in protected}
busy=set();limits=set();reasons=[]
def mark(value,kind):
 for key in roots:
  if value==key or value.startswith(key+'/') or (kind=='argv' and key in value):
   busy.add(key);reasons.append(dict(root=key,kind=kind,path=value))
for proc in Path('/proc').glob('[0-9]*'):
 try:
  mark(os.readlink(proc/'exe'),'exe')
  for arg in (proc/'cmdline').read_bytes().split(b'\0'):mark(os.fsdecode(arg),'argv')
 except PermissionError:limits.add(str(proc))
 except (FileNotFoundError,ProcessLookupError):pass
 for task in (proc/'task').glob('[0-9]*'):
  try:
   for descriptor in (task/'fd').iterdir():
    try:mark(os.readlink(descriptor),'fd')
    except (FileNotFoundError,PermissionError,ProcessLookupError):pass
  except PermissionError:limits.add(str(task/'fd'))
  except (FileNotFoundError,ProcessLookupError):pass
ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
containers=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
for c in containers:
 for m in c['Mounts']:
  for key in roots:
   source=Path(m['Source']).resolve();root=Path(key)
   if source.is_relative_to(root) or root.is_relative_to(source):busy.add(key)
loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
for device in loops['loopdevices']:mark(device['back-file'],'loop')
mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
def walk(rows):
 for row in rows:
  mark(row['target'],'mount');mark(row['source'],'mount');walk(row.get('children',[]))
walk(mounts['filesystems'])
files={};groups={}
for key,root in sorted(roots.items()):
 if key in busy:continue
 for path in sorted(root.rglob('*.test')):
  if path.is_symlink() or not path.is_file():continue
  s=path.stat()
  if s.st_blocks*512<32*1024*1024 or s.st_nlink!=1:continue
  h=hashlib.sha256()
  with path.open('rb') as stream:
   for block in iter(lambda:stream.read(1<<20),b''):h.update(block)
  after=path.stat();assert (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
  r=dict(bytes=s.st_size,sha256=h.hexdigest(),mode=s.st_mode&0o777,mtime_ns=s.st_mtime_ns,allocated_bytes=s.st_blocks*512,root=key)
  files[str(path)]=r;groups.setdefault(r['sha256'],[]).append(str(path))
out=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),files=files,groups=groups,protected_roots=sorted(protected),busy_roots=sorted(busy),visible_activity_reasons=reasons,observation_limits=sorted(limits),scope='Read-only candidate catalogue. No files deleted or source/native/lifetime gates qualified. Inaccessible processes are outside this visible observation.')
Path('/tmp/js-wf-closed-binary-catalogue-before-offload-20261006.json').write_text(json.dumps(out,indent=2)+'\n')
print('FILES',len(files),'GROUPS',len(groups),'ALLOCATED_GIB',sum(r['allocated_bytes'] for r in files.values())/1024**3,'DUPLICATE_GIB',sum(sum(files[p]['allocated_bytes'] for p in paths[1:]) for paths in groups.values())/1024**3,flush=True)
print('LARGEST_DUPLICATE_GROUPS',[(h,len(paths)) for h,paths in sorted(groups.items(),key=lambda x:-len(x[1]))[:10]],flush=True)
