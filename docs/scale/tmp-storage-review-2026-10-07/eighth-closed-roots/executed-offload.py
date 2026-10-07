from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
from concurrent.futures import ThreadPoolExecutor
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   for link in ['cwd','root']:
    target=(p/link).resolve();assert not target.is_relative_to(root),(p,link,target)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 docker_retries=[]
 for attempt in range(5):
  result=subprocess.run(['docker','inspect',*ids],capture_output=True,text=True) if ids else None
  if result is None or result.returncode==0:
   running=json.loads(result.stdout) if result else [];break
  docker_retries.append(dict(attempt=attempt,ids=ids,error=result.stderr))
  ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 else:raise RuntimeError('Docker inspection did not stabilize after five fresh-list retries')
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
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids,docker_inspection_retries=docker_retries)

config=s3.credentials()
def get(url,verify):
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(config);p.stdin.close()
  try: result=verify(p.stdout)
  except BaseException:p.kill();p.wait();raise
  error=p.stderr.read();assert p.wait()==0,error
 return result


head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
blob=lambda p:subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo)
base=repo/'docs/scale/tmp-storage-review-2026-10-07/eighth-closed-roots'
reports=[]
pending=[p for p in sorted(base.iterdir()) if p.is_dir() and not (p/'offload.json').exists() and (p/'s3-readback.json').exists()]
def remote_verify(out):
 rel=out.relative_to(repo);meta=json.loads(blob(rel/'archive-verification.json'));manifest=json.loads(blob(rel/'fixture-inventory.json'));receipt=json.loads(blob(rel/'s3-readback.json'))
 for file,key in [('archive-verification.json','metadata'),('fixture-inventory.json','inventory')]:
  data=blob(rel/file);assert get(receipt[key]['url'],s3.digest)==dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 inv,actual=get(receipt['archive']['url'],lambda stream:fixture_archive.verify_hashed_stream(stream,expected));assert inv==manifest
 return out,(rel,meta,manifest,receipt,expected,actual)
with ThreadPoolExecutor(max_workers=4) as pool:
 verified=dict(pool.map(remote_verify,pending))
for out in pending:
 root=Path('/tmp')/out.name
 rel,meta,manifest,receipt,expected,actual=verified[out]
 before=fixture_archive.inventory(root)
 assert all(manifest['files'].get(k)==v for k,v in before.items())
 missing={k:v for k,v in manifest['files'].items() if k not in before}
 prior=[]
 journal=out/'unlink-journal.jsonl'
 if journal.exists():prior=[json.loads(line) for line in journal.read_text().splitlines()]
 prior_by_name={r['path']:r for r in prior}
 recovery=json.loads((out/'partial-removal-recovery.json').read_text()) if (out/'partial-removal-recovery.json').exists() else {}
 assert set(missing)<=set(prior_by_name)|set(recovery.get('missing_files',{}))
 names={k:v for k,v in before.items() if (('/' in k and k.split('/')[0] not in ('reviewer','model-source')) or v['bytes']>=1048576 or (k.endswith('.log') and v['bytes']>=65536))}
 assert names or missing
 observation=closure(root)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
 links=[prior_by_name[k] if k in prior_by_name else dict(path=k,device=None,inode=None,links_before_unlink=None,allocated_bytes=0,accounting='Prior partial deletion; original link/allocation observation unavailable, excluded from reclaimed-byte totals.') for k in missing]
 changed_dirs=[]
 def writable_parent(path):
  parent=path.parent
  assert parent.is_relative_to(root) and not parent.is_symlink()
  mode=parent.stat().st_mode & 0o777
  if not mode & 0o200:
   parent.chmod(mode|0o200)
   changed_dirs.append(dict(path=str(parent.relative_to(root)),original_mode=mode,removal_mode=mode|0o200,scope='Directory permissions only; archived file bytes/modes/mtimes unchanged.'))
 with journal.open('a') as evidence:
  for k in names:
   path=root/k;stat=path.stat()
   row=dict(path=k,device=stat.st_dev,inode=stat.st_ino,links_before_unlink=stat.st_nlink,allocated_bytes=stat.st_blocks*512)
   evidence.write(json.dumps(row)+'\n');evidence.flush();os.fsync(evidence.fileno())
   writable_parent(path)
   path.unlink();links.append(row)
 allocated=sum(row['allocated_bytes'] for row in links if row['links_before_unlink']==1)
 for d in sorted((p for p in root.rglob('*') if p.is_dir()),key=lambda d:len(d.parts),reverse=True):
  if not any(d.iterdir()):writable_parent(d);d.rmdir()
 names.update(missing)
 assert fixture_archive.inventory(root)=={k:v for k,v in manifest['files'].items() if k not in names}
 (out/'directory-permission-adjustments.json').write_text(json.dumps(changed_dirs,indent=2)+'\n')
 (out/'removed-links.json').write_text(json.dumps(links,indent=2)+'\n')
 raw=Path('/tmp')/(out.name+'-storage-eighth-complete-20261007.tar.gz')
 with raw.open('rb') as stream:assert s3.digest(stream)==expected
 rawclosure=closure(raw);staging=raw.stat().st_blocks*512;assert raw.stat().st_nlink==1;raw.unlink()
 (out/'removed-links.json').write_text(json.dumps(links,indent=2)+'\n')
 (out/'removed-files.json').write_text(json.dumps(names,indent=2)+'\n')
 report=dict(head=head,root=str(root),receipt=str(rel/'s3-readback.json'),archive_url=receipt['archive']['url'],verified_remote=actual,closure=observation,staging_closure=rawclosure,allocated_bytes_removed=allocated,staging_bytes_removed=staging,removed_files=len(names),scope='Historical closed nested evidence/source/media and large files only; exact archived inventory verified; root logs/provenance and small reviewer/model source retained. Hard-link aliases are unlinked without modifying remaining aliases; allocated bytes count only final-link removals. No native verdict changed or broker opened.')
 (out/'offload.json').write_text(json.dumps(report,indent=2)+'\n');reports.append(report);print('OFFLOADED_NEW',out.name,allocated,staging,flush=True)
shutil.copyfile(__file__,base/'executed-offload.py')
print('TOTAL_NEW',sum(r['allocated_bytes_removed'] for r in reports),flush=True)
