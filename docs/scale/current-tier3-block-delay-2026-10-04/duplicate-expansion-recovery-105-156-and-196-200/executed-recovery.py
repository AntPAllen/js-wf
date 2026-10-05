import datetime,hashlib,json,os,shutil,subprocess,tarfile,zipfile
from pathlib import Path,PurePosixPath
repo=Path('/home/exedev/js-wf')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]==head
out=repo/'docs/scale/current-tier3-block-delay-2026-10-04/duplicate-expansion-recovery-105-156-and-196-200'
out.mkdir(exist_ok=False)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def streamsha(f):return hashlib.file_digest(f,'sha256').hexdigest()
reports=[]
for first,last in [(f,f+12) for f in range(105,157,13)]+[(196,200)]:
 root=Path(f'/tmp/js-wf-tier3-block-delay{first}-{last}-37164231641');target=root/'raw';doc=repo/f'docs/scale/current-tier3-block-delay-2026-10-04/seeds-{first}-{last}'
 manifest_path=doc/'manifest.json';m=json.loads(manifest_path.read_text());r=json.loads((root/'row-review.json').read_text());download=json.loads((root/'download-review.json').read_text())
 assert r['shard_qualified'] and json.loads((root/'model-review.json').read_text())['all_three_models_exact_ok'] and r['first_seed']==first and r['last_seed']==last and r['row']=='block_delay'
 assert m['all_members_readback_verified'] and m['all_parts_readback_verified']
 for n in ['row-review.json','model-review.json','download-review.json','run.json','job.json','artifact.json']:assert sha(root/n)==m['files'][n]['sha256']
 assert hashlib.sha256(subprocess.check_output(['git','show',head+':'+str(manifest_path.relative_to(repo))],cwd=repo)).hexdigest()==sha(manifest_path)
 assert sha(root/'proof.tar.gz')==m['archive_sha256'] and (root/'proof.tar.gz').stat().st_size==m['archive_bytes']
 combined=hashlib.sha256();combined_size=0
 for part in m['parts']:
  p=doc/part['path'];assert p.is_file() and not p.is_symlink() and sha(p)==part['sha256'] and p.stat().st_size==part['bytes']
  proc=subprocess.Popen(['git','show',head+':'+str(p.relative_to(repo))],cwd=repo,stdout=subprocess.PIPE);h=hashlib.sha256();size=0
  for b in iter(lambda:proc.stdout.read(1024*1024),b''):h.update(b);combined.update(b);size+=len(b)
  assert proc.wait()==0 and h.hexdigest()==part['sha256'] and size==part['bytes'];combined_size+=size
 assert combined.hexdigest()==m['archive_sha256'] and combined_size==m['archive_bytes']
 seen=set()
 with tarfile.open(root/'proof.tar.gz','r:gz') as t:
  for member in t:
   name=PurePosixPath(member.name)
   assert member.isfile() and not name.is_absolute() and '..' not in name.parts and name.as_posix()==member.name and member.name not in seen and member.name in m['files']
   entry=m['files'][member.name];assert member.size==entry['bytes'] and streamsha(t.extractfile(member))==entry['sha256'];seen.add(member.name)
 assert seen==set(m['files'])
 expected={n.removeprefix('raw/'):v['sha256'] for n,v in m['files'].items() if n.startswith('raw/')}
 assert expected==r['artifact_sha256'] and len(expected)==94*(last-first+1) and target.is_dir() and not target.is_symlink()
 paths=list(target.rglob('*'));assert not any(p.is_symlink() for p in paths)
 actual={str(p.relative_to(target)):p for p in paths if p.is_file()};assert set(actual)==set(expected)
 for name,p in actual.items():assert sha(p)==expected[name]
 assert sha(root/'raw.zip')==download['zip_sha256']==m['files']['raw.zip']['sha256']
 assert sha(root/'history-review')==m['files']['history-review']['sha256']
 with zipfile.ZipFile(root/'raw.zip') as z:
  assert len(z.namelist())==len(expected) and set(z.namelist())==set(expected)
  for name in expected:
   with z.open(name) as f:assert streamsha(f)==expected[name]
 opened=[]
 for fd in Path('/proc').glob('[0-9]*/fd/*'):
  try:link=os.readlink(fd)
  except (FileNotFoundError,PermissionError):continue
  if link==str(target) or link.startswith(str(target)+'/'):opened.append(str(fd))
 assert not opened,opened
 inodes={}
 for p in actual.values():
  s=p.stat();key=(s.st_dev,s.st_ino);v=inodes.setdefault(key,dict(paths=0,links=s.st_nlink,allocated=s.st_blocks*512));v['paths']+=1
 exclusive=sum(v['allocated'] for v in inodes.values() if v['paths']==v['links'])
 report=dict(range=f'{first}-{last}',proof_commit=head,pushed_main_verified=True,target=str(target),files=len(actual),canonical_archive=str(root/'proof.tar.gz'),canonical_sha256=m['archive_sha256'],original_zip=str(root/'raw.zip'),original_zip_sha256=download['zip_sha256'],all_canonical_members_and_committed_parts_verified=True,all_expanded_and_zip_members_verified=True,visible_open_fds=opened,exclusive_allocated_bytes_recovered=exclusive,actual_model_binary_retained=True,failed_evidence_unchanged=True)
 shutil.rmtree(target);assert not target.exists();report['removed']=True;reports.append(report)
 (out/'recovery.json').write_text(json.dumps(dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),reports=reports,total_exclusive_allocated_bytes_recovered=sum(x['exclusive_allocated_bytes_recovered'] for x in reports)),indent=2)+'\n')
 print(json.dumps(report),flush=True)
shutil.copyfile(__file__,out/'executed-recovery.py')
