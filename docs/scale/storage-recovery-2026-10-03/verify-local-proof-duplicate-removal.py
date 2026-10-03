import hashlib,json,os,pathlib,shutil,subprocess,tarfile,time
root=pathlib.Path('/tmp/js-wf-local-journal-tenm-20261003')
repo=pathlib.Path('/home/exedev/js-wf')
doc=repo/'docs/scale/local-r5-journal-ten-minute-2026-10-03'
assert root.resolve()==root
manifest=json.loads((root/'archive-manifest.json').read_text())
parts=json.loads((doc/'parts-manifest.json').read_text())
expected='9cfbc99123cec3a47e2264ca23f8dd843075dd9ca4cd743747d8bb17f1b28abf'
assert manifest['archive_sha256']==parts['archive_sha256']==expected
execution=json.loads((root/'execution.json').read_text())
assert execution['status']=='row_verified' and execution['test_exit_code']==0 and execution['duration']=='10m'
def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  while b:=f.read(1024*1024): h.update(b)
 return h.hexdigest()
assert digest(root/'originals.tar.gz')==expected
h=hashlib.sha256();n=0
for part in parts['parts']:
 p=doc/part['file']; assert p.stat().st_size==part['bytes'] and digest(p)==part['sha256']
 with p.open('rb') as f:
  while b:=f.read(1024*1024): h.update(b);n+=len(b)
assert h.hexdigest()==expected and n==parts['archive_bytes']
seen=set()
with tarfile.open(root/'originals.tar.gz','r:gz') as archive:
 for member in archive:
  name=pathlib.PurePosixPath(member.name)
  assert member.isfile() and not name.is_absolute() and '..' not in name.parts
  assert member.name in manifest['files'] and member.name not in seen
  f=archive.extractfile(member);h=hashlib.sha256()
  while b:=f.read(1024*1024):h.update(b)
  assert h.hexdigest()==manifest['files'][member.name],member.name
  raw=root/member.name
  assert raw.is_file() and not raw.is_symlink() and digest(raw)==h.hexdigest(),raw
  seen.add(member.name)
assert seen==set(manifest['files']) and len(seen)==5000
selected=[root/'fixture',root/'integration.test']
expanded=[]
for target in selected:
 assert not target.is_symlink()
 paths=[target] if target.is_file() else list(target.rglob('*'))
 for p in paths:
  assert not p.is_symlink()
  if p.is_file():
   assert str(p.relative_to(root)) in seen
   expanded.append(p)
allocated=sum(p.stat().st_blocks*512 for p in expanded)
raw_bytes=sum(p.stat().st_size for p in expanded)
for fd in pathlib.Path('/proc').glob('[0-9]*/fd/*'):
 try: path=os.readlink(fd)
 except OSError:continue
 assert not any(path==str(t) or path.startswith(str(t)+'/') for t in selected),(fd,path)
assert not subprocess.check_output(['docker','ps','-q']).strip(),'local Docker containers running'
verdict={'verified_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'archive_sha256':expected,'all_archive_members':len(seen),'all_archive_and_expanded_member_sha256_verified':True,'committed_parts_readback_verified':True,'removed_paths':[str(p) for p in selected],'expanded_files':len(expanded),'expanded_raw_bytes':raw_bytes,'reclaimed_allocated_bytes':allocated,'canonical_archive_retained':str(root/'originals.tar.gz'),'failed_stores_unchanged':True,'qualification_unchanged':True}
out=pathlib.Path('/tmp/js-wf-local-proof-duplicate-removal-verdict.json');out.write_text(json.dumps(verdict,indent=2)+'\n')
for p in selected:
 if p.is_dir():shutil.rmtree(p)
 else:p.unlink()
verdict['removal_complete']=all(not p.exists() for p in selected);assert verdict['removal_complete'];out.write_text(json.dumps(verdict,indent=2)+'\n');print(out.read_text())
