from pathlib import Path
import json,hashlib,subprocess,io,tarfile,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-callback-400k-four-core-copied-comparison-20261005');clone=root/'copied-stores'
base='docs/scale/callback-audit-delivery-2026-10-05/capacity-400k/'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]==head
meta=json.loads(subprocess.check_output(['git','show',head+':'+base+'archive-verification.json'],cwd=repo))
e=json.loads((root/'execution.json').read_text());assert e['status']=='failed' and not Path('/proc/'+str(e['pid'])).exists()
for x in json.loads((root/'actual-containers/actual-servers.json').read_text()):assert not Path('/proc/'+str(x['host_pid'])).exists()
assert clone.is_dir() and not clone.is_symlink()
for p in Path('/proc').glob('[0-9]*'):
 try:
  for fd in (p/'fd').iterdir():
   try:assert not fd.resolve().is_relative_to(clone)
   except (PermissionError,FileNotFoundError):pass
 except (PermissionError,FileNotFoundError):pass
class Parts(io.RawIOBase):
 def __init__(self):self.index=0;self.block=b'';self.offset=0;self.hash=hashlib.sha256()
 def readable(self):return True
 def readinto(self,target):
  if self.offset==len(self.block):
   if self.index==len(meta['parts']):return 0
   part=meta['parts'][self.index];self.block=subprocess.check_output(['git','cat-file','blob',head+':'+base+part['file']],cwd=repo)
   assert len(self.block)==part['bytes'] and hashlib.sha256(self.block).hexdigest()==part['sha256']
   self.hash.update(self.block);self.index+=1;self.offset=0
  n=min(len(target),len(self.block)-self.offset);target[:n]=self.block[self.offset:self.offset+n];self.offset+=n;return n
raw=Parts();reader=io.BufferedReader(raw);manifest=None
with tarfile.open(fileobj=reader,mode='r|gz') as t:
 for m in t:
  if m.name=='archive-manifest.json':manifest=json.load(t.extractfile(m))
while reader.read(1024*1024):pass
assert raw.hash.hexdigest()==meta['archive_sha256'] and manifest==json.loads((root/'archive-manifest.json').read_text())
files=0;size=0
for p in clone.rglob('*'):
 if p.is_file():
  key=str(p.relative_to(root));assert not p.is_symlink()
  with p.open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
  assert digest==manifest[key]['sha256'] and p.stat().st_size==manifest[key]['bytes'];files+=1;size+=p.stat().st_size
assert files>0
shutil.rmtree(clone)
record={'removed_copy':str(clone),'files_verified':files,'bytes':size,'canonical_archive_sha256':meta['archive_sha256'],'pushed_main':head,'canonical_git_parts_and_manifest_verified':True,'observed_processes_closed':True,'no_visible_open_fds':True,'scope':'Only closed disposable failed callback comparison copy removed; originals, source, executable, profiles and canonical archive retained; reconstructable from committed parts'}
Path('/tmp/js-wf-archived-callback-400k-copy-cleanup-20261005.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record),flush=True)
