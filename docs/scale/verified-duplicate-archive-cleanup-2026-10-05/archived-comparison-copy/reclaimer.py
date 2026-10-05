from pathlib import Path
import json,hashlib,subprocess,tarfile,shutil
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-failed-3140-concurrent-state-copied-comparison-20261005');clone=r/'copied-stores';meta=repo/'docs/scale/concurrent-state-audit-2026-10-05/copied-87920-comparison/archive-verification.json'
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
e=json.loads((r/'execution.json').read_text());assert e['status']=='passed' and not Path('/proc/'+str(e['pid'])).exists()
for x in json.loads((r/'actual-containers/actual-servers.json').read_text()):assert not Path('/proc/'+str(x['host_pid'])).exists()
assert clone.is_dir() and not clone.is_symlink()
for process in Path('/proc').glob('[0-9]*'):
 try:
  for fd in (process/'fd').iterdir():
   try:assert not fd.resolve().is_relative_to(clone)
   except (PermissionError,FileNotFoundError):pass
 except (PermissionError,FileNotFoundError):pass
assert meta.read_bytes()==subprocess.check_output(['git','show','HEAD:'+str(meta.relative_to(repo))],cwd=repo)
d=json.loads(meta.read_text());h=hashlib.sha256()
for part in d['parts']:
 p=meta.parent/part['file'];data=p.read_bytes();assert hashlib.sha256(data).hexdigest()==part['sha256']
 assert subprocess.check_output(['git','hash-object',str(p)],cwd=repo,text=True).strip()==subprocess.check_output(['git','rev-parse','HEAD:'+str(p.relative_to(repo))],cwd=repo,text=True).strip()
 h.update(data)
assert h.hexdigest()==d['archive_sha256']==sha(r/'proof.tar.gz')
files={str(p.relative_to(r)):sha(p) for p in clone.rglob('*') if p.is_file()};assert files
checked=set()
with tarfile.open(r/'proof.tar.gz','r|gz') as t:
 for m in t:
  if m.name in files:
   assert m.isfile() and hashlib.file_digest(t.extractfile(m),'sha256').hexdigest()==files[m.name];checked.add(m.name)
assert checked==set(files)
size=sum(p.stat().st_size for p in clone.rglob('*') if p.is_file());shutil.rmtree(clone)
out={'scope':'Only closed diagnostic copied-stores removed after complete file and committed archive verification; originals and executable/source/log captures retained. Copy can be reconstructed from committed archive parts.','removed_copy':str(clone),'bytes':size,'files_verified':len(files),'committed_archive_metadata':str(meta.relative_to(repo)),'archive_sha256':d['archive_sha256'],'actual_sdk_gone':True,'all_observed_servers_gone':True,'no_open_file_descriptors':True}
Path('/tmp/js-wf-archived-comparison-copy-cleanup-20261005.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
