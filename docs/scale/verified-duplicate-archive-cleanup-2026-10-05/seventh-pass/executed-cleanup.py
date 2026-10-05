from pathlib import Path
import json,hashlib,subprocess
repo=Path('/home/exedev/js-wf')
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
paths=subprocess.check_output(['git','ls-files','**/archive-verification.json'],cwd=repo,text=True).splitlines();byhash={}
for n in paths:
 p=repo/n
 try:d=json.loads(p.read_text())
 except (ValueError,FileNotFoundError):continue
 if 'archive_sha256' in d and d.get('all_archive_members_and_parts_read_back') and all((p.parent/part['file']).is_file() for part in d['parts']):byhash[d['archive_sha256']]=(p,d)
removed=[];total=0
for p in sorted(Path('/tmp').glob('js-wf-*/proof.tar.gz'),key=lambda p:p.stat().st_size,reverse=True):
 h=sha(p)
 if h not in byhash:continue
 meta,d=byhash[h]
 assert meta.read_bytes()==subprocess.check_output(['git','show','HEAD:'+str(meta.relative_to(repo))],cwd=repo)
 combined=hashlib.sha256();parts=[]
 for part in d['parts']:
  f=meta.parent/part['file'];data=f.read_bytes()
  assert hashlib.sha256(data).hexdigest()==part['sha256'] and len(data)==part['bytes']
  assert subprocess.check_output(['git','hash-object',str(f)],cwd=repo,text=True).strip()==subprocess.check_output(['git','rev-parse','HEAD:'+str(f.relative_to(repo))],cwd=repo,text=True).strip()
  combined.update(data);parts.append(str(f.relative_to(repo)))
 assert combined.hexdigest()==h and p.stat().st_size==d['archive_bytes']
 n=p.stat().st_size;p.unlink();total+=n;removed.append({'temporary_duplicate':str(p),'bytes':n,'sha256':h,'committed_metadata':str(meta.relative_to(repo)),'committed_parts':parts})
 Path('/tmp/js-wf-verified-duplicate-archive-cleanup-seventh-progress-20261005.json').write_text(json.dumps({'removed_bytes':total,'removed':removed},indent=2)+'\n')
 if total>=600_000_000:break
out={'scope':'Only redundant temporary proof.tar.gz removed after full byte verification against committed archive parts; original files/stores/source/media untouched','removed_bytes':total,'removed':removed}
Path('/tmp/js-wf-verified-duplicate-archive-cleanup-seventh-20261005.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'removed_bytes':total,'files':len(removed)}))
