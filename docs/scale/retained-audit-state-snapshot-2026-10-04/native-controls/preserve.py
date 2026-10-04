"""Preserve a fully reviewed native reader run with complete original inputs."""
import argparse,hashlib,json,tarfile
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--review',type=Path,required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args();root=a.root.resolve();review=a.review.resolve();out=a.out.resolve()
out.mkdir(parents=True,exist_ok=False)
def sha(path):
 with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
files={'producer/'+str(path.relative_to(root)):path for path in root.rglob('*') if path.is_file() and path.name!='proof.tar.gz'}
files.update({'reviewer/review.py':review/'review.py','reviewer/independent-review.json':review/'independent-review.json','reviewer/preserve.py':Path(__file__).resolve()})
ledger={name:dict(sha256=sha(path),bytes=path.stat().st_size) for name,path in files.items()}
archive=root/'proof.tar.gz'
with tarfile.open(archive,'w:gz',compresslevel=6) as tar:
 for name,path in sorted(files.items()):tar.add(path,arcname=name,recursive=False)
seen=set()
with tarfile.open(archive,'r:gz') as tar:
 for member in tar:
  assert member.isfile() and member.name in ledger and member.name not in seen
  seen.add(member.name);h=hashlib.sha256();count=0
  with tar.extractfile(member) as f:
   for block in iter(lambda:f.read(1024*1024),b''):h.update(block);count+=len(block)
  assert count==member.size==ledger[member.name]['bytes'] and h.hexdigest()==ledger[member.name]['sha256'],member.name
assert seen==set(ledger)
for name,path in files.items():assert sha(path)==ledger[name]['sha256'],name
whole=hashlib.sha256();parts=[]
with archive.open('rb') as f:
 while block:=f.read(25*1024*1024):
  name=f'proof.tar.gz.part-{len(parts):02d}';path=out/name;path.write_bytes(block)
  digest=hashlib.sha256(block).hexdigest();assert sha(path)==digest
  whole.update(path.read_bytes());parts.append(dict(name=name,bytes=len(block),sha256=digest))
assert whole.hexdigest()==sha(archive) and sum(p['bytes'] for p in parts)==archive.stat().st_size
manifest=dict(files=ledger,members=len(seen),member_bytes=sum(v['bytes'] for v in ledger.values()),archive_sha256=sha(archive),archive_bytes=archive.stat().st_size,parts=parts,all_original_member_hashes_verified=True,all_original_inputs_unchanged=True,parts_and_concat_sha_verified=True)
(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
for name in ['review.py','preserve.py','independent-review.json']:
 src=Path(__file__).resolve() if name=='preserve.py' else review/name
 (out/name).write_bytes(src.read_bytes())
print(json.dumps({k:v for k,v in manifest.items() if k!='files'},indent=2))
