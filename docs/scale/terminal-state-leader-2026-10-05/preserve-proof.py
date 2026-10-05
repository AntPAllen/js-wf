import pathlib,hashlib,json,tarfile,shutil,sys
root=pathlib.Path(sys.argv[1]);out=pathlib.Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=True)
manifest={}
for p in sorted(root.rglob('*')):
 if p.is_file() and p.name not in ('proof.tar.gz','archive-manifest.json'):
  manifest[str(p.relative_to(root))]={'bytes':p.stat().st_size,'sha256':hashlib.file_digest(p.open('rb'),'sha256').hexdigest()}
(root/'archive-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
archive=root/'proof.tar.gz'
with tarfile.open(archive,'w:gz') as t:
 for name in [*manifest,'archive-manifest.json']:t.add(root/name,arcname=name)
with tarfile.open(archive) as t:
 for name,m in manifest.items():
  f=t.extractfile(name);assert f is not None
  data=f.read();assert len(data)==m['bytes'] and hashlib.sha256(data).hexdigest()==m['sha256']
parts=[]
with archive.open('rb') as f:
 i=0
 while data:=f.read(25*1024*1024):
  p=out/f'proof.tar.gz.part-{i:03d}';p.write_bytes(data);parts.append({'file':p.name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()});i+=1
h=hashlib.sha256()
for part in parts:
 data=(out/part['file']).read_bytes();assert hashlib.sha256(data).hexdigest()==part['sha256'];h.update(data)
sha=hashlib.file_digest(archive.open('rb'),'sha256').hexdigest();assert h.hexdigest()==sha
review={'archive_sha256':sha,'archive_bytes':archive.stat().st_size,'members':len(manifest)+1,'parts':parts,'all_archive_members_and_parts_read_back':True}
(out/'archive-verification.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
