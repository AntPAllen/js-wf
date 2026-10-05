import pathlib,json,hashlib,tarfile
repo=pathlib.Path('/home/exedev/js-wf')
for label,rootname,outname in [('failed24h','js-wf-journal-byte-bounded-explicit-routes-normal-2g-24h-20261005','docs/scale/byte-bounded-audit-2026-10-05/24h-failed-1750'),('auto_seed1','js-wf-auto-journal-seed1-parent-stack-race-512m-20261005','docs/scale/current-tier3-automatic-membership-2026-10-05/seed1-terminal')]:
 root=pathlib.Path('/tmp')/rootname;out=repo/outname;out.mkdir(parents=True,exist_ok=True)
 manifest=json.loads((root/'archive-manifest.json').read_text());observed={}
 with tarfile.open(root/'originals.tar.gz','r|gz') as archive:
  for m in archive:
   assert m.isfile() and m.name not in observed and not m.name.startswith('/') and '..' not in pathlib.PurePosixPath(m.name).parts
   observed[m.name]=hashlib.file_digest(archive.extractfile(m),'sha256').hexdigest()
 assert observed==manifest['files']
 assert json.loads((root/'source-before.json').read_text())==json.loads((root/'source-after.json').read_text())
 physical={}
 for name,digest in observed.items():
  assert hashlib.file_digest((root/name).open('rb'),'sha256').hexdigest()==digest,name
  physical[name]={'bytes':(root/name).stat().st_size,'sha256':digest}
 archive=root/'originals.tar.gz';digest=hashlib.file_digest(archive.open('rb'),'sha256').hexdigest();assert digest==manifest['archive_sha256']
 parts=[];h=hashlib.sha256()
 with archive.open('rb') as f:
  i=0
  while data:=f.read(25*1024*1024):
   p=out/f'proof.tar.gz.part-{i:03d}';p.write_bytes(data);back=p.read_bytes();assert back==data;h.update(back)
   parts.append({'file':p.name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()});i+=1
 assert h.hexdigest()==digest
 review={'original_root':str(root),'archive_sha256':digest,'archive_bytes':archive.stat().st_size,'members':len(observed),'all_archive_members_and_physical_originals_match':True,'source_before_after_equal':True,'parts':parts,'all_parts_read_back':True}
 (out/'archive-verification.json').write_text(json.dumps(review,indent=2)+'\n')
 for name in ['execution.json','result.json','archive-manifest.json','actual-sdk.json','binary.json']:
  if (root/name).exists():(out/name).write_bytes((root/name).read_bytes())
 print(label,len(observed),digest,flush=True)
