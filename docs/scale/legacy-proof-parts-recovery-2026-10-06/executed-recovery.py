from pathlib import Path
import hashlib, io, json, subprocess, sys, tarfile, importlib.util, shutil
repo=Path('/home/exedev/js-wf'); sys.path.insert(0,str(repo/'scripts')); import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py'); closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
blob=lambda path: subprocess.check_output(['git','cat-file','blob',head+':'+path],cwd=repo)
tracked=subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines()
groups=sorted({str(Path(p).parent) for p in tracked if '.tar.gz.part-' in p and (repo/p).is_file()})
records=[]; verified=[]; skipped=[]; fdchecks=[]
for group in groups:
 try:
  meta=None
  for name in ['manifest.json','parts.json','archive-verification.json']:
   if group+'/'+name in tracked:
    candidate=json.loads(blob(group+'/'+name))
    if 'parts' in candidate or 'original_parts' in candidate:meta=candidate;break
  if meta is None:skipped.append(dict(group=group,reason='No supported complete part inventory'));continue
  parts=meta.get('parts',meta.get('original_parts'))
  total=meta.get('archive_bytes',meta.get('original_archive_bytes',meta.get('bytes')))
  archivehash=meta.get('archive_sha256',meta.get('original_archive_sha256'))
  partrecords=[]; combined=hashlib.sha256(); size=0; chunks=[]
  for part in parts:
   name=part.get('path',part.get('file',part.get('name'))); fixture_delta.safe_name(name)
   relative=group+'/'+name; data=blob(relative)
   assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256']
   combined.update(data);size+=len(data);chunks.append(data)
   p=repo/relative
   if p.exists():
    assert not p.is_symlink() and p.is_file() and p.stat().st_size==part['bytes'] and closed.sha(p)==part['sha256']
    partrecords.append(dict(relative=relative,sha256=part['sha256'],bytes=part['bytes'],allocated_bytes=p.stat().st_blocks*512))
  assert size==total and combined.hexdigest()==archivehash
  actual={}; embedded=None; hardlinks=0
  with tarfile.open(fileobj=io.BytesIO(b''.join(chunks)),mode='r|gz') as archive:
   for member in archive:
    fixture_delta.safe_name(member.name)
    if member.isdir():continue
    assert member.name not in actual
    if member.islnk():
     fixture_delta.safe_name(member.linkname);assert member.linkname in actual
     actual[member.name]=actual[member.linkname];hardlinks+=1
    else:
     assert member.isfile()
     if member.name=='archive-manifest.json' and meta.get('all_archive_members_and_parts_read_back'):
      assert embedded is None; embedded=json.load(archive.extractfile(member))
     else:actual[member.name]=fixture_delta.digest(archive.extractfile(member))
  del chunks
  if embedded is not None:expected=embedded
  elif 'files' in meta:expected=meta['files']
  else:
   for n in ['archive-manifest.json','original-member-manifest.json']:
    if group+'/'+n in tracked:expected=json.loads(blob(group+'/'+n))['files'];break
   else:raise ValueError('Missing full member inventory')
  assert set(actual)==set(expected), ('member census',len(actual),len(expected))
  for name,value in expected.items():
   if isinstance(value,str):assert actual[name]['sha256']==value
   else:assert actual[name]=={k:value[k] for k in ['bytes','sha256']}
  if 'verified_hardlink_members' in meta:assert hardlinks==meta['verified_hardlink_members']
  for record in partrecords:fdchecks.append(closed.verify_no_open_originals(repo/record['relative']))
  verified.append(dict(group=group,archive_bytes=total,archive_sha256=archivehash,members=len(actual),hardlinks=hardlinks,parts=len(parts)))
  records.extend(partrecords); print('VERIFIED',group,len(actual),flush=True)
 except Exception as error:
  skipped.append(dict(group=group,reason=str(error)));print('SKIPPED',group,str(error),flush=True)
assert records
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
for record in records:assert closed.sha(repo/record['relative'])==record['sha256']
out=Path('/tmp/js-wf-legacy-proof-parts-recovery-20261006');out.mkdir(exist_ok=False);shutil.copy2(__file__,out/'executed-recovery.py')
patterns=repo/'.git/info/sparse-checkout';before=patterns.read_text();(out/'patterns-before.txt').write_text(before)
patterns.write_text(before+''.join('!/'+r['relative']+'\n' for r in records));subprocess.run(['git','sparse-checkout','reapply'],cwd=repo,check=True)
for record in records:
 assert not (repo/record['relative']).exists()
 assert hashlib.sha256(blob(record['relative'])).hexdigest()==record['sha256']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
report=dict(head=head,pushed_main_matches=True,verified_archives=verified,skipped=skipped,parts=records,visible_fd_checks=fdchecks,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),free_bytes=shutil.disk_usage(repo).free,scope='Only byte-identical working-tree archive part duplicates sparse-excluded. Every selected Git archive part, concatenation and full member inventory verified. Git proof, raw archives, original/failed fixtures, executables, sources, metadata, caches and live stores retained. No NATS startup; no change to historical test verdicts or claim of live producer closure.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
