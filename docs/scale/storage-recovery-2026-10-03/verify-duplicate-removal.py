import gzip, hashlib, json, os, pathlib, shutil, time
base=pathlib.Path('/tmp/js-wf-scale-live-inline-5m-20260930')
dup=pathlib.Path('/dev/shm/js-wf-inline5m-restoration-20261003')
archive=base/'broker-store-archive'
expected_manifest='6412ac26751a841a7f2e358b28c7f54daac382a028309c3332487fda5bc8e178'
expected_report='5a25cd93d6b93e9fe5f6364cc60e1854d96f21210bd19cc3e656417483b0026b'
def digest(p, compressed=False):
 h=hashlib.sha256(); n=0
 with (gzip.open(p,'rb') if compressed else p.open('rb')) as f:
  while b:=f.read(1024*1024): h.update(b); n+=len(b)
 return h.hexdigest(),n
assert digest(archive/'manifest.jsonl')[0]==expected_manifest
reports=list(base.glob('*report*.json'))
assert any(digest(p)[0]==expected_report for p in reports), reports
rows=[json.loads(x) for x in (archive/'manifest.jsonl').read_text().splitlines()]
assert len(rows)==2543
expected=set(); total=0
for row in rows:
 original=pathlib.PurePosixPath(row['original']); zipped=pathlib.PurePosixPath(row['archive'])
 assert not original.is_absolute() and '..' not in original.parts
 assert not zipped.is_absolute() and '..' not in zipped.parts
 a=base/zipped; p=dup/original; expected.add(str(original))
 assert not p.is_symlink() and not a.is_symlink()
 assert digest(a)==(row['archive_sha256'],row['archive_bytes']),a
 assert digest(a,True)==(row['sha256'],row['bytes']),a
 assert digest(p)==(row['sha256'],row['bytes']),p
 st=p.stat(); assert st.st_mode & 0o7777==row['mode'],p
 assert st.st_mtime_ns==row['mtime_ns'],p
 total+=row['bytes']
actual={str(p.relative_to(dup)) for p in dup.rglob('*') if p.is_file()}
assert actual==expected
for proc in pathlib.Path('/proc').glob('[0-9]*'):
 for fd in (proc/'fd').glob('*'):
  try: target=os.readlink(fd)
  except (FileNotFoundError,PermissionError,ProcessLookupError): continue
  assert not target.startswith(str(dup)+'/'), (proc,fd,target)
assert dup.resolve()==dup and dup.parent==pathlib.Path('/dev/shm')
verdict={'files':len(rows),'duplicate_raw_bytes':total,'canonical_manifest_sha256':expected_manifest,'canonical_report_sha256':expected_report,'compressed_hashes_verified':True,'decompressed_hashes_verified':True,'restored_hashes_modes_timestamps_verified':True,'no_open_duplicate_file_descriptors':True,'canonical_archive_retained':str(archive),'duplicate_removed':str(dup),'verified_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
out=pathlib.Path('/tmp/js-wf-restoration-duplicate-removal-verdict.json')
out.write_text(json.dumps(verdict,indent=2)+'\n')
shutil.rmtree(dup)
verdict['removal_complete']=not dup.exists()
out.write_text(json.dumps(verdict,indent=2)+'\n')
print(out.read_text())
