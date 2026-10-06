from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 for c in running:
  for m in c['Mounts']:
   source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
 loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
 for device in loops['loopdevices']:
  assert not Path(device['back-file']).resolve().is_relative_to(root)
 mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
 def walk(rows):
  for row in rows:
   assert not Path(row['target']).resolve().is_relative_to(root)
   assert not row['source'].startswith(str(root))
   walk(row.get('children',[]))
 walk(mounts['filesystems'])
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids)
cases=[
 ('js-wf-scale-live-inline-10m-20260930','live-cardinality-inline-10m-2026-09-30','terminal-scale-store-archives-2026-10-02/js-wf-scale-live-inline-10m-20260930-manifest.jsonl.gz'),
 ('js-wf-scale-spilled-10m-20261002','live-cardinality-spilled-10m-2026-10-02','terminal-scale-store-archives-2026-10-03/manifest.jsonl.gz'),
 ('js-wf-scale-live-large-5m-20260930','live-cardinality-large-5m-2026-09-30','terminal-scale-store-archives-2026-10-02/js-wf-scale-live-large-5m-20260930-manifest.jsonl.gz'),
 ('js-wf-scale-live-inline-5m-20260930','live-cardinality-inline-5m-2026-09-30',None),
 ('js-wf-scale-live-1m-20260930','live-cardinality-1m-2026-09-30','terminal-scale-store-archives-2026-10-02/js-wf-scale-live-1m-20260930-manifest.jsonl.gz'),
]
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
import gzip
for name,native,previous in cases:
 root=Path('/tmp')/name
 report_bytes=(root/'report.json').read_bytes();report=json.loads(report_bytes)
 original=subprocess.check_output(['git','show',head+':docs/scale/'+native+'/report.json'+('.gz' if 'spilled' in name else '')],cwd=repo)
 if 'spilled' in name:original=gzip.decompress(original)
 assert report_bytes==original and report['status']=='completed'
 summary=json.loads((root/'broker-store-archive/summary.json').read_text())
 manifest_bytes=(root/'broker-store-archive/manifest.jsonl').read_bytes()
 assert summary['status']=='complete' and summary['all_decompressed_bytes_verified']
 assert summary['report_sha256']==hashlib.sha256(report_bytes).hexdigest()
 assert summary['manifest_sha256']==hashlib.sha256(manifest_bytes).hexdigest()
 if previous:
  assert gzip.decompress(subprocess.check_output(['git','show',head+':docs/scale/'+previous],cwd=repo))==manifest_bytes
 rows=[json.loads(line) for line in manifest_bytes.splitlines()]
 assert len(rows)==summary['files']
 assert len({r['archive'] for r in rows})==len(rows)
 expected=set()
 for r in rows:
  file=root/r['archive'];assert file.is_relative_to(root/'broker-store-archive')
  assert not file.is_symlink() and file.is_file()
  with file.open('rb') as stream:compressed=s3.digest(stream)
  assert compressed['bytes']==r['archive_bytes']
  if 'archive_sha256' in r:assert compressed['sha256']==r['archive_sha256']
  with gzip.open(file,'rb') as stream:assert s3.digest(stream)==dict(bytes=r['bytes'],sha256=r['sha256'])
  expected.add(r['archive'])
 actual={str(p.relative_to(root)) for p in (root/'broker-store-archive').rglob('*.gz')}
 assert actual==expected
 initial=closure(root);print('LEGACY_REPORT_GZIP_MEMBERS_CLOSURE_VERIFIED',name,len(rows),flush=True)
 out=repo/'docs/scale/closed-scale-s3-2026-10-06'/name/'complete'
 raw=Path('/tmp')/(name+'-complete-s3-20261006.tar.gz')
 proof=fixture_archive.capture(root,raw,out);final=closure(root)
 assert fixture_archive.inventory(root)==json.loads((out/'fixture-inventory.json').read_text())['files']
 shutil.copyfile(__file__,out/'executed-capture.py')
 capture=dict(head=head,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),archive=str(raw),complete_archive=proof,native_revision=report['revision'],native_report_equals_committed_bytes=True,legacy_manifest_equals_committed_bytes=bool(previous),legacy_summary_manifest_hash_bound=True,all_legacy_gzip_original_hashes_and_sizes_verified=len(rows),legacy_compressed_hash_records=sum('archive_sha256' in r for r in rows),legacy_compressed_hash_scope='Older manifest schema binds decompressed SHA/size and compressed size, not compressed SHA; modern complete census binds every current compressed byte. No historical compressed SHA invented.',source_native_evidence='Existing canonical report preserved; this root has no retained SDK/source lifetime inventory. No new execution/source/qualification claim.',known_sdk_supervisor_pids_closed=[],producer_unit_name=None,producer_lifetime_scope='Historical unit/PIDs not captured here; only global visible root process/descriptor/mount closure verified. No observed terminal SDK exit claim.',closure_before=initial,closure_after=final,scope='Complete present terminal historical cardinality fixture preserved without broker stores reopened; existing native report scope unchanged. Compressed broker media may be removed only after committed complete proof and full fresh S3 member/body/current census/closure checks. Future restore must verify modern complete archive then legacy decompression ledger.')
 (out/'capture.json').write_text(json.dumps(capture,indent=2)+'\n')
 print('COMPLETE_LEGACY_SCALE_CAPTURE_VERIFIED',name,proof,flush=True)
