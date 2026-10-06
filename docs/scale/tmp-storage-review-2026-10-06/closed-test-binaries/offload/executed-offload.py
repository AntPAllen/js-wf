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
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
canonical='docs/scale/tmp-storage-review-2026-10-06/closed-test-binaries/complete'
blob=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+name],cwd=repo)
meta_bytes=blob('archive-verification.json');inventory_bytes=blob('fixture-inventory.json');mapping_bytes=blob('original-paths.json')
meta=json.loads(meta_bytes);manifest=json.loads(inventory_bytes);receipt=json.loads(blob('s3-readback.json'));mapping=json.loads(mapping_bytes)
expected_archive=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert receipt['archive']['full_readback']==expected_archive
assert hashlib.sha256(inventory_bytes).hexdigest()==meta['inventory_sha256']
assert manifest['files']['original-paths.json']['sha256']==hashlib.sha256(mapping_bytes).hexdigest()
roots={Path(r['root']) for r in mapping['files'].values()}
assert not any(root.name in mapping['protected_roots'] for root in roots)
print('COMMITTED_BINARY_MAPPING_AND_COMPLETE_PROOF_VERIFIED',len(mapping['files']),flush=True)
config=s3.credentials()
def get(url,verify):
 command=['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url]
 with subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as process:
  process.stdin.write(config);process.stdin.close()
  try:result=verify(process.stdout)
  except BaseException:process.kill();process.wait();raise
  error=process.stderr.read();code=process.wait();assert code==0,('S3 GET failed',code,error.decode())
 return result
for name,key,expected in [('archive-verification.json','metadata',meta_bytes),('fixture-inventory.json','inventory',inventory_bytes)]:
 assert receipt[key]['full_readback']==dict(bytes=len(expected),sha256=hashlib.sha256(expected).hexdigest())
 assert get(receipt[key]['url'],s3.digest)==receipt[key]['full_readback']
remote_manifest,remote_archive=get(receipt['archive']['url'],lambda stream:fixture_archive.verify_hashed_stream(stream,expected_archive))
assert remote_manifest==manifest
for path,r in mapping['files'].items():
 archived=manifest['files'][r['sha256']+'.test']
 assert archived['bytes']==r['bytes'] and archived['sha256']==r['sha256']
print('COMPLETE_REMOTE_BINARY_BYTES_AND_MAPPING_VERIFIED',remote_archive,flush=True)
# Fresh global process/thread-FD/container/mount/loop observation plus full SHA
# reads of candidates, with an independent output path. Original catalogue stays.
script=Path('/tmp/js-wf-catalogue-closed-binaries-20261006.py').read_text()
assert script.count('/tmp/js-wf-closed-binary-catalogue-20261006.json')==1
script=script.replace('/tmp/js-wf-closed-binary-catalogue-20261006.json','/tmp/js-wf-closed-binary-catalogue-before-offload-20261006.json')
fresh_script=Path('/tmp/js-wf-catalogue-closed-binaries-before-offload-20261006.py');fresh_script.write_text(script)
subprocess.run([sys.executable,str(fresh_script)],check=True)
fresh=json.loads(Path('/tmp/js-wf-closed-binary-catalogue-before-offload-20261006.json').read_text())
for path,r in mapping['files'].items():assert fresh['files'].get(path)==r
assert not any(str(root) in fresh['busy_roots'] for root in roots)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=repo/'docs/scale/tmp-storage-review-2026-10-06/closed-test-binaries/offload';out.mkdir()
shutil.copyfile(__file__,out/'executed-offload.py');shutil.copyfile(fresh_script,out/'executed-fresh-closure-catalogue.py')
shutil.copyfile('/tmp/js-wf-closed-binary-catalogue-before-offload-20261006.json',out/'fresh-catalogue.json')
allocated=0
for path,r in mapping['files'].items():
 p=Path(path);s=p.stat();assert not p.is_symlink() and s.st_nlink==1
 assert s.st_size==r['bytes'] and s.st_mtime_ns==r['mtime_ns'] and s.st_mode&0o777==r['mode']
 allocated+=s.st_blocks*512;p.unlink()
assert all(not Path(p).exists() for p in mapping['files'])
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),original_files_removed=len(mapping['files']),unique_binaries_preserved=len(mapping['groups']),complete_remote_archive_readback=remote_archive,archive_url=receipt['archive']['url'],remote_metadata_inventory_full_readback=True,all_original_bytes_modes_mtimes_verified_before_removal=True,fresh_global_visible_closure_and_current_candidate_catalogue='fresh-catalogue.json',protected_roots=mapping['protected_roots'],allocated_older_bytes_recovered=allocated,free_bytes=shutil.disk_usage(repo).free,scope='Closed selected regular single-link test binaries offloaded after full remote archive/member/mapping and fresh original hashes/modes/mtime/global visible closure checks. Every source/store/log/provenance file retained. No broker started or native/source/terminal/fullmatrix/24h gate qualified. Inaccessible processes recorded; no exhaustive lifetime or provider durability claim. Fresh restore of content-addressed members plus original-paths index required for future inspection.')
(out/'offload.json').write_text(json.dumps(report,indent=2)+'\n');print('BINARY_OFFLOAD',allocated,'FREE',report['free_bytes'],flush=True)
raw=Path('/tmp/js-wf-closed-test-binaries-complete-20261006.tar.gz')
with raw.open('rb') as stream:assert s3.digest(stream)==remote_archive
raw_closure=closed.verify_no_open_originals(raw);raw_allocated=raw.stat().st_blocks*512;raw.unlink()
report['temporary_archive_staging_removed']=dict(path=str(raw),allocated_bytes=raw_allocated,verified_fingerprint=remote_archive,visible_descriptor_closure=raw_closure)
report['free_bytes']=shutil.disk_usage(repo).free
(out/'offload.json').write_text(json.dumps(report,indent=2)+'\n');print('BINARY_STAGING_REMOVED',raw_allocated,'FREE',report['free_bytes'],flush=True)
