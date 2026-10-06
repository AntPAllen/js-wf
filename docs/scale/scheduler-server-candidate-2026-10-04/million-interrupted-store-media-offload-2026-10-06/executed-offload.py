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
root=Path('/tmp/js-wf-native-million-scheduler-candidate-24h-20261004')
canonical='docs/scale/scheduler-server-candidate-2026-10-04/million-interrupted-complete-offload-2026-10-06'
blob=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+name],cwd=repo)
meta_bytes=blob('archive-verification.json');inventory_bytes=blob('fixture-inventory.json')
meta=json.loads(meta_bytes);manifest=json.loads(inventory_bytes);receipt=json.loads(blob('s3-readback.json'))
assert meta['schema']==manifest['schema']==fixture_archive.SCHEMA
expected_archive=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert receipt['archive']['full_readback']==expected_archive
assert hashlib.sha256(inventory_bytes).hexdigest()==meta['inventory_sha256']
assert fixture_archive.inventory(root)==manifest['files']
execution=json.loads((root/'campaign/report.json').read_text());assert execution['status']=='running'  # Retained stale partial report; closure is checked independently.
capture=json.loads(blob('capture.json'))
for pid in capture['known_sdk_supervisor_pids_closed']:assert not Path('/proc',str(pid)).exists()
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show',capture['producer_unit_name'],'-p','ActiveState','-p','MainPID'],text=True).splitlines())
assert unit['ActiveState'] in ('failed','inactive') and unit['MainPID']=='0'
initial_closure=closure(root)
print('COMPLETE_CURRENT_CENSUS_AND_CLOSURE_VERIFIED',len(manifest['files']),flush=True)
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
print('FULL_REMOTE_COMPRESSED_BODY_AND_ALL_MEMBERS_VERIFIED',remote_archive,flush=True)
assert fixture_archive.inventory(root)==manifest['files']
final_closure=closure(root)
media=[root/'campaign/cluster'/('node-'+str(i))/'jetstream' for i in range(3)]
prefixes=[p.relative_to(root).as_posix()+'/' for p in media]
for path in media:
 assert path.is_dir() and not path.is_symlink()
 for file in path.rglob('*'):
  assert not file.is_symlink()
  if file.is_file():assert file.stat().st_nlink==1
removed={name:record for name,record in manifest['files'].items() if any(name.startswith(prefix) for prefix in prefixes)}
assert removed and len(removed)<len(manifest['files'])
remaining={name:record for name,record in manifest['files'].items() if name not in removed}
assert 'wf-timer-volume' in remaining and 'campaign/nats-server-candidate' in remaining and 'campaign/report.json' in remaining
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=repo/'docs/scale/scheduler-server-candidate-2026-10-04/million-interrupted-store-media-offload-2026-10-06';out.mkdir()
shutil.copyfile(__file__,out/'executed-offload.py')
allocated=sum(file.stat().st_blocks*512 for path in media for file in path.rglob('*') if file.is_file())
for path in media:shutil.rmtree(path)
assert fixture_archive.inventory(root)==remaining
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),original_root=str(root),canonical=canonical,archive_url=receipt['archive']['url'],archive_sha256=meta['archive_sha256'],complete_remote_archive_readback=remote_archive,all_remote_members_and_embedded_inventory_equal_committed_census=True,remote_metadata_inventory_full_readback=True,current_full_census_verified_before_offload=True,known_sdk_supervisor_pids_closed=capture['known_sdk_supervisor_pids_closed'],producer_unit=unit,closure_before=initial_closure,closure_after_remote_readback=final_closure,offloaded_media_paths=[str(path) for path in media],offloaded_files=len(removed),local_remaining_files=len(remaining),allocated_bytes_recovered=allocated,free_bytes=shutil.disk_usage(repo).free,remaining_bytes_modes_mtimes_unchanged=True,native_verdict='interrupted; no terminal SDK exit/drain observed',preserved_report_status='running',candidate_adopted=False,original_stores_reopened=False,scope='Closed interrupted unadopted million-candidate store media offloaded to complete committed S3 archive after fresh full compressed-body/member/current-file and closure checks. Source/executables/configs/logs/partial artifacts/metadata retained locally; complete original store bytes remain in verified remote archive. Any future audit requires a newly restored verified copy. No terminal SDK exit assertion, million/24h qualification, candidate adoption or independent S3 durability claim.')
(out/'offload.json').write_text(json.dumps(report,indent=2)+'\n');print('OFFLOADED',allocated,'FREE',report['free_bytes'],flush=True)
raw=Path('/tmp/js-wf-native-million-interrupted-complete-offload-20261006.tar.gz')
with raw.open('rb') as stream:assert s3.digest(stream)==remote_archive
raw_closure=closed.verify_no_open_originals(raw)
raw_allocated=raw.stat().st_blocks*512
raw.unlink()
report['verified_local_raw_duplicate_removed']=dict(path=str(raw),fingerprint=remote_archive,allocated_bytes_recovered=raw_allocated,visible_descriptor_closure=raw_closure)
report['free_bytes']=shutil.disk_usage(repo).free
(out/'offload.json').write_text(json.dumps(report,indent=2)+'\n')
print('RAW_DUPLICATE_REMOVED',raw_allocated,'FREE',report['free_bytes'],flush=True)
