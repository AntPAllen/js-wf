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
config=s3.credentials()
def get(url,verify):
 command=['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url]
 with subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as process:
  process.stdin.write(config);process.stdin.close()
  try:result=verify(process.stdout)
  except BaseException:process.kill();process.wait();raise
  error=process.stderr.read();code=process.wait();assert code==0,('S3 GET failed',code,error.decode())
 return result
cases=[
 'docs/scale/terminal-campaigns-disk-pressure-2026-10-06/journal-store-media-offload/offload.json',
 'docs/scale/watch-observed-journal-24h-2026-10-05/store-media-offload-2026-10-06/offload.json',
 'docs/scale/continuous-byte-journal-24h-2026-10-05/store-media-offload-2026-10-06/offload.json',
 'docs/scale/byte-bounded-audit-2026-10-05/store-media-offload-2026-10-06/offload.json',
 'docs/scale/local-r5-streaming-audit-2026-10-04/explicit-routes-normal-2g-24h-store-media-offload-2026-10-06/offload.json',
]
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
blob=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+name],cwd=repo)
for previous_path in cases:
 previous=json.loads(blob(previous_path));root=Path(previous['original_root']);canonical=previous['canonical']
 meta_bytes=blob(canonical+'/archive-verification.json');inventory_bytes=blob(canonical+'/fixture-inventory.json')
 meta=json.loads(meta_bytes);manifest=json.loads(inventory_bytes);receipt=json.loads(blob(canonical+'/s3-readback.json'))
 assert meta['schema']==manifest['schema']==fixture_archive.SCHEMA
 expected_archive=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 assert receipt['archive']['full_readback']==expected_archive
 assert hashlib.sha256(inventory_bytes).hexdigest()==meta['inventory_sha256']
 prefixes=[Path(p).relative_to(root).as_posix()+'/' for p in previous['offloaded_media_paths']]
 expected={n:r for n,r in manifest['files'].items() if not any(n.startswith(p) for p in prefixes)}
 assert fixture_archive.inventory(root)==expected
 state=json.loads((root/'execution.json').read_text());assert state['status']=='failed' and state['test_exit_code']==1
 for pid in previous.get('known_sdk_supervisor_pids_closed',[]):assert not Path('/proc',str(pid)).exists()
 initial=closure(root)
 selected={n:r for n,r in expected.items() if r['bytes']>=32*1024**2 and not n.startswith(('source/','selected-source/'))}
 assert selected
 # Do not narrow verification to selected large files: recheck every remote member.
 for key,content in (('metadata',meta_bytes),('inventory',inventory_bytes)):
  assert receipt[key]['full_readback']==dict(bytes=len(content),sha256=hashlib.sha256(content).hexdigest())
  assert get(receipt[key]['url'],s3.digest)==receipt[key]['full_readback']
 remote_manifest,remote_archive=get(receipt['archive']['url'],lambda stream:fixture_archive.verify_hashed_stream(stream,expected_archive))
 assert remote_manifest==manifest
 assert fixture_archive.inventory(root)==expected
 final=closure(root)
 for n in selected:
  p=root/n;assert p.is_file() and not p.is_symlink() and p.stat().st_nlink==1
 allocated=sum((root/n).stat().st_blocks*512 for n in selected)
 out=repo/'docs/scale/tmp-storage-review-2026-10-06'/root.name/'large-artifact-offload';out.mkdir(parents=True)
 shutil.copyfile(__file__,out/'executed-offload.py')
 for n in selected:(root/n).unlink()
 remaining={n:r for n,r in expected.items() if n not in selected}
 assert fixture_archive.inventory(root)==remaining
 result=dict(head=head,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),canonical=canonical,previous_media_offload=previous_path,archive_url=receipt['archive']['url'],full_remote_archive_readback=remote_archive,all_remote_members_and_embedded_inventory_equal_committed_census=True,remote_metadata_inventory_full_readback=True,full_current_post_media_census_before_verified=True,offloaded_files=selected,allocated_bytes_recovered=allocated,local_remaining_files=len(remaining),remaining_bytes_modes_mtimes_unchanged=True,closure_before=initial,closure_after_remote_readback=final,native_state_preserved=state,scope='User-requested broader /tmp cleanup: large closed logs/binaries/partial artifacts already in complete committed S3 proof removed locally only after fresh full remote body/member/current-census and visible closure checks. Complete exact original bytes remain in verified S3 archive, including actual binaries and failed/partial evidence. Source trees, small verdict/provenance/restore ledgers retained locally. No old stores reopened or historical native/current-source/qualification upgrade. Future artifact use requires verified fresh restoration.')
 (out/'offload.json').write_text(json.dumps(result,indent=2)+'\n');print('LARGE_CLOSED_ARTIFACTS_OFFLOADED',root.name,allocated,sorted(selected),flush=True)
print('ALL_FIVE_LARGE_ARTIFACT_OFFLOADS_COMPLETED',flush=True)
